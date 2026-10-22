//! End-to-end front-proxy (`--requestheader-*`) authentication (#3414).
//!
//! A REAL TLS handshake against the production serving path:
//! `TlsConfig::into_mtls_server_config_for_cas([--client-ca-file,
//! --requestheader-client-ca-file])` served through
//! `PeerCertAcceptor::with_request_header`, `skip_auth = false`. Identity is
//! read back through `SelfSubjectReview`.
//!
//! Upstream behaviour pinned here (every point is a rule in
//! `pkg/kubeapiserver/authenticator/config.go:113-131` / `x509.Verifier`):
//!   * a cert signed by the front-proxy CA with an allowed CN authenticates
//!     the `X-Remote-*` headers;
//!   * the same headers on a connection without such a cert are NOT trusted;
//!   * a cert from the regular client CA still maps CN -> user and its
//!     `X-Remote-*` headers are ignored;
//!   * a CN outside `--requestheader-allowed-names` is rejected (401);
//!   * a front-proxy-CA cert carrying no username header is rejected by the
//!     x509 authenticator (it does not verify against `--client-ca-file`).

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rusternetes_api_server::request_header_authentication::{mtls_acceptor, RequestHeaderTrust};
use rusternetes_api_server::router::build_router;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_middleware::request_header::RequestHeaderConfig;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::StorageBackend;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{json, Value};

const SSR_PATH: &str = "/apis/authentication.k8s.io/v1/selfsubjectreviews";

struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
}

struct Leaf {
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl TestCa {
    fn new(cn: &str) -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, cn);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key }
    }

    fn issue(&self, cn: &str, sans: Vec<String>, client_auth: bool) -> Leaf {
        let mut params = CertificateParams::new(sans).unwrap();
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(DnType::CommonName, cn);
        params.distinguished_name = dn;
        params.extended_key_usages = vec![if client_auth {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        Leaf { cert, key }
    }
}

struct Harness {
    base: String,
    client_ca: TestCa,
    proxy_ca: TestCa,
    server_ca_root: reqwest::Certificate,
}

fn write_ca(ca: &TestCa) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("rn-rh-ca-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&p, ca.cert.pem().as_bytes()).unwrap();
    p
}

async fn start(allowed_names: &[&str]) -> Harness {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let client_ca = TestCa::new("rusternetes-client-ca");
    let proxy_ca = TestCa::new("rusternetes-front-proxy-ca");
    let server = client_ca.issue("rusternetes-apiserver", vec!["127.0.0.1".into()], false);

    let client_ca_path = write_ca(&client_ca);
    let proxy_ca_path = write_ca(&proxy_ca);

    let tls_config = TlsConfig {
        cert: vec![server.cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server.key.serialize_der())),
        cert_pem: None,
    };
    let names: Vec<String> = allowed_names.iter().map(|s| s.to_string()).collect();
    let rh_config = Arc::new(
        RequestHeaderConfig::new(
            &["X-Remote-User".to_string()],
            &["X-Remote-Uid".to_string()],
            &["X-Remote-Group".to_string()],
            &["X-Remote-Extra-".to_string()],
            &names,
        )
        .unwrap(),
    );
    let trust = RequestHeaderTrust {
        config: rh_config,
        client_ca_file: proxy_ca_path.to_str().unwrap().to_string(),
    };
    // The production wiring: dynamic CA providers watch the files, so they
    // must outlive the server.
    let (acceptor, reload) = mtls_acceptor(
        tls_config,
        Some(client_ca_path.to_str().unwrap()),
        Some(&trust),
    )
    .expect("mtls acceptor");
    std::mem::forget(reload);

    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let state = Arc::new(ApiServerState::new(
        backend,
        Arc::new(TokenManager::new(b"rh-e2e-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        false,
    ));
    let app = build_router(state, None);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum_server::from_tcp(listener)
            .acceptor(acceptor)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let server_ca_root = reqwest::Certificate::from_pem(client_ca.cert.pem().as_bytes()).unwrap();
    Harness {
        base: format!("https://127.0.0.1:{port}"),
        client_ca,
        proxy_ca,
        server_ca_root,
    }
}

impl Harness {
    fn client(&self, leaf: Option<&Leaf>) -> reqwest::Client {
        let mut b = reqwest::Client::builder()
            .add_root_certificate(self.server_ca_root.clone())
            .use_rustls_tls();
        if let Some(leaf) = leaf {
            let pem = format!("{}{}", leaf.cert.pem(), leaf.key.serialize_pem());
            b = b.identity(reqwest::Identity::from_pem(pem.as_bytes()).unwrap());
        }
        b.build().unwrap()
    }

    /// POST a SelfSubjectReview with the given extra request headers.
    async fn ssr(&self, client: &reqwest::Client, headers: &[(&str, &str)]) -> (u16, Value) {
        let mut req = client
            .post(format!("{}{SSR_PATH}", self.base))
            .json(&json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "SelfSubjectReview"
            }));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.expect("request");
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        (
            status,
            body.pointer("/status/userInfo")
                .cloned()
                .unwrap_or(Value::Null),
        )
    }
}

const PROXY_HEADERS: &[(&str, &str)] = &[
    ("X-Remote-User", "alice"),
    ("X-Remote-Uid", "uid-alice"),
    ("X-Remote-Group", "devs"),
    ("X-Remote-Extra-Scopes", "read"),
];

fn username(ui: &Value) -> Option<&str> {
    ui.get("username").and_then(Value::as_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn front_proxy_cert_authenticates_remote_user_headers() {
    let h = start(&["front-proxy-client"]).await;
    let proxy = h.proxy_ca.issue("front-proxy-client", vec![], true);
    let (status, ui) = h.ssr(&h.client(Some(&proxy)), PROXY_HEADERS).await;
    assert_eq!(status, 201, "userInfo={ui:?}");
    assert_eq!(username(&ui), Some("alice"));
    assert_eq!(
        ui.pointer("/uid").and_then(Value::as_str),
        Some("uid-alice")
    );
    let groups: Vec<&str> = ui
        .get("groups")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    // group.NewAuthenticatedGroupAdder (config.go:240).
    assert_eq!(groups, ["devs", "system:authenticated"], "{ui:?}");
    assert_eq!(
        ui.pointer("/extra/scopes").cloned(),
        Some(json!(["read"])),
        "{ui:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_user_headers_without_a_front_proxy_cert_are_not_trusted() {
    let h = start(&[]).await;
    // No client cert at all: anonymous, headers ignored.
    let (status, ui) = h.ssr(&h.client(None), PROXY_HEADERS).await;
    assert_eq!(status, 201);
    assert_eq!(username(&ui), Some("system:anonymous"), "{ui:?}");

    // A regular client-CA cert: authenticated by CN, headers ignored.
    let user = h.client_ca.issue("test-user", vec![], true);
    let (status, ui) = h.ssr(&h.client(Some(&user)), PROXY_HEADERS).await;
    assert_eq!(status, 201, "{ui:?}");
    assert_eq!(username(&ui), Some("test-user"), "{ui:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn front_proxy_cn_outside_allowed_names_is_rejected() {
    let h = start(&["front-proxy-client"]).await;
    let other = h.proxy_ca.issue("other-proxy", vec![], true);
    let (status, _) = h.ssr(&h.client(Some(&other)), PROXY_HEADERS).await;
    assert_eq!(status, 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_allowed_names_accepts_any_front_proxy_ca_cert() {
    let h = start(&[]).await;
    let other = h.proxy_ca.issue("whatever", vec![], true);
    let (status, ui) = h.ssr(&h.client(Some(&other)), PROXY_HEADERS).await;
    assert_eq!(status, 201, "{ui:?}");
    assert_eq!(username(&ui), Some("alice"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn front_proxy_cert_without_username_header_is_rejected_by_x509() {
    let h = start(&[]).await;
    let proxy = h.proxy_ca.issue("front-proxy-client", vec![], true);
    // Header authenticator: not ok. x509 (verifies against --client-ca-file
    // only): error. The union fails the request.
    let (status, _) = h.ssr(&h.client(Some(&proxy)), &[]).await;
    assert_eq!(status, 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spoofed_headers_never_reach_handlers_after_authentication() {
    // WithAuthentication strips the standard front-proxy headers from every
    // authenticated/anonymous request (filters/authentication.go:91-98); the
    // identity above came from the authenticator, not from a handler reading
    // the raw header. With a regular cert plus X-Remote-User: root the user
    // stays the cert's CN (covered above) - here, anonymous plus a spoofed
    // header must not become that user either.
    let h = start(&[]).await;
    let (status, ui) = h
        .ssr(&h.client(None), &[("X-Remote-User", "system:admin")])
        .await;
    assert_eq!(status, 201);
    assert_eq!(username(&ui), Some("system:anonymous"), "{ui:?}");
}
