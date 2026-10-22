//! `--client-ca-file` / `--requestheader-client-ca-file` hot reload (#3426).
//!
//! A REAL TLS handshake against the production serving wiring. Upstream builds
//! both CA providers with `dynamiccertificates.NewDynamicCAContentFromFile`
//! (staging/src/k8s.io/apiserver/pkg/server/options/authentication.go:165,
//! :238), so rotating a CA file takes effect with no restart: a cert signed by
//! a CA that was added to the file is accepted, and one whose CA was removed
//! is refused.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rusternetes_api_server::request_header_authentication::RequestHeaderTrust;
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

fn tmp_pem(ca: &TestCa) -> PathBuf {
    let p = std::env::temp_dir().join(format!("rn-dyn-ca-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&p, ca.cert.pem().as_bytes()).unwrap();
    p
}

/// Rotate in place (`os.WriteFile`: an IN_MODIFY on the watched inode).
fn rewrite(path: &Path, cas: &[&TestCa]) {
    let pem: String = cas.iter().map(|c| c.cert.pem()).collect();
    std::fs::write(path, pem).unwrap();
}

/// Rotate the way a mounted ConfigMap/Secret does: write a sibling and rename
/// it over the file (the watched inode gets IN_DELETE_SELF/IN_MOVE_SELF).
fn replace_by_rename(path: &Path, cas: &[&TestCa]) {
    let tmp = path.with_extension("new");
    rewrite(&tmp, cas);
    std::fs::rename(&tmp, path).unwrap();
}

struct Harness {
    base: String,
    server_ca_root: reqwest::Certificate,
    client_ca_path: PathBuf,
    proxy_ca_path: PathBuf,
    // Keeps the reload tasks alive for the life of the test.
    _reload: Option<Box<dyn std::any::Any + Send>>,
}

async fn start(client_ca: &TestCa, proxy_ca: &TestCa) -> Harness {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let server_ca = TestCa::new("rusternetes-server-ca");
    let server = server_ca.issue("rusternetes-apiserver", vec!["127.0.0.1".into()], false);
    let client_ca_path = tmp_pem(client_ca);
    let proxy_ca_path = tmp_pem(proxy_ca);

    let tls_config = TlsConfig {
        cert: vec![server.cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server.key.serialize_der())),
        cert_pem: None,
    };
    let rh_config = Arc::new(
        RequestHeaderConfig::new(&["X-Remote-User".to_string()], &[], &[], &[], &[]).unwrap(),
    );
    let trust = RequestHeaderTrust {
        config: rh_config,
        client_ca_file: proxy_ca_path.to_str().unwrap().to_string(),
    };

    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let state = Arc::new(ApiServerState::new(
        backend,
        Arc::new(TokenManager::new(b"dyn-ca-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        false,
    ));
    let app = build_router(state, None);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let (acceptor, reload) = rusternetes_api_server::request_header_authentication::mtls_acceptor(
        tls_config,
        Some(client_ca_path.to_str().unwrap()),
        Some(&trust),
    )
    .expect("mtls_acceptor");
    tokio::spawn(async move {
        axum_server::from_tcp(listener)
            .acceptor(acceptor)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    Harness {
        base: format!("https://127.0.0.1:{port}"),
        server_ca_root: reqwest::Certificate::from_pem(server_ca.cert.pem().as_bytes()).unwrap(),
        client_ca_path,
        proxy_ca_path,
        _reload: Some(Box::new(reload)),
    }
}

impl Harness {
    fn client(&self, leaf: &Leaf) -> reqwest::Client {
        let pem = format!("{}{}", leaf.cert.pem(), leaf.key.serialize_pem());
        reqwest::Client::builder()
            .add_root_certificate(self.server_ca_root.clone())
            .use_rustls_tls()
            .identity(reqwest::Identity::from_pem(pem.as_bytes()).unwrap())
            .build()
            .unwrap()
    }

    /// `Some((status, username))`, or `None` when the handshake is refused.
    async fn whoami(&self, leaf: &Leaf, headers: &[(&str, &str)]) -> Option<(u16, String)> {
        let mut req = self
            .client(leaf)
            .post(format!("{}{SSR_PATH}", self.base))
            .json(&json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "SelfSubjectReview"
            }));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.ok()?;
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let user = body
            .pointer("/status/userInfo/username")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        Some((status, user))
    }

    /// Poll until `whoami` returns `want` (reload is asynchronous: inotify
    /// event -> queue -> worker), or fail after 10s.
    async fn eventually(&self, leaf: &Leaf, headers: &[(&str, &str)], want: Option<(u16, &str)>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = self.whoami(leaf, headers).await;
            let ok = match (&got, &want) {
                (None, None) => true,
                (Some((s, u)), Some((ws, wu))) => s == ws && u == wu,
                _ => false,
            };
            if ok {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "never reached {want:?}, last saw {got:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ca_added_to_the_file_is_trusted_without_restart() {
    let ca_a = TestCa::new("ca-a");
    let ca_b = TestCa::new("ca-b");
    let proxy = TestCa::new("proxy-ca");
    let h = start(&ca_a, &proxy).await;
    let alice = ca_a.issue("alice", vec![], true);
    let bob = ca_b.issue("bob", vec![], true);

    h.eventually(&alice, &[], Some((201, "alice"))).await;
    // CA B is unknown: the handshake is refused.
    assert_eq!(h.whoami(&bob, &[]).await, None);

    rewrite(&h.client_ca_path, &[&ca_a, &ca_b]);
    h.eventually(&bob, &[], Some((201, "bob"))).await;
    h.eventually(&alice, &[], Some((201, "alice"))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ca_replaced_by_rename_and_rotated_out_is_refused() {
    let ca_a = TestCa::new("ca-a");
    let ca_b = TestCa::new("ca-b");
    let proxy = TestCa::new("proxy-ca");
    let h = start(&ca_a, &proxy).await;
    let alice = ca_a.issue("alice", vec![], true);
    let bob = ca_b.issue("bob", vec![], true);
    h.eventually(&alice, &[], Some((201, "alice"))).await;

    // A ConfigMap/Secret-style atomic swap, then a second swap: the watch must
    // be re-established after each Remove/Rename (handleWatchEvent).
    replace_by_rename(&h.client_ca_path, &[&ca_b]);
    h.eventually(&bob, &[], Some((201, "bob"))).await;
    h.eventually(&alice, &[], None).await;
    replace_by_rename(&h.client_ca_path, &[&ca_a]);
    h.eventually(&alice, &[], Some((201, "alice"))).await;
    h.eventually(&bob, &[], None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requestheader_ca_rotation_re_decides_which_authenticator_owns_a_cert() {
    let ca_a = TestCa::new("ca-a");
    let proxy_1 = TestCa::new("proxy-1");
    let proxy_2 = TestCa::new("proxy-2");
    let h = start(&ca_a, &proxy_1).await;
    let p1 = proxy_1.issue("front-proxy", vec![], true);
    let p2 = proxy_2.issue("front-proxy", vec![], true);
    let hdr = [("X-Remote-User", "carol")];

    h.eventually(&p1, &hdr, Some((201, "carol"))).await;
    assert_eq!(h.whoami(&p2, &hdr).await, None);

    rewrite(&h.proxy_ca_path, &[&proxy_2]);
    h.eventually(&p2, &hdr, Some((201, "carol"))).await;
    h.eventually(&p1, &hdr, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unparsable_replacement_keeps_serving_the_last_good_bundle() {
    let ca_a = TestCa::new("ca-a");
    let proxy = TestCa::new("proxy-ca");
    let h = start(&ca_a, &proxy).await;
    let alice = ca_a.issue("alice", vec![], true);
    h.eventually(&alice, &[], Some((201, "alice"))).await;

    // loadCABundle: empty content errors and the old value stays.
    std::fs::write(&h.client_ca_path, b"").unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.whoami(&alice, &[]).await, Some((201, "alice".into())));
    std::fs::write(&h.client_ca_path, b"not a certificate").unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.whoami(&alice, &[]).await, Some((201, "alice".into())));
}
