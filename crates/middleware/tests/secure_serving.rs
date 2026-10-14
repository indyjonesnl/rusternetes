//! End to end: the secure server of the controller-manager / scheduler over a
//! real TLS socket, with the delegating authn/authz handler chain (#3437).
//!
//! Upstream behaviour pinned here:
//! * `tls.RequestClientCert` (secure_serving.go:78-82): a client certificate is
//!   requested but not verified by the TLS layer; the x509 authenticator
//!   verifies it, so an untrusted certificate is a 401 and not a failed handshake.
//! * `system:masters` is always allowed, `/healthz` `/livez` `/readyz` are always
//!   allowed (`authorization-always-allow-paths`), everything else needs the
//!   delegated SubjectAccessReview (none here: no kubeconfig, so it is denied).
//! * anonymous requests are authenticated as `system:anonymous` (403, not 401).

use axum::routing::get;
use axum::Router;
use rusternetes_middleware::secure_serving::filters::build_handler_chain;
use rusternetes_middleware::secure_serving::options::{
    DelegatingAuthenticationOptions, DelegatingAuthorizationOptions, SecureServingOptions,
};
use rusternetes_middleware::secure_serving::serve::serve;

struct Pki {
    ca: rcgen::Certificate,
    key: rcgen::KeyPair,
}

fn pki(name: &str) -> Pki {
    let mut p = rcgen::CertificateParams::default();
    p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name.push(rcgen::DnType::CommonName, name);
    p.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    let key = rcgen::KeyPair::generate().unwrap();
    let ca = p.self_signed(&key).unwrap();
    Pki { ca, key }
}

/// A client identity (cert + key PEM) signed by `pki`.
fn identity(pki: &Pki, cn: &str, org: &str) -> reqwest::Identity {
    let mut p = rcgen::CertificateParams::default();
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name.push(rcgen::DnType::CommonName, cn);
    p.distinguished_name
        .push(rcgen::DnType::OrganizationName, org);
    p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = p.signed_by(&key, &pki.ca, &pki.key).unwrap();
    let pem = format!("{}\n{}", cert.pem(), key.serialize_pem());
    reqwest::Identity::from_pem(pem.as_bytes()).unwrap()
}

fn client(identity: Option<reqwest::Identity>) -> reqwest::Client {
    let mut b = reqwest::Client::builder().danger_accept_invalid_certs(true);
    if let Some(i) = identity {
        b = b.identity(i);
    }
    b.build().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn secure_port_authenticates_and_authorizes_like_the_upstream_chain() {
    let trusted = pki("client-ca");
    let other = pki("other-ca");
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("client-ca.crt");
    std::fs::write(&ca_file, trusted.ca.pem()).unwrap();

    // SecureServingOptions as the controller-manager configures them: an
    // in-memory self-signed certificate for `localhost` / 127.0.0.1.
    let mut serving = SecureServingOptions::new();
    serving.bind_address = "127.0.0.1".parse().unwrap();
    serving.bind_port = 0;
    serving.listener = Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    serving.server_cert.cert_directory = String::new();
    serving.server_cert.pair_name = "test".into();
    serving
        .maybe_default_with_self_signed_certs("localhost", &[], &["127.0.0.1".parse().unwrap()])
        .unwrap();
    let mut info = serving.apply_to().unwrap().unwrap();

    // RemoteKubeConfigFileOptional: no kubeconfig and not in a cluster.
    let mut authn = DelegatingAuthenticationOptions::new();
    authn.remote_kubeconfig_file_optional = true;
    authn.client_ca = ca_file.to_str().unwrap().to_string();
    let authn = authn.apply_to(Some(&mut info)).await.unwrap();
    let mut authz = DelegatingAuthorizationOptions::new();
    authz.remote_kubeconfig_file_optional = true;
    let authz = authz.apply_to().await.unwrap();

    let routes = Router::new()
        .route("/metrics", get(|| async { "metrics" }))
        .route("/healthz", get(|| async { "ok" }));
    let router = build_handler_chain(routes, Some(authz.authorizer), authn.authenticator);
    let addr = info.local_addr().unwrap();
    let server = serve(router, info).unwrap();
    let url = |p: &str| format!("https://127.0.0.1:{}{}", addr.port(), p);

    // anonymous: allowed on the always-allow path only
    let c = client(None);
    let r = c.get(url("/healthz")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["cache-control"], "no-cache, private");
    let r = c.get(url("/metrics")).send().await.unwrap();
    assert_eq!(r.status(), 403);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["message"],
        "forbidden: User \"system:anonymous\" cannot get path \"/metrics\""
    );

    // a bearer token cannot be validated without an authentication-kubeconfig:
    // the request stays anonymous
    let r = c
        .get(url("/metrics"))
        .header("authorization", "Bearer whatever")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // system:masters through a client certificate the client CA signed
    let masters = client(Some(identity(&trusted, "admin", "system:masters")));
    let r = masters.get(url("/metrics")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "metrics");

    // an authenticated user without rights is a 403 for that user
    let nobody = client(Some(identity(&trusted, "someone", "no-group")));
    let r = nobody.get(url("/metrics")).send().await.unwrap();
    assert_eq!(r.status(), 403);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["message"],
        "forbidden: User \"someone\" cannot get path \"/metrics\""
    );

    // a certificate from an unknown CA is requested but not verified by the TLS
    // layer; the x509 authenticator rejects it: 401, not a failed handshake
    let stranger = client(Some(identity(&other, "admin", "system:masters")));
    let r = stranger.get(url("/healthz")).send().await.unwrap();
    assert_eq!(r.status(), 401);

    server
        .handle
        .graceful_shutdown(Some(std::time::Duration::from_secs(1)));
    let _ = server.task.await;
}
