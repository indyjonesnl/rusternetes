//! End-to-end coverage for the kubelet's API-mode node-registration transport:
//! `ApiClient` POSTing a Node to an **external HTTPS api-server with a
//! self-signed cert**, using `insecure-skip-tls-verify` (Mikronetes M2b worker
//! kubelet: `--kubeconfig` server `https://10.88.0.2:6443`,
//! `insecure-skip-tls-verify: true`).
//!
//! Two properties are asserted:
//!   1. With `insecure_skip_tls_verify = true` the client completes the TLS
//!      handshake against a self-signed cert and the POST succeeds (this is the
//!      exact transport `register_node` -> `ApiStorage::create` -> `post` runs).
//!   2. With verification ON (system roots) the same POST fails, and the error
//!      surfaces the *actionable* detail — the target URL and a TLS/cert cause —
//!      instead of an opaque "Failed to send POST request". Regression for the
//!      M2b opaque-error hunt.

use std::sync::Arc;

use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Spawn a genuine TLS server (self-signed cert for `localhost`/`127.0.0.1`)
/// that answers a single POST to `/api/v1/nodes` with `201 Created` echoing a
/// Node object back (as a real api-server would). Returns the bound port.
///
/// Mirrors the real-TLS test server pattern introduced for the HTTPS
/// lifecycle-hook coverage (#1588): tokio-rustls + rcgen, no client auth.
async fn spawn_fake_apiserver_https() -> u16 {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_der = certified.cert.der().clone();
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
    // Install a process-default crypto provider for the server side; the reqwest
    // client bundles its own. Ignore the error if another test already did it.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der.into())
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                // Drain the request (headers + body) enough to unblock the
                // client; we don't need to parse it for this test.
                let mut buf = [0_u8; 4096];
                let _ = tls.read(&mut buf).await;
                let node = json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": { "name": "node-2", "resourceVersion": "1" },
                })
                .to_string();
                let resp = format!(
                    "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    node.len(),
                    node
                );
                let _ = tls.write_all(resp.as_bytes()).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insecure_https_node_registration_succeeds() {
    let port = spawn_fake_apiserver_https().await;
    let base = format!("https://127.0.0.1:{port}");

    // insecure_skip_tls_verify = true — exactly the kubelet API-mode wiring for a
    // self-signed api-server cert.
    let client =
        Arc::new(ApiClient::with_tls(&base, true, None, None, None, None).expect("build client"));

    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": "node-2" },
    });
    let created: Value = client
        .post("/api/v1/nodes", &node)
        .await
        .expect("insecure HTTPS node registration must succeed against a self-signed api-server");
    assert_eq!(created["metadata"]["name"], "node-2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_https_registration_error_names_url_and_cause() {
    let port = spawn_fake_apiserver_https().await;
    let base = format!("https://127.0.0.1:{port}");

    // Verification ON, no CA supplied -> the self-signed cert is rejected.
    let client =
        Arc::new(ApiClient::with_tls(&base, false, None, None, None, None).expect("build client"));

    let node = json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"node-2"}});
    let err = client
        .post::<_, Value>("/api/v1/nodes", &node)
        .await
        .expect_err("verified POST against a self-signed cert must fail");

    // The full chain (`{:#}`) must name the target endpoint AND the transport
    // cause — not just "Failed to send POST request".
    let full = format!("{err:#}");
    assert!(
        full.contains("/api/v1/nodes"),
        "error must name the target URL for diagnosis: {full}"
    );
    let lower = full.to_lowercase();
    assert!(
        lower.contains("certificate") || lower.contains("tls") || lower.contains("handshake"),
        "error must surface the TLS/cert cause, not an opaque send failure: {full}"
    );
}
