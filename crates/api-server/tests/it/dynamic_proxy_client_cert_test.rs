//! `--proxy-client-cert-file` / `--proxy-client-key-file` and the
//! `aggregator-reload-proxy-client-cert` post-start hook.
//!
//! Ported from kube-aggregator `pkg/apiserver/apiserver.go:297-313`
//! (`NewDynamicServingContentFromFiles` + `RunOnce` + the hook that runs
//! `DynamicCertKeyPairContent.Run`) and apiserver's
//! `dynamiccertificates/dynamic_serving_content.go`, whose tests
//! (`dynamic_serving_content_test.go`, `dynamic_cafile_content_test.go`) pin
//! reload-on-change, listener notification, and that a half-rotated
//! (mismatched) pair is never applied. The last test is the consumer half: a
//! real TLS handshake from the aggregator's proxy request path
//! (`availability_discovery_request`), which must present the CURRENT pair.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusternetes_api_server::dynamic_certificates::{DynamicCertKeyPairContent, Listener};
use rusternetes_api_server::handlers::aggregator::{
    availability_discovery_request, current_proxy_client_identity_pem, install_proxy_client_certs,
    proxy_current_cert_key_content, AggregatorTarget,
};
use rusternetes_api_server::post_start_hooks::{
    register_aggregator_reload_proxy_client_cert_hook, PostStartHooks,
    AGGREGATOR_RELOAD_PROXY_CLIENT_CERT_HOOK,
};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Pair {
    cert: String,
    key: String,
}

fn pair(cn: &str) -> Pair {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Pair {
        cert: cert.pem(),
        key: key.serialize_pem(),
    }
}

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("rn-proxycert-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write_pair(dir: &Path, p: &Pair) -> (String, String) {
    let (c, k) = (dir.join("proxy.crt"), dir.join("proxy.key"));
    std::fs::write(&c, &p.cert).unwrap();
    std::fs::write(&k, &p.key).unwrap();
    (c.to_str().unwrap().into(), k.to_str().unwrap().into())
}

/// Replace a file the way a secret/volume rotation does: write a sibling and
/// rename it over the path (the old inode goes away).
fn rename_over(path: &str, content: &str) {
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

#[derive(Default)]
struct Counter(AtomicUsize);
impl Listener for Counter {
    fn enqueue(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn missing_filename_is_an_error() {
    // NewDynamicServingContentFromFiles: len(certFile) == 0 || len(keyFile) == 0.
    assert!(DynamicCertKeyPairContent::new("x", "", "k").is_err());
    assert!(DynamicCertKeyPairContent::new("x", "c", "").is_err());
}

#[test]
fn name_is_purpose_cert_and_key() {
    let d = dir("name");
    let (c, k) = write_pair(&d, &pair("a"));
    let p = DynamicCertKeyPairContent::new("aggregator-proxy-cert", &c, &k).unwrap();
    assert_eq!(p.name(), format!("aggregator-proxy-cert::{c}::{k}"));
}

#[test]
fn unusable_files_fail_to_load() {
    let d = dir("bad");
    let a = pair("a");
    let (c, k) = write_pair(&d, &a);

    // loadCertKeyPair: "missing content for serving cert".
    std::fs::write(&c, "").unwrap();
    let err = DynamicCertKeyPairContent::new("x", &c, &k).err().unwrap();
    assert!(
        format!("{err:#}").contains("missing content for serving cert"),
        "{err:#}"
    );
    // tls.X509KeyPair: not PEM / key of another cert.
    std::fs::write(&c, "garbage").unwrap();
    assert!(DynamicCertKeyPairContent::new("x", &c, &k).is_err());
    std::fs::write(&c, &a.cert).unwrap();
    std::fs::write(&k, pair("b").key).unwrap();
    assert!(DynamicCertKeyPairContent::new("x", &c, &k).is_err());
    // missing file.
    assert!(DynamicCertKeyPairContent::new("x", "/nonexistent/c", "/nonexistent/k").is_err());
}

#[test]
fn run_once_failure_is_reported_and_keeps_the_last_good_pair() {
    // apiserver.go:303: a RunOnce error aborts startup; a later failure keeps
    // serving the previous content.
    let d = dir("keep");
    let a = pair("a");
    let (c, k) = write_pair(&d, &a);
    let p = DynamicCertKeyPairContent::new("x", &c, &k).unwrap();
    std::fs::write(&k, "").unwrap();
    assert!(p.run_once().is_err());
    std::fs::write(&k, "garbage").unwrap();
    assert!(p.run_once().is_err());
    let (cert, key) = p.current_cert_key_content();
    assert_eq!((cert, key), (a.cert.into_bytes(), a.key.into_bytes()));
}

#[test]
fn reload_notifies_listeners_only_when_content_changes() {
    let d = dir("listen");
    let (c, k) = write_pair(&d, &pair("a"));
    let p = DynamicCertKeyPairContent::new("x", &c, &k).unwrap();
    let l = Arc::new(Counter::default());
    p.add_listener(l.clone());

    p.run_once().unwrap();
    assert_eq!(l.0.load(Ordering::SeqCst), 0, "unchanged content");

    let b = pair("b");
    std::fs::write(&c, &b.cert).unwrap();
    std::fs::write(&k, &b.key).unwrap();
    p.run_once().unwrap();
    assert_eq!(l.0.load(Ordering::SeqCst), 1);
    assert_eq!(p.current_cert_key_content().0, b.cert.as_bytes());
    p.run_once().unwrap();
    assert_eq!(l.0.load(Ordering::SeqCst), 1);
}

#[test]
fn half_rotated_pair_is_not_applied() {
    // handleWatchEvent's doc: "If one file is updated before the other, the
    // loadCertKeyPair method will catch the mismatch and will not apply the
    // change."
    let d = dir("half");
    let a = pair("a");
    let (c, k) = write_pair(&d, &a);
    let p = DynamicCertKeyPairContent::new("x", &c, &k).unwrap();
    let b = pair("b");
    std::fs::write(&c, &b.cert).unwrap();
    assert!(p.run_once().is_err());
    assert_eq!(p.current_cert_key_content().0, a.cert.as_bytes());
    std::fs::write(&k, &b.key).unwrap();
    p.run_once().unwrap();
    assert_eq!(p.current_cert_key_content().0, b.cert.as_bytes());
}

#[tokio::test]
async fn run_reloads_on_in_place_write_and_on_rename_replacement() {
    let d = dir("watch");
    let (c, k) = write_pair(&d, &pair("a"));
    let p = DynamicCertKeyPairContent::new("x", &c, &k).unwrap();
    let l = Arc::new(Counter::default());
    p.add_listener(l.clone());
    let tasks = p.clone().run();

    // In-place write of the pair (cert first: the mismatch is retried until
    // the key event completes the pair).
    let b = pair("b");
    std::fs::write(&c, &b.cert).unwrap();
    std::fs::write(&k, &b.key).unwrap();
    eventually("in-place rotation", || {
        p.current_cert_key_content().0 == b.cert.as_bytes()
    })
    .await;

    // Rename-over replacement drops the watched inode; the watch must be
    // re-added (handleWatchEvent) so a SECOND rotation is still seen.
    for cn in ["c", "d"] {
        let next = pair(cn);
        rename_over(&c, &next.cert);
        rename_over(&k, &next.key);
        eventually("rename rotation", || {
            p.current_cert_key_content()
                == (
                    next.cert.clone().into_bytes(),
                    next.key.clone().into_bytes(),
                )
        })
        .await;
    }
    assert!(l.0.load(Ordering::SeqCst) >= 3);
    for t in tasks {
        t.abort();
    }
}

#[tokio::test]
async fn hook_is_registered_under_the_upstream_name_and_finishes() {
    let d = dir("hook");
    let b = pair("b");
    let (c, k) = write_pair(&d, &pair("a"));
    let p = DynamicCertKeyPairContent::new("x", &c, &k).unwrap();
    let registry = PostStartHooks::new();
    register_aggregator_reload_proxy_client_cert_hook(&registry, p.clone());

    assert_eq!(
        AGGREGATOR_RELOAD_PROXY_CLIENT_CERT_HOOK,
        "aggregator-reload-proxy-client-cert"
    );
    eventually("hook done", || {
        registry.check(AGGREGATOR_RELOAD_PROXY_CLIENT_CERT_HOOK) == Some(Ok(()))
    })
    .await;
    assert!(registry
        .checks()
        .iter()
        .any(|(n, _)| n == "poststarthook/aggregator-reload-proxy-client-cert"));

    // The hook ran `Run`: a file change is picked up with no RunOnce.
    rename_over(&c, &b.cert);
    rename_over(&k, &b.key);
    eventually("reload via the hook's Run", || {
        p.current_cert_key_content().0 == b.cert.as_bytes()
    })
    .await;
}

/// Accepts any client certificate (so a self-signed proxy cert completes the
/// handshake) and lets the test read the presented chain.
#[derive(Debug)]
struct AcceptAnyClientCert(Arc<rustls::crypto::CryptoProvider>);

impl ClientCertVerifier for AcceptAnyClientCert {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// An aggregated-API stand-in: records the DER of the client certificate each
/// accepted connection presented.
async fn backend(presented: Arc<Mutex<Vec<Vec<u8>>>>) -> u16 {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let srv_key = rcgen::KeyPair::generate().unwrap();
    let srv = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&srv_key)
        .unwrap();
    let config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(Arc::new(AcceptAnyClientCert(provider)))
        .with_single_cert(
            vec![srv.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(srv_key.serialize_der())),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let presented = presented.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                if let Some(chain) = tls.get_ref().1.peer_certificates() {
                    if let Some(leaf) = chain.first() {
                        presented.lock().unwrap().push(leaf.as_ref().to_vec());
                    }
                }
                let mut buf = vec![0u8; 4096];
                let _ = tls.read(&mut buf).await;
                let _ = tls
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    port
}

fn der_of(pem: &str) -> Vec<u8> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .next()
        .unwrap()
        .unwrap()
        .as_ref()
        .to_vec()
}

/// The process-global provider can be installed once per test binary, so the
/// whole consumer story is one test.
#[tokio::test]
async fn rotated_cert_is_what_the_next_proxied_connection_presents() {
    // Unconfigured: `proxyCurrentCertKeyContent` defaults to `nil, nil`.
    assert!(!install_proxy_client_certs(None, None).unwrap());
    assert!(!install_proxy_client_certs(Some("/c"), None).unwrap());
    assert!(!install_proxy_client_certs(Some(""), Some("")).unwrap());
    assert_eq!(proxy_current_cert_key_content(), (vec![], vec![]));
    assert!(current_proxy_client_identity_pem().is_none());

    // A bad pair is a startup error (apiserver.go:299-307).
    let d = dir("consumer");
    let a = pair("proxy-a");
    let (c, k) = write_pair(&d, &a);
    std::fs::write(&k, "garbage").unwrap();
    assert!(install_proxy_client_certs(Some(&c), Some(&k)).is_err());
    std::fs::write(&k, &a.key).unwrap();

    assert!(install_proxy_client_certs(Some(&c), Some(&k)).unwrap());
    let certs = rusternetes_api_server::handlers::aggregator::proxy_client_certs().unwrap();
    // The aggregator-reload-proxy-client-cert hook starts the reloader.
    let registry = PostStartHooks::new();
    register_aggregator_reload_proxy_client_cert_hook(&registry, certs.clone());

    let presented = Arc::new(Mutex::new(Vec::new()));
    let port = backend(presented.clone()).await;
    let target = AggregatorTarget {
        host: "127.0.0.1".into(),
        port,
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "https",
        server_name: None,
    };

    availability_discovery_request(&target, "v1.example.io", "example.io", "v1")
        .await
        .unwrap();
    assert_eq!(presented.lock().unwrap().last().unwrap(), &der_of(&a.cert));

    // Rotate both files; no restart and no explicit reload.
    let b = pair("proxy-b");
    rename_over(&c, &b.cert);
    rename_over(&k, &b.key);
    eventually("rotated pair loaded", || {
        proxy_current_cert_key_content().0 == b.cert.as_bytes()
    })
    .await;

    availability_discovery_request(&target, "v1.example.io", "example.io", "v1")
        .await
        .unwrap();
    let last = presented.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        last,
        der_of(&b.cert),
        "the next connection presents the rotated cert"
    );
    assert_ne!(last, der_of(&a.cert));
}
