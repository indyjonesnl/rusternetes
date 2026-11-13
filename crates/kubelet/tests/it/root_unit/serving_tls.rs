//! Unit tests moved from `src/serving_tls.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use k3rs_middleware::PeerCertificates;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use k3rs_kubelet::serving_tls::*;

    // Regression for #1644: the kubelet API port must be servable over TLS.
    // Before the fix it bound plain HTTP (`axum::serve(TcpListener)`), so a
    // vanilla api-server's HTTPS proxy to `/containerLogs` failed and cascaded
    // across NodeConformance. Assert the serving config builds from a
    // self-signed cert (cert-gen + rustls wiring intact).
    #[tokio::test]
    async fn builds_a_tls_serving_config() {
        let cfg = kubelet_serving_tls("k3rs-node");
        assert!(
            cfg.is_ok(),
            "kubelet serving TLS config must build, got {:?}",
            cfg.err()
        );
    }
    // `--tls-cert-file`/`--tls-private-key-file`: a supplied pair is served
    // instead of a generated self-signed one (cmd/kubelet/app/server.go).
    #[tokio::test]
    async fn builds_a_tls_serving_config_from_files() {
        let ck = rcgen::generate_simple_self_signed(vec!["node-1".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("serving.crt");
        let key = dir.path().join("serving.key");
        std::fs::write(&cert, ck.cert.pem()).unwrap();
        std::fs::write(&key, ck.key_pair.serialize_pem()).unwrap();
        let cfg = kubelet_serving_tls_with_options(
            "node-1",
            None,
            Some((cert.to_str().unwrap(), key.to_str().unwrap())),
        );
        assert!(cfg.is_ok(), "got {:?}", cfg.err());
        let missing = kubelet_serving_tls_with_options(
            "node-1",
            None,
            Some(("/nonexistent.crt", "/nonexistent.key")),
        );
        assert!(missing.is_err());
    }
}
