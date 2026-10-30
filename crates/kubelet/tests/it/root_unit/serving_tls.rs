//! Unit tests moved from `src/serving_tls.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_middleware::PeerCertificates;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::serving_tls::*;

    // Regression for #1644: the kubelet API port must be servable over TLS.
    // Before the fix it bound plain HTTP (`axum::serve(TcpListener)`), so a
    // vanilla api-server's HTTPS proxy to `/containerLogs` failed and cascaded
    // across NodeConformance. Assert the serving config builds from a
    // self-signed cert (cert-gen + rustls wiring intact).
    #[tokio::test]
    async fn builds_a_tls_serving_config() {
        let cfg = kubelet_serving_tls("rusternetes-node");
        assert!(
            cfg.is_ok(),
            "kubelet serving TLS config must build, got {:?}",
            cfg.err()
        );
    }
}
