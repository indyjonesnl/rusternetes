//! TLS for the kubelet API port (`:10250` — `/containerLogs`, `/exec`,
//! `/attach`, `/metrics`, `/configz`).
//!
//! Upstream kubelets serve this endpoint over HTTPS, and a Kubernetes
//! api-server ALWAYS dials `https://<nodeIP>:10250` when proxying
//! logs/exec/attach/metrics — it never speaks plain HTTP to a kubelet. Serving
//! plain HTTP here makes every such proxied request fail with
//! `http: server gave HTTP response to HTTPS client`, which cascades across
//! NodeConformance (a vanilla api-server proxying `kubectl logs` to a
//! Rusternetes kubelet) — see #1644.
//!
//! We present a freshly generated self-signed serving certificate. The
//! api-server does not verify the kubelet serving cert unless
//! `--kubelet-certificate-authority` is set (kind does not set it), so a
//! self-signed cert is accepted — this matches upstream's default
//! kubelet-serving posture.

use anyhow::{anyhow, Result};
use std::sync::Arc;

/// Build an `axum-server` rustls config with a self-signed serving certificate
/// for the kubelet API port.
///
/// HTTP/1.1 only (no h2 ALPN): the api-server's kubelet transport uses
/// HTTP/1.1, and `exec`/`attach` depend on HTTP/1.1 `Upgrade`
/// (SPDY / WebSocket) which HTTP/2 would break.
pub fn kubelet_serving_tls(node_name: &str) -> Result<axum_server::tls_rustls::RustlsConfig> {
    // SANs are cosmetic while the api-server skips kubelet-cert verification,
    // but populate the obvious ones anyway (node name + loopback).
    let sans = vec![
        node_name.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];
    // generate_self_signed() installs the process-wide rustls crypto provider,
    // so it must run before ServerConfig::builder() below.
    let tls = rusternetes_common::tls::TlsConfig::generate_self_signed(node_name, sans)?;

    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(tls.cert, tls.key)
        .map_err(|e| anyhow!("kubelet serving TLS config: {e}"))?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(cfg),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
