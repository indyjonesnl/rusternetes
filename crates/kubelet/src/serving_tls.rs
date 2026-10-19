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
use rusternetes_middleware::PeerCertificates;
use std::sync::Arc;

/// Build an `axum-server` rustls config with a self-signed serving certificate
/// for the kubelet API port.
///
/// HTTP/1.1 only (no h2 ALPN): the api-server's kubelet transport uses
/// HTTP/1.1, and `exec`/`attach` depend on HTTP/1.1 `Upgrade`
/// (SPDY / WebSocket) which HTTP/2 would break.
#[allow(dead_code)] // the kubelet binary serves via `..._with_client_ca`; the all-in-one lib uses this
pub fn kubelet_serving_tls(node_name: &str) -> Result<axum_server::tls_rustls::RustlsConfig> {
    kubelet_serving_tls_with_client_ca(node_name, None)
}

/// [`kubelet_serving_tls`] that, when `client_ca_file` is set, requests (but
/// does not require or verify) a client certificate:
/// `tlsOptions.Config.ClientCAs = clientCAs; ClientAuth = tls.RequestClientCert`
/// (`cmd/kubelet/app/server.go:1170-1180`, "Populate PeerCertificates in
/// requests, but don't reject connections without verified certificates").
/// The x509 authenticator verifies the chain per request
/// ([`crate::server_auth::X509Authenticator`]).
pub fn kubelet_serving_tls_with_client_ca(
    node_name: &str,
    client_ca_file: Option<&str>,
) -> Result<axum_server::tls_rustls::RustlsConfig> {
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

    let builder = rustls::ServerConfig::builder();
    let builder = match client_ca_file {
        None => builder.with_no_client_auth(),
        Some(ca) => {
            // `certutil.NewPool(kc.Authentication.X509.ClientCAFile)`
            let inner = rusternetes_common::tls::client_cert_verifier_for_ca_file(ca)
                .map_err(|e| anyhow!("unable to load client CA file {ca}: {e}"))?;
            builder.with_client_cert_verifier(Arc::new(RequestClientCert { inner }))
        }
    };
    let mut cfg = builder
        .with_single_cert(tls.cert, tls.key)
        .map_err(|e| anyhow!("kubelet serving TLS config: {e}"))?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(cfg),
    ))
}

/// `tls.RequestClientCert`: advertise the client CAs and ask for a certificate,
/// but accept the handshake whatever the client presents. The chain is verified
/// by the x509 authenticator, which answers an invalid one with a 401 instead
/// of a failed handshake.
#[derive(Debug)]
struct RequestClientCert {
    inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
}

impl rustls::server::danger::ClientCertVerifier for RequestClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// An `axum-server` acceptor that surfaces the connection's peer certificate
/// chain to handlers as a [`PeerCertificates`] request extension
/// (`req.TLS.PeerCertificates`). The chain is unverified here; see
/// [`RequestClientCert`].
#[derive(Clone)]
pub struct PeerCertAcceptor {
    inner: axum_server::tls_rustls::RustlsAcceptor,
}

impl PeerCertAcceptor {
    pub fn new(config: axum_server::tls_rustls::RustlsConfig) -> Self {
        Self {
            inner: axum_server::tls_rustls::RustlsAcceptor::new(config),
        }
    }
}

impl<I, S> axum_server::accept::Accept<I, S> for PeerCertAcceptor
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Stream =
        <axum_server::tls_rustls::RustlsAcceptor as axum_server::accept::Accept<I, S>>::Stream;
    type Service = <axum::Extension<PeerCertificates> as tower::Layer<S>>::Service;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<(Self::Stream, Self::Service)>> + Send,
        >,
    >;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let (tls_stream, service) = inner.accept(stream, service).await?;
            let chain: Vec<Vec<u8>> = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .map(|certs| certs.iter().map(|c| c.as_ref().to_vec()).collect())
                .unwrap_or_default();
            let service =
                tower::Layer::layer(&axum::Extension(PeerCertificates(Arc::new(chain))), service);
            Ok((tls_stream, service))
        })
    }
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
