//! TLS connector for proxying to the kubelet.
//!
//! The kubelet serves its `:10250` API (logs/exec/attach/metrics) over HTTPS
//! with a self-signed serving certificate (#1644). The api-server proxies to
//! `https://<nodeIP>:10250`, so this crate's reverse-proxy client must speak
//! TLS — and must NOT verify the kubelet's self-signed cert (upstream's default
//! posture; no `--kubelet-certificate-authority`). The connector still handles
//! plain `http://` targets (`.https_or_http()`), so existing callers are
//! unaffected.

use std::sync::Arc;

use hyper_util::client::legacy::connect::HttpConnector;

/// A hyper connector that dials both `http://` and `https://`, skipping
/// verification of the (self-signed) kubelet serving certificate.
pub type KubeletProxyConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// Build the shared kubelet-proxy connector.
pub fn kubelet_proxy_connector() -> KubeletProxyConnector {
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("aws-lc-rs supports the default TLS protocol versions")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(NoVerify))
    .with_no_client_auth();

    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .build()
}

/// Accept any server certificate. The kubelet presents a self-signed serving
/// cert; the api-server→kubelet trust model does not verify it (matches the
/// reqwest `danger_accept_invalid_certs` log client and upstream defaults).
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for #1644: the kubelet-proxy connector must build (TLS wiring
    // intact) so the api-server can proxy logs/exec/attach to the HTTPS kubelet.
    #[test]
    fn connector_builds() {
        let _ = kubelet_proxy_connector();
    }
}
