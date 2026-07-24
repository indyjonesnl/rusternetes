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

/// Standard kubeadm location of the api-server's kubelet client credential.
/// Upstream's `--kubelet-client-certificate` / `--kubelet-client-key`. The
/// kubelet authenticates incoming api-server requests (logs/exec/attach) via
/// this x509 client cert; without it the request is anonymous and a vanilla
/// kubelet (`--anonymous-auth=false`) rejects it with 401 (#1670).
const KUBELET_CLIENT_CERT: &str = "/etc/kubernetes/pki/apiserver-kubelet-client.crt";
const KUBELET_CLIENT_KEY: &str = "/etc/kubernetes/pki/apiserver-kubelet-client.key";

/// Load the api-server's kubelet client identity (cert chain + key) from the
/// standard kubeadm path, if present. Returns `None` when the files are absent
/// (rusternetes' own cluster, whose kubelet accepts the api-server without a
/// client cert) so the connector falls back to `.with_no_client_auth()`.
fn load_kubelet_client_identity() -> Option<(
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let cert_pem = std::fs::read(KUBELET_CLIENT_CERT).ok()?;
    let key_pem = std::fs::read(KUBELET_CLIENT_KEY).ok()?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        tracing::warn!("kubelet client cert {KUBELET_CLIENT_CERT} contained no certificates");
        return None;
    }
    let key = match rustls_pemfile::private_key(&mut key_pem.as_slice()) {
        Ok(Some(k)) => k,
        _ => {
            tracing::warn!("kubelet client key {KUBELET_CLIENT_KEY} contained no private key");
            return None;
        }
    };
    Some((certs, key))
}

/// Build the shared kubelet-proxy connector. Presents the kubeadm
/// `apiserver-kubelet-client` cert when it exists so a vanilla kubelet
/// authenticates the api-server; otherwise no client auth (rusternetes' own
/// kubelet does not require it).
pub fn kubelet_proxy_connector() -> KubeletProxyConnector {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("aws-lc-rs supports the default TLS protocol versions")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(NoVerify));

    let tls = match load_kubelet_client_identity() {
        Some((certs, key)) => {
            tracing::info!(
                "kubelet proxy: presenting client cert from {}",
                KUBELET_CLIENT_CERT
            );
            builder
                .with_client_auth_cert(certs, key)
                .expect("valid kubelet client cert/key")
        }
        None => builder.with_no_client_auth(),
    };

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
