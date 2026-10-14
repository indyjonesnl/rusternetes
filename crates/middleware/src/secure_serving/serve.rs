//! TLS serving of the secure port: `SecureServingInfo.Serve`.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/server/secure_serving.go:44-160`
//!   (`tlsConfig`, `SecureServingInfo.Serve`)
//! * `staging/src/k8s.io/apiserver/pkg/server/options/serving.go:268-338`
//!   (`ApplyTo`: cert/key files, cipher suites, min TLS version)
//! * `staging/src/k8s.io/component-base/cli/flag/ciphersuites_flag.go:30-147`
//!   (`TLSCipherSuites`, `TLSVersion`, `DefaultTLSVersion`)
//! * `staging/src/k8s.io/apiserver/pkg/server/config.go:~ (ApplyClientCert)` and
//!   `pkg/authentication/request/x509/x509.go` for the client-CA union.
//!
//! Deliberate reductions, each tracked in the issue: Go cipher suites that
//! rustls does not implement (CBC, RC4, 3DES, RSA key exchange) and
//! `VersionTLS10`/`VersionTLS11` are rejected with an error; SNI certificates,
//! `--http2-max-streams-per-connection`, `--permit-port-sharing` and
//! `--permit-address-sharing` are not offered.

use super::authn::CaContent;
use axum::Router;
use std::sync::Arc;

/// `tls.VersionTLS12` / `tls.VersionTLS13` (the versions rustls implements).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

/// `cliflag.TLSVersion` (ciphersuites_flag.go:131-139): empty means
/// [`TlsVersion::Tls12`] (`DefaultTLSVersion`).
pub fn tls_version(_name: &str) -> Result<TlsVersion, String> {
    unimplemented!("tls_version")
}

/// `cliflag.TLSPossibleVersions` (ciphersuites_flag.go:121-128).
pub fn tls_possible_versions() -> Vec<String> {
    unimplemented!("tls_possible_versions")
}

/// `cliflag.TLSCipherSuites` (ciphersuites_flag.go:98-112): the rustls suites for
/// the Go cipher names.
pub fn tls_cipher_suites(
    _names: &[String],
) -> Result<Vec<rustls::SupportedCipherSuite>, String> {
    unimplemented!("tls_cipher_suites")
}

/// `cliflag.PreferredTLSCipherNames` + `InsecureTLSCipherNames`, the lists the
/// `--tls-cipher-suites` help prints.
pub fn tls_cipher_names() -> (Vec<String>, Vec<String>) {
    unimplemented!("tls_cipher_names")
}

/// Where the serving certificate comes from (`c.Cert`): files that are
/// re-read when they change (`NewDynamicServingContentFromFiles`), or a fixed
/// in-memory pair (`NewStaticCertKeyContent`).
#[derive(Clone)]
pub enum ServingCert {
    Files { cert_file: String, key_file: String },
    Static { cert_pem: Vec<u8>, key_pem: Vec<u8> },
}

/// `server.SecureServingInfo`.
pub struct SecureServingInfo {
    pub listener: std::net::TcpListener,
    pub cert: Option<ServingCert>,
    pub cipher_suites: Vec<rustls::SupportedCipherSuite>,
    pub min_tls_version: TlsVersion,
    pub disable_http2: bool,
    /// The union of every client CA (`ClientCA`): the TLS layer requests and
    /// verifies a client certificate against any of them (`VerifyClientCertIfGiven`).
    pub client_cas: Vec<Arc<CaContent>>,
}

impl SecureServingInfo {
    /// The address the listener is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// `ApplyClientCert` (config.go): add a client CA to the union.
    pub fn apply_client_cert(&mut self, ca: Arc<CaContent>) {
        self.client_cas.push(ca);
    }
}

/// `tlsConfig` (secure_serving.go:44-100): the rustls server config.
pub fn build_server_config(_info: &SecureServingInfo) -> Result<Arc<rustls::ServerConfig>, String> {
    unimplemented!("build_server_config")
}

/// Handle on a running secure server (`stoppedCh` / `listenerStoppedCh`).
pub struct ServerHandle {
    pub handle: axum_server::Handle,
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// `SecureServingInfo.Serve` (secure_serving.go:~ 150): serve `handler` over
/// TLS until `shutdown_timeout` after the handle is shut down. Every request
/// carries its verified client certificate chain as [`crate::PeerCertificates`].
pub fn serve(_handler: Router, _info: SecureServingInfo) -> Result<ServerHandle, String> {
    unimplemented!("serve")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ciphersuites_flag.go:131-147
    #[test]
    fn tls_version_names() {
        assert_eq!(tls_version(""), Ok(TlsVersion::Tls12));
        assert_eq!(tls_version("VersionTLS12"), Ok(TlsVersion::Tls12));
        assert_eq!(tls_version("VersionTLS13"), Ok(TlsVersion::Tls13));
        assert_eq!(
            tls_version("VersionTLS99").unwrap_err(),
            "unknown tls version \"VersionTLS99\""
        );
        // real Go names rustls cannot serve
        assert!(tls_version("VersionTLS10").is_err());
        assert!(tls_version("VersionTLS11").is_err());
    }

    #[test]
    fn possible_versions_are_sorted_go_names() {
        assert_eq!(
            tls_possible_versions(),
            vec!["VersionTLS10", "VersionTLS11", "VersionTLS12", "VersionTLS13"]
        );
    }

    // ciphersuites_flag.go:98-112
    #[test]
    fn cipher_suites_by_go_name() {
        assert!(tls_cipher_suites(&[]).unwrap().is_empty());
        let got = tls_cipher_suites(&[
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string(),
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256".to_string(),
        ])
        .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(
            tls_cipher_suites(&["TLS_NOPE".to_string()]).unwrap_err(),
            "Cipher suite TLS_NOPE not supported or doesn't exist"
        );
    }

    #[test]
    fn cipher_names_lists_are_non_empty_and_disjoint() {
        let (preferred, insecure) = tls_cipher_names();
        assert!(preferred.contains(&"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()));
        assert!(insecure.contains(&"TLS_RSA_WITH_RC4_128_SHA".to_string()));
        assert!(preferred.iter().all(|p| !insecure.contains(p)));
    }
}
