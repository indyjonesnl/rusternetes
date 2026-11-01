//! Unit tests moved from `src/secure_serving/serve.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::Extension;
    pub use axum::Router;
    pub use axum_server::accept::Accept;
    pub use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
    pub use rusternetes_middleware::secure_serving::authn::CaContent;
    pub use rustls::crypto::aws_lc_rs::cipher_suite as cs;
    pub use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
    pub use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
    pub use rustls::{
        DigitallySignedStruct, DistinguishedName, SignatureScheme, SupportedCipherSuite,
    };
    pub use std::collections::BTreeSet;
    pub use std::future::Future;
    pub use std::hash::{Hash, Hasher};
    pub use std::io;
    pub use std::pin::Pin;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::io::{AsyncRead, AsyncWrite};
    pub use tower::Layer;
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::secure_serving::serve::*;

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
            vec![
                "VersionTLS10",
                "VersionTLS11",
                "VersionTLS12",
                "VersionTLS13"
            ]
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

    /// secure_serving.go:177-201: 100 streams unless the flag sets more, 256 KiB
    /// per stream and frame, and a connection window of stream window times
    /// stream count.
    #[test]
    fn http2_settings_follow_serve() {
        let k = 256 * 1024;
        assert_eq!(http2_settings(0), (100, k, k * 100));
        assert_eq!(http2_settings(-5), (100, k, k * 100));
        assert_eq!(http2_settings(250), (250, k, k * 250));
        // The connection window is an int32 upstream; never above its range.
        assert_eq!(http2_settings(i32::MAX).2, i32::MAX as u32);
    }
}
