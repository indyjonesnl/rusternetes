//! Unit tests moved from `src/tls.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rcgen::{CertificateParams, DistinguishedName, DnType, Ia5String, SanType};
    pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    pub use std::str::FromStr;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::tls::*;

    fn pem_pair(key: &rcgen::KeyPair) -> (String, String) {
        let cert = CertificateParams::new(vec!["example.com".to_string()])
            .unwrap()
            .self_signed(key)
            .unwrap();
        (cert.pem(), key.serialize_pem())
    }

    #[test]
    fn x509_key_pair_accepts_a_matching_pair() {
        let key = rcgen::KeyPair::generate().unwrap();
        let (cert, key) = pem_pair(&key);
        assert_eq!(x509_key_pair(cert.as_bytes(), key.as_bytes()), Ok(()));
    }

    #[test]
    fn x509_key_pair_rejects_another_key() {
        let (cert, _) = pem_pair(&rcgen::KeyPair::generate().unwrap());
        let (_, other) = pem_pair(&rcgen::KeyPair::generate().unwrap());
        assert_eq!(
            x509_key_pair(cert.as_bytes(), other.as_bytes()),
            Err("tls: private key does not match public key".to_string())
        );
    }

    #[test]
    fn x509_key_pair_rejects_another_key_type() {
        let (cert, _) = pem_pair(&rcgen::KeyPair::generate().unwrap());
        let ed = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        assert_eq!(
            x509_key_pair(cert.as_bytes(), ed.serialize_pem().as_bytes()),
            Err("tls: private key type does not match public key type".to_string())
        );
    }

    /// The PEM-scan errors of `X509KeyPair`, in Go's wording.
    #[test]
    fn x509_key_pair_pem_errors() {
        let (cert, key) = pem_pair(&rcgen::KeyPair::generate().unwrap());
        assert_eq!(
            x509_key_pair(b"", key.as_bytes()),
            Err("tls: failed to find any PEM data in certificate input".to_string())
        );
        assert_eq!(
            x509_key_pair(key.as_bytes(), cert.as_bytes()),
            Err("tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched".to_string())
        );
        assert_eq!(
            x509_key_pair(cert.as_bytes(), b"junk"),
            Err("tls: failed to find any PEM data in key input".to_string())
        );
        assert_eq!(
            x509_key_pair(cert.as_bytes(), cert.as_bytes()),
            Err(
                "tls: found a certificate rather than a key in the PEM for the private key"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_generate_self_signed_cert() {
        let tls_config = TlsConfig::generate_self_signed(
            "localhost",
            vec!["localhost".to_string(), "127.0.0.1".to_string()],
        )
        .expect("Failed to generate self-signed certificate");

        assert!(!tls_config.cert.is_empty());

        // Should be able to create server config
        let _server_config = tls_config
            .into_server_config()
            .expect("Failed to create server config");
    }

    #[test]
    fn test_cert_with_multiple_sans() {
        let tls_config = TlsConfig::generate_self_signed(
            "rusternetes-api",
            vec![
                "localhost".to_string(),
                "api.rusternetes.local".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
            ],
        )
        .expect("Failed to generate certificate");

        assert!(!tls_config.cert.is_empty());
    }
}
