//! Unit tests moved from `src/controllers/cert_authority.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use chrono::Datelike;
    pub use rcgen::{
        Certificate, CertificateParams, CertificateSigningRequestParams, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose,
    };
    pub use rusternetes_common::resources::KeyUsage;
}

mod tests {
    use super::__parent::*;
    use rcgen::{BasicConstraints, CertificateParams, DnType, KeyPair};
    use rusternetes_controller_manager::controllers::cert_authority::*;
    use x509_parser::prelude::*;
    // Disambiguate from `x509_parser::prelude::KeyUsage` (a different type that
    // the glob above also brings into scope).
    use rusternetes_common::resources::KeyUsage;

    /// Generate a self-signed CA, returning `(cert_pem, key_pem)`.
    fn make_ca() -> (String, String) {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "rusternetes-test-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// Generate a PKCS#10 CSR PEM for the given subject common name.
    fn make_csr(common_name: &str) -> String {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let key = KeyPair::generate().unwrap();
        params.serialize_request(&key).unwrap().pem().unwrap()
    }

    fn parse<'a>(pem: &'a ::pem::Pem) -> X509Certificate<'a> {
        X509Certificate::from_der(pem.contents()).unwrap().1
    }

    #[test]
    fn sign_issues_cert_that_chains_to_the_ca() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let csr = make_csr("system:node:node-1");

        let issued = ca
            .sign(
                &csr,
                &[KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                None,
            )
            .expect("signing an approved CSR must succeed");

        let issued_pem = ::pem::parse(&issued).expect("issued cert must be valid PEM");
        let ca_pem = ::pem::parse(&ca_cert_pem).unwrap();
        let leaf = parse(&issued_pem);
        let ca_x = parse(&ca_pem);

        // The leaf carries the requested subject.
        assert!(
            leaf.subject().to_string().contains("system:node:node-1"),
            "leaf subject must come from the CSR; got {}",
            leaf.subject()
        );
        // The leaf's issuer is the CA's subject — and its signature verifies
        // against the CA public key, i.e. it chains to the on-disk CA.
        assert_eq!(
            leaf.issuer().to_string(),
            ca_x.subject().to_string(),
            "leaf issuer DN must equal CA subject DN"
        );
        leaf.verify_signature(Some(ca_x.public_key()))
            .expect("issued leaf must be signed by the CA key");
    }

    #[test]
    fn sign_applies_usages_from_spec_not_csr() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let csr = make_csr("serving.example.com");

        let issued = ca
            .sign(
                &csr,
                &[
                    KeyUsage::DigitalSignature,
                    KeyUsage::KeyEncipherment,
                    KeyUsage::ServerAuth,
                ],
                Some(3600),
            )
            .unwrap();

        let issued_pem = ::pem::parse(&issued).unwrap();
        let leaf = parse(&issued_pem);

        let eku = leaf
            .extended_key_usage()
            .unwrap()
            .expect("issued cert must carry extended key usage")
            .value;
        assert!(
            eku.server_auth,
            "server auth EKU must be set from spec.usages"
        );
        assert!(
            !eku.client_auth,
            "client auth must NOT be set (not requested)"
        );

        let ku = leaf
            .key_usage()
            .unwrap()
            .expect("issued cert must carry key usage")
            .value;
        assert!(ku.digital_signature(), "digital signature KU must be set");
        assert!(ku.key_encipherment(), "key encipherment KU must be set");
    }

    #[test]
    fn sign_rejects_a_malformed_request() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let err = ca.sign(
            "-----BEGIN CERTIFICATE REQUEST-----\nnot-a-csr\n-----END CERTIFICATE REQUEST-----\n",
            &[KeyUsage::ClientAuth],
            None,
        );
        assert!(err.is_err(), "a malformed CSR must be rejected, not signed");
    }
}
