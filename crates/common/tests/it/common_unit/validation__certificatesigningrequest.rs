//! Unit tests moved from `src/validation/certificatesigningrequest.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use base64::Engine;
    pub use rusternetes_common::resources::certificates::{
        CertificateSigningRequest, CertificateSigningRequestCondition, KeyUsage,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
    pub use x509_parser::prelude::FromDer;
}

mod status_certificate_tests {
    use base64::Engine;
    use rusternetes_common::validation::certificatesigningrequest::status_certificate_error;

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    /// A real self-signed cert PEM via rcgen.
    fn valid_cert_pem() -> String {
        rcgen::generate_simple_self_signed(vec!["test.example.com".to_string()])
            .expect("cert")
            .cert
            .pem()
    }

    #[test]
    fn unset_or_empty_passes() {
        assert_eq!(status_certificate_error(None), None);
        assert_eq!(status_certificate_error(Some("")), None);
    }

    #[test]
    fn valid_certificate_passes() {
        let data = b64(&valid_cert_pem());
        assert_eq!(status_certificate_error(Some(&data)), None);
    }

    #[test]
    fn two_concatenated_certs_pass() {
        let chain = format!("{}{}", valid_cert_pem(), valid_cert_pem());
        let data = b64(&chain);
        assert_eq!(status_certificate_error(Some(&data)), None);
    }

    #[test]
    fn garbage_base64_rejected() {
        assert!(status_certificate_error(Some("not base64!!!")).is_some());
    }

    #[test]
    fn non_pem_payload_rejected() {
        // valid base64, but no PEM blocks inside.
        let data = b64("definitely not a pem block");
        let err = status_certificate_error(Some(&data)).expect("err");
        assert!(err.contains("at least one CERTIFICATE PEM block"), "{err}");
    }

    #[test]
    fn wrong_block_type_rejected() {
        let data =
            b64("-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----\n");
        let err = status_certificate_error(Some(&data)).expect("err");
        assert!(
            err.contains("only CERTIFICATE PEM blocks are allowed"),
            "{err}"
        );
    }
}

mod request_signature_tests {
    use base64::Engine;
    use rusternetes_common::validation::certificatesigningrequest::parse_csr_request_error;

    /// Generate a real PKCS#10 CSR PEM with a valid self-signature via rcgen.
    fn valid_csr_pem() -> String {
        let key_pair = rcgen::KeyPair::generate().expect("keypair");
        let params =
            rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).expect("params");
        params
            .serialize_request(&key_pair)
            .expect("csr")
            .pem()
            .expect("pem")
    }

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    #[test]
    fn valid_csr_passes() {
        let request = b64(&valid_csr_pem());
        assert_eq!(parse_csr_request_error(&request), None);
    }

    #[test]
    fn empty_request_rejected() {
        assert!(parse_csr_request_error("").is_some());
    }

    #[test]
    fn garbage_base64_rejected() {
        let err = parse_csr_request_error("not base64!!!").expect("err");
        assert!(err.contains("error parsing request"), "{err}");
    }

    #[test]
    fn non_csr_pem_rejected() {
        let request = b64("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n");
        let err = parse_csr_request_error(&request).expect("err");
        assert!(err.contains("CERTIFICATE REQUEST"), "{err}");
    }

    #[test]
    fn tampered_signature_rejected() {
        // Decode a valid CSR to DER, flip the final signature byte (keeps the
        // ASN.1 lengths intact so the structure still parses), re-PEM it.
        let pem_str = valid_csr_pem();
        let block = pem::parse(pem_str.as_bytes()).expect("pem");
        let mut der = block.contents().to_vec();
        let last = der.len() - 1;
        der[last] ^= 0xff;
        let tampered_pem = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", der));
        let request = b64(&tampered_pem);

        let err = parse_csr_request_error(&request).expect("tampered must fail");
        assert!(
            err.contains("signature verification failed") || err.contains("error parsing request"),
            "{err}"
        );
    }
}

mod update_and_wiring_tests {
    use super::__parent::*;
    use base64::Engine;
    use rusternetes_common::resources::certificates::{
        CertificateSigningRequestCondition, CertificateSigningRequestSpec,
        CertificateSigningRequestStatus,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_common::validation::certificatesigningrequest::*;

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    fn valid_csr_request_b64() -> String {
        let key_pair = rcgen::KeyPair::generate().expect("keypair");
        let params =
            rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).expect("params");
        let pem = params
            .serialize_request(&key_pair)
            .expect("csr")
            .pem()
            .expect("pem");
        b64(&pem)
    }

    fn valid_cert_b64() -> String {
        let pem = rcgen::generate_simple_self_signed(vec!["test.example.com".to_string()])
            .expect("cert")
            .cert
            .pem();
        b64(&pem)
    }

    fn cond(type_: &str, status: &str) -> CertificateSigningRequestCondition {
        CertificateSigningRequestCondition {
            type_: type_.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_update_time: None,
            last_transition_time: None,
        }
    }

    fn base_csr() -> CertificateSigningRequest {
        CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::default(),
            spec: CertificateSigningRequestSpec {
                request: valid_csr_request_b64(),
                signer_name: "example.com/my-signer".to_string(),
                usages: vec![KeyUsage::ClientAuth],
                ..Default::default()
            },
            status: None,
        }
    }

    fn with_conditions(
        mut csr: CertificateSigningRequest,
        conditions: Vec<CertificateSigningRequestCondition>,
    ) -> CertificateSigningRequest {
        csr.status = Some(CertificateSigningRequestStatus {
            conditions: Some(conditions),
            certificate: None,
        });
        csr
    }

    fn with_cert(
        mut csr: CertificateSigningRequest,
        cert: Option<String>,
    ) -> CertificateSigningRequest {
        let conditions = csr.status.as_ref().and_then(|s| s.conditions.clone());
        csr.status = Some(CertificateSigningRequestStatus {
            conditions,
            certificate: cert,
        });
        csr
    }

    // ---- create-path wiring ----

    #[test]
    fn create_rejects_invalid_condition_status() {
        // Approved must be True; "False" is rejected via the now-wired validateConditions.
        let csr = with_conditions(base_csr(), vec![cond("Approved", "False")]);
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(!errs.is_empty(), "expected condition status rejection");
    }

    #[test]
    fn create_rejects_duplicate_condition_types() {
        let csr = with_conditions(
            base_csr(),
            vec![cond("Approved", "True"), cond("Approved", "True")],
        );
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(
            errs.iter().any(|e| format!("{e}").contains("Duplicate")
                || format!("{e:?}").to_lowercase().contains("duplicate")),
            "expected duplicate condition type rejection, got {errs:?}"
        );
    }

    #[test]
    fn create_rejects_arbitrary_certificate() {
        let csr = with_cert(base_csr(), Some(b64("not a pem cert")));
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(!errs.is_empty(), "expected certificate rejection on create");
    }

    #[test]
    fn create_accepts_valid_certificate_and_conditions() {
        let csr = with_cert(
            with_conditions(base_csr(), vec![cond("Approved", "True")]),
            Some(valid_cert_b64()),
        );
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(errs.is_empty(), "expected clean create, got {errs:?}");
    }

    // ---- update: removal of existing Approved/Denied/Failed ----

    #[test]
    fn update_forbids_removing_approved_condition() {
        let old = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let new = base_csr(); // no conditions
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not remove a condition")),
            "expected forbid-remove, got {errs:?}"
        );
    }

    #[test]
    fn update_forbids_removing_failed_condition() {
        let old = with_conditions(base_csr(), vec![cond("Failed", "True")]);
        let new = base_csr();
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not remove a condition")),
            "expected forbid-remove Failed, got {errs:?}"
        );
    }

    // ---- update: add/modify Approved/Denied only via /approval ----

    #[test]
    fn main_update_forbids_adding_approved() {
        let old = base_csr();
        let new = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not add a condition")),
            "expected forbid-add via main update, got {errs:?}"
        );
    }

    #[test]
    fn approval_update_allows_adding_approved() {
        let old = base_csr();
        let new = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let errs = validate_certificate_signing_request_approval_update(&new, &old);
        assert!(
            errs.is_empty(),
            "approval update should add Approved, got {errs:?}"
        );
    }

    #[test]
    fn main_update_forbids_modifying_denied() {
        let old = with_conditions(base_csr(), vec![cond("Denied", "True")]);
        let mut modified = cond("Denied", "True");
        modified.reason = Some("changed".to_string());
        let new = with_conditions(base_csr(), vec![modified]);
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not modify a condition")),
            "expected forbid-modify, got {errs:?}"
        );
    }

    // ---- update: status.certificate immutability ----

    #[test]
    fn main_update_forbids_setting_certificate() {
        let old = base_csr();
        let new = with_cert(base_csr(), Some(valid_cert_b64()));
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not set certificate content")),
            "expected forbid-set-cert on main update, got {errs:?}"
        );
    }

    #[test]
    fn status_update_allows_setting_certificate_once() {
        let old = base_csr();
        let new = with_cert(base_csr(), Some(valid_cert_b64()));
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.is_empty(),
            "status update should set cert, got {errs:?}"
        );
    }

    #[test]
    fn status_update_forbids_modifying_existing_certificate() {
        let old = with_cert(base_csr(), Some(valid_cert_b64()));
        let new = with_cert(base_csr(), Some(valid_cert_b64())); // different bytes (new keypair)
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not modify existing certificate")),
            "expected forbid-modify-existing-cert, got {errs:?}"
        );
    }

    #[test]
    fn update_tolerates_preexisting_invalid_certificate_when_unchanged() {
        // allowArbitraryCertificate: old has an invalid cert; new leaves it
        // unchanged -> the PEM check is skipped and no immutability error fires.
        let bad = b64("garbage cert");
        let old = with_cert(base_csr(), Some(bad.clone()));
        let new = with_cert(base_csr(), Some(bad));
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.is_empty(),
            "unchanged invalid cert should be tolerated, got {errs:?}"
        );
    }

    /// validation_test.go:555-559 "compatible update, existing duplicate usages".
    #[test]
    fn get_validation_options_existing_duplicate_usages() {
        let mut old = base_csr();
        old.spec.usages = vec![KeyUsage::Any, KeyUsage::Any];
        let opts = get_validation_options(&base_csr(), &old);
        assert!(opts.allow_duplicate_usages);
        let mut clean = base_csr();
        clean.spec.usages = vec![KeyUsage::Any];
        assert!(!get_validation_options(&base_csr(), &clean).allow_duplicate_usages);
    }

    #[test]
    fn update_tolerates_duplicate_usages_when_old_had_them() {
        let mut old = base_csr();
        old.spec.usages = vec![KeyUsage::ClientAuth, KeyUsage::ClientAuth];
        let new = old.clone();
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            !errs.iter().any(|e| format!("{e}").contains("Duplicate")),
            "old object had duplicates: {errs:?}"
        );
    }

    #[test]
    fn update_rejects_new_duplicate_usages() {
        let old = base_csr();
        let mut new = base_csr();
        new.spec.usages = vec![KeyUsage::ClientAuth, KeyUsage::ClientAuth];
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter().any(|e| format!("{e}").contains("Duplicate")),
            "{errs:?}"
        );
    }
}
