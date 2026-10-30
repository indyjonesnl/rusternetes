//! Unit tests moved from `src/controllers/certificate_signing_request.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use chrono::Utc;
    pub use rusternetes_common::resources::{
        CertificateSigningRequest, CertificateSigningRequestCondition,
        CertificateSigningRequestStatus, KeyUsage,
    };
    pub use rusternetes_controller_manager::controllers::cert_authority::CertificateAuthority;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{CertificateSigningRequest, CertificateSigningRequestSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::certificate_signing_request::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_validate_csr_spec_valid() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        // Create a CSR for testing (using rcgen for test data generation)
        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        // Generate CSR, not a certificate
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_ok());
    }

    /// Build a self-signed CA and return `(CertificateAuthority, base64-PEM CSR
    /// for `cn`)` ready to drop into a CSR `spec.request`.
    fn ca_and_request(
        cn: &str,
    ) -> (
        Arc<rusternetes_controller_manager::controllers::cert_authority::CertificateAuthority>,
        String,
    ) {
        use base64::{engine::general_purpose, Engine as _};

        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "rusternetes-ca");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca = Arc::new(
            rusternetes_controller_manager::controllers::cert_authority::CertificateAuthority::from_pem(
                &ca_cert.pem(),
                &ca_key.serialize_pem(),
            )
            .unwrap(),
        );

        let leaf_params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let req = leaf_params.serialize_request(&leaf_key).unwrap();
        let req_pem = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", req.der().to_vec()));
        let request_b64 = general_purpose::STANDARD.encode(req_pem);
        (ca, request_b64)
    }

    fn approved_csr(name: &str, request_b64: String) -> CertificateSigningRequest {
        CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new(name),
            spec: CertificateSigningRequestSpec {
                request: request_b64,
                signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
                usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                expiration_seconds: Some(86400),
                uid: None,
                groups: None,
                username: None,
                extra: None,
            },
            status: Some(CertificateSigningRequestStatus {
                conditions: Some(vec![CertificateSigningRequestCondition {
                    type_: "Approved".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_update_time: None,
                    last_transition_time: None,
                }]),
                certificate: None,
            }),
        }
    }

    #[tokio::test]
    async fn reconcile_issues_certificate_for_approved_csr_when_ca_present() {
        let (ca, request_b64) = ca_and_request("system:node:n1");
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage.clone())
            .with_certificate_authority(ca);

        let csr = approved_csr("issue-me", request_b64);
        let key = "/registry/certificatesigningrequests/issue-me";
        storage.create(key, &csr).await.unwrap();

        controller.reconcile_csr(&csr).await.unwrap();

        let updated: CertificateSigningRequest = storage.get(key).await.unwrap();
        let cert = updated
            .status
            .and_then(|s| s.certificate)
            .expect("status.certificate must be populated by the signer");
        assert!(
            cert.contains("BEGIN CERTIFICATE"),
            "issued certificate must be PEM; got {cert}"
        );
    }

    #[tokio::test]
    async fn reconcile_leaves_certificate_empty_when_no_ca_configured() {
        // Without a configured signer the controller must not invent a cert —
        // it leaves issuance to an external signer (pre-existing behaviour).
        let (_ca, request_b64) = ca_and_request("system:node:n2");
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage.clone());

        let csr = approved_csr("no-signer", request_b64);
        let key = "/registry/certificatesigningrequests/no-signer";
        storage.create(key, &csr).await.unwrap();

        controller.reconcile_csr(&csr).await.unwrap();

        let updated: CertificateSigningRequest = storage.get(key).await.unwrap();
        assert!(
            updated.status.and_then(|s| s.certificate).is_none(),
            "no CA configured → status.certificate must stay empty"
        );
    }

    #[tokio::test]
    async fn test_validate_csr_spec_empty_request() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let spec = CertificateSigningRequestSpec {
            request: "".to_string(),
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_csr_spec_empty_signer() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "".to_string(),
            usages: vec![KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_csr_spec_no_usages() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_key_usage() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        // Test common usages
        assert!(controller
            .validate_key_usage(&KeyUsage::DigitalSignature)
            .is_ok());
        assert!(controller.validate_key_usage(&KeyUsage::ClientAuth).is_ok());
        assert!(controller.validate_key_usage(&KeyUsage::ServerAuth).is_ok());
        assert!(controller
            .validate_key_usage(&KeyUsage::KeyEncipherment)
            .is_ok());
    }

    #[tokio::test]
    async fn test_should_auto_approve_kubelet_client() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["system:node:test".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let csr = CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new("kubelet-client-test"),
            spec: CertificateSigningRequestSpec {
                request: csr_b64,
                signer_name: "kubernetes.io/kube-apiserver-client-kubelet".to_string(),
                usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                expiration_seconds: Some(3600),
                uid: None,
                groups: None,
                username: Some("system:node:test".to_string()),
                extra: None,
            },
            status: None,
        };

        assert!(controller.should_auto_approve(&csr).unwrap());
    }

    #[tokio::test]
    async fn test_should_auto_approve_kubelet_serving() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["node1.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let csr = CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new("kubelet-serving-test"),
            spec: CertificateSigningRequestSpec {
                request: csr_b64,
                signer_name: "kubernetes.io/kubelet-serving".to_string(),
                usages: vec![
                    KeyUsage::DigitalSignature,
                    KeyUsage::KeyEncipherment,
                    KeyUsage::ServerAuth,
                ],
                expiration_seconds: Some(3600),
                uid: None,
                groups: None,
                username: Some("system:node:test".to_string()),
                extra: None,
            },
            status: None,
        };

        assert!(controller.should_auto_approve(&csr).unwrap());
    }
}
