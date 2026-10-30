//! Unit tests moved from `src/admission/certificates.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::state::ApiServerState;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{Decision, RequestAttributes};
    pub use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
    pub use rusternetes_common::resources::{
        CertificateSigningRequest, CertificateSigningRequestStatus, ClusterTrustBundle,
    };
}

mod subject_restriction_tests {
    //! Cases of subjectrestriction/admission_test.go `TestPlugin_Validate`
    //! (:34-110). "ignored resource", "ignored subresource" and "wrong type"
    //! are the caller's resource match and Rust's typing.
    use super::__parent::*;
    use base64::Engine;
    use rusternetes_api_server::admission::certificates::*;

    /// `pemWithGroup` (admission_test.go:166-191), base64'd as `spec.request`.
    fn request_with_group(group: &str) -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::OrganizationName, group);
        let pem = params.serialize_request(&key).unwrap().pem().unwrap();
        base64::engine::general_purpose::STANDARD.encode(pem)
    }

    fn csr(request: String, signer: &str) -> CertificateSigningRequest {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "x"},
            "spec": {"request": request, "signerName": signer, "usages": []}
        }))
        .unwrap()
    }

    #[test]
    fn some_other_signer() {
        let c = csr(
            request_with_group("system:masters"),
            "kubernetes.io/kube-apiserver-client-kubelet",
        );
        assert_eq!(subject_restriction_error(&c), None);
    }

    #[test]
    fn invalid_request() {
        let c = csr(
            base64::engine::general_purpose::STANDARD.encode("this is not a CSR"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(
            subject_restriction_error(&c).as_deref(),
            Some("failed to parse CSR: PEM block type must be CERTIFICATE REQUEST")
        );
    }

    #[test]
    fn some_other_group() {
        let c = csr(
            request_with_group("system:admin"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(subject_restriction_error(&c), None);
    }

    #[test]
    fn request_for_system_masters() {
        let c = csr(
            request_with_group("system:masters"),
            KUBE_APISERVER_CLIENT_SIGNER_NAME,
        );
        assert_eq!(
            subject_restriction_error(&c).as_deref(),
            Some(
                "use of kubernetes.io/kube-apiserver-client signer with system:masters group is not allowed"
            )
        );
    }
}
