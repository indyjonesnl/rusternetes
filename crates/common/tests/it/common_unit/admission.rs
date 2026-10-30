//! Unit tests moved from `src/admission.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::pod::Pod;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::admission::*;

    #[test]
    fn test_admission_response() {
        let allow = AdmissionResponse::Allow;
        assert!(allow.is_allowed());
        assert!(allow.deny_reason().is_none());
        assert!(allow.patches().is_none());

        let deny = AdmissionResponse::Deny("test reason".to_string());
        assert!(!deny.is_allowed());
        assert_eq!(deny.deny_reason(), Some("test reason"));

        let patch = AdmissionResponse::AllowWithPatch(vec![]);
        assert!(patch.is_allowed());
        assert_eq!(patch.patches(), Some(&[][..]));
    }

    #[test]
    fn test_pod_security_levels() {
        assert_eq!(PodSecurityLevel::Privileged.as_str(), "privileged");
        assert_eq!(PodSecurityLevel::Baseline.as_str(), "baseline");
        assert_eq!(PodSecurityLevel::Restricted.as_str(), "restricted");
    }

    #[test]
    fn test_allowed_capabilities() {
        assert!(is_allowed_capability_baseline("NET_BIND_SERVICE"));
        assert!(is_allowed_capability_baseline("CHOWN"));
        assert!(!is_allowed_capability_baseline("SYS_ADMIN"));
        assert!(!is_allowed_capability_baseline("NET_ADMIN"));
    }

    #[test]
    fn test_admission_review_response() {
        let allow = AdmissionReviewResponse::allow("test-uid".to_string());
        assert!(allow.allowed);
        assert!(allow.status.is_none());

        let deny = AdmissionReviewResponse::deny("test-uid".to_string(), "denied".to_string());
        assert!(!deny.allowed);
        assert!(deny.status.is_some());
    }

    #[test]
    fn test_group_version_kind() {
        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };
        assert_eq!(gvk.group, "apps");
        assert_eq!(gvk.version, "v1");
        assert_eq!(gvk.kind, "Deployment");
    }

    #[test]
    fn test_parse_real_webhook_response() {
        // Exact response from K8s conformance test webhook server
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "ab818250-607b-4060-812a-973a0c206d26",
                "allowed": false,
                "status": {
                    "metadata": {},
                    "message": "this webhook denies all requests"
                }
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse real webhook response");
        let response = review.response.expect("Should have response");
        assert_eq!(response.uid, "ab818250-607b-4060-812a-973a0c206d26");
        assert!(!response.allowed);
        let status = response.status.expect("Should have status");
        assert_eq!(
            status.message.as_deref(),
            Some("this webhook denies all requests")
        );
    }

    #[test]
    fn test_parse_webhook_allow_response() {
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "test-uid",
                "allowed": true
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse allow response");
        let response = review.response.expect("Should have response");
        assert!(response.allowed);
        assert!(response.status.is_none());
    }

    #[test]
    fn test_parse_webhook_mutating_response() {
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "test-uid",
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": "W3sib3AiOiAiYWRkIiwgInBhdGgiOiAiL21ldGFkYXRhL2xhYmVscy9tdXRhdGVkIiwgInZhbHVlIjogInRydWUifV0="
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse mutating response");
        let response = review.response.expect("Should have response");
        assert!(response.allowed);
        assert!(response.patch.is_some());
        assert_eq!(response.patch_type.as_deref(), Some("JSONPatch"));
    }
}
