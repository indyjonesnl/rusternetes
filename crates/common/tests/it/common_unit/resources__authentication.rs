//! Unit tests moved from `src/resources/authentication.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::authentication::*;

    #[test]
    fn test_token_review_serialization() {
        let token_review = TokenReview {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: TokenReviewSpec {
                audiences: Some(vec!["https://kubernetes.default.svc".to_string()]),
                token: "my-token".to_string(),
            },
            status: Some(TokenReviewStatus {
                authenticated: Some(true),
                user: Some(UserInfo {
                    username: Some("system:serviceaccount:default:my-sa".to_string()),
                    uid: Some("12345".to_string()),
                    groups: Some(vec!["system:serviceaccounts".to_string()]),
                    extra: None,
                }),
                audiences: Some(vec!["https://kubernetes.default.svc".to_string()]),
                error: None,
            }),
        };

        let json = serde_json::to_string(&token_review).unwrap();
        assert!(json.contains("authentication.k8s.io/v1"));
        assert!(json.contains("TokenReview"));
        assert!(json.contains("my-token"));
    }

    #[test]
    fn test_token_request_serialization() {
        let token_request = TokenRequest {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenRequest".to_string(),
            metadata: ObjectMeta::new(""),
            spec: TokenRequestSpec {
                audiences: vec!["https://kubernetes.default.svc".to_string()],
                bound_object_ref: None,
                expiration_seconds: Some(3600),
            },
            status: None,
        };

        let json = serde_json::to_string(&token_request).unwrap();
        assert!(json.contains("authentication.k8s.io/v1"));
        assert!(json.contains("TokenRequest"));
        assert!(json.contains("3600"));
    }

    #[test]
    fn test_token_request_deserialization_with_extra_fields() {
        // The K8s client may send extra fields like "status" or unknown fields.
        // Our deserialization should tolerate them (serde default behavior with
        // camelCase rename ignores unknown fields).
        let json = r#"{
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {
                "name": "e2e-sa-test",
                "namespace": "e2e-ns",
                "creationTimestamp": null
            },
            "spec": {
                "audiences": ["https://kubernetes.default.svc"],
                "expirationSeconds": 3600,
                "boundObjectRef": null
            },
            "status": null
        }"#;

        let token_request: TokenRequest = serde_json::from_str(json).unwrap();
        assert_eq!(token_request.api_version, "authentication.k8s.io/v1");
        assert_eq!(token_request.kind, "TokenRequest");
        assert_eq!(
            token_request.spec.audiences,
            vec!["https://kubernetes.default.svc"]
        );
        assert_eq!(token_request.spec.expiration_seconds, Some(3600));
    }

    #[test]
    fn test_token_request_deserialization_minimal() {
        // The protobuf fallback may produce a minimal body with just apiVersion/kind/metadata
        let json = r#"{
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {}
        }"#;

        let token_request: TokenRequest = serde_json::from_str(json).unwrap();
        assert_eq!(token_request.spec.audiences.len(), 0);
        assert_eq!(token_request.spec.expiration_seconds, None);
        assert!(token_request.spec.bound_object_ref.is_none());
    }

    #[test]
    fn test_token_request_deserialization_empty_spec() {
        // K8s client may send an empty spec object
        let json = r#"{
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {},
            "spec": {}
        }"#;

        let token_request: TokenRequest = serde_json::from_str(json).unwrap();
        assert_eq!(token_request.spec.audiences.len(), 0);
    }

    #[test]
    fn test_token_request_deserialization_go_style_null_status() {
        // Go's encoding/json serializes zero-valued TokenRequestStatus as:
        //   "status": {"token": "", "expirationTimestamp": null}
        // because metav1.Time marshals to null for the zero time.
        // Our deserialization must handle this without error.
        let json = r#"{
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {"creationTimestamp": null},
            "spec": {
                "audiences": ["https://kubernetes.default.svc"],
                "expirationSeconds": 3607
            },
            "status": {
                "token": "",
                "expirationTimestamp": null
            }
        }"#;

        let token_request: TokenRequest = serde_json::from_str(json).unwrap();
        assert_eq!(token_request.api_version, "authentication.k8s.io/v1");
        assert_eq!(token_request.kind, "TokenRequest");
        assert_eq!(
            token_request.spec.audiences,
            vec!["https://kubernetes.default.svc"]
        );
        assert_eq!(token_request.spec.expiration_seconds, Some(3607));
        // Status should be Some with default/empty values for null fields
        let status = token_request.status.unwrap();
        assert_eq!(status.token, "");
        assert_eq!(status.expiration_timestamp, "");
    }

    #[test]
    fn test_token_request_deserialization_with_bound_object_ref() {
        // The conformance test sends a TokenRequest with a boundObjectRef to a Pod.
        // This must deserialize correctly so the handler can set pod binding info.
        let json = r#"{
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {"creationTimestamp": null},
            "spec": {
                "audiences": ["https://kubernetes.default.svc"],
                "expirationSeconds": 600,
                "boundObjectRef": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": "pod-test-123",
                    "uid": "pod-uid-456"
                }
            },
            "status": {
                "token": "",
                "expirationTimestamp": null
            }
        }"#;

        let token_request: TokenRequest = serde_json::from_str(json).unwrap();
        let bound_ref = token_request.spec.bound_object_ref.unwrap();
        assert_eq!(bound_ref.kind.as_deref(), Some("Pod"));
        assert_eq!(bound_ref.name.as_deref(), Some("pod-test-123"));
        assert_eq!(bound_ref.uid.as_deref(), Some("pod-uid-456"));
    }

    #[test]
    fn test_self_subject_review_serialization() {
        let review = SelfSubjectReview {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "SelfSubjectReview".to_string(),
            metadata: ObjectMeta::new(""),
            status: Some(SelfSubjectReviewStatus {
                user_info: Some(UserInfo {
                    username: Some("admin".to_string()),
                    uid: Some("admin-uid".to_string()),
                    groups: Some(vec!["system:masters".to_string()]),
                    extra: None,
                }),
            }),
        };

        let json = serde_json::to_string(&review).unwrap();
        assert!(json.contains("authentication.k8s.io/v1"));
        assert!(json.contains("SelfSubjectReview"));
        assert!(json.contains("admin"));
    }
}
