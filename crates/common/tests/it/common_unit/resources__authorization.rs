//! Unit tests moved from `src/resources/authorization.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::authorization::*;

    #[test]
    fn test_subject_access_review_serialization() {
        let sar = SubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SubjectAccessReviewSpec {
                user: Some("admin".to_string()),
                groups: Some(vec!["system:masters".to_string()]),
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("get".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("pods".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                extra: None,
                uid: None,
            },
            status: Some(SubjectAccessReviewStatus {
                allowed: true,
                denied: Some(false),
                evaluation_error: None,
                reason: Some("RBAC allows".to_string()),
            }),
        };

        let json = serde_json::to_string(&sar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SubjectAccessReview"));
        assert!(json.contains("admin"));
    }

    #[test]
    fn test_self_subject_access_review_serialization() {
        let ssar = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("create".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("pods".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
            },
            status: None,
        };

        let json = serde_json::to_string(&ssar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SelfSubjectAccessReview"));
    }

    #[test]
    fn test_local_subject_access_review_serialization() {
        let lsar = LocalSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "LocalSubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SubjectAccessReviewSpec {
                user: Some("user1".to_string()),
                groups: None,
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("delete".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("services".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                extra: None,
                uid: None,
            },
            status: None,
        };

        let json = serde_json::to_string(&lsar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("LocalSubjectAccessReview"));
    }

    #[test]
    fn test_self_subject_rules_review_serialization() {
        let ssrr = SelfSubjectRulesReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectRulesReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SelfSubjectRulesReviewSpec {
                namespace: "default".to_string(),
            },
            status: Some(SubjectRulesReviewStatus {
                incomplete: false,
                non_resource_rules: vec![],
                resource_rules: vec![ResourceRule {
                    verbs: vec!["get".to_string(), "list".to_string()],
                    api_groups: Some(vec!["".to_string()]),
                    resources: Some(vec!["pods".to_string()]),
                    resource_names: None,
                }],
                evaluation_error: None,
            }),
        };

        let json = serde_json::to_string(&ssrr).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SelfSubjectRulesReview"));
    }

    /// SubjectAccessReview without spec must decode (Go-parity).
    /// Reproduces sig-auth SubjectReview 422 "missing field `spec`" from the
    /// 2026-05-31 conformance run (subjectreviews.go:101).
    #[test]
    fn test_subject_access_review_without_spec_decodes() {
        let json = r#"{
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "metadata": {}
        }"#;

        let sar: SubjectAccessReview =
            serde_json::from_str(json).expect("SAR without spec must decode (Go zero-value)");
        // spec defaults to zero-value: all None fields
        assert!(sar.spec.user.is_none());
        assert!(sar.spec.resource_attributes.is_none());
    }

    /// SubjectRulesReviewStatus with missing list fields must decode.
    /// Go: []NonResourceRule, []ResourceRule — absent leaves empty slice.
    #[test]
    fn test_subject_rules_review_status_without_lists_decodes() {
        let json = r#"{
            "incomplete": false
        }"#;

        let status: SubjectRulesReviewStatus =
            serde_json::from_str(json).expect("status without rule lists must decode");
        assert!(!status.incomplete);
        assert!(status.non_resource_rules.is_empty());
        assert!(status.resource_rules.is_empty());
    }
}
