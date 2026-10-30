//! Unit tests moved from `src/commands/auth.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::types::AuthCommands;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::{json, Value};
    pub use std::fs;
}

mod auth_reconcile_tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::auth::*;

    #[test]
    fn test_reconcile_merge_rules() {
        let current_rules =
            vec![json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]})];
        let desired_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}),
            json!({"apiGroups": [""], "resources": ["services"], "verbs": ["list"]}),
        ];

        let mut merged = current_rules;
        for rule in desired_rules {
            if !merged.contains(&rule) {
                merged.push(rule);
            }
        }

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_reconcile_merge_subjects() {
        let current_subjects =
            vec![json!({"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"})];
        let desired_subjects = vec![
            json!({"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}),
            json!({"kind": "User", "name": "bob", "apiGroup": "rbac.authorization.k8s.io"}),
        ];

        let mut merged = current_subjects;
        for sub in desired_subjects {
            if !merged.contains(&sub) {
                merged.push(sub);
            }
        }

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_reconcile_replace_rules_when_remove_extra() {
        let current_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}),
            json!({"apiGroups": [""], "resources": ["nodes"], "verbs": ["list"]}),
        ];
        let desired_rules =
            vec![json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]})];

        let remove_extra_permissions = true;
        let result = if remove_extra_permissions {
            desired_rules.clone()
        } else {
            let mut merged = current_rules;
            for rule in desired_rules {
                if !merged.contains(&rule) {
                    merged.push(rule);
                }
            }
            merged
        };

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_reconcile_replace_subjects_when_remove_extra() {
        let current_subjects = vec![
            json!({"kind": "User", "name": "alice"}),
            json!({"kind": "User", "name": "charlie"}),
        ];
        let desired_subjects = vec![json!({"kind": "User", "name": "alice"})];

        let remove_extra_subjects = true;
        let result = if remove_extra_subjects {
            desired_subjects.clone()
        } else {
            let mut merged = current_subjects;
            for sub in desired_subjects {
                if !merged.contains(&sub) {
                    merged.push(sub);
                }
            }
            merged
        };

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_self_subject_access_review_serialization() {
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: "get".to_string(),
                    resource: "pods".to_string(),
                    name: None,
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert_eq!(json["apiVersion"], "authorization.k8s.io/v1");
        assert_eq!(json["kind"], "SelfSubjectAccessReview");
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "get");
        assert_eq!(json["spec"]["resourceAttributes"]["resource"], "pods");
        assert_eq!(json["spec"]["resourceAttributes"]["namespace"], "default");
        assert!(json["spec"]["resourceAttributes"]["name"].is_null());
    }

    #[test]
    fn test_collection_path_from_resource_path() {
        let path = "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin";
        let collection_path = path.rsplit_once('/').map(|(p, _)| p).unwrap_or(path);
        assert_eq!(
            collection_path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles"
        );
    }

    #[test]
    fn test_self_subject_access_review_all_namespaces() {
        // When all_namespaces is true, namespace should be None
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: None,
                    verb: "list".to_string(),
                    resource: "pods".to_string(),
                    name: None,
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert!(json["spec"]["resourceAttributes"]["namespace"].is_null());
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "list");
    }

    #[test]
    fn test_self_subject_access_review_with_name() {
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("prod".to_string()),
                    verb: "delete".to_string(),
                    resource: "deployments".to_string(),
                    name: Some("web-app".to_string()),
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert_eq!(json["spec"]["resourceAttributes"]["name"], "web-app");
        assert_eq!(json["spec"]["resourceAttributes"]["namespace"], "prod");
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "delete");
        assert_eq!(
            json["spec"]["resourceAttributes"]["resource"],
            "deployments"
        );
    }

    #[test]
    fn test_collection_path_for_namespaced_resources() {
        let role_path = "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role";
        let collection = role_path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .unwrap_or(role_path);
        assert_eq!(
            collection,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles"
        );

        let binding_path =
            "/apis/rbac.authorization.k8s.io/v1/namespaces/prod/rolebindings/my-binding";
        let collection = binding_path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .unwrap_or(binding_path);
        assert_eq!(
            collection,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/prod/rolebindings"
        );
    }

    #[test]
    fn test_reconcile_merge_no_duplicates() {
        // Merging identical rules should not create duplicates
        let current_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}),
            json!({"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}),
        ];
        let desired_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}),
            json!({"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}),
        ];

        let mut merged = current_rules;
        for rule in desired_rules {
            if !merged.contains(&rule) {
                merged.push(rule);
            }
        }

        assert_eq!(merged.len(), 2);
    }
}
