//! Unit tests moved from `src/controllers/network_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::{NetworkPolicy, Pod};
    pub use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{
        NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
    };
    use rusternetes_common::types::{
        LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta,
    };
    use rusternetes_controller_manager::controllers::network_policy::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_validate_policy_valid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let policy = NetworkPolicy {
            type_meta: TypeMeta {
                kind: "NetworkPolicy".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-policy").with_namespace("default"),
            spec: NetworkPolicySpec {
                pod_selector: LabelSelector {
                    match_labels: Some(HashMap::new()),
                    match_expressions: None,
                },
                ingress: None,
                egress: None,
                policy_types: Some(vec!["Ingress".to_string()]),
            },
        };

        assert!(controller.validate_policy(&policy).is_ok());
    }

    #[tokio::test]
    async fn test_validate_policy_invalid_type() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let policy = NetworkPolicy {
            type_meta: TypeMeta {
                kind: "NetworkPolicy".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-policy").with_namespace("default"),
            spec: NetworkPolicySpec {
                pod_selector: LabelSelector {
                    match_labels: Some(HashMap::new()),
                    match_expressions: None,
                },
                ingress: None,
                egress: None,
                policy_types: Some(vec!["Invalid".to_string()]),
            },
        };

        assert!(controller.validate_policy(&policy).is_err());
    }

    #[tokio::test]
    async fn test_validate_ingress_rule() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let rule = NetworkPolicyIngressRule {
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".to_string()),
                port: None,
                end_port: Some(8080),
            }]),
            from: Some(vec![NetworkPolicyPeer {
                pod_selector: Some(LabelSelector {
                    match_labels: Some(HashMap::new()),
                    match_expressions: None,
                }),
                namespace_selector: None,
                ip_block: None,
            }]),
        };

        assert!(controller.validate_ingress_rule(&rule, 0).is_ok());
    }

    #[tokio::test]
    async fn test_validate_port_invalid_protocol() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let port = NetworkPolicyPort {
            protocol: Some("HTTP".to_string()), // Invalid
            port: None,
            end_port: None,
        };

        assert!(controller
            .validate_network_policy_port(&port, 0, 0)
            .is_err());
    }

    #[tokio::test]
    async fn test_pod_matches_selector_empty() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod"),
            spec: None,
            status: None,
        };

        // Empty selector matches all pods
        let selector = LabelSelector {
            match_labels: Some(HashMap::new()),
            match_expressions: None,
        };

        assert!(controller.pod_matches_selector(&pod, &selector));
    }

    #[tokio::test]
    async fn test_pod_matches_selector_with_labels() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let mut pod_labels = HashMap::new();
        pod_labels.insert("app".to_string(), "nginx".to_string());

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                labels: Some(pod_labels),
                ..ObjectMeta::new("test-pod")
            },
            spec: None,
            status: None,
        };

        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_string(), "nginx".to_string());

        let selector = LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        };

        assert!(controller.pod_matches_selector(&pod, &selector));
    }

    fn bare_pod() -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod"),
            spec: None,
            status: None,
        }
    }

    #[tokio::test]
    async fn test_pod_matches_expression() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let mut pod = bare_pod();
        pod.metadata.labels = Some(HashMap::from([("env".to_string(), "prod".to_string())]));

        let selector = |req: LabelSelectorRequirement| LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![req]),
        };

        for (label, req) in [
            (
                "In",
                LabelSelectorRequirement {
                    key: "env".to_string(),
                    operator: "In".to_string(),
                    values: Some(vec!["prod".to_string(), "staging".to_string()]),
                },
            ),
            (
                "NotIn",
                LabelSelectorRequirement {
                    key: "env".to_string(),
                    operator: "NotIn".to_string(),
                    values: Some(vec!["dev".to_string()]),
                },
            ),
            (
                "Exists",
                LabelSelectorRequirement {
                    key: "env".to_string(),
                    operator: "Exists".to_string(),
                    values: None,
                },
            ),
            (
                "DoesNotExist",
                LabelSelectorRequirement {
                    key: "nonexistent".to_string(),
                    operator: "DoesNotExist".to_string(),
                    values: None,
                },
            ),
        ] {
            assert!(
                controller.pod_matches_selector(&pod, &selector(req)),
                "{label} must select the pod"
            );
        }
    }

    /// A pod with no labels at all is still put through `matchExpressions`.
    /// The hand-rolled matcher answered it from `matchLabels` alone, so a
    /// `DoesNotExist` term — which such a pod satisfies by definition — never
    /// selected it (#2012).
    #[tokio::test]
    async fn a_pod_with_no_labels_is_still_matched_against_expressions() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);
        let pod = bare_pod();

        let does_not_exist = LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "env".to_string(),
                operator: "DoesNotExist".to_string(),
                values: None,
            }]),
        };
        assert!(controller.pod_matches_selector(&pod, &does_not_exist));

        let exists = LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "env".to_string(),
                operator: "Exists".to_string(),
                values: None,
            }]),
        };
        assert!(!controller.pod_matches_selector(&pod, &exists));
    }

    /// An empty (`{}`) `podSelector` selects every pod in the namespace,
    /// including one with no labels — `LabelSelectorAsSelector` answers
    /// `labels.Everything()` for it (`helpers.go:40-42`).
    #[tokio::test]
    async fn an_empty_pod_selector_selects_every_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NetworkPolicyController::new(storage);

        let empty = LabelSelector {
            match_labels: None,
            match_expressions: None,
        };
        assert!(controller.pod_matches_selector(&bare_pod(), &empty));

        let mut labelled = bare_pod();
        labelled.metadata.labels = Some(HashMap::from([("a".to_string(), "b".to_string())]));
        assert!(controller.pod_matches_selector(&labelled, &empty));
    }
}
