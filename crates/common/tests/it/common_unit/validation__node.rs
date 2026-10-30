//! Unit tests moved from `src/validation/node.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::resources::node::{
        ConfigMapNodeConfigSource, Node, NodeConfigSource, NodeConfigStatus, NodeSwapStatus, Taint,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::resourcequota::validate_resource_quantity_value;
    pub use serde::Deserialize;
    pub use std::collections::{HashMap, HashSet};
    pub use std::net::IpAddr;
}

mod status_update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::node::*;

    fn node_with_status(status: serde_json::Value) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "node-1", "resourceVersion": "1"},
            "status": status,
        }))
        .expect("node decodes")
    }

    #[test]
    fn clean_status_passes() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "Hostname", "address": "node-1"}
            ],
            "capacity": {"cpu": "4", "memory": "8Gi"},
            "allocatable": {"cpu": "4", "memory": "8Gi"},
            "declaredFeatures": ["GuaranteedQoSPodCPUResize"]
        }));
        assert!(validate_node_update(&node, &node).is_empty());
    }

    #[test]
    fn duplicate_address_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "InternalIP", "address": "10.0.0.1"}
            ]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.field.contains("addresses")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "capacity": {"cpu": "-1"}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.field.contains("capacity") && e.detail.contains("greater than or equal")),
            "{errs:?}"
        );
    }

    #[test]
    fn unsorted_declared_features_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["ZebraFeature", "AlphaFeature"]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("sorted alphabetically")),
            "{errs:?}"
        );
    }

    #[test]
    fn bad_declared_feature_name_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["lowercaseStart"]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.detail.contains("UpperCamelCase")),
            "{errs:?}"
        );
    }

    #[test]
    fn empty_status_passes() {
        let node = node_with_status(serde_json::json!({}));
        assert!(validate_node_update(&node, &node).is_empty());
    }

    #[test]
    fn config_status_valid_assigned_passes() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "kube-system", "name": "my-config",
                "uid": "abc-123", "resourceVersion": "42", "kubeletConfigKey": "kubelet"
            }}}
        }));
        assert!(
            validate_node_update(&node, &node).is_empty(),
            "{:?}",
            validate_node_update(&node, &node)
        );
    }

    #[test]
    fn config_status_requires_uid_and_resource_version() {
        let node = node_with_status(serde_json::json!({
            "config": {"active": {"configMap": {
                "namespace": "kube-system", "name": "my-config", "kubeletConfigKey": "kubelet"
            }}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(errs.iter().any(|e| e.field.ends_with("uid")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("resourceVersion")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_empty_source_rejected() {
        // a NodeConfigSource with no subfield set -> "exactly one reference subfield"
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("exactly one reference subfield")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_bad_namespace_and_key_rejected() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "Bad_NS", "name": "cfg", "uid": "u", "resourceVersion": "1",
                "kubeletConfigKey": "bad key!"
            }}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("kubeletConfigKey")),
            "{errs:?}"
        );
    }
}

mod avoid_pods_tests {
    use super::__parent::*;
    use rusternetes_common::validation::node::*;

    const KEY: &str = "scheduler.alpha.kubernetes.io/preferAvoidPods";

    fn node_with_avoid(raw: &str) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "abc-123", "annotations": {KEY: raw}},
        }))
        .expect("node decodes")
    }

    fn fields(raw: &str) -> Vec<String> {
        validate_node(&node_with_avoid(raw))
            .into_iter()
            .map(|e| e.field)
            .collect()
    }

    // validation_test.go:18415-18435 (valid entry).
    #[test]
    fn valid_entry_passes() {
        let raw = r#"{"preferAvoidPods":[{"podSignature":{"podController":{
            "apiVersion":"v1","kind":"ReplicationController","name":"foo",
            "uid":"abcdef123456","controller":true}},
            "reason":"some reason","message":"some message"}]}"#;
        assert!(fields(raw).is_empty());
    }

    // validation_test.go:18608-18628 ("missing-podSignature").
    #[test]
    fn missing_pod_signature_is_required() {
        let f = fields(r#"{"preferAvoidPods":[{"reason":"r","message":"m"}]}"#);
        assert_eq!(
            f,
            vec![format!("metadata.annotations.{KEY}[0].PodSignature")]
        );
    }

    // validation_test.go:18630-18660 ("invalid-podController").
    #[test]
    fn non_controller_pod_controller_is_invalid() {
        let raw = r#"{"preferAvoidPods":[{"podSignature":{"podController":{
            "apiVersion":"v1","kind":"ReplicationController","name":"foo",
            "uid":"abcdef123456","controller":false}}}]}"#;
        let errs = validate_node(&node_with_avoid(raw));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].field,
            format!("metadata.annotations.{KEY}[0].PodSignature.PodController.Controller")
        );
        assert!(errs[0].detail.contains("must point to a controller"));
    }

    // validatePreferAvoidPodsEntry (validation.go:5121-5135) dereferences
    // `*PodController.Controller` unchecked: an owner reference without
    // `controller` is a nil-pointer dereference, i.e. a Go panic that
    // `WithPanicRecovery` turns into a 500 (#3378).
    #[test]
    #[should_panic(expected = "nil pointer dereference")]
    fn unset_controller_panics_like_upstream_nil_deref() {
        let raw = r#"{"preferAvoidPods":[{"podSignature":{"podController":{
            "apiVersion":"v1","kind":"ReplicationController","name":"foo",
            "uid":"abcdef123456"}}}]}"#;
        let _ = validate_node(&node_with_avoid(raw));
    }

    #[test]
    fn undecodable_annotation_is_invalid_at_avoid_pods() {
        let f = fields("not json");
        assert_eq!(f, vec!["metadata.annotations.AvoidPods".to_string()]);
    }
}

mod pod_cidr_tests {
    use super::__parent::*;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::node::*;

    fn node_with_cidrs(cidrs: &[&str]) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "abc-123"},
            "spec": {"podCIDRs": cidrs},
        }))
        .expect("node decodes")
    }

    // validation.go:7200 validates each podCIDR with IsValidCIDRForLegacyField
    // (ParseCIDRSloppy), so a leading-zero IPv4 CIDR is accepted (and only
    // warned about by nodeWarnings -> GetWarningsForCIDR).
    #[test]
    fn leading_zero_ipv4_cidr_is_accepted() {
        assert!(validate_node(&node_with_cidrs(&["010.009.008.0/24"])).is_empty());
    }

    // validation.go:7205: IsDualStackCIDRStrings -> ParseCIDRs -> ParseCIDRSloppy.
    #[test]
    fn leading_zero_ipv4_cidr_dual_stack_is_accepted() {
        assert!(validate_node(&node_with_cidrs(&["010.009.008.0/24", "2001:db8::/64"])).is_empty());
    }

    #[test]
    fn garbage_cidr_is_still_invalid() {
        let errs = validate_node(&node_with_cidrs(&["10.9.8.0/33"]));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].field, "spec.podCIDRs[0]");
    }

    // validation.go:7205-7208: when ParseCIDRs fails on a multi-CIDR list,
    // upstream emits field.InternalError(podCIDRsField, ...) and then, since
    // dualStack is false, the Invalid "no more than one CIDR" error too.
    #[test]
    fn unparseable_multi_cidr_list_emits_internal_error() {
        let errs = validate_node(&node_with_cidrs(&["10.9.8.0/24", "bogus"]));
        let internal: Vec<_> = errs
            .iter()
            .filter(|e| e.error_type == ErrorType::Internal)
            .collect();
        assert_eq!(internal.len(), 1, "{errs:?}");
        assert_eq!(internal[0].field, "spec.podCIDRs");
        assert_eq!(
            internal[0].detail,
            "invalid PodCIDRs. failed to check with dual stack with error:invalid CIDR[1]: <nil> (invalid CIDR address: bogus)"
        );
        // per-element Invalid + the dual-stack Invalid remain.
        assert!(errs.iter().any(|e| e.field == "spec.podCIDRs[1]"));
        assert!(errs.iter().any(|e| e.field == "spec.podCIDRs"
            && e.error_type == ErrorType::Invalid
            && e.detail == "may specify no more than one CIDR for each IP family"));
    }

    #[test]
    fn single_bad_cidr_has_no_internal_error() {
        let errs = validate_node(&node_with_cidrs(&["bogus"]));
        assert!(errs.iter().all(|e| e.error_type != ErrorType::Internal));
    }
}
