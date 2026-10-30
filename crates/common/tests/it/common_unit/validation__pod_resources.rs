//! Unit tests moved from `src/validation/pod_resources.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::quota::aggregate_container_resources;
    pub use rusternetes_common::resources::pod::PodSpec;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_common::types::{ResourceClaim, ResourceRequirements};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::limitrange::is_overcommit_allowed;
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_common::validation::pod_options::PodValidationOptions;
    pub use rusternetes_common::validation::resourcequota::validate_resource_quantity_value;
    pub use rusternetes_common::validation::runtimeclass::{
        is_huge_page_resource_name, validate_resource_name,
    };
    pub use std::collections::BTreeSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::pod_resources::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }
    fn opts() -> PodValidationOptions {
        PodValidationOptions {
            allow_indivisible_huge_pages_values: true,
            ..Default::default()
        }
    }
    fn names() -> BTreeSet<String> {
        ["resource-claim".to_string()].into()
    }
    fn run(s: &PodSpec) -> Vec<String> {
        validate_pod_resources(s, &names(), &Path::new("spec"), &opts())
            .iter()
            .map(|e| e.to_string())
            .collect()
    }
    fn base(os: Option<&str>, resources: serde_json::Value) -> PodSpec {
        let mut v = json!({
            "containers": [{"name": "c", "image": "i"}],
            "resourceClaims": [{"name": "resource-claim"}],
            "resources": resources,
        });
        if let Some(os) = os {
            v["os"] = json!({"name": os});
        }
        spec(v)
    }
    fn good() -> serde_json::Value {
        json!({
            "requests": {"cpu": "10", "memory": "10Mi"},
            "limits": {"cpu": "20", "memory": "20Mi", "hugepages-2Mi": "100Mi"},
        })
    }

    /// `TestValidatePodResources` (validation_test.go:20484).
    #[test]
    fn pod_resources_os_and_claims() {
        assert!(run(&base(Some("linux"), good())).is_empty());
        let errs = run(&base(Some("windows"), good()));
        assert_eq!(
            errs,
            vec!["spec.resources: Forbidden: may not be set for a windows pod"]
        );
        let mut r = good();
        r["claims"] = json!([{"name": "resource-claim"}]);
        let errs = run(&base(Some("linux"), r));
        assert_eq!(
            errs,
            vec!["spec.resources.claims: Forbidden: claims may not be set for Resources at pod-level"]
        );
    }

    /// No `spec.resources` means no pod-level validation at all.
    #[test]
    fn no_pod_resources_is_a_no_op() {
        let s = spec(json!({"containers": [{"name": "c", "image": "i"}]}));
        assert!(run(&s).is_empty());
    }

    /// `validatePodResourceName` (:7457): only cpu, memory and hugepages.
    #[test]
    fn unsupported_pod_level_resource_name() {
        let errs = run(&base(
            None,
            json!({"requests": {"ephemeral-storage": "1Gi"}}),
        ));
        assert!(
            errs.iter().any(|e| e.contains("Unsupported value")),
            "{errs:?}"
        );
    }

    /// `validateResourceRequirements` rules reached at pod level (:7826).
    #[test]
    fn requirements_rules() {
        let errs = run(&base(
            None,
            json!({"requests": {"cpu": "2"}, "limits": {"cpu": "1"}}),
        ));
        assert!(
            errs.iter()
                .any(|e| e.contains("must be less than or equal to cpu limit of 1")),
            "{errs:?}"
        );
        let errs = run(&base(None, json!({"requests": {"hugepages-2Mi": "2Mi"}})));
        assert!(
            errs.iter()
                .any(|e| e.contains("Limit must be set for non overcommitable resources")),
            "{errs:?}"
        );
        let errs = run(&base(
            None,
            json!({"requests": {"hugepages-2Mi": "2Mi"}, "limits": {"hugepages-2Mi": "2Mi"}}),
        ));
        assert!(
            errs.iter()
                .any(|e| e.contains("HugePages require cpu or memory")),
            "{errs:?}"
        );
        let mut strict = opts();
        strict.allow_indivisible_huge_pages_values = false;
        let s = base(
            None,
            json!({"limits": {"cpu": "1", "hugepages-2Mi": "3Mi"}}),
        );
        let errs = validate_pod_resources(&s, &names(), &Path::new("spec"), &strict);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("is not positive integer multiple of")),
            "{errs:?}"
        );
    }

    fn with_containers(pod: serde_json::Value, ctrs: serde_json::Value) -> PodSpec {
        spec(json!({"containers": ctrs, "resources": pod}))
    }
    fn consistency(s: &PodSpec) -> Vec<String> {
        validate_pod_resource_consistency(s, &Path::new("resources"))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// `TestValidatePodResourceConsistency` (validation_test.go:20568).
    #[test]
    fn consistency_rules() {
        let c = |r: serde_json::Value| json!({"name": "c", "image": "i", "resources": r});
        // Aggregate requests above pod requests: two errors (cpu, memory).
        let s = with_containers(
            json!({"requests": {"cpu": "10", "memory": "10Mi"}}),
            json!([
                c(json!({"requests": {"cpu": "6", "memory": "5Mi"}})),
                c(json!({"requests": {"cpu": "8", "memory": "7Mi"}}))
            ]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs
            .iter()
            .all(|e| e.contains("must be greater than or equal to aggregate container requests")));
        // Equal is fine.
        let s = with_containers(
            json!({"requests": {"cpu": "10"}}),
            json!([
                c(json!({"requests": {"cpu": "5"}})),
                c(json!({"requests": {"cpu": "5"}}))
            ]),
        );
        assert!(consistency(&s).is_empty());
        // Resource unsupported at pod level (storage) in containers: ignored.
        let s = with_containers(
            json!({"requests": {"cpu": "10"}}),
            json!([c(json!({"requests": {"cpu": "6", "storage": "10Gi"}}))]),
        );
        assert!(consistency(&s).is_empty());
        // Aggregate cpu limits above the pod limit are NOT an error (only
        // hugepages aggregate); each individual limit is checked instead.
        let s = with_containers(
            json!({"limits": {"cpu": "10"}}),
            json!([
                c(json!({"limits": {"cpu": "5"}})),
                c(json!({"limits": {"cpu": "6"}}))
            ]),
        );
        assert!(consistency(&s).is_empty());
        // Hugepage aggregate limits above pod limits.
        let s = with_containers(
            json!({"limits": {"cpu": "10", "hugepages-2Mi": "4Mi"}}),
            json!([
                c(json!({"limits": {"cpu": "6", "hugepages-2Mi": "4Mi"}})),
                c(json!({"limits": {"cpu": "6", "hugepages-2Mi": "4Mi"}}))
            ]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("must be greater than or equal to aggregate container limits"));
        // Individual container limit above the pod limit.
        let s = with_containers(
            json!({"limits": {"cpu": "10"}}),
            json!([c(json!({"limits": {"cpu": "11"}}))]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("must be less than or equal to pod limits of 10"));
    }

    /// "bad OS for PodLevelResources hides bad cpu limit" (:10633): the
    /// windows Forbidden replaces every other pod-level error.
    #[test]
    fn windows_hides_consistency_errors() {
        let s = spec(json!({
            "containers": [{"name": "c", "image": "i",
                "resources": {"limits": {"cpu": "200m"}}}],
            "resources": {"limits": {"cpu": "100m"}},
            "os": {"name": "windows"},
        }));
        assert_eq!(
            run(&s),
            vec!["spec.resources: Forbidden: may not be set for a windows pod"]
        );
    }

    #[test]
    fn claim_names_not_found() {
        let errs = validate_resource_claim_names(
            &[ResourceClaim {
                name: "x".into(),
                request: None,
            }],
            &BTreeSet::new(),
            &Path::new("claims"),
        );
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must be one of the names in pod.spec.resourceClaims which is empty"));
    }
}
