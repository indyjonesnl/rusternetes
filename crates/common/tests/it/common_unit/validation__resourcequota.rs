//! Unit tests moved from `src/validation/resourcequota.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::policy::{ResourceQuota, ResourceQuotaSpec};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_qualified_name;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use std::collections::HashMap;
}

mod scope_selector_tests {
    use super::__parent::*;
    use rusternetes_common::validation::resourcequota::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let spec: ResourceQuotaSpec = serde_json::from_value(json).unwrap();
        validate_resource_quota_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn priorityclass_in_with_values_passes() {
        assert!(errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "PriorityClass", "operator": "In", "values": ["high"]}
            ]}
        }))
        .is_empty());
    }

    #[test]
    fn besteffort_must_use_exists() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "BestEffort", "operator": "In", "values": ["x"]}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("must be 'Exists'")), "{e:?}");
    }

    #[test]
    fn in_operator_requires_values() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "PriorityClass", "operator": "In"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("at least one value")), "{e:?}");
    }

    #[test]
    fn exists_rejects_values() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "Terminating", "operator": "Exists", "values": ["x"]}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("no value")), "{e:?}");
    }

    #[test]
    fn bad_scope_and_operator_rejected() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "Nope", "operator": "Weird"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("unsupported scope")), "{e:?}");
        assert!(
            e.iter()
                .any(|m| m.contains("not a valid selector operator")),
            "{e:?}"
        );
    }

    #[test]
    fn conflicting_scope_pair_rejected() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "BestEffort", "operator": "Exists"},
                {"scopeName": "NotBestEffort", "operator": "Exists"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("conflicting scopes")), "{e:?}");
    }
}

mod hard_resource_tests {
    use super::__parent::*;
    use rusternetes_common::validation::resourcequota::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let spec: ResourceQuotaSpec = serde_json::from_value(json).unwrap();
        validate_resource_quota_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn standard_quota_resource_passes() {
        assert!(errs(
            serde_json::json!({ "hard": { "pods": "10", "requests.cpu": "2", "memory": "1Gi" } })
        )
        .is_empty());
    }

    #[test]
    fn hugepages_quota_resource_passes() {
        assert!(errs(serde_json::json!({ "hard": {
                "hugepages-2Mi": "100Mi",
                "requests.hugepages-2Mi": "100Mi"
            } }))
        .is_empty());
    }

    #[test]
    fn fully_qualified_resource_passes() {
        // A "/"-prefixed name is not standard-quota-checked, only qualified.
        assert!(
            errs(serde_json::json!({ "hard": { "example.com/dongle": "4" } })).is_empty(),
            "{:?}",
            errs(serde_json::json!({ "hard": { "example.com/dongle": "4" } }))
        );
    }

    #[test]
    fn unprefixed_nonstandard_resource_rejected() {
        let e = errs(serde_json::json!({ "hard": { "bananas": "10" } }));
        assert!(
            e.iter()
                .any(|m| m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn invalid_qualified_name_rejected() {
        let e = errs(serde_json::json!({ "hard": { "Bad Name!": "10" } }));
        assert!(!e.is_empty(), "expected a qualified-name error, got {e:?}");
        // A bad qualified name short-circuits the standard-resource check.
        assert!(
            !e.iter()
                .any(|m| m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn integer_resource_with_fraction_rejected() {
        let e = errs(serde_json::json!({ "hard": { "pods": "10.5" } }));
        assert!(e.iter().any(|m| m.contains("must be an integer")), "{e:?}");
    }

    #[test]
    fn integer_resource_with_milli_rejected() {
        // 500m of pods is 0.5 → MilliValue() % 1000 != 0.
        let e = errs(serde_json::json!({ "hard": { "services": "500m" } }));
        assert!(e.iter().any(|m| m.contains("must be an integer")), "{e:?}");
    }

    #[test]
    fn integer_resource_whole_value_passes() {
        assert!(errs(serde_json::json!({ "hard": { "pods": "10" } })).is_empty());
        // 1000m == 1, which is a whole integer.
        assert!(errs(serde_json::json!({ "hard": { "pods": "1000m" } })).is_empty());
    }

    #[test]
    fn fractional_cpu_allowed_not_integer_resource() {
        // cpu is not an integer-only resource; 500m is fine.
        assert!(errs(serde_json::json!({ "hard": { "requests.cpu": "500m" } })).is_empty());
    }

    #[test]
    fn negative_quantity_rejected() {
        let e = errs(serde_json::json!({ "hard": { "memory": "-1" } }));
        assert!(
            e.iter().any(|m| m.contains("greater than or equal to 0")),
            "{e:?}"
        );
    }

    // Note: a syntactically-invalid quantity (e.g. "notaquantity") is rejected
    // at deserialization by the custom Quantity deserializer, before this
    // validator runs — so the `Quantity::parse` Err arm is a backstop the API
    // path can't reach via `from_value` (not unit-tested here).
}

mod status_update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::resourcequota::*;

    fn rq(json: serde_json::Value) -> ResourceQuota {
        serde_json::from_value(json).unwrap()
    }

    fn msgs(new: &ResourceQuota, old: &ResourceQuota) -> Vec<String> {
        validate_resource_quota_status_update(new, old)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_status_update_passes() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "42" },
            "spec": {},
            "status": { "hard": { "pods": "10" }, "used": { "pods": "3" } }
        }));
        let old = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "41" },
            "spec": {}
        }));
        assert!(msgs(&new, &old).is_empty(), "{:?}", msgs(&new, &old));
    }

    #[test]
    fn missing_resource_version_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q" },
            "spec": {},
            "status": { "hard": { "pods": "10" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter()
                .any(|m| m.contains("resourceVersion") && m.contains("Required value")),
            "{e:?}"
        );
    }

    #[test]
    fn status_used_bad_resource_name_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "7" },
            "spec": {},
            "status": { "used": { "bananas": "1" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter().any(|m| m.contains("status.used")
                && m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn status_hard_integer_fraction_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "7" },
            "spec": {},
            "status": { "hard": { "pods": "2500m" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter()
                .any(|m| m.contains("status.hard") && m.contains("must be an integer")),
            "{e:?}"
        );
    }
}
