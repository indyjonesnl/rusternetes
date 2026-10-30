//! Unit tests moved from `src/validation/runtimeclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::pod::Toleration;
    pub use rusternetes_common::resources::runtimeclass::{Overhead, Scheduling};
    pub use rusternetes_common::resources::RuntimeClass;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_qualified_name, validate_labels,
    };
    pub use rusternetes_common::validation::pod::validate_tolerations;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::Toleration;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::runtimeclass::*;
    use std::collections::HashMap;

    fn tol(key: &str, op: &str, val: &str, effect: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some(op.to_string()),
            value: Some(val.to_string()),
            effect: Some(effect.to_string()),
            toleration_seconds: None,
        }
    }

    fn overhead_with(pairs: &[(&str, &str)]) -> Overhead {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        Overhead { pod_fixed: Some(m) }
    }

    // ---- overhead.podFixed -------------------------------------------------

    #[test]
    fn overhead_valid_standard_resources() {
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("cpu", "250m"), ("memory", "128Mi")]));
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn overhead_valid_extended_integer_resource() {
        // Extended resource with whole-number quantity is OK.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("example.com/gpu", "2")]));
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn overhead_rejects_fractional_extended_resource() {
        // Extended (integer) resources must not be fractional.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("example.com/gpu", "1500m")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "overhead.podFixed[example.com/gpu]"
                    && e.detail == "must be an integer"),
            "expected integer error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_unprefixed_nonstandard_resource() {
        // A bare (unprefixed) name that is not a standard resource is rejected.
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[("widgets", "1")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed[widgets]"
                && e.detail == "must be a standard resource type or fully qualified"),
            "expected standard-resource error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_negative_quantity() {
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[("memory", "-1")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed[memory]"
                && e.detail == "must be greater than or equal to 0"),
            "expected non-negative error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_unparseable_quantity() {
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("cpu", "not-a-number")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "overhead.podFixed[cpu]" && e.error_type == ErrorType::Invalid),
            "expected parse error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_hugepages_require_cpu_or_memory() {
        // hugepages-2Mi without cpu/memory → forbidden.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("hugepages-2Mi", "2Mi")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed"
                && e.error_type == ErrorType::Forbidden
                && e.detail == "HugePages require cpu or memory"),
            "expected hugepages-forbidden error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_hugepages_with_memory_ok() {
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[
            ("hugepages-2Mi", "2Mi"),
            ("memory", "64Mi"),
        ]));
        let errs = validate_runtime_class(&rc);
        assert!(
            !errs
                .iter()
                .any(|e| e.detail == "HugePages require cpu or memory"),
            "did not expect hugepages-forbidden error, got {errs:?}"
        );
    }

    // ---- scheduling.tolerations uniqueness --------------------------------

    #[test]
    fn tolerations_unique_ok() {
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![
                tol("k1", "Equal", "v1", "NoSchedule"),
                tol("k2", "Equal", "v2", "NoSchedule"),
            ]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn tolerations_duplicate_rejected() {
        let dup = tol("k1", "Equal", "v1", "NoSchedule");
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![dup.clone(), dup]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "scheduling.tolerations[1]"
                    && e.error_type == ErrorType::Duplicate),
            "expected duplicate error at index 1, got {errs:?}"
        );
    }

    #[test]
    fn tolerations_differ_by_effect_not_duplicate() {
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![
                tol("k1", "Equal", "v1", "NoSchedule"),
                tol("k1", "Equal", "v1", "NoExecute"),
            ]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            !errs.iter().any(|e| e.error_type == ErrorType::Duplicate),
            "did not expect duplicate error, got {errs:?}"
        );
    }

    #[test]
    fn tolerations_duplicate_ignores_toleration_seconds() {
        // tolerationSeconds is NOT part of the list-key, so two NoExecute
        // tolerations differing only in tolerationSeconds are duplicates.
        let mut a = tol("k1", "Equal", "v1", "NoExecute");
        a.toleration_seconds = Some(10);
        let mut b = tol("k1", "Equal", "v1", "NoExecute");
        b.toleration_seconds = Some(20);
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![a, b]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.error_type == ErrorType::Duplicate),
            "expected duplicate error ignoring tolerationSeconds, got {errs:?}"
        );
    }
}
