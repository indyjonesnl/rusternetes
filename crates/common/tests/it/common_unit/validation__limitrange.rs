//! Unit tests moved from `src/validation/limitrange.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::policy::{LimitRange, LimitRangeItem};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use std::cmp::Ordering;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::policy::{LimitRange, LimitRangeItem, LimitRangeSpec};
    use rusternetes_common::validation::limitrange::*;

    fn map(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn item(item_type: &str) -> LimitRangeItem {
        LimitRangeItem {
            item_type: item_type.to_string(),
            max: None,
            min: None,
            default: None,
            default_request: None,
            max_limit_request_ratio: None,
        }
    }

    fn lr(items: Vec<LimitRangeItem>) -> LimitRange {
        LimitRange::new("lr", "default", LimitRangeSpec { limits: items })
    }

    /// Assert that some error's field-path ends with `suffix` and detail
    /// contains `needle`.
    fn has_err(errs: &ErrorList, suffix: &str, needle: &str) -> bool {
        errs.iter()
            .any(|e| e.field.ends_with(suffix) && e.detail.contains(needle))
    }

    #[test]
    fn limit_range_valid_container_ordering_passes() {
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.default_request = map(&[("cpu", "200m")]);
        it.default = map(&[("cpu", "300m")]);
        it.max = map(&[("cpu", "400m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_unknown_type_rejected() {
        let errs = validate_limit_range(&lr(vec![item("Bogus")]));
        assert!(has_err(&errs, "type", "supported"));
    }

    #[test]
    fn limit_range_duplicate_type_rejected() {
        use rusternetes_common::validation::field::ErrorType;
        let errs = validate_limit_range(&lr(vec![item("Container"), item("Container")]));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("type") && e.error_type == ErrorType::Duplicate),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_min_greater_than_max_rejected() {
        let mut it = item("Container");
        it.min = map(&[("cpu", "500m")]);
        it.max = map(&[("cpu", "200m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "min[cpu]",
            "min value 500m is greater than max value 200m"
        ));
    }

    #[test]
    fn limit_range_ratio_less_than_one_rejected() {
        let mut it = item("Container");
        it.max_limit_request_ratio = map(&[("cpu", "0.5")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "maxLimitRequestRatio[cpu]",
            "is less than 1"
        ));
    }

    #[test]
    fn limit_range_ratio_above_max_min_rejected() {
        // max/min = 400/100 = 4, ratio 5 exceeds it.
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.max = map(&[("cpu", "400m")]);
        it.max_limit_request_ratio = map(&[("cpu", "5")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(
            has_err(
                &errs,
                "maxLimitRequestRatio[cpu]",
                "is greater than max/min"
            ),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_ratio_within_max_min_passes() {
        // max/min = 400/100 = 4, ratio 4 is allowed (not strictly greater).
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.max = map(&[("cpu", "400m")]);
        it.max_limit_request_ratio = map(&[("cpu", "4")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_pvc_without_storage_rejected() {
        let mut it = item("PersistentVolumeClaim");
        it.min = map(&[("cpu", "1")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "limits",
            "either minimum or maximum storage value is required"
        ));
    }

    #[test]
    fn limit_range_pvc_with_storage_passes() {
        let mut it = item("PersistentVolumeClaim");
        it.max = map(&[("storage", "10Gi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_pod_with_default_rejected() {
        let mut it = item("Pod");
        it.default = map(&[("cpu", "100m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(&errs, "default", "may not be specified"));
    }

    #[test]
    fn limit_range_pod_with_default_request_rejected() {
        let mut it = item("Pod");
        it.default_request = map(&[("cpu", "100m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(&errs, "defaultRequest", "may not be specified"));
    }

    #[test]
    fn limit_range_non_overcommit_default_mismatch_rejected() {
        // hugepages-2Mi is non-overcommittable: default must equal defaultRequest.
        let mut it = item("Container");
        it.default = map(&[("hugepages-2Mi", "4Mi")]);
        it.default_request = map(&[("hugepages-2Mi", "2Mi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(
            has_err(&errs, "defaultRequest[hugepages-2Mi]", "must equal to"),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_non_overcommit_default_equal_passes() {
        let mut it = item("Container");
        it.default = map(&[("hugepages-2Mi", "2Mi")]);
        it.default_request = map(&[("hugepages-2Mi", "2Mi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_overcommit_resource_default_mismatch_allowed() {
        // cpu is overcommittable: default != defaultRequest is fine (as long as
        // ordering holds).
        let mut it = item("Container");
        it.default_request = map(&[("cpu", "100m")]);
        it.default = map(&[("cpu", "200m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }
}
