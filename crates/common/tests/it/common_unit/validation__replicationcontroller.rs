//! Unit tests moved from `src/validation/replicationcontroller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::workloads::{
        ReplicationController, ReplicationControllerSpec, ReplicationControllerStatus,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_nonnegative_field, validate_object_meta,
        validate_object_meta_update,
    };
    pub use rusternetes_common::validation::podtemplate::validate_pod_template_spec_with_old;
}

mod old_template_options_tests {
    use super::__parent::*;
    use rusternetes_common::validation::replicationcontroller::*;

    fn rc(search: &str) -> ReplicationController {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "r", "namespace": "d", "resourceVersion": "1"},
            "spec": {"replicas": 1, "selector": {"a": "b"},
                "template": {"metadata": {"labels": {"a": "b"}}, "spec": {
                    "containers": [{"name": "c", "image": "x"}],
                    "dnsConfig": {"searches": [search]}}}}
        }))
        .unwrap()
    }

    /// strategy.go:127 vs :160 (`GetValidationOptionsFromPodTemplate`).
    #[test]
    #[serial_test::serial]
    fn update_derives_pod_options_from_the_old_template() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::RelaxedDNSSearchValidation,
            false,
        );
        let relaxed = "my_search.example.com";
        assert!(!validate_replication_controller(&rc(relaxed)).is_empty());
        assert!(validate_replication_controller_update(&rc(relaxed), &rc(relaxed)).is_empty());
        assert!(
            !validate_replication_controller_update(&rc(relaxed), &rc("a.example.com")).is_empty()
        );
    }
}
