//! Unit tests moved from `src/registry/resource/devicetaintrule.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::resources::devicetaintrule::{
        DeviceTaintRule, DeviceTaintRuleStatus,
    };
    pub use rusternetes_common::validation::devicetaintrule::{
        validate_device_taint_rule, validate_device_taint_rule_status_update,
        validate_device_taint_rule_update,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::resource::devicetaintrule::*;
    use rusternetes_common::resources::dra::{DeviceTaint, DeviceTaintEffect};
    use rusternetes_common::types::Condition;

    fn paths(rf: &ResetFields) -> Vec<Vec<String>> {
        let mut v = rf.for_version(API_VERSION).to_vec();
        v.sort();
        v
    }

    fn base() -> DeviceTaintRule {
        let mut r = DeviceTaintRule::default();
        r.metadata.name = "valid-patch".into();
        r.metadata.generation = Some(1);
        r.spec.taint = DeviceTaint {
            key: "example.com/tainted".into(),
            effect: Some(DeviceTaintEffect::NoExecute),
            ..Default::default()
        };
        r
    }

    fn with_status() -> DeviceTaintRule {
        let mut r = base();
        r.status.conditions.push(Condition {
            condition_type: "foo".into(),
            status: "False".into(),
            observed_generation: None,
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("something".into()),
            message: Some("else".into()),
        });
        r
    }

    // strategy_test.go TestDeviceTaintRuleStrategy
    #[test]
    fn strategy_flags() {
        assert!(!NamespaceScopedStrategy::namespace_scoped(&Strategy));
        assert!(!RestUpdateStrategy::<DeviceTaintRule>::allow_create_on_update(&Strategy));
        assert!(RestUpdateStrategy::<DeviceTaintRule>::allow_unconditional_update(&Strategy));
    }

    // strategy_test.go TestDeviceTaintRuleStrategyCreate: drop-status, set-generation
    #[test]
    fn create_drops_status_and_sets_generation() {
        let ctx = RequestContext::new(None);
        let mut obj = with_status();
        obj.metadata.generation = Some(42);
        RestCreateStrategy::prepare_for_create(&Strategy, &ctx, &mut obj);
        assert_eq!(obj, base());
    }

    // strategy_test.go TestDeviceTaintRuleStrategyUpdate: drop-status, bump-generation
    #[test]
    fn update_keeps_status_and_bumps_generation() {
        let ctx = RequestContext::new(None);
        let old = base();
        let mut new = with_status();
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut new, &old);
        assert_eq!(new, base());

        let mut new = base();
        new.spec.taint.effect = Some(DeviceTaintEffect::None);
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut new, &old);
        assert_eq!(new.metadata.generation, Some(2));
    }

    // strategy_test.go TestStatusStrategyUpdate: drop-meta-changes, drop-spec
    #[test]
    fn status_update_drops_spec_and_meta_changes() {
        let ctx = RequestContext::new(None);
        let old = base();
        let mut new = base();
        new.metadata.finalizers = Some(vec!["foo".into()]);
        new.metadata.annotations = Some([("foo".to_string(), "bar".to_string())].into());
        new.metadata.labels = Some([("foo".to_string(), "bar".to_string())].into());
        new.spec.taint.effect = Some(DeviceTaintEffect::None);
        RestUpdateStrategy::prepare_for_update(&StatusStrategy, &ctx, &mut new, &old);
        assert_eq!(new, base());
    }

    // strategy.go:48-57 and :118-127
    #[test]
    fn reset_fields() {
        let main = RestUpdateStrategy::<DeviceTaintRule>::get_reset_fields(&Strategy);
        assert_eq!(paths(&main), vec![vec!["status".to_string()]]);
        let status = RestUpdateStrategy::<DeviceTaintRule>::get_reset_fields(&StatusStrategy);
        assert_eq!(
            paths(&status),
            vec![vec!["metadata".to_string()], vec!["spec".to_string()]]
        );
    }

    // defaults.go:31-35; defaults_test.go: an explicit timeAdded is kept.
    #[test]
    fn time_added_defaulted_only_when_absent() {
        let mut r = base();
        convert_to_internal(&mut r);
        let t = r.spec.taint.time_added.expect("defaulted");
        assert_eq!(t.timestamp_subsec_nanos(), 0);
        let fixed = chrono::DateTime::from_timestamp(1_700_000_000, 0);
        r.spec.taint.time_added = fixed;
        convert_to_internal(&mut r);
        assert_eq!(r.spec.taint.time_added, fixed);
    }
}
