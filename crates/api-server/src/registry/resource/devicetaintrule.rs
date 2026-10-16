//! DeviceTaintRule strategy and storage — port of
//! `pkg/registry/resource/devicetaintrule/strategy.go` (`deviceTaintRuleStrategy`
//! :37-109, `deviceTaintRuleStatusStrategy` :111-148) and
//! `storage/storage.go` (`NewREST` :37-70).
//!
//! `GetResetFields` is keyed `resource.k8s.io/v1alpha3` (strategy.go:48-57,
//! :118-127): the main strategy resets `status`, `/status` resets `metadata`
//! and `spec`, so server-side apply never lets a manager own them.

use std::sync::Arc;

use rusternetes_common::resources::devicetaintrule::{DeviceTaintRule, DeviceTaintRuleStatus};
use rusternetes_common::validation::devicetaintrule::{
    validate_device_taint_rule, validate_device_taint_rule_status_update,
    validate_device_taint_rule_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::ssa::ResetFields;

const API_VERSION: &str = "resource.k8s.io/v1alpha3";

/// The v1alpha3 defaulting a decoded DeviceTaintRule goes through:
/// `SetDefaults_DeviceTaint` (`pkg/apis/resource/v1alpha3/defaults.go:31-35`)
/// sets `timeAdded` to the current time truncated to the second.
pub fn convert_to_internal(rule: &mut DeviceTaintRule) {
    if rule.spec.taint.time_added.is_none() {
        let now = chrono::Utc::now();
        rule.spec.taint.time_added = chrono::DateTime::from_timestamp(now.timestamp(), 0);
    }
}

/// `deviceTaintRuleStrategy` (strategy.go:37-46), `names.SimpleNameGenerator`.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:48-50.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<DeviceTaintRule> for Strategy {
    /// `PrepareForCreate` (strategy.go:59-64): a client cannot set the status
    /// and the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut DeviceTaintRule) {
        obj.status = DeviceTaintRuleStatus::default();
        obj.metadata.generation = Some(1);
    }

    /// `Validate` (strategy.go:66-69).
    fn validate(&self, _ctx: &RequestContext, obj: &DeviceTaintRule) -> ErrorList {
        validate_device_taint_rule(obj)
    }
}

impl RestUpdateStrategy<DeviceTaintRule> for Strategy {
    /// strategy.go:79-81.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:83-91): the status is kept and any
    /// change to the spec increments the generation (`apiequality.Semantic.
    /// DeepEqual`; the spec has no quantities or times that compare unequal
    /// while serialising equal, so JSON equality is the same test).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut DeviceTaintRule,
        old: &DeviceTaintRule,
    ) {
        obj.status = old.status.clone();
        if serde_json::to_value(&obj.spec).ok() != serde_json::to_value(&old.spec).ok() {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateUpdate` (strategy.go:93-95).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &DeviceTaintRule,
        old: &DeviceTaintRule,
    ) -> ErrorList {
        validate_device_taint_rule_update(obj, old)
    }

    /// strategy.go:101-103: `true`.
    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:48-57): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["status"]])
    }
}

impl RestDeleteStrategy<DeviceTaintRule> for Strategy {}

/// `deviceTaintRuleStatusStrategy` (strategy.go:111-148): the update strategy
/// of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<DeviceTaintRule> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `statusStrategy.PrepareForUpdate` (strategy.go:129-134): `/status`
    /// keeps the stored spec and the metadata a status update cannot change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut DeviceTaintRule,
        old: &DeviceTaintRule,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `statusStrategy.ValidateUpdate` (strategy.go:136-140).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &DeviceTaintRule,
        old: &DeviceTaintRule,
    ) -> ErrorList {
        validate_device_taint_rule_status_update(obj, old)
    }

    /// Inherited from the embedded `deviceTaintRuleStrategy` (strategy.go:101-103).
    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:118-127): `metadata` and `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["metadata"], &["spec"]])
    }
}

/// `NewREST` (storage/storage.go:37-70): the main and `/status` stores;
/// `ReturnDeletedObject: true`.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<DeviceTaintRule, StorageBackend>,
    Store<DeviceTaintRule, StorageBackend>,
) {
    let mut store = Store::new(
        storage,
        GroupResource::new("resource.k8s.io", "devicetaintrules"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}

#[cfg(test)]
mod tests {
    use super::*;
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
