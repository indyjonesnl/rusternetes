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

pub const API_VERSION: &str = "resource.k8s.io/v1alpha3";

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
