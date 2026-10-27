//! StorageVersionMigration strategy and storage — port of
//! `pkg/registry/storagemigration/storagemigration/strategy.go` (`strategy`
//! :43-112, `statusStrategy` :114-150) and `storage/storage.go` (`NewREST`
//! :48-84).
//!
//! `GetResetFields` is keyed `storagemigration.k8s.io/v1beta1` (strategy.go:55-63,
//! :119-128): the main strategy resets `status`, `/status` resets `metadata`
//! and `spec`, so server-side apply never lets a manager own them.

use std::sync::Arc;

use rusternetes_common::resources::storageversionmigration::{
    StorageVersionMigration, StorageVersionMigrationStatus,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::storageversionmigration::{
    validate_storage_version_migration, validate_storage_version_migration_status_update,
    validate_storage_version_migration_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::ssa::ResetFields;

const API_VERSION: &str = "storagemigration.k8s.io/v1beta1";

/// `strategy` (strategy.go:43-52), with `names.SimpleNameGenerator`.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:53-55.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<StorageVersionMigration> for Strategy {
    /// `PrepareForCreate` (strategy.go:66-70): clears the status.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut StorageVersionMigration) {
        obj.status = StorageVersionMigrationStatus::default();
    }

    /// `Validate` (strategy.go:80-83).
    fn validate(&self, _ctx: &RequestContext, obj: &StorageVersionMigration) -> ErrorList {
        validate_storage_version_migration(obj)
    }
}

impl RestUpdateStrategy<StorageVersionMigration> for Strategy {
    /// strategy.go:92-94.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:73-77): the status is kept.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut StorageVersionMigration,
        old: &StorageVersionMigration,
    ) {
        obj.status = old.status.clone();
    }

    /// `ValidateUpdate` (strategy.go:96-100).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageVersionMigration,
        old: &StorageVersionMigration,
    ) -> ErrorList {
        validate_storage_version_migration_update(obj, old)
    }

    /// strategy.go:106-108.
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (strategy.go:55-63): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["status"]])
    }
}

impl RestDeleteStrategy<StorageVersionMigration> for Strategy {}

/// `statusStrategy` (strategy.go:110-150): the update strategy of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<StorageVersionMigration> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `statusStrategy.PrepareForUpdate` (strategy.go:130-136): `/status`
    /// keeps the stored spec and the metadata a status update cannot change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut StorageVersionMigration,
        old: &StorageVersionMigration,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `statusStrategy.ValidateUpdate` (strategy.go:138-144).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageVersionMigration,
        old: &StorageVersionMigration,
    ) -> ErrorList {
        validate_storage_version_migration_status_update(obj, old)
    }

    /// Inherited from the embedded `strategy` (strategy.go:106-108).
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (strategy.go:119-128): `metadata` and `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["metadata"], &["spec"]])
    }
}

/// `NewREST` (storage/storage.go:48-84): the main and `/status` stores.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<StorageVersionMigration, StorageBackend>,
    Store<StorageVersionMigration, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("storagemigration.k8s.io", "storageversionmigrations"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::Condition;

    fn paths(rf: &ResetFields) -> Vec<Vec<String>> {
        let mut v = rf.for_version(API_VERSION).to_vec();
        v.sort();
        v
    }

    fn running() -> Condition {
        Condition {
            condition_type: "Running".into(),
            status: "True".into(),
            observed_generation: None,
            last_transition_time: None,
            reason: Some("R".into()),
            message: None,
        }
    }

    /// strategy.go:66-70: a client cannot create a migration with a status.
    #[test]
    fn create_clears_status() {
        let ctx = RequestContext::new(None);
        let mut obj = StorageVersionMigration::default();
        obj.status.resource_version = "5".into();
        obj.status.conditions.push(running());
        RestCreateStrategy::prepare_for_create(&Strategy, &ctx, &mut obj);
        assert_eq!(obj.status, StorageVersionMigrationStatus::default());
    }

    /// strategy.go:73-77 and :130-136.
    #[test]
    fn update_keeps_status_and_status_update_keeps_spec() {
        let ctx = RequestContext::new(None);
        let mut old = StorageVersionMigration::default();
        old.status.resource_version = "7".into();
        old.spec.resource.resource = "old".into();
        old.metadata.labels = Some([("a".to_string(), "b".to_string())].into());

        let mut new = StorageVersionMigration::default();
        new.status.resource_version = "8".into();
        new.spec.resource.resource = "new".into();
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut new, &old);
        assert_eq!(new.status.resource_version, "7");
        assert_eq!(new.spec.resource.resource, "new");

        let mut new = StorageVersionMigration::default();
        new.status.resource_version = "8".into();
        new.spec.resource.resource = "new".into();
        RestUpdateStrategy::prepare_for_update(&StatusStrategy, &ctx, &mut new, &old);
        assert_eq!(new.spec.resource.resource, "old");
        assert_eq!(new.status.resource_version, "8");
        assert_eq!(new.metadata.labels, old.metadata.labels);
    }

    /// strategy.go:92-94, :106-108, :53-55.
    #[test]
    fn strategy_flags() {
        assert!(!NamespaceScopedStrategy::namespace_scoped(&Strategy));
        assert!(!NamespaceScopedStrategy::namespace_scoped(&StatusStrategy));
        assert!(!RestUpdateStrategy::<StorageVersionMigration>::allow_create_on_update(&Strategy));
        assert!(
            !RestUpdateStrategy::<StorageVersionMigration>::allow_unconditional_update(&Strategy)
        );
        assert!(
            !RestUpdateStrategy::<StorageVersionMigration>::allow_unconditional_update(
                &StatusStrategy
            )
        );
    }

    /// strategy.go:55-63 and :119-128.
    #[test]
    fn reset_fields() {
        let main = RestUpdateStrategy::<StorageVersionMigration>::get_reset_fields(&Strategy);
        assert_eq!(paths(&main), vec![vec!["status".to_string()]]);
        let status =
            RestUpdateStrategy::<StorageVersionMigration>::get_reset_fields(&StatusStrategy);
        assert_eq!(
            paths(&status),
            vec![vec!["metadata".to_string()], vec!["spec".to_string()]]
        );
    }
}
