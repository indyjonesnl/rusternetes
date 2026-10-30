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

pub const API_VERSION: &str = "storagemigration.k8s.io/v1beta1";

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
