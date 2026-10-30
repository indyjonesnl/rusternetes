//! StorageVersion strategy and storage — port of
//! `pkg/registry/apiserverinternal/storageversion/strategy.go`
//! (`storageVersionStrategy` :33-115, `storageVersionStatusStrategy`
//! :117-150) and `storage/storage.go` (`NewREST` :39-68).
//!
//! `GetResetFields` is keyed `internal.apiserver.k8s.io/v1alpha1`
//! (strategy.go:48-57, :124-134): the main strategy resets `status`, `/status`
//! resets `metadata` and `spec`, so server-side apply never lets a manager own
//! them.

use std::sync::Arc;

use rusternetes_common::resources::storageversion::{StorageVersion, StorageVersionStatus};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::storageversion::{
    validate_storage_version, validate_storage_version_status_update,
    validate_storage_version_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::ssa::ResetFields;

pub const API_VERSION: &str = "internal.apiserver.k8s.io/v1alpha1";

/// `storageVersionStrategy` (strategy.go:33-41), `names.SimpleNameGenerator`.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:43-46: all StorageVersions are cluster scoped.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<StorageVersion> for Strategy {
    /// `PrepareForCreate` (strategy.go:60-63): clears the status.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut StorageVersion) {
        obj.status = StorageVersionStatus::default();
    }

    /// `Validate` (strategy.go:72-75).
    fn validate(&self, _ctx: &RequestContext, obj: &StorageVersion) -> ErrorList {
        validate_storage_version(obj)
    }
}

impl RestUpdateStrategy<StorageVersion> for Strategy {
    /// strategy.go:87-89: no create with a PUT.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:66-69): the status is kept.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut StorageVersion,
        old: &StorageVersion,
    ) {
        obj.status = old.status.clone();
    }

    /// `ValidateUpdate` (strategy.go:92-97).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageVersion,
        old: &StorageVersion,
    ) -> ErrorList {
        validate_storage_version_update(obj, old)
    }

    /// strategy.go:105-107: "Status update should only be allowed if
    /// version match."
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (strategy.go:48-57): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["status"]])
    }
}

impl RestDeleteStrategy<StorageVersion> for Strategy {}

/// `storageVersionStatusStrategy` (strategy.go:109-150): the update strategy
/// of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<StorageVersion> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `storageVersionStatusStrategy.PrepareForUpdate` (strategy.go:136-142):
    /// `/status` keeps the stored spec and the metadata a status update
    /// cannot change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut StorageVersion,
        old: &StorageVersion,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `storageVersionStatusStrategy.ValidateUpdate` (strategy.go:144-146).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageVersion,
        old: &StorageVersion,
    ) -> ErrorList {
        validate_storage_version_status_update(obj, old)
    }

    /// Inherited from the embedded `storageVersionStrategy` (strategy.go:105-107).
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (strategy.go:124-134): `metadata` and `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with(API_VERSION, &[&["metadata"], &["spec"]])
    }
}

/// `NewREST` (storage/storage.go:39-68): the main and `/status` stores.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<StorageVersion, StorageBackend>,
    Store<StorageVersion, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("internal.apiserver.k8s.io", "storageversions"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
