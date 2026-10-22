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

const API_VERSION: &str = "internal.apiserver.k8s.io/v1alpha1";

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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::storageversion::ServerStorageVersion;

    fn paths(rf: &ResetFields) -> Vec<Vec<String>> {
        let mut v = rf.for_version(API_VERSION).to_vec();
        v.sort();
        v
    }

    fn ssv(id: &str, enc: &str) -> ServerStorageVersion {
        ServerStorageVersion {
            api_server_id: id.into(),
            encoding_version: enc.into(),
            decodable_versions: vec!["v1".into(), "v2".into()],
            served_versions: vec![],
        }
    }

    /// strategy_test.go `validStorageVersion`.
    fn valid_storage_version() -> StorageVersion {
        let mut sv = StorageVersion::default();
        sv.metadata.name = "core.pods".into();
        sv.status.storage_versions = vec![ssv("1", "v1"), ssv("2", "v1"), ssv("3", "v2")];
        sv
    }

    /// strategy_test.go `TestStorageVersionStrategy`.
    #[test]
    fn storage_version_strategy() {
        let ctx = RequestContext::new(None);
        assert!(!NamespaceScopedStrategy::namespace_scoped(&Strategy));
        assert!(!RestUpdateStrategy::<StorageVersion>::allow_create_on_update(&Strategy));

        let mut sv = valid_storage_version();
        RestCreateStrategy::prepare_for_create(&Strategy, &ctx, &mut sv);
        assert_eq!(sv.status, StorageVersionStatus::default());
        assert!(RestCreateStrategy::validate(&Strategy, &ctx, &sv).is_empty());

        let mut without_name = StorageVersion::default();
        let old = valid_storage_version();
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut without_name, &old);
        assert_eq!(without_name.status, old.status);
        assert!(
            RestUpdateStrategy::validate_update(&Strategy, &ctx, &without_name, &old).is_empty()
        );
    }

    /// strategy.go:105-107, :136-142.
    #[test]
    fn status_strategy_keeps_spec_and_meta() {
        let ctx = RequestContext::new(None);
        let mut old = valid_storage_version();
        old.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        let mut new = valid_storage_version();
        new.metadata.labels = None;
        RestUpdateStrategy::prepare_for_update(&StatusStrategy, &ctx, &mut new, &old);
        assert_eq!(new.metadata.labels, old.metadata.labels);
        assert!(!RestUpdateStrategy::<StorageVersion>::allow_unconditional_update(&Strategy));
        assert!(!RestUpdateStrategy::<StorageVersion>::allow_unconditional_update(&StatusStrategy));
        assert!(!NamespaceScopedStrategy::namespace_scoped(&StatusStrategy));
    }

    /// strategy.go:48-57 and :124-134.
    #[test]
    fn reset_fields() {
        let main = RestUpdateStrategy::<StorageVersion>::get_reset_fields(&Strategy);
        assert_eq!(paths(&main), vec![vec!["status".to_string()]]);
        let status = RestUpdateStrategy::<StorageVersion>::get_reset_fields(&StatusStrategy);
        assert_eq!(
            paths(&status),
            vec![vec!["metadata".to_string()], vec!["spec".to_string()]]
        );
    }
}
