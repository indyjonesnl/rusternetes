//! Unit tests moved from `src/registry/apiserverinternal/storageversion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::resources::storageversion::{StorageVersion, StorageVersionStatus};
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::storageversion::{
        validate_storage_version, validate_storage_version_status_update,
        validate_storage_version_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiserverinternal::storageversion::*;
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
