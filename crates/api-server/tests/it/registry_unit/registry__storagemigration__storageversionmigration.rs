//! Unit tests moved from `src/registry/storagemigration/storageversionmigration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::resources::storageversionmigration::{
        StorageVersionMigration, StorageVersionMigrationStatus,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::storageversionmigration::{
        validate_storage_version_migration, validate_storage_version_migration_status_update,
        validate_storage_version_migration_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::storagemigration::storageversionmigration::*;
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
