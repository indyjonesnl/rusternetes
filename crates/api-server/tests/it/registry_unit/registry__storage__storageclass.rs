//! Unit tests moved from `src/registry/storage/storageclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeReclaimPolicy, VolumeBindingMode,
    };
    pub use rusternetes_common::resources::StorageClass;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::storageclass::{
        get_warnings_for_storage_class, validate_storage_class, validate_storage_class_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::storage::storageclass::*;

    fn sc(v: serde_json::Value) -> StorageClass {
        let mut base = serde_json::json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": "sc"}, "provisioner": "kubernetes.io/no-provisioner"
        });
        base.as_object_mut()
            .unwrap()
            .extend(v.as_object().unwrap().clone());
        let mut sc: StorageClass = serde_json::from_value(base).unwrap();
        convert_to_internal(&mut sc);
        sc
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn defaults_and_flags_match_upstream() {
        let obj = sc(serde_json::json!({}));
        assert_eq!(
            obj.reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Delete)
        );
        assert_eq!(obj.volume_binding_mode, Some(VolumeBindingMode::Immediate));
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
    }

    /// `ValidateStorageClassUpdate`: provisioner is immutable and the create
    /// validation runs again on update.
    #[test]
    fn update_forbids_a_provisioner_change() {
        let old = sc(serde_json::json!({}));
        let new = sc(serde_json::json!({"provisioner": "example.com/other"}));
        let errs = Strategy.validate_update(&ctx(), &new, &old);
        assert!(errs.iter().any(|e| e.field == "provisioner"), "{errs:?}");
    }

    /// `GetWarningsForStorageClass` (pkg/api/storage/util.go).
    #[test]
    fn a_deprecated_topology_key_warns() {
        let obj = sc(
            serde_json::json!({"allowedTopologies": [{"matchLabelExpressions": [
                {"key": "failure-domain.beta.kubernetes.io/zone", "values": ["a"]}
            ]}]}),
        );
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &obj),
            vec!["allowedTopologies[0].matchLabelExpressions[0].key: deprecated since v1.17; use \"topology.kubernetes.io/zone\" instead".to_string()]
        );
    }
}
