//! StorageClass strategy and storage — port of
//! `pkg/registry/storage/storageclass/strategy.go` and
//! `pkg/registry/storage/storageclass/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::volume::{PersistentVolumeReclaimPolicy, VolumeBindingMode};
use rusternetes_common::resources::StorageClass;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::storageclass::{
    get_warnings_for_storage_class, validate_storage_class, validate_storage_class_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// The v1 defaulting a decoded StorageClass goes through:
/// `SetDefaults_StorageClass` (pkg/apis/storage/v1/defaults.go:31-41).
pub fn convert_to_internal(sc: &mut StorageClass) {
    if sc.reclaim_policy.is_none() {
        sc.reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
    }
    if sc.volume_binding_mode.is_none() {
        sc.volume_binding_mode = Some(VolumeBindingMode::Immediate);
    }
}

/// `storageClassStrategy` (strategy.go:37-41).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<StorageClass> for Strategy {
    /// `PrepareForCreate` does nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut StorageClass) {}

    /// `Validate`: `ValidateStorageClass`. The declarative-validation
    /// migration check that follows it is not modelled.
    fn validate(&self, _ctx: &RequestContext, obj: &StorageClass) -> ErrorList {
        validate_storage_class(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &StorageClass) -> Vec<String> {
        get_warnings_for_storage_class(obj)
    }
}

impl RestUpdateStrategy<StorageClass> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` does nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut StorageClass,
        _old: &StorageClass,
    ) {
    }

    /// `ValidateUpdate`: `ValidateStorageClass` plus
    /// `ValidateStorageClassUpdate`.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageClass,
        old: &StorageClass,
    ) -> ErrorList {
        let mut errs = validate_storage_class(obj);
        errs.extend(validate_storage_class_update(obj, old));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &StorageClass,
        _old: &StorageClass,
    ) -> Vec<String> {
        get_warnings_for_storage_class(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `storageClassStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<StorageClass> for Strategy {}

/// `NewREST` (storage/storage.go): the StorageClass store, which sets
/// `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<StorageClass, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "storageclasses"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}

#[cfg(test)]
mod tests {
    use super::*;

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
