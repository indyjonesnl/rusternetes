//! CSIStorageCapacity strategy and storage — port of
//! `pkg/registry/storage/csistoragecapacity/strategy.go` and
//! `pkg/registry/storage/csistoragecapacity/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::CSIStorageCapacity;
use rusternetes_common::validation::csistoragecapacity::{
    get_warnings_for_csi_storage_capacity, has_invalid_label_value_in_label_selector,
    validate_csi_storage_capacity, validate_csi_storage_capacity_update,
    CsiStorageCapacityValidateOptions,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `ValidateCSIStorageCapacity` (validation.go:595-606): ObjectMeta with
/// `ValidateStorageCapacityName` (`NameIsDNSSubdomain`), then the fields.
fn validate(obj: &CSIStorageCapacity, opts: CsiStorageCapacityValidateOptions) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_csi_storage_capacity(obj, opts));
    errs
}

/// `csiStorageCapacityStrategy` (strategy.go:32-45).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<CSIStorageCapacity> for Strategy {
    /// `PrepareForCreate` is a NOP.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut CSIStorageCapacity) {}

    fn validate(&self, _ctx: &RequestContext, obj: &CSIStorageCapacity) -> ErrorList {
        validate(obj, CsiStorageCapacityValidateOptions::default())
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &CSIStorageCapacity) -> Vec<String> {
        get_warnings_for_csi_storage_capacity(obj)
    }
}

impl RestUpdateStrategy<CSIStorageCapacity> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` is a NOP.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut CSIStorageCapacity,
        _old: &CSIStorageCapacity,
    ) {
    }

    /// `ValidateUpdate` (strategy.go:76-83): an old object that already
    /// carries an invalid label value keeps its selector updatable.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CSIStorageCapacity,
        old: &CSIStorageCapacity,
    ) -> ErrorList {
        let opts = CsiStorageCapacityValidateOptions {
            allow_invalid_label_value_in_selector: has_invalid_label_value_in_label_selector(old),
        };
        let mut errs = validate(obj, opts);
        errs.extend(validate_csi_storage_capacity_update(obj, old));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &CSIStorageCapacity,
        _old: &CSIStorageCapacity,
    ) -> Vec<String> {
        get_warnings_for_csi_storage_capacity(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `csiStorageCapacityStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<CSIStorageCapacity> for Strategy {}

/// `NewREST` (storage/storage.go): the store does NOT set
/// `ReturnDeletedObject`, so a delete answers with a Status.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<CSIStorageCapacity, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "csistoragecapacities"),
        Arc::new(Strategy),
    )
}
