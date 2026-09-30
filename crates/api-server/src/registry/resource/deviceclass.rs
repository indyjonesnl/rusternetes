//! DeviceClass strategy and storage — port of
//! `pkg/registry/resource/deviceclass/strategy.go` and
//! `pkg/registry/resource/deviceclass/storage/storage.go`.
//!
//! `dropDisabledFields` clears `spec.extendedResourceName` while
//! `DRAExtendedResource` is off; `DeviceClassSpec` has no such field, so a
//! client-supplied one is already discarded on decode. The declarative
//! validation check that follows `ValidateDeviceClass` is not modelled.

use std::sync::Arc;

use rusternetes_common::resources::DeviceClass;
use rusternetes_common::validation::deviceclass::validate_device_class;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `ValidateDeviceClass` / `ValidateDeviceClassUpdate` (validation.go:546-567):
/// ObjectMeta with `validate.LongName`, then the spec.
fn validate_meta(obj: &DeviceClass) -> ErrorList {
    validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    )
}

/// `deviceClassStrategy` (strategy.go:37-46).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<DeviceClass> for Strategy {
    /// `Generation = 1` (strategy.go:52).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut DeviceClass) {
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &DeviceClass) -> ErrorList {
        let mut errs = validate_meta(obj);
        errs.extend(validate_device_class(obj));
        errs
    }
}

impl RestUpdateStrategy<DeviceClass> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Any change to the spec increments the generation (strategy.go:78-81).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut DeviceClass, old: &DeviceClass) {
        if serde_json::to_value(&obj.spec).ok() != serde_json::to_value(&old.spec).ok() {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateDeviceClassUpdate` (validation.go:558-567).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &DeviceClass,
        old: &DeviceClass,
    ) -> ErrorList {
        let mut errs = validate_meta(obj);
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &Path::new("metadata"),
        ));
        errs.extend(validate_device_class(obj));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<DeviceClass> for Strategy {}

/// `NewREST` (storage/storage.go:40-50): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<DeviceClass, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("resource.k8s.io", "deviceclasses"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}
