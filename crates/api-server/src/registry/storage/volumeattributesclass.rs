//! VolumeAttributesClass strategy and storage — port of
//! `pkg/registry/storage/volumeattributesclass/strategy.go` and
//! `pkg/registry/storage/volumeattributesclass/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::csi::VolumeAttributesClass;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::volumeattributesclass::{
    validate_volume_attributes_class, validate_volume_attributes_class_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `volumeAttributesClassStrategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<VolumeAttributesClass> for Strategy {
    /// `PrepareForCreate` does nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut VolumeAttributesClass) {}

    fn validate(&self, _ctx: &RequestContext, obj: &VolumeAttributesClass) -> ErrorList {
        validate_volume_attributes_class(obj)
    }
}

impl RestUpdateStrategy<VolumeAttributesClass> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` does nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut VolumeAttributesClass,
        _old: &VolumeAttributesClass,
    ) {
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &VolumeAttributesClass,
        old: &VolumeAttributesClass,
    ) -> ErrorList {
        validate_volume_attributes_class_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `volumeAttributesClassStrategy` implements no
/// `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<VolumeAttributesClass> for Strategy {}

/// `NewREST` (storage/storage.go): the VolumeAttributesClass store, which
/// sets `ReturnDeletedObject: true`. storage/v1 registers no defaulter for it.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<VolumeAttributesClass, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "volumeattributesclasses"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}
