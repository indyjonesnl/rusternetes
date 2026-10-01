//! VolumeSnapshot strategies and storage — the custom-resource strategy
//! (`customresource/strategy.go`) and status strategy
//! (`customresource/status_strategy.go`) of the apiextensions-apiserver, for
//! the external-snapshotter CRD `snapshot.storage.k8s.io_volumesnapshots.yaml`
//! (namespaced, `subresources: status: {}`).

use std::sync::Arc;

use rusternetes_common::resources::VolumeSnapshot;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::volumesnapshot::{
    validate_volume_snapshot, validate_volume_snapshot_update,
};
use rusternetes_storage::StorageBackend;

use super::{same_json, GROUP};
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// `customResourceStrategy` for VolumeSnapshot.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<VolumeSnapshot> for Strategy {
    /// `PrepareForCreate` (strategy.go:117-129): the CRD has a status
    /// subresource, so "create cannot set status"; the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut VolumeSnapshot) {
        obj.status = None;
        obj.metadata.generation = Some(1);
    }

    /// `customResourceValidator.Validate` (validator.go:53): ObjectMeta with
    /// `NameIsDNSSubdomain`, then the schema.
    fn validate(&self, _ctx: &RequestContext, obj: &VolumeSnapshot) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            true,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_volume_snapshot(obj));
        errs
    }
}

impl RestUpdateStrategy<VolumeSnapshot> for Strategy {
    /// `AllowCreateOnUpdate` is false for custom resources (strategy.go:262).
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:132-165): the status is the stored one;
    /// "except for the changes to `metadata`, any other changes cause the
    /// generation to increment" — with the status pinned, that is the spec.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshot,
        old: &VolumeSnapshot,
    ) {
        obj.status = old.status.clone();
        if !same_json(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &VolumeSnapshot,
        old: &VolumeSnapshot,
    ) -> ErrorList {
        validate_volume_snapshot_update(obj, old)
    }

    /// `AllowUnconditionalUpdate` is false for custom resources
    /// (strategy.go:267-269).
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<VolumeSnapshot> for Strategy {}

/// `statusStrategy` (status_strategy.go): an update writes only the status.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<VolumeSnapshot> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (status_strategy.go:66-91): the stored object, with
    /// the request's status.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshot,
        old: &VolumeSnapshot,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `ValidateUpdate` (status_strategy.go:93-140) validates the status
    /// schema; the typed status has nothing left to reject after decoding.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        _obj: &VolumeSnapshot,
        _old: &VolumeSnapshot,
    ) -> ErrorList {
        Vec::new()
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// The main store and the `status` store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<VolumeSnapshot, StorageBackend>,
    Store<VolumeSnapshot, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new(GROUP, "volumesnapshots"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
