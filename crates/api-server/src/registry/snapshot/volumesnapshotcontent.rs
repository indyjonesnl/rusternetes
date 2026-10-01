//! VolumeSnapshotContent strategies and storage — the custom-resource strategy
//! (`customresource/strategy.go`) and status strategy
//! (`customresource/status_strategy.go`) of the apiextensions-apiserver, for
//! the external-snapshotter CRD
//! `snapshot.storage.k8s.io_volumesnapshotcontents.yaml` (cluster-scoped,
//! `subresources: status: {}`).

use std::sync::Arc;

use rusternetes_common::resources::VolumeSnapshotContent;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::volumesnapshot::{
    validate_volume_snapshot_content, validate_volume_snapshot_content_update,
};
use rusternetes_storage::StorageBackend;

use super::{same_json, GROUP};
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// `customResourceStrategy` for VolumeSnapshotContent.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<VolumeSnapshotContent> for Strategy {
    /// `PrepareForCreate` (strategy.go:117-129): "create cannot set status";
    /// the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut VolumeSnapshotContent) {
        obj.status = None;
        obj.metadata.generation = Some(1);
    }

    /// `customResourceValidator.Validate` (validator.go:53).
    fn validate(&self, _ctx: &RequestContext, obj: &VolumeSnapshotContent) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_volume_snapshot_content(obj));
        errs
    }
}

impl RestUpdateStrategy<VolumeSnapshotContent> for Strategy {
    /// `AllowCreateOnUpdate` is false for custom resources (strategy.go:262).
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:132-165): the status is the stored one;
    /// a change to anything but metadata (here the spec) bumps the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshotContent,
        old: &VolumeSnapshotContent,
    ) {
        obj.status = old.status.clone();
        if !same_json(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &VolumeSnapshotContent,
        old: &VolumeSnapshotContent,
    ) -> ErrorList {
        validate_volume_snapshot_content_update(obj, old)
    }

    /// `AllowUnconditionalUpdate` is false for custom resources
    /// (strategy.go:267-269).
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<VolumeSnapshotContent> for Strategy {}

/// `statusStrategy` (status_strategy.go): an update writes only the status.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<VolumeSnapshotContent> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (status_strategy.go:66-91).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeSnapshotContent,
        old: &VolumeSnapshotContent,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `ValidateUpdate` (status_strategy.go:93-140) validates the status
    /// schema; the typed status has nothing left to reject after decoding.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        _obj: &VolumeSnapshotContent,
        _old: &VolumeSnapshotContent,
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
    Store<VolumeSnapshotContent, StorageBackend>,
    Store<VolumeSnapshotContent, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new(GROUP, "volumesnapshotcontents"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
