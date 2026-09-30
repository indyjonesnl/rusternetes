//! VolumeAttachment strategies and storage — port of
//! `pkg/registry/storage/volumeattachment/strategy.go` and
//! `pkg/registry/storage/volumeattachment/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::csi::VolumeAttachmentStatus;
use rusternetes_common::resources::VolumeAttachment;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::volumeattachment::{
    validate_volume_attachment, validate_volume_attachment_update, validate_volume_attachment_v1,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// A Go `storage.VolumeAttachmentStatus{}`: the status is not a pointer, so
/// it always serializes, with `attached: false`.
fn empty_status() -> Option<VolumeAttachmentStatus> {
    Some(VolumeAttachmentStatus {
        attached: false,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    })
}

/// `ValidateVolumeAttachment` (validation.go:141-146): ObjectMeta with
/// `ValidateClassName` (`NameIsDNSSubdomain`), then spec and status.
fn validate(obj: &VolumeAttachment) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_volume_attachment(obj));
    errs
}

/// `volumeAttachmentStrategy` (strategy.go:38-49).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<VolumeAttachment> for Strategy {
    /// `PrepareForCreate` clears the status.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut VolumeAttachment) {
        obj.status = empty_status();
    }

    /// `Validate`: `ValidateVolumeAttachment`, then the v1-only extras
    /// (strategy.go:72-79).
    fn validate(&self, _ctx: &RequestContext, obj: &VolumeAttachment) -> ErrorList {
        let mut errs = validate(obj);
        errs.extend(validate_volume_attachment_v1(obj));
        errs
    }
}

impl RestUpdateStrategy<VolumeAttachment> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// The status is not writable through the main resource; the spec is
    /// immutable so no generation bump (strategy.go:97-104).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeAttachment,
        old: &VolumeAttachment,
    ) {
        obj.status = old.status.clone().or_else(empty_status);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &VolumeAttachment,
        old: &VolumeAttachment,
    ) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_volume_attachment_update(obj, old));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<VolumeAttachment> for Strategy {}

/// `volumeAttachmentStatusStrategy` (strategy.go:131-165).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<VolumeAttachment> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Only status may change: the spec is reset and the metadata a status
    /// write cannot touch is kept. `MutableCSINodeAllocatableCount` is on, so
    /// error codes are not cleared.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut VolumeAttachment,
        old: &VolumeAttachment,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// The embedded `Strategy.ValidateUpdate`.
    fn validate_update(
        &self,
        ctx: &RequestContext,
        obj: &VolumeAttachment,
        old: &VolumeAttachment,
    ) -> ErrorList {
        Strategy.validate_update(ctx, obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewStorage` (storage/storage.go:41-72): the main store and the status
/// store; `ReturnDeletedObject: true`.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<VolumeAttachment, StorageBackend>,
    Store<VolumeAttachment, StorageBackend>,
) {
    let mut store = Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "volumeattachments"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
