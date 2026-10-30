//! VolumeAttachment validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateVolumeAttachment`
//! and the v1-only extra checks in `ValidateVolumeAttachmentV1` (release-1.35).
//!
//! Covers `spec.attacher` (required + valid CSI driver name), `spec.source`
//! (exactly one of `persistentVolumeName` / `inlineVolumeSpec`; PV name
//! non-empty and a valid DNS-subdomain PV name), the full `inlineVolumeSpec`
//! PersistentVolumeSpec (via the shared PV-spec validator), `spec.nodeName`
//! (DNS-subdomain node name) and the whole `status` block (attachmentMetadata
//! total size, attach/detach error message length + non-negative errorCode).
//! ObjectMeta is validated separately. CSI is a non-negotiable contract.

use crate::resources::csi::{VolumeAttachment, VolumeAttachmentStatus, VolumeError};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::persistentvolume::validate_persistent_volume_spec;

/// `maxAttachedVolumeMetadataSize` — 256 kB (upstream validation.go:44).
const MAX_ATTACHED_VOLUME_METADATA_SIZE: usize = 256 * (1 << 10);
/// `maxVolumeErrorMessageSize` — 1024 (upstream validation.go:45).
const MAX_VOLUME_ERROR_MESSAGE_SIZE: usize = 1024;

/// `ValidateVolumeAttachmentV1` (validation.go:150-160): the checks only
/// newly created v1 attachments get — a valid CSI driver name for the
/// attacher and a valid PV name.
pub fn validate_volume_attachment_v1(va: &VolumeAttachment) -> ErrorList {
    let mut errs = validate_csi_driver_name(&va.spec.attacher, &Path::new("spec.attacher"));
    if let Some(pv) = &va.spec.source.persistent_volume_name {
        for msg in is_dns1123_subdomain(pv) {
            errs.push(Error::invalid(
                &Path::new("spec.source.persistentVolumeName"),
                pv.clone(),
                msg,
            ));
        }
    }
    errs
}

/// Validate a `VolumeAttachment`. Mirrors upstream `ValidateVolumeAttachment`
/// minus ObjectMeta (validateVolumeAttachmentSpec + status).
pub fn validate_volume_attachment(va: &VolumeAttachment) -> ErrorList {
    let spec = &va.spec;
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();

    // attacher — required (validateAttacher, validation.go:174-180).
    if spec.attacher.is_empty() {
        errs.push(Error::required(&spec_path.child("attacher"), ""));
    }

    // source — exactly one of inlineVolumeSpec / persistentVolumeName.
    let source = &spec.source;
    let source_path = spec_path.child("source");
    match (&source.inline_volume_spec, &source.persistent_volume_name) {
        (None, None) => errs.push(Error::required(
            &source_path,
            "must specify exactly one of inlineVolumeSpec and persistentVolumeName",
        )),
        (Some(_), Some(_)) => errs.push(Error::forbidden(
            &source_path,
            "must specify exactly one of inlineVolumeSpec and persistentVolumeName",
        )),
        (None, Some(pv)) if pv.is_empty() => errs.push(Error::required(
            &source_path.child("persistentVolumeName"),
            "must specify non empty persistentVolumeName",
        )),
        // inlineVolumeSpec set: run the full PersistentVolumeSpec validator with
        // inline=true (upstream `validateVolumeAttachmentSource` passes
        // `validateInlinePersistentVolumeSpec=true`), adding the inline-only
        // deltas: claimRef/capacity forbidden, CSI required, reclaimPolicy must
        // be Retain, nodeAffinity/storageClassName forbidden, volumeMode must be
        // Filesystem.
        (Some(inline), None) => {
            errs.extend(validate_persistent_volume_spec(
                inline,
                // Upstream passes an empty pvName here (`pkg/apis/storage/
                // validation/validation.go:197`) — an inline spec has no object
                // name for the iscsi `<name>:<targetPortal>` length bound.
                "",
                true,
                &source_path.child("inlineVolumeSpec"),
            ));
        }
        _ => {}
    }

    // nodeName — a DNS-subdomain node name (also rejects empty).
    for msg in is_dns1123_subdomain(&spec.node_name) {
        errs.push(Error::invalid(
            &spec_path.child("nodeName"),
            spec.node_name.clone(),
            msg,
        ));
    }

    // status — attachmentMetadata size + attach/detach error checks
    // (validateVolumeAttachmentStatus :212-250).
    if let Some(status) = &va.status {
        errs.extend(validate_volume_attachment_status(
            status,
            &Path::new("status"),
        ));
    }

    errs
}

/// `validateVolumeAttachmentStatus` (validation.go:212-218): attachmentMetadata
/// total size and the attach/detach `VolumeError`s.
fn validate_volume_attachment_status(
    status: &VolumeAttachmentStatus,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // attachmentMetadata — total size (sum of key+value byte lengths) ≤ 256 kB.
    if let Some(metadata) = &status.attachment_metadata {
        let size: usize = metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
        if size > MAX_ATTACHED_VOLUME_METADATA_SIZE {
            errs.push(Error::too_long(
                &fld_path.child("attachmentMetadata"),
                MAX_ATTACHED_VOLUME_METADATA_SIZE,
            ));
        }
    }

    if let Some(e) = &status.attach_error {
        errs.extend(validate_volume_error(e, &fld_path.child("attachError")));
    }
    if let Some(e) = &status.detach_error {
        errs.extend(validate_volume_error(e, &fld_path.child("detachError")));
    }
    errs
}

/// `validateVolumeError` (validation.go:233-250): message length ≤ 1024 and a
/// non-negative errorCode (`must be between 0 and 2147483647, inclusive`).
fn validate_volume_error(e: &VolumeError, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(message) = &e.message {
        if message.len() > MAX_VOLUME_ERROR_MESSAGE_SIZE {
            // Upstream passes maxAttachedVolumeMetadataSize as the limit here
            // (validation.go:240) — a known upstream quirk we mirror so the
            // emitted message matches.
            errs.push(Error::too_long(
                &fld_path.child("message"),
                MAX_ATTACHED_VOLUME_METADATA_SIZE,
            ));
        }
    }

    if let Some(value) = e.error_code {
        if value < 0 {
            errs.push(Error::invalid(
                &fld_path.child("errorCode"),
                value.to_string(),
                format!("must be between 0 and {}, inclusive", i32::MAX),
            ));
        }
    }
    errs
}

/// `ValidateVolumeAttachmentUpdate` (validation.go:253-262): the create
/// validation (without the v1-only extras), and the spec is read-only.
pub fn validate_volume_attachment_update(
    new_va: &VolumeAttachment,
    old_va: &VolumeAttachment,
) -> ErrorList {
    let mut errs = validate_volume_attachment(new_va);
    errs.extend(validate_immutable_spec(new_va, old_va));
    errs
}

fn validate_immutable_spec(new_va: &VolumeAttachment, old_va: &VolumeAttachment) -> ErrorList {
    let new_spec = serde_json::to_value(&new_va.spec).unwrap_or_default();
    if new_spec == serde_json::to_value(&old_va.spec).unwrap_or_default() {
        return Vec::new();
    }
    vec![Error::invalid(
        &Path::new("spec"),
        new_spec,
        "field is immutable",
    )]
}
