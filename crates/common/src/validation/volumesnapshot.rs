//! VolumeSnapshot / VolumeSnapshotContent validation.
//!
//! **Stated exception to the upstream-first rule (CLAUDE.md rule 8):**
//! `snapshot.storage.k8s.io` is not a Kubernetes API. `../kubernetes` has no
//! validator for it — the types are CRDs owned by
//! `kubernetes-csi/external-snapshotter`, and their *schema* is the contract
//! the api-server enforces. So the source ported here is that repo's
//! `client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml` and
//! `…_volumesnapshotcontents.yaml`: the `required:` lists and the
//! `x-kubernetes-validations` CEL rules, whose `message:` strings are
//! reproduced verbatim so a client sees what it would see against a real
//! external-snapshotter install.
//!
//! Rusternetes serves these two natively rather than through the CRD machinery
//! (`router.rs`, `/apis/snapshot.storage.k8s.io/v1/...`), so nothing else
//! applies the schema — without this, a `VolumeSnapshot` with no `spec.source`
//! at all was written.
//!
//! The immutability CEL rules (`self == oldSelf` on each handle / name, and
//! the "required once set" pair) are update-time and live in the `*_update`
//! entry points.

use crate::resources::volume::{
    VolumeSnapshot, VolumeSnapshotContent, VolumeSnapshotContentSource, VolumeSnapshotContentSpec,
    VolumeSnapshotSource, VolumeSnapshotSpec,
};
use crate::validation::field::{Error, ErrorList, Path};

/// Validate a `VolumeSnapshot` on create.
pub fn validate_volume_snapshot(snapshot: &VolumeSnapshot) -> ErrorList {
    validate_volume_snapshot_spec(&snapshot.spec, &Path::new("spec"))
}

/// Validate an update to a `VolumeSnapshot`: the create rules plus the
/// immutability the CRD declares with `self == oldSelf` on each source name,
/// and the "required once set" pair.
pub fn validate_volume_snapshot_update(new: &VolumeSnapshot, old: &VolumeSnapshot) -> ErrorList {
    let mut errs = validate_volume_snapshot(new);
    let src = Path::new("spec").child("source");
    errs.extend(validate_immutable_source_name(
        old.spec.source.persistent_volume_claim_name.as_deref(),
        new.spec.source.persistent_volume_claim_name.as_deref(),
        &src,
        "persistentVolumeClaimName",
    ));
    errs.extend(validate_immutable_source_name(
        old.spec.source.volume_snapshot_content_name.as_deref(),
        new.spec.source.volume_snapshot_content_name.as_deref(),
        &src,
        "volumeSnapshotContentName",
    ));
    errs
}

fn validate_volume_snapshot_spec(spec: &VolumeSnapshotSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_volume_snapshot_source(
        &spec.source,
        &fld_path.child("source"),
    ));
    errs.extend(validate_snapshot_class_name(
        spec.volume_snapshot_class_name.as_deref(),
        &fld_path.child("volumeSnapshotClassName"),
    ));
    errs
}

/// `source` is in the spec's `required:` list, and carries
/// `exactly one of volumeSnapshotContentName and persistentVolumeClaimName must be set`.
fn validate_volume_snapshot_source(source: &VolumeSnapshotSource, fld_path: &Path) -> ErrorList {
    let pvc = source.persistent_volume_claim_name.as_deref();
    let content = source.volume_snapshot_content_name.as_deref();
    exactly_one(
        fld_path,
        pvc.is_some(),
        content.is_some(),
        "exactly one of volumeSnapshotContentName and persistentVolumeClaimName must be set",
    )
}

/// Validate a `VolumeSnapshotContent` on create.
pub fn validate_volume_snapshot_content(content: &VolumeSnapshotContent) -> ErrorList {
    validate_volume_snapshot_content_spec(&content.spec, &Path::new("spec"))
}

/// Validate an update to a `VolumeSnapshotContent`: the create rules plus the
/// `self == oldSelf` immutability on each source handle.
pub fn validate_volume_snapshot_content_update(
    new: &VolumeSnapshotContent,
    old: &VolumeSnapshotContent,
) -> ErrorList {
    let mut errs = validate_volume_snapshot_content(new);
    let src = Path::new("spec").child("source");
    errs.extend(validate_immutable_source_name(
        old.spec.source.snapshot_handle.as_deref(),
        new.spec.source.snapshot_handle.as_deref(),
        &src,
        "snapshotHandle",
    ));
    errs.extend(validate_immutable_source_name(
        old.spec.source.volume_handle.as_deref(),
        new.spec.source.volume_handle.as_deref(),
        &src,
        "volumeHandle",
    ));
    errs
}

fn validate_volume_snapshot_content_spec(
    spec: &VolumeSnapshotContentSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // `required: [deletionPolicy, driver, source, volumeSnapshotRef]`.
    if spec.driver.is_empty() {
        errs.push(Error::required(&fld_path.child("driver"), ""));
    }
    if matches!(
        spec.deletion_policy,
        crate::resources::volume::DeletionPolicy::Unspecified
    ) {
        errs.push(Error::required(&fld_path.child("deletionPolicy"), ""));
    }

    errs.extend(validate_volume_snapshot_content_source(
        &spec.source,
        &fld_path.child("source"),
    ));

    // `volumeSnapshotRef` is a core ObjectReference. The CRD requires the field
    // itself; external-snapshotter's controller then needs a name and a
    // namespace to bind back to the VolumeSnapshot, and cannot act on a
    // reference missing either.
    let ref_path = fld_path.child("volumeSnapshotRef");
    if spec
        .volume_snapshot_ref
        .name
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&ref_path.child("name"), ""));
    }
    if spec
        .volume_snapshot_ref
        .namespace
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&ref_path.child("namespace"), ""));
    }

    errs.extend(validate_snapshot_class_name(
        spec.volume_snapshot_class_name.as_deref(),
        &fld_path.child("volumeSnapshotClassName"),
    ));

    errs
}

/// `exactly one of volumeHandle and snapshotHandle must be set`.
fn validate_volume_snapshot_content_source(
    source: &VolumeSnapshotContentSource,
    fld_path: &Path,
) -> ErrorList {
    exactly_one(
        fld_path,
        source.volume_handle.is_some(),
        source.snapshot_handle.is_some(),
        "exactly one of volumeHandle and snapshotHandle must be set",
    )
}

/// Both CRDs declare `volumeSnapshotClassName` as optional but non-empty:
/// "Empty string is not allowed for this field", enforced by
/// `volumeSnapshotClassName must not be the empty string when set`.
fn validate_snapshot_class_name(name: Option<&str>, fld_path: &Path) -> ErrorList {
    match name {
        Some("") => vec![Error::invalid(
            fld_path,
            String::new(),
            "volumeSnapshotClassName must not be the empty string when set",
        )],
        _ => Vec::new(),
    }
}

fn exactly_one(fld_path: &Path, a: bool, b: bool, message: &str) -> ErrorList {
    if a == b {
        // Neither set, or both set — the CEL rule fails either way, and the
        // apiserver reports a CEL failure against the parent object.
        return vec![Error::invalid(fld_path, String::new(), message)];
    }
    Vec::new()
}

/// `self == oldSelf` on a source name, plus the matching
/// `<field> is required once set` rule: once the old object carried the name,
/// the new one must carry the same one.
fn validate_immutable_source_name(
    old: Option<&str>,
    new: Option<&str>,
    fld_path: &Path,
    field: &str,
) -> ErrorList {
    match (old, new) {
        (Some(_), None) => vec![Error::invalid(
            fld_path,
            String::new(),
            format!("{field} is required once set"),
        )],
        (Some(o), Some(n)) if o != n => vec![Error::invalid(
            &fld_path.child(field),
            n.to_string(),
            format!("{field} is immutable"),
        )],
        _ => Vec::new(),
    }
}
