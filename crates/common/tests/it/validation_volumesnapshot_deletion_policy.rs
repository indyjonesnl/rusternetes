//! An out-of-enum `deletionPolicy` decodes (a Go string) and is answered
//! `Unsupported value` (422), not a 400 decode failure (#2498).
//!
//! Source: external-snapshotter CRDs
//! `client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml`
//! (`deletionPolicy` `enum: [Delete, Retain]`) and `..._volumesnapshotcontents.yaml`;
//! the enum violation is reported as `field.NotSupported` with the enum values
//! in declaration order by `kubeOpenAPIResultToFieldErrors`
//! (apiextensions-apiserver/pkg/apiserver/validation/validation.go:177-190).

use rusternetes_common::resources::volume::{VolumeSnapshotClass, VolumeSnapshotContent};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::volumesnapshot::{
    validate_volume_snapshot_class, validate_volume_snapshot_content,
};
use serde_json::json;

#[test]
fn class_unknown_deletion_policy_not_supported() {
    let class: VolumeSnapshotClass = serde_json::from_value(json!({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
        "metadata": {"name": "c"}, "driver": "d", "deletionPolicy": "Archive"
    }))
    .expect("unknown deletionPolicy must decode");
    let errs = validate_volume_snapshot_class(&class);
    let e = errs
        .iter()
        .find(|e| e.field == "deletionPolicy")
        .unwrap_or_else(|| panic!("{errs:?}"));
    assert_eq!(e.error_type, ErrorType::NotSupported);
    assert!(e.to_string().contains("\"Delete\", \"Retain\""), "{e}");
}

#[test]
fn content_unknown_deletion_policy_not_supported() {
    let content: VolumeSnapshotContent = serde_json::from_value(json!({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
        "metadata": {"name": "c"},
        "spec": {"driver": "d", "deletionPolicy": "Archive",
                 "source": {"snapshotHandle": "h"},
                 "volumeSnapshotRef": {"name": "s", "namespace": "n"}}
    }))
    .expect("unknown deletionPolicy must decode");
    let errs = validate_volume_snapshot_content(&content);
    let e = errs
        .iter()
        .find(|e| e.field == "spec.deletionPolicy")
        .unwrap_or_else(|| panic!("{errs:?}"));
    assert_eq!(e.error_type, ErrorType::NotSupported);
}
