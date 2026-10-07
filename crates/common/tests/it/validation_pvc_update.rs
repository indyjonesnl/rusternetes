//! Tests for PVC update validation: ports of the scenarios in upstream
//! `TestValidatePersistentVolumeClaimUpdate`
//! (pkg/apis/core/validation/validation_test.go:2346-3136, release-1.35).

use rusternetes_common::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimPhase,
    PersistentVolumeClaimSpec, PersistentVolumeClaimStatus, PersistentVolumeMode,
    ResourceRequirements, TypedLocalObjectReference,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::pvc::validate_persistent_volume_claim_update;
use std::collections::HashMap;

fn pvc(storage: &str, mode: Option<PersistentVolumeMode>) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), storage.to_string());
    PersistentVolumeClaim {
        type_meta: Default::default(),
        // `ValidatePersistentVolumeClaim` now checks ObjectMeta too.
        metadata: rusternetes_common::types::ObjectMeta {
            name: "c".into(),
            namespace: Some("default".into()),
            resource_version: Some("1".into()),
            ..Default::default()
        },
        spec: PersistentVolumeClaimSpec {
            // A real update target satisfies the create-time spec rules, which
            // `validate_persistent_volume_claim_update` re-runs (upstream
            // `ValidatePersistentVolumeClaimUpdate` calls `ValidatePersistentVolumeClaim`).
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None,
            storage_class_name: None,
            volume_mode: mode,
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        // Upstream's `validClaim` carries a Bound status.
        status: Some(PersistentVolumeClaimStatus {
            phase: PersistentVolumeClaimPhase::Bound,
            ..Default::default()
        }),
    }
}

fn unbound(mut p: PersistentVolumeClaim) -> PersistentVolumeClaim {
    p.status = None;
    p
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field == field)
}

#[test]
fn grow_storage_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("2Gi", Some(PersistentVolumeMode::Filesystem));
    assert!(ok(&new, &old));
}

#[test]
fn equal_storage_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    assert!(ok(&new, &old));
}

#[test]
fn shrink_storage_rejected() {
    // Shrinking to at-or-below status.capacity is rejected (upstream
    // `invalid-expand-shrink-to-status-resize-enabled`).
    let mut old = pvc("2Gi", Some(PersistentVolumeMode::Filesystem));
    old.status.as_mut().unwrap().capacity =
        Some([("storage".to_string(), "2Gi".to_string())].into());
    let new = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let errs = validate_persistent_volume_claim_update(&new, &old);
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.resources.requests.storage"
            && e.error_type == ErrorType::Forbidden));
}

#[test]
fn volume_mode_change_rejected() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Block));
    let errs = validate_persistent_volume_claim_update(&new, &old);
    assert!(errs
        .iter()
        .any(|e| e.field == "volumeMode" && e.error_type == ErrorType::Invalid));
}

#[test]
fn same_volume_mode_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Block));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Block));
    assert!(ok(&new, &old));
}

#[test]
fn shrink_with_units_detected() {
    // 1024Mi == 1Gi; 512Mi < 1Gi → shrink
    let mut old = pvc("1Gi", None);
    old.status.as_mut().unwrap().capacity =
        Some([("storage".to_string(), "1Gi".to_string())].into());
    let new = pvc("512Mi", None);
    assert!(has(
        &validate_persistent_volume_claim_update(&new, &old),
        "spec.resources.requests.storage"
    ));
}

fn bound(mut p: PersistentVolumeClaim) -> PersistentVolumeClaim {
    p.status = Some(PersistentVolumeClaimStatus {
        phase: PersistentVolumeClaimPhase::Bound,
        ..Default::default()
    });
    p
}

fn with_ann(mut p: PersistentVolumeClaim, v: &str) -> PersistentVolumeClaim {
    let mut m = p.metadata.annotations.take().unwrap_or_default();
    m.insert("volume.beta.kubernetes.io/storage-class".into(), v.into());
    p.metadata.annotations = Some(m);
    p
}

fn with_sc(mut p: PersistentVolumeClaim, v: Option<&str>) -> PersistentVolumeClaim {
    p.spec.storage_class_name = v.map(String::from);
    p
}

fn with_vac(mut p: PersistentVolumeClaim, v: Option<&str>) -> PersistentVolumeClaim {
    p.spec.volume_attributes_class_name = v.map(String::from);
    p
}

fn ok(new: &PersistentVolumeClaim, old: &PersistentVolumeClaim) -> bool {
    let e = validate_persistent_volume_claim_update(new, old);
    e.is_empty()
}

#[test]
fn volume_name_set_once_ok_but_not_changed() {
    // valid-update-volumeName-only
    let old = pvc("1Gi", None);
    let mut new = pvc("1Gi", None);
    new.spec.volume_name = Some("pv1".into());
    assert!(ok(&new, &old));
    let mut newer = new.clone();
    newer.spec.volume_name = Some("pv2".into());
    assert!(has(
        &validate_persistent_volume_claim_update(&newer, &new),
        "spec"
    ));
}

#[test]
fn spec_change_access_modes_rejected() {
    // invalid-update-change-access-modes-on-bound-claim
    let old = pvc("1Gi", None);
    let mut new = pvc("1Gi", None);
    new.spec.access_modes = vec![PersistentVolumeAccessMode::ReadOnlyMany];
    let errs = validate_persistent_volume_claim_update(&new, &old);
    let e = errs.iter().find(|e| e.field == "spec").expect("spec error");
    assert_eq!(e.error_type, ErrorType::Forbidden);
    assert!(e.detail.starts_with(
        "spec is immutable after creation except resources.requests and volumeAttributesClassName for bound claims"
    ));
}

#[test]
fn unbound_size_update_rejected_bound_ok() {
    // unbound-size-update-resize-enabled vs valid-size-update
    assert!(has(
        &validate_persistent_volume_claim_update(
            &unbound(pvc("2Gi", None)),
            &unbound(pvc("1Gi", None))
        ),
        "spec"
    ));
    assert!(ok(&bound(pvc("2Gi", None)), &bound(pvc("1Gi", None))));
}

#[test]
fn equal_quantities_in_other_units_not_a_spec_change() {
    let old = bound(pvc("1Gi", None));
    let new = bound(pvc("1024Mi", None));
    assert!(ok(&new, &old));
}

#[test]
fn shrink_to_status_capacity_rejected_with_recover_message() {
    // invalid-expand-shrink-to-status-resize-enabled
    let mut old = bound(pvc("3Gi", None));
    old.status.as_mut().unwrap().capacity =
        Some([("storage".to_string(), "2Gi".to_string())].into());
    let errs = validate_persistent_volume_claim_update(&bound(pvc("2Gi", None)), &old);
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.resources.requests.storage"
            && e.detail == "field can not be less than status.capacity"));
    // valid-expand-shrink-resize-enabled: above status capacity is allowed.
    assert!(ok(&bound(pvc("2500Mi", None)), &old));
}

#[test]
fn storage_class_annotation_immutable() {
    // invalid-update-change-storage-class-annotation-after-creation
    let old = with_ann(pvc("1Gi", None), "fast");
    let new = with_ann(pvc("1Gi", None), "slow");
    let errs = validate_persistent_volume_claim_update(&new, &old);
    assert!(errs.iter().any(|e| e.field
        == "metadata.annotations.volume.beta.kubernetes.io/storage-class"
        && e.error_type == ErrorType::Invalid
        && e.detail == "field is immutable"));
}

#[test]
fn storage_class_upgrade_paths() {
    let old_ann = with_ann(pvc("1Gi", None), "fast");
    // valid-upgrade-storage-class-annotation-to-spec
    assert!(ok(&with_sc(pvc("1Gi", None), Some("fast")), &old_ann));
    // invalid-upgrade-storage-class-annotation-to-spec (name differs)
    assert!(!ok(&with_sc(pvc("1Gi", None), Some("fast2")), &old_ann));
    // valid-upgrade-storage-class-annotation-to-annotation-and-spec
    assert!(ok(
        &with_sc(with_ann(pvc("1Gi", None), "fast"), Some("fast")),
        &old_ann
    ));
    // invalid-upgrade-storage-class-annotation-to-annotation-and-spec
    assert!(!ok(
        &with_sc(with_ann(pvc("1Gi", None), "fast2"), Some("fast")),
        &old_ann
    ));
    // valid-upgrade-nil-storage-class-spec-to-spec
    assert!(ok(
        &with_sc(pvc("1Gi", None), Some("fast")),
        &pvc("1Gi", None)
    ));
    // invalid-upgrade-not-nil-storage-class-spec-to-spec
    assert!(!ok(
        &with_sc(pvc("1Gi", None), Some("fast2")),
        &with_sc(pvc("1Gi", None), Some("fast"))
    ));
    // invalid-upgrade-to-nil-storage-class-spec-to-spec
    assert!(!ok(
        &pvc("1Gi", None),
        &with_sc(pvc("1Gi", None), Some("fast"))
    ));
    // invalid-downgrade-storage-class-spec-to-annotation
    assert!(!ok(
        &with_ann(pvc("1Gi", None), "fast"),
        &with_sc(pvc("1Gi", None), Some("fast"))
    ));
}

#[test]
fn mutable_annotation_ok() {
    // valid-update-add-annotation
    let old = pvc("1Gi", None);
    let mut new = pvc("1Gi", None);
    new.metadata.annotations = Some([("a".to_string(), "b".to_string())].into());
    assert!(ok(&new, &old));
}

#[test]
fn volume_attributes_class_transitions() {
    let b = |v| bound(with_vac(pvc("1Gi", None), v));
    // valid-update-volume-attributes-class-{from-nil,from-empty,,to-nil,to-empty}
    assert!(ok(&b(Some("vac1")), &b(None)));
    assert!(ok(&b(Some("vac1")), &b(Some(""))));
    assert!(ok(&b(Some("vac2")), &b(Some("vac1"))));
    assert!(ok(&b(None), &b(Some("vac1"))));
    assert!(ok(&b(Some("")), &b(Some("vac1"))));
    // invalid ...-to-nil / -to-empty-when-current-vac-set
    let mut old = b(Some("vac1"));
    old.status
        .as_mut()
        .unwrap()
        .current_volume_attributes_class_name = Some("vac1".into());
    let errs = validate_persistent_volume_claim_update(&b(None), &old);
    assert!(errs.iter().any(|e| {
        e.field == "spec.volumeAttributesClassName"
        && e.error_type == ErrorType::Forbidden
        && e.detail
            == "update to nil is forbidden when status.currentVolumeAttributesClassName is not nil"
    }));
    let errs = validate_persistent_volume_claim_update(&b(Some("")), &old);
    assert!(errs.iter().any(|e| e.detail
        == "update to empty string is forbidden when status.currentVolumeAttributesClassName is not nil"));
    // invalid-update-volume-attributes-class-when-claim-not-bound
    let un = |v| unbound(with_vac(pvc("1Gi", None), v));
    assert!(has(
        &validate_persistent_volume_claim_update(&un(Some("vac2")), &un(Some("vac1"))),
        "spec"
    ));
}

#[test]
fn invalid_api_group_kept_when_old_object_has_it() {
    // allow-update-pvc-when-data-source-used
    let mut old = pvc("1Gi", None);
    old.spec.data_source = Some(TypedLocalObjectReference {
        api_group: Some("^invalid".into()),
        kind: "VolumeSnapshot".into(),
        name: "snap".into(),
    });
    assert!(ok(&old.clone(), &old));
}
