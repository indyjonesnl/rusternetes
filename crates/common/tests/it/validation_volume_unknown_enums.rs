//! Unknown values of volume.rs's string-typed enum fields decode and validation
//! reports `Unsupported value` (422), as upstream's plain `string` types do
//! (#2475). Upstream checks (release-1.35):
//! `pkg/apis/core/validation/validation.go` :1395 (hostPath type), :1986 and
//! :2476 (accessModes), :2021 (persistentVolumeReclaimPolicy), :2242 and :2504
//! (volumeMode); `pkg/apis/storage/validation/validation.go` :133
//! (reclaimPolicy), :271 (volumeBindingMode).

use rusternetes_common::resources::volume::{
    PersistentVolume, PersistentVolumeClaim, StorageClass,
};
use rusternetes_common::validation::persistentvolume::validate_persistent_volume;
use rusternetes_common::validation::pvc::validate_persistent_volume_claim;
use rusternetes_common::validation::storageclass::validate_storage_class;
use serde_json::{json, Value};

fn pv_errs(spec: Value) -> Vec<String> {
    let pv: PersistentVolume = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv"},
        "spec": spec
    }))
    .expect("must decode");
    validate_persistent_volume(&pv)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn pvc_errs(spec: Value) -> Vec<String> {
    let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "data", "namespace": "default"},
        "spec": spec
    }))
    .expect("must decode");
    validate_persistent_volume_claim(&pvc)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn sc_errs(extra: Value) -> Vec<String> {
    let mut v = json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": "sc"},
        "provisioner": "example.com/prov",
        "volumeBindingMode": "Immediate"
    });
    for (k, val) in extra.as_object().unwrap() {
        v[k] = val.clone();
    }
    let sc: StorageClass = serde_json::from_value(v).expect("must decode");
    validate_storage_class(&sc)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn valid_pv() -> Value {
    json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/data"}
    })
}

fn valid_pvc() -> Value {
    json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": "1Gi"}}
    })
}

fn assert_has(errs: &[String], want: &str) {
    assert!(
        errs.iter().any(|e| e.contains(want)),
        "want {want:?} in {errs:?}"
    );
}

const MODES: &str =
    "supported values: \"ReadOnlyMany\", \"ReadWriteMany\", \"ReadWriteOnce\", \"ReadWriteOncePod\"";

#[test]
fn valid_objects_have_no_errors() {
    assert_eq!(pv_errs(valid_pv()), Vec::<String>::new());
    assert_eq!(pvc_errs(valid_pvc()), Vec::<String>::new());
    assert_eq!(sc_errs(json!({})), Vec::<String>::new());
}

#[test]
fn unknown_pv_enum_values_report_unsupported_value() {
    let mut s = valid_pv();
    s["accessModes"] = json!(["Bogus"]);
    assert_has(
        &pv_errs(s),
        &format!("spec.accessModes: Unsupported value: \"Bogus\": {MODES}"),
    );

    let mut s = valid_pv();
    s["persistentVolumeReclaimPolicy"] = json!("Bogus");
    assert_has(
        &pv_errs(s),
        "spec.persistentVolumeReclaimPolicy: Unsupported value: \"Bogus\": supported values: \"Delete\", \"Recycle\", \"Retain\"",
    );

    let mut s = valid_pv();
    s["volumeMode"] = json!("Bogus");
    assert_has(
        &pv_errs(s),
        "spec.volumeMode: Unsupported value: \"Bogus\": supported values: \"Block\", \"Filesystem\"",
    );

    let mut s = valid_pv();
    s["hostPath"]["type"] = json!("Bogus");
    assert_has(
        &pv_errs(s),
        "spec.hostPath.type: Unsupported value: \"Bogus\": supported values: \"\", \"BlockDevice\", \"CharDevice\", \"Directory\", \"DirectoryOrCreate\", \"File\", \"FileOrCreate\", \"Socket\"",
    );
}

#[test]
fn unknown_pvc_enum_values_report_unsupported_value() {
    let mut s = valid_pvc();
    s["accessModes"] = json!(["Bogus"]);
    assert_has(
        &pvc_errs(s),
        &format!("spec.accessModes: Unsupported value: \"Bogus\": {MODES}"),
    );

    let mut s = valid_pvc();
    s["volumeMode"] = json!("Bogus");
    assert_has(
        &pvc_errs(s),
        "spec.volumeMode: Unsupported value: \"Bogus\": supported values: \"Block\", \"Filesystem\"",
    );
}

/// An unrecognised access mode must not be counted as "another mode" next to
/// ReadWriteOncePod: upstream's `else if supportedAccessModes.Has(mode)`
/// (validation.go:1992) leaves it out.
#[test]
fn unknown_access_mode_is_not_counted_as_other_with_rwop() {
    let mut s = valid_pvc();
    s["accessModes"] = json!(["ReadWriteOncePod", "Bogus"]);
    let errs = pvc_errs(s);
    assert!(
        !errs
            .iter()
            .any(|e| e.contains("may not use ReadWriteOncePod")),
        "{errs:?}"
    );
    assert_has(&errs, "Unsupported value: \"Bogus\"");
}

#[test]
fn unknown_storage_class_enum_values_report_unsupported_value() {
    assert_has(
        &sc_errs(json!({"reclaimPolicy": "Bogus"})),
        "reclaimPolicy: Unsupported value: \"Bogus\": supported values: \"Delete\", \"Retain\"",
    );
    assert_has(
        &sc_errs(json!({"volumeBindingMode": "Bogus"})),
        "volumeBindingMode: Unsupported value: \"Bogus\": supported values: \"Immediate\", \"WaitForFirstConsumer\"",
    );
}

#[test]
fn unknown_values_round_trip() {
    let pv: PersistentVolume = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": "pv"},
        "spec": {
            "accessModes": ["Bogus"],
            "persistentVolumeReclaimPolicy": "Weird",
            "volumeMode": "Odd",
            "hostPath": {"path": "/d", "type": "Strange"}
        }
    }))
    .unwrap();
    let back = serde_json::to_value(&pv).unwrap();
    assert_eq!(back["spec"]["accessModes"][0], "Bogus");
    assert_eq!(back["spec"]["persistentVolumeReclaimPolicy"], "Weird");
    assert_eq!(back["spec"]["volumeMode"], "Odd");
    assert_eq!(back["spec"]["hostPath"]["type"], "Strange");
}
