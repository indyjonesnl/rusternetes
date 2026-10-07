//! #2497: DRA closed string-enums decode any string (upstream fields are plain
//! Go strings) and validation answers `NotSupported` (422), not a decode error.

use rusternetes_common::resources::dra::{ResourceClaim, ResourceClaimStatus, ResourceSlice};
use rusternetes_common::validation::field::{Error, ErrorType, Path};
use rusternetes_common::validation::resourceclaim::{
    validate_resource_claim, validate_resource_claim_status_update,
};
use rusternetes_common::validation::resourceslice::validate_resource_slice;
use serde_json::json;

fn claim(exactly: serde_json::Value) -> ResourceClaim {
    serde_json::from_value(json!({
        "apiVersion": "resource.k8s.io/v1",
        "kind": "ResourceClaim",
        "metadata": {"name": "c", "namespace": "default"},
        "spec": {"devices": {"requests": [{"name": "r", "exactly": exactly}]}},
    }))
    .expect("an unknown enum string must decode")
}

fn not_supported<'a>(errs: &'a [Error], field: &str) -> Option<&'a Error> {
    errs.iter()
        .find(|e| e.error_type == ErrorType::NotSupported && e.field == field)
}

#[test]
fn unknown_allocation_mode_is_not_supported() {
    // validation.go:283 (`default:` arm of validateDeviceAllocationMode).
    let c = claim(json!({"deviceClassName": "gpu", "allocationMode": "Sometimes"}));
    let errs = validate_resource_claim(&c);
    assert!(
        not_supported(&errs, "spec.devices.requests[0].exactly.allocationMode").is_some(),
        "{errs:?}"
    );
}

#[test]
fn unknown_toleration_operator_is_not_supported() {
    // validation.go:1426.
    let c = claim(json!({"deviceClassName": "gpu", "allocationMode": "All",
        "tolerations": [{"operator": "Maybe"}]}));
    let errs = validate_resource_claim(&c);
    assert!(
        not_supported(
            &errs,
            "spec.devices.requests[0].exactly.tolerations[0].operator"
        )
        .is_some(),
        "{errs:?}"
    );
}

#[test]
fn unknown_toleration_effect_is_not_supported() {
    // validation.go:1431-1432.
    let c = claim(json!({"deviceClassName": "gpu", "allocationMode": "All",
        "tolerations": [{"operator": "Exists", "effect": "NoSuch"}]}));
    let errs = validate_resource_claim(&c);
    assert!(
        not_supported(
            &errs,
            "spec.devices.requests[0].exactly.tolerations[0].effect"
        )
        .is_some(),
        "{errs:?}"
    );
}

#[test]
fn unknown_allocation_config_source_is_not_supported() {
    // validation.go:540 (`validateAllocationConfigSource` default arm).
    let status: ResourceClaimStatus = serde_json::from_value(json!({
        "allocation": {"devices": {"config": [{
            "source": "FromNowhere", "opaque": {"driver": "d.example.com", "parameters": {}}}]}}
    }))
    .expect("an unknown enum string must decode");
    let errs = validate_resource_claim_status_update(
        &status,
        &ResourceClaimStatus::default(),
        &Default::default(),
        false,
        &Path::new("status"),
    );
    assert!(
        not_supported(&errs, "status.allocation.devices.config[0].source").is_some(),
        "{errs:?}"
    );
}

#[test]
fn unknown_device_taint_effect_is_not_supported() {
    // validation.go:1402-1403.
    let s: ResourceSlice = serde_json::from_value(json!({
        "apiVersion": "resource.k8s.io/v1",
        "kind": "ResourceSlice",
        "metadata": {"name": "s"},
        "spec": {"driver": "d.example.com", "nodeName": "n1",
            "pool": {"name": "p", "generation": 1, "resourceSliceCount": 1},
            "devices": [{"name": "dev", "taints": [{"key": "k", "effect": "NoSuch"}]}]},
    }))
    .expect("an unknown enum string must decode");
    let errs = validate_resource_slice(&s);
    assert!(
        not_supported(&errs, "spec.devices[0].taints[0].effect").is_some(),
        "{errs:?}"
    );
}
