//! ResourceSlice (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateResourceSlice`
//! (release-1.35).
//!
//! Scope (create path): `driver` (CSI driver name, via
//! `corevalidation.ValidateCSIDriverName`), `pool` (name segments +
//! generation/resourceSliceCount bounds), the exactly-one node-selection rule
//! (`nodeName` via `ValidateNodeName`/`nodeSelector`/`allNodes`/
//! `perDeviceNodeSelection`), the `devices` set (size cap + unique names, each
//! a DNS-1123 label), each device's `taints` and `consumesCounters`, and the
//! spec's `sharedCounters`. Deep `nodeSelector` term validation and the
//! attribute/capacity maps are tracked in #1442. ObjectMeta is validated
//! separately.

use crate::resources::dra::{CounterSet, Device, DeviceCounterConsumption, DeviceTaint};
use crate::resources::{ResourceSlice, ResourceSliceSpec};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain, validate_label_name};
use std::collections::HashSet;

/// Port of upstream `validateNodeName` (resource/validation): node names use
/// `corevalidation.ValidateNodeName`, which is `NameIsDNSSubdomain` — a
/// DNS-1123 subdomain.
fn validate_node_name(name: &str, fld_path: &Path) -> ErrorList {
    is_dns1123_subdomain(name)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, name.to_string(), msg))
        .collect()
}

const POOL_NAME_MAX_LENGTH: usize = 253;
/// Upstream `resource.DeviceTaintsMaxLength` (`pkg/apis/resource/types.go:614`).
const DEVICE_TAINTS_MAX_LENGTH: usize = 16;
/// Upstream `resource.ResourceSliceMaxDeviceCounterConsumptionsPerDevice`
/// (`pkg/apis/resource/types.go:268`).
const MAX_COUNTER_CONSUMPTIONS_PER_DEVICE: usize = 2;
/// Upstream `resource.ResourceSliceMaxCountersPerCounterSet` and
/// `...PerDeviceCounterConsumption` (`pkg/apis/resource/types.go:263,272`).
const MAX_COUNTERS_PER_SET: usize = 32;
/// The effects `validDeviceTaintEffects` holds
/// (`pkg/apis/resource/validation/validation.go:1389`).
const DEVICE_TAINT_EFFECTS: &[&str] = &["NoExecute", "NoSchedule", "None"];
const RESOURCE_SLICE_MAX_DEVICES: usize = 128;
const RESOURCE_SLICE_MAX_DEVICES_WITH_COUNTERS: usize = 64;

/// Validate a `ResourceSlice` on create. Mirrors upstream `ValidateResourceSlice`
/// (minus ObjectMeta and the deeper nodeSelector/per-device checks).
pub fn validate_resource_slice(slice: &ResourceSlice) -> ErrorList {
    validate_resource_slice_spec(&slice.spec, &Path::new("spec"))
}

/// Validate a `ResourceSlice` on update. Mirrors upstream
/// `ValidateResourceSliceUpdate`: re-run the create validation, then enforce
/// that `pool.name`, `driver`, and `nodeName` are immutable.
pub fn validate_resource_slice_update(new: &ResourceSlice, old: &ResourceSlice) -> ErrorList {
    let mut errs = validate_resource_slice(new);
    let spec = Path::new("spec");
    if new.spec.pool.name != old.spec.pool.name {
        errs.push(Error::invalid(
            &spec.child("pool").child("name"),
            new.spec.pool.name.clone(),
            "field is immutable",
        ));
    }
    if new.spec.driver != old.spec.driver {
        errs.push(Error::invalid(
            &spec.child("driver"),
            new.spec.driver.clone(),
            "field is immutable",
        ));
    }
    if new.spec.node_name != old.spec.node_name {
        errs.push(Error::invalid(
            &spec.child("nodeName"),
            new.spec.node_name.clone().unwrap_or_default(),
            "field is immutable",
        ));
    }
    errs
}

fn validate_resource_slice_spec(spec: &ResourceSliceSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // driver — CSI driver name (upstream `validateDriverName` =
    // `corevalidation.ValidateCSIDriverName`): required, ≤63 chars, DNS-1123
    // subdomain when lowercased.
    let driver_path = fld_path.child("driver");
    errs.extend(validate_csi_driver_name(&spec.driver, &driver_path));

    // pool.
    let pool_path = fld_path.child("pool");
    let name_path = pool_path.child("name");
    if spec.pool.name.is_empty() {
        errs.push(Error::required(&name_path, ""));
    } else {
        if spec.pool.name.len() > POOL_NAME_MAX_LENGTH {
            errs.push(Error::too_long(&name_path, POOL_NAME_MAX_LENGTH));
        }
        for part in spec.pool.name.split('/') {
            for msg in is_dns1123_subdomain(part) {
                errs.push(Error::invalid(&name_path, spec.pool.name.clone(), msg));
            }
        }
    }
    if spec.pool.resource_slice_count <= 0 {
        errs.push(Error::invalid(
            &pool_path.child("resourceSliceCount"),
            spec.pool.resource_slice_count,
            "must be greater than zero",
        ));
    }
    if spec.pool.generation < 0 {
        errs.push(Error::invalid(
            &pool_path.child("generation"),
            spec.pool.generation,
            "must be greater than or equal to zero",
        ));
    }

    // Exactly one of nodeName / nodeSelector / allNodes / perDeviceNodeSelection.
    let mut set_fields: Vec<&str> = Vec::new();
    if let Some(node_name) = &spec.node_name {
        if node_name.is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("nodeName"),
                String::new(),
                "must be either unset or set to a non-empty string",
            ));
        } else {
            set_fields.push("`nodeName`");
            errs.extend(validate_node_name(node_name, &fld_path.child("nodeName")));
        }
    }
    if spec.node_selector.is_some() {
        set_fields.push("`nodeSelector`");
    }
    if let Some(all_nodes) = spec.all_nodes {
        if all_nodes {
            set_fields.push("`allNodes`");
        } else {
            errs.push(Error::invalid(
                &fld_path.child("allNodes"),
                false,
                "must be either unset or set to true",
            ));
        }
    }
    if let Some(per_device) = spec.per_device_node_selection {
        if per_device {
            set_fields.push("`perDeviceNodeSelection`");
        } else {
            errs.push(Error::invalid(
                &fld_path.child("perDeviceNodeSelection"),
                false,
                "must be either unset or set to true",
            ));
        }
    }
    if set_fields.is_empty() {
        errs.push(Error::required(
            fld_path,
            "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required",
        ));
    } else if set_fields.len() > 1 {
        errs.push(Error::invalid(
            fld_path,
            format!("{{{}}}", set_fields.join(", ")),
            "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set",
        ));
    }

    // devices: size cap + unique non-empty names.
    let devices_path = fld_path.child("devices");
    let has_counters = spec.devices.iter().any(|d| !d.consumes_counters.is_empty());
    let max_devices = if has_counters {
        RESOURCE_SLICE_MAX_DEVICES_WITH_COUNTERS
    } else {
        RESOURCE_SLICE_MAX_DEVICES
    };
    if spec.devices.len() > max_devices {
        errs.push(Error::too_many(&devices_path, max_devices));
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, device) in spec.devices.iter().enumerate() {
        let dp = devices_path.index(i);
        if !device.name.is_empty() && !seen.insert(device.name.as_str()) {
            errs.push(Error::duplicate(&dp.child("name"), device.name.clone()));
        }
        errs.extend(validate_device(device, &dp));
    }

    // sharedCounters (upstream `validateResourceSliceSpec` runs `validateSet`
    // over them with `validateCounterSet`, validation.go:754-770).
    let shared_path = fld_path.child("sharedCounters");
    let mut seen_sets: HashSet<&str> = HashSet::new();
    for (i, set) in spec.shared_counters.iter().enumerate() {
        let sp = shared_path.index(i);
        if !set.name.is_empty() && !seen_sets.insert(set.name.as_str()) {
            errs.push(Error::duplicate(&sp.child("name"), set.name.clone()));
        }
        errs.extend(validate_counter_set(set, &sp));
    }

    errs
}

/// Port of upstream `validateDevice`
/// (`pkg/apis/resource/validation/validation.go:801-870`), covering the parts
/// that need no CEL environment: the name, the taints and the counter
/// consumptions. The attribute/capacity maps and the per-device node-selection
/// coupling stay in #1442.
fn validate_device(device: &Device, fld_path: &Path) -> ErrorList {
    let mut errs = validate_device_name(&device.name, &fld_path.child("name"));

    let taints_path = fld_path.child("taints");
    if device.taints.len() > DEVICE_TAINTS_MAX_LENGTH {
        errs.push(Error::too_many(&taints_path, DEVICE_TAINTS_MAX_LENGTH));
    }
    for (i, taint) in device.taints.iter().enumerate() {
        errs.extend(validate_device_taint(taint, &taints_path.index(i)));
    }

    let consumes_path = fld_path.child("consumesCounters");
    if device.consumes_counters.len() > MAX_COUNTER_CONSUMPTIONS_PER_DEVICE {
        errs.push(Error::too_many(
            &consumes_path,
            MAX_COUNTER_CONSUMPTIONS_PER_DEVICE,
        ));
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, consumption) in device.consumes_counters.iter().enumerate() {
        let cp = consumes_path.index(i);
        // Upstream keys the set on `counterSet` (validation.go:830-833).
        if !consumption.counter_set.is_empty() && !seen.insert(consumption.counter_set.as_str()) {
            errs.push(Error::duplicate(
                &cp.child("counterSet"),
                consumption.counter_set.clone(),
            ));
        }
        errs.extend(validate_device_counter_consumption(consumption, &cp));
    }

    errs
}

/// Upstream aliases `validateDeviceName` and `validateCounterName` to
/// `corevalidation.ValidateDNS1123Label`
/// (`pkg/apis/resource/validation/validation.go:73,76`), which reports an empty
/// name as an invalid label rather than as `Required`. Rusternetes already
/// answered a nameless device with `Required`, and a client can act on either,
/// so the empty case keeps the clearer `Required` and everything else takes
/// upstream's label check.
fn validate_device_name(name: &str, fld_path: &Path) -> ErrorList {
    if name.is_empty() {
        return vec![Error::required(fld_path, "")];
    }
    is_dns1123_label(name)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, name.to_string(), msg))
        .collect()
}

/// Port of upstream `validateDeviceTaint`
/// (`pkg/apis/resource/validation/validation.go:1391-1408`). `key` goes through
/// `ValidateLabelName`, which includes the non-empty check, and `effect` is
/// required with a closed set of values.
fn validate_device_taint(taint: &DeviceTaint, fld_path: &Path) -> ErrorList {
    let mut errs = validate_label_name(&taint.key, &fld_path.child("key"));
    match &taint.effect {
        None => errs.push(Error::required(&fld_path.child("effect"), "")),
        Some(effect) => {
            let effect = format!("{effect:?}");
            if !DEVICE_TAINT_EFFECTS.contains(&effect.as_str()) {
                errs.push(Error::not_supported(
                    &fld_path.child("effect"),
                    effect,
                    DEVICE_TAINT_EFFECTS,
                ));
            }
        }
    }
    errs
}

/// Port of upstream `validateDeviceCounterConsumption`
/// (`pkg/apis/resource/validation/validation.go:871-886`).
fn validate_device_counter_consumption(
    consumption: &DeviceCounterConsumption,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = validate_counter_name(&consumption.counter_set, &fld_path.child("counterSet"));
    errs.extend(validate_counters(
        consumption.counters.as_ref().map(|c| c.len()).unwrap_or(0),
        &fld_path.child("counters"),
    ));
    errs
}

/// Port of upstream `validateCounterSet`
/// (`pkg/apis/resource/validation/validation.go:771-787`).
fn validate_counter_set(set: &CounterSet, fld_path: &Path) -> ErrorList {
    let mut errs = validate_counter_name(&set.name, &fld_path.child("name"));
    errs.extend(validate_counters(
        set.counters.as_ref().map(|c| c.len()).unwrap_or(0),
        &fld_path.child("counters"),
    ));
    errs
}

/// The `counters` half both counter validators share: required, and capped at
/// upstream's per-set limit. Individual counter *values* need no check —
/// upstream's `validateDeviceCounter` is "any parsed quantity is valid"
/// (`pkg/apis/resource/validation/validation.go:1087-1090`), and a
/// non-quantity fails to decode here.
fn validate_counters(len: usize, fld_path: &Path) -> ErrorList {
    if len == 0 {
        return vec![Error::required(fld_path, "")];
    }
    if len > MAX_COUNTERS_PER_SET {
        return vec![Error::too_many(fld_path, MAX_COUNTERS_PER_SET)];
    }
    Vec::new()
}

/// Upstream `validateCounterName` = `ValidateDNS1123Label`
/// (`pkg/apis/resource/validation/validation.go:76`), with the explicit
/// non-empty branch its callers add (`:773`, `:874`).
fn validate_counter_name(name: &str, fld_path: &Path) -> ErrorList {
    if name.is_empty() {
        return vec![Error::required(fld_path, "")];
    }
    is_dns1123_label(name)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, name.to_string(), msg))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(json: serde_json::Value) -> ResourceSlice {
        serde_json::from_value(json).unwrap()
    }

    fn base() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "slice-1"},
            "spec": {
                "driver": "dra.example.com",
                "pool": {"name": "pool-1", "generation": 1, "resourceSliceCount": 1},
                "allNodes": true,
                "devices": [{"name": "gpu-0"}]
            }
        })
    }

    fn errs(spec: serde_json::Value) -> Vec<String> {
        let mut v = base();
        v["spec"] = spec;
        validate_resource_slice(&slice(v))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_slice_passes() {
        assert!(validate_resource_slice(&slice(base())).is_empty());
    }

    #[test]
    fn driver_required_and_dns() {
        assert!(errs(serde_json::json!({
            "driver": "", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }))
        .iter()
        .any(|m| m.contains("driver")));
        assert!(errs(serde_json::json!({
            "driver": "Bad_Driver", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }))
        .iter()
        .any(|m| m.contains("driver")));
    }

    #[test]
    fn driver_uses_csi_driver_name_length_cap() {
        // 64 chars: a valid DNS-1123 subdomain, but exceeds the CSI driver-name
        // 63-char cap. DNS-1123-subdomain validation would accept it; the CSI
        // validator must reject it.
        let long = "a".repeat(64);
        let e = errs(serde_json::json!({
            "driver": long,
            "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }));
        assert!(
            e.iter().any(|m| m.contains("driver") && m.contains("63")),
            "expected CSI driver-name length error, got {e:?}"
        );
        // exactly 63 chars is accepted.
        let ok = "a".repeat(63);
        let e2 = errs(serde_json::json!({
            "driver": ok,
            "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }));
        assert!(
            !e2.iter().any(|m| m.contains("driver")),
            "63-char driver should pass, got {e2:?}"
        );
    }

    #[test]
    fn node_name_uses_node_name_validator() {
        // Invalid node name (uppercase / underscore) is rejected via the
        // node-name (DNS-1123 subdomain) validator on the `nodeName` path.
        let e = errs(serde_json::json!({
            "driver": "d.example.com",
            "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "nodeName": "Bad_Node", "devices": []
        }));
        assert!(
            e.iter().any(|m| m.contains("nodeName")),
            "expected nodeName validation error, got {e:?}"
        );
        // A valid node name passes.
        let e2 = errs(serde_json::json!({
            "driver": "d.example.com",
            "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "nodeName": "node-1.example.com", "devices": []
        }));
        assert!(
            !e2.iter().any(|m| m.contains("nodeName")),
            "valid nodeName should pass, got {e2:?}"
        );
    }

    #[test]
    fn pool_bounds() {
        let e = errs(serde_json::json!({
            "driver": "d.example.com",
            "pool": {"name": "p", "generation": -1, "resourceSliceCount": 0},
            "allNodes": true, "devices": []
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("resourceSliceCount") && m.contains("greater than zero")),
            "{e:?}"
        );
        assert!(
            e.iter()
                .any(|m| m.contains("generation") && m.contains("greater than or equal")),
            "{e:?}"
        );
    }

    #[test]
    fn node_selection_exactly_one() {
        // none set
        let none = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "devices": []
        }));
        assert!(
            none.iter().any(|m| m.contains("exactly one of")),
            "{none:?}"
        );
        // two set
        let two = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "nodeName": "node-1", "devices": []
        }));
        assert!(
            two.iter().any(|m| m.contains("multiple fields are set")),
            "{two:?}"
        );
        // allNodes:false invalid
        let f = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": false, "devices": []
        }));
        assert!(f.iter().any(|m| m.contains("set to true")), "{f:?}");
    }

    #[test]
    fn duplicate_device_names_rejected() {
        let e = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": [{"name": "a"}, {"name": "a"}]
        }));
        assert!(
            e.iter().any(|m| m.to_lowercase().contains("duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn update_immutable_driver_pool_node() {
        let old = slice(base());
        // unchanged -> ok
        assert!(validate_resource_slice_update(&slice(base()), &old).is_empty());
        // changed driver -> immutable
        let mut v = base();
        v["spec"]["driver"] = serde_json::json!("other.example.com");
        let errs = validate_resource_slice_update(&slice(v), &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("driver") && e.detail == "field is immutable"),
            "{errs:?}"
        );
        // changed pool name -> immutable
        let mut v2 = base();
        v2["spec"]["pool"]["name"] = serde_json::json!("pool-2");
        let errs2 = validate_resource_slice_update(&slice(v2), &old);
        assert!(
            errs2
                .iter()
                .any(|e| e.field.ends_with("pool.name") && e.detail == "field is immutable"),
            "{errs2:?}"
        );
    }
}
