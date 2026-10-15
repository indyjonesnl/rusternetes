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

use crate::quantity::Quantity;
use crate::resources::dra::{
    CapacityRequestPolicy, CapacityRequestPolicyRange, CounterSet, Device, DeviceCapacity,
    DeviceCounterConsumption, DeviceTaint, DeviceTaintEffect,
};
use crate::resources::{ResourceSlice, ResourceSliceSpec};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain, validate_label_name};
use crate::validation::resourceclaim::too_many_max_items;
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
/// `ResourceSliceMaxCounterSets` (`pkg/apis/resource/types.go:259`).
const RESOURCE_SLICE_MAX_COUNTER_SETS: usize = 8;
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
    errs.extend(validate_pool_name(
        &spec.pool.name,
        &pool_path.child("name"),
    ));
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
        errs.push(too_many_max_items(
            &devices_path,
            spec.devices.len(),
            max_devices,
        ));
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
    // over them with `validateCounterSet`, validation.go:723-727). The size
    // check short-circuits (`validateSet`, :1198-1204).
    let shared_path = fld_path.child("sharedCounters");
    if spec.shared_counters.len() > RESOURCE_SLICE_MAX_COUNTER_SETS {
        errs.push(too_many_max_items(
            &shared_path,
            spec.shared_counters.len(),
            RESOURCE_SLICE_MAX_COUNTER_SETS,
        ));
        return errs;
    }
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
/// consumptions and the capacity request policies. The attribute map, the capacity keys and the per-device node-selection
/// coupling stay in #1442. The binding conditions are validated too
/// (`validation.go:867`).
fn validate_device(device: &Device, fld_path: &Path) -> ErrorList {
    let mut errs = validate_device_name(&device.name, &fld_path.child("name"));

    let taints_path = fld_path.child("taints");
    if device.taints.len() > DEVICE_TAINTS_MAX_LENGTH {
        errs.push(too_many_max_items(
            &taints_path,
            device.taints.len(),
            DEVICE_TAINTS_MAX_LENGTH,
        ));
    }
    for (i, taint) in device.taints.iter().enumerate() {
        errs.extend(validate_device_taint(taint, &taints_path.index(i)));
    }

    let consumes_path = fld_path.child("consumesCounters");
    if device.consumes_counters.len() > MAX_COUNTER_CONSUMPTIONS_PER_DEVICE {
        errs.push(too_many_max_items(
            &consumes_path,
            device.consumes_counters.len(),
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

    // validation.go:812-817: the capacity map, validated as multi- or
    // single-allocatable. The map key validator (`validateQualifiedName`) and the
    // attributes stay in #1442.
    let allow_multiple = device.allow_multiple_allocations == Some(true);
    if let Some(capacity) = &device.capacity {
        let cap_path = fld_path.child("capacity");
        for (key, cap) in capacity {
            let key_path = cap_path.key(key.clone());
            if allow_multiple {
                errs.extend(validate_multi_allocatable_device_capacity(cap, &key_path));
            } else {
                errs.extend(validate_single_allocatable_device_capacity(cap, &key_path));
            }
        }
    }

    errs.extend(validate_device_binding_parameters(
        &device.binding_conditions,
        &device.binding_failure_conditions,
        fld_path,
    ));

    errs
}

/// Upstream `CapacityRequestPolicyDiscreteMaxOptions`
/// (`pkg/apis/resource/types.go:466`).
const CAPACITY_REQUEST_POLICY_DISCRETE_MAX_OPTIONS: usize = 10;

fn q(s: &str) -> Option<Quantity> {
    Quantity::parse(s).ok()
}

/// Port of `validateMultiAllocatableDeviceCapacity` (`validation.go:956-964`).
fn validate_multi_allocatable_device_capacity(cap: &DeviceCapacity, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if let Some(policy) = &cap.request_policy {
        if let Some(max) = q(&cap.value) {
            errs.extend(validate_request_policy(
                &max,
                policy,
                &fld_path.child("requestPolicy"),
            ));
        }
    }
    errs
}

/// Port of `validateSingleAllocatableDeviceCapacity` (`validation.go:966-973`).
fn validate_single_allocatable_device_capacity(cap: &DeviceCapacity, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if cap.request_policy.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("requestPolicy"),
            "allowMultipleAllocations must be true",
        ));
    }
    errs
}

/// Port of `validateRequestPolicy` (`validation.go:975-984`).
fn validate_request_policy(
    max_capacity: &Quantity,
    policy: &CapacityRequestPolicy,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if !policy.valid_values.is_empty() && policy.valid_range.is_some() {
        errs.push(Error::forbidden(
            fld_path,
            r#"exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time"#,
        ));
    } else {
        errs.extend(validate_valid_request_values(
            max_capacity,
            policy,
            fld_path,
        ));
    }
    errs
}

/// Port of `validateValidRequestValues` (`validation.go:986-1004`).
fn validate_valid_request_values(
    max_capacity: &Quantity,
    policy: &CapacityRequestPolicy,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if !policy.valid_values.is_empty() {
        match policy.default.as_deref().and_then(q) {
            None => errs.push(Error::required(
                &fld_path.child("default"),
                "required when validValues is defined",
            )),
            Some(default) => errs.extend(validate_request_policy_valid_values(
                &default,
                max_capacity,
                &policy.valid_values,
                &fld_path.child("validValues"),
            )),
        }
    } else if let Some(range) = &policy.valid_range {
        match policy.default.as_deref().and_then(q) {
            None => errs.push(Error::required(
                &fld_path.child("default"),
                "required when validRange is defined",
            )),
            Some(default) => errs.extend(validate_request_policy_range(
                &default,
                max_capacity,
                range,
                &fld_path.child("validRange"),
            )),
        }
    }
    errs
}

/// Port of `validateRequestPolicyValidValues` (`validation.go:1006-1033`),
/// including the `validateSet` size short-circuit (`validation.go:1193-1203`)
/// and the `quantityKey` duplicate check (`:1232`).
fn validate_request_policy_valid_values(
    default: &Quantity,
    max_capacity: &Quantity,
    valid_values: &[String],
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let vals: Vec<Quantity> = valid_values.iter().filter_map(|v| q(v)).collect();
    for i in 0..vals.len().saturating_sub(1) {
        if vals[i].cmp_value(&vals[i + 1]) == std::cmp::Ordering::Greater {
            errs.push(Error::invalid(
                &fld_path.index(i + 1),
                vals[i + 1].canonical_string(),
                "values must be sorted in ascending order",
            ));
        }
    }
    if vals.len() > CAPACITY_REQUEST_POLICY_DISCRETE_MAX_OPTIONS {
        errs.push(too_many_max_items(
            fld_path,
            vals.len(),
            CAPACITY_REQUEST_POLICY_DISCRETE_MAX_OPTIONS,
        ));
        return errs;
    }
    let mut found_default = false;
    let mut seen: HashSet<i128> = HashSet::new();
    for (i, option) in vals.iter().enumerate() {
        let ip = fld_path.index(i);
        if option.cmp_value(max_capacity) == std::cmp::Ordering::Greater {
            errs.push(Error::invalid(
                &ip,
                option.canonical_string(),
                format!(
                    "option is larger than capacity value: {}",
                    max_capacity.canonical_string()
                ),
            ));
        }
        if option.cmp_value(default) == std::cmp::Ordering::Equal {
            found_default = true;
        }
    }
    for (i, option) in vals.iter().enumerate() {
        let key = option.value();
        if !seen.insert(key) {
            errs.push(Error::duplicate(&fld_path.index(i), key.to_string()));
        }
    }
    if !found_default {
        errs.push(Error::invalid(
            fld_path,
            default.canonical_string(),
            "default value is not valid according to the requestPolicy",
        ));
    }
    errs
}

/// Port of `validateRequestPolicyRange` (`validation.go:1035-1075`).
fn validate_request_policy_range(
    default: &Quantity,
    max_capacity: &Quantity,
    range: &CapacityRequestPolicyRange,
    fld_path: &Path,
) -> ErrorList {
    use std::cmp::Ordering::Greater;
    let mut errs = ErrorList::new();
    let Some(min) = range.min.as_deref().and_then(q) else {
        errs.push(Error::required(
            &fld_path.child("min"),
            "required when validRange is defined",
        ));
        return errs;
    };
    let max_s = max_capacity.canonical_string();
    if min.cmp_value(max_capacity) == Greater {
        errs.push(Error::invalid(
            &fld_path.child("min"),
            min.canonical_string(),
            format!("min is larger than capacity value: {max_s}"),
        ));
    }
    if default.cmp_value(&min) == std::cmp::Ordering::Less {
        errs.push(Error::invalid(
            &fld_path.child("min"),
            default.canonical_string(),
            format!("default is less than min: {}", min.canonical_string()),
        ));
    }
    let max = range.max.as_deref().and_then(q);
    if let Some(max) = &max {
        if min.cmp_value(max) == Greater {
            errs.push(Error::invalid(
                &fld_path.child("max"),
                min.canonical_string(),
                format!("min is larger than max: {}", max.canonical_string()),
            ));
        }
        if max.cmp_value(max_capacity) == Greater {
            errs.push(Error::invalid(
                &fld_path.child("max"),
                max.canonical_string(),
                format!("max is larger than capacity value: {max_s}"),
            ));
        }
        if default.cmp_value(max) == Greater {
            errs.push(Error::invalid(
                &fld_path.child("max"),
                default.canonical_string(),
                format!("default is more than max: {}", max.canonical_string()),
            ));
        }
    }
    if let Some(step) = range.step.as_deref().and_then(q) {
        if let Some(added) = min.add(&step) {
            if added.cmp_value(max_capacity) == Greater {
                errs.push(Error::invalid(
                    &fld_path.child("step"),
                    step.canonical_string(),
                    format!(
                        "one step {} is larger than capacity value: {max_s}",
                        added.canonical_string()
                    ),
                ));
            }
        }
        errs.extend(validate_request_policy_range_step(
            default,
            &min,
            &step,
            &fld_path.child("step"),
        ));
        if let Some(max) = &max {
            errs.extend(validate_request_policy_range_step(
                max,
                &min,
                &step,
                &fld_path.child("step"),
            ));
        }
    }
    errs
}

/// Port of `validateRequestPolicyRangeStep` (`validation.go:1077-1088`).
fn validate_request_policy_range_step(
    value: &Quantity,
    min: &Quantity,
    step: &Quantity,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let step_val = step.value();
    // Go's `added%stepVal` would panic on a zero step; a zero step is rejected
    // by declarative validation upstream, so skip rather than crash.
    if step_val == 0 {
        return errs;
    }
    let added = value.value() - min.value();
    if added % step_val != 0 {
        errs.push(Error::invalid(
            fld_path,
            value.canonical_string(),
            format!(
                "value is not a multiple of a given step ({}) from ({})",
                step.canonical_string(),
                min.canonical_string()
            ),
        ));
    }
    errs
}

/// Upstream `BindingConditionsMaxSize` / `BindingFailureConditionsMaxSize`
/// (`pkg/apis/resource/types.go:254-255`).
const BINDING_CONDITIONS_MAX_SIZE: usize = 4;

/// The bad value for an empty list. Upstream passes the Go slice itself, a
/// `[]string(nil)` when the field was absent, which `json.Marshal`s as `null`
/// (`validation_resourceclaim_test.go:1968`, `validation_resourceslice_test.go:977`).
fn nil_slice(list: &[String]) -> serde_json::Value {
    if list.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::from(list.to_vec())
    }
}

/// Port of upstream `validateDeviceBindingParameters`
/// (`pkg/apis/resource/validation/validation.go:1450-1485`), shared with
/// `AllocationResult` validation upstream. `validateSlice` short-circuits on
/// the size cap (`:1176-1187`), so an oversized list reports only `TooMany`.
pub(crate) fn validate_device_binding_parameters(
    binding_conditions: &[String],
    binding_failure_conditions: &[String],
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let bc_path = fld_path.child("bindingConditions");
    let bfc_path = fld_path.child("bindingFailureConditions");
    for (list, path) in [
        (binding_conditions, &bc_path),
        (binding_failure_conditions, &bfc_path),
    ] {
        if list.len() > BINDING_CONDITIONS_MAX_SIZE {
            errs.push(too_many_max_items(
                path,
                list.len(),
                BINDING_CONDITIONS_MAX_SIZE,
            ));
        } else {
            for (i, c) in list.iter().enumerate() {
                errs.extend(validate_label_name(c, &path.index(i)));
            }
        }
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for (i, c) in binding_conditions.iter().enumerate() {
        if !seen.insert(c.as_str()) {
            errs.push(Error::duplicate(&bc_path.index(i), c.clone()));
        }
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, c) in binding_failure_conditions.iter().enumerate() {
        if !seen.insert(c.as_str()) {
            errs.push(Error::duplicate(&bfc_path.index(i), c.clone()));
        }
        if binding_conditions.contains(c) {
            errs.push(Error::invalid(
                &bfc_path.index(i),
                c.clone(),
                "bindingFailureConditions must not overlap with bindingConditions",
            ));
        }
    }

    if binding_conditions.is_empty() && !binding_failure_conditions.is_empty() {
        errs.push(Error::invalid(
            &bc_path,
            nil_slice(binding_conditions),
            "bindingConditions are required to use bindingFailureConditions",
        ));
    }
    if binding_failure_conditions.is_empty() && !binding_conditions.is_empty() {
        errs.push(Error::invalid(
            &bfc_path,
            nil_slice(binding_failure_conditions),
            "bindingFailureConditions are required to use bindingConditions",
        ));
    }
    errs
}

/// Port of upstream `validateDeviceName`, an alias of
/// `corevalidation.ValidateDNS1123Label`
/// (`pkg/apis/resource/validation/validation.go:73`;
/// `pkg/apis/core/validation/validation.go:158-164`). It has no empty-name
/// branch, so an empty name is an Invalid label (bad value `""`), not Required.
pub(crate) fn validate_device_name(name: &str, fld_path: &Path) -> ErrorList {
    is_dns1123_label(name)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, name.to_string(), msg))
        .collect()
}

/// Port of upstream `validatePoolName`
/// (`pkg/apis/resource/validation/validation.go:82-96`): required, at most
/// `PoolNameMaxLength`, and every `/`-separated part a DNS-1123 subdomain.
/// Shared with the allocation results in a `ResourceClaim`'s status, which
/// upstream validates with this same function.
pub(crate) fn validate_pool_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    if name.len() > POOL_NAME_MAX_LENGTH {
        errs.push(Error::too_long(fld_path, POOL_NAME_MAX_LENGTH));
    }
    for part in name.split('/') {
        for msg in is_dns1123_subdomain(part) {
            errs.push(Error::invalid(fld_path, name.to_string(), msg));
        }
    }
    errs
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
            // validation.go:1402-1403: `!validDeviceTaintEffects.Has(...)`.
            if let DeviceTaintEffect::Unknown(effect) = effect {
                errs.push(Error::not_supported(
                    &fld_path.child("effect"),
                    effect.clone(),
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
        return vec![Error::too_many(fld_path, len, MAX_COUNTERS_PER_SET)];
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

    /// Upstream `validateDeviceName = corevalidation.ValidateDNS1123Label`
    /// (`pkg/apis/resource/validation/validation.go:73`) has no empty-name
    /// branch, so an empty name is an Invalid label, never Required.
    #[test]
    fn empty_device_name_is_invalid_label_not_required() {
        let e = validate_device_name(
            "",
            &Path::new("spec").child("devices").index(0).child("name"),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert_eq!(
            e[0].to_string(),
            "spec.devices[0].name: Invalid value: \"\": a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"
        );
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

    // Port of the binding-conditions cases in upstream
    // `pkg/apis/resource/validation/validation_resourceslice_test.go:958-995`.
    fn binding_errs(bc: serde_json::Value, bfc: serde_json::Value) -> Vec<String> {
        let mut dev = serde_json::json!({"name": "gpu-0"});
        if !bc.is_null() {
            dev["bindingConditions"] = bc;
        }
        if !bfc.is_null() {
            dev["bindingFailureConditions"] = bfc;
        }
        errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": [dev]
        }))
    }

    #[test]
    fn binding_conditions_valid() {
        let e = binding_errs(
            serde_json::json!(["example.com/condition1", "condition2"]),
            serde_json::json!(["example.com/condition3", "condition4"]),
        );
        assert!(e.is_empty(), "{e:?}");
    }

    #[test]
    fn binding_conditions_too_many() {
        let e = binding_errs(
            serde_json::json!(["c1", "c2", "c3", "c4", "c5"]),
            serde_json::json!(["c6", "c7"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("spec.devices[0].bindingConditions"), "{e:?}");
        let e = binding_errs(
            serde_json::json!(["c1", "c2"]),
            serde_json::json!(["c3", "c4", "c5", "c6", "c7"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingFailureConditions"),
            "{e:?}"
        );
    }

    #[test]
    fn binding_conditions_invalid_names() {
        let e = binding_errs(
            serde_json::json!(["condition1", "condition2!"]),
            serde_json::json!(["condition3", "condition4"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingConditions[1]") && e[0].contains("condition2!"),
            "{e:?}"
        );
        let e = binding_errs(
            serde_json::json!(["condition1", "condition2"]),
            serde_json::json!(["condition3!", "condition4"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingFailureConditions[0]"),
            "{e:?}"
        );
    }

    #[test]
    fn binding_conditions_pairing_required() {
        let e = binding_errs(serde_json::Value::Null, serde_json::json!(["c1", "c2"]));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingConditions")
                && e[0].contains("bindingConditions are required to use bindingFailureConditions"),
            "{e:?}"
        );
        let e = binding_errs(serde_json::json!(["c1", "c2"]), serde_json::Value::Null);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingFailureConditions")
                && e[0].contains("bindingFailureConditions are required to use bindingConditions"),
            "{e:?}"
        );
    }

    #[test]
    fn binding_conditions_duplicates_and_overlap() {
        let e = binding_errs(
            serde_json::json!(["condition1", "condition1"]),
            serde_json::json!(["condition2", "condition3"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingConditions[1]") && e[0].contains("Duplicate"),
            "{e:?}"
        );
        let e = binding_errs(
            serde_json::json!(["condition1", "condition2"]),
            serde_json::json!(["condition3", "condition3"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingFailureConditions[1]")
                && e[0].contains("Duplicate"),
            "{e:?}"
        );
        let e = binding_errs(
            serde_json::json!(["condition1", "condition2"]),
            serde_json::json!(["condition1", "condition3"]),
        );
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("spec.devices[0].bindingFailureConditions[0]")
                && e[0].contains("must not overlap with bindingConditions"),
            "{e:?}"
        );
    }

    // ---- TooMany origin (#3412) ------------------------------------------
    //
    // `validateSlice` / `validateSet` tag size breaches
    // `.WithOrigin("maxItems")` (`validation.go:1181`, `:1198`); `validateMap`
    // for a counter set's `counters` (`:1246`) does not.
    fn too_many_origins(spec: serde_json::Value) -> Vec<(String, String)> {
        let mut v = base();
        v["spec"] = spec;
        let mut out: Vec<(String, String)> = validate_resource_slice(&slice(v))
            .into_iter()
            .filter(|e| e.error_type == crate::validation::field::ErrorType::TooMany)
            .map(|e| (e.field, e.origin))
            .collect();
        out.sort();
        out
    }

    fn spec_with_devices(devices: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({
            "driver": "dra.example.com",
            "pool": {"name": "pool-1", "generation": 1, "resourceSliceCount": 1},
            "allNodes": true,
            "devices": devices,
        })
    }

    #[test]
    fn devices_too_many_carries_max_items_origin() {
        let devices: Vec<_> = (0..129)
            .map(|i| serde_json::json!({"name": format!("d{i}")}))
            .collect();
        assert_eq!(
            too_many_origins(spec_with_devices(devices)),
            vec![("spec.devices".to_string(), "maxItems".to_string())]
        );
    }

    #[test]
    fn device_lists_too_many_carry_max_items_origin() {
        let taints: Vec<_> = (0..17)
            .map(|i| serde_json::json!({"key": format!("k{i}"), "effect": "NoSchedule"}))
            .collect();
        let consumes: Vec<_> = (0..3)
            .map(|i| serde_json::json!({"counterSet": format!("cs{i}"), "counters": {"c": {"value": "1"}}}))
            .collect();
        let conds: Vec<String> = (0..5).map(|i| format!("cond{i}")).collect();
        let device = serde_json::json!({
            "name": "d0", "taints": taints, "consumesCounters": consumes,
            "bindingConditions": conds, "bindingFailureConditions": conds,
        });
        let m = |f: &str| (f.to_string(), "maxItems".to_string());
        assert_eq!(
            too_many_origins(spec_with_devices(vec![device])),
            vec![
                m("spec.devices[0].bindingConditions"),
                m("spec.devices[0].bindingFailureConditions"),
                m("spec.devices[0].consumesCounters"),
                m("spec.devices[0].taints"),
            ]
        );
    }

    #[test]
    fn counter_set_counters_too_many_has_no_origin() {
        let counters: serde_json::Map<String, serde_json::Value> = (0..33)
            .map(|i| (format!("c{i}"), serde_json::json!({"value": "1"})))
            .collect();
        let mut spec = spec_with_devices(vec![serde_json::json!({"name": "d0"})]);
        spec["sharedCounters"] = serde_json::json!([{"name": "cs", "counters": counters}]);
        assert_eq!(
            too_many_origins(spec),
            vec![("spec.sharedCounters[0].counters".to_string(), String::new())]
        );
    }

    /// Port of `max-shared-counters` / `too-many-shared-counters`
    /// (validation_resourceslice_test.go:783-800): `validateSet` over
    /// `sharedCounters` (`validation.go:723`) caps at
    /// `ResourceSliceMaxCounterSets` (8) with `WithOrigin("maxItems")`.
    fn shared_counters(n: usize) -> serde_json::Value {
        let sets: Vec<_> = (0..n)
            .map(|i| serde_json::json!({"name": format!("counterset-{i}"), "counters": {"c": {"value": "1"}}}))
            .collect();
        let mut spec = spec_with_devices(vec![]);
        spec["sharedCounters"] = serde_json::json!(sets);
        spec
    }

    #[test]
    fn shared_counters_over_cap_carry_max_items_origin() {
        assert_eq!(
            too_many_origins(shared_counters(9)),
            vec![("spec.sharedCounters".to_string(), "maxItems".to_string())]
        );
    }

    #[test]
    fn shared_counters_at_cap_are_valid() {
        let mut v = base();
        v["spec"] = shared_counters(8);
        let got = validate_resource_slice(&slice(v));
        assert!(got.is_empty(), "{got:?}");
    }

    /// `validateSet` short-circuits: an over-cap list yields only TooMany.
    #[test]
    fn shared_counters_over_cap_short_circuit() {
        let mut spec = shared_counters(9);
        spec["sharedCounters"][0]["name"] = serde_json::json!("Bad Name");
        let mut v = base();
        v["spec"] = spec;
        let got = validate_resource_slice(&slice(v));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].field, "spec.sharedCounters");
    }

    /// Capacity request-policy cases. Upstream
    /// `validateRequestPolicyValidValues` (`validation.go:1006-1033`) runs
    /// `validateSet(validValues, CapacityRequestPolicyDiscreteMaxOptions, ...)`
    /// (`types.go:466` = 10).
    fn cap_errs(allow_multiple: bool, capacity: serde_json::Value) -> Vec<(String, String)> {
        let mut spec = spec_with_devices(vec![serde_json::json!({
            "name": "gpu-0",
            "allowMultipleAllocations": allow_multiple,
            "capacity": {"cap": capacity},
        })]);
        spec["allNodes"] = serde_json::json!(true);
        let mut v = base();
        v["spec"] = spec;
        validate_resource_slice(&slice(v))
            .into_iter()
            .map(|e| (e.field.clone(), e.to_string()))
            .collect()
    }

    fn valid_values(n: usize) -> serde_json::Value {
        let vals: Vec<String> = (1..=n).map(|i| format!("{i}Mi")).collect();
        serde_json::json!({
            "value": "1Gi",
            "requestPolicy": {"default": "1Mi", "validValues": vals},
        })
    }

    #[test]
    fn valid_values_over_cap_is_too_many_with_origin() {
        let mut v = base();
        v["spec"] = spec_with_devices(vec![serde_json::json!({
            "name": "gpu-0", "allowMultipleAllocations": true,
            "capacity": {"cap": valid_values(11)},
        })]);
        let got = validate_resource_slice(&slice(v));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(
            got[0].field,
            "spec.devices[0].capacity[cap].requestPolicy.validValues"
        );
        assert_eq!(got[0].origin, "maxItems");
    }

    #[test]
    fn valid_values_at_cap_is_valid() {
        assert!(cap_errs(true, valid_values(10)).is_empty());
    }

    #[test]
    fn request_policy_forbidden_without_allow_multiple_allocations() {
        let got = cap_errs(true, valid_values(1));
        assert!(got.is_empty(), "{got:?}");
        let got = cap_errs(false, valid_values(1));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].0, "spec.devices[0].capacity[cap].requestPolicy");
        assert!(got[0].1.contains("allowMultipleAllocations must be true"));
    }

    #[test]
    fn request_policy_valid_values_and_valid_range_exclusive() {
        let got = cap_errs(
            true,
            serde_json::json!({"value": "1Gi", "requestPolicy": {
                "default": "1Mi", "validValues": ["1Mi"], "validRange": {"min": "1Mi"}}}),
        );
        assert_eq!(got.len(), 1, "{got:?}");
        assert!(got[0].1.contains(
            r#"exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time"#
        ));
    }

    #[test]
    fn request_policy_valid_values_rules() {
        // Unsorted, option larger than capacity, default not among the options.
        let got = cap_errs(
            true,
            serde_json::json!({"value": "1Gi", "requestPolicy": {
                "default": "3Mi", "validValues": ["2Mi", "1Mi", "2Gi"]}}),
        );
        let msgs: Vec<_> = got.iter().map(|g| g.1.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("values must be sorted in ascending order")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("option is larger than capacity value: 1Gi")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("default value is not valid according to the requestPolicy")),
            "{msgs:?}"
        );
    }

    #[test]
    fn request_policy_default_required() {
        let got = cap_errs(
            true,
            serde_json::json!({"value": "1Gi", "requestPolicy": {"validValues": ["1Mi"]}}),
        );
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(
            got[0].0,
            "spec.devices[0].capacity[cap].requestPolicy.default"
        );
        assert!(got[0].1.contains("required when validValues is defined"));
    }

    #[test]
    fn request_policy_range_rules() {
        let got = cap_errs(
            true,
            serde_json::json!({"value": "10", "requestPolicy": {
                "default": "1", "validRange": {"min": "2", "max": "20", "step": "3"}}}),
        );
        let msgs: Vec<_> = got.iter().map(|g| g.1.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("default is less than min: 2")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("max is larger than capacity value: 10")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("value is not a multiple of a given step (3) from (2)")),
            "{msgs:?}"
        );
    }

    #[test]
    fn request_policy_range_min_required() {
        let got = cap_errs(
            true,
            serde_json::json!({"value": "1Gi", "requestPolicy": {
                "default": "1Mi", "validRange": {"max": "1Mi"}}}),
        );
        assert_eq!(got.len(), 1, "{got:?}");
        assert!(got[0].0.ends_with("validRange.min"));
    }
}
