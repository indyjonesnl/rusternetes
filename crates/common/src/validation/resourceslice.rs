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
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
};
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
        errs.extend(validate_device_taint(taint, None, &taints_path.index(i)));
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
pub fn validate_multi_allocatable_device_capacity(
    cap: &DeviceCapacity,
    fld_path: &Path,
) -> ErrorList {
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
    // No zero-step guard, deliberately: upstream has none (`Step` carries no
    // validation tag and the imperative validation runs first,
    // `resourceslice/strategy.go:63`), so `added%stepVal` (`validation.go:1085`)
    // is an integer divide-by-zero panic that `WithPanicRecovery` answers 500
    // (`apiserver/pkg/server/filters/wrap.go:30-58`). `%` panics identically
    // here and the router's `CatchPanicLayer` gives the same 500 (#3250).
    let added = value.value().wrapping_sub(min.value());
    if added.wrapping_rem(step_val) != 0 {
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
pub fn validate_device_binding_parameters(
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
pub fn validate_device_name(name: &str, fld_path: &Path) -> ErrorList {
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
pub fn validate_pool_name(name: &str, fld_path: &Path) -> ErrorList {
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
/// `ValidateLabelName`, which includes the non-empty check, a non-empty `value`
/// through `validateLabelValue`, and `effect` is required with a closed set of
/// values, checked only for a new taint or one whose effect changed
/// (`oldTaint == nil || oldTaint.Effect != taint.Effect`).
pub fn validate_device_taint(
    taint: &DeviceTaint,
    old_taint: Option<&DeviceTaint>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = validate_label_name(&taint.key, &fld_path.child("key"));
    if let Some(value) = taint.value.as_deref().filter(|v| !v.is_empty()) {
        // validation.go:1438-1447 `validateLabelValue`.
        for msg in is_valid_label_value(value) {
            errs.push(Error::invalid(
                &fld_path.child("value"),
                value.to_string(),
                msg,
            ));
        }
    }
    if old_taint.is_none_or(|old| old.effect != taint.effect) {
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
