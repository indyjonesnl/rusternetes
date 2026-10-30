//! Unit tests moved from `src/validation/resourceslice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::dra::{
        CapacityRequestPolicy, CapacityRequestPolicyRange, CounterSet, Device, DeviceCapacity,
        DeviceCounterConsumption, DeviceTaint, DeviceTaintEffect,
    };
    pub use rusternetes_common::resources::{ResourceSlice, ResourceSliceSpec};
    pub use rusternetes_common::validation::csinode::validate_csi_driver_name;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
    };
    pub use rusternetes_common::validation::resourceclaim::too_many_max_items;
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::resourceslice::*;

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
            .filter(|e| e.error_type == rusternetes_common::validation::field::ErrorType::TooMany)
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

/// Port of upstream `TestValidateDeviceCapacity`
/// (`pkg/apis/resource/validation/validation_device_capacity_test.go:53-198`,
/// 21 scenarios), run through `validateMultiAllocatableDeviceCapacity`, plus
/// the zero-step behaviour (#3250).
mod device_capacity_scenarios {
    use super::__parent::*;
    use rusternetes_common::validation::resourceslice::*;
    use serde_json::{json, Value};

    const ONE: &str = "1Gi";
    const TWO: &str = "2Gi";
    const MAX: &str = "10Gi";
    const OVER: &str = "20Gi";

    /// `testDeviceCapacity` + `testCapacityRequestPolicy`: `default`,
    /// `validValues` and `validRange` are only present when given.
    fn capacity(value: &str, policy: Option<Value>) -> DeviceCapacity {
        let mut v = json!({ "value": value });
        if let Some(p) = policy {
            v["requestPolicy"] = p;
        }
        serde_json::from_value(v).unwrap()
    }

    fn policy(
        default: Option<&str>,
        valid_values: Option<Vec<&str>>,
        range: Option<Value>,
    ) -> Value {
        let mut p = json!({});
        if let Some(d) = default {
            p["default"] = json!(d);
        }
        if let Some(vv) = valid_values {
            p["validValues"] = json!(vv);
        }
        if let Some(r) = range {
            p["validRange"] = r;
        }
        p
    }

    /// `testValidRange`.
    fn range(min: Option<&str>, max: Option<&str>, step: Option<&str>) -> Value {
        let mut r = json!({});
        if let Some(m) = min {
            r["min"] = json!(m);
        }
        if let Some(m) = max {
            r["max"] = json!(m);
        }
        if let Some(s) = step {
            r["step"] = json!(s);
        }
        r
    }

    fn run(cap: &DeviceCapacity) -> ErrorList {
        validate_multi_allocatable_device_capacity(
            cap,
            &Path::new("spec").child("devices").child("capacity"),
        )
    }

    #[test]
    fn validate_device_capacity_scenarios() {
        let capacity_field = Path::new("spec").child("devices").child("capacity");
        let policy_field = capacity_field.child("requestPolicy");
        let valid_values_field = policy_field.child("validValues");
        let valid_range_field = policy_field.child("validRange");

        let scenarios: Vec<(&str, DeviceCapacity, ErrorList)> = vec![
            ("no-policy", capacity(ONE, None), vec![]),
            (
                "policy-without-default",
                capacity(ONE, Some(policy(None, None, None))),
                vec![],
            ),
            (
                "policy-with-valid-values-without-default",
                capacity(ONE, Some(policy(None, Some(vec![ONE]), None))),
                vec![Error::required(
                    &policy_field.child("default"),
                    "required when validValues is defined",
                )],
            ),
            (
                "policy-with-valid-range-without-default",
                capacity(ONE, Some(policy(None, None, Some(range(Some(ONE), None, None))))),
                vec![Error::required(
                    &policy_field.child("default"),
                    "required when validRange is defined",
                )],
            ),
            (
                "policy-with-valid-range-without-min",
                capacity(ONE, Some(policy(Some(ONE), None, Some(range(None, None, None))))),
                vec![Error::required(
                    &valid_range_field.child("min"),
                    "required when validRange is defined",
                )],
            ),
            (
                "valid-simple-range",
                capacity(MAX, Some(policy(Some(ONE), None, Some(range(Some(ONE), None, None))))),
                vec![],
            ),
            (
                "valid-range-with-max",
                capacity(MAX, Some(policy(Some(ONE), None, Some(range(Some(ONE), Some(MAX), None))))),
                vec![],
            ),
            (
                "valid-range-with-step",
                capacity(MAX, Some(policy(Some(ONE), None, Some(range(Some(ONE), None, Some(ONE)))))),
                vec![],
            ),
            (
                "valid-range-with-max-and-step",
                capacity(
                    MAX,
                    Some(policy(Some(ONE), None, Some(range(Some(ONE), Some(MAX), Some(ONE))))),
                ),
                vec![],
            ),
            (
                "valid-single-option",
                capacity(MAX, Some(policy(Some(ONE), Some(vec![ONE]), None))),
                vec![],
            ),
            (
                "valid-two-options",
                capacity(MAX, Some(policy(Some(ONE), Some(vec![ONE, MAX]), None))),
                vec![],
            ),
            (
                "default-without-policy",
                capacity(MAX, Some(policy(Some(ONE), None, None))),
                vec![],
            ),
            (
                "more-than-one-policy",
                capacity(
                    MAX,
                    Some(policy(Some(ONE), Some(vec![ONE]), Some(range(Some(ONE), None, None)))),
                ),
                vec![Error::forbidden(
                    &policy_field,
                    "exactly one policy can be specified, cannot specify \"validValues\" and \"validRange\" at the same time",
                )],
            ),
            (
                "invalid-options",
                capacity(MAX, Some(policy(Some(ONE), Some(vec![OVER]), None))),
                vec![
                    Error::invalid(
                        &valid_values_field.index(0),
                        "20Gi",
                        "option is larger than capacity value: 10Gi",
                    ),
                    Error::invalid(
                        &valid_values_field,
                        "1Gi",
                        "default value is not valid according to the requestPolicy",
                    ),
                ],
            ),
            (
                "invalid-options-duplicate",
                capacity(MAX, Some(policy(Some(ONE), Some(vec![ONE, ONE]), None))),
                vec![Error::duplicate(&valid_values_field.index(1), "1073741824")],
            ),
            (
                "invalid-options-duplicate-normalized",
                capacity(MAX, Some(policy(Some("1Ki"), Some(vec!["1Ki", "1024"]), None))),
                vec![Error::duplicate(&valid_values_field.index(1), "1024")],
            ),
            (
                "invalid-options-unsort",
                capacity(MAX, Some(policy(Some(ONE), Some(vec![TWO, ONE]), None))),
                vec![Error::invalid(
                    &valid_values_field.index(1),
                    ONE,
                    "values must be sorted in ascending order",
                )],
            ),
            (
                "invalid-range-large-min-small-max",
                capacity(
                    MAX,
                    Some(policy(Some(TWO), None, Some(range(Some(OVER), Some(ONE), None)))),
                ),
                vec![
                    Error::invalid(
                        &valid_range_field.child("min"),
                        "20Gi",
                        "min is larger than capacity value: 10Gi",
                    ),
                    Error::invalid(
                        &valid_range_field.child("min"),
                        "2Gi",
                        "default is less than min: 20Gi",
                    ),
                    Error::invalid(
                        &valid_range_field.child("max"),
                        "20Gi",
                        "min is larger than max: 1Gi",
                    ),
                    Error::invalid(
                        &valid_range_field.child("max"),
                        "2Gi",
                        "default is more than max: 1Gi",
                    ),
                ],
            ),
            (
                "invalid-range-large-max",
                capacity(
                    MAX,
                    Some(policy(Some(ONE), None, Some(range(Some(ONE), Some(OVER), None)))),
                ),
                vec![Error::invalid(
                    &valid_range_field.child("max"),
                    "20Gi",
                    "max is larger than capacity value: 10Gi",
                )],
            ),
            (
                "invalid-range-multiple-of-step",
                capacity(
                    MAX,
                    Some(policy(Some(TWO), None, Some(range(Some(ONE), Some(MAX), Some(TWO))))),
                ),
                vec![
                    Error::invalid(
                        &valid_range_field.child("step"),
                        "2Gi",
                        "value is not a multiple of a given step (2Gi) from (1Gi)",
                    ),
                    Error::invalid(
                        &valid_range_field.child("step"),
                        "10Gi",
                        "value is not a multiple of a given step (2Gi) from (1Gi)",
                    ),
                ],
            ),
            (
                "invalid-range-large-step",
                capacity(
                    MAX,
                    Some(policy(Some(ONE), None, Some(range(Some(ONE), None, Some(MAX))))),
                ),
                vec![Error::invalid(
                    &valid_range_field.child("step"),
                    "10Gi",
                    "one step 11Gi is larger than capacity value: 10Gi",
                )],
            ),
        ];
        assert_eq!(scenarios.len(), 21);
        for (name, cap, want) in scenarios {
            let got = run(&cap);
            assert_eq!(
                got.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                want.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "scenario {name}"
            );
        }
    }

    /// An unparsable quantity never reaches validation: upstream's typed
    /// `Quantity` fails decode, so `q()` returning `None` is unreachable. Ours
    /// must fail decode too, for every quantity-typed policy field.
    #[test]
    fn unparsable_policy_quantities_fail_decode() {
        for p in [
            json!({ "default": "abc" }),
            json!({ "validValues": ["1Gi", "abc"] }),
            json!({ "validRange": { "min": "abc" } }),
            json!({ "validRange": { "min": "1", "max": "abc" } }),
            json!({ "validRange": { "min": "1", "step": "abc" } }),
        ] {
            let r: Result<DeviceCapacity, _> =
                serde_json::from_value(json!({ "value": "1Gi", "requestPolicy": p.clone() }));
            assert!(r.is_err(), "{p} must fail decode");
        }
    }

    /// A zero `validRange.step` is NOT rejected before the imperative
    /// validation: `CapacityRequestPolicyRange.Step` has no validation tag
    /// (`pkg/apis/resource/types.go:521-546`; `zz_generated.validations.go:242`
    /// "Device.Capacity has no validation") and `ResourceSliceStrategy.Validate`
    /// runs `ValidateResourceSlice` first (`resourceslice/strategy.go:63`). Go
    /// then evaluates `added%stepVal` with `stepVal == 0`
    /// (`validation.go:1085`): an integer divide-by-zero panic, answered 500 by
    /// `WithPanicRecovery`. Rust's `%` panics identically (#3250: replicate).
    #[test]
    #[should_panic(expected = "divisor of zero")]
    fn zero_step_panics_like_go() {
        let cap = capacity(
            MAX,
            Some(policy(
                Some(ONE),
                None,
                Some(range(Some(ONE), None, Some("0"))),
            )),
        );
        let _ = run(&cap);
    }
}
