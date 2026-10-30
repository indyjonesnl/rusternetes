//! Unit tests moved from `src/validation/resourceclaim.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
    pub use rusternetes_common::resources::{
        AllocatedDeviceStatus, AllocationConfigSource, AllocationResult,
        DeviceAllocationConfiguration, DeviceAllocationMode, DeviceClaim, DeviceClaimConfiguration,
        DeviceRequestAllocationResult, DeviceSelector, DeviceTaintEffect, DeviceToleration,
        ExactDeviceRequest, NetworkDeviceData, OpaqueDeviceConfiguration, ResourceClaim,
        ResourceClaimSpec, ResourceClaimStatus, TolerationOperator,
    };
    pub use rusternetes_common::types::Condition;
    pub use rusternetes_common::validation::csinode::validate_csi_driver_name;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_conditions,
        validate_label_name,
    };
    pub use rusternetes_common::validation::objectmeta::validate_immutable_field;
    pub use rusternetes_common::validation::pod::validate_node_selector;
    pub use rusternetes_common::validation::resourceslice::{
        validate_device_binding_parameters, validate_device_name, validate_pool_name,
    };
    pub use std::collections::{HashMap, HashSet};
    pub use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::resourceclaim::*;

    fn claim(devices: serde_json::Value) -> ResourceClaim {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "claim-1"},
            "spec": {"devices": devices},
        }))
        .unwrap()
    }

    fn errs(devices: serde_json::Value) -> Vec<String> {
        validate_resource_claim(&claim(devices))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn selector_errs(expression: &str) -> Vec<String> {
        errs(serde_json::json!({
            "requests": [{
                "name": "gpu",
                "exactly": {
                    "deviceClassName": "gpu.example.com",
                    "allocationMode": "ExactCount",
                    "count": 1,
                    "selectors": [{"cel": {"expression": expression}}]
                }
            }]
        }))
    }

    /// Port of `CEL-compile-errors` (validation_resourceclaim_test.go:567): the
    /// expression parses but does not type-check against the DRA environment.
    #[test]
    fn selector_index_type_error_is_compilation_failed() {
        let e = selector_errs("device.attributes[true].someBoolean");
        assert!(
            e.iter().any(|m| m.contains(
                "compilation failed: ERROR: <input>:1:18: found no matching overload for '_[_]' applied to '(map(string, map(string, any)), bool)'\n | device.attributes[true].someBoolean\n | .................^"
            )),
            "{e:?}"
        );
    }

    /// Port of `CEL-cost` (validation_resourceclaim_test.go:610).
    #[test]
    fn selector_exceeding_cost_limit_is_forbidden() {
        let e = selector_errs(
            "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))",
        );
        assert!(
            e.iter().any(|m| m.contains("selectors[0].cel.expression")
                && m.contains("Forbidden")
                && m.contains("too complex, exceeds cost limit")),
            "{e:?}"
        );
    }

    /// `CompileCELExpression` (compile.go:166-170): the output must be bool or any.
    #[test]
    fn selector_must_evaluate_to_bool() {
        let e = selector_errs("device.driver");
        assert!(
            e.iter()
                .any(|m| m.contains("must evaluate to bool or the unknown type, not string")),
            "{e:?}"
        );
    }

    #[test]
    fn selector_typed_ok_expressions_pass() {
        for x in [
            "device.driver == 'dra.example.com'",
            "device.attributes['d'].model == 'a'",
            "device.attributes['d']['model'] == 'a'",
            "device.allowMultipleAllocations",
            "device.attributes['d'].x",
            "has(device.attributes['d'].x) && device.attributes['d'].x == 1",
            "device.capacity['d'].memory.compareTo(quantity('1Gi')) >= 0",
            "['a','b'].exists(x, x == device.driver)",
            "['a','b'][0] == 'a'",
        ] {
            let e = selector_errs(x);
            assert!(e.is_empty(), "{x}: {e:?}");
        }
    }

    #[test]
    fn valid_exact_request_passes() {
        assert!(errs(serde_json::json!({
            "requests": [{
                "name": "gpu",
                "exactly": {"deviceClassName": "gpu.example.com", "allocationMode": "ExactCount", "count": 2}
            }]
        }))
        .is_empty());
    }

    #[test]
    fn request_needs_exactly_one_of_exactly_or_first_available() {
        // neither
        assert!(errs(serde_json::json!({"requests": [{"name": "r"}]}))
            .iter()
            .any(|m| m.contains("exactly one of")));
        // both
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r",
                "exactly": {"deviceClassName": "c.example.com"},
                "firstAvailable": [{"name": "s", "deviceClassName": "c.example.com"}]}]
        }))
        .iter()
        .any(|m| m.contains("multiple fields are set")));
    }

    #[test]
    fn duplicate_request_names_rejected() {
        let e = errs(serde_json::json!({
            "requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}},
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}}
            ]
        }));
        assert!(
            e.iter().any(|m| m.to_lowercase().contains("duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn allocation_mode_count_coupling() {
        // ExactCount with count 0 -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}]
        }))
        .iter()
        .any(|m| m.contains("greater than zero")));
        // All with a count -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "All", "count": 3}}]
        }))
        .iter()
        .any(|m| m.contains("must not be specified when allocationMode is 'All'")));
    }

    #[test]
    fn bad_device_class_name_rejected() {
        let e = errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": ""}}]
        }));
        assert!(e.iter().any(|m| m.contains("deviceClassName")), "{e:?}");
    }

    #[test]
    fn spec_immutable_on_update() {
        let old = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1}}]
        }));
        let new = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 2}}]
        }));
        let e = validate_resource_claim_update(&new, &old);
        assert!(
            e.iter()
                .any(|x| x.field == "spec" && x.detail == "field is immutable"),
            "{e:?}"
        );
    }

    #[test]
    fn template_validates_embedded_spec() {
        use rusternetes_common::resources::ResourceClaimTemplate;
        let tmpl: ResourceClaimTemplate = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "t"},
            "spec": {"spec": {"devices": {"requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}
            ]}}}
        })).unwrap();
        let errs = validate_resource_claim_template(&tmpl);
        // count 0 with ExactCount -> error, attached under spec.spec.devices...
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("greater than zero")),
            "{errs:?}"
        );
        assert!(
            errs.iter()
                .any(|e| e.field.starts_with("spec.spec.devices")),
            "{errs:?}"
        );
    }

    // ---- status-update validators for the gated fields -------------------
    //
    // Ports of cases in `validation_resourceclaim_test.go`
    // (`TestValidateClaimStatusUpdate`, release-1.35).

    const GOOD_SHARE_ID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

    fn status_errs(status: serde_json::Value) -> Vec<String> {
        let spec = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1}}]
        }));
        let status: ResourceClaimStatus = serde_json::from_value(status).unwrap();
        validate_resource_claim_status_update(
            &status,
            &ResourceClaimStatus::default(),
            &spec.spec.devices,
            false,
            &Path::new("status"),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    fn result(extra: serde_json::Value) -> serde_json::Value {
        let mut r = serde_json::json!({
            "request": "r", "driver": "d.example.com", "pool": "p", "device": "x"
        });
        for (k, v) in extra.as_object().unwrap() {
            r[k] = v.clone();
        }
        r
    }

    fn allocated(results: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"devices": {"results": results}})
    }

    fn device(extra: serde_json::Value) -> serde_json::Value {
        let mut d = serde_json::json!({"driver": "d.example.com", "pool": "p", "device": "x"});
        for (k, v) in extra.as_object().unwrap() {
            d[k] = v.clone();
        }
        d
    }

    /// `invalid-add-allocated-status-mismatch-share-id` (:2185-2215): the
    /// identity includes the shareID and the bad value is the JSON-marshalled
    /// `SharedDeviceID` struct (errors.go:84-107 `default:` arm).
    #[test]
    fn device_status_share_id_must_match_allocated_share() {
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({"shareID": GOOD_SHARE_ID}))]),
            "devices": [device(serde_json::json!({"shareID": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}))],
        }));
        assert_eq!(
            e,
            vec![
                r#"status.devices[0]: Invalid value: {"Driver":"d.example.com","Pool":"p","Device":"x","ShareID":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}: must be an allocated device in the claim"#
            ]
        );
    }

    /// `invalid-device-status-duplicate-with-share-id` (:1427-1455).
    #[test]
    fn device_status_duplicate_includes_share_id() {
        let d = device(serde_json::json!({"shareID": GOOD_SHARE_ID}));
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({"shareID": GOOD_SHARE_ID}))]),
            "devices": [d.clone(), d],
        }));
        assert_eq!(
            e,
            vec![format!(
                r#"status.devices[1]: Duplicate value: {{"Driver":"d.example.com","Pool":"p","Device":"x","ShareID":"{GOOD_SHARE_ID}"}}"#
            )]
        );
    }

    /// `invalid-add-allocated-status-invalid-share-id` (:2221-2275).
    #[test]
    fn share_id_must_be_a_normalized_uuid() {
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![
                result(serde_json::json!({"shareID": "a"})),
                result(serde_json::json!({"shareID": "Aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})),
            ]),
            "devices": [
                device(serde_json::json!({"shareID": "a"})),
                device(serde_json::json!({"shareID": "Aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})),
            ],
        }));
        for want in [
            r#"status.devices[0].shareID: Invalid value: "a": error validating uid: invalid UUID length: 1"#,
            "status.devices[1].shareID: Invalid value: \"Aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\": uid must be in RFC 4122 normalized form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` with lowercase hexadecimal characters",
            r#"status.allocation.devices.results[0].shareID: Invalid value: "a": error validating uid: invalid UUID length: 1"#,
            "status.allocation.devices.results[1].shareID: Invalid value: \"Aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\": uid must be in RFC 4122 normalized form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` with lowercase hexadecimal characters",
        ] {
            assert!(e.iter().any(|m| m == want), "missing {want}\n in {e:#?}");
        }
    }

    /// `invalid-network-device-status` (:1456-1490).
    #[test]
    fn network_device_data_is_validated() {
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({}))]),
            "devices": [device(serde_json::json!({"networkData": {
                "interfaceName": "x".repeat(257),
                "hardwareAddress": "x".repeat(129),
                "ips": ["300.9.8.0/24", "010.009.008.000/24", "2001:0db8::1/64"],
            }}))],
        }));
        assert_eq!(
            e,
            vec![
                "status.devices[0].networkData.interfaceName: Too long: may not be more than 256 bytes".to_string(),
                "status.devices[0].networkData.hardwareAddress: Too long: may not be more than 128 bytes".to_string(),
                r#"status.devices[0].networkData.ips[0]: Invalid value: "300.9.8.0/24": must be a valid address in CIDR form, (e.g. 10.9.8.7/24 or 2001:db8::1/64)"#.to_string(),
                r#"status.devices[0].networkData.ips[1]: Invalid value: "010.009.008.000/24": must be in canonical form ("10.9.8.0/24")"#.to_string(),
                r#"status.devices[0].networkData.ips[2]: Invalid value: "2001:0db8::1/64": must be in canonical form ("2001:db8::1/64")"#.to_string(),
            ]
        );
    }

    /// `invalid-device-status-duplicate` (:1396-1425) and the ips cap from
    /// `invalid-data-device-status-limits` (:1510).
    #[test]
    fn network_device_data_ips_unique_and_capped() {
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({}))]),
            "devices": [device(serde_json::json!({"networkData": {
                "ips": ["2001:db8::1/64", "2001:db8::1/64"],
            }}))],
        }));
        assert_eq!(
            e,
            vec![r#"status.devices[0].networkData.ips[1]: Duplicate value: "2001:db8::1/64""#]
        );
        let ips: Vec<String> = (0..17).map(|i| format!("10.9.8.{i}/24")).collect();
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({}))]),
            "devices": [device(serde_json::json!({"networkData": {"ips": ips}}))],
        }));
        assert_eq!(
            e,
            vec!["status.devices[0].networkData.ips: Too many: 17: must have at most 16 items"]
        );
    }

    /// `invalid-data-device-status-limits` (:1514-1545): `data` over
    /// `AllocatedDeviceStatusDataMaxLength` (types.go:1779) is `TooLong`.
    #[test]
    fn device_status_data_is_bounded_and_must_be_an_object() {
        let big = serde_json::json!({"str": "x".repeat(10 * 1024)});
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({}))]),
            "devices": [device(serde_json::json!({"data": big}))],
        }));
        assert_eq!(
            e,
            vec!["status.devices[0].data: Too long: may not be more than 10240 bytes"]
        );
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({}))]),
            "devices": [device(serde_json::json!({"data": [1]}))],
        }));
        assert_eq!(
            e,
            vec![
                r#"status.devices[0].data: Invalid value: "<value omitted>": must be a valid JSON object"#
            ]
        );
    }

    /// `validateDeviceRequestAllocationResult` runs
    /// `validateDeviceBindingParameters` on the result path (:509-520).
    #[test]
    fn allocation_result_binding_parameters_are_validated() {
        let e = status_errs(serde_json::json!({
            "allocation": allocated(vec![result(serde_json::json!({
                "bindingConditions": ["a"],
            }))]),
        }));
        assert_eq!(
            e,
            vec![
                r#"status.allocation.devices.results[0].bindingFailureConditions: Invalid value: null: bindingFailureConditions are required to use bindingConditions"#
            ]
        );
    }

    // ---- TooMany origin (#3412) ------------------------------------------
    //
    // Upstream `validateSlice` / `validateSet`
    // (`pkg/apis/resource/validation/validation.go:1181`, `:1198`) tag every
    // size breach `.WithOrigin("maxItems")`; `validateMap` (`:1246`) and the
    // direct `field.TooMany` for device-status conditions (`:1286`) do not.

    fn too_many_origins(errs: ErrorList) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = errs
            .into_iter()
            .filter(|e| e.error_type == rusternetes_common::validation::field::ErrorType::TooMany)
            .map(|e| (e.field, e.origin))
            .collect();
        v.sort();
        v
    }

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("r{i}")).collect()
    }

    #[test]
    fn spec_too_many_carries_max_items_origin() {
        let exact = serde_json::json!({
            "deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1
        });
        let selectors: Vec<_> = (0..33)
            .map(|_| serde_json::json!({"cel": {"expression": "true"}}))
            .collect();
        let mut with_selectors = exact.clone();
        with_selectors["selectors"] = serde_json::json!(selectors);
        let first_available: Vec<_> = (0..9)
            .map(|i| {
                serde_json::json!({"name": format!("s{i}"), "deviceClassName": "c.example.com",
                    "allocationMode": "ExactCount", "count": 1})
            })
            .collect();
        let mut requests: Vec<serde_json::Value> = (0..33)
            .map(|i| serde_json::json!({"name": format!("q{i}"), "exactly": exact}))
            .collect();
        requests[0] = serde_json::json!({"name": "q0", "exactly": with_selectors});
        requests[1] = serde_json::json!({"name": "q1", "firstAvailable": first_available});
        let config: Vec<_> = (0..33)
            .map(|i| match i {
                0 => serde_json::json!({"requests": names(33)}),
                _ => serde_json::json!({}),
            })
            .collect();
        let got = too_many_origins(validate_resource_claim(&claim(serde_json::json!({
            "requests": requests, "config": config,
        }))));
        let want: Vec<(String, String)> = [
            "spec.devices.config",
            "spec.devices.config[0].requests",
            "spec.devices.requests",
            "spec.devices.requests[0].exactly.selectors",
            "spec.devices.requests[1].firstAvailable",
        ]
        .iter()
        .map(|f| (f.to_string(), "maxItems".to_string()))
        .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn status_too_many_carries_max_items_origin_except_conditions() {
        let consumers: Vec<_> = (0..257)
            .map(|i| serde_json::json!({"resource": "pods", "name": format!("p{i}"), "uid": format!("{i}")}))
            .collect();
        let results: Vec<_> = (0..33).map(|_| result(serde_json::json!({}))).collect();
        let configs: Vec<_> = (0..65)
            .map(|_| serde_json::json!({"source": "FromClaim", "opaque": {"driver": "d.example.com", "parameters": {}}}))
            .collect();
        let conditions: Vec<_> = (0..9)
            .map(|i| serde_json::json!({"type": format!("T{i}"), "status": "True", "reason": "R", "lastTransitionTime": "2024-01-01T00:00:00Z"}))
            .collect();
        let status: ResourceClaimStatus = serde_json::from_value(serde_json::json!({
            "reservedFor": consumers,
            "allocation": {"devices": {"results": results, "config": configs}},
            "devices": [device(serde_json::json!({"conditions": conditions}))],
        }))
        .unwrap();
        let spec = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1}}]
        }));
        let got = too_many_origins(validate_resource_claim_status_update(
            &status,
            &ResourceClaimStatus::default(),
            &spec.spec.devices,
            false,
            &Path::new("status"),
        ));
        let m = |f: &str, o: &str| (f.to_string(), o.to_string());
        assert_eq!(
            got,
            vec![
                m("status.allocation.devices.config", "maxItems"),
                m("status.allocation.devices.results", "maxItems"),
                m("status.devices[0].conditions", ""),
                m("status.reservedFor", "maxItems"),
            ]
        );
    }

    // ---- constraints caps (#3558) ----------------------------------------

    fn one_request() -> serde_json::Value {
        serde_json::json!({"name": "q0", "exactly": {
            "deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1
        }})
    }

    /// Port of `invalid-request-with-max-size-short-circuit`
    /// (validation_resourceclaim_test.go:299-340): `validateSlice` over
    /// `constraints` (`validation.go:151`) returns only the TooMany error, so
    /// the invalid entries are not validated.
    #[test]
    fn constraints_over_cap_short_circuit_with_max_items_origin() {
        let mut constraints =
            vec![serde_json::json!({"requests": ["q0", "bad name"], "matchAttribute": "foo/bar"})];
        for _ in 1..33 {
            constraints.push(serde_json::json!({"matchAttribute": "foo/bar"}));
        }
        let got = validate_resource_claim(&claim(serde_json::json!({
            "requests": [one_request()], "constraints": constraints,
        })));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].field, "spec.devices.constraints");
        assert_eq!(got[0].origin, "maxItems");
        assert!(
            got[0].to_string().contains("must have at most 32 items"),
            "{got:?}"
        );
    }

    #[test]
    fn constraints_at_cap_are_valid() {
        let constraints: Vec<_> = (0..32)
            .map(|_| serde_json::json!({"matchAttribute": "foo/bar"}))
            .collect();
        let got = validate_resource_claim(&claim(serde_json::json!({
            "requests": [one_request()], "constraints": constraints,
        })));
        assert!(got.is_empty(), "{got:?}");
    }

    /// `validateDeviceConstraint` runs `validateSet(constraint.Requests,
    /// DeviceRequestsMaxSize, ...)` (`validation.go:355`): capped, and
    /// duplicate names are rejected.
    #[test]
    fn constraint_requests_over_cap_carry_max_items_origin() {
        let got = validate_resource_claim(&claim(serde_json::json!({
            "requests": [one_request()],
            "constraints": [{"requests": names(33), "matchAttribute": "foo/bar"}],
        })));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].field, "spec.devices.constraints[0].requests");
        assert_eq!(got[0].origin, "maxItems");
    }

    #[test]
    fn constraint_requests_duplicate_is_rejected() {
        let got = validate_resource_claim(&claim(serde_json::json!({
            "requests": [one_request()],
            "constraints": [{"requests": ["q0", "q0"], "matchAttribute": "foo/bar"}],
        })));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].field, "spec.devices.constraints[0].requests[1]");
        assert!(got[0].to_string().contains("Duplicate"), "{got:?}");
    }

    // ---- tolerations (#3559) ---------------------------------------------

    /// Upstream has no list cap on `tolerations`: `validateDeviceRequest`
    /// loops them (`validation.go:262-264`) and the generated v1 validation
    /// only does `EachSliceVal` (zz_generated.validations.go:1209-1217).
    #[test]
    fn tolerations_have_no_list_cap() {
        let tolerations: Vec<_> = (0..17)
            .map(|i| serde_json::json!({"key": format!("k{i}"), "operator": "Exists"}))
            .collect();
        let mut exact = serde_json::json!({
            "deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1
        });
        exact["tolerations"] = serde_json::json!(tolerations);
        let got = validate_resource_claim(&claim(serde_json::json!({
            "requests": [{"name": "q0", "exactly": exact}],
        })));
        assert!(got.is_empty(), "{got:?}");
    }
}
