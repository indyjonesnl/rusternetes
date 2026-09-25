//! A ResourceSlice's devices, taints and counters are validated.
//!
//! `dra.rs` slice of #1939. `validate_resource_slice_spec` validated the
//! driver, the pool and the node-selection rule, and then checked only that
//! each device's `name` was non-empty and unique. Everything hanging off a
//! device was unvalidated: `sharedCounters`, `consumesCounters` and `taints`
//! were written as-is, and a device name that is not a DNS-1123 label was
//! accepted.
//!
//! Upstream `validateResourceSliceSpec`
//! (`pkg/apis/resource/validation/validation.go:649-770`) runs
//! `validateCounterSet` (`:771`) over `sharedCounters` and `validateDevice`
//! (`:801`) over each device, which in turn runs `validateDeviceName`
//! (`= ValidateDNS1123Label`, `:73`), `validateDeviceTaint` (`:1391`) and
//! `validateDeviceCounterConsumption` (`:871`).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// A ResourceSlice spec with the always-required scalars filled in; `extra` is
/// merged over the top.
fn spec(extra: Value) -> Value {
    let mut s = json!({
        "driver": "dra.example.com",
        "pool": { "name": "pool-1", "generation": 1, "resourceSliceCount": 1 },
        "allNodes": true,
        "devices": [{ "name": "gpu-0" }]
    });
    if let (Some(obj), Some(src)) = (s.as_object_mut(), extra.as_object()) {
        for (k, v) in src {
            obj.insert(k.clone(), v.clone());
        }
    }
    s
}

/// A spec whose single device is `device`.
fn with_device(device: Value) -> Value {
    spec(json!({ "devices": [device] }))
}

/// `(label, spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a pool with no name",
            spec(json!({ "pool": { "generation": 1, "resourceSliceCount": 1 } })),
            "spec.pool.name: Required value",
        ),
        (
            "a pool with no resourceSliceCount",
            spec(json!({ "pool": { "name": "pool-1", "generation": 1 } })),
            "spec.pool.resourceSliceCount: Invalid value: 0: must be greater than zero",
        ),
        (
            "a spec with no driver",
            spec(json!({ "driver": "" })),
            "spec.driver: Required value",
        ),
        (
            "a device with no name",
            with_device(json!({})),
            "spec.devices[0].name: Required value",
        ),
        (
            "a device name that is not a DNS-1123 label",
            with_device(json!({ "name": "GPU_0" })),
            "spec.devices[0].name: Invalid value: \"GPU_0\"",
        ),
        (
            "a taint with no key",
            with_device(json!({ "name": "gpu-0", "taints": [{ "effect": "NoSchedule" }] })),
            // `ValidateLabelName` is what upstream uses, and its comment says
            // it "Includes checking for non-empty" — as an Invalid, not a
            // Required (`pkg/apis/resource/validation/validation.go:1394`).
            "spec.devices[0].taints[0].key: Invalid value: \"\": name part must be non-empty",
        ),
        (
            "a taint with no effect",
            with_device(json!({ "name": "gpu-0", "taints": [{ "key": "example.com/taint" }] })),
            "spec.devices[0].taints[0].effect: Required value",
        ),
        (
            "a counter consumption with no counterSet",
            with_device(json!({
                "name": "gpu-0",
                "consumesCounters": [{ "counters": { "mem": { "value": "1Gi" } } }]
            })),
            "spec.devices[0].consumesCounters[0].counterSet: Required value",
        ),
        (
            "a counter consumption with no counters",
            with_device(json!({
                "name": "gpu-0",
                "consumesCounters": [{ "counterSet": "set-1" }]
            })),
            "spec.devices[0].consumesCounters[0].counters: Required value",
        ),
        (
            "a shared counter set with no name",
            spec(json!({ "sharedCounters": [{ "counters": { "mem": { "value": "1Gi" } } }] })),
            "spec.sharedCounters[0].name: Required value",
        ),
        (
            "a shared counter set with no counters",
            spec(json!({ "sharedCounters": [{ "name": "set-1" }] })),
            "spec.sharedCounters[0].counters: Required value",
        ),
    ]
}

#[tokio::test]
async fn every_bad_resource_slice_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, slice_spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/apis/resource.k8s.io/v1/resourceslices",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "resource.k8s.io/v1",
                    "kind": "ResourceSlice",
                    "metadata": { "name": format!("slice-{i}") },
                    "spec": slice_spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side: a slice using every construct this port touches must still
/// be written, so a validator that rejects everything cannot pass the table.
#[tokio::test]
async fn a_well_formed_resource_slice_is_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/apis/resource.k8s.io/v1/resourceslices",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "resource.k8s.io/v1",
                "kind": "ResourceSlice",
                "metadata": { "name": "slice-ok" },
                "spec": spec(json!({
                    "sharedCounters": [
                        { "name": "set-1", "counters": { "mem": { "value": "1Gi" } } }
                    ],
                    "devices": [{
                        "name": "gpu-0",
                        "taints": [
                            { "key": "example.com/taint", "value": "v", "effect": "NoSchedule" }
                        ],
                        "consumesCounters": [
                            { "counterSet": "set-1", "counters": { "mem": { "value": "1Gi" } } }
                        ]
                    }]
                })),
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a well-formed ResourceSlice must be written: {status} {body}"
    );
}
