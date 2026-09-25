//! A ResourceClaim's selectors, tolerations and config are validated.
//!
//! ResourceClaim half of the `dra.rs` #1939 slice (#2015 did the ResourceSlice
//! half). `validate_device_claim` validated request names, the
//! `exactly`/`firstAvailable` split and `deviceClassName`, and stopped there:
//! `selectors`, `tolerations` and `config` were stored exactly as posted, and
//! the fields inside them were required at decode time, so a body upstream
//! answers with a 422 and a field path got serde's 400 BadRequest.
//!
//! Upstream `validateDeviceClaim`
//! (`pkg/apis/resource/validation/validation.go:140-160`) also runs
//! `validateDeviceClaimConfiguration` (`:372`) over `config` — which
//! cross-checks every entry's `requests` against the names gathered from the
//! claim (`gatherRequestNames`, `:186`) — and `validateDeviceRequest` (`:212`)
//! runs `validateSelectorSlice` (`:298`) and `validateDeviceToleration`
//! (`:1411`) over each request.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// A claim whose single request is `request`.
fn claim_with_request(request: Value) -> Value {
    json!({ "devices": { "requests": [request] } })
}

/// A claim with one valid request plus the given `config`.
fn claim_with_config(config: Value) -> Value {
    json!({
        "devices": {
            "requests": [{ "name": "req", "exactly": { "deviceClassName": "class" } }],
            "config": [config]
        }
    })
}

/// `(label, claim spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a request with no name",
            claim_with_request(json!({ "exactly": { "deviceClassName": "class" } })),
            "spec.devices.requests[0].name: Required value",
        ),
        (
            "an exact request with no deviceClassName",
            claim_with_request(json!({ "name": "req", "exactly": {} })),
            "spec.devices.requests[0].exactly.deviceClassName: Required value",
        ),
        (
            "a selector with no cel",
            claim_with_request(json!({
                "name": "req",
                "exactly": { "deviceClassName": "class", "selectors": [{}] }
            })),
            "spec.devices.requests[0].exactly.selectors[0].cel: Required value",
        ),
        (
            "a cel selector with no expression",
            claim_with_request(json!({
                "name": "req",
                "exactly": { "deviceClassName": "class", "selectors": [{ "cel": {} }] }
            })),
            "spec.devices.requests[0].exactly.selectors[0].cel.expression: Required value",
        ),
        (
            "a toleration with no operator",
            claim_with_request(json!({
                "name": "req",
                "exactly": {
                    "deviceClassName": "class",
                    "tolerations": [{ "key": "example.com/taint" }]
                }
            })),
            "spec.devices.requests[0].exactly.tolerations[0].operator: Required value",
        ),
        (
            "a toleration with operator Exists and a value",
            claim_with_request(json!({
                "name": "req",
                "exactly": {
                    "deviceClassName": "class",
                    "tolerations": [{
                        "key": "example.com/taint", "operator": "Exists", "value": "v"
                    }]
                }
            })),
            "spec.devices.requests[0].exactly.tolerations[0].value: Invalid value: \"v\": \
             must be empty for operator `Exists`",
        ),
        (
            "a config entry with no opaque",
            claim_with_config(json!({ "requests": ["req"] })),
            "spec.devices.config[0].opaque: Required value",
        ),
        (
            "an opaque config with no driver",
            claim_with_config(json!({
                "requests": ["req"],
                "opaque": { "parameters": { "a": 1 } }
            })),
            "spec.devices.config[0].opaque.driver: Required value",
        ),
        (
            "an opaque config with no parameters",
            claim_with_config(json!({
                "requests": ["req"],
                "opaque": { "driver": "dra.example.com" }
            })),
            "spec.devices.config[0].opaque.parameters: Required value",
        ),
        (
            "an opaque config whose parameters are not an object",
            claim_with_config(json!({
                "requests": ["req"],
                "opaque": { "driver": "dra.example.com", "parameters": 7 }
            })),
            "spec.devices.config[0].opaque.parameters: Invalid value: \"<value omitted>\": \
             must be a valid JSON object",
        ),
        (
            "a config entry naming a request that does not exist",
            claim_with_config(json!({
                "requests": ["nope"],
                "opaque": { "driver": "dra.example.com", "parameters": { "a": 1 } }
            })),
            "spec.devices.config[0].requests[0]: Invalid value: \"nope\"",
        ),
    ]
}

#[tokio::test]
async fn every_bad_resource_claim_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "resource.k8s.io/v1",
                    "kind": "ResourceClaim",
                    "metadata": { "name": format!("claim-{i}"), "namespace": "default" },
                    "spec": spec,
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

/// The accept side: a claim exercising every construct this port touches must
/// still be written, including a sub-request reference of the form
/// `request/subrequest` that `requestNames.Has` accepts (`:163-185`).
#[tokio::test]
async fn a_well_formed_resource_claim_is_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "resource.k8s.io/v1",
                "kind": "ResourceClaim",
                "metadata": { "name": "claim-ok", "namespace": "default" },
                "spec": { "devices": {
                    "requests": [
                        {
                            "name": "req",
                            "exactly": {
                                "deviceClassName": "class",
                                "selectors": [{ "cel": { "expression": "device.driver == 'x'" } }],
                                "tolerations": [
                                    { "key": "example.com/taint", "operator": "Equal", "value": "v" },
                                    { "operator": "Exists" }
                                ]
                            }
                        },
                        {
                            "name": "alt",
                            "firstAvailable": [
                                { "name": "sub", "deviceClassName": "class" }
                            ]
                        }
                    ],
                    "config": [{
                        "requests": ["req", "alt/sub"],
                        "opaque": { "driver": "dra.example.com", "parameters": { "a": 1 } }
                    }]
                } },
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a well-formed ResourceClaim must be written: {status} {body}"
    );
}

/// The DRA list types carry plain slices upstream, so a body with no `items`
/// decodes to an empty list rather than failing.
#[tokio::test]
async fn a_dra_list_without_items_decodes() {
    use rusternetes_common::resources::dra::{
        DeviceClassList, ResourceClaimList, ResourceClaimTemplateList, ResourceSliceList,
    };

    let claims: ResourceClaimList =
        serde_json::from_value(json!({ "apiVersion": "resource.k8s.io/v1" })).expect("claims");
    assert!(claims.items.is_empty());
    let templates: ResourceClaimTemplateList =
        serde_json::from_value(json!({ "apiVersion": "resource.k8s.io/v1" })).expect("templates");
    assert!(templates.items.is_empty());
    let classes: DeviceClassList =
        serde_json::from_value(json!({ "apiVersion": "resource.k8s.io/v1" })).expect("classes");
    assert!(classes.items.is_empty());
    let slices: ResourceSliceList =
        serde_json::from_value(json!({ "apiVersion": "resource.k8s.io/v1" })).expect("slices");
    assert!(slices.items.is_empty());
}
