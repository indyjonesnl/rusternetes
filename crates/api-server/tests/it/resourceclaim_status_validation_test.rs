//! A ResourceClaim's status update is validated.
//!
//! `update_resourceclaim_status` did `existing.status = claim.status;` and
//! wrote it — no validation at all. Upstream runs
//! `validateResourceClaimStatusUpdate`
//! (`pkg/apis/resource/validation/validation.go:434-466`) on every status
//! write, so a reservation with no consumer identity, an allocation naming a
//! request the claim does not declare, a device status for a device that was
//! never allocated, and a rewrite of an existing allocation are all rejected.
//!
//! This is also the second half of the `dra.rs` #1939 slice: the status-side
//! fields stay required at decode time until a validator stands behind them,
//! because defaulting them with nothing to check would convert serde's 400 into
//! a silent accept.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

/// Create a claim with one `req` request and one `alt/sub` sub-request.
async fn create_claim(api: &TestApiServer, name: &str) {
    let (status, body) = api
        .send(
            "POST",
            CLAIMS,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "resource.k8s.io/v1",
                "kind": "ResourceClaim",
                "metadata": { "name": name, "namespace": "default" },
                "spec": { "devices": { "requests": [
                    { "name": "req", "exactly": { "deviceClassName": "class" } },
                    { "name": "alt", "firstAvailable": [
                        { "name": "sub", "deviceClassName": "class" }
                    ] }
                ] } },
            })),
        )
        .await;
    assert!(status.is_success(), "claim {name} must be created: {body}");
}

/// A valid allocation of `req`.
fn allocation() -> Value {
    json!({
        "devices": { "results": [{
            "request": "req", "driver": "dra.example.com",
            "pool": "pool-1", "device": "gpu-0"
        }] }
    })
}

fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a reservedFor entry with no resource, name or uid",
            json!({ "allocation": allocation(), "reservedFor": [{}] }),
            "status.reservedFor[0].resource: Required value",
        ),
        (
            "a reservation with no allocation",
            json!({ "reservedFor": [
                { "resource": "pods", "name": "p", "uid": "uid-1" }
            ] }),
            "status.reservedFor: Forbidden: may not be specified when `allocated` is not set",
        ),
        (
            "two reservations for the same consumer",
            json!({ "allocation": allocation(), "reservedFor": [
                { "resource": "pods", "name": "p", "uid": "uid-1" },
                { "resource": "pods", "name": "q", "uid": "uid-1" }
            ] }),
            "status.reservedFor[1]: Duplicate value",
        ),
        (
            "an allocation naming a request that does not exist",
            json!({ "allocation": { "devices": { "results": [{
                "request": "nope", "driver": "dra.example.com",
                "pool": "pool-1", "device": "gpu-0"
            }] } } }),
            "status.allocation.devices.results[0].request: Invalid value: \"nope\"",
        ),
        (
            "an allocation result with no driver",
            json!({ "allocation": { "devices": { "results": [{
                "request": "req", "pool": "pool-1", "device": "gpu-0"
            }] } } }),
            "status.allocation.devices.results[0].driver: Required value",
        ),
        (
            "an allocation result with no pool",
            json!({ "allocation": { "devices": { "results": [{
                "request": "req", "driver": "dra.example.com", "device": "gpu-0"
            }] } } }),
            "status.allocation.devices.results[0].pool: Required value",
        ),
        (
            "an allocation result with no device",
            json!({ "allocation": { "devices": { "results": [{
                "request": "req", "driver": "dra.example.com", "pool": "pool-1"
            }] } } }),
            // `validate_device_name` answers an empty name with `Required`
            // rather than upstream's invalid-label message — a deliberate,
            // documented divergence that predates this slice
            // (`validation/resourceslice.rs`).
            "status.allocation.devices.results[0].device: Required value",
        ),
        (
            "an allocation config with no source",
            json!({ "allocation": { "devices": {
                "results": [{
                    "request": "req", "driver": "dra.example.com",
                    "pool": "pool-1", "device": "gpu-0"
                }],
                "config": [{
                    "requests": ["req"],
                    "opaque": { "driver": "dra.example.com", "parameters": { "a": 1 } }
                }]
            } } }),
            "status.allocation.devices.config[0].source: Required value",
        ),
        (
            "an allocation config with no opaque",
            json!({ "allocation": { "devices": {
                "results": [{
                    "request": "req", "driver": "dra.example.com",
                    "pool": "pool-1", "device": "gpu-0"
                }],
                "config": [{ "source": "FromClaim", "requests": ["req"] }]
            } } }),
            "status.allocation.devices.config[0].opaque: Required value",
        ),
        (
            "a device status for a device that was never allocated",
            json!({
                "allocation": allocation(),
                "devices": [{
                    "driver": "dra.example.com", "pool": "pool-1", "device": "gpu-9"
                }]
            }),
            "must be an allocated device in the claim",
        ),
    ]
}

#[tokio::test]
async fn every_bad_status_update_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, status_body, expected)) in cases().into_iter().enumerate() {
        let name = format!("claim-{i}");
        create_claim(&api, &name).await;

        let (status, body) = api
            .send(
                "PUT",
                &format!("{CLAIMS}/{name}/status"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "resource.k8s.io/v1",
                    "kind": "ResourceClaim",
                    "metadata": { "name": name, "namespace": "default" },
                    "status": status_body,
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

/// `validateResourceClaimStatusUpdate` (`:459-463`): an allocation that is
/// already set may not be rewritten, because the rules for a *new* result are
/// tighter than the ones its stored form was admitted under.
#[tokio::test]
async fn a_populated_allocation_may_not_be_rewritten() {
    let api = TestApiServer::new();
    create_claim(&api, "claim-immutable").await;

    let put = |status_body: Value| {
        let api = api.clone();
        async move {
            api.send(
                "PUT",
                &format!("{CLAIMS}/claim-immutable/status"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "resource.k8s.io/v1",
                    "kind": "ResourceClaim",
                    "metadata": { "name": "claim-immutable", "namespace": "default" },
                    "status": status_body,
                })),
            )
            .await
        }
    };

    let (status, body) = put(json!({ "allocation": allocation() })).await;
    assert!(
        status.is_success(),
        "first allocation must be written: {body}"
    );

    let (status, body) = put(json!({ "allocation": { "devices": { "results": [{
        "request": "alt/sub", "driver": "dra.example.com",
        "pool": "pool-2", "device": "gpu-1"
    }] } } }))
    .await;
    assert_eq!(status.as_u16(), 422, "a rewrite must be Invalid: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("status.allocation: Invalid value"),
        "must report the immutable allocation, got: {body}"
    );
}

/// The accept side: a claim reserved for a consumer, with a device status for
/// an allocated device and a sub-request allocation, must be written.
#[tokio::test]
async fn a_well_formed_status_is_written() {
    let api = TestApiServer::new();
    create_claim(&api, "claim-ok").await;

    let (status, body) = api
        .send(
            "PUT",
            &format!("{CLAIMS}/claim-ok/status"),
            Some("application/json"),
            Some(&json!({
                "apiVersion": "resource.k8s.io/v1",
                "kind": "ResourceClaim",
                "metadata": { "name": "claim-ok", "namespace": "default" },
                "status": {
                    "allocation": { "devices": {
                        "results": [
                            { "request": "req", "driver": "dra.example.com",
                              "pool": "pool-1", "device": "gpu-0" },
                            { "request": "alt/sub", "driver": "dra.example.com",
                              "pool": "pool-1", "device": "gpu-1" }
                        ],
                        "config": [{
                            "source": "FromClaim",
                            "requests": ["req"],
                            "opaque": { "driver": "dra.example.com", "parameters": { "a": 1 } }
                        }]
                    } },
                    "reservedFor": [
                        { "resource": "pods", "name": "consumer", "uid": "uid-1" }
                    ],
                    "devices": [{
                        "driver": "dra.example.com", "pool": "pool-1", "device": "gpu-0",
                        "conditions": [{
                            "type": "Ready", "status": "True",
                            "lastTransitionTime": "2026-01-01T00:00:00Z",
                            "reason": "Healthy"
                        }]
                    }]
                },
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a well-formed status must be written: {status} {body}"
    );
}
