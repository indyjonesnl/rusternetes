//! Update immutability for RuntimeClass (handler) and ServiceCIDR (spec.cidrs),
//! ported from upstream ValidateRuntimeClassUpdate / ValidateServiceCIDRUpdate.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---- RuntimeClass: handler is immutable ----------------------------------

fn rc_uri() -> &'static str {
    "/apis/node.k8s.io/v1/runtimeclasses"
}
fn rc(name: &str, handler: &str) -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": name}, "handler": handler
    })
}

#[tokio::test]
async fn runtimeclass_handler_immutable() {
    let state = TestApiServer::new();
    let name = "myrc";
    let (code, _) = state.post(rc_uri(), &rc(name, "runc")).await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", rc_uri());

    let (code, _) = state.put(&item, &rc(name, "gvisor")).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "handler change must be rejected"
    );

    let (code, _) = state.put(&item, &rc(name, "runc")).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "unchanged handler update must succeed"
    );
}

// ---- ServiceCIDR: spec.cidrs immutable (dual-stack expansion allowed) -----

fn sc_uri() -> &'static str {
    "/apis/networking.k8s.io/v1/servicecidrs"
}
fn sc(name: &str, cidrs: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
        "metadata": {"name": name}, "spec": {"cidrs": cidrs}
    })
}

#[tokio::test]
async fn servicecidr_cidrs_immutable() {
    let state = TestApiServer::new();
    let name = "mycidr";
    let (code, _) = state
        .post(sc_uri(), &sc(name, json!(["10.0.0.0/24"])))
        .await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", sc_uri());

    // Changing the existing CIDR is rejected.
    let (code, _) = state.put(&item, &sc(name, json!(["10.1.0.0/24"]))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "changing cidr must be rejected"
    );

    // Appending a second (dual-stack) CIDR is allowed.
    let (code, _) = state
        .put(&item, &sc(name, json!(["10.0.0.0/24", "2001:db8::/64"])))
        .await;
    assert_eq!(
        code,
        StatusCode::OK,
        "single->dual-stack expansion must be allowed"
    );
}

/// Create writes no status: upstream's registry strategy drops whatever the
/// client sent (`pkg/registry/networking/servicecidr/strategy.go:67-71`), and
/// the `Ready` condition belongs to the servicecidrs controller
/// (`pkg/controller/servicecidrs/servicecidrs_controller.go:341-346`) — which
/// is also the only component that can flip it to `Terminating`. Seeding it
/// here would make a range that is going away look permanently Ready (#1747).
#[tokio::test]
async fn servicecidr_create_writes_no_status() {
    let state = TestApiServer::new();
    let name = "nostatus";
    let (code, body) = state
        .post(sc_uri(), &sc(name, json!(["10.2.0.0/24"])))
        .await;
    assert_eq!(code, StatusCode::CREATED);
    assert!(
        body.get("status").is_none_or(Value::is_null),
        "create must not seed a status, got {:?}",
        body.get("status")
    );

    let (code, body) = state.get(&format!("{}/{name}", sc_uri())).await;
    assert_eq!(code, StatusCode::OK);
    assert!(
        body.get("status").is_none_or(Value::is_null),
        "the stored object must have no status either, got {:?}",
        body.get("status")
    );
}

/// A client-supplied status is dropped, not persisted.
#[tokio::test]
async fn servicecidr_create_drops_client_supplied_status() {
    let state = TestApiServer::new();
    let name = "clientstatus";
    let mut payload = sc(name, json!(["10.3.0.0/24"]));
    payload["status"] = json!({
        "conditions": [{
            "type": "Ready", "status": "True",
            "reason": "", "message": "i said so"
        }]
    });
    let (code, body) = state.post(sc_uri(), &payload).await;
    assert_eq!(code, StatusCode::CREATED);
    assert!(
        body.get("status").is_none_or(Value::is_null),
        "client-supplied status must be cleared, got {:?}",
        body.get("status")
    );
}
