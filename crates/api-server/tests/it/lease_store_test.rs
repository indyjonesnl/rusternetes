//! Leases on the generic Store (#1990 step 6): upstream's
//! `pkg/registry/coordination/lease/strategy.go` runs inside `Store.Create`
//! and `Store.Update`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const LEASES: &str = "/apis/coordination.k8s.io/v1/namespaces/default/leases";

fn lease(name: &str) -> Value {
    json!({
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"holderIdentity": "a", "leaseDurationSeconds": 15}
    })
}

/// `PrepareForCreate` (strategy.go:47-55): CoordinatedLeaderElection is off
/// in 1.35, so `strategy` and `preferredHolder` are dropped.
#[tokio::test]
async fn a_create_drops_the_gated_fields() {
    let api = TestApiServer::new();
    let mut body = lease("l");
    body["spec"]["strategy"] = json!("OldestEmulationVersion");
    body["spec"]["preferredHolder"] = json!("b");
    let (s, created) = api.post(LEASES, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert!(created["spec"].get("strategy").is_none(), "{created}");
    assert!(
        created["spec"].get("preferredHolder").is_none(),
        "{created}"
    );
}

/// `AllowUnconditionalUpdate` is false: a PUT to an existing Lease without
/// a resourceVersion is invalid (registry/generic/registry/store.go:727-733),
/// so two electors cannot both overwrite the holder blind.
#[tokio::test]
async fn a_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(LEASES, &lease("l")).await;
    assert_eq!(s, StatusCode::CREATED);
    let mut takeover = lease("l");
    takeover["spec"]["holderIdentity"] = json!("b");
    let (s, body) = api.put(&format!("{LEASES}/l"), &takeover).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (_, stored) = api.get(&format!("{LEASES}/l")).await;
    assert_eq!(stored["spec"]["holderIdentity"], "a", "{stored}");
}

/// A PUT with a stale resourceVersion loses to the one that got there
/// first: the elector's optimistic lock.
#[tokio::test]
async fn a_put_with_a_stale_resource_version_is_a_conflict() {
    let api = TestApiServer::new();
    let (s, created) = api.post(LEASES, &lease("l")).await;
    assert_eq!(s, StatusCode::CREATED);
    let rv = created["metadata"]["resourceVersion"].clone();

    let mut first = lease("l");
    first["metadata"]["resourceVersion"] = rv.clone();
    first["spec"]["holderIdentity"] = json!("b");
    let (s, body) = api.put(&format!("{LEASES}/l"), &first).await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let mut second = lease("l");
    second["metadata"]["resourceVersion"] = rv;
    second["spec"]["holderIdentity"] = json!("c");
    let (s, body) = api.put(&format!("{LEASES}/l"), &second).await;
    assert_eq!(s, StatusCode::CONFLICT, "{body}");
}

/// `ValidateLeaseUpdate` re-runs `ValidateLeaseSpec`, on PATCH too.
#[tokio::test]
async fn a_patch_to_a_zero_duration_is_invalid() {
    let api = TestApiServer::new();
    let (s, _) = api.post(LEASES, &lease("l")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(
            &format!("{LEASES}/l"),
            &json!({"spec": {"leaseDurationSeconds": 0}}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}
