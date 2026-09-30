//! RuntimeClasses on the generic Store (#1990 step 6): upstream's
//! `pkg/registry/node/runtimeclass/strategy.go` runs inside `Store.Create`
//! and `Store.Update`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const RCS: &str = "/apis/node.k8s.io/v1/runtimeclasses";

fn rc(name: &str, handler: &str) -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": name}, "handler": handler
    })
}

/// `AllowCreateOnUpdate` is true (strategy.go): a PUT to a missing
/// RuntimeClass creates it.
#[tokio::test]
async fn a_put_creates_a_missing_runtime_class() {
    let api = TestApiServer::new();
    let (s, body) = api
        .put(&format!("{RCS}/gvisor"), &rc("gvisor", "runsc"))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, stored) = api.get(&format!("{RCS}/gvisor")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(stored["handler"], "runsc");
}

/// `AllowUnconditionalUpdate` is false (strategy.go): a PUT to an existing
/// RuntimeClass without a resourceVersion is invalid
/// (registry/generic/registry/store.go:727-733).
#[tokio::test]
async fn a_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(RCS, &rc("a", "runc")).await;
    assert_eq!(s, StatusCode::CREATED);
    let mut update = rc("a", "runc");
    update["metadata"]["labels"] = json!({"x": "y"});
    let (s, body) = api.put(&format!("{RCS}/a"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["details"]["causes"][0]["field"], "metadata.resourceVersion",
        "{body}"
    );
}

/// `ValidateUpdate` (strategy.go): `handler` is immutable, on PATCH too.
#[tokio::test]
async fn a_patch_cannot_change_the_handler() {
    let api = TestApiServer::new();
    let (s, _) = api.post(RCS, &rc("a", "runc")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(&format!("{RCS}/a"), &json!({"handler": "crun"}))
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (_, stored) = api.get(&format!("{RCS}/a")).await;
    assert_eq!(stored["handler"], "runc");
}
