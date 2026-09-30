//! ResourceQuota /status subresource runs validate_resource_quota_status_update
//! (#1484): the status `hard`/`used` maps must carry valid resource names and
//! parseable quantities, mirroring upstream ValidateResourceQuotaStatusUpdate.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn create_quota(api: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": name, "namespace": "default" },
        "spec": { "hard": { "pods": "10" } }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::POST.as_str(),
            "/api/v1/namespaces/default/resourcequotas",
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "create quota: {b}");
}

async fn put_status(api: &TestApiServer, name: &str, used: &str) -> (StatusCode, Value) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": name, "namespace": "default" },
        "status": { "hard": { "pods": "10" }, "used": { "pods": used } }
    });
    api.send(
        Method::PUT.as_str(),
        &format!("/api/v1/namespaces/default/resourcequotas/{name}/status"),
        Some("application/json"),
        Some(&body),
    )
    .await
}

/// `ValidateResourceQuotaStatusUpdate` (validation.go:8158-8163) runs
/// `ValidateResourceQuantityValue` over `status.used`: a negative quantity is
/// Invalid.
#[tokio::test]
async fn status_update_rejects_a_negative_used_quantity() {
    let api = TestApiServer::new();
    create_quota(&api, "rq-neg").await;
    let (status, b) = put_status(&api, "rq-neg", "-1").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{status}: {b}");
    assert!(
        b["message"]
            .as_str()
            .is_some_and(|m| m.contains("status.used[pods]")),
        "{b}"
    );
}

/// A quantity that does not parse fails decoding, before any strategy runs
/// (`resource.Quantity.UnmarshalJSON`): a 400 BadRequest, and nothing is
/// written.
#[tokio::test]
async fn status_update_rejects_an_unparseable_used_quantity() {
    let api = TestApiServer::new();
    create_quota(&api, "rq-bad").await;
    let (status, b) = put_status(&api, "rq-bad", "notaquantity").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{status}: {b}");
    let (_, got) = api
        .get("/api/v1/namespaces/default/resourcequotas/rq-bad")
        .await;
    assert!(got["status"].get("used").is_none(), "{got}");
}

#[tokio::test]
async fn status_update_accepts_valid_used_quantity() {
    let api = TestApiServer::new();
    create_quota(&api, "rq-ok").await;

    let good = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "rq-ok", "namespace": "default" },
        "status": { "hard": { "pods": "10" }, "used": { "pods": "3" } }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::PUT.as_str(),
            "/api/v1/namespaces/default/resourcequotas/rq-ok/status",
            Some("application/json"),
            Some(&good),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "valid status update must succeed: {b}"
    );
    assert_eq!(b["status"]["used"]["pods"], json!("3"), "{b}");
}
