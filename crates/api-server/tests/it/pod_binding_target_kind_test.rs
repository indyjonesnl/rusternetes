//! Pod /binding: target.kind, when set, must be "Node" (upstream
//! ValidatePodBinding). An unsupported kind is rejected; "Node" / empty work.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pods_uri() -> String {
    format!("/api/v1/namespaces/{NS}/pods")
}
fn binding_uri(pod: &str) -> String {
    format!("{}/{pod}/binding", pods_uri())
}

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

fn binding(pod: &str, kind: &str, node: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Binding",
        "metadata": {"name": pod, "namespace": NS},
        "target": {"kind": kind, "name": node}
    })
}

#[tokio::test]
async fn binding_rejects_non_node_target_kind() {
    let state = TestApiServer::new();
    let (code, _) = state.post(&pods_uri(), &pod("p-bad")).await;
    assert_eq!(code, StatusCode::CREATED, "pod create must succeed");

    // target.kind = "Pod" is unsupported.
    let (code, body) = state
        .post(&binding_uri("p-bad"), &binding("p-bad", "Pod", "node-a"))
        .await;
    // BindingREST.Create returns `errs.ToAggregate()` (storage.go:189), not an
    // APIStatus, so ErrorToAPIStatus's default arm (status.go:60-82) renders a
    // 500 with reason Unknown ("", omitted), the aggregate text as message and
    // no details.
    assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["status"], "Failure");
    assert_eq!(body["code"], 500);
    assert!(
        body.get("reason").is_none(),
        "reason Unknown is omitted: {body}"
    );
    assert!(body.get("details").is_none(), "no details: {body}");
    assert_eq!(
        body["message"],
        "target.kind: Unsupported value: \"Pod\": supported values: \"Node\", \"<empty>\""
    );
}

#[tokio::test]
async fn binding_multiple_errors_render_as_bracketed_aggregate() {
    let state = TestApiServer::new();
    let (code, _) = state.post(&pods_uri(), &pod("p-two")).await;
    assert_eq!(code, StatusCode::CREATED);
    // ToAggregate message for several errors is "[a, b]" (errors.go:70-96).
    let (code, body) = state
        .post(&binding_uri("p-two"), &binding("p-two", "Pod", ""))
        .await;
    assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(
        body["message"],
        "[target.kind: Unsupported value: \"Pod\": supported values: \"Node\", \"<empty>\", target.name: Required value]"
    );
}

#[tokio::test]
async fn binding_accepts_node_target_kind() {
    let state = TestApiServer::new();
    let (code, _) = state.post(&pods_uri(), &pod("p-ok")).await;
    assert_eq!(code, StatusCode::CREATED);

    let (code, body) = state
        .post(&binding_uri("p-ok"), &binding("p-ok", "Node", "node-a"))
        .await;
    assert!(
        code == StatusCode::CREATED || code == StatusCode::OK,
        "Node target.kind binding must succeed: {code} {body}"
    );
}
