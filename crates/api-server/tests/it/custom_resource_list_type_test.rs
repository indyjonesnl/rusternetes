//! #2135: `x-kubernetes-list-type` map/set uniqueness on custom resources.
//!
//! Upstream: `customResourceStrategy.Validate` appends
//! `structurallisttype.ValidateListSetsAndMaps` (strategy.go:214); an update
//! ratchets it — checked only when the old object was clean (strategy.go:300-302,
//! status_strategy.go:117-122).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";
const PATH: &str = "/apis/stable.example.com/v1/namespaces/default/widgets";

fn crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("widgets.{GROUP}") },
        "spec": {
            "group": GROUP, "scope": "Namespaced",
            "names": { "plural": "widgets", "kind": "Widget", "listKind": "WidgetList" },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "subresources": { "status": {} },
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "spec": { "type": "object", "properties": {
                            "tags": { "type": "array", "x-kubernetes-list-type": "set",
                                      "items": { "type": "string" } },
                            "ports": { "type": "array", "x-kubernetes-list-type": "map",
                                       "x-kubernetes-list-map-keys": ["port"],
                                       "items": { "type": "object", "properties": {
                                           "port": { "type": "integer" } },
                                           "required": ["port"] } }
                        } },
                        "status": { "type": "object", "properties": {
                            "tags": { "type": "array", "x-kubernetes-list-type": "set",
                                      "items": { "type": "string" } } } }
                    }
                } }
            }]
        }
    })
}

fn widget(spec: Value) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": "w1" },
        "spec": spec
    })
}

async fn setup() -> TestApiServer {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &crd(),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("widgets.{GROUP}")).await;
    api
}

fn message(out: &Value) -> &str {
    out["message"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn create_rejects_a_duplicate_set_element() {
    let api = setup().await;
    let (status, out) = api
        .post(PATH, &widget(json!({"tags": ["a", "b", "a"]})))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains(r#"spec.tags[2]: Duplicate value: "a""#),
        "{out}"
    );
}

#[tokio::test]
async fn create_rejects_a_duplicate_map_key() {
    let api = setup().await;
    let (status, out) = api
        .post(
            PATH,
            &widget(json!({"ports": [{"port": 80}, {"port": 81}, {"port": 80}]})),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.ports[2]: Duplicate value"),
        "{out}"
    );
}

#[tokio::test]
async fn create_accepts_unique_lists() {
    let api = setup().await;
    let (status, out) = api
        .post(
            PATH,
            &widget(json!({"tags": ["a", "b"], "ports": [{"port": 80}, {"port": 81}]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
}

#[tokio::test]
async fn update_rejects_a_new_duplicate() {
    let api = setup().await;
    let (status, out) = api.post(PATH, &widget(json!({"tags": ["a"]}))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let mut obj = out;
    obj["spec"]["tags"] = json!(["a", "a"]);
    let (status, out) = api.put(&format!("{PATH}/w1"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.tags[1]: Duplicate value"),
        "{out}"
    );
}

#[tokio::test]
async fn status_update_rejects_a_duplicate() {
    let api = setup().await;
    let (status, out) = api.post(PATH, &widget(json!({"tags": ["a"]}))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let mut obj = out;
    obj["status"] = json!({"tags": ["x", "x"]});
    let (status, out) = api.put(&format!("{PATH}/w1/status"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("status.tags[1]: Duplicate value"),
        "{out}"
    );
}
