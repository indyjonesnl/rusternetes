//! A custom resource's top-level `spec: null` / `status: null` survive decode
//! (#3369). Upstream decodes a CR into an unstructured map, so an explicit
//! null reaches `PruneNonNullableNullsWithoutDefaults`
//! (schema/defaulting/prunenulls.go:39-66) and the defaulter
//! (`unstructuredDefaulter`, customresource_handler.go:1406-1470) intact: a
//! nullable null is kept, a non-nullable null without a default is removed,
//! and a non-nullable null with a default is replaced by the default.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
const WIDGETS: &str = "/apis/example.com/v1/namespaces/default/widgets";

fn widget_crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "widgets.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": { "plural": "widgets", "kind": "Widget" },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "spec": { "type": "object", "nullable": true,
                                  "properties": { "a": { "type": "integer" } } },
                        "status": { "type": "object", "nullable": true,
                                    "properties": { "b": { "type": "integer" } } },
                        "plain": { "type": "object",
                                   "properties": { "c": { "type": "integer" } } },
                        "defaulted": { "type": "object", "default": { "c": 1 },
                                       "properties": { "c": { "type": "integer" } } }
                    }
                }}
            }]
        }
    })
}

async fn setup() -> TestApiServer {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established("widgets.example.com").await;
    api
}

fn widget(name: &str) -> Value {
    json!({
        "apiVersion": "example.com/v1", "kind": "Widget",
        "metadata": { "name": name, "namespace": "default" }
    })
}

#[tokio::test]
async fn nullable_null_spec_and_status_are_kept() {
    let api = setup().await;
    let mut w = widget("n");
    w["spec"] = Value::Null;
    w["status"] = Value::Null;
    let (status, out) = api.post(WIDGETS, &w).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        out.as_object()
            .unwrap()
            .get("spec")
            .is_some_and(Value::is_null),
        "{out}"
    );
    assert!(
        out.as_object()
            .unwrap()
            .get("status")
            .is_some_and(Value::is_null),
        "{out}"
    );
    let (status, got) = api.get(&format!("{WIDGETS}/n")).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert!(
        got.as_object()
            .unwrap()
            .get("spec")
            .is_some_and(Value::is_null),
        "{got}"
    );
}

#[tokio::test]
async fn non_nullable_null_is_pruned_or_defaulted() {
    let api = setup().await;
    let mut w = widget("p");
    w["plain"] = Value::Null;
    w["defaulted"] = Value::Null;
    let (status, out) = api.post(WIDGETS, &w).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(out.as_object().unwrap().get("plain").is_none(), "{out}");
    assert_eq!(out["defaulted"], json!({"c": 1}), "{out}");
}

/// A non-nullable top-level `spec: null` / `status: null` is removed by
/// `PruneNonNullableNullsWithoutDefaults` (prunenulls.go:39-66), as upstream
/// sees them as ordinary map entries.
#[tokio::test]
async fn non_nullable_null_spec_and_status_are_pruned() {
    let api = TestApiServer::new();
    let mut crd = widget_crd();
    crd["metadata"]["name"] = json!("widgets.example.com");
    let props = &mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"];
    props["spec"]["nullable"] = json!(false);
    props["status"]["nullable"] = json!(false);
    let (status, out) = api.post(CRDS, &crd).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established("widgets.example.com").await;
    let mut w = widget("q");
    w["spec"] = Value::Null;
    w["status"] = Value::Null;
    let (status, out) = api.post(WIDGETS, &w).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    // `apply_schema_defaults` still materialises an absent `spec` as `{}`
    // (pre-existing, tracked separately); what matters is the null is gone.
    assert!(!out["spec"].is_null(), "{out}");
    assert!(out.as_object().unwrap().get("status").is_none(), "{out}");
}
