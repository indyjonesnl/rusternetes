//! #3399: the unknown fields of a custom resource's ROOT `metadata` are
//! reported by the decoder.
//!
//! Upstream `unstructuredSchemaCoercer.apply` calls
//! `schemaobjectmeta.GetObjectMetaWithOptions` on the root with
//! `ReturnUnknownFieldPaths` (customresource_handler.go:1416-1423) and
//! `schemaCoercingDecoder.Decode` appends each path to the
//! `StrictDecodingError` (:1338-1342).

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
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "properties": { "spec": { "type": "object",
                        "properties": { "a": { "type": "string" } } } }
                } }
            }]
        }
    })
}

fn widget() -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": "w1", "zzz": "x" },
        "spec": { "a": "b" }
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

#[tokio::test]
async fn warn_mode_reports_the_unknown_root_metadata_field() {
    let api = setup().await;
    let (status, headers, bytes, _) = api
        .send_full(
            "POST",
            PATH,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&widget()).unwrap()),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let joined: String = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        joined.contains(r#"unknown field \"metadata.zzz\""#),
        "{joined:?}"
    );
}

#[tokio::test]
async fn strict_mode_refuses_the_unknown_root_metadata_field() {
    let api = setup().await;
    let (status, out) = api
        .post(&format!("{PATH}?fieldValidation=Strict"), &widget())
        .await;
    assert!(status.is_client_error(), "{status} {out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .contains(r#"unknown field "metadata.zzz""#),
        "{out}"
    );
}
