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

/// #3469: a PATCH is decoded again after it is applied, by the same
/// `schemaCoercingDecoder`, so its unknown ROOT metadata fields are reported
/// too (patch.go:331-364 `applyPatch`'s `DecodeInto` with a strict decoder;
/// customresource_handler.go:1416-1423 + :1338-1342).
#[tokio::test]
async fn patch_strict_refuses_the_unknown_root_metadata_field() {
    let api = setup().await;
    let mut clean = widget();
    clean["metadata"] = json!({ "name": "w1" });
    let (status, out) = api.post(PATH, &clean).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, out) = api
        .patch(
            &format!("{PATH}/w1?fieldValidation=Strict"),
            &json!({ "metadata": { "zzz": "x" } }),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(r#"strict decoding error: unknown field "metadata.zzz""#),
        "{out}"
    );
}

#[tokio::test]
async fn patch_warn_reports_the_unknown_root_metadata_field() {
    let api = setup().await;
    let mut clean = widget();
    clean["metadata"] = json!({ "name": "w1" });
    let (status, out) = api.post(PATH, &clean).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, headers, bytes, _) = api
        .send_full(
            "PATCH",
            &format!("{PATH}/w1?fieldValidation=Warn"),
            Some("application/merge-patch+json"),
            None,
            Some(serde_json::to_vec(&json!({ "metadata": { "zzz": "x" } })).unwrap()),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
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

async fn patch_with(
    api: &TestApiServer,
    query: &str,
    content_type: &str,
    body: &Value,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, bytes, _) = api
        .send_full(
            "PATCH",
            &format!("{PATH}/w1?{query}"),
            Some(content_type),
            None,
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap_or_default().to_string())
        .collect();
    let out = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, warnings, out)
}

/// A JSON patch reaches the same strict decode as a merge patch.
#[tokio::test]
async fn json_patch_strict_refuses_the_unknown_root_metadata_field() {
    let api = setup().await;
    let mut clean = widget();
    clean["metadata"] = json!({ "name": "w1" });
    let (status, out) = api.post(PATH, &clean).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, _, out) = patch_with(
        &api,
        "fieldValidation=Strict",
        "application/json-patch+json",
        &json!([{ "op": "add", "path": "/metadata/zzz", "value": "x" }]),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .contains(r#"unknown field "metadata.zzz""#),
        "{out}"
    );
}

/// Warn: an unknown `spec` field the schema prunes is reported by a PATCH
/// as it is by a create (the decoder's `unknownFieldPaths`).
#[tokio::test]
async fn patch_warn_reports_a_pruned_unknown_spec_field() {
    let api = setup().await;
    let mut clean = widget();
    clean["metadata"] = json!({ "name": "w1" });
    let (status, out) = api.post(PATH, &clean).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, warnings, out) = patch_with(
        &api,
        "fieldValidation=Warn",
        "application/merge-patch+json",
        &json!({ "spec": { "nope": 1 } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains(r#"unknown field \"spec.nope\""#)),
        "{warnings:?}"
    );
}
