//! #2703: the PATCH document itself is strict-decoded, not only the patched
//! object. Upstream `jsonPatcher.applyJSPatch` (patch.go:389-401, :420-427)
//! and `strategicPatchObject` (patch.go:559-565) collect `appliedStrictErrs`
//! with `kjson.UnmarshalStrict`, and `applyPatchToCurrentObject`
//! (patch.go:338-363) puts them ahead of the object's own errors.
//! Expectations mirror test/integration/apiserver/field_validation_test.go
//! (`testFieldValidationPatchTyped`, :985-1100).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const MERGE: &str = "application/merge-patch+json";
const JSON_PATCH: &str = "application/json-patch+json";
const CM: &str = "/api/v1/namespaces/default/configmaps";
const GROUP: &str = "stable.example.com";

async fn patch(
    api: &TestApiServer,
    uri: &str,
    content_type: &str,
    body: &str,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _b, value) = api
        .send_full(
            "PATCH",
            uri,
            Some(content_type),
            None,
            Some(body.as_bytes().to_vec()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    (status, warnings, value)
}

async fn seed_cm(api: &TestApiServer) {
    let (s, out) = api
        .post(
            CM,
            &json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c"},"data":{"a":"1"}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
}

const DUP_MERGE: &str = r#"{"data":{"a":"2","a":"3"}}"#;

#[tokio::test]
async fn merge_patch_duplicate_key_strict_is_invalid_on_patch() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let uri = format!("{CM}/c?fieldValidation=Strict");
    let (status, _, out) = patch(&api, &uri, MERGE, DUP_MERGE).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(r#"strict decoding error: duplicate field "data.a""#),
        "{msg}"
    );
}

#[tokio::test]
async fn merge_patch_duplicate_key_warn_and_default_warn_ignore_is_silent() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    for q in ["?fieldValidation=Warn", ""] {
        let (status, warnings, out) = patch(&api, &format!("{CM}/c{q}"), MERGE, DUP_MERGE).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert!(
            warnings
                .iter()
                .any(|w| w.contains(r#"duplicate field \"data.a\""#)),
            "{q}: {warnings:?}"
        );
    }
    let (status, warnings, out) = patch(
        &api,
        &format!("{CM}/c?fieldValidation=Ignore"),
        MERGE,
        DUP_MERGE,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(warnings.is_empty(), "{warnings:?}");
}

const JSON_OPS: &str = r#"[{"op":"add","path":"/data/b","value":"1","foo":"x"},
 {"op":"add","path":"/data/c","path":"/data/c","value":"1"}]"#;

#[tokio::test]
async fn json_patch_unknown_and_duplicate_op_fields_strict() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, _, out) = patch(
        &api,
        &format!("{CM}/c?fieldValidation=Strict"),
        JSON_PATCH,
        JSON_OPS,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(
            r#"strict decoding error: json patch unknown field "[0].foo", json patch duplicate field "[1].path""#
        ),
        "{msg}"
    );
}

#[tokio::test]
async fn json_patch_unknown_and_duplicate_op_fields_warn() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, warnings, out) = patch(
        &api,
        &format!("{CM}/c?fieldValidation=Warn"),
        JSON_PATCH,
        JSON_OPS,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let joined = warnings.join("\n");
    assert!(
        joined.contains(r#"json patch unknown field \"[0].foo\""#),
        "{joined}"
    );
    assert!(
        joined.contains(r#"json patch duplicate field \"[1].path\""#),
        "{joined}"
    );
}

#[tokio::test]
async fn json_patch_strict_error_precedes_object_unknown_field() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    // The patched object also has an unknown top-level field.
    let ops = r#"[{"op":"add","path":"/bogus","value":1,"foo":"x"}]"#;
    let (status, _, out) = patch(
        &api,
        &format!("{CM}/c?fieldValidation=Strict"),
        JSON_PATCH,
        ops,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(
            r#"strict decoding error: json patch unknown field "[0].foo", unknown field "bogus""#
        ),
        "{msg}"
    );
}

#[tokio::test]
async fn custom_resource_merge_patch_duplicate_key_strict() {
    let api = TestApiServer::new();
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
        "metadata": {"name": format!("widgets.{GROUP}")},
        "spec": {"group": GROUP, "scope": "Namespaced",
            "names": {"plural":"widgets","kind":"Widget","listKind":"WidgetList"},
            "versions": [{"name":"v1","served":true,"storage":true,
                "schema":{"openAPIV3Schema":{"type":"object","x-kubernetes-preserve-unknown-fields":true}}}]}
    });
    let (s, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &crd,
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    let base = format!("/apis/{GROUP}/v1/namespaces/default/widgets");
    let (s, out) = api
        .post(
            &base,
            &json!({"apiVersion": format!("{GROUP}/v1"),"kind":"Widget","metadata":{"name":"w"},"spec":{"size":1}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    let (status, _, out) = patch(
        &api,
        &format!("{base}/w?fieldValidation=Strict"),
        MERGE,
        r#"{"spec":{"size":2,"size":3}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(r#"strict decoding error: duplicate field "spec.size""#),
        "{msg}"
    );
}
