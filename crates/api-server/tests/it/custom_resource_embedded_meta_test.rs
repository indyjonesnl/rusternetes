//! #3304: the request decoder coerces the `metadata` of an
//! `x-kubernetes-embedded-resource` object and reports its unknown fields.
//!
//! Upstream: `unstructuredSchemaCoercer.apply` runs
//! `schemaobjectmeta.CoerceWithOptions` on the decoded object
//! (customresource_handler.go:1442-1449); the embedded `metadata` round-trips
//! through `ObjectMeta` (schema/objectmeta/algorithm.go:98-116), dropping what
//! it does not declare, a malformed one is a `field.Invalid` that the decoder
//! error path wraps in `transformDecodeError` (handlers/rest.go:245-256), and
//! the unknown paths feed the `StrictDecodingError` (:1338-1342).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";

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
                    "properties": { "spec": {
                        "type": "object",
                        "properties": {
                            "emb": {
                                "type": "object",
                                "x-kubernetes-embedded-resource": true,
                                "x-kubernetes-preserve-unknown-fields": true
                            }
                        }
                    } }
                } }
            }]
        }
    })
}

fn widget(emb_metadata: Value) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": "w1" },
        "spec": { "emb": {
            "apiVersion": "v1", "kind": "Pod", "metadata": emb_metadata } }
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

const PATH: &str = "/apis/stable.example.com/v1/namespaces/default/widgets";

#[tokio::test]
async fn embedded_metadata_is_pruned_to_object_meta() {
    let api = setup().await;
    let body = widget(json!({"name": "e1", "zzz": "x", "labels": {"a": null}}));
    let (status, out) = api.post(PATH, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(
        out["spec"]["emb"]["metadata"],
        json!({"name": "e1", "labels": {"a": ""}}),
        "{out}"
    );
}

#[tokio::test]
async fn warn_mode_reports_the_unknown_embedded_metadata_field() {
    let api = setup().await;
    let body = widget(json!({"name": "e1", "zzz": "x"}));
    let (status, headers, bytes, _) = api
        .send_full(
            "POST",
            PATH,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
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
        joined.contains(r#"unknown field \"spec.emb.metadata.zzz\""#),
        "{joined:?}"
    );
}

/// Strict mode refuses the same field with the decoder's wording.
#[tokio::test]
async fn strict_mode_refuses_the_unknown_embedded_metadata_field() {
    let api = setup().await;
    let body = widget(json!({"name": "e1", "zzz": "x"}));
    let (status, out) = api
        .post(&format!("{PATH}?fieldValidation=Strict"), &body)
        .await;
    // The status code is whatever `check_strict` gives an unknown top-level
    // field too; upstream's is 400 (create.go:131-139), tracked by #3306.
    assert!(status.is_client_error(), "{status} {out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .contains(r#"unknown field "spec.emb.metadata.zzz""#),
        "{out}"
    );
}

#[tokio::test]
async fn malformed_embedded_metadata_is_a_decode_error() {
    let api = setup().await;
    let body = widget(json!({"name": "e1", "labels": 3}));
    let (status, out) = api.post(PATH, &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(
        out["message"],
        "Widget in version \"v1\" cannot be handled as a Widget: spec.emb.metadata: Invalid value: {\"labels\":3,\"name\":\"e1\"}: json: cannot unmarshal number into Go struct field ObjectMeta.labels of type map[string]string",
        "{out}"
    );
}
