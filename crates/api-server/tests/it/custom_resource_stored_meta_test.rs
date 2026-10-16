//! #3398: what a custom resource is read from storage as has its embedded
//! `metadata` repaired, not refused.
//!
//! Upstream decodes the storage read through `crdConversionRESTOptionsGetter`
//! with `dropInvalidMetadata: true, repairGeneration: true`
//! (customresource_handler.go:1280-1283): "drop invalid fields while decoding
//! old CRs (before we haven't had any ObjectMeta validation)". The embedded
//! `metadata` of an `x-kubernetes-embedded-resource` then goes through
//! `schemaobjectmeta.CoerceWithOptions` with `DropInvalidFields: true`
//! (:1442-1445), which drops each malformed field instead of failing
//! (schema/objectmeta/coerce.go:94-131).

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
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

/// A CR as written before ObjectMeta was validated: its embedded metadata
/// carries a malformed `labels` next to a good `name`.
async fn setup_with_malformed_stored_cr() -> TestApiServer {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &crd(),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("widgets.{GROUP}")).await;
    let stored = json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": {
            "name": "w1", "namespace": "default", "uid": "u1", "generation": 1,
            "creationTimestamp": "2020-01-01T00:00:00Z"
        },
        "spec": { "emb": {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "e1", "labels": 3 } } }
    });
    let key = build_key("stable_example_com_widgets", Some("default"), "w1");
    api.storage.create(&key, &stored).await.unwrap();
    api
}

#[tokio::test]
async fn get_drops_malformed_embedded_metadata_fields() {
    let api = setup_with_malformed_stored_cr().await;
    let (status, out) = api.get(&format!("{PATH}/w1")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["spec"]["emb"]["metadata"],
        json!({"name": "e1"}),
        "{out}"
    );
}

#[tokio::test]
async fn list_drops_malformed_embedded_metadata_fields() {
    let api = setup_with_malformed_stored_cr().await;
    let (status, out) = api.get(PATH).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["items"][0]["spec"]["emb"]["metadata"],
        json!({"name": "e1"}),
        "{out}"
    );
}
