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

/// #3468: the ROOT `metadata` goes through `GetObjectMetaWithOptions` with
/// `DropMalformedFields` too (`unstructuredSchemaCoercer.apply`,
/// customresource_handler.go:1415-1419, `dropInvalidMetadata` :1282), so a
/// stored CR whose root `metadata.labels` is `3` is served with the field
/// dropped, not failed.
async fn setup_with_malformed_root_metadata() -> TestApiServer {
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
            "creationTimestamp": "2020-01-01T00:00:00Z",
            "labels": 3, "annotations": {"a": "b"}
        },
        "spec": {}
    });
    let key = build_key("stable_example_com_widgets", Some("default"), "w1");
    api.storage.create(&key, &stored).await.unwrap();
    api
}

#[tokio::test]
async fn get_drops_malformed_root_metadata_fields() {
    let api = setup_with_malformed_root_metadata().await;
    let (status, out) = api.get(&format!("{PATH}/w1")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["metadata"].get("labels").is_none(), "{out}");
    assert_eq!(out["metadata"]["annotations"], json!({"a": "b"}), "{out}");
    assert_eq!(out["metadata"]["name"], "w1", "{out}");
}

#[tokio::test]
async fn list_drops_malformed_root_metadata_fields() {
    let api = setup_with_malformed_root_metadata().await;
    let (status, out) = api.get(PATH).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["items"][0]["metadata"]["name"], "w1", "{out}");
    assert!(out["items"][0]["metadata"].get("labels").is_none(), "{out}");
}

#[tokio::test]
async fn update_and_delete_read_a_malformed_root_metadata_object() {
    let api = setup_with_malformed_root_metadata().await;
    let (_, current) = api.get(&format!("{PATH}/w1")).await;
    let rv = current["metadata"]["resourceVersion"].clone();
    let (status, out) = api
        .put(
            &format!("{PATH}/w1"),
            &json!({
                "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
                "metadata": {"name": "w1", "namespace": "default", "resourceVersion": rv},
                "spec": {"x": 1}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["metadata"].get("labels").is_none(), "{out}");

    let api = setup_with_malformed_root_metadata().await;
    let (status, out) = api.delete(&format!("{PATH}/w1")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

/// #3950: a watch decodes its events through the same storage codec
/// (customresource_handler.go:1280-1283), so an event whose root
/// `metadata.labels` is `3` is delivered with the field dropped — both the
/// synthetic initial ADDED and a live MODIFIED.
#[tokio::test]
async fn watch_drops_malformed_root_metadata_fields() {
    use futures::StreamExt;
    let api = setup_with_malformed_root_metadata().await;
    let resp = api
        .respond(
            "GET",
            &format!("{PATH}?watch=true&resourceVersion=0&timeoutSeconds=10"),
            None,
            None,
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let mut stream = resp.into_body().into_data_stream();

    // A live write of another malformed object after the watch opened.
    let key = build_key("stable_example_com_widgets", Some("default"), "w2");
    let stored = json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": {
            "name": "w2", "namespace": "default", "uid": "u2", "generation": 1,
            "creationTimestamp": "2020-01-01T00:00:00Z", "labels": 3
        },
        "spec": {}
    });
    api.storage.create(&key, &stored).await.unwrap();

    let mut buf = String::new();
    let mut seen: Vec<Value> = Vec::new();
    let run = async {
        while seen.len() < 2 {
            let Some(Ok(bytes)) = stream.next().await else {
                return;
            };
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if v["type"] != "BOOKMARK" {
                        seen.push(v);
                    }
                }
            }
        }
    };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), run).await;
    let mut names: Vec<&str> = seen
        .iter()
        .filter_map(|e| e["object"]["metadata"]["name"].as_str())
        .collect();
    names.sort();
    assert_eq!(names, ["w1", "w2"], "{seen:?}");
    for e in &seen {
        assert!(e["object"]["metadata"].get("labels").is_none(), "{e}");
    }
}
