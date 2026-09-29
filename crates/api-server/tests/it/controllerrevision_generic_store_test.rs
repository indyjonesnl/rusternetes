//! ControllerRevision served through the generic Store and endpoint handlers
//! (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin ControllerRevision's own:
//! `pkg/registry/apps/controllerrevision/strategy.go` and the validators it
//! calls (`pkg/apis/apps/validation/validation.go:328-367`).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CR: &str = "/apis/apps/v1/namespaces/default/controllerrevisions";

fn revision(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "ControllerRevision",
        "metadata": {"name": name},
        "data": {"spec": {"template": {"metadata": {"labels": {"app": name}}}}},
        "revision": 1
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(CR, &revision(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `ValidateControllerRevisionCreate` (validation.go:340-358): `data` is
/// mandatory and must be a JSON object.
#[tokio::test]
async fn create_requires_object_data() {
    let api = TestApiServer::new();
    let mut missing = revision("r-missing");
    missing.as_object_mut().unwrap().remove("data");
    let (status, out) = api.post(CR, &missing).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("data is mandatory"), "{out}");

    let mut array = revision("r-array");
    array["data"] = json!([1, 2]);
    let (status, out) = api.post(CR, &array).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("data must be a valid JSON object"),
        "{out}"
    );
}

/// `ValidateControllerRevisionUpdate` (validation.go:361-367): `revision`
/// and metadata may change, `data` may not.
#[tokio::test]
async fn update_allows_revision_and_forbids_data_changes() {
    let api = TestApiServer::new();
    let obj = create(&api, "r-update").await;
    let uri = format!("{CR}/r-update");

    let mut bumped = obj.clone();
    bumped["revision"] = json!(2);
    bumped["metadata"]["labels"] = json!({"l": "v"});
    let (status, out) = api.put(&uri, &bumped).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["revision"], 2, "{out}");

    let mut changed = out;
    changed["data"] = json!({"spec": {"other": true}});
    let (status, out) = api.put(&uri, &changed).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("data: Invalid value"), "{out}");
    assert!(message(&out).contains("field is immutable"), "{out}");
}

/// A PATCH goes through the same strategy as a PUT. The old PATCH handler
/// skipped update validation, so `data` could be patched.
#[tokio::test]
async fn patch_cannot_change_data() {
    let api = TestApiServer::new();
    create(&api, "r-patch").await;
    let uri = format!("{CR}/r-patch");

    let (status, out) = api
        .patch(&uri, &json!({"data": {"spec": {"other": true}}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("field is immutable"), "{out}");

    let (status, out) = api.patch(&uri, &json!({"revision": -1})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("revision"), "{out}");

    let (status, out) = api.patch(&uri, &json!({"revision": 3})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["revision"], 3, "{out}");
}

/// The cross-namespace path serves LIST and WATCH only
/// (staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:589-596).
/// DELETECOLLECTION is namespaced.
#[tokio::test]
async fn there_is_no_cross_namespace_delete_collection() {
    let api = TestApiServer::new();
    create(&api, "r-keep").await;
    let (status, out) = api.delete("/apis/apps/v1/controllerrevisions").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{out}");

    let (status, list) = api.get("/apis/apps/v1/controllerrevisions").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["items"].as_array().map(Vec::len), Some(1), "{list}");
}

/// `Store.DeleteCollection` returns the list of what it deleted.
#[tokio::test]
async fn delete_collection_returns_the_deleted_list() {
    let api = TestApiServer::new();
    create(&api, "r-a").await;
    create(&api, "r-b").await;
    let (status, out) = api.delete(CR).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "ControllerRevisionList", "{out}");
    assert_eq!(out["items"].as_array().map(Vec::len), Some(2), "{out}");
}
