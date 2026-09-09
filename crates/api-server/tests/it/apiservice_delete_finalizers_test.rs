//! APIService is stored as an untyped `serde_json::Value`, and its delete
//! handlers used to remove the object outright:
//!
//! ```rust,ignore
//! let deleted: Value = state.storage.get(&key).await?;
//! state.storage.delete(&key).await?;
//! ```
//!
//! Upstream has no untyped path. `Store.Delete` calls
//! `deletionFinalizersForGarbageCollection`
//! (`registry/generic/registry/store.go:976`) for every resource, with no kind
//! or representation check, so a finalizer holds an APIService exactly as it
//! holds a ConfigMap, and `propagationPolicy` is honoured the same way.
//!
//! These tests drive the real routes, so they pin the behaviour rather than
//! the shape of the code — the structural half lives in
//! `delete_propagation_guard_test.rs` (#1911).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const COLLECTION: &str = "/apis/apiregistration.k8s.io/v1/apiservices";

fn apiservice(name: &str, finalizers: Option<Vec<&str>>) -> Value {
    let mut metadata = json!({ "name": name, "labels": { "probe": "yes" } });
    if let Some(f) = finalizers {
        metadata["finalizers"] = json!(f);
    }
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": metadata,
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 100,
            "versionPriority": 100,
        },
    })
}

async fn create(state: &TestApiServer, body: &Value) {
    let (code, created) = state
        .send("POST", COLLECTION, Some("application/json"), Some(body))
        .await;
    assert_eq!(code, StatusCode::CREATED, "create failed: {created}");
}

async fn get(state: &TestApiServer, name: &str) -> (StatusCode, Value) {
    state
        .send("GET", &format!("{COLLECTION}/{name}"), None, None)
        .await
}

#[tokio::test]
async fn a_finalizer_holds_an_apiservice_through_delete() {
    let state = TestApiServer::new();
    let name = "v1alpha1.held.example.com";
    create(&state, &apiservice(name, Some(vec!["example.com/hold"]))).await;

    let (code, deleted) = state
        .send("DELETE", &format!("{COLLECTION}/{name}"), None, None)
        .await;
    assert_eq!(code, StatusCode::OK, "delete failed: {deleted}");

    let (code, live) = get(&state, name).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "a finalizer must keep the APIService alive, but it was removed: {live}"
    );
    assert!(
        live.pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "held APIService carries no deletionTimestamp: {live}"
    );
    assert_eq!(
        live.pointer("/metadata/finalizers"),
        Some(&json!(["example.com/hold"])),
        "the finalizer must survive the delete: {live}"
    );
    // Everything outside metadata has to survive the round-trip through the
    // typed ObjectMeta view.
    assert_eq!(
        live.pointer("/spec/group"),
        Some(&json!("wardle.example.com")),
        "spec was lost while marking for deletion: {live}"
    );
}

#[tokio::test]
async fn draining_the_last_finalizer_finishes_the_delete() {
    let state = TestApiServer::new();
    let name = "v1alpha1.drain.example.com";
    create(&state, &apiservice(name, Some(vec!["example.com/hold"]))).await;

    let (code, _) = state
        .send("DELETE", &format!("{COLLECTION}/{name}"), None, None)
        .await;
    assert_eq!(code, StatusCode::OK);

    let (_, mut live) = get(&state, name).await;
    live["metadata"]["finalizers"] = json!([]);
    let (code, updated) = state
        .send(
            "PUT",
            &format!("{COLLECTION}/{name}"),
            Some("application/json"),
            Some(&live),
        )
        .await;
    assert_eq!(code, StatusCode::OK, "finalizer removal failed: {updated}");

    let (code, body) = get(&state, name).await;
    assert_eq!(
        code,
        StatusCode::NOT_FOUND,
        "draining the last finalizer must complete the pending delete: {body}"
    );
}

/// `propagationPolicy=Foreground` stamps `foregroundDeletion`
/// (`deletionFinalizersForGarbageCollection`, store.go:984-997) — the request
/// option was silently dropped on this path.
#[tokio::test]
async fn foreground_propagation_stamps_the_gc_finalizer() {
    let state = TestApiServer::new();
    let name = "v1alpha1.foreground.example.com";
    create(&state, &apiservice(name, None)).await;

    let (code, deleted) = state
        .send(
            "DELETE",
            &format!("{COLLECTION}/{name}?propagationPolicy=Foreground"),
            None,
            None,
        )
        .await;
    assert_eq!(code, StatusCode::OK, "delete failed: {deleted}");

    let (code, live) = get(&state, name).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "a foreground delete must leave the object for the GC: {live}"
    );
    assert_eq!(
        live.pointer("/metadata/finalizers"),
        Some(&json!(["foregroundDeletion"])),
        "foregroundDeletion was not stamped: {live}"
    );
}

/// The unheld case must still delete outright — otherwise the assertions above
/// would pass on a handler that simply never deletes anything.
#[tokio::test]
async fn an_unheld_apiservice_is_deleted_outright() {
    let state = TestApiServer::new();
    let name = "v1alpha1.free.example.com";
    create(&state, &apiservice(name, None)).await;

    let (code, _) = state
        .send("DELETE", &format!("{COLLECTION}/{name}"), None, None)
        .await;
    assert_eq!(code, StatusCode::OK);

    let (code, body) = get(&state, name).await;
    assert_eq!(code, StatusCode::NOT_FOUND, "still present: {body}");
}

#[tokio::test]
async fn deletecollection_honours_finalizers() {
    let state = TestApiServer::new();
    let held = "v1alpha1.collection-held.example.com";
    let free = "v1alpha1.collection-free.example.com";
    create(&state, &apiservice(held, Some(vec!["example.com/hold"]))).await;
    create(&state, &apiservice(free, None)).await;

    let (code, status) = state
        .send(
            "DELETE",
            &format!("{COLLECTION}?labelSelector=probe%3Dyes"),
            None,
            None,
        )
        .await;
    assert_eq!(code, StatusCode::OK, "deletecollection failed: {status}");

    let (code, live) = get(&state, held).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "deletecollection ignored the finalizer: {live}"
    );
    assert!(
        live.pointer("/metadata/deletionTimestamp").is_some(),
        "held APIService carries no deletionTimestamp: {live}"
    );

    let (code, body) = get(&state, free).await;
    assert_eq!(
        code,
        StatusCode::NOT_FOUND,
        "the unheld APIService should have been removed: {body}"
    );
}
