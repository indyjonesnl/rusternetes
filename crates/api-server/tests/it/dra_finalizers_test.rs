//! DRA resources honour finalizers and propagationPolicy like every other kind.
//!
//! They did not, and the reason was structural rather than per-handler: the DRA
//! types declared their own `dra::ObjectMeta`, so they could not implement
//! `HasMetadata`, so neither `handle_delete_with_finalizers` nor
//! `delete_collection_item` typechecked against them. Eight delete paths
//! carried this comment instead:
//!
//! ```text
//! // NOTE: DRA resources use dra::ObjectMeta which is incompatible with finalizers.
//! // We perform a simple delete without finalizer support.
//! state.storage.delete(&key).await?;
//! ```
//!
//! Upstream has no such split: `Store.Delete` runs
//! `deletionFinalizersForGarbageCollection`
//! (`registry/generic/registry/store.go:976`) for every resource with no kind
//! check, and `updateForGracefulDeletionAndFinalizers` (`:1044`) stamps
//! `deletionTimestamp` and keeps the object while a finalizer remains.
//!
//! With one shared `ObjectMeta` (#1895) the generic path reaches DRA, so these
//! assertions are about the behaviour that unlocks.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DEVICECLASSES: &str = "/apis/resource.k8s.io/v1/deviceclasses";
const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

fn device_class(name: &str, finalizers: Option<Value>) -> Value {
    let mut meta = json!({"name": name});
    if let Some(f) = finalizers {
        meta["finalizers"] = f;
    }
    json!({"apiVersion":"resource.k8s.io/v1","kind":"DeviceClass",
           "metadata": meta, "spec": {}})
}

fn claim(name: &str, finalizers: Option<Value>) -> Value {
    let mut meta = json!({"name": name});
    if let Some(f) = finalizers {
        meta["finalizers"] = f;
    }
    json!({"apiVersion":"resource.k8s.io/v1","kind":"ResourceClaim",
           "metadata": meta, "spec": {}})
}

/// The core of it: a finalizer must keep the object alive with a
/// `deletionTimestamp`, not let it be deleted outright.
#[tokio::test]
async fn a_finalizer_keeps_a_deviceclass_alive_with_a_deletion_timestamp() {
    let state = TestApiServer::new();
    let body = device_class("dc-fin", Some(json!(["example.com/protect"])));
    let (code, created) = state.post(DEVICECLASSES, &body).await;
    assert_eq!(code, StatusCode::CREATED, "{created}");

    let (code, deleted) = state.delete(&format!("{DEVICECLASSES}/dc-fin")).await;
    assert!(code.is_success(), "{code}: {deleted}");

    // Still there, and now marked for deletion.
    let (code, got) = state.get(&format!("{DEVICECLASSES}/dc-fin")).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "a finalized DeviceClass must survive DELETE: {got}"
    );
    assert!(
        got["metadata"]["deletionTimestamp"].is_string(),
        "DELETE must stamp deletionTimestamp: {got}"
    );
    assert_eq!(
        got["metadata"]["finalizers"],
        json!(["example.com/protect"]),
        "the finalizer must be preserved: {got}"
    );
}

/// And once the finalizer is gone, the object goes.
#[tokio::test]
async fn clearing_the_finalizer_lets_the_deviceclass_go() {
    let state = TestApiServer::new();
    let body = device_class("dc-drain", Some(json!(["example.com/protect"])));
    let (code, created) = state.post(DEVICECLASSES, &body).await;
    assert_eq!(code, StatusCode::CREATED, "{created}");

    let (code, _) = state.delete(&format!("{DEVICECLASSES}/dc-drain")).await;
    assert!(code.is_success());

    // Drain the finalizer with a PUT, carrying the stored deletionTimestamp.
    let (_, pending) = state.get(&format!("{DEVICECLASSES}/dc-drain")).await;
    let mut drained = pending.clone();
    drained["metadata"]["finalizers"] = json!([]);
    let (code, updated) = state
        .put(&format!("{DEVICECLASSES}/dc-drain"), &drained)
        .await;
    assert!(code.is_success(), "{code}: {updated}");

    let (code, got) = state.get(&format!("{DEVICECLASSES}/dc-drain")).await;
    assert_eq!(
        code,
        StatusCode::NOT_FOUND,
        "draining the last finalizer must complete the deletion: {got}"
    );
}

/// Without a finalizer nothing changes: the object is deleted outright.
#[tokio::test]
async fn a_deviceclass_without_finalizers_is_deleted_outright() {
    let state = TestApiServer::new();
    let (code, created) = state
        .post(DEVICECLASSES, &device_class("dc-plain", None))
        .await;
    assert_eq!(code, StatusCode::CREATED, "{created}");

    let (code, _) = state.delete(&format!("{DEVICECLASSES}/dc-plain")).await;
    assert!(code.is_success());

    let (code, got) = state.get(&format!("{DEVICECLASSES}/dc-plain")).await;
    assert_eq!(code, StatusCode::NOT_FOUND, "{got}");
}

/// The namespaced kind takes the same path.
#[tokio::test]
async fn a_finalizer_keeps_a_resourceclaim_alive() {
    let state = TestApiServer::new();
    let body = claim("rc-fin", Some(json!(["example.com/protect"])));
    let (code, created) = state.post(CLAIMS, &body).await;
    assert_eq!(code, StatusCode::CREATED, "{created}");

    let (code, _) = state.delete(&format!("{CLAIMS}/rc-fin")).await;
    assert!(code.is_success());

    let (code, got) = state.get(&format!("{CLAIMS}/rc-fin")).await;
    assert_eq!(code, StatusCode::OK, "{got}");
    assert!(got["metadata"]["deletionTimestamp"].is_string(), "{got}");
}

/// DeleteCollection goes through the same shared helper, so a finalized item
/// must survive it too — the collection path had its own copy of the bypass.
#[tokio::test]
async fn deletecollection_respects_a_finalizer() {
    let state = TestApiServer::new();
    let (code, _) = state
        .post(
            DEVICECLASSES,
            &device_class("dc-keep", Some(json!(["example.com/protect"]))),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED);
    let (code, _) = state
        .post(DEVICECLASSES, &device_class("dc-go", None))
        .await;
    assert_eq!(code, StatusCode::CREATED);

    let (code, body) = state.delete(DEVICECLASSES).await;
    assert!(code.is_success(), "{code}: {body}");

    let (code, kept) = state.get(&format!("{DEVICECLASSES}/dc-keep")).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "a finalized item must survive DeleteCollection: {kept}"
    );
    assert!(kept["metadata"]["deletionTimestamp"].is_string(), "{kept}");

    let (code, gone) = state.get(&format!("{DEVICECLASSES}/dc-go")).await;
    assert_eq!(code, StatusCode::NOT_FOUND, "{gone}");
}

// ---------------------------------------------------------------------------
// The drain rule, applied laterally
// ---------------------------------------------------------------------------
//
// Adding the DRA handlers to `finalizer_drain_guard_test` showed 22 *other*
// update handlers were missing the same call, so a PUT draining the last
// finalizer left those objects in storage forever with a deletionTimestamp and
// nothing left to remove them. These cases sample that set across shapes: a
// namespaced resource, a cluster-scoped one, an upsert-on-update path, and the
// untyped-document path.

async fn assert_drain_completes_deletion(label: &str, collection: &str, body: Value) {
    let state = TestApiServer::new();
    let name = body["metadata"]["name"]
        .as_str()
        .expect("named")
        .to_string();

    let (code, created) = state.post(collection, &body).await;
    assert!(
        code.is_success(),
        "{label}: create failed {code}: {created}"
    );

    let (code, _) = state.delete(&format!("{collection}/{name}")).await;
    assert!(code.is_success(), "{label}: delete failed {code}");

    let (code, pending) = state.get(&format!("{collection}/{name}")).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "{label}: a finalized object must survive DELETE: {pending}"
    );
    assert!(
        pending["metadata"]["deletionTimestamp"].is_string(),
        "{label}: DELETE must stamp deletionTimestamp: {pending}"
    );

    let mut drained = pending.clone();
    drained["metadata"]["finalizers"] = json!([]);
    let (code, updated) = state.put(&format!("{collection}/{name}"), &drained).await;
    assert!(code.is_success(), "{label}: PUT failed {code}: {updated}");

    let (code, got) = state.get(&format!("{collection}/{name}")).await;
    assert_eq!(
        code,
        StatusCode::NOT_FOUND,
        "{label}: draining the last finalizer must complete the deletion in the \
         same request (upstream ShouldDeleteDuringUpdate, store.go:565): {got}"
    );
}

#[tokio::test]
async fn draining_the_last_finalizer_completes_deletion_for_a_namespaced_resource() {
    assert_drain_completes_deletion(
        "endpoints",
        "/api/v1/namespaces/default/endpoints",
        json!({"apiVersion":"v1","kind":"Endpoints",
               "metadata":{"name":"ep-drain","finalizers":["example.com/p"]},
               "subsets":[]}),
    )
    .await;
}

#[tokio::test]
async fn draining_the_last_finalizer_completes_deletion_for_a_cluster_scoped_resource() {
    assert_drain_completes_deletion(
        "clusterroles",
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole",
               "metadata":{"name":"cr-drain","finalizers":["example.com/p"]},
               "rules":[]}),
    )
    .await;
}

/// Lease takes the update-then-create upsert path, so the drain call sits after
/// a `match` rather than a plain `?`.
#[tokio::test]
async fn draining_the_last_finalizer_completes_deletion_on_the_upsert_path() {
    assert_drain_completes_deletion(
        "leases",
        "/apis/coordination.k8s.io/v1/namespaces/default/leases",
        json!({"apiVersion":"coordination.k8s.io/v1","kind":"Lease",
               "metadata":{"name":"lease-drain","finalizers":["example.com/p"]},
               "spec":{"holderIdentity":"h"}}),
    )
    .await;
}

#[tokio::test]
async fn draining_the_last_finalizer_completes_deletion_for_a_namespaced_policy() {
    assert_drain_completes_deletion(
        "networkpolicies",
        "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies",
        json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy",
               "metadata":{"name":"np-drain","finalizers":["example.com/p"]},
               "spec":{"podSelector":{}}}),
    )
    .await;
}
