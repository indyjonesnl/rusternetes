//! `propagationPolicy` must work on EVERY resource, not just workloads, and it
//! must be read from the DELETE **body** — which is where kubectl puts it.
//!
//! Upstream applies this to every resource at once, with no kind check, in the
//! generic registry store: `deletionFinalizersForGarbageCollection`
//! (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:976)
//! adds `orphan` / `foregroundDeletion` based on `shouldOrphanDependents`
//! (:883) and `shouldDeleteDependents` (:932). And `DeleteOptions` itself is
//! decoded once, before the registry, in
//! `endpoints/handlers/delete.go:86`.
//!
//! Rusternetes had neither property. Only 8 of ~50 delete handlers passed a
//! propagation policy at all, and only the pod handler read the request body —
//! so `kubectl delete configmap --cascade=orphan` was silently a Background
//! delete. Any object can be an owner, so that is wrong for every kind.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

/// Create an object, DELETE it with the given DeleteOptions **body**, and
/// return what is left in storage (`None` once it is really gone).
async fn delete_with_body(
    state: &TestApiServer,
    collection: &str,
    name: &str,
    create: &serde_json::Value,
    delete_options: &serde_json::Value,
) -> Option<serde_json::Value> {
    let (status, _, created) = state
        .send_raw("POST", collection, Some("application/json"), Some(create))
        .await;
    assert!(status.is_success(), "create failed {status}: {created}");

    let (status, _, body) = state
        .send_raw(
            "DELETE",
            &format!("{collection}/{name}"),
            Some("application/json"),
            Some(delete_options),
        )
        .await;
    assert!(status.is_success(), "delete failed {status}: {body}");

    let (status, _, stored) = state
        .send_raw("GET", &format!("{collection}/{name}"), None, None)
        .await;
    if status.is_success() {
        Some(stored)
    } else {
        None
    }
}

fn finalizers(obj: &serde_json::Value) -> Vec<String> {
    obj["metadata"]["finalizers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn configmap(name: &str) -> serde_json::Value {
    json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name},"data":{"k":"v"}})
}

/// A non-workload resource must honour `Orphan` sent in the DELETE body.
/// Before this was wired, the ConfigMap handler ignored propagation entirely
/// and the object was deleted outright.
#[tokio::test]
async fn orphan_in_the_delete_body_is_honoured_on_a_plain_resource() {
    let state = TestApiServer::new();
    let left = delete_with_body(
        &state,
        "/api/v1/namespaces/default/configmaps",
        "cm-orphan",
        &configmap("cm-orphan"),
        &json!({"apiVersion":"v1","kind":"DeleteOptions","propagationPolicy":"Orphan"}),
    )
    .await
    .expect("an Orphan delete must leave the object pending its orphan finalizer");

    assert_eq!(
        finalizers(&left),
        vec!["orphan".to_string()],
        "upstream adds metav1.FinalizerOrphanDependents for Orphan \
         (store.go:992-994); got {left}"
    );
    assert!(
        left["metadata"]["deletionTimestamp"].as_str().is_some(),
        "an object held by a GC finalizer must be marked for deletion: {left}"
    );
}

/// Same for `Foreground`, which upstream marks with a different finalizer.
#[tokio::test]
async fn foreground_in_the_delete_body_is_honoured_on_a_plain_resource() {
    let state = TestApiServer::new();
    let left = delete_with_body(
        &state,
        "/api/v1/namespaces/default/configmaps",
        "cm-fg",
        &configmap("cm-fg"),
        &json!({"apiVersion":"v1","kind":"DeleteOptions","propagationPolicy":"Foreground"}),
    )
    .await
    .expect("a Foreground delete must leave the object pending its finalizer");

    assert_eq!(
        finalizers(&left),
        vec!["foregroundDeletion".to_string()],
        "upstream adds metav1.FinalizerDeleteDependents for Foreground \
         (store.go:995-997); got {left}"
    );
}

/// Switching policy must REPLACE the GC finalizer, not accumulate.
///
/// Upstream strips both `orphan` and `foregroundDeletion` and then re-adds only
/// the one that applies (store.go:984-997). Appending — which is what the
/// propagation-aware helper used to do — leaves an object carrying both, so it
/// is orphaned *and* foreground-deleted, and neither controller can finish it.
#[tokio::test]
async fn switching_policy_replaces_the_gc_finalizer_instead_of_accumulating() {
    let state = TestApiServer::new();
    let collection = "/api/v1/namespaces/default/configmaps";

    let left = delete_with_body(
        &state,
        collection,
        "cm-switch",
        &configmap("cm-switch"),
        &json!({"propagationPolicy":"Foreground"}),
    )
    .await
    .expect("still pending after the first delete");
    assert_eq!(finalizers(&left), vec!["foregroundDeletion".to_string()]);

    let (status, _, body) = state
        .send_raw(
            "DELETE",
            &format!("{collection}/cm-switch"),
            Some("application/json"),
            Some(&json!({"propagationPolicy":"Orphan"})),
        )
        .await;
    assert!(status.is_success(), "second delete failed {status}: {body}");

    let (_, _, stored) = state
        .send_raw("GET", &format!("{collection}/cm-switch"), None, None)
        .await;
    assert_eq!(
        finalizers(&stored),
        vec!["orphan".to_string()],
        "the foregroundDeletion finalizer must be stripped, not kept alongside \
         orphan (store.go:984-997); got {stored}"
    );
}

/// A user finalizer is not a GC finalizer and must survive untouched: upstream
/// only ever strips the two it owns.
#[tokio::test]
async fn a_user_finalizer_survives_the_gc_recomputation() {
    let state = TestApiServer::new();
    let mut cm = configmap("cm-user-fin");
    cm["metadata"]["finalizers"] = json!(["example.com/keep"]);

    let left = delete_with_body(
        &state,
        "/api/v1/namespaces/default/configmaps",
        "cm-user-fin",
        &cm,
        &json!({"propagationPolicy":"Orphan"}),
    )
    .await
    .expect("a user finalizer holds the object regardless");

    let f = finalizers(&left);
    assert!(
        f.contains(&"example.com/keep".to_string()) && f.contains(&"orphan".to_string()),
        "the user's finalizer must be preserved alongside the GC one; got {f:?}"
    );
}

/// The deprecated `orphanDependents` bool overrides `propagationPolicy`
/// entirely — upstream checks it first, ahead of the policy
/// (store.go:898-901), and older clients still send it.
#[tokio::test]
async fn the_deprecated_orphan_dependents_bool_overrides_the_policy() {
    let state = TestApiServer::new();
    let left = delete_with_body(
        &state,
        "/api/v1/namespaces/default/configmaps",
        "cm-deprecated",
        &configmap("cm-deprecated"),
        &json!({"propagationPolicy":"Foreground","orphanDependents":true}),
    )
    .await
    .expect("orphanDependents=true must orphan, so the object stays");

    assert_eq!(
        finalizers(&left),
        vec!["orphan".to_string()],
        "orphanDependents=true wins over propagationPolicy=Foreground \
         (store.go:898-901); got {left}"
    );
}

/// No DeleteOptions at all is Background: no GC finalizer, object gone.
#[tokio::test]
async fn a_delete_without_options_still_deletes_outright() {
    let state = TestApiServer::new();
    let state_ref = &state;
    let (status, _, created) = state_ref
        .send_raw(
            "POST",
            "/api/v1/namespaces/default/configmaps",
            Some("application/json"),
            Some(&configmap("cm-plain")),
        )
        .await;
    assert!(status.is_success(), "create failed {status}: {created}");

    let (status, _, body) = state_ref
        .send_raw(
            "DELETE",
            "/api/v1/namespaces/default/configmaps/cm-plain",
            None,
            None,
        )
        .await;
    assert!(status.is_success(), "delete failed {status}: {body}");

    let (status, _, _) = state_ref
        .send_raw(
            "GET",
            "/api/v1/namespaces/default/configmaps/cm-plain",
            None,
            None,
        )
        .await;
    assert!(
        status.is_client_error(),
        "a Background delete must remove the object outright"
    );
}
