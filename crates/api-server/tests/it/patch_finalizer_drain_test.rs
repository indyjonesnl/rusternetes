//! A PATCH that drains the last finalizer must finish the deletion — the same
//! rule PUT already obeyed.
//!
//! Upstream has exactly one write path: `Store.Update`
//! (`staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:565`)
//! consults `ShouldDeleteDuringUpdate` and calls `deleteWithoutFinalizers`, and
//! the PATCH endpoint reaches storage *through* that same `Update`
//! (`endpoints/handlers/patch.go` → `rest.Updater`). So "an update that removes
//! the last finalizer from an object already pending deletion completes the
//! deletion in that request" holds for PUT and PATCH alike, for every resource,
//! by construction.
//!
//! Rusternetes had it per-handler, and #1833 wired it into all 55 **update**
//! handlers only. PATCH is the mechanism that actually matters: upstream's
//! garbage collector removes a finalizer with a JSON **merge patch** —
//! `removeFinalizer` builds `objectForFinalizersPatch` and sends it as
//! `types.MergePatchType` (`pkg/controller/garbagecollector/operations.go:104-146`,
//! patch issued at :141 via `patchObject`). With the drain check missing from
//! the PATCH path, the GC's own way of finishing a deletion silently did
//! nothing: the object kept its `deletionTimestamp`, lost its finalizer, and
//! nothing was left to remove it.
//!
//! That is #1919: `[sig-api-machinery] Garbage collector should not be blocked
//! by dependency circle` against a vanilla kube-controller-manager left pod1
//! and pod2 terminating forever and pod3 never deleted at all.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// Create an object that already carries a finalizer, DELETE it so the server
/// stamps a `deletionTimestamp` (grace 0, so no graceful-deletion window is
/// cut short), then drain the finalizer with the merge patch the garbage
/// collector itself sends. Returns whether the object is still in storage.
async fn drain_with_patch(
    api: &TestApiServer,
    collection: &str,
    name: &str,
    create: &Value,
) -> (bool, Value) {
    let (status, body) = api.post(collection, create).await;
    assert!(status.is_success(), "create failed {status}: {body}");

    let (status, body) = api
        .send(
            "DELETE",
            &format!("{collection}/{name}"),
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "gracePeriodSeconds": 0
            })),
        )
        .await;
    assert!(status.is_success(), "delete failed {status}: {body}");

    // The finalizer must have held the delete: this test measures the *drain*,
    // so an object already gone here would make it vacuous.
    let (status, pending) = api.get(&format!("{collection}/{name}")).await;
    assert!(
        status.is_success(),
        "the finalizer did not hold the DELETE ({status}) -- nothing left to \
         drain, so this test would pass for the wrong reason: {pending}"
    );
    assert!(
        pending["metadata"]["deletionTimestamp"].is_string(),
        "{collection}/{name} has no deletionTimestamp after DELETE: {pending}"
    );

    // Exactly what the GC sends: `objectForFinalizersPatch` with the target
    // finalizer removed, as application/merge-patch+json.
    let (status, patched) = api
        .patch(
            &format!("{collection}/{name}"),
            &json!({"metadata": {"finalizers": []}}),
        )
        .await;
    assert!(status.is_success(), "patch failed {status}: {patched}");

    let (status, left) = api.get(&format!("{collection}/{name}")).await;
    (status.is_success(), left)
}

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "finalizers": ["example.com/hold"]},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

/// The resource the GC deletes in #1919, on the hand-rolled pod PATCH handler.
#[tokio::test]
async fn draining_a_pods_last_finalizer_by_patch_finishes_the_deletion() {
    let api = TestApiServer::new();
    let (still_there, left) = drain_with_patch(
        &api,
        "/api/v1/namespaces/default/pods",
        "gc-pod",
        &pod("gc-pod"),
    )
    .await;
    assert!(
        !still_there,
        "the pod survived a PATCH that removed its last finalizer while it was \
         pending deletion. Upstream's ShouldDeleteDuringUpdate \
         (store.go:565) completes the deletion in that request, and the GC \
         removes finalizers with exactly this merge patch \
         (garbagecollector/operations.go:141) -- so nothing else will ever \
         delete it: {left}"
    );
}

/// The generic namespaced PATCH path, which every resource without a
/// hand-rolled handler goes through.
#[tokio::test]
async fn draining_a_namespaced_resources_last_finalizer_by_patch_deletes_it() {
    let api = TestApiServer::new();
    let (still_there, left) = drain_with_patch(
        &api,
        "/api/v1/namespaces/default/configmaps",
        "gc-cm",
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "gc-cm", "finalizers": ["example.com/hold"]},
            "data": {"k": "v"}
        }),
    )
    .await;
    assert!(
        !still_there,
        "a ConfigMap survived a finalizer-draining PATCH -- \
         generic_patch::patch_namespaced_resource does not run the drain check \
         its cluster-scoped sibling does: {left}"
    );
}

/// The cluster-scoped generic path already did this (#1831). Pin it so the
/// refactor that shares one mechanism does not lose it.
#[tokio::test]
async fn draining_a_cluster_scoped_resources_last_finalizer_by_patch_deletes_it() {
    let api = TestApiServer::new();
    let (still_there, left) = drain_with_patch(
        &api,
        "/api/v1/persistentvolumes",
        "gc-pv",
        &json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "gc-pv", "finalizers": ["example.com/hold"]},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/tmp/gc-pv"}
            }
        }),
    )
    .await;
    assert!(
        !still_there,
        "a PersistentVolume survived a finalizer-draining PATCH: {left}"
    );
}

/// A namespace is the one resource whose registry overrides the predicate:
/// `ShouldDeleteNamespaceDuringUpdate`
/// (`pkg/registry/core/namespace/storage/storage.go:258`) is
///
/// ```go
/// return len(ns.Spec.Finalizers) == 0 && genericregistry.ShouldDeleteDuringUpdate(...)
/// ```
///
/// A Terminating namespace normally has an empty `metadata.finalizers` and a
/// `spec.finalizers` of `["kubernetes"]`, which the namespace controller clears
/// through `/finalize` only once the namespace is drained. The PUT path honours
/// that; the PATCH path never consulted `spec.finalizers` at all, so a patch
/// could delete a namespace whose contents were still being removed.
#[tokio::test]
async fn a_patch_must_not_delete_a_namespace_whose_spec_finalizers_remain() {
    let api = TestApiServer::new();

    let (status, body) = api
        .post(
            "/api/v1/namespaces",
            &json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "gc-ns", "finalizers": ["example.com/hold"]},
                "spec": {"finalizers": ["kubernetes"]}
            }),
        )
        .await;
    assert!(status.is_success(), "create failed {status}: {body}");

    let (status, body) = api.delete("/api/v1/namespaces/gc-ns").await;
    assert!(status.is_success(), "delete failed {status}: {body}");

    let (status, pending) = api.get("/api/v1/namespaces/gc-ns").await;
    assert!(status.is_success(), "namespace already gone: {pending}");
    assert_eq!(
        pending["spec"]["finalizers"],
        json!(["kubernetes"]),
        "test premise broken -- spec.finalizers must survive the DELETE: {pending}"
    );

    let (status, patched) = api
        .patch(
            "/api/v1/namespaces/gc-ns",
            &json!({"metadata": {"finalizers": []}}),
        )
        .await;
    assert!(status.is_success(), "patch failed {status}: {patched}");

    let (status, left) = api.get("/api/v1/namespaces/gc-ns").await;
    assert!(
        status.is_success(),
        "the namespace was deleted by a PATCH that drained metadata.finalizers \
         while spec.finalizers still held [\"kubernetes\"]. Upstream's \
         ShouldDeleteNamespaceDuringUpdate (storage.go:258) requires BOTH to \
         be empty, because spec.finalizers is what the namespace controller \
         clears once the contents are actually gone: {left}"
    );
}
