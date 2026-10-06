//! #2383: a watch with `fieldSelector=metadata.name=<pod>` from the pod's
//! create resourceVersion must replay from the ring, not 410, even when an
//! earlier RV-less watch of the same namespace already created the shared
//! watcher for `/registry/pods/<ns>/`.
//!
//! Upstream: `watchCache.getAllEventsSinceLocked`
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:875-929`)
//! expires only `resourceVersion < oldest-1` where `oldest = listRV+1`.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const PODS: &str = "/api/v1/namespaces/default/pods";

async fn collect_for(api: &TestApiServer, uri: String, window: Duration) -> Vec<Value> {
    let resp = api.respond("GET", &uri, None, None).await;
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut events = Vec::new();
    let run = async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    events.push(v);
                }
            }
        }
    };
    let _ = timeout(window, run).await;
    events
}

#[tokio::test]
async fn name_selected_pod_watch_from_create_rv_after_rvless_watch_is_served() {
    let api = TestApiServer::new();
    // Earlier RV-less watch of the namespace: creates the shared watcher.
    let _warm = api
        .respond("GET", &format!("{PODS}?watch=true"), None, None)
        .await;
    let (status, created) = api
        .post(
            PODS,
            &json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": { "name": "pod-init", "namespace": "default" },
                "spec": { "restartPolicy": "Never",
                  "containers": [{ "name": "c", "image": "busybox" }] },
            }),
        )
        .await;
    assert!(status.is_success(), "create: {status} {created}");
    let rv = created["metadata"]["resourceVersion"].as_str().unwrap();

    let events = collect_for(
        &api,
        format!("{PODS}?watch=true&resourceVersion={rv}&fieldSelector=metadata.name%3Dpod-init"),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        !events.iter().any(|e| e["type"] == "ERROR"),
        "watch got an in-stream ERROR: {events:?}"
    );
}

/// Divergence probe: the RV-less watch starts AFTER the pod was created.
/// Upstream's ring is valid from the cache's list RV, so a watch from the
/// create RV is served; a floor at "head when the shared watch started"
/// would 410 it.
#[tokio::test]
async fn watch_from_create_rv_when_rvless_watch_started_later_is_served() {
    let api = TestApiServer::new();
    let (status, created) = api
        .post(
            PODS,
            &json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": { "name": "pod-init", "namespace": "default" },
                "spec": { "restartPolicy": "Never",
                  "containers": [{ "name": "c", "image": "busybox" }] },
            }),
        )
        .await;
    assert!(status.is_success(), "create: {status} {created}");
    let rv = created["metadata"]["resourceVersion"].as_str().unwrap();
    // A later write (scheduler bind / kubelet status) moves the head past rv.
    let (status, patched) = api
        .patch(
            &format!("{PODS}/pod-init"),
            &json!({ "metadata": { "labels": { "step": "later" } } }),
        )
        .await;
    assert!(status.is_success(), "patch: {status} {patched}");
    let _warm = api
        .respond("GET", &format!("{PODS}?watch=true"), None, None)
        .await;
    let events = collect_for(
        &api,
        format!("{PODS}?watch=true&resourceVersion={rv}&fieldSelector=metadata.name%3Dpod-init"),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        !events.iter().any(|e| e["type"] == "ERROR"),
        "watch got an in-stream ERROR: {events:?}"
    );
}
