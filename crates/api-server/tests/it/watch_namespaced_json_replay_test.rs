//! #2037: `watch_namespaced_json` replays from a resourceVersion.
//!
//! This path serves the DRA `resourceclaims` and `resourceclaimtemplates`
//! watches. It used to subscribe "from now" whatever `resourceVersion` the
//! client sent, and papered over the lost history with an unconditional store
//! snapshot, so a client watching from its CREATE's resourceVersion got the
//! post-update object as a synthetic `ADDED` instead of the real `MODIFIED`.
//!
//! Upstream serves every watch from one place,
//! `watchCache.getAllEventsSinceLocked`
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:878-913`):
//! a specific resourceVersion is answered from the event ring, and a store
//! snapshot is sent only when `sendInitialEvents` or `resourceVersion=0`/unset
//! ask for one.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

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

/// Create a claim, then change a label on it. Returns the resourceVersion the
/// CREATE returned and the one the PATCH returned.
async fn create_then_modify(api: &TestApiServer) -> (String, String) {
    let (status, created) = api
        .post(
            CLAIMS,
            &json!({
                "apiVersion": "resource.k8s.io/v1",
                "kind": "ResourceClaim",
                "metadata": { "name": "claim", "namespace": "default" },
                "spec": { "devices": { "requests": [
                    { "name": "req", "exactly": { "deviceClassName": "class" } }
                ] } },
            }),
        )
        .await;
    assert!(status.is_success(), "create: {status} {created}");
    let (status, patched) = api
        .patch(
            &format!("{CLAIMS}/claim"),
            &json!({ "metadata": { "labels": { "step": "modified" } } }),
        )
        .await;
    assert!(status.is_success(), "patch: {status} {patched}");
    let rv = |v: &Value| {
        v["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string()
    };
    (rv(&created), rv(&patched))
}

fn summary(events: &[Value]) -> Vec<(String, Option<String>)> {
    events
        .iter()
        .map(|e| {
            (
                e["type"].as_str().unwrap_or_default().to_string(),
                e["object"]["metadata"]["labels"]["step"]
                    .as_str()
                    .map(str::to_string),
            )
        })
        .collect()
}

/// The issue's regression: watching from the CREATE's resourceVersion must
/// deliver the update as the `MODIFIED` it was, not re-`ADD` the object.
#[tokio::test]
async fn a_watch_from_the_create_revision_replays_the_modification() {
    let api = TestApiServer::new();
    let (created_rv, _) = create_then_modify(&api).await;

    let events = collect_for(
        &api,
        format!("{CLAIMS}?watch=true&resourceVersion={created_rv}"),
        Duration::from_millis(500),
    )
    .await;

    assert_eq!(
        summary(&events),
        vec![("MODIFIED".to_string(), Some("modified".to_string()))],
        "expected exactly the replayed MODIFIED: {events:?}"
    );
}

/// Watching from the latest resourceVersion has nothing to replay, and a
/// specific resourceVersion never asks for a snapshot.
#[tokio::test]
async fn a_watch_from_the_latest_revision_sends_nothing() {
    let api = TestApiServer::new();
    let (_, patched_rv) = create_then_modify(&api).await;

    let events = collect_for(
        &api,
        format!("{CLAIMS}?watch=true&resourceVersion={patched_rv}"),
        Duration::from_millis(500),
    )
    .await;

    assert!(
        events.is_empty(),
        "nothing happened after {patched_rv}: {events:?}"
    );
}

/// `resourceVersion` unset still opens with the current state (upstream's
/// backward-compatibility branch), unchanged by the replay.
#[tokio::test]
async fn a_watch_without_a_revision_still_opens_with_current_state() {
    let api = TestApiServer::new();
    create_then_modify(&api).await;

    let events = collect_for(
        &api,
        format!("{CLAIMS}?watch=true"),
        Duration::from_millis(500),
    )
    .await;

    assert_eq!(
        summary(&events),
        vec![("ADDED".to_string(), Some("modified".to_string()))],
        "{events:?}"
    );
}
