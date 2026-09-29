//! #1824: a watch opened at a LIST's resourceVersion must not replay the
//! CREATE that list already contained.
//!
//! The spec is `[sig-api-machinery] API priority and fairness should support
//! FlowSchema API operations [Conformance]` (`test/e2e/apimachinery/
//! flowcontrol.go:395-433`): CREATE, LIST with a label selector, `Watch` at
//! `list.ResourceVersion`, PATCH, UPDATE, and every event must be `MODIFIED`:
//!
//! ```go
//! fsWatch, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion, LabelSelector: label})
//! ...
//! gomega.Expect(evt.Type).To(gomega.Equal(watch.Modified))
//! ```
//!
//! It failed intermittently with `ADDED`. The prefix's shared watcher is warm
//! (the APF controller watches flowschemas), but it ingests writes
//! asynchronously; when it had not yet taken in the CREATE, the ring replay
//! was empty and the CREATE then arrived on the live broadcast. The live
//! filter started from the highest *replayed* revision — 0 — so the CREATE
//! went out as `ADDED` although the client asked for events after it.
//! Upstream's `cacheWatcher.process` starts that filter at the requested
//! revision (`event.ResourceVersion > resourceVersion`,
//! `staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go`).

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const FLOWSCHEMAS: &str = "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas";
const LABEL: &str = "e2e-fs-label=1824";

fn flowschema(name: &str) -> Value {
    json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": { "name": name, "labels": { "e2e-fs-label": "1824" } },
        "spec": {
            "priorityLevelConfiguration": { "name": "exempt" },
            "matchingPrecedence": 1000,
            "rules": [{
                "subjects": [{ "kind": "User", "user": { "name": "e2e" } }],
                "nonResourceRules": [{ "verbs": ["get"], "nonResourceURLs": ["/healthz"] }]
            }]
        }
    })
}

/// Read events until `n` have arrived or `window` passes.
async fn first_events(stream_resp: axum::response::Response, n: usize) -> Vec<Value> {
    let mut stream = stream_resp.into_body().into_data_stream();
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
                    if events.len() >= n {
                        return;
                    }
                }
            }
        }
    };
    let _ = timeout(Duration::from_secs(2), run).await;
    events
}

/// A current-thread runtime makes the race deterministic: the shared watcher's
/// ingest task cannot run until the test yields, so the CREATE is still in
/// flight to the ring when the watch opens — the conformance timeline, where
/// the watch lands inside the ingest lag.
#[tokio::test]
async fn a_watch_at_the_list_revision_sees_only_modifications() {
    const ROUNDS: usize = 20;
    for round in 0..ROUNDS {
        let api = TestApiServer::new();
        // The APF controller's long-lived watch: the prefix's shared watcher
        // is established before anything is written.
        let _warm = api
            .respond("GET", &format!("{FLOWSCHEMAS}?watch=true"), None, None)
            .await;

        let name = format!("fs-{round}");
        let (status, body) = api.post(FLOWSCHEMAS, &flowschema(&name)).await;
        assert!(status.is_success(), "create: {status} {body}");

        let (status, list) = api
            .get(&format!("{FLOWSCHEMAS}?labelSelector={LABEL}"))
            .await;
        assert!(status.is_success(), "list: {status} {list}");
        let list_rv = list["metadata"]["resourceVersion"]
            .as_str()
            .expect("list resourceVersion")
            .to_string();

        let watch = api
            .respond(
                "GET",
                &format!(
                    "{FLOWSCHEMAS}?watch=true&resourceVersion={list_rv}&labelSelector={LABEL}"
                ),
                None,
                None,
            )
            .await;

        let (status, body) = api
            .patch(
                &format!("{FLOWSCHEMAS}/{name}"),
                &json!({
                    "metadata": { "annotations": { "patched": "true" } },
                    "spec": { "matchingPrecedence": 9999 }
                }),
            )
            .await;
        assert!(status.is_success(), "patch: {status} {body}");

        let events = first_events(watch, 1).await;
        let first = events.first().unwrap_or_else(|| {
            panic!("round {round}: no event after the PATCH (list rv {list_rv})")
        });
        assert_eq!(
            first["type"], "MODIFIED",
            "round {round}: watch at list rv {list_rv} replayed the CREATE: {first}"
        );
        assert_eq!(
            first["object"]["metadata"]["annotations"]["patched"], "true",
            "round {round}: first event is not the PATCH: {first}"
        );
    }
}
