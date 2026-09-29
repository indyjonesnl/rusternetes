//! #2044: the JSON watch paths honour `labelSelector` and `fieldSelector`.
//!
//! `watch_namespaced_json` (resourceclaims, resourceclaimtemplates) and
//! `watch_cluster_scoped_json` (CRDs, deviceclasses, resourceslices, the
//! generic cluster-scoped handler) streamed every object under the prefix,
//! whatever the client asked for. Upstream filters every watch through the
//! cacher's predicate, `filterWithAttrsAndPrefixFunction`
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go:1201-1209`),
//! and `cacheWatcher.convertToWatchEvent` (`cache_watcher.go:374-395`) turns a
//! change that moves an object into the selector into `ADDED` and out of it
//! into `DELETED`.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const SLICES: &str = "/apis/resource.k8s.io/v1/resourceslices";
const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

/// Every event on `resp` within `window`, as `(type, name)`.
async fn events(resp: axum::response::Response, window: Duration) -> Vec<(String, String)> {
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut out = Vec::new();
    let run = async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    out.push((
                        v["type"].as_str().unwrap_or_default().to_string(),
                        v["object"]["metadata"]["name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    ));
                }
            }
        }
    };
    let _ = timeout(window, run).await;
    out
}

fn slice(name: &str, node: &str) -> Value {
    json!({
        "apiVersion": "resource.k8s.io/v1",
        "kind": "ResourceSlice",
        "metadata": { "name": name },
        "spec": {
            "driver": "dra.example.com",
            "pool": { "name": name, "generation": 1, "resourceSliceCount": 1 },
            "nodeName": node,
            "devices": [{ "name": "gpu-0" }]
        }
    })
}

fn claim(name: &str, labels: Value) -> Value {
    json!({
        "apiVersion": "resource.k8s.io/v1",
        "kind": "ResourceClaim",
        "metadata": { "name": name, "namespace": "default", "labels": labels },
        "spec": { "devices": { "requests": [
            { "name": "req", "exactly": { "deviceClassName": "class" } }
        ] } },
    })
}

fn added(names: &[&str]) -> Vec<(String, String)> {
    names
        .iter()
        .map(|n| ("ADDED".to_string(), n.to_string()))
        .collect()
}

/// The kubelet's DRA plugin and the scheduler watch their own node's slices;
/// another node's slice must not reach them — neither in the initial state
/// nor live.
#[tokio::test]
async fn a_cluster_scoped_json_watch_honours_the_field_selector() {
    let api = TestApiServer::new();
    for (name, node) in [("a-before", "node-a"), ("b-before", "node-b")] {
        let (status, body) = api.post(SLICES, &slice(name, node)).await;
        assert!(status.is_success(), "create {name}: {status} {body}");
    }

    let watch = api
        .respond(
            "GET",
            &format!("{SLICES}?watch=true&fieldSelector=spec.nodeName%3Dnode-a"),
            None,
            None,
        )
        .await;
    for (name, node) in [("b-after", "node-b"), ("a-after", "node-a")] {
        let (status, body) = api.post(SLICES, &slice(name, node)).await;
        assert!(status.is_success(), "create {name}: {status} {body}");
    }

    assert_eq!(
        events(watch, Duration::from_millis(500)).await,
        added(&["a-before", "a-after"]),
    );
}

/// A namespaced JSON watch filters by label, and a change that moves an
/// object out of the selector and back surfaces as `DELETED` then `ADDED`.
#[tokio::test]
async fn a_namespaced_json_watch_honours_the_label_selector_and_its_transitions() {
    let api = TestApiServer::new();
    let (status, _) = api.post(CLAIMS, &claim("plain", json!({}))).await;
    assert!(status.is_success());
    let (status, _) = api
        .post(CLAIMS, &claim("picked", json!({ "app": "x" })))
        .await;
    assert!(status.is_success());

    let watch = api
        .respond(
            "GET",
            &format!("{CLAIMS}?watch=true&labelSelector=app%3Dx"),
            None,
            None,
        )
        .await;

    let relabel = |app: Value| json!({ "metadata": { "labels": { "app": app } } });
    for (name, app) in [
        ("plain", json!("y")),  // still outside: nothing
        ("picked", json!("y")), // moves out: DELETED
        ("picked", json!("x")), // moves back in: ADDED
    ] {
        let (status, body) = api.patch(&format!("{CLAIMS}/{name}"), &relabel(app)).await;
        assert!(status.is_success(), "patch {name}: {status} {body}");
    }

    assert_eq!(
        events(watch, Duration::from_millis(500)).await,
        vec![
            ("ADDED".to_string(), "picked".to_string()),
            ("DELETED".to_string(), "picked".to_string()),
            ("ADDED".to_string(), "picked".to_string()),
        ],
    );
}
