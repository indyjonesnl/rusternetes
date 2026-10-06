//! #2232: the `/apis/autoscaling/v1/watch/...` route streams v1-shaped HPAs.
//!
//! Upstream serves every watch through `serveWatchHandler`, which encodes each
//! event for the requested version via `scope.Serializer.EncoderForVersion`
//! (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:123-144), so
//! `ADDED`, `MODIFIED`, `DELETED` and `BOOKMARK` are all `autoscaling/v1`.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const V1: &str = "/apis/autoscaling/v1/namespaces/default/horizontalpodautoscalers";
const V1_WATCH: &str = "/apis/autoscaling/v1/watch/namespaces/default/horizontalpodautoscalers";
const V2_WATCH: &str = "/apis/autoscaling/v2/watch/namespaces/default/horizontalpodautoscalers";

async fn collect(api: &TestApiServer, uri: String, window: Duration) -> Vec<Value> {
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

fn hpa() -> Value {
    json!({"apiVersion": "autoscaling/v1", "kind": "HorizontalPodAutoscaler",
           "metadata": {"name": "h1"},
           "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                    "maxReplicas": 5, "targetCPUUtilizationPercentage": 60}})
}

#[tokio::test]
async fn the_v1_watch_route_streams_v1_objects() {
    let api = TestApiServer::new();
    let (s, b) = api.post(V1, &hpa()).await;
    assert!(s.is_success(), "{b}");
    let events = collect(
        &api,
        format!("{V1_WATCH}?resourceVersion=0"),
        Duration::from_millis(700),
    )
    .await;
    let added = events
        .iter()
        .find(|e| e["type"] == "ADDED")
        .unwrap_or_else(|| panic!("no ADDED in {events:?}"));
    let o = &added["object"];
    assert_eq!(o["apiVersion"], "autoscaling/v1", "{added}");
    assert_eq!(o["spec"]["targetCPUUtilizationPercentage"], 60, "{added}");
    assert!(o["spec"].get("metrics").is_none(), "{added}");
}

/// `?watch=true` on the v1 list endpoint shares the mechanism.
#[tokio::test]
async fn a_v1_list_watch_is_v1_shaped() {
    let api = TestApiServer::new();
    api.post(V1, &hpa()).await;
    let events = collect(
        &api,
        format!("{V1}?watch=true&resourceVersion=0"),
        Duration::from_millis(700),
    )
    .await;
    let o = &events
        .iter()
        .find(|e| e["type"] == "ADDED")
        .unwrap_or_else(|| panic!("no ADDED in {events:?}"))["object"];
    assert_eq!(o["apiVersion"], "autoscaling/v1", "{o}");
    assert_eq!(o["spec"]["targetCPUUtilizationPercentage"], 60, "{o}");
}

#[tokio::test]
async fn the_v2_watch_route_still_streams_v2_objects() {
    let api = TestApiServer::new();
    let (s, b) = api.post(V1, &hpa()).await;
    assert!(s.is_success(), "{b}");
    let events = collect(
        &api,
        format!("{V2_WATCH}?resourceVersion=0"),
        Duration::from_millis(700),
    )
    .await;
    let added = events
        .iter()
        .find(|e| e["type"] == "ADDED")
        .unwrap_or_else(|| panic!("no ADDED in {events:?}"));
    assert_eq!(added["object"]["apiVersion"], "autoscaling/v2", "{added}");
    assert!(added["object"]["spec"]["metrics"].is_array(), "{added}");
}
