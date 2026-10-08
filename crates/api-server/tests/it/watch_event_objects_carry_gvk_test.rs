//! A watch event's object always names its kind and apiVersion.
//!
//! Conformance `[sig-apps] Deployment should run the lifecycle of a
//! Deployment` (run 37736208599) timed out at `deployment.go:372`
//! ("failed to see MODIFIED event"). The spec updates the Deployment through
//! the dynamic client with an object that carries no TypeMeta, then watches
//! from the previous list's resourceVersion. The replayed MODIFIED event's
//! object had empty `kind`/`apiVersion`; client-go cannot decode such an
//! event, reports a 500 ERROR which `RetryWatcher` retries silently every
//! second, so the spec saw nothing for 30s.
//!
//! Upstream's watch encoder stamps the GVK on every event object
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:81-83`,
//! `EncoderForVersion(..., scope.Kind.GroupVersion())`, whose versioning codec
//! does `objectKind.SetGroupVersionKind(gvk)` in
//! `apimachinery/pkg/runtime/serializer/versioning/versioning.go`).

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const DEPLOYMENTS: &str = "/apis/apps/v1/namespaces/default/deployments";

fn deployment(replicas: i64, image: &str) -> Value {
    // No kind/apiVersion: what a typed or dynamic client's body carries.
    json!({
        "metadata": { "name": "d", "namespace": "default", "labels": { "a": "b" } },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "a": "b" } },
            "template": {
                "metadata": { "labels": { "a": "b" } },
                "spec": { "containers": [ { "name": "c", "image": image } ] }
            }
        }
    })
}

async fn collect(api: &TestApiServer, uri: String) -> Vec<Value> {
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
    let _ = timeout(Duration::from_millis(500), run).await;
    events
}

#[tokio::test]
async fn a_replayed_modified_event_names_its_kind_after_a_typemeta_less_update() {
    let api = TestApiServer::new();
    let _warm = api
        .respond("GET", &format!("{DEPLOYMENTS}?watch=true"), None, None)
        .await;

    let (status, created) = api.post(DEPLOYMENTS, &deployment(1, "img:1")).await;
    assert!(status.is_success(), "create: {status} {created}");
    let created_rv = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, updated) = api
        .put(&format!("{DEPLOYMENTS}/d"), &deployment(2, "img:2"))
        .await;
    assert!(status.is_success(), "update: {status} {updated}");

    let events = collect(
        &api,
        format!("{DEPLOYMENTS}?watch=true&resourceVersion={created_rv}"),
    )
    .await;

    let modified: Vec<&Value> = events.iter().filter(|e| e["type"] == "MODIFIED").collect();
    assert!(!modified.is_empty(), "no MODIFIED replayed: {events:?}");
    for event in modified {
        assert_eq!(event["object"]["kind"], "Deployment", "{event}");
        assert_eq!(event["object"]["apiVersion"], "apps/v1", "{event}");
    }
}
