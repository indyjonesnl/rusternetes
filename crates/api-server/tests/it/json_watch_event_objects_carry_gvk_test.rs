//! A JSON-valued watch path (DRA, CRD, APIService) stamps `kind` and
//! `apiVersion` on every event object (#2772).
//!
//! Upstream's watch encoder is `EncoderForVersion(..., scope.Kind.GroupVersion())`
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:81-83`), whose
//! versioning codec does `objectKind.SetGroupVersionKind(gvk)` in
//! `apimachinery/pkg/runtime/serializer/versioning/versioning.go` `doEncode`.
//! A typed or dynamic client's write body carries no TypeMeta, so the stored
//! object has none; client-go cannot decode such an event and `RetryWatcher`
//! retries silently.

use futures::StreamExt;
use rusternetes_api_server::handlers::watch::build_delete_fallback_json;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

fn claim() -> Value {
    // No kind/apiVersion: what a typed or dynamic client's body carries.
    json!({
        "metadata": { "name": "c", "namespace": "default" },
        "spec": { "devices": { "requests": [
            { "name": "r", "exactly": { "deviceClassName": "dc" } }
        ] } }
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
async fn a_replayed_json_watch_event_names_its_kind_after_a_typemeta_less_write() {
    let api = TestApiServer::new();
    let _warm = api
        .respond("GET", &format!("{CLAIMS}?watch=true"), None, None)
        .await;

    let (status, created) = api.post(CLAIMS, &claim()).await;
    assert!(status.is_success(), "create: {status} {created}");
    let rv = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    let mut body = claim();
    body["metadata"]["labels"] = json!({ "x": "y" });
    body["metadata"]["resourceVersion"] = json!(rv);
    let (status, updated) = api.put(&format!("{CLAIMS}/c"), &body).await;
    assert!(status.is_success(), "update: {status} {updated}");

    let events = collect(&api, format!("{CLAIMS}?watch=true&resourceVersion={rv}")).await;
    let modified: Vec<&Value> = events.iter().filter(|e| e["type"] == "MODIFIED").collect();
    assert!(!modified.is_empty(), "no MODIFIED replayed: {events:?}");
    for event in modified {
        assert_eq!(event["object"]["kind"], "ResourceClaim", "{event}");
        assert_eq!(
            event["object"]["apiVersion"], "resource.k8s.io/v1",
            "{event}"
        );
    }
}

#[tokio::test]
async fn the_initial_added_event_names_its_kind() {
    let api = TestApiServer::new();
    let (status, created) = api.post(CLAIMS, &claim()).await;
    assert!(status.is_success(), "create: {status} {created}");

    let events = collect(&api, format!("{CLAIMS}?watch=true")).await;
    let added: Vec<&Value> = events.iter().filter(|e| e["type"] == "ADDED").collect();
    assert!(!added.is_empty(), "no ADDED: {events:?}");
    for event in added {
        assert_eq!(event["object"]["kind"], "ResourceClaim", "{event}");
        assert_eq!(
            event["object"]["apiVersion"], "resource.k8s.io/v1",
            "{event}"
        );
    }
}

#[test]
fn the_deleted_fallback_envelope_names_its_kind() {
    let prev = json!({ "metadata": { "name": "c", "namespace": "default" } }).to_string();
    let line = build_delete_fallback_json(
        "/registry/resourceclaims/default/c",
        &prev,
        Some(("ResourceClaim", "resource.k8s.io/v1")),
    )
    .unwrap();
    let event: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["type"], "DELETED");
    assert_eq!(event["object"]["kind"], "ResourceClaim", "{event}");
    assert_eq!(
        event["object"]["apiVersion"], "resource.k8s.io/v1",
        "{event}"
    );
}
