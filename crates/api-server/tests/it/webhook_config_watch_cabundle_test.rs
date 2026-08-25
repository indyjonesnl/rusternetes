//! Regression for #1566: cert-manager's cainjector hot-loops (~20 updates/sec)
//! against a ValidatingWebhookConfiguration because rusternetes emits a MODIFIED
//! watch event (and bumps resourceVersion) for a *no-op* update — one whose
//! object is byte-identical (modulo resourceVersion) to what is already stored.
//!
//! cainjector's reconcile is idempotent: every trigger it re-sets the same
//! caBundle and calls Update. Upstream Kubernetes short-circuits a no-op update
//! in the storage layer (etcd3 store.go: `if !origState.stale &&
//! bytes.Equal(data, origState.data)`) — no write, no resourceVersion bump, no
//! watch event — so cainjector's own idempotent Update does not re-trigger it.
//! rusternetes wrote+published unconditionally, so each idempotent Update
//! produced a new MODIFIED event → re-trigger → infinite loop.
//!
//! This test drives the exact shape: one real change (add caBundle) must emit
//! exactly one MODIFIED; a second identical Update must emit none.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const GROUP: &str = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations";
const NAME: &str = "cabundle-noop-test";
const CA: &str = "dGVzdC1jYS1idW5kbGUtdmFsdWU="; // base64("test-ca-bundle-value")

fn config(ca_bundle: Option<&str>) -> Value {
    let mut client_config = json!({
        "service": { "namespace": "default", "name": "svc", "path": "/validate" }
    });
    if let Some(ca) = ca_bundle {
        client_config["caBundle"] = json!(ca);
    }
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": { "name": NAME },
        "webhooks": [{
            "name": "webhook.example.com",
            "clientConfig": client_config,
            "rules": [{
                "apiGroups": ["cert-manager.io"],
                "apiVersions": ["v1"],
                "operations": ["CREATE", "UPDATE"],
                "resources": ["*/*"]
            }],
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"]
        }]
    })
}

fn ca_bundle_of(obj: &Value) -> Option<String> {
    obj.pointer("/webhooks/0/clientConfig/caBundle")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

#[tokio::test]
async fn noop_update_emits_no_modified_event() {
    let api = TestApiServer::new();

    // Create WITHOUT a caBundle.
    let (status, _created) = api.post(GROUP, &config(None)).await;
    assert!(status.is_success(), "create failed: {status}");

    // Open a collection WATCH; collect every MODIFIED for our object.
    let resp = api
        .respond("GET", &format!("{GROUP}?watch=true"), None, None)
        .await;
    assert_eq!(resp.status(), 200, "watch dispatch failed");
    let mut stream = resp.into_body().into_data_stream();

    let modifieds: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = modifieds.clone();
    let collector = tokio::spawn(async move {
        let mut buf = String::new();
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if frame.get("type").and_then(|t| t.as_str()) == Some("MODIFIED") {
                    if let Some(obj) = frame.get("object") {
                        if obj.pointer("/metadata/name").and_then(|n| n.as_str()) == Some(NAME) {
                            sink.lock().await.push(ca_bundle_of(obj));
                        }
                    }
                }
            }
        }
    });

    // Let the watch subscribe.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Update #1: add the caBundle (a REAL change) → exactly one MODIFIED.
    let (s1, updated1) = api.put(&format!("{GROUP}/{NAME}"), &config(Some(CA))).await;
    assert!(s1.is_success(), "update #1 failed: {s1} {updated1:#}");
    assert_eq!(
        ca_bundle_of(&updated1).as_deref(),
        Some(CA),
        "update #1 response dropped caBundle"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Update #2: PUT the exact object the server just returned — a NO-OP, like
    // cainjector re-setting the same caBundle every reconcile. Must emit NO
    // MODIFIED (and not bump resourceVersion).
    let (s2, updated2) = api.put(&format!("{GROUP}/{NAME}"), &updated1).await;
    assert!(s2.is_success(), "update #2 failed: {s2} {updated2:#}");
    assert_eq!(
        updated2.pointer("/metadata/resourceVersion"),
        updated1.pointer("/metadata/resourceVersion"),
        "no-op update #2 bumped resourceVersion (drives the cainjector hot-loop, #1566)"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    collector.abort();

    let events = modifieds.lock().await.clone();
    assert_eq!(
        events.len(),
        1,
        "expected exactly ONE MODIFIED event (the real caBundle change); got {}: {:?} \
         — a no-op update emitting a MODIFIED is what makes cainjector hot-loop (#1566)",
        events.len(),
        events
    );
    assert_eq!(
        events[0].as_deref(),
        Some(CA),
        "the MODIFIED event must carry the caBundle"
    );
}
