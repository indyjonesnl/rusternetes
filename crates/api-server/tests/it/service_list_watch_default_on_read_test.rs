//! Service list and watch must run the Store's `Decorator`
//! (`defaultOnReadService`, pkg/registry/core/service/storage/storage.go:255-274)
//! exactly as GET does. Upstream decorates ServiceList items in
//! `ServiceStorage.List`/`Watch` (storage.go:243-252, :276-300 via
//! `registry/generic/registry.Store.DecorateList`), so a Service stored before
//! `ipFamilies` existed reads with them inferred on every verb.

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_storage::Storage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;

async fn seed_legacy(api: &TestApiServer) {
    // Written around the API: no ipFamilies/ipFamilyPolicy/clusterIPs.
    let legacy = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "legacy", "namespace": "default", "uid": "u-1"},
        "spec": {"type": "ClusterIP", "clusterIP": "10.96.0.77",
                 "ports": [{"port": 80, "protocol": "TCP"}]}
    });
    api.storage
        .create("/registry/services/default/legacy", &legacy)
        .await
        .unwrap();
}

fn assert_defaulted(svc: &Value, what: &str) {
    assert_eq!(
        svc.pointer("/spec/ipFamilies"),
        Some(&json!(["IPv4"])),
        "{what}: ipFamilies not defaulted on read: {svc}"
    );
    assert_eq!(
        svc.pointer("/spec/ipFamilyPolicy"),
        Some(&json!("SingleStack")),
        "{what}: ipFamilyPolicy not defaulted on read: {svc}"
    );
    assert_eq!(
        svc.pointer("/spec/clusterIPs"),
        Some(&json!(["10.96.0.77"])),
        "{what}: clusterIPs not normalized on read: {svc}"
    );
}

async fn first_event(api: TestApiServer, uri: &str) -> Value {
    let resp = api.respond("GET", uri, None, None).await;
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let run = async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            if let Some(i) = buf.find('\n') {
                return serde_json::from_str::<Value>(&buf[..i]).unwrap();
            }
        }
        panic!("watch ended without an event");
    };
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("no watch event within 5s")
}

#[tokio::test]
async fn get_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let (st, body) = api.get("/api/v1/namespaces/default/services/legacy").await;
    assert_eq!(st, StatusCode::OK);
    assert_defaulted(&body, "GET");
}

#[tokio::test]
async fn namespaced_list_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let (st, body) = api.get("/api/v1/namespaces/default/services").await;
    assert_eq!(st, StatusCode::OK);
    assert_defaulted(&body["items"][0], "namespaced LIST");
}

#[tokio::test]
async fn all_namespaces_list_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let (st, body) = api.get("/api/v1/services").await;
    assert_eq!(st, StatusCode::OK);
    assert_defaulted(&body["items"][0], "cluster-wide LIST");
}

#[tokio::test]
async fn namespaced_watch_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let ev = first_event(
        api,
        "/api/v1/namespaces/default/services?watch=true&resourceVersion=0",
    )
    .await;
    assert_eq!(ev["type"], "ADDED");
    assert_defaulted(&ev["object"], "namespaced WATCH");
}

#[tokio::test]
async fn all_namespaces_watch_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let ev = first_event(api, "/api/v1/services?watch=true&resourceVersion=0").await;
    assert_eq!(ev["type"], "ADDED");
    assert_defaulted(&ev["object"], "cluster-wide WATCH");
}

// Legacy `/api/v1/watch/...` prefix routes (upstream installer.go registers a
// `watch/` variant for the namespaced and all-namespaces scopes) must
// decorate exactly like `?watch=true` (#2192).
#[tokio::test]
async fn legacy_namespaced_watch_route_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let ev = first_event(
        api,
        "/api/v1/watch/namespaces/default/services?resourceVersion=0",
    )
    .await;
    assert_eq!(ev["type"], "ADDED");
    assert_defaulted(&ev["object"], "legacy namespaced WATCH route");
}

#[tokio::test]
async fn legacy_all_namespaces_watch_route_defaults_on_read() {
    let api = TestApiServer::new();
    seed_legacy(&api).await;
    let ev = first_event(api, "/api/v1/watch/services?resourceVersion=0").await;
    assert_eq!(ev["type"], "ADDED");
    assert_defaulted(&ev["object"], "legacy all-namespaces WATCH route");
}
