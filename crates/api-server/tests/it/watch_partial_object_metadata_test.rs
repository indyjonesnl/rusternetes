//! #1628: a metadata-only WATCH (`Accept: ...;as=PartialObjectMetadata`, what
//! cert-manager's cainjector uses for its Secret cache) must stream.
//!
//! The Accept-`as=` conversion in `rusternetes-middleware` buffered the whole
//! response with `axum::body::to_bytes`, which on an open-ended watch blocks
//! until the stream ends. The informer therefore saw no event for the life of
//! the watch: an object created after its LIST (the webhook CA Secret created
//! ~200ms after cainjector starts) was never delivered, and cainjector never
//! injected the caBundle (`UnknownIssuer`). When the Secret already existed at
//! LIST time the flake did not show, which is why it tracked start-up timing
//! rather than load.
//!
//! Upstream converts the embedded object of every event
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/response.go`
//! `watchEmbeddedEncoder.doEncode` -> `doTransformObject`); `Status` objects
//! are passed through.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;

/// client-go's metadata client Accept (`metadata.ConfigFor`).
const CLIENT_GO_METADATA_ACCEPT: &str = "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json";

async fn open_watch(api: &TestApiServer, uri: &str, accept: &str) -> axum::response::Response {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("accept", accept)
        .body(Body::empty())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), api.router.clone().oneshot(req))
        .await
        .expect("watch response headers must not wait for the stream to end")
        .unwrap()
}

async fn next_event(
    stream: &mut (impl futures::Stream<Item = Result<bytes::Bytes, axum::Error>> + Unpin),
    buf: &mut String,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(i) = buf.find('\n') {
                let line: String = buf.drain(..=i).collect();
                if let Ok(v) = serde_json::from_str::<Value>(line.trim()) {
                    return v;
                }
                continue;
            }
            let chunk = stream
                .next()
                .await
                .expect("watch stream ended")
                .expect("watch stream error");
            buf.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("no watch event within 5s: the as=PartialObjectMetadata watch is being buffered")
}

fn secret(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {
            "name": name, "namespace": "cert-manager",
            "labels": {"app.kubernetes.io/managed-by": "cert-manager"},
        },
        "data": {"tls.crt": "YQ=="},
    })
}

#[tokio::test]
async fn metadata_only_watch_streams_converted_events() {
    let api = TestApiServer::new();
    let (_, list) = api
        .get("/api/v1/secrets?labelSelector=app.kubernetes.io%2Fmanaged-by%3Dcert-manager")
        .await;
    let rv = list["metadata"]["resourceVersion"].as_str().unwrap();

    let resp = open_watch(
        &api,
        &format!("/api/v1/secrets?watch=true&allowWatchBookmarks=true&resourceVersion={rv}"),
        CLIENT_GO_METADATA_ACCEPT,
    )
    .await;
    assert_eq!(resp.status(), 200);
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();

    // The Secret is created AFTER the watch opened, like the webhook CA.
    let (status, _) = api
        .post(
            "/api/v1/namespaces/cert-manager/secrets",
            &secret("cert-manager-webhook-ca"),
        )
        .await;
    assert!(status.is_success());

    loop {
        let ev = next_event(&mut stream, &mut buf).await;
        if ev["type"] == "BOOKMARK" {
            assert_eq!(ev["object"]["kind"], "PartialObjectMetadata");
            continue;
        }
        assert_eq!(ev["type"], "ADDED");
        assert_eq!(ev["object"]["kind"], "PartialObjectMetadata");
        assert_eq!(ev["object"]["apiVersion"], "meta.k8s.io/v1");
        assert_eq!(ev["object"]["metadata"]["name"], "cert-manager-webhook-ca");
        assert_eq!(
            ev["object"]["metadata"]["labels"]["app.kubernetes.io/managed-by"],
            "cert-manager"
        );
        assert!(
            ev["object"].get("data").is_none(),
            "metadata-only watch must not carry the Secret payload: {ev}"
        );
        break;
    }
}

#[tokio::test]
async fn plain_json_watch_keeps_full_objects() {
    let api = TestApiServer::new();
    let (_, list) = api.get("/api/v1/secrets").await;
    let rv = list["metadata"]["resourceVersion"].as_str().unwrap();
    let resp = open_watch(
        &api,
        &format!("/api/v1/secrets?watch=true&resourceVersion={rv}"),
        "application/json",
    )
    .await;
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let (status, _) = api
        .post("/api/v1/namespaces/cert-manager/secrets", &secret("plain"))
        .await;
    assert!(status.is_success());
    let ev = next_event(&mut stream, &mut buf).await;
    assert_eq!(ev["object"]["kind"], "Secret");
    assert!(ev["object"].get("data").is_some());
}
