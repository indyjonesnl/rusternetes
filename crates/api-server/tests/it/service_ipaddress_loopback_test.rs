//! The ClusterIP allocator writes its IPAddress objects through the
//! IPAddress REST storage, so admission sees them (#2326).
//!
//! Upstream's allocator never touches etcd: `createIPAddress` calls
//! `a.client.IPAddresses().Create(...)` (pkg/registry/core/service/
//! ipallocator/ipallocator.go:155) and `release` calls `.Delete(...)` (:382),
//! on the loopback client built at pkg/registry/core/rest/storage_core.go:358
//! and :430. A request on that client runs the whole handler chain, mutating
//! and validating admission included, so a webhook registered for
//! `ipaddresses` sees every allocation and release a Service makes.

use axum::http::StatusCode;
use rusternetes_common::admission::{AdmissionReview, AdmissionReviewResponse};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::Filter;

const SVCS: &str = "/api/v1/namespaces/default/services";

/// A validating webhook that records `"<OPERATION> <resource>"` for every
/// review and denies when `deny` is set.
async fn start_webhook(deny: bool, seen: Arc<Mutex<Vec<String>>>) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |review: AdmissionReview| {
            let request = review.request.expect("a request");
            seen.lock().unwrap().push(format!(
                "{} {}",
                serde_json::to_value(&request.operation)
                    .unwrap()
                    .as_str()
                    .unwrap(),
                request.resource.resource
            ));
            let response = if deny {
                AdmissionReviewResponse::deny(request.uid, "no new IPs".to_string())
            } else {
                AdmissionReviewResponse::allow(request.uid)
            };
            warp::reply::json(&AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(response),
            })
        });
    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            rx.await.ok();
        });
    tokio::spawn(server);
    (format!("http://{addr}"), tx)
}

async fn register(api: &TestApiServer, url: &str) {
    let cfg = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "ip.example.com"},
        "webhooks": [{
            "name": "ip.example.com",
            "clientConfig": {"url": url},
            "rules": [{
                "operations": ["CREATE", "DELETE"],
                "apiGroups": ["networking.k8s.io"],
                "apiVersions": ["v1"],
                "resources": ["ipaddresses"]
            }],
            "failurePolicy": "Fail",
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"]
        }]
    });
    // The REST validator only admits https URLs, which a local mock cannot
    // serve; the webhook manager reads the same storage key.
    let cfg: rusternetes_common::resources::ValidatingWebhookConfiguration =
        serde_json::from_value(cfg).unwrap();
    api.storage
        .create(
            &build_key("validatingwebhookconfigurations", None, "ip.example.com"),
            &cfg,
        )
        .await
        .unwrap();
}

fn svc(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"selector": {"app": name}, "ports": [{"port": 80}]}
    })
}

#[tokio::test]
async fn a_webhook_on_ipaddresses_sees_the_allocation_and_the_release() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _stop) = start_webhook(false, seen.clone()).await;
    let api = TestApiServer::new();
    register(&api, &url).await;

    let (s, c) = api.post(SVCS, &svc("a")).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["CREATE ipaddresses"],
        "the allocation went around admission"
    );

    let (s, _) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["CREATE ipaddresses", "DELETE ipaddresses"],
        "the release went around admission"
    );
}

#[tokio::test]
async fn a_webhook_that_denies_the_ipaddress_fails_the_service_create() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _stop) = start_webhook(true, seen.clone()).await;
    let api = TestApiServer::new();
    register(&api, &url).await;

    let (s, c) = api.post(SVCS, &svc("a")).await;
    assert!(!s.is_success(), "the denied allocation must fail: {s} {c}");
    let (s, _) = api.get(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "no Service without its IP");
}
