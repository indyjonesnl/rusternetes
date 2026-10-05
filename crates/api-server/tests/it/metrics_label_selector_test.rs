//! #1782: `metrics.k8s.io/v1beta1` list endpoints must honour `labelSelector`
//! (and `fieldSelector`), so `kubectl top pod -l app=x` is filtered.
//!
//! Upstream: metrics-server `pkg/api/pod.go` / `node.go` `List` build each
//! PodMetrics/NodeMetrics with `Labels` copied from the source pod/node and
//! filter with the request's label selector; the generic registry does the
//! same via `ListPredicate`
//! (`staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`).
//! The bystander (unlabelled) object must be dropped by the filter.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn names(body: &Value) -> Vec<String> {
    let mut v: Vec<String> = body["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

async fn seed(api: &TestApiServer) {
    let (s, b) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"m1782"}}),
        )
        .await;
    assert!(s.is_success(), "{s} {b}");
    for (name, labels) in [
        ("web", json!({"app":"web"})),
        ("bystander", json!({"app":"db"})),
    ] {
        let (s, b) = api
            .post(
                "/api/v1/namespaces/m1782/pods",
                &json!({"apiVersion":"v1","kind":"Pod",
                    "metadata":{"name":name,"labels":labels},
                    "spec":{"containers":[{"name":"c","image":"busybox"}]}}),
            )
            .await;
        assert!(s.is_success(), "{s} {b}");
    }
    for (name, labels) in [
        ("n-web", json!({"role":"web"})),
        ("n-other", json!({"role":"db"})),
    ] {
        let (s, b) = api
            .post(
                "/api/v1/nodes",
                &json!({"apiVersion":"v1","kind":"Node","metadata":{"name":name,"labels":labels}}),
            )
            .await;
        assert!(s.is_success(), "{s} {b}");
    }
}

#[tokio::test]
async fn pod_metrics_list_honours_label_selector() {
    let api = TestApiServer::new();
    seed(&api).await;
    for uri in [
        "/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods?labelSelector=app%3Dweb",
        "/apis/metrics.k8s.io/v1beta1/pods?labelSelector=app%3Dweb",
    ] {
        let (s, b) = api.get(uri).await;
        assert_eq!(s.as_u16(), 200, "{b}");
        assert_eq!(names(&b), vec!["web"], "{uri}");
    }
    let (_, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods")
        .await;
    assert_eq!(names(&b), vec!["bystander", "web"]);
}

#[tokio::test]
async fn node_metrics_list_honours_label_selector() {
    let api = TestApiServer::new();
    seed(&api).await;
    let (s, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/nodes?labelSelector=role%3Dweb")
        .await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert_eq!(names(&b), vec!["n-web"]);
}
