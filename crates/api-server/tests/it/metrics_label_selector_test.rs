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
    // Only pods with a published sample are listed (pkg/storage/pod.go).
    publish_pod_sample(&api, "m1782", "web").await;
    publish_pod_sample(&api, "m1782", "bystander").await;
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
    // Only nodes with a published sample are listed (pkg/storage/node.go).
    publish_node_sample(&api, "n-web").await;
    publish_node_sample(&api, "n-other").await;
    let (s, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/nodes?labelSelector=role%3Dweb")
        .await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert_eq!(names(&b), vec!["n-web"]);
}

/// Publish a kubelet-style (label-less) NodeMetrics sample for `name`.
async fn publish_node_sample(api: &TestApiServer, name: &str) {
    use rusternetes_storage::Storage;
    let sample: rusternetes_common::resources::NodeMetrics = serde_json::from_value(json!({
        "apiVersion": "metrics.k8s.io/v1beta1",
        "kind": "NodeMetrics",
        "metadata": {"name": name},
        "timestamp": "2026-10-12T00:00:00Z",
        "window": "30s",
        "usage": {"cpu": "10m", "memory": "10Mi"}
    }))
    .unwrap();
    api.storage
        .create(&format!("/registry/metrics.k8s.io/nodes/{name}"), &sample)
        .await
        .unwrap();
}

/// #3327: metrics-server `pkg/storage/node.go` `GetMetrics` does `continue`
/// for a node with no stored point, and `pkg/api/node.go` `List` returns
/// exactly those results: a node without a sample is absent from the list,
/// not listed with zero usage. A node WITH a sample still honours the
/// selector and carries the node's labels.
#[tokio::test]
async fn node_metrics_list_omits_nodes_without_sample() {
    let api = TestApiServer::new();
    seed(&api).await;
    publish_node_sample(&api, "n-web").await;
    let (s, b) = api.get("/apis/metrics.k8s.io/v1beta1/nodes").await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert_eq!(names(&b), vec!["n-web"], "{b}");
    assert_eq!(b["items"][0]["metadata"]["labels"]["role"], "web", "{b}");

    let (_, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/nodes?labelSelector=role%3Ddb")
        .await;
    assert!(names(&b).is_empty(), "{b}");

    let (s, b) = api.get("/apis/metrics.k8s.io/v1beta1/nodes/n-web").await;
    assert_eq!(s.as_u16(), 200, "{b}");

    // No node has a sample: an empty list, not one zero-usage entry per node.
    let bare = TestApiServer::new();
    seed(&bare).await;
    let (s, b) = bare.get("/apis/metrics.k8s.io/v1beta1/nodes").await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert!(names(&b).is_empty(), "{b}");
}

/// #3327: metrics-server `pkg/api/node.go` `Get` returns
/// `errors.NewNotFound(m.groupResource, name)` when no metrics exist for the
/// node, rather than a synthesized zero-usage object. `NewNotFound`
/// (`apimachinery/pkg/api/errors/errors.go`) renders
/// `{group-resource} "{name}" not found` with `details.group`/`kind`/`name`.
/// `kubectl top node <name>` therefore reports NotFound for a node without a
/// sample.
#[tokio::test]
async fn node_metrics_get_without_sample_is_not_found() {
    let api = TestApiServer::new();
    seed(&api).await;
    let (s, b) = api.get("/apis/metrics.k8s.io/v1beta1/nodes/n-web").await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(b["kind"], "Status", "{b}");
    assert_eq!(b["reason"], "NotFound", "{b}");
    assert_eq!(
        b["message"], "nodemetrics.metrics.k8s.io \"n-web\" not found",
        "{b}"
    );
    assert_eq!(b["details"]["group"], "metrics.k8s.io", "{b}");
    assert_eq!(b["details"]["kind"], "nodemetrics", "{b}");
    assert_eq!(b["details"]["name"], "n-web", "{b}");
}

/// Publish a kubelet-style (label-less) PodMetrics sample for `ns/name`.
async fn publish_pod_sample(api: &TestApiServer, ns: &str, name: &str) {
    use rusternetes_storage::Storage;
    let sample: rusternetes_common::resources::PodMetrics = serde_json::from_value(json!({
        "apiVersion": "metrics.k8s.io/v1beta1",
        "kind": "PodMetrics",
        "metadata": {"name": name, "namespace": ns},
        "timestamp": "2026-10-12T00:00:00Z",
        "window": "30s",
        "containers": [{"name": "c", "usage": {"cpu": "10m", "memory": "10Mi"}}]
    }))
    .unwrap();
    api.storage
        .create(
            &format!("/registry/metrics.k8s.io/pods/{ns}/{name}"),
            &sample,
        )
        .await
        .unwrap();
}

/// #3341: metrics-server `pkg/storage/pod.go` `GetMetrics` does `continue`
/// for a pod with no stored point (lines 51-59), and `pkg/api/pod.go` `List`
/// returns exactly those results (lines 85-91): a pod without a sample is
/// absent from the list, not listed with usage fabricated from its requests.
/// A pod WITH a sample carries the pod's labels and the sample's usage.
#[tokio::test]
async fn pod_metrics_list_omits_pods_without_sample() {
    let api = TestApiServer::new();
    seed(&api).await;
    publish_pod_sample(&api, "m1782", "web").await;
    for uri in [
        "/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods",
        "/apis/metrics.k8s.io/v1beta1/pods",
    ] {
        let (s, b) = api.get(uri).await;
        assert_eq!(s.as_u16(), 200, "{b}");
        assert_eq!(names(&b), vec!["web"], "{uri}: {b}");
        assert_eq!(b["items"][0]["metadata"]["labels"]["app"], "web", "{b}");
        assert_eq!(b["items"][0]["containers"][0]["usage"]["cpu"], "10m", "{b}");
    }

    // No pod has a sample: an empty list, not one fabricated entry per pod.
    let bare = TestApiServer::new();
    seed(&bare).await;
    let (s, b) = bare
        .get("/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods")
        .await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert!(names(&b).is_empty(), "{b}");
    let (_, b) = bare.get("/apis/metrics.k8s.io/v1beta1/pods").await;
    assert!(names(&b).is_empty(), "{b}");
}

/// #3341: metrics-server `pkg/api/pod.go` `Get` returns
/// `errors.NewNotFound(m.groupResource, fmt.Sprintf("%s/%s", namespace, name))`
/// when `len(ms) == 0` (lines 134-136), with `groupResource =
/// metrics.Resource("podmetrics")` (`pkg/api/install.go:61`). The name is the
/// `namespace/name` pair, so the message is
/// `podmetrics.metrics.k8s.io "m1782/web" not found`. A pod that does not
/// exist at all is the lister's `pods "<name>" not found` (lines 118-121).
#[tokio::test]
async fn pod_metrics_get_without_sample_is_not_found() {
    let api = TestApiServer::new();
    seed(&api).await;
    let (s, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods/web")
        .await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(b["kind"], "Status", "{b}");
    assert_eq!(b["reason"], "NotFound", "{b}");
    assert_eq!(
        b["message"], "podmetrics.metrics.k8s.io \"m1782/web\" not found",
        "{b}"
    );
    assert_eq!(b["details"]["group"], "metrics.k8s.io", "{b}");
    assert_eq!(b["details"]["kind"], "podmetrics", "{b}");
    assert_eq!(b["details"]["name"], "m1782/web", "{b}");

    publish_pod_sample(&api, "m1782", "web").await;
    let (s, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods/web")
        .await;
    assert_eq!(s.as_u16(), 200, "{b}");
    assert_eq!(b["metadata"]["labels"]["app"], "web", "{b}");

    // Missing pod: NotFound from the pod lister, not from the metrics path.
    let (s, b) = api
        .get("/apis/metrics.k8s.io/v1beta1/namespaces/m1782/pods/ghost")
        .await;
    assert_eq!(s.as_u16(), 404, "{b}");
}
