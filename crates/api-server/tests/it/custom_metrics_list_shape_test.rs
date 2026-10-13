//! #3389: every custom.metrics route is `REST.List` and answers a
//! `MetricValueList` whose items carry `metric: {name, selector}`.
//!
//! Upstream: kubernetes-sigs/custom-metrics-apiserver @
//! ed62571de2338833370b3cf22e6ad91e323186c4
//! - `pkg/registry/custom_metrics/reststorage.go:136-149` `handleIndividualOp`
//!   wraps the single value in a `MetricValueList`.
//! - `pkg/apiserver/installer/cmhandlers.go:160-247` installs the root-scoped,
//!   namespaced and `namespaces/{namespace}/metrics/{name}` routes on the one
//!   `ListResourceWithOptions` handler; `pkg/apiserver/endpoints/handlers/
//!   get.go:49-58` rewrites the last into resource `namespaces`.
//! - `k8s.io/metrics/pkg/apis/custom_metrics/v1beta2/types.go`:
//!   `MetricValue{DescribedObject, Metric MetricIdentifier{Name, Selector},
//!   Timestamp, WindowSeconds, Value}`.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CM: &str = "/apis/custom.metrics.k8s.io/v1beta2";

async fn pod(api: &TestApiServer, name: &str, app: &str) {
    let body = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": {"app": app}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let (s, b) = api.post("/api/v1/namespaces/default/pods", &body).await;
    assert!(s.is_success(), "create pod {name}: {s} {b}");
}

async fn namespace(api: &TestApiServer, name: &str) {
    let body = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}});
    let (s, b) = api.post("/api/v1/namespaces", &body).await;
    assert!(s.is_success(), "create namespace {name}: {s} {b}");
}

async fn node(api: &TestApiServer, name: &str) {
    let body = json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": name}});
    let (s, b) = api.post("/api/v1/nodes", &body).await;
    assert!(s.is_success(), "create node {name}: {s} {b}");
}

fn names(list: &Value) -> Vec<String> {
    let mut n: Vec<String> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["describedObject"]["name"].as_str().unwrap().to_string())
        .collect();
    n.sort();
    n
}

async fn seeded() -> TestApiServer {
    let api = TestApiServer::new();
    pod(&api, "web-1", "web").await;
    pod(&api, "web-2", "web").await;
    pod(&api, "db-1", "db").await;
    api
}

#[tokio::test]
async fn custom_named_get_is_a_one_item_metric_value_list() {
    let api = seeded().await;
    let (s, b) = api
        .get(&format!("{CM}/namespaces/default/pods/web-1/qps"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["kind"], "MetricValueList", "{b}");
    let items = b["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{b}");
    let item = &items[0];
    assert_eq!(item["describedObject"]["name"], "web-1", "{b}");
    // `selector` has no omitempty, so an unset one is `null`.
    assert_eq!(item["metric"]["name"], "qps", "{b}");
    assert!(
        item["metric"].as_object().unwrap().contains_key("selector"),
        "{b}"
    );
    assert!(item["metric"]["selector"].is_null(), "{b}");
    assert!(item.get("metricName").is_none(), "{b}");
    assert!(item.get("selector").is_none(), "{b}");
    assert!(item.get("window").is_none(), "{b}");
}

#[tokio::test]
async fn custom_list_items_carry_the_metric_identifier() {
    let api = seeded().await;
    let (s, b) = api.get(&format!("{CM}/namespaces/default/pods/qps")).await;
    assert!(s.is_success(), "{s} {b}");
    for item in b["items"].as_array().unwrap() {
        assert_eq!(item["metric"]["name"], "qps", "{b}");
        assert!(item.get("metricName").is_none(), "{b}");
    }
}

#[tokio::test]
async fn custom_metric_identifier_echoes_the_metric_selector() {
    let api = seeded().await;
    let (s, b) = api
        .get(&format!(
            "{CM}/namespaces/default/pods/web-1/qps?metricLabelSelector=!foo"
        ))
        .await;
    assert!(s.is_success(), "{s} {b}");
    let sel = &b["items"][0]["metric"]["selector"];
    assert_eq!(sel["matchExpressions"][0]["key"], "foo", "{b}");
    assert_eq!(
        sel["matchExpressions"][0]["operator"], "DoesNotExist",
        "{b}"
    );
}

#[tokio::test]
async fn custom_named_route_with_star_is_the_wildcard_list() {
    let api = seeded().await;
    // get.go: the path name `*` becomes `metadata.name=*`, which `List` treats
    // as `name == "*"` -> handleWildcardOp.
    let (s, b) = api
        .get(&format!(
            "{CM}/namespaces/default/pods/*/qps?labelSelector=app%3Dweb"
        ))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["kind"], "MetricValueList", "{b}");
    assert_eq!(names(&b), ["web-1", "web-2"]);
}

#[tokio::test]
async fn custom_namespace_route_is_a_metric_value_list_for_the_namespace() {
    let api = TestApiServer::new();
    namespace(&api, "team-a").await;
    let (s, b) = api
        .get(&format!("{CM}/namespaces/team-a/metrics/qps"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["kind"], "MetricValueList", "{b}");
    assert_eq!(b["items"].as_array().unwrap().len(), 1, "{b}");
    let item = &b["items"][0];
    assert_eq!(item["describedObject"]["name"], "team-a", "{b}");
    assert!(item["describedObject"].get("namespace").is_none(), "{b}");
    assert_eq!(item["metric"]["name"], "qps", "{b}");
    // Named lookup of a missing namespace: NewMetricNotFoundForError.
    let (s, b) = api.get(&format!("{CM}/namespaces/nope/metrics/qps")).await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(
        b["message"], "the server could not find the metric qps for namespaces nope",
        "{b}"
    );
}

#[tokio::test]
async fn custom_cluster_route_is_a_metric_value_list() {
    let api = TestApiServer::new();
    node(&api, "n1").await;
    let (s, b) = api.get(&format!("{CM}/nodes/n1/qps")).await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["kind"], "MetricValueList", "{b}");
    assert_eq!(b["items"][0]["describedObject"]["name"], "n1", "{b}");
    assert_eq!(b["items"][0]["metric"]["name"], "qps", "{b}");
    let (s, b) = api.get(&format!("{CM}/nodes/nope/qps")).await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(
        b["message"], "the server could not find the metric qps for nodes nope",
        "{b}"
    );
}

#[tokio::test]
async fn custom_namespace_and_cluster_routes_decode_list_options() {
    let api = TestApiServer::new();
    namespace(&api, "team-a").await;
    node(&api, "n1").await;
    for base in [
        format!("{CM}/namespaces/team-a/metrics/qps"),
        format!("{CM}/nodes/n1/qps"),
    ] {
        // get.go:84-89: labelSelector parse failure is a 400.
        let (s, b) = api
            .get(&format!("{base}?labelSelector=a%3D%3D%3Db%3D%3D"))
            .await;
        assert_eq!(s.as_u16(), 400, "{base}: {b}");
        // get.go:114-123: a fieldSelector naming another object is a 400.
        let (s, b) = api
            .get(&format!("{base}?fieldSelector=metadata.name%3Dother"))
            .await;
        assert_eq!(s.as_u16(), 400, "{base}: {b}");
        // reststorage.go:84-91: malformed metricLabelSelector is 500 Unknown.
        let (s, b) = api
            .get(&format!("{base}?metricLabelSelector=a%3D%3D%3Db%3D%3D"))
            .await;
        assert_eq!(s.as_u16(), 500, "{base}: {b}");
        // provider.go:230-232: non-matching metric selector is NotFound.
        let (s, b) = api
            .get(&format!("{base}?metricLabelSelector=foo%3Dbar"))
            .await;
        assert_eq!(s.as_u16(), 404, "{base}: {b}");
    }
}
