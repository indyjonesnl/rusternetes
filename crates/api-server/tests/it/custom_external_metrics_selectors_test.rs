//! #3342: custom/external metrics list selectors.
//!
//! Upstream: kubernetes-sigs/custom-metrics-apiserver @
//! ed62571de2338833370b3cf22e6ad91e323186c4
//! - `pkg/registry/custom_metrics/reststorage.go:72-134` `List`: the standard
//!   `labelSelector` selects the described OBJECTS (`GetMetricBySelector`);
//!   `metricLabelSelector` (`labels.Parse`, error returned as-is) filters the
//!   metric's labels; `fieldSelector` `metadata.name` (`RequiresExactMatch`)
//!   routes to `GetMetricByName`.
//! - `pkg/registry/external_metrics/reststorage.go:72-90`: `labelSelector` is
//!   the metric label selector.
//! - `test-adapter/provider/provider.go:215-235,265-292` (provider semantics:
//!   `metricSelector.Matches(series labels)`, NotFound objects skipped in a
//!   selector list, `NewMetricNotFoundForSelectorError` for a named miss).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const BASE: &str = "/apis/custom.metrics.k8s.io/v1beta2/namespaces/default/pods/qps";

async fn pod(api: &TestApiServer, name: &str, app: &str) {
    let body = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": {"app": app}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let (s, b) = api.post("/api/v1/namespaces/default/pods", &body).await;
    assert!(s.is_success(), "create pod {name}: {s} {b}");
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
async fn custom_label_selector_selects_described_objects() {
    let api = seeded().await;
    let (s, b) = api.get(&format!("{BASE}?labelSelector=app%3Dweb")).await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(names(&b), ["web-1", "web-2"]);
    // Set-based requirements select objects too.
    let (_, b) = api
        .get(&format!("{BASE}?labelSelector=app%20notin%20(web)"))
        .await;
    assert_eq!(names(&b), ["db-1"]);
    let (_, b) = api
        .get(&format!("{BASE}?labelSelector=app%20in%20(web%2Cdb)"))
        .await;
    assert_eq!(names(&b), ["db-1", "web-1", "web-2"]);
    // No selector: every object.
    let (_, b) = api.get(BASE).await;
    assert_eq!(names(&b), ["db-1", "web-1", "web-2"]);
}

#[tokio::test]
async fn custom_metric_label_selector_filters_metric_labels() {
    let api = seeded().await;
    // The built-in series carries no labels (test-adapter valueFor: a
    // non-matching selector is NotFound, skipped in a selector list).
    let (s, b) = api
        .get(&format!("{BASE}?metricLabelSelector=foo%3Dbar"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert!(names(&b).is_empty(), "{b}");
    let (_, b) = api.get(&format!("{BASE}?metricLabelSelector=!foo")).await;
    assert_eq!(names(&b), ["db-1", "web-1", "web-2"]);
    // Both selectors together.
    let (_, b) = api
        .get(&format!(
            "{BASE}?labelSelector=app%3Dweb&metricLabelSelector=foo%20notin%20(x)"
        ))
        .await;
    assert_eq!(names(&b), ["web-1", "web-2"]);
}

#[tokio::test]
async fn custom_malformed_metric_label_selector_is_returned_as_is() {
    let api = seeded().await;
    // `labels.Parse` error returned from List unwrapped: not an APIStatus, so
    // ErrorToAPIStatus answers 500 / reason Unknown with err.Error().
    let (s, b) = api
        .get(&format!("{BASE}?metricLabelSelector=a%3D%3D%3Db%3D%3D"))
        .await;
    assert_eq!(s.as_u16(), 500, "{b}");
    assert_eq!(b["reason"], "Unknown", "{b}");
}

#[tokio::test]
async fn custom_field_selector_metadata_name_is_an_individual_get() {
    let api = seeded().await;
    let (s, b) = api
        .get(&format!("{BASE}?fieldSelector=metadata.name%3Dweb-1"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(names(&b), ["web-1"]);
    // handleIndividualOp ignores the object labelSelector.
    let (_, b) = api
        .get(&format!(
            "{BASE}?fieldSelector=metadata.name%3Dweb-1&labelSelector=app%3Ddb"
        ))
        .await;
    assert_eq!(names(&b), ["web-1"]);
    // Named + non-matching metric selector: NewMetricNotFoundForSelectorError.
    let (s, b) = api
        .get(&format!(
            "{BASE}?fieldSelector=metadata.name%3Dweb-1&metricLabelSelector=foo%3Dbar"
        ))
        .await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(
        b["message"],
        "the server could not find the metric qps for pods web-1 with selector foo=bar",
        "{b}"
    );
    // Named object that does not exist: NewMetricNotFoundForError.
    let (s, b) = api
        .get(&format!("{BASE}?fieldSelector=metadata.name%3Dnope"))
        .await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(
        b["message"], "the server could not find the metric qps for pods nope",
        "{b}"
    );
}

#[tokio::test]
async fn external_label_selector_is_the_metric_selector_set_based() {
    let api = TestApiServer::new();
    let base = "/apis/external.metrics.k8s.io/v1beta1/namespaces/default/qps";
    let count = |b: &Value| b["items"].as_array().unwrap().len();
    // The built-in series carries no labels.
    let (_, b) = api.get(&format!("{base}?labelSelector=foo%3Dbar")).await;
    assert_eq!(count(&b), 0, "{b}");
    let (_, b) = api
        .get(&format!("{base}?labelSelector=foo%20in%20(a%2Cb)"))
        .await;
    assert_eq!(count(&b), 0, "{b}");
    let (_, b) = api.get(&format!("{base}?labelSelector=!foo")).await;
    assert_eq!(count(&b), 1, "{b}");
    let (_, b) = api
        .get(&format!("{base}?labelSelector=foo%20notin%20(a)"))
        .await;
    assert_eq!(count(&b), 1, "{b}");
}
