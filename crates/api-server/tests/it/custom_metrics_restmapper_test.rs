//! #3430: the custom.metrics object lookup and the `DescribedObject`
//! reference go through the provider's RESTMapper.
//!
//! Upstream: kubernetes-sigs/custom-metrics-apiserver @
//! ed62571de2338833370b3cf22e6ad91e323186c4
//! - `pkg/provider/interfaces.go:55-68` `CustomMetricInfo.Normalized`:
//!   `mapper.ResourceFor(GroupResource.WithVersion(""))`, so a singular or a
//!   `resource.group` request resolves to the plural resource.
//! - `pkg/provider/helpers/helpers.go:36-49` `ResourceFor` (`ResourcesFor`,
//!   first element; error text `unable to find preferred version to list
//!   matching resource names: %v`), `:55-69` `ReferenceFor`
//!   (`mapper.KindFor`; `APIVersion: kind.Group + "/" + kind.Version`, so the
//!   core group yields `/v1`), `:74-107` `ListObjectNames`.
//! - `test-adapter/provider/provider.go:215-262` `valueFor` / `metricFor` and
//!   `:265-292` `metricsFor` return those errors as-is (500 Unknown).
//! - The mapper is `restmapper.NewDiscoveryRESTMapper` over the served
//!   discovery (`pkg/dynamicmapper/mapper.go:65-78`).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CM: &str = "/apis/custom.metrics.k8s.io/v1beta2";

async fn pod(api: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let (s, b) = api.post("/api/v1/namespaces/default/pods", &body).await;
    assert!(s.is_success(), "create pod {name}: {s} {b}");
}

async fn deployment(api: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]},
            },
        },
    });
    let (s, b) = api
        .post("/apis/apps/v1/namespaces/default/deployments", &body)
        .await;
    assert!(s.is_success(), "create deployment {name}: {s} {b}");
}

async fn node(api: &TestApiServer, name: &str) {
    let body = json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": name}});
    let (s, b) = api.post("/api/v1/nodes", &body).await;
    assert!(s.is_success(), "create node {name}: {s} {b}");
}

fn reference(b: &Value) -> (String, String, String) {
    let d = &b["items"][0]["describedObject"];
    (
        d["apiVersion"].as_str().unwrap_or_default().to_string(),
        d["kind"].as_str().unwrap_or_default().to_string(),
        d["name"].as_str().unwrap_or_default().to_string(),
    )
}

#[tokio::test]
async fn described_object_is_the_kind_the_mapper_resolves() {
    let api = TestApiServer::new();
    pod(&api, "web-1").await;
    let (s, b) = api
        .get(&format!("{CM}/namespaces/default/pods/web-1/qps"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    // helpers.go:63-68: core group -> "" + "/" + "v1".
    assert_eq!(reference(&b), ("/v1".into(), "Pod".into(), "web-1".into()));
    assert_eq!(b["items"][0]["describedObject"]["namespace"], "default");
}

#[tokio::test]
async fn singular_resource_resolves_to_the_plural() {
    let api = TestApiServer::new();
    pod(&api, "web-1").await;
    let (s, b) = api
        .get(&format!("{CM}/namespaces/default/pod/web-1/qps"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(reference(&b), ("/v1".into(), "Pod".into(), "web-1".into()));
    // A wildcard list resolves the same way (ListObjectNames -> ResourceFor).
    let (s, b) = api.get(&format!("{CM}/namespaces/default/pod/qps")).await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(b["items"].as_array().unwrap().len(), 1, "{b}");
    assert_eq!(reference(&b).1, "Pod");
}

#[tokio::test]
async fn group_qualified_resource_resolves_and_names_its_group() {
    let api = TestApiServer::new();
    deployment(&api, "web").await;
    for resource in ["deployments.apps", "deployment.apps", "deployments"] {
        let (s, b) = api
            .get(&format!("{CM}/namespaces/default/{resource}/web/qps"))
            .await;
        assert!(s.is_success(), "{resource}: {s} {b}");
        assert_eq!(
            reference(&b),
            ("apps/v1".into(), "Deployment".into(), "web".into()),
            "{resource}: {b}"
        );
    }
}

#[tokio::test]
async fn cluster_scoped_singular_resolves() {
    let api = TestApiServer::new();
    node(&api, "n1").await;
    let (s, b) = api.get(&format!("{CM}/node/n1/qps")).await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(reference(&b), ("/v1".into(), "Node".into(), "n1".into()));
}

#[tokio::test]
async fn namespace_route_reference_is_a_namespace() {
    let api = TestApiServer::new();
    let body = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "team-a"}});
    let (s, b) = api.post("/api/v1/namespaces", &body).await;
    assert!(s.is_success(), "{s} {b}");
    let (s, b) = api
        .get(&format!("{CM}/namespaces/team-a/metrics/qps"))
        .await;
    assert!(s.is_success(), "{s} {b}");
    assert_eq!(
        reference(&b),
        ("/v1".into(), "Namespace".into(), "team-a".into())
    );
}

#[tokio::test]
async fn unknown_resource_is_the_mapper_error_as_unknown() {
    let api = TestApiServer::new();
    // provider.go:215-218 `valueFor`: Normalized's error is returned as-is.
    let (s, b) = api
        .get(&format!("{CM}/namespaces/default/foos/x/qps"))
        .await;
    assert_eq!(s.as_u16(), 500, "{b}");
    assert_eq!(b["reason"], "Unknown", "{b}");
    assert_eq!(b["message"], "no matches for /, Resource=foos", "{b}");
    // helpers.go:39-46 `ResourceFor` wraps it for a selector list.
    let (s, b) = api.get(&format!("{CM}/namespaces/default/foos/qps")).await;
    assert_eq!(s.as_u16(), 500, "{b}");
    assert_eq!(
        b["message"],
        "unable to find preferred version to list matching resource names: \
         no matches for /, Resource=foos",
        "{b}"
    );
}

#[tokio::test]
async fn not_found_message_names_the_normalized_group_resource() {
    let api = TestApiServer::new();
    let (s, b) = api
        .get(&format!("{CM}/namespaces/default/deployment.apps/nope/qps"))
        .await;
    assert_eq!(s.as_u16(), 404, "{b}");
    assert_eq!(
        b["message"],
        "the server could not find the metric qps for deployments.apps nope",
        "{b}"
    );
}
