//! `crdHandler.ServeHTTP` scope gate (#3348): "if the scope in the CRD and
//! the scope in request differ (with exception of the verbs in
//! possiblyAcrossAllNamespacesVerbs for namespaced resources), pass request
//! to the delegate, which is supposed to lead to a 404"
//! (apiextensions-apiserver/pkg/apiserver/customresource_handler.go:270-280,
//! `possiblyAcrossAllNamespacesVerbs = list, watch` at :225).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn crd(plural: &str, kind: &str, scope: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.example.com") },
        "spec": {
            "group": "example.com",
            "scope": scope,
            "names": { "plural": plural, "kind": kind },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "x-kubernetes-preserve-unknown-fields": true
                }}
            }]
        }
    })
}

async fn install(api: &TestApiServer, plural: &str, kind: &str, scope: &str) {
    let (status, out) = api.post(CRDS, &crd(plural, kind, scope)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("{plural}.example.com"))
        .await;
}

#[tokio::test]
async fn namespaced_request_for_a_cluster_scoped_crd_is_404() {
    let api = TestApiServer::new();
    install(&api, "globals", "Global", "Cluster").await;
    let obj = json!({
        "apiVersion": "example.com/v1", "kind": "Global",
        "metadata": { "name": "g" }
    });
    let (status, out) = api.post("/apis/example.com/v1/globals", &obj).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let base = "/apis/example.com/v1/namespaces/default/globals";
    let (status, out) = api.get(base).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "list {out}");
    let (status, out) = api.get(&format!("{base}/g")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "get {out}");
    let (status, out) = api.post(base, &obj).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "create {out}");
    let (status, out) = api.get("/apis/example.com/v1/globals/g").await;
    assert_eq!(status, StatusCode::OK, "cluster-wide get still works {out}");
}

#[tokio::test]
async fn cluster_wide_request_for_a_namespaced_crd_is_404_except_list_and_watch() {
    let api = TestApiServer::new();
    install(&api, "widgets", "Widget", "Namespaced").await;
    let obj = json!({
        "apiVersion": "example.com/v1", "kind": "Widget",
        "metadata": { "name": "w", "namespace": "default" }
    });
    let (status, out) = api
        .post("/apis/example.com/v1/namespaces/default/widgets", &obj)
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    // list across all namespaces is the exception.
    let (status, out) = api.get("/apis/example.com/v1/widgets").await;
    assert_eq!(status, StatusCode::OK, "list {out}");
    // every other verb without a namespace is a 404.
    let (status, out) = api.get("/apis/example.com/v1/widgets/w").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "get {out}");
    let (status, out) = api.post("/apis/example.com/v1/widgets", &obj).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "create {out}");
    let (status, out) = api.delete("/apis/example.com/v1/widgets").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deletecollection {out}");
}
