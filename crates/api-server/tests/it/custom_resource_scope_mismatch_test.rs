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

// ---- the remaining verbs (ported from #3489's DELETE cases, extended) ----

fn crd_with_subresources(plural: &str, kind: &str, scope: &str) -> Value {
    let mut c = crd(plural, kind, scope);
    c["spec"]["versions"][0]["subresources"] = json!({
        "status": {},
        "scale": {
            "specReplicasPath": ".spec.replicas",
            "statusReplicasPath": ".status.replicas"
        }
    });
    c
}

async fn install_with_subresources(api: &TestApiServer, plural: &str, kind: &str, scope: &str) {
    let (status, out) = api
        .post(CRDS, &crd_with_subresources(plural, kind, scope))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("{plural}.example.com"))
        .await;
}

/// DELETE, named DELETE and DELETE-collection on a namespaced path of a
/// cluster-scoped CRD are 404, and the object survives.
#[tokio::test]
async fn delete_verbs_for_a_cluster_scoped_crd_under_a_namespace_are_404() {
    let api = TestApiServer::new();
    install(&api, "globals", "Global", "Cluster").await;
    let obj = json!({
        "apiVersion": "example.com/v1", "kind": "Global",
        "metadata": { "name": "g" }
    });
    let (status, out) = api.post("/apis/example.com/v1/globals", &obj).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let base = "/apis/example.com/v1/namespaces/default/globals";
    let (status, out) = api.delete(&format!("{base}/g")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "named delete {out}");
    let (status, out) = api.delete(base).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deletecollection {out}");
    let (status, out) = api.get("/apis/example.com/v1/globals/g").await;
    assert_eq!(status, StatusCode::OK, "object must survive {out}");
}

/// DELETE, named DELETE and DELETE-collection with no namespace on a
/// namespaced CRD are 404 (delete/deletecollection are not in
/// possiblyAcrossAllNamespacesVerbs), and the object survives.
#[tokio::test]
async fn delete_verbs_for_a_namespaced_crd_without_a_namespace_are_404() {
    let api = TestApiServer::new();
    install(&api, "widgets", "Widget", "Namespaced").await;
    let obj = json!({
        "apiVersion": "example.com/v1", "kind": "Widget",
        "metadata": { "name": "w", "namespace": "default" }
    });
    let ns = "/apis/example.com/v1/namespaces/default/widgets";
    let (status, out) = api.post(ns, &obj).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, out) = api.delete("/apis/example.com/v1/widgets/w").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "named delete {out}");
    let (status, out) = api.delete("/apis/example.com/v1/widgets").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deletecollection {out}");
    let (status, out) = api.get(&format!("{ns}/w")).await;
    assert_eq!(status, StatusCode::OK, "object must survive {out}");
}

/// update (PUT) and patch, on the resource and on its status and scale
/// subresources, hit the same gate in both mismatch directions.
#[tokio::test]
async fn update_patch_status_and_scale_scope_mismatch_are_404() {
    let api = TestApiServer::new();
    install_with_subresources(&api, "globals", "Global", "Cluster").await;
    install_with_subresources(&api, "widgets", "Widget", "Namespaced").await;
    let g = json!({
        "apiVersion": "example.com/v1", "kind": "Global",
        "metadata": { "name": "g" }, "spec": { "replicas": 1 }
    });
    let (status, out) = api.post("/apis/example.com/v1/globals", &g).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let w = json!({
        "apiVersion": "example.com/v1", "kind": "Widget",
        "metadata": { "name": "w", "namespace": "default" }, "spec": { "replicas": 1 }
    });
    let (status, out) = api
        .post("/apis/example.com/v1/namespaces/default/widgets", &w)
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let scale = json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": { "name": "x" }, "spec": { "replicas": 3 }
    });
    // (uri, object for a PUT)
    let cases = [
        // cluster-scoped CRD addressed under a namespace.
        ("/apis/example.com/v1/namespaces/default/globals/g", &g),
        (
            "/apis/example.com/v1/namespaces/default/globals/g/status",
            &g,
        ),
        (
            "/apis/example.com/v1/namespaces/default/globals/g/scale",
            &scale,
        ),
        // namespaced CRD addressed without a namespace.
        ("/apis/example.com/v1/widgets/w", &w),
        ("/apis/example.com/v1/widgets/w/status", &w),
        ("/apis/example.com/v1/widgets/w/scale", &scale),
    ];
    for (uri, body) in cases {
        let (status, out) = api.put(uri, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "PUT {uri}: {out}");
        let (status, out) = api.patch(uri, &json!({ "spec": { "replicas": 2 } })).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "PATCH {uri}: {out}");
        let (status, out) = api.get(uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "GET {uri}: {out}");
    }
    // nothing was modified.
    let (status, out) = api.get("/apis/example.com/v1/globals/g").await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], json!(1), "{out}");
}
