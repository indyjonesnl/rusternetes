//! Scope/namespace mismatch gate of `crdHandler.ServeHTTP` (#3348).
//!
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go:268-280`:
//! "if the scope in the CRD and the scope in request differ (with exception of
//! the verbs in possiblyAcrossAllNamespacesVerbs for namespaced resources),
//! pass request to the delegate, which is supposed to lead to a 404."

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";
const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn crd(plural: &str, kind: &str, scope: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.{GROUP}") },
        "spec": {
            "group": GROUP,
            "scope": scope,
            "names": { "plural": plural, "kind": kind, "listKind": format!("{kind}List") },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object", "x-kubernetes-preserve-unknown-fields": true } }
            }]
        }
    })
}

async fn install(api: &TestApiServer, plural: &str, kind: &str, scope: &str) {
    let (status, out) = api.post(CRDS, &crd(plural, kind, scope)).await;
    assert_eq!(status, StatusCode::CREATED, "install CRD: {out}");
    api.wait_crd_established(&format!("{plural}.{GROUP}")).await;
}

fn obj(kind: &str, name: &str) -> Value {
    json!({ "apiVersion": format!("{GROUP}/v1"), "kind": kind, "metadata": { "name": name } })
}

/// cluster-scoped CRD + namespaced request => delegate (404) for every verb.
#[tokio::test]
async fn cluster_scoped_crd_namespaced_request_is_not_found() {
    let api = TestApiServer::new();
    install(&api, "globals", "Global", "Cluster").await;
    let (s, out) = api
        .post(&format!("/apis/{GROUP}/v1/globals"), &obj("Global", "g1"))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");

    let ns = format!("/apis/{GROUP}/v1/namespaces/default/globals");
    let (s, out) = api.post(&ns, &obj("Global", "g2")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "create: {out}");
    let (s, out) = api.get(&ns).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "list: {out}");
    let (s, out) = api.get(&format!("{ns}/g1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "get: {out}");
    let (s, out) = api.delete(&format!("{ns}/g1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "delete: {out}");
    // the object is untouched.
    let (s, out) = api.get(&format!("/apis/{GROUP}/v1/globals/g1")).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}

/// namespaced CRD + cluster-wide request: only list/watch pass.
#[tokio::test]
async fn namespaced_crd_cluster_wide_request_only_list_and_watch_pass() {
    let api = TestApiServer::new();
    install(&api, "widgets", "Widget", "Namespaced").await;
    let ns = format!("/apis/{GROUP}/v1/namespaces/default/widgets");
    let (s, out) = api.post(&ns, &obj("Widget", "w1")).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");

    let all = format!("/apis/{GROUP}/v1/widgets");
    // possiblyAcrossAllNamespacesVerbs = list, watch.
    let (s, out) = api.get(&all).await;
    assert_eq!(s, StatusCode::OK, "list: {out}");
    let (s, out) = api.post(&all, &obj("Widget", "w2")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "create: {out}");
    let (s, out) = api.get(&format!("{all}/w1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "get: {out}");
    let (s, out) = api.delete(&format!("{all}/w1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "delete: {out}");
    let (s, out) = api.delete(&all).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "deletecollection: {out}");
    let (s, out) = api.get(&format!("{ns}/w1")).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}
