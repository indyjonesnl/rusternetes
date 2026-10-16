//! `forbidCreateAdmission` (#3488): while a CRD is Terminating, a create of
//! one of its custom resources is refused at admission.
//!
//! Upstream `crdHandler.ServeHTTP` wraps the admission chain for the `create`
//! and `patch` verbs when `IsCRDConditionTrue(crd, Terminating)`
//! (apiextensions-apiserver/pkg/apiserver/customresource_handler.go:388-401);
//! the wrapper refuses every `admission.Create` with
//! `apierrors.NewForbidden(gr, name, "create not allowed while custom
//! resource definition is terminating")` (:1523-1554). A patch only reaches
//! `Create` when it creates the object (server-side apply on a missing name).

use std::time::Duration;

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
const WIDGETS: &str = "/apis/example.com/v1/namespaces/default/widgets";
const FORBIDDEN_TAIL: &str = "create not allowed while custom resource definition is terminating";

fn widget_crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "widgets.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": { "plural": "widgets", "kind": "Widget" },
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

fn widget(name: &str, finalizers: bool) -> Value {
    let mut w = json!({
        "apiVersion": "example.com/v1", "kind": "Widget",
        "metadata": { "name": name, "namespace": "default" },
        "spec": { "a": 1 }
    });
    if finalizers {
        w["metadata"]["finalizers"] = json!(["example.com/hold"]);
    }
    w
}

/// A CRD held in Terminating by a custom resource that carries a finalizer.
async fn terminating_crd(api: &TestApiServer) {
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established("widgets.example.com").await;
    let (status, out) = api.post(WIDGETS, &widget("held", true)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, out) = api.delete(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    for _ in 0..500 {
        let (_, crd) = api.get(&format!("{CRDS}/widgets.example.com")).await;
        let terminating = crd["status"]["conditions"].as_array().is_some_and(|c| {
            c.iter()
                .any(|c| c["type"] == "Terminating" && c["status"] == "True")
        });
        if terminating {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("CRD never became Terminating");
}

#[tokio::test]
async fn create_is_forbidden_while_the_crd_is_terminating() {
    let api = TestApiServer::new();
    terminating_crd(&api).await;
    let (status, out) = api.post(WIDGETS, &widget("late", false)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{out}");
    assert_eq!(
        out["message"],
        format!("widgets.example.com \"late\" is forbidden: {FORBIDDEN_TAIL}"),
        "{out}"
    );
}

/// The patch that creates (server-side apply on a missing name) is an
/// `admission.Create`; a patch of an existing object is not.
#[tokio::test]
async fn apply_that_creates_is_forbidden_but_patching_an_existing_one_is_not() {
    let api = TestApiServer::new();
    terminating_crd(&api).await;
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{WIDGETS}/late?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&widget("late", false)),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{out}");
    assert_eq!(
        out["message"],
        format!("widgets.example.com \"late\" is forbidden: {FORBIDDEN_TAIL}"),
        "{out}"
    );
    let (status, out) = api
        .patch(&format!("{WIDGETS}/held"), &json!({"spec": {"a": 2}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

#[tokio::test]
async fn create_is_allowed_while_the_crd_is_not_terminating() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established("widgets.example.com").await;
    let (status, out) = api.post(WIDGETS, &widget("ok", false)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
}
