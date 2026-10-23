//! The CRD naming/establishing controllers run asynchronously (#3264, #2128).
//!
//! Upstream's `Established` condition is set by a workqueue-driven controller
//! that reacts to CRD events, never by the create request itself:
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/controller/establish/establishing_controller.go`
//! (`sync`, :127-171) and `pkg/controller/status/naming_controller.go`
//! (`sync`, :240-292). The create response therefore carries no
//! `Established=True`; the condition shows up on a later MODIFIED event.

use std::time::Duration;

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

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

fn condition<'a>(crd: &'a Value, kind: &str) -> Option<&'a Value> {
    crd["status"]["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == kind)
}

/// Poll the CRD until `done` holds (the controllers are prompt: a queue wake,
/// not a timer), or fail after a generous bound.
async fn wait_for(api: &TestApiServer, name: &str, done: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..200 {
        let (status, out) = api.get(&format!("{CRDS}/{name}")).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if done(&out) {
            return out;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (_, out) = api.get(&format!("{CRDS}/{name}")).await;
    panic!("condition never reached: {out}");
}

/// The create response is the object as stored by the write: no controller
/// has run, so it is not Established (establishing_controller.go:127).
#[tokio::test]
async fn create_response_is_not_established() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_ne!(
        condition(&out, "Established").map(|c| c["status"].clone()),
        Some(json!("True")),
        "create returned an Established CRD: {out}"
    );
}

/// The controllers then establish it promptly, in order: the naming
/// controller's NamesAccepted, the establishing controller's Established.
#[tokio::test]
async fn crd_becomes_established_shortly_after_create() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let created_rv = out["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    let got = wait_for(&api, "widgets.example.com", |c| {
        condition(c, "Established").is_some_and(|e| e["status"] == "True")
    })
    .await;
    assert_eq!(
        condition(&got, "NamesAccepted").unwrap()["status"],
        "True",
        "{got}"
    );
    assert_ne!(
        got["metadata"]["resourceVersion"],
        json!(created_rv),
        "{got}"
    );
}

/// `establishing_controller.go:143-158`: a conversion webhook with an invalid
/// CABundle leaves the CRD not Established, reason `InvalidCABundle`.
#[tokio::test]
async fn invalid_ca_bundle_is_not_established() {
    let api = TestApiServer::new();
    let mut body = widget_crd();
    body["spec"]["conversion"] = json!({
        "strategy": "Webhook",
        "webhook": {
            "conversionReviewVersions": ["v1"],
            "clientConfig": {
                "url": "https://example.com/convert",
                "caBundle": "bm90LWEtY2VydA=="
            }
        }
    });
    let (status, out) = api.post(CRDS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let got = wait_for(&api, "widgets.example.com", |c| {
        condition(c, "NamesAccepted").is_some_and(|e| e["status"] == "True")
    })
    .await;
    // Give the establishing controller time to act, then check it refused.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, got2) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    let est = condition(&got2, "Established").unwrap_or_else(|| panic!("{got}"));
    assert_eq!(est["status"], "False", "{got2}");
    assert_eq!(est["reason"], "InvalidCABundle", "{got2}");
    assert_eq!(
        est["message"], "The conversion webhook CABundle is invalid",
        "{got2}"
    );
}

/// A write through the `/status` store is an informer update event too
/// (naming_controller.go:359-363, establishing_controller.go:71-73), so it
/// wakes the controllers: a status write that unsets `Established` is
/// repaired promptly, not on the 5 minute resync (#3297).
#[tokio::test]
async fn status_write_wakes_the_controllers() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let name = "widgets.example.com";
    let mut got = wait_for(&api, name, |c| {
        condition(c, "Established").is_some_and(|e| e["status"] == "True")
    })
    .await;
    for c in got["status"]["conditions"].as_array_mut().unwrap() {
        if c["type"] == "Established" {
            c["status"] = json!("False");
        }
    }
    let (status, out) = api.put(&format!("{CRDS}/{name}/status"), &got).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    wait_for(&api, name, |c| {
        condition(c, "Established").is_some_and(|e| e["status"] == "True")
    })
    .await;
}

/// The apiapproval controller is its own informer-driven controller
/// (apiapproval_controller.go:73-77, `RunWithContext(5, ..)` at
/// apiserver.go:248): the create response carries no
/// `KubernetesAPIApprovalPolicyConformant` condition, and the controller's
/// own `UpdateStatus` adds it shortly after (#2128).
#[tokio::test]
async fn api_approval_condition_arrives_after_create() {
    let api = TestApiServer::new();
    let mut body = widget_crd();
    body["metadata"]["name"] = json!("widgets.example.k8s.io");
    body["spec"]["group"] = json!("example.k8s.io");
    body["metadata"]["annotations"] =
        json!({"api-approved.kubernetes.io": "https://github.com/kubernetes/kubernetes/pull/1"});
    let (status, out) = api.post(CRDS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        condition(&out, "KubernetesAPIApprovalPolicyConformant").is_none(),
        "create returned the controller's condition: {out}"
    );
    let got = wait_for(&api, "widgets.example.k8s.io", |c| {
        condition(c, "KubernetesAPIApprovalPolicyConformant").is_some()
    })
    .await;
    assert_eq!(
        condition(&got, "KubernetesAPIApprovalPolicyConformant").unwrap()["status"],
        "True",
        "{got}"
    );
}
