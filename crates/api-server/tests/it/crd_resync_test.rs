//! A CRD left behind by a restart is picked up by the resync (#2157).
//!
//! Upstream's CRD controllers are informer-driven with a 5 minute resync
//! (`apiextensions-apiserver/pkg/apiserver/apiserver.go:170`), and the
//! finalizer re-enqueues a Terminating CRD on every resync
//! (`pkg/controller/finalizer/crd_finalizer.go:330-342`, "always requeue
//! resyncs just in case"). Nothing here retried a CRD whose first delete was
//! interrupted, so it stayed Terminating forever.

use std::sync::Arc;

use axum::http::StatusCode;
use rusternetes_api_server::registry::apiextensions::customresourcedefinition::new_rest;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
const FINALIZER: &str = "customresourcecleanup.apiextensions.k8s.io";

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

/// A CRD that an api-server restart left Terminating, with the cleanup
/// finalizer still on it and an instance still stored, is finalized by the
/// resync: the instances go, then the CRD.
#[tokio::test]
async fn resync_finalizes_a_crd_left_terminating() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        )
        .await;
    assert!(
        status.is_success() || status == StatusCode::CONFLICT,
        "{out}"
    );
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let cr = "/apis/example.com/v1/namespaces/default/widgets";
    let (status, out) = api
        .post(
            cr,
            &json!({"apiVersion": "example.com/v1", "kind": "Widget", "metadata": {"name": "w1"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    // The state a crash right after `REST.Delete` stamped the CRD leaves
    // (etcd.go:116-174): deletionTimestamp and finalizer set, nothing else done.
    let key = build_key("customresourcedefinitions", None, "widgets.example.com");
    let mut stored: Value = api.storage.get(&key).await.unwrap();
    stored["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    stored["metadata"]["finalizers"] = json!([FINALIZER]);
    api.storage.update(&key, &stored).await.unwrap();

    let backend = Arc::new(StorageBackend::Memory(api.storage.clone()));
    let failed = new_rest(backend).resync().await;
    assert!(failed.is_empty(), "resync failed for {failed:?}");

    let (status, out) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    let leftover: Vec<Value> = api
        .storage
        .list(&rusternetes_storage::build_prefix(
            "example_com_widgets",
            Some("default"),
        ))
        .await
        .unwrap();
    assert!(leftover.is_empty(), "instances left: {leftover:?}");
}

/// The naming and establishing controllers are resynced too: a CRD whose
/// status was lost is accepted and established again.
#[tokio::test]
async fn resync_establishes_a_crd_without_conditions() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CRDS, &widget_crd()).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let key = build_key("customresourcedefinitions", None, "widgets.example.com");
    let mut stored: Value = api.storage.get(&key).await.unwrap();
    stored["status"] = json!({"storedVersions": ["v1"]});
    api.storage.update(&key, &stored).await.unwrap();

    let backend = Arc::new(StorageBackend::Memory(api.storage.clone()));
    assert!(new_rest(backend).resync().await.is_empty());

    let (status, out) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let conditions = out["status"]["conditions"].as_array().expect("conditions");
    for kind in ["NamesAccepted", "Established"] {
        assert!(
            conditions
                .iter()
                .any(|c| c["type"] == kind && c["status"] == "True"),
            "{kind} missing: {out}"
        );
    }
}
