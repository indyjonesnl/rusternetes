//! Evidence for #2130: the controller-manager CRD controller must reach a
//! custom resource under the key the api-server stores it at,
//! `/registry/{group with . -> _}_{plural}/{ns}/{name}`
//! (`registry::apiextensions::customresource::storage_prefix`).

use rusternetes_common::resources::CustomResourceDefinition;
use rusternetes_controller_manager::controllers::crd::CRDController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn crd_deletion_removes_instances_at_the_api_server_key() {
    let storage = Arc::new(MemoryStorage::new());
    let crd: CustomResourceDefinition = serde_json::from_value(json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "widgets.example.com",
            "deletionTimestamp": "2026-01-01T00:00:00Z",
            "finalizers": ["customresourcecleanup.apiextensions.k8s.io"]
        },
        "spec": {
            "group": "example.com",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "scope": "Namespaced",
            "versions": [{"name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}}]
        }
    }))
    .unwrap();
    storage
        .create(
            &build_key("customresourcedefinitions", None, "widgets.example.com"),
            &crd,
        )
        .await
        .unwrap();
    let cr_key = build_key("example_com_widgets", Some("default"), "w1");
    storage
        .create(
            &cr_key,
            &json!({"apiVersion": "example.com/v1", "kind": "Widget",
                "metadata": {"name": "w1", "namespace": "default"}}),
        )
        .await
        .unwrap();

    CRDController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    assert!(
        storage.get::<serde_json::Value>(&cr_key).await.is_err(),
        "the instance at {cr_key} survived the CRD's deletion"
    );
}
