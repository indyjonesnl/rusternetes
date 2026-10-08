//! Ports of TestLastTriggerChangeTimeAnnotation*
//! (pkg/controller/endpoint/endpoints_controller_test.go:1684-1830).

use super::endpoints_controller_test::{create_test_pod, create_test_service};
use chrono::TimeZone;
use rusternetes_common::resources::{Endpoints, Pod};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

const TRIGGER_TIME_ANNOTATION: &str = "endpoints.kubernetes.io/last-change-trigger-time";

async fn fixture(
    storage: &Arc<MemoryStorage>,
    service_created: Option<chrono::DateTime<chrono::Utc>>,
) {
    let selector = HashMap::from([("app".to_string(), "web".to_string())]);
    let mut service = create_test_service("foo", "other", selector.clone());
    service.metadata.creation_timestamp = service_created;
    storage
        .create(&build_key("services", Some("other"), "foo"), &service)
        .await
        .unwrap();
    let pod = create_test_pod("pod0", "other", selector, Some("1.2.3.4".to_string()), true);
    storage
        .create(&build_key("pods", Some("other"), "pod0"), &pod)
        .await
        .unwrap();
}

async fn existing_endpoints(storage: &Arc<MemoryStorage>, annotation: Option<&str>) {
    let mut ep = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("foo").with_namespace("other"),
        subsets: vec![],
    };
    ep.metadata.annotations =
        annotation.map(|a| HashMap::from([(TRIGGER_TIME_ANNOTATION.to_string(), a.to_string())]));
    storage
        .create(&build_key("endpoints", Some("other"), "foo"), &ep)
        .await
        .unwrap();
}

async fn annotation(storage: &Arc<MemoryStorage>) -> Option<String> {
    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("other"), "foo"))
        .await
        .unwrap();
    ep.metadata
        .annotations
        .and_then(|a| a.get(TRIGGER_TIME_ANNOTATION).cloned())
}

fn trigger_time() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap()
}

/// TestLastTriggerChangeTimeAnnotation: a new Service's creationTimestamp is
/// exported as the trigger time.
#[tokio::test]
async fn test_last_trigger_change_time_annotation() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());
    existing_endpoints(&storage, None).await;
    fixture(&storage, Some(trigger_time())).await;
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-01T00:00:00Z")
    );
}

/// TestLastTriggerChangeTimeAnnotation_AnnotationOverridden: an old value is
/// replaced by the newly computed one.
#[tokio::test]
async fn test_last_trigger_change_time_annotation_overridden() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());
    existing_endpoints(&storage, Some("2017-01-01T00:00:00Z")).await;
    fixture(&storage, Some(trigger_time())).await;
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-01T00:00:00Z")
    );
}

/// TestLastTriggerChangeTimeAnnotation_AnnotationCleared: with no trigger time
/// on the Service or pod the annotation is removed.
#[tokio::test]
async fn test_last_trigger_change_time_annotation_cleared() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());
    existing_endpoints(&storage, Some("2018-01-01T00:00:00Z")).await;
    fixture(&storage, None).await;
    controller.reconcile_all().await.unwrap();
    assert_eq!(annotation(&storage).await, None);
}

/// A pod becoming Ready later exports that Ready transition time as the new
/// trigger (TestPodsUpdated), computed across syncs by the tracker.
#[tokio::test]
async fn test_last_trigger_change_time_tracks_pod_ready_transition() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());
    fixture(&storage, Some(trigger_time())).await;
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-01T00:00:00Z")
    );

    let ready_at = chrono::Utc.with_ymd_and_hms(2018, 1, 2, 3, 4, 5).unwrap();
    let pod_key = build_key("pods", Some("other"), "pod0");
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    let status = pod.status.as_mut().unwrap();
    status.conditions.as_mut().unwrap()[0].last_transition_time = Some(ready_at);
    status.pod_ip = Some("1.2.3.5".to_string());
    storage.update(&pod_key, &pod).await.unwrap();
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-02T03:04:05Z")
    );
}
