//! EndpointSlice `endpoints.kubernetes.io/last-change-trigger-time` export.
//!
//! Ports of:
//! - TestSyncServiceFull's `NotEmpty(slice.Annotations[
//!   EndpointsLastChangeTriggerTime])`
//!   (pkg/controller/endpointslice/endpointslice_controller_test.go:375) and the
//!   creationTimestamp assertion (:1309);
//! - `addTriggerTimeAnnotation` (staging/src/k8s.io/endpointslice/utils.go:187,
//!   clear on a zero trigger time), reached from reconciler.go:444,459.

use super::endpoints_controller_test::{create_test_pod, create_test_service};
use chrono::TimeZone;
use rusternetes_common::resources::{EndpointSlice, Pod};
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_storage::{build_key, build_prefix, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

const ANN: &str = "endpoints.kubernetes.io/last-change-trigger-time";

fn selector() -> HashMap<String, String> {
    HashMap::from([("app".to_string(), "web".to_string())])
}

fn created_at() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap()
}

async fn fixture(storage: &Arc<MemoryStorage>) {
    let mut service = create_test_service("foo", "other", selector());
    service.metadata.creation_timestamp = Some(created_at());
    storage
        .create(&build_key("services", Some("other"), "foo"), &service)
        .await
        .unwrap();
    let pod = create_test_pod("pod0", "other", selector(), Some("1.2.3.4".into()), true);
    storage
        .create(&build_key("pods", Some("other"), "pod0"), &pod)
        .await
        .unwrap();
}

async fn slices(storage: &Arc<MemoryStorage>) -> Vec<EndpointSlice> {
    storage
        .list::<EndpointSlice>(&build_prefix("endpointslices", Some("other")))
        .await
        .unwrap()
}

async fn annotation(storage: &Arc<MemoryStorage>) -> Option<String> {
    let all = slices(storage).await;
    assert_eq!(all.len(), 1);
    all[0]
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN).cloned())
}

/// A new Service's creationTimestamp is exported on the created slice
/// (endpointslice_controller_test.go:1309, trigger_time_tracker.go:117-120).
#[tokio::test]
async fn created_slice_carries_service_creation_time() {
    let storage = Arc::new(MemoryStorage::new());
    fixture(&storage).await;
    let c = EndpointSliceController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-01T00:00:00Z")
    );
}

/// A pod becoming Ready later exports its Ready transition time on the updated
/// slice (tracker state persists across syncs, controller.go:443).
#[tokio::test]
async fn updated_slice_carries_pod_ready_transition_time() {
    let storage = Arc::new(MemoryStorage::new());
    fixture(&storage).await;
    let c = EndpointSliceController::new(storage.clone());
    c.reconcile_all().await.unwrap();

    let ready_at = chrono::Utc.with_ymd_and_hms(2018, 1, 2, 3, 4, 5).unwrap();
    let pod_key = build_key("pods", Some("other"), "pod0");
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    let status = pod.status.as_mut().unwrap();
    status.conditions.as_mut().unwrap()[0].last_transition_time = Some(ready_at);
    status.pod_ip = Some("1.2.3.5".to_string());
    storage.update(&pod_key, &pod).await.unwrap();
    c.reconcile_all().await.unwrap();
    assert_eq!(
        annotation(&storage).await.as_deref(),
        Some("2018-01-02T03:04:05Z")
    );
}

/// No new trigger time on a (non-no-op) update clears the annotation
/// (utils.go:194-195).
#[tokio::test]
async fn update_without_new_trigger_time_clears_annotation() {
    let storage = Arc::new(MemoryStorage::new());
    fixture(&storage).await;
    let c = EndpointSliceController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert!(annotation(&storage).await.is_some());

    let pod = create_test_pod("pod1", "other", selector(), Some("1.2.3.5".into()), true);
    storage
        .create(&build_key("pods", Some("other"), "pod1"), &pod)
        .await
        .unwrap();
    c.reconcile_all().await.unwrap();
    let all = slices(&storage).await;
    assert_eq!(all[0].endpoints.len(), 2);
    assert_eq!(annotation(&storage).await, None);
}
