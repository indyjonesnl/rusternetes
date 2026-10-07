//! Test double for the kubelet's half of a graceful pod delete.
//!
//! Controllers delete pods the way upstream's `RealPodControl.DeletePod` does
//! (`pkg/controller/controller_utils.go:618`): the pod is stamped with a
//! `deletionTimestamp` and the kubelet removes it once its grace period ends.
//! `MemoryStorage` has no kubelet, so tests that drive `reconcile_all()` by
//! hand and then expect the pod to be gone replay that step here.

use rusternetes_common::resources::Pod;
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Physically delete every pod (any namespace) that carries a
/// `deletionTimestamp`.
pub async fn reap_terminating(storage: &Arc<MemoryStorage>) {
    let pods: Vec<Pod> = storage.list("/registry/pods/").await.unwrap();
    for pod in pods
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_some())
    {
        let key = format!(
            "/registry/pods/{}/{}",
            pod.metadata.namespace.as_deref().unwrap_or("default"),
            pod.metadata.name
        );
        let _ = storage.delete(&key).await;
    }
}

/// `reconcile_all`, let the kubelet reap what was deleted, `reconcile_all`
/// again: the Job controller delays a terminal condition while pods are
/// terminating (`enactJobFinished`, `job_controller.go:1520-1524`).
pub async fn job_reconcile_all_settled(
    controller: &JobController<MemoryStorage>,
    storage: &Arc<MemoryStorage>,
) {
    controller.reconcile_all().await.unwrap();
    reap_terminating(storage).await;
    controller.reconcile_all().await.unwrap();
}
