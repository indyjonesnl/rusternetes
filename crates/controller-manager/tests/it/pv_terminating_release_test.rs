//! Regression for #4367: a volume deleted right after its claim must still be
//! marked Released. Upstream's `syncVolume`
//! (`pkg/controller/volume/persistentvolume/pv_controller.go:556-770`) never
//! consults DeletionTimestamp, and pv-protection
//! (`pvprotection/pv_protection_controller.go` `isBeingUsed`) keeps a Bound
//! volume's finalizer, so skipping terminating volumes left the PV forever.

use rusternetes_common::resources::volume::{PersistentVolume, PersistentVolumePhase};
use rusternetes_controller_manager::controllers::pv_binder::PVBinderController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WorkQueue};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn terminating_volume_with_deleted_claim_is_released() {
    let storage = Arc::new(MemoryStorage::new());
    let key = build_key("persistentvolumes", None, "pv1");
    let pv: PersistentVolume = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv1", "uid": "pu",
            "finalizers": ["kubernetes.io/pv-protection"],
            "deletionTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "hostPath": {"path": "/tmp/pv1"},
            "claimRef": {"namespace": "ns", "name": "gone", "uid": "cu"}
        },
        "status": {"phase": "Bound"}
    }))
    .unwrap();
    storage.create(&key, &pv).await.unwrap();

    PVBinderController::new(storage.clone())
        .sync_volumes(&WorkQueue::new())
        .await
        .unwrap();

    let got: PersistentVolume = storage.get(&key).await.unwrap();
    assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
}
