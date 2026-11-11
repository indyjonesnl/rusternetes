//! The PV controller drains one queue per kind, `claims` and `volumes`
//! (`pkg/controller/volume/persistentvolume/pv_controller_base.go:91-92`), and
//! a deleted claim / volume wakes the other side (`deleteClaim` :273-295,
//! `deleteVolume` :228-243).

use k3rs_common::resources::volume::{PersistentVolume, PersistentVolumePhase};
use k3rs_controller_manager::controllers::pv_binder::{
    deleted_claim_volume_key, deleted_volume_claim_key, PVBinderController,
};
use k3rs_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn pv(name: &str, claim_ref: Option<serde_json::Value>, phase: &str) -> PersistentVolume {
    let mut spec = json!({
        "capacity": {"storage": "1Gi"},
        "accessModes": ["ReadWriteOnce"],
        "persistentVolumeReclaimPolicy": "Retain",
        "hostPath": {"path": "/tmp/pv"}
    });
    if let Some(c) = claim_ref {
        spec["claimRef"] = c;
    }
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": name, "uid": format!("uid-{name}")},
        "spec": spec,
        "status": {"phase": phase}
    }))
    .unwrap()
}

async fn phase_of(storage: &MemoryStorage, name: &str) -> PersistentVolumePhase {
    let got: PersistentVolume = storage
        .get(&build_key("persistentvolumes", None, name))
        .await
        .unwrap();
    got.status.unwrap().phase
}

async fn wait_for_phase(storage: &MemoryStorage, name: &str, want: PersistentVolumePhase) {
    for _ in 0..100 {
        if phase_of(storage, name).await == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "{name} never reached {want:?}, is {:?}",
        phase_of(storage, name).await
    );
}

#[tokio::test]
async fn volume_worker_syncs_a_volume_event() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = Arc::new(PVBinderController::new(storage.clone()));
    tokio::spawn(controller.run());
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A volume with no claimRef becomes Available (`syncVolume` :576-596).
    storage
        .create(
            &build_key("persistentvolumes", None, "free"),
            &pv("free", None, "Pending"),
        )
        .await
        .unwrap();
    wait_for_phase(&storage, "free", PersistentVolumePhase::Available).await;
}

#[tokio::test]
async fn volume_bound_to_a_deleted_claim_is_released_without_the_resync() {
    let storage = Arc::new(MemoryStorage::new());
    // The claim is already gone; the volume still points at it.
    storage
        .create(
            &build_key("persistentvolumes", None, "orphan"),
            &pv(
                "orphan",
                Some(json!({"namespace": "ns", "name": "gone", "uid": "cu"})),
                "Bound",
            ),
        )
        .await
        .unwrap();
    let controller = Arc::new(PVBinderController::new(storage.clone()));
    tokio::spawn(controller.run());
    wait_for_phase(&storage, "orphan", PersistentVolumePhase::Released).await;
}

#[test]
fn deleted_claim_names_its_bound_volume() {
    assert_eq!(
        deleted_claim_volume_key(r#"{"spec":{"volumeName":"pv1"}}"#).as_deref(),
        Some("persistentvolumes/pv1")
    );
    assert_eq!(
        deleted_claim_volume_key(r#"{"spec":{"volumeName":""}}"#),
        None
    );
    assert_eq!(deleted_claim_volume_key(r#"{"spec":{}}"#), None);
    assert_eq!(deleted_claim_volume_key("not json"), None);
}

#[test]
fn deleted_volume_names_its_claim() {
    assert_eq!(
        deleted_volume_claim_key(r#"{"spec":{"claimRef":{"namespace":"ns","name":"c"}}}"#)
            .as_deref(),
        Some("persistentvolumeclaims/ns/c")
    );
    assert_eq!(deleted_volume_claim_key(r#"{"spec":{}}"#), None);
}
