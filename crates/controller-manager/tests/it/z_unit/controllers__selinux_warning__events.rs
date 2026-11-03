//! The informer event handlers of the SELinux warning controller:
//! `updatePod`, `updatePVC`, `addPV`/`updatePV` and their add counterparts
//! (`selinux_warning_controller.go:182-283`). Upstream's test file
//! (`selinux_warning_controller_test.go`) has only `TestSELinuxWarningController_Sync`
//! and no test of these predicates, so these pin the conditions read from
//! the handlers.

use rusternetes_common::types::Phase;
use rusternetes_controller_manager::controllers::selinux_warning::*;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{WatchEvent, WorkQueue};
use serde_json::{json, Value};
use std::sync::Arc;

#[test]
fn pod_update_only_on_phase_change_to_final() {
    use Phase::*;
    // updatePod: unchanged phase, and a change to a non-final phase, are ignored.
    assert!(!pod_update_enqueues(Some(&Running), Some(&Running)));
    assert!(!pod_update_enqueues(Some(&Failed), Some(&Failed)));
    assert!(!pod_update_enqueues(Some(&Pending), Some(&Running)));
    assert!(!pod_update_enqueues(Some(&Running), Some(&Unknown)));
    assert!(!pod_update_enqueues(None, None));
    assert!(pod_update_enqueues(Some(&Running), Some(&Succeeded)));
    assert!(pod_update_enqueues(Some(&Pending), Some(&Failed)));
    assert!(pod_update_enqueues(None, Some(&Failed)));
}

#[test]
fn pvc_update_only_when_volume_name_changed() {
    assert!(!pvc_update_enqueues(Some("pv1"), Some("pv1")));
    assert!(!pvc_update_enqueues(None, Some("")));
    assert!(pvc_update_enqueues(Some(""), Some("pv1")));
    assert!(pvc_update_enqueues(None, Some("pv1")));
    assert!(pvc_update_enqueues(Some("pv1"), Some("pv2")));
    // addPVC ignores unbound claims.
    assert!(!pvc_add_enqueues(None));
    assert!(!pvc_add_enqueues(Some("")));
    assert!(pvc_add_enqueues(Some("pv1")));
}

#[test]
fn pv_update_only_when_bound_or_rebound() {
    // addPV: nil claimRef and an incomplete binding (no UID) are ignored.
    assert!(!pv_add_enqueues(None));
    assert!(!pv_add_enqueues(Some("")));
    assert!(pv_add_enqueues(Some("u1")));
    // updatePV: new claimRef nil or incomplete.
    assert!(!pv_update_enqueues(Some("u1"), None));
    assert!(!pv_update_enqueues(None, Some("")));
    // unchanged UID: a status-only update.
    assert!(!pv_update_enqueues(Some("u1"), Some("u1")));
    // just bound (old claimRef nil or incomplete UID differs) or re-bound.
    assert!(pv_update_enqueues(None, Some("u1")));
    assert!(pv_update_enqueues(Some(""), Some("u1")));
    assert!(pv_update_enqueues(Some("u1"), Some("u2")));
}

fn pod(name: &str, phase: &str, claim: Option<&str>) -> Value {
    let mut v = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"namespace": "ns1", "name": name},
        "spec": {"containers": [{"name": "c", "image": "i"}]},
        "status": {"phase": phase},
    });
    if let Some(c) = claim {
        v["spec"]["volumes"] = json!([{"name": "v", "persistentVolumeClaim": {"claimName": c}}]);
    }
    v
}

fn pod_ev(kind: &str, name: &str, phase: &str, claim: Option<&str>) -> WatchEvent {
    let key = format!("/registry/pods/ns1/{name}");
    let val = pod(name, phase, claim).to_string();
    match kind {
        "add" => WatchEvent::Added(key, val),
        "mod" => WatchEvent::Modified(key, val),
        _ => WatchEvent::Deleted(key, val),
    }
}

fn pvc_ev(kind: &str, volume: Option<&str>) -> WatchEvent {
    let mut v = json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": "claim", "namespace": "ns1"},
        "spec": {"accessModes": ["ReadWriteOnce"]},
    });
    if let Some(n) = volume {
        v["spec"]["volumeName"] = json!(n);
    }
    let key = "/registry/persistentvolumeclaims/ns1/claim".to_string();
    match kind {
        "add" => WatchEvent::Added(key, v.to_string()),
        _ => WatchEvent::Modified(key, v.to_string()),
    }
}

fn pv_ev(kind: &str, uid: Option<&str>, labels: &str) -> WatchEvent {
    let mut v = json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv1", "labels": {"x": labels}},
        "spec": {"awsElasticBlockStore": {"volumeID": "pv1"}},
    });
    if let Some(u) = uid {
        v["spec"]["claimRef"] = json!({
            "kind": "PersistentVolumeClaim", "namespace": "ns1",
            "name": "claim", "uid": u, "apiVersion": "v1",
        });
    }
    let key = "/registry/persistentvolumes/pv1".to_string();
    match kind {
        "add" => WatchEvent::Added(key, v.to_string()),
        _ => WatchEvent::Modified(key, v.to_string()),
    }
}

async fn queued(q: &WorkQueue) -> Vec<String> {
    let mut out = vec![];
    while q.len().await > 0 {
        let k = q.get().await.unwrap();
        q.done(&k).await;
        out.push(k);
    }
    out.sort();
    out
}

#[tokio::test]
async fn pod_status_only_update_is_not_queued() {
    let c = SELinuxWarningController::new(Arc::new(MemoryStorage::new()));
    let q = WorkQueue::new();
    c.on_pod_event(&q, &pod_ev("add", "p1", "Running", None))
        .await;
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]); // enqueuePod on add
                                                   // A status-only update (same phase) used to be queued every time.
    c.on_pod_event(&q, &pod_ev("mod", "p1", "Running", None))
        .await;
    assert!(queued(&q).await.is_empty());
    // Running -> Succeeded is queued; so is the delete.
    c.on_pod_event(&q, &pod_ev("mod", "p1", "Succeeded", None))
        .await;
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
    c.on_pod_event(&q, &pod_ev("del", "p1", "Succeeded", None))
        .await;
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
}

#[tokio::test]
async fn pvc_modification_without_volume_name_change_is_not_queued() {
    let q = WorkQueue::new();
    let s = Arc::new(MemoryStorage::new());
    let c2 = SELinuxWarningController::new(s.clone());
    use rusternetes_storage::Storage;
    let pod_json: rusternetes_common::resources::Pod =
        serde_json::from_value(pod("p1", "Running", Some("claim"))).unwrap();
    s.create(
        &rusternetes_storage::build_key("pods", Some("ns1"), "p1"),
        &pod_json,
    )
    .await
    .unwrap();
    // Added bound PVC: addPVC queues the pods that use it.
    c2.on_pvc_event(&q, &pvc_ev("add", Some("pv1"))).await;
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
    // Any later modification of the bound PVC (same volumeName): not queued.
    c2.on_pvc_event(&q, &pvc_ev("mod", Some("pv1"))).await;
    assert!(queued(&q).await.is_empty());
    // Rebinding is.
    c2.on_pvc_event(&q, &pvc_ev("mod", Some("pv2"))).await;
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
}

#[tokio::test]
async fn pv_modification_with_same_claim_uid_is_not_queued() {
    use rusternetes_storage::Storage;
    let s = Arc::new(MemoryStorage::new());
    let c = SELinuxWarningController::new(s.clone());
    let q = WorkQueue::new();
    let pod_json: rusternetes_common::resources::Pod =
        serde_json::from_value(pod("p1", "Running", Some("claim"))).unwrap();
    s.create(
        &rusternetes_storage::build_key("pods", Some("ns1"), "p1"),
        &pod_json,
    )
    .await
    .unwrap();
    c.on_pv_event(&q, &pv_ev("add", None, "a")).await; // unbound: ignored
    assert!(queued(&q).await.is_empty());
    c.on_pv_event(&q, &pv_ev("mod", Some("u1"), "a")).await; // just bound
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
    c.on_pv_event(&q, &pv_ev("mod", Some("u1"), "b")).await; // unrelated change
    assert!(queued(&q).await.is_empty());
    c.on_pv_event(&q, &pv_ev("mod", Some("u2"), "b")).await; // re-bound
    assert_eq!(queued(&q).await, ["pods/ns1/p1"]);
}
