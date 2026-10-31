//! VolumeBinding PreFilter/Filter through the plugin object
//! (`volumebinding/volume_binding.go:360-390`, `:424-453`); the binder's
//! matching logic is covered in `volume_binding.rs`.
use async_trait::async_trait;
use rusternetes_common::resources::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, Handle, NoHandle, NodeInfo, PreFilterPlugin,
};
use rusternetes_scheduler::in_tree::{
    clear_pod_volumes_by_node, take_pod_volumes_by_node, VolumeBinding,
};
use rusternetes_scheduler::volume_binding::VolumeSnapshot;
use serde_json::json;

struct Listers(VolumeSnapshot);

#[async_trait]
impl Handle for Listers {
    async fn volume_binding_snapshot(&self, _ns: &str) -> Result<VolumeSnapshot, String> {
        Ok(self.0.clone())
    }
}

fn pod(volumes: serde_json::Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "ns"},
        "spec": {"containers": [{"name": "c"}], "volumes": volumes}
    }))
    .unwrap()
}

fn pod_with_pvc(claim: &str) -> Pod {
    pod(json!([{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]))
}

fn node(name: &str) -> Node {
    serde_json::from_value(json!({"metadata": {"name": name}})).unwrap()
}

fn bound_snapshot() -> VolumeSnapshot {
    let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
        "metadata": {"name": "c", "namespace": "ns", "annotations": {"pv.kubernetes.io/bind-completed": "yes"}},
        "spec": {"volumeName": "pv1", "storageClassName": "std", "resources": {"requests": {"storage": "1Gi"}}},
        "status": {"phase": "Bound"}
    }))
    .unwrap();
    let pv: PersistentVolume = serde_json::from_value(json!({
        "metadata": {"name": "pv1"},
        "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                 "storageClassName": "std", "hostPath": {"path": "/x"},
                 "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                     {"key": "zone", "operator": "In", "values": ["a"]}]}]}}},
        "status": {"phase": "Bound"}
    }))
    .unwrap();
    VolumeSnapshot {
        pvcs: vec![pvc],
        pvs: vec![pv],
        ..Default::default()
    }
}

#[tokio::test]
async fn pod_without_pvcs_is_skipped() {
    let (r, s) = VolumeBinding
        .pre_filter(
            &mut CycleState::new(),
            &pod(json!([{"name": "v", "emptyDir": {}}])),
            &[],
            &NoHandle,
        )
        .await;
    assert!(r.is_none() && s.is_skip());
}

#[tokio::test]
async fn missing_pvc_is_unresolvable() {
    let (_, s) = VolumeBinding
        .pre_filter(
            &mut CycleState::new(),
            &pod_with_pvc("gone"),
            &[],
            &Listers(VolumeSnapshot::default()),
        )
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn snapshot_failure_is_an_error() {
    let (_, s) = VolumeBinding
        .pre_filter(&mut CycleState::new(), &pod_with_pvc("c"), &[], &NoHandle)
        .await;
    assert_eq!(s.code(), Code::Error);
}

#[tokio::test]
async fn filter_records_volumes_per_node_and_rejects_on_affinity() {
    let p = pod_with_pvc("c");
    let mut state = CycleState::new();
    let (_, s) = VolumeBinding
        .pre_filter(&mut state, &p, &[], &Listers(bound_snapshot()))
        .await;
    assert!(s.is_success());

    let pods: Vec<Pod> = vec![];
    let mut a = node("na");
    a.metadata.labels = Some([("zone".to_string(), "a".to_string())].into());
    let b = node("nb");
    assert!(VolumeBinding
        .filter(&state, &p, &NodeInfo::new(&a, &pods))
        .await
        .is_success());
    let s = VolumeBinding
        .filter(&state, &p, &NodeInfo::new(&b, &pods))
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert!(!s.reasons().is_empty());

    let by_node = take_pod_volumes_by_node(&state);
    assert_eq!(by_node.keys().collect::<Vec<_>>(), vec!["na"]);
    // Taking drains it; a later pass starts empty either way.
    assert!(take_pod_volumes_by_node(&state).is_empty());
    clear_pod_volumes_by_node(&state);
}

#[tokio::test]
async fn filter_without_pre_filter_state_is_an_error() {
    let n = node("n");
    let pods: Vec<Pod> = vec![];
    let s = VolumeBinding
        .filter(
            &CycleState::new(),
            &pod_with_pvc("c"),
            &NodeInfo::new(&n, &pods),
        )
        .await;
    assert_eq!(s.code(), Code::Error);
}
