//! VolumeBinding PreFilter/Filter through the plugin object
//! (`volumebinding/volume_binding.go:360-390`, `:424-453`); the binder's
//! matching logic is covered in `volume_binding.rs`.
use async_trait::async_trait;
use rusternetes_common::resources::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, Handle, NoHandle, NodeInfo, PreFilterPlugin, PreScorePlugin,
    ScorePlugin,
};
use rusternetes_scheduler::in_tree::{
    clear_pod_volumes_by_node, pod_volumes_by_node, VolumeBinding, VolumeBindingScore,
};
use rusternetes_scheduler::profile::Profile;
use rusternetes_scheduler::volume_binding::{default_shape, VolumeSnapshot};
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

    let by_node = pod_volumes_by_node(&state);
    assert_eq!(by_node.keys().collect::<Vec<_>>(), vec!["na"]);
    // A later pass over the nodes starts empty.
    clear_pod_volumes_by_node(&state);
    assert!(pod_volumes_by_node(&state).is_empty());
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

/// `PreScore` (volume_binding.go:456-468): `pl.scorer == nil` (the gate is
/// off) is `Skip`.
#[tokio::test]
#[serial_test::serial]
async fn pre_score_skips_without_storage_capacity_scoring() {
    let _gate = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
        false,
    );
    let s = VolumeBindingScore::new(default_shape())
        .pre_score(
            &mut CycleState::new(),
            &pod_with_pvc("c"),
            &[],
            &[],
            &[],
            &NoHandle,
        )
        .await;
    assert!(s.is_skip());
}

/// With the gate on, a pod whose Filter recorded nothing for the node scores
/// 0 (`podVolumesByNode[nodeName]` absent, :480-483).
#[tokio::test]
#[serial_test::serial]
async fn score_is_zero_for_a_node_without_recorded_volumes() {
    let _gate = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
        true,
    );
    let plugin = VolumeBindingScore::new(default_shape());
    let p = pod_with_pvc("c");
    let mut state = CycleState::new();
    let (_, s) = VolumeBinding
        .pre_filter(&mut state, &p, &[], &Listers(bound_snapshot()))
        .await;
    assert!(s.is_success());
    let s = plugin
        .pre_score(&mut state, &p, &[], &[], &[], &NoHandle)
        .await;
    assert!(s.is_success());
    let pods: Vec<Pod> = vec![];
    let n = node("nb");
    let (score, s) = plugin.score(&state, &p, &NodeInfo::new(&n, &pods));
    assert!(s.is_success());
    assert_eq!(score, 0);
}

/// A handle that records the binder calls VolumeBinding's Reserve, PreBind
/// and Unreserve make.
#[derive(Default)]
struct Binder {
    snapshot: VolumeSnapshot,
    reserve_err: Option<String>,
    all_bound: bool,
    pre_binds: std::sync::atomic::AtomicUsize,
    unreserves: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Handle for Binder {
    async fn volume_binding_snapshot(&self, _ns: &str) -> Result<VolumeSnapshot, String> {
        Ok(self.snapshot.clone())
    }

    fn volume_reserve(
        &self,
        state: &mut rusternetes_scheduler::volume_bind::VolumeBindingState,
        _pod: &Pod,
        _node_name: &str,
    ) -> Result<(), String> {
        state.all_bound = self.all_bound;
        self.reserve_err.clone().map_or(Ok(()), Err)
    }

    async fn volume_pre_bind(
        &self,
        _state: &mut rusternetes_scheduler::volume_bind::VolumeBindingState,
        _pod: &Pod,
        _node_name: &str,
    ) -> Result<(), String> {
        self.pre_binds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn volume_unreserve(
        &self,
        _state: &rusternetes_scheduler::volume_bind::VolumeBindingState,
        _pod: &Pod,
        _node_name: &str,
    ) {
        self.unreserves
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn filtered_state(binder: &Binder, p: &Pod) -> CycleState {
    let mut state = CycleState::new();
    let (_, s) = VolumeBinding.pre_filter(&mut state, p, &[], binder).await;
    assert!(s.is_success());
    let mut a = node("na");
    a.metadata.labels = Some([("zone".to_string(), "a".to_string())].into());
    let pods: Vec<Pod> = vec![];
    assert!(VolumeBinding
        .filter(&state, p, &NodeInfo::new(&a, &pods))
        .await
        .is_success());
    state
}

/// `RunReservePluginsReserve` wraps a plugin error as
/// `running Reserve plugin "<name>": <err>` (framework.go:1623).
#[tokio::test]
async fn reserve_failure_is_wrapped_with_the_plugin_name() {
    let binder = Binder {
        snapshot: bound_snapshot(),
        reserve_err: Some("boom".into()),
        ..Default::default()
    };
    let p = pod_with_pvc("c");
    let state = filtered_state(&binder, &p).await;
    let fw = Profile::named("default-scheduler").framework;
    let s = fw
        .run_reserve_plugins_reserve(&state, &p, "na", &binder)
        .await;
    assert_eq!(s.code(), Code::Error);
    assert_eq!(
        s.message(),
        "running Reserve plugin \"VolumeBinding\": boom"
    );
    fw.run_reserve_plugins_unreserve(&state, &p, "na", &binder)
        .await;
    assert_eq!(
        binder.unreserves.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// `RunPreBindPreFlights` records a Skip (all volumes bound) and
/// `RunPreBindPlugins` then leaves that plugin out (framework.go:1413-1500).
#[tokio::test]
async fn pre_bind_is_skipped_when_every_volume_is_bound() {
    let binder = Binder {
        snapshot: bound_snapshot(),
        all_bound: true,
        ..Default::default()
    };
    let p = pod_with_pvc("c");
    let mut state = filtered_state(&binder, &p).await;
    let fw = Profile::named("default-scheduler").framework;
    assert!(fw
        .run_reserve_plugins_reserve(&state, &p, "na", &binder)
        .await
        .is_success());
    assert!(fw.run_pre_bind_pre_flights(&mut state, &p, "na").is_skip());
    assert!(fw
        .run_pre_bind_plugins(&state, &p, "na", &binder)
        .await
        .is_success());
    assert_eq!(
        binder.pre_binds.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// Not all bound: PreBindPreFlight succeeds and PreBind binds the volumes.
#[tokio::test]
async fn pre_bind_runs_when_volumes_are_not_all_bound() {
    let binder = Binder {
        snapshot: bound_snapshot(),
        all_bound: false,
        ..Default::default()
    };
    let p = pod_with_pvc("c");
    let mut state = filtered_state(&binder, &p).await;
    let fw = Profile::named("default-scheduler").framework;
    assert!(fw
        .run_reserve_plugins_reserve(&state, &p, "na", &binder)
        .await
        .is_success());
    assert!(fw
        .run_pre_bind_pre_flights(&mut state, &p, "na")
        .is_success());
    assert!(fw
        .run_pre_bind_plugins(&state, &p, "na", &binder)
        .await
        .is_success());
    assert_eq!(
        binder.pre_binds.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// A node Filter never recorded is `errNoPodVolumeForNode` at PreBindPreFlight.
#[tokio::test]
async fn pre_bind_pre_flight_errors_for_a_node_without_pod_volumes() {
    let binder = Binder {
        snapshot: bound_snapshot(),
        all_bound: false,
        ..Default::default()
    };
    let p = pod_with_pvc("c");
    let mut state = filtered_state(&binder, &p).await;
    let fw = Profile::named("default-scheduler").framework;
    let s = fw.run_pre_bind_pre_flights(&mut state, &p, "nb");
    assert_eq!(s.code(), Code::Error);
    assert!(
        s.message().contains("no pod volume found for node"),
        "{}",
        s.message()
    );
}

/// A pod without PVCs has no VolumeBinding state: every hook is a no-op.
#[tokio::test]
async fn pods_without_pvcs_pass_through_reserve_and_pre_bind() {
    let binder = Binder::default();
    let p = pod(json!([]));
    let mut state = CycleState::new();
    let fw = Profile::named("default-scheduler").framework;
    assert!(fw
        .run_reserve_plugins_reserve(&state, &p, "na", &binder)
        .await
        .is_success());
    assert!(fw.run_pre_bind_pre_flights(&mut state, &p, "na").is_skip());
    assert!(fw
        .run_pre_bind_plugins(&state, &p, "na", &binder)
        .await
        .is_success());
}
