//! Ports of `pkg/kubelet/volumemanager/populator/desired_state_of_world_populator_test.go`.
//! Each test keeps the name of the Go test it came from.
//!
//! The upstream fixtures run over `volumetesting.FakeVolumePlugin`; these run
//! over the real registry of the kubelet's volume manager, with a hostPath PV
//! standing in for the fake plugin's PV and an in-memory storage backend for
//! the fake clientset.

use rusternetes_common::resources::Pod;
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use rusternetes_kubelet::volume_manager::populator::{
    DesiredStateOfWorldPopulator, PodStateProvider,
};
use rusternetes_kubelet::volume_plugins::util::get_unique_pod_name;
use rusternetes_kubelet::volume_plugins::util::operation_executor::MarkVolumeOpts;
use rusternetes_kubelet::volume_plugins::util::selinux::Translator;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `kubepod.NewBasicPodManager()` as far as the populator reads it.
#[derive(Default)]
struct FakePodManager {
    pods: Mutex<Vec<Pod>>,
}

impl FakePodManager {
    fn add_pod(&self, pod: &Pod) {
        self.pods.lock().unwrap().push(pod.clone());
    }
    fn remove_pod(&self, pod: &Pod) {
        self.pods
            .lock()
            .unwrap()
            .retain(|p| p.metadata.uid != pod.metadata.uid);
    }
    fn set_phase(&self, uid: &str, phase: &str) {
        for p in self.pods.lock().unwrap().iter_mut() {
            if p.metadata.uid == uid {
                p.status = Some(serde_json::from_value(json!({ "phase": phase })).unwrap());
            }
        }
    }
}

impl PodManager for FakePodManager {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod> {
        self.pods
            .lock()
            .unwrap()
            .iter()
            .find(|p| p.metadata.uid == uid)
            .cloned()
    }
    fn get_pods(&self) -> Vec<Pod> {
        self.pods.lock().unwrap().clone()
    }
}

/// `fakePodStateProvider` (`desired_state_of_world_populator_test.go:1568-1586`).
#[derive(Default)]
struct FakePodStateProvider {
    terminating: Mutex<HashSet<String>>,
    removed: Mutex<HashSet<String>>,
}

impl PodStateProvider for FakePodStateProvider {
    fn should_pod_containers_be_terminating(&self, uid: &str) -> bool {
        self.removed.lock().unwrap().contains(uid) || self.terminating.lock().unwrap().contains(uid)
    }
    fn should_pod_runtime_be_removed(&self, uid: &str) -> bool {
        self.removed.lock().unwrap().contains(uid)
    }
}

struct Fixture {
    dswp: DesiredStateOfWorldPopulator,
    dsw: Arc<DesiredStateOfWorld>,
    asw: Arc<ActualStateOfWorld>,
    pods: Arc<FakePodManager>,
    state: Arc<FakePodStateProvider>,
    storage: Arc<StorageBackend>,
    _dir: tempfile::TempDir,
}

fn pv_json(mode: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "dswp-test-volume-name"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "volumeMode": mode,
            "claimRef": {"namespace": "ns", "name": "file-bound", "uid": "pvc-uid"},
            "hostPath": {"path": "/tmp/dswp-test"}
        }
    })
}

fn pvc_json() -> serde_json::Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": "file-bound", "namespace": "ns", "uid": "pvc-uid"},
        "spec": {"volumeName": "dswp-test-volume-name", "accessModes": ["ReadWriteOnce"]},
        "status": {"phase": "Bound"}
    })
}

/// `createDswpWithVolume`.
async fn fixture(pv: serde_json::Value, pvc: Option<serde_json::Value>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(StorageBackend::new_memory());
    let spec_factory = Arc::new(rusternetes_kubelet::volumes::VolumeManager::new(
        dir.path().display().to_string(),
        Some(storage.clone()),
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
    ));
    storage
        .create(
            &build_key("persistentvolumes", None, "dswp-test-volume-name"),
            &pv,
        )
        .await
        .unwrap();
    if let Some(pvc) = pvc {
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("ns"), "file-bound"),
                &pvc,
            )
            .await
            .unwrap();
    }
    let plugin_mgr = spec_factory.plugin_mgr.clone();
    let dsw = Arc::new(DesiredStateOfWorld::new(
        plugin_mgr.clone(),
        Arc::new(Translator),
    ));
    let asw = Arc::new(ActualStateOfWorld::new("node-test", plugin_mgr.clone()));
    let pods = Arc::new(FakePodManager::default());
    let state = Arc::new(FakePodStateProvider::default());
    let dswp = DesiredStateOfWorldPopulator::new(
        storage.clone(),
        Duration::from_millis(100),
        pods.clone(),
        state.clone(),
        dsw.clone(),
        asw.clone(),
        spec_factory,
        plugin_mgr,
    );
    Fixture {
        dswp,
        dsw,
        asw,
        pods,
        state,
        storage,
        _dir: dir,
    }
}

/// `createPodWithVolume`.
fn pod_with_volume(pod: &str, pv_name: &str, pvc: &str, container_extra: serde_json::Value) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": pod, "namespace": "ns", "uid": "dswp-test-pod-uid"},
        "spec": {
            "nodeName": "node-test",
            "containers": [{"name": "c", "image": "busybox", "volumeMounts": container_extra.get("volumeMounts").cloned().unwrap_or(json!([])), "volumeDevices": container_extra.get("volumeDevices").cloned().unwrap_or(json!([]))}],
            "volumes": [{"name": pv_name, "persistentVolumeClaim": {"claimName": pvc}}]
        }
    }))
    .unwrap()
}

fn mounts() -> serde_json::Value {
    json!({"volumeMounts": [{"name": "dswp-test-volume-name", "mountPath": "/mnt"}]})
}

fn devices() -> serde_json::Value {
    json!({"volumeDevices": [{"name": "dswp-test-volume-name", "devicePath": "/dev/x"}]})
}

/// `pluginPVOmittingClient`: the PVC and PV can no longer be fetched.
async fn omit_pvc_and_pv(f: &Fixture) {
    f.storage
        .delete(&build_key(
            "persistentvolumeclaims",
            Some("ns"),
            "file-bound",
        ))
        .await
        .unwrap();
    f.storage
        .delete(&build_key(
            "persistentvolumes",
            None,
            "dswp-test-volume-name",
        ))
        .await
        .unwrap();
}

/// `prepareDSWPWithPodPV`: a pod with a file PV added to the desired state.
async fn prepare_with_pod_pv() -> (
    Fixture,
    Pod,
    rusternetes_kubelet::volume_plugins::util::types::UniqueVolumeName,
) {
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let volumes = f.dsw.get_volumes_to_mount();
    assert_eq!(
        volumes.len(),
        1,
        "the volume was added to the desired state"
    );
    let name = volumes[0].volume_name.clone();
    (f, pod, name)
}

/// `reconcileASW`: what the reconciler's `MountVolume` leaves behind.
fn reconcile_asw(f: &Fixture) {
    for v in f.dsw.get_volumes_to_mount() {
        f.asw
            .mark_volume_as_attached(Some(&v.volume_name), v.volume_spec.clone(), "node-test", "")
            .unwrap();
        f.asw
            .mark_volume_as_mounted(MarkVolumeOpts {
                pod_name: v.pod_name.clone(),
                pod_uid: v.pod.metadata.uid.clone(),
                volume_name: v.volume_name.clone(),
                mounter: None,
                block_volume_mapper: None,
                volume_gid_volume: String::new(),
                volume_spec: v.volume_spec.clone(),
                volume_mount_state: rusternetes_kubelet::volume_plugins::util::operation_executor::VolumeMountState::VolumeMounted,
                selinux_mount_context: String::new(),
            })
            .unwrap();
    }
}

#[tokio::test]
async fn find_and_add_new_pods_with_different_conditions() {
    // (hasAddedPods, podState, expectedFound)
    let cases = [
        (false, "Removed", false),
        (false, "Terminating", true),
        (false, "Other", true),
        (true, "Terminating", false),
        (true, "Removed", false),
        (true, "Other", true),
    ];
    for (has_added_pods, pod_state, expected_found) in cases {
        let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
        let pod = pod_with_volume(
            "dswp-test-pod",
            "dswp-test-volume-name",
            "file-bound",
            mounts(),
        );
        f.pods.add_pod(&pod);
        match pod_state {
            "Removed" => {
                f.state
                    .removed
                    .lock()
                    .unwrap()
                    .insert(pod.metadata.uid.clone());
            }
            "Terminating" => {
                f.state
                    .terminating
                    .lock()
                    .unwrap()
                    .insert(pod.metadata.uid.clone());
            }
            _ => {}
        }
        f.dswp.set_has_added_pods(has_added_pods);
        f.dswp.find_and_add_new_pods().await;
        let found = f.dsw.get_pods().contains(&get_unique_pod_name(&pod));
        assert_eq!(
            found, expected_found,
            "hasAddedPods={has_added_pods} state={pod_state}"
        );
    }
}

#[tokio::test]
async fn find_and_add_new_pods_with_reprocess_pod_and_volume_retrieval_error() {
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    let pod_name = get_unique_pod_name(&pod);

    f.dswp.find_and_add_new_pods().await;
    assert!(f.dswp.pod_previously_processed(&pod_name));

    omit_pvc_and_pv(&f).await;
    f.dswp.reprocess_pod(&pod_name);
    f.dswp.find_and_add_new_pods().await;

    // Seen once, so it is still marked processed despite the retrieval error.
    assert!(f.dswp.pod_previously_processed(&pod_name));
    f.pods.remove_pod(&pod);
}

#[tokio::test]
async fn find_and_add_new_pods_with_volume_retrieval_error() {
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    omit_pvc_and_pv(&f).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    let pod_name = get_unique_pod_name(&pod);

    f.dswp.find_and_add_new_pods().await;

    assert!(!f.dswp.pod_previously_processed(&pod_name));
    assert!(!f.dswp.pod_has_been_seen_once(&pod_name));
    // The failure is recorded against the pod for the volume manager to report.
    let errors = f.dsw.pop_pod_errors(&pod_name);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("failed to fetch PVC from API server")),
        "{errors:?}"
    );
}

#[tokio::test]
async fn find_and_add_new_pods_find_and_remove_deleted_pods() {
    let (f, pod, volume_name) = prepare_with_pod_pv().await;
    let pod_name = get_unique_pod_name(&pod);

    // let the pod be terminated
    f.pods.set_phase(&pod.metadata.uid, "Failed");
    f.pods.remove_pod(&pod);

    f.dswp.find_and_remove_deleted_pods();
    assert!(
        f.dswp.pod_previously_processed(&pod_name),
        "Pod should not been removed from desired state of world since pod state still thinks it exists"
    );

    f.state
        .removed
        .lock()
        .unwrap()
        .insert(pod.metadata.uid.clone());

    // the pod state is marked as removed, so here find_and_remove_deleted_pods
    // will remove the pod and the volumes it mounted
    f.dswp.find_and_remove_deleted_pods();
    assert!(
        !f.dswp.pod_previously_processed(&pod_name),
        "Failed to remove pods from desired state of world since they no longer exist"
    );

    // podWorker may call volume_manager WaitForUnmount() after we processed the pod
    f.dswp.reprocess_pod(&pod_name);
    f.dswp.find_and_remove_deleted_pods();

    // find_and_remove_deleted_pods must detect the orphaned pod and delete it from the map
    assert!(
        !f.dswp.pod_has_been_seen_once(&pod_name),
        "Failed to remove orphaned pods from internal map"
    );
    assert!(!f.dsw.volume_exists(&volume_name, ""));
    assert!(!f.dsw.pod_exists_in_volume(&pod_name, &volume_name, ""));
    assert!(f
        .dsw
        .get_volumes_to_mount()
        .iter()
        .all(|v| v.volume_name != volume_name));
}

#[tokio::test]
async fn find_and_remove_deleted_pods_with_actual_state() {
    let (f, pod, volume_name) = prepare_with_pod_pv().await;
    let pod_name = get_unique_pod_name(&pod);

    // let the pod be terminated
    f.pods.set_phase(&pod.metadata.uid, "Failed");

    f.dswp.find_and_remove_deleted_pods();
    // Although the pod status is terminated, the pod still exists in the pod
    // manager and the actual state has no information about this volume.
    assert!(f.dsw.volume_exists(&volume_name, ""));
    assert!(f.dsw.pod_exists_in_volume(&pod_name, &volume_name, ""));

    // The pod state still thinks the runtime is there.
    reconcile_asw(&f);
    f.dswp.find_and_remove_deleted_pods();
    assert!(f.dsw.volume_exists(&volume_name, ""));

    f.state
        .removed
        .lock()
        .unwrap()
        .insert(pod.metadata.uid.clone());

    // The volume is in the actual state, the pod still exists in the pod
    // manager, and its runtime is removed: now the populator deletes it.
    reconcile_asw(&f);
    f.dswp.find_and_remove_deleted_pods();
    assert!(!f.dsw.volume_exists(&volume_name, ""));
    assert!(!f.dsw.pod_exists_in_volume(&pod_name, &volume_name, ""));
}

#[tokio::test]
async fn find_and_remove_deleted_pods_with_uncertain() {
    let (f, pod, volume_name) = prepare_with_pod_pv().await;
    let pod_name = get_unique_pod_name(&pod);

    // let the pod be terminated
    f.pods.set_phase(&pod.metadata.uid, "Failed");
    f.pods.remove_pod(&pod);
    f.state
        .removed
        .lock()
        .unwrap()
        .insert(pod.metadata.uid.clone());

    // Add the volume to the ASW by reconciling, then mark it uncertain.
    reconcile_asw(&f);
    let v = f.dsw.get_volumes_to_mount().remove(0);
    f.asw
        .mark_volume_mount_as_uncertain(MarkVolumeOpts {
            pod_name: pod_name.clone(),
            pod_uid: pod.metadata.uid.clone(),
            volume_name: volume_name.clone(),
            mounter: None,
            block_volume_mapper: None,
            volume_gid_volume: String::new(),
            volume_spec: v.volume_spec.clone(),
            volume_mount_state: rusternetes_kubelet::volume_plugins::util::operation_executor::VolumeMountState::VolumeMountUncertain,
            selinux_mount_context: String::new(),
        })
        .unwrap();

    f.dswp.find_and_remove_deleted_pods();
    // The volume is uncertain in the ASW, but the pod is gone and its runtime
    // removed: the desired state drops it so the reconciler unmounts it.
    assert!(!f.dsw.volume_exists(&volume_name, ""));
    assert!(!f.dsw.pod_exists_in_volume(&pod_name, &volume_name, ""));
}

#[tokio::test]
async fn create_volume_spec_valid_file_volume_mounts() {
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    assert!(f.dswp.pod_previously_processed(&get_unique_pod_name(&pod)));
    assert_eq!(f.dsw.get_volumes_to_mount().len(), 1);
}

#[tokio::test]
async fn create_volume_spec_valid_nil_volume_mounts() {
    // A volume no container mounts is skipped, not an error.
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        json!({}),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    assert!(f.dswp.pod_previously_processed(&get_unique_pod_name(&pod)));
    assert!(f.dsw.get_volumes_to_mount().is_empty());
}

#[tokio::test]
async fn create_volume_spec_invalid_file_volume_devices() {
    let f = fixture(pv_json("Filesystem"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        devices(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let pod_name = get_unique_pod_name(&pod);
    assert!(!f.dswp.pod_previously_processed(&pod_name));
    assert!(f.dsw.pop_pod_errors(&pod_name).iter().any(|e| e
        == "volume dswp-test-volume-name has volumeMode Filesystem, but is specified in volumeDevices"));
}

#[tokio::test]
async fn create_volume_spec_invalid_block_volume_mounts() {
    let f = fixture(pv_json("Block"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let pod_name = get_unique_pod_name(&pod);
    assert!(!f.dswp.pod_previously_processed(&pod_name));
    assert!(f.dsw.pop_pod_errors(&pod_name).iter().any(|e| e
        == "volume dswp-test-volume-name has volumeMode Block, but is specified in volumeMounts"));
}

#[tokio::test]
async fn find_and_add_new_pods_valid_block_volume_devices() {
    let f = fixture(pv_json("Block"), Some(pvc_json())).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        devices(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    assert!(f.dswp.pod_previously_processed(&get_unique_pod_name(&pod)));
    assert_eq!(f.dsw.get_volumes_to_mount().len(), 1);
}

/// `getPVCExtractPV` / `getPVSpec` error paths (`:505-574`).
#[tokio::test]
async fn pvc_and_pv_validation_errors() {
    // PVC not bound.
    let mut pvc = pvc_json();
    pvc["status"]["phase"] = json!("Pending");
    let f = fixture(pv_json("Filesystem"), Some(pvc)).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let pod_name = get_unique_pod_name(&pod);
    assert_eq!(
        f.dsw.pop_pod_errors(&pod_name),
        vec!["error processing PVC ns/file-bound: PVC is not bound".to_string()]
    );

    // PV bound to a different claim UID.
    let mut pv = pv_json("Filesystem");
    pv["spec"]["claimRef"]["uid"] = json!("someone-else");
    let f = fixture(pv, Some(pvc_json())).await;
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let errors = f.dsw.pop_pod_errors(&pod_name);
    assert!(
        errors.iter().any(|e| e.contains(
            "found PV object dswp-test-volume-name but its pv.Spec.ClaimRef.UID someone-else does not point to claim.UID pvc-uid"
        )),
        "{errors:?}"
    );
}

/// `TestCheckVolumeFSResize` (`:972`), the read-only and PVC-without-capacity
/// arms: the desired size comes from the PV, the claim size from the PVC status.
#[tokio::test]
async fn check_volume_fs_resize_records_sizes() {
    let mut pvc = pvc_json();
    pvc["status"]["capacity"] = json!({"storage": "1Gi"});
    let mut pv = pv_json("Filesystem");
    pv["spec"]["capacity"] = json!({"storage": "2Gi"});
    let f = fixture(pv, Some(pvc)).await;
    let pod = pod_with_volume(
        "dswp-test-pod",
        "dswp-test-volume-name",
        "file-bound",
        mounts(),
    );
    f.pods.add_pod(&pod);
    f.dswp.find_and_add_new_pods().await;
    let v = f.dsw.get_volumes_to_mount().remove(0);
    let desired = v.desired_persistent_volume_size.expect("desired size");
    assert_eq!(desired.canonical_string(), "2Gi");
    // The actual state's claim size is initialized once the volume is attached.
    f.asw
        .mark_volume_as_attached(Some(&v.volume_name), v.volume_spec.clone(), "node-test", "")
        .unwrap();
    f.dswp.reprocess_pod(&get_unique_pod_name(&pod));
    f.dswp.find_and_add_new_pods().await;
    assert_eq!(
        f.asw
            .get_claim_size(&v.volume_name)
            .unwrap()
            .canonical_string(),
        "1Gi"
    );
}

/// The kubelet's `PodStateProvider` (`pod_workers.go:685-707`).
mod kubelet_pod_state_provider {
    use super::*;
    use rusternetes_kubelet::kubelet::{KubeletPodStateProvider, PodWorkerState};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[allow(clippy::type_complexity)]
    fn provider(
        pods: &Arc<FakePodManager>,
    ) -> (
        KubeletPodStateProvider,
        Arc<Mutex<HashMap<String, PodWorkerState>>>,
        Arc<AtomicBool>,
    ) {
        let states = Arc::new(Mutex::new(HashMap::new()));
        let synced = Arc::new(AtomicBool::new(false));
        (
            KubeletPodStateProvider {
                pod_states: states.clone(),
                pods_synced: synced.clone(),
                pod_manager: pods.clone(),
            },
            states,
            synced,
        )
    }

    fn pod(extra: serde_json::Value) -> Pod {
        let mut p = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "u1"},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        });
        for (k, v) in extra.as_object().unwrap() {
            p[k] = v.clone();
        }
        serde_json::from_value(p).unwrap()
    }

    #[test]
    fn a_running_pod_is_neither_terminating_nor_removable() {
        let pods = Arc::new(FakePodManager::default());
        pods.add_pod(&pod(json!({"status": {"phase": "Running"}})));
        let (p, _, synced) = provider(&pods);
        synced.store(true, Ordering::SeqCst);
        assert!(!p.should_pod_containers_be_terminating("u1"));
        assert!(!p.should_pod_runtime_be_removed("u1"));
    }

    #[test]
    fn worker_state_decides_for_a_known_pod() {
        let pods = Arc::new(FakePodManager::default());
        pods.add_pod(&pod(json!({})));
        let (p, states, _) = provider(&pods);
        states
            .lock()
            .unwrap()
            .insert("u1".into(), PodWorkerState::TerminatingPod);
        assert!(p.should_pod_containers_be_terminating("u1"));
        assert!(!p.should_pod_runtime_be_removed("u1"));
        states
            .lock()
            .unwrap()
            .insert("u1".into(), PodWorkerState::TerminatedPod);
        assert!(p.should_pod_containers_be_terminating("u1"));
        assert!(p.should_pod_runtime_be_removed("u1"));
    }

    #[test]
    fn a_pod_with_a_deletion_timestamp_is_terminating() {
        let pods = Arc::new(FakePodManager::default());
        let mut deleted = pod(json!({}));
        deleted.metadata.deletion_timestamp = Some(chrono::Utc::now());
        pods.add_pod(&deleted);
        let (p, _, _) = provider(&pods);
        assert!(p.should_pod_containers_be_terminating("u1"));
        assert!(!p.should_pod_runtime_be_removed("u1"));
    }

    #[test]
    fn an_unknown_pod_answers_pods_synced() {
        let pods = Arc::new(FakePodManager::default());
        let (p, _, synced) = provider(&pods);
        assert!(!p.should_pod_containers_be_terminating("gone"));
        assert!(!p.should_pod_runtime_be_removed("gone"));
        synced.store(true, Ordering::SeqCst);
        assert!(p.should_pod_containers_be_terminating("gone"));
        assert!(p.should_pod_runtime_be_removed("gone"));
    }
}
