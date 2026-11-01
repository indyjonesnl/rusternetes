//! Tests for the unmount arms of `pkg/kubelet/volumemanager/reconciler`
//! (`reconciler_common.go`: `unmountVolumes`, `unmountDetachDevices`) and
//! `readyToUnmount` (`reconstruct.go`): a volume no pod wants is unmounted,
//! but only once the desired state is fully populated.

use rusternetes_common::resources::{ConfigMap, Pod};
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::VolumeManager as VolumeManagerShell;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::path::Path;
use std::sync::{Arc, Mutex};

struct Pods(Mutex<Vec<Pod>>);

impl PodManager for Pods {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|p| p.metadata.uid == uid)
            .cloned()
    }
    fn get_pods(&self) -> Vec<Pod> {
        self.0.lock().unwrap().clone()
    }
}

struct Running;

impl PodStateProvider for Running {
    fn should_pod_containers_be_terminating(&self, _uid: &str) -> bool {
        false
    }
    fn should_pod_runtime_be_removed(&self, _uid: &str) -> bool {
        false
    }
}

fn pod(uid: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": format!("p-{uid}"), "namespace": "ns", "uid": uid},
        "spec": {
            "nodeName": "node-rr",
            "containers": [{"name": "c", "image": "i",
                "volumeMounts": [{"name": "cfg", "mountPath": "/cfg"}]}],
            "volumes": [{"name": "cfg", "configMap": {"name": "cfg"}}]
        }
    }))
    .unwrap()
}

fn config_map() -> ConfigMap {
    serde_json::from_value(json!({
        "metadata": {"name": "cfg", "namespace": "ns"},
        "data": {"app.conf": "one"}
    }))
    .unwrap()
}

/// A kubelet over `dir` with `pods` known to the pod manager; its actual
/// state of world starts empty, like a restarted kubelet's.
async fn kubelet(dir: &Path, pods: Vec<Pod>) -> (Arc<VolumeManager>, VolumeManagerShell) {
    let storage = Arc::new(StorageBackend::new_memory());
    let _ = Storage::create(
        storage.as_ref(),
        &build_key("configmaps", Some("ns"), "cfg"),
        &config_map(),
    )
    .await;
    let node: rusternetes_common::resources::Node = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "node-rr"},
        "status": {"volumesAttached": [{"name": "kubernetes.io/csi/other", "devicePath": "/dev/x"}]}
    }))
    .unwrap();
    let _ = Storage::create(
        storage.as_ref(),
        &build_key("nodes", None, "node-rr"),
        &node,
    )
    .await;
    let mounter = Arc::new(
        VolumeManager::new_with_recording_empty_dir_mounts(
            dir.display().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name("node-rr"),
    );
    let shell = VolumeManagerShell::new(
        "node-rr",
        storage,
        Arc::new(Pods(Mutex::new(pods))),
        Arc::new(Running),
        mounter.clone(),
        mounter.plugin_mgr.clone(),
    );
    (mounter, shell)
}

/// The directory the previous kubelet mounted `uid`'s configMap volume into.
async fn previous_kubelet_mounts(dir: &Path, uids: &[&str]) -> Vec<String> {
    let pods: Vec<Pod> = uids.iter().map(|u| pod(u)).collect();
    let (mounter, _shell) = kubelet(dir, pods.clone()).await;
    let mut paths = Vec::new();
    for pod in &pods {
        paths.push(mounter.create_pod_volumes(pod).await.unwrap()["cfg"].clone());
    }
    paths
}

/// The previous kubelet's mount of `uid`'s configMap, as a restarted kubelet
/// finds it: reconstructed into the actual state as uncertain.
async fn restarted(
    dir: &Path,
    uid: &str,
    pods: Vec<Pod>,
) -> (Arc<VolumeManager>, VolumeManagerShell, String) {
    let paths = previous_kubelet_mounts(dir, &[uid]).await;
    let (mounter, shell) = kubelet(dir, pods).await;
    shell.reconciler.reconstruct_volumes().await;
    assert_eq!(mounter.asw.get_all_mounted_volumes().len(), 1);
    (mounter, shell, paths[0].clone())
}

/// `readyToUnmount`: "allow unmount only when DSW is fully populated to
/// prevent unmounting volumes that did not reach DSW yet".
#[tokio::test]
async fn nothing_is_unmounted_before_the_desired_state_is_populated() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, _) = restarted(dir.path(), "gone", vec![]).await;

    assert!(!shell.reconciler.ready_to_unmount());
    shell.reconciler.reconcile().await;

    assert_eq!(mounter.asw.get_all_mounted_volumes().len(), 1);
}

/// `unmountVolumes`: a volume mounted (or possibly mounted) for a pod that the
/// desired state does not have is torn down and leaves the actual state.
#[tokio::test]
async fn volume_of_a_pod_the_desired_state_lacks_is_unmounted() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, path) = restarted(dir.path(), "gone", vec![]).await;
    assert!(Path::new(&path).join("app.conf").exists());

    shell
        .desired_state_of_world_populator
        .set_has_added_pods(true);
    shell
        .reconciler
        .update_reconstructed_from_node_status()
        .await;
    assert!(shell.reconciler.ready_to_unmount());
    shell.reconciler.reconcile().await;

    assert!(mounter.asw.get_all_mounted_volumes().is_empty());
    assert!(!Path::new(&path).join("app.conf").exists(), "TearDown ran");
}

/// A volume a pod still wants is not unmounted: it is mounted again instead.
#[tokio::test]
async fn volume_of_a_desired_pod_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, path) = restarted(dir.path(), "uid-keep", vec![pod("uid-keep")]).await;

    shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    shell.reconciler.reconcile().await;

    assert_eq!(mounter.asw.get_mounted_volumes().len(), 1);
    assert!(Path::new(&path).join("app.conf").exists());
}

/// `unmountDetachDevices` for a volume with no device: once its last pod is
/// unmounted the volume is "detached", i.e. leaves the actual state.
#[tokio::test]
async fn unmounted_volume_is_detached() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, _) = restarted(dir.path(), "gone", vec![]).await;
    shell
        .desired_state_of_world_populator
        .set_has_added_pods(true);
    shell
        .reconciler
        .update_reconstructed_from_node_status()
        .await;

    shell.reconciler.reconcile().await; // unmountVolumes
    assert_eq!(mounter.asw.get_unmounted_volumes().len(), 1);
    shell.reconciler.reconcile().await; // unmountDetachDevices

    assert!(mounter.asw.get_unmounted_volumes().is_empty());
}

/// `readyToUnmount`: "allow unmount only when ASW device paths were corrected
/// from node.status to prevent calling unmount with a wrong devicePath".
#[tokio::test]
async fn unmount_waits_for_the_device_paths_from_node_status() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, _) = restarted(dir.path(), "gone", vec![]).await;
    shell
        .desired_state_of_world_populator
        .set_has_added_pods(true);
    assert!(
        !shell.reconciler.ready_to_unmount(),
        "device paths not read yet"
    );

    // `reconcile` reads the node status; the next pass may unmount.
    shell.reconciler.reconcile().await;
    assert_eq!(mounter.asw.get_all_mounted_volumes().len(), 1);
    assert!(shell.reconciler.ready_to_unmount());
    shell.reconciler.reconcile().await;
    assert!(mounter.asw.get_all_mounted_volumes().is_empty());
}

/// `updateReconstructedFromNodeStatus`: a volume the node does not list as
/// attached is not attachable.
#[tokio::test]
async fn volume_missing_from_node_status_is_not_attachable() {
    let dir = tempfile::tempdir().unwrap();
    let (mounter, shell, _) = restarted(dir.path(), "gone", vec![]).await;
    let name = mounter.asw.get_all_mounted_volumes()[0].volume_name.clone();

    shell
        .reconciler
        .update_reconstructed_from_node_status()
        .await;

    let attached = mounter.asw.get_attached_volume(&name).unwrap();
    assert!(!attached.plugin_is_attachable);
}
