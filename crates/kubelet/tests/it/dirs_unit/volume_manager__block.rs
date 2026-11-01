//! A pod's `volumeDevices` through the volume manager: the reconciler maps a
//! Block volume where it would mount a Filesystem one
//! (`operationExecutor.MountVolume`, `operation_executor.go:827-866`), unmaps
//! it when the pod is gone, and releases its device once nothing wants it
//! (`UnmountVolume`, `UnmountDevice`).

use anyhow::Result;
use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, Pod};
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::VolumeManager as VolumeManagerShell;
use rusternetes_kubelet::volume_plugins::util::block_operations::BlockVolumePathHandler;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Blk(Mutex<Vec<String>>);

impl Blk {
    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn push(&self, s: String) {
        self.0.lock().unwrap().push(s);
    }
}

impl BlockVolumePathHandler for Blk {
    fn map_device(&self, _d: &str, _m: &str, link: &str, bind: bool) -> Result<()> {
        self.push(format!("map {link} bind={bind}"));
        Ok(())
    }
    fn unmap_device(&self, _m: &str, link: &str, bind: bool) -> Result<()> {
        self.push(format!("unmap {link} bind={bind}"));
        Ok(())
    }
    fn remove_map_path(&self, _m: &str) -> Result<()> {
        self.push("remove".to_string());
        Ok(())
    }
    fn attach_file_device(&self, _p: &str) -> Result<String> {
        self.push("attach".to_string());
        Ok(String::new())
    }
    fn detach_file_device(&self, _p: &str) -> Result<()> {
        self.push("detach".to_string());
        Ok(())
    }
    fn get_device_bind_mount_refs(&self, _d: &str, _m: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

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

/// Containers are running until `removable` is set.
#[derive(Default)]
struct Running {
    removable: std::sync::atomic::AtomicBool,
}

impl PodStateProvider for Running {
    fn should_pod_containers_be_terminating(&self, _uid: &str) -> bool {
        false
    }
    fn should_pod_runtime_be_removed(&self, _uid: &str) -> bool {
        self.removable.load(std::sync::atomic::Ordering::SeqCst)
    }
}

struct Fx {
    vm: Arc<VolumeManager>,
    shell: VolumeManagerShell,
    pods: Arc<Pods>,
    state: Arc<Running>,
    blk: Arc<Blk>,
    _dir: tempfile::TempDir,
}

async fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let device = dir.path().join("sdb");
    std::fs::write(&device, "").unwrap();
    let storage = Arc::new(StorageBackend::new_memory());
    let pv: PersistentVolume = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv-block"},
        "spec": {"local": {"path": device.display().to_string()}, "volumeMode": "Block",
                 "accessModes": ["ReadWriteOnce"], "capacity": {"storage": "1Gi"},
                 "claimRef": {"namespace": "ns", "name": "claim", "uid": "uid-claim"}}
    }))
    .unwrap();
    let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": "claim", "namespace": "ns", "uid": "uid-claim"},
        "spec": {"volumeName": "pv-block", "volumeMode": "Block",
                 "accessModes": ["ReadWriteOnce"],
                 "resources": {"requests": {"storage": "1Gi"}}},
        "status": {"phase": "Bound"}
    }))
    .unwrap();
    Storage::create(
        storage.as_ref(),
        &build_key("persistentvolumes", None, "pv-block"),
        &pv,
    )
    .await
    .unwrap();
    Storage::create(
        storage.as_ref(),
        &build_key("persistentvolumeclaims", Some("ns"), "claim"),
        &pvc,
    )
    .await
    .unwrap();
    let node: rusternetes_common::resources::Node = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node", "metadata": {"name": "node-bk"}
    }))
    .unwrap();
    Storage::create(
        storage.as_ref(),
        &build_key("nodes", None, "node-bk"),
        &node,
    )
    .await
    .unwrap();
    let blk = Arc::new(Blk::default());
    let vm = Arc::new(
        VolumeManager::new_with_recording_empty_dir_mounts(
            dir.path().display().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name("node-bk")
        .with_block_util(blk.clone()),
    );
    let pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "ns", "uid": "uid-bk"},
        "spec": {"nodeName": "node-bk",
            "containers": [{"name": "c", "image": "i",
                "volumeDevices": [{"name": "blk", "devicePath": "/dev/xvda"}]}],
            "volumes": [{"name": "blk", "persistentVolumeClaim": {"claimName": "claim"}}]}
    }))
    .unwrap();
    let pods = Arc::new(Pods(Mutex::new(vec![pod])));
    let state = Arc::new(Running::default());
    let shell = VolumeManagerShell::new(
        storage,
        pods.clone(),
        state.clone(),
        vm.clone(),
        vm.plugin_mgr.clone(),
    );
    Fx {
        vm,
        shell,
        pods,
        state,
        blk,
        _dir: dir,
    }
}

/// A Block volume the pod wants is mapped, not mounted, by the reconciler.
#[tokio::test]
async fn reconciler_maps_a_block_volume() {
    let f = fx().await;
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    // attach (`VerifyControllerAttachedVolume`), then map
    f.shell.reconciler.reconcile().await;
    f.shell.reconciler.reconcile().await;

    let calls = f.blk.calls();
    assert!(
        calls.contains(&"map uid-bk bind=true".to_string()),
        "{calls:?}"
    );
    assert!(
        calls.contains(&"map blk bind=false".to_string()),
        "{calls:?}"
    );
    let mounted = f.vm.asw.get_mounted_volumes();
    assert_eq!(mounted.len(), 1, "{calls:?}");
    assert!(mounted[0].block_volume_mapper.is_some());
}

/// Once the pod is gone the volume is unmapped, then its device released.
#[tokio::test]
async fn reconciler_unmaps_and_releases_a_block_volume() {
    let f = fx().await;
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    f.shell.reconciler.reconcile().await;
    f.shell.reconciler.reconcile().await;
    assert_eq!(f.vm.asw.get_mounted_volumes().len(), 1);

    f.pods.0.lock().unwrap().clear();
    f.state
        .removable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    f.shell
        .desired_state_of_world_populator
        .set_has_added_pods(true);
    // the populator drops the pod's volumes once it is not in the pod manager
    f.shell
        .desired_state_of_world_populator
        .find_and_remove_deleted_pods();
    f.shell.reconciler.reconcile().await; // UnmapVolume
    f.shell.reconciler.reconcile().await; // UnmapDevice

    let calls = f.blk.calls();
    assert!(calls.contains(&"detach".to_string()), "{calls:?}");
    assert!(
        calls.contains(&"unmap uid-bk bind=true".to_string()),
        "{calls:?}"
    );
    assert!(f.vm.asw.get_all_mounted_volumes().is_empty());
    assert!(calls.contains(&"remove".to_string()), "{calls:?}");
}

/// `reconstructVolumes` finds a block volume under `<pod>/volumeDevices`,
/// rebuilds its spec with `ConstructBlockVolumeSpec` and records it in the
/// actual state as uncertainly mapped, with a `BlockVolumeMapper` instead of a
/// mounter and its device (the global map path) uncertain.
#[tokio::test]
async fn reconstruction_recovers_a_mapped_block_volume() {
    use rusternetes_kubelet::volume_plugins::util::operation_executor::DeviceMountState;
    let f = fx().await;
    let link = f
        ._dir
        .path()
        .join("pods/uid-bk/volumeDevices/kubernetes.io~local-volume/pv-block");
    std::fs::create_dir_all(link.parent().unwrap()).unwrap();
    std::fs::write(&link, "").unwrap();

    f.shell.reconciler.reconstruct_volumes().await;

    let mounted = f.vm.asw.get_all_mounted_volumes();
    assert_eq!(mounted.len(), 1);
    assert!(mounted[0].block_volume_mapper.is_some());
    assert!(mounted[0].mounter.is_none());
    let attached = f.vm.asw.get_attached_volumes();
    assert_eq!(attached.len(), 1);
    assert_eq!(
        attached[0].device_mount_state,
        DeviceMountState::DeviceMountUncertain
    );
    assert!(
        attached[0]
            .device_mount_path
            .contains("plugins/kubernetes.io~local-volume/volumeDevices/pv-block"),
        "{}",
        attached[0].device_mount_path
    );
    assert!(f
        .shell
        .reconciler
        .volumes_failed_reconstruction()
        .is_empty());
}
