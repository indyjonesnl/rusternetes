//! Test support: drive a kubelet's `volumes::VolumeManager` through the
//! upstream volume manager (`pkg/kubelet/volumemanager`) the way a restarted
//! kubelet does: reconstruct what is on disk into the actual state of world,
//! populate the desired state from the pods the kubelet knows, then run the
//! reconciler (`unmountVolumes`, `unmountDetachDevices`).

use rusternetes_common::resources::Pod;
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::VolumeManager as VolumeManagerShell;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct Pods(Mutex<Vec<Pod>>);

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

/// `ShouldPodRuntimeBeRemoved` answers true for the pods in `removed`
/// (terminated pods still present in the API, `pod_workers.go:698`).
pub struct PodStates(Mutex<HashSet<String>>);

impl PodStateProvider for PodStates {
    fn should_pod_containers_be_terminating(&self, uid: &str) -> bool {
        self.0.lock().unwrap().contains(uid)
    }
    fn should_pod_runtime_be_removed(&self, uid: &str) -> bool {
        self.0.lock().unwrap().contains(uid)
    }
}

pub struct World {
    pub shell: VolumeManagerShell,
    pub storage: Arc<StorageBackend>,
    pods: Arc<Pods>,
    removed: Arc<PodStates>,
}

/// A volume manager shell over `vm` (sharing its actual state of world),
/// with `pods` known to the pod manager and a Node `node` in `storage`. Build
/// `vm` `with_node_name(node)` if the test reads the node name off the world.
pub async fn world(
    vm: &VolumeManager,
    node: &str,
    storage: Arc<StorageBackend>,
    pods: Vec<Pod>,
) -> World {
    let node_obj: rusternetes_common::resources::Node = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node", "metadata": {"name": node}
    }))
    .unwrap();
    let _ = Storage::create(storage.as_ref(), &build_key("nodes", None, node), &node_obj).await;
    let pods = Arc::new(Pods(Mutex::new(pods)));
    let removed = Arc::new(PodStates(Mutex::new(HashSet::new())));
    let mounter = Arc::new(vm.clone());
    let shell = VolumeManagerShell::new(
        node,
        storage.clone(),
        pods.clone(),
        removed.clone(),
        mounter.clone(),
        mounter.plugin_mgr.clone(),
    );
    World {
        shell,
        storage,
        pods,
        removed,
    }
}

impl World {
    /// `ShouldPodRuntimeBeRemoved` now holds for `uid`.
    pub fn terminate(&self, uid: &str) {
        self.removed.0.lock().unwrap().insert(uid.to_string());
    }

    /// Forget a pod (deleted from the API) whose containers have all exited:
    /// the populator keeps a deleted pod's volumes until
    /// `ShouldPodRuntimeBeRemoved` (`findAndRemoveDeletedPods`).
    pub fn delete_pod(&self, uid: &str) {
        self.terminate(uid);
        self.pods
            .0
            .lock()
            .unwrap()
            .retain(|p| p.metadata.uid != uid);
    }

    /// Kubelet start: `reconstructVolumes`, a populated desired state, and the
    /// node status read `readyToUnmount` waits for.
    pub async fn restart(&self) {
        self.shell.reconciler.reconstruct_volumes().await;
        self.ready().await;
    }

    /// A populated desired state and the node status read, without
    /// reconstruction: the actual state is whatever the kubelet mounted.
    pub async fn ready(&self) {
        self.populate().await;
        self.shell
            .reconciler
            .update_reconstructed_from_node_status()
            .await;
        assert!(self.shell.reconciler.ready_to_unmount());
    }

    /// One populator loop (`populatorLoop`): drop the volumes of deleted or
    /// terminated pods, then add those of the pods the kubelet knows.
    pub async fn populate(&self) {
        let populator = &self.shell.desired_state_of_world_populator;
        populator.set_has_added_pods(true);
        populator.find_and_remove_deleted_pods();
        populator.find_and_add_new_pods().await;
    }

    /// `passes` reconciler passes (`unmountVolumes` then, a pass later,
    /// `unmountDetachDevices`), re-populating before each.
    pub async fn reconcile(&self, passes: usize) {
        for _ in 0..passes {
            self.populate().await;
            self.shell.reconciler.reconcile().await;
        }
    }
}
