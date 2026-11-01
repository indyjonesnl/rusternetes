//! Tests for the remount arm of `pkg/kubelet/volumemanager/reconciler`
//! (`reconciler_common.go:132-232`): a volume the desired state wants and the
//! actual state has mounted for the pod, whose plugin asks for a remount, is
//! mounted again through the plugin's `SetUp`, and an unknown one is attached
//! and mounted.

use rusternetes_common::resources::{ConfigMap, Pod};
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::VolumeManager as VolumeManagerShell;
use rusternetes_kubelet::volume_plugins::util::get_unique_pod_name;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
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

fn config_map(value: &str) -> ConfigMap {
    serde_json::from_value(json!({
        "metadata": {"name": "cfg", "namespace": "ns"},
        "data": {"app.conf": value}
    }))
    .unwrap()
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "ns", "uid": "uid-rc"},
        "spec": {
            "nodeName": "node-rc",
            "containers": [{"name": "c", "image": "i",
                "volumeMounts": [{"name": "cfg", "mountPath": "/cfg"}]}],
            "volumes": [{"name": "cfg", "configMap": {"name": "cfg"}}]
        }
    }))
    .unwrap()
}

struct Fx {
    mounter: Arc<VolumeManager>,
    shell: VolumeManagerShell,
    storage: Arc<StorageBackend>,
    pod: Pod,
    dir_of_cfg: String,
    _dir: tempfile::TempDir,
}

async fn fx() -> Fx {
    let mut f = fx_inner().await;
    let paths = f.mounter.create_pod_volumes(&f.pod).await.unwrap();
    f.dir_of_cfg = paths["cfg"].clone();
    f
}

async fn fx_inner() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(StorageBackend::new_memory());
    Storage::create(
        storage.as_ref(),
        &build_key("configmaps", Some("ns"), "cfg"),
        &config_map("one"),
    )
    .await
    .unwrap();
    let mounter = Arc::new(
        VolumeManager::new_with_recording_empty_dir_mounts(
            dir.path().display().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name("node-rc"),
    );
    let pod = pod();
    let shell = VolumeManagerShell::new(
        storage.clone(),
        Arc::new(Pods(Mutex::new(vec![pod.clone()]))),
        Arc::new(Running),
        mounter.clone(),
        mounter.plugin_mgr.clone(),
    );
    Fx {
        mounter,
        shell,
        storage,
        pod,
        dir_of_cfg: String::new(),
        _dir: dir,
    }
}

fn read(f: &Fx) -> String {
    std::fs::read_to_string(std::path::Path::new(&f.dir_of_cfg).join("app.conf")).unwrap()
}

async fn update_config_map(f: &Fx, value: &str) {
    Storage::update(
        f.storage.as_ref(),
        &build_key("configmaps", Some("ns"), "cfg"),
        &config_map(value),
    )
    .await
    .unwrap();
}

/// Mounted by the kubelet, desired by the populator, changed at the source,
/// reprocessed by sync: the reconciler mounts it again with the new data.
#[tokio::test]
async fn remount_required_volume_is_set_up_again() {
    let f = fx().await;
    assert_eq!(read(&f), "one");

    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    update_config_map(&f, "two").await;

    // Added to the desired state: pods are remounted (e.g. DownwardAPI).
    f.shell.reconciler.reconcile().await;
    assert_eq!(read(&f), "two");
}

/// Nothing asks for a remount, so the reconciler leaves the volume alone.
#[tokio::test]
async fn volume_without_remount_request_is_left_alone() {
    let f = fx().await;
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    f.shell.reconciler.reconcile().await; // consumes the request the populator made
    update_config_map(&f, "two").await;

    f.shell.reconciler.reconcile().await;
    assert_eq!(read(&f), "one", "no remount requested, no SetUp");

    // `ReprocessPod` + the populator ask again.
    f.shell
        .desired_state_of_world_populator
        .reprocess_pod(&get_unique_pod_name(&f.pod));
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    f.shell.reconciler.reconcile().await;
    assert_eq!(read(&f), "two");
}

/// A volume the actual state does not know is attached first
/// (`waitForVolumeAttach`), then mounted on the next pass, as upstream's
/// reconciler does across loop iterations.
#[tokio::test]
async fn volume_unknown_to_the_actual_state_is_attached_then_mounted() {
    let f = fx_inner().await;
    f.shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;

    f.shell.reconciler.reconcile().await;
    assert_eq!(
        f.shell.actual_state_of_world.get_attached_volumes().len(),
        1
    );
    assert!(f
        .shell
        .actual_state_of_world
        .get_mounted_volumes()
        .is_empty());

    f.shell.reconciler.reconcile().await;
    assert_eq!(f.shell.actual_state_of_world.get_mounted_volumes().len(), 1);
}

// ---- `WaitForAttachAndMount` (`volume_manager.go:395-472`) ----

fn started(f: &Fx) -> Arc<VolumeManagerShell> {
    // Same shell as `f.shell`, which owns plain fields: build the one the
    // mounter hands its `create_pod_volumes` to.
    let shell = Arc::new(VolumeManagerShell::new(
        f.storage.clone(),
        Arc::new(Pods(Mutex::new(vec![f.pod.clone()]))),
        Arc::new(Running),
        f.mounter.clone(),
        f.mounter.plugin_mgr.clone(),
    ));
    f.mounter.set_volume_manager(&shell);
    shell.run(|| true);
    shell
}

/// With a volume manager attached, `create_pod_volumes` mounts nothing itself:
/// it waits for the reconciler and reads the path from the actual state.
#[tokio::test]
async fn create_pod_volumes_waits_for_the_reconciler() {
    let f = fx_inner().await;
    let shell = started(&f);

    let paths = f.mounter.create_pod_volumes(&f.pod).await.unwrap();
    let dir = paths.get("cfg").expect("the volume's path");
    assert_eq!(
        std::fs::read_to_string(std::path::Path::new(dir).join("app.conf")).unwrap(),
        "one"
    );
    assert_eq!(shell.actual_state_of_world.get_mounted_volumes().len(), 1);
    // Mounted: a second call returns at once with the same path.
    assert_eq!(f.mounter.create_pod_volumes(&f.pod).await.unwrap(), paths);
}

/// A volume that cannot be set up (a required ConfigMap that does not exist)
/// leaves the wait to time out: the pod is waiting, not failing, and the
/// message names what is unmounted (`volume_manager.go:462-468`).
#[tokio::test]
async fn wait_for_attach_and_mount_times_out_naming_the_unmounted_volume() {
    let f = fx_inner().await;
    Storage::delete(
        f.storage.as_ref(),
        &build_key("configmaps", Some("ns"), "cfg"),
    )
    .await
    .unwrap();
    let shell = started(&f);

    let err = shell
        .wait_for_attach_and_mount_within(&f.pod, std::time::Duration::from_millis(800))
        .await
        .unwrap_err();
    assert!(
        rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err),
        "{err:#}"
    );
    assert!(
        err.to_string().starts_with("unmounted volumes=[\"cfg\"]"),
        "{err}"
    );
}

/// A volume no container uses is never mounted nor waited for
/// (`desired_state_of_world_populator.go:293-298`).
#[tokio::test]
async fn unused_volume_is_not_waited_for() {
    let mut f = fx_inner().await;
    f.pod.spec.as_mut().unwrap().containers[0].volume_mounts = None;
    let shell = started(&f);

    shell.wait_for_attach_and_mount(&f.pod).await.unwrap();
    assert!(f
        .mounter
        .create_pod_volumes(&f.pod)
        .await
        .unwrap()
        .is_empty());
}
