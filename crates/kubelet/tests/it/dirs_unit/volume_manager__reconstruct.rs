//! Tests for `pkg/kubelet/volumemanager/reconciler/reconstruct.go`
//! (`reconstruct_test.go`: `TestReconstructVolumes`, `TestCleanOrphanVolumes`,
//! `TestReconstructVolumesMount`): a restarted kubelet rebuilds its actual
//! state of the world from the volume directories on disk.

use rusternetes_common::resources::{ConfigMap, Pod};
use rusternetes_kubelet::pod_dirs::PodVolume;
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::VolumeManager as VolumeManagerShell;
use rusternetes_kubelet::volume_plugins::util::get_unique_pod_name;
use rusternetes_kubelet::volume_plugins::util::types::UniquePodName;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::path::{Path, PathBuf};
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
    let mounter = Arc::new(
        VolumeManager::new_with_recording_empty_dir_mounts(
            dir.display().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name("node-rr"),
    );
    let shell = VolumeManagerShell::new(
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

/// `TestReconstructVolumes`: volumes on disk the actual state does not know
/// are added as uncertain, for every pod using them.
#[tokio::test]
async fn volumes_on_disk_are_reconstructed_as_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    previous_kubelet_mounts(dir.path(), &["pod1", "pod2"]).await;

    let (mounter, shell) = kubelet(dir.path(), vec![]).await;
    assert!(mounter.asw.get_all_mounted_volumes().is_empty());
    shell.reconciler.reconstruct_volumes().await;

    let asw = &mounter.asw;
    assert_eq!(asw.get_mounted_volumes().len(), 0, "no certain mounts");
    let all = asw.get_all_mounted_volumes();
    assert_eq!(all.len(), 2, "two uncertain pods");
    assert_eq!(
        asw.get_possibly_mounted_volumes_for_pod(&UniquePodName("pod1".into()))
            .len(),
        1
    );
    for m in &all {
        assert_eq!(m.inner_volume_spec_name, "cfg");
        assert!(
            asw.is_volume_reconstructed(&m.volume_name, &m.pod_name),
            "{} is marked reconstructed",
            m.pod_name
        );
    }
    assert!(shell.reconciler.volumes_failed_reconstruction().is_empty());
}

/// A volume the actual state already knows is not reconstructed again
/// (`VolumeExistsWithSpecName`).
#[tokio::test]
async fn volume_in_actual_state_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    previous_kubelet_mounts(dir.path(), &["pod1"]).await;

    let (mounter, shell) = kubelet(dir.path(), vec![]).await;
    shell.reconciler.reconstruct_volumes().await;
    let before = mounter.asw.get_all_mounted_volumes();
    shell.reconciler.reconstruct_volumes().await;
    assert_eq!(mounter.asw.get_all_mounted_volumes().len(), before.len());
    assert!(shell.reconciler.volumes_failed_reconstruction().is_empty());
}

/// `TestReconstructVolumes` "when reconstruction fails for a volume": no
/// plugin, or a plugin that returns a nil spec (`image`), leaves the volume
/// for `cleanOrphanVolumes`.
#[tokio::test]
async fn volume_that_cannot_be_reconstructed_is_remembered() {
    let dir = tempfile::tempdir().unwrap();
    for (plugin, name) in [
        ("missing~plugin", "pvc-abcdef"),
        ("kubernetes.io~image", "img"),
    ] {
        std::fs::create_dir_all(dir.path().join("pods/pod1/volumes").join(plugin).join(name))
            .unwrap();
    }

    let (mounter, shell) = kubelet(dir.path(), vec![]).await;
    shell.reconciler.reconstruct_volumes().await;

    let mut failed: Vec<String> = shell
        .reconciler
        .volumes_failed_reconstruction()
        .into_iter()
        .map(|v| v.volume_spec_name)
        .collect();
    failed.sort();
    assert_eq!(failed, ["img", "pvc-abcdef"]);
    assert!(mounter.asw.get_all_mounted_volumes().is_empty());
}

fn failed_volume(dir: &Path, uid: &str) -> (PodVolume, PathBuf) {
    let path = dir
        .join("pods")
        .join(uid)
        .join("volumes/kubernetes.io~empty-dir/volume-name");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("f"), "x").unwrap();
    (
        PodVolume {
            pod_uid: uid.to_string(),
            volume_spec_name: "volume-name".to_string(),
            volume_path: path.clone(),
            plugin_name: "kubernetes.io/empty-dir".to_string(),
            volume_mode: rusternetes_kubelet::pod_dirs::PodVolumeMode::Filesystem,
        },
        path,
    )
}

/// `TestCleanOrphanVolumes` "volume is not in DSW and is cleaned".
#[tokio::test]
async fn failed_volume_not_in_desired_state_is_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    let (volume, path) = failed_volume(dir.path(), "pod1uid");
    let (_mounter, shell) = kubelet(dir.path(), vec![]).await;
    shell
        .reconciler
        .set_volumes_failed_reconstruction(vec![volume]);

    shell.reconciler.clean_orphan_volumes().await;

    assert!(shell.reconciler.volumes_failed_reconstruction().is_empty());
    assert!(!path.join("f").exists(), "the volume was torn down");
}

/// `TestCleanOrphanVolumes` "volume is in DSW and is not cleaned".
#[tokio::test]
async fn failed_volume_in_desired_state_is_not_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    // A pod whose emptyDir is named like the failed volume.
    let mut p = pod("pod1uid");
    let spec = p.spec.as_mut().unwrap();
    spec.volumes = serde_json::from_value(json!([
        {"name": "volume-name", "emptyDir": {}}
    ]))
    .unwrap();
    spec.containers[0].volume_mounts = serde_json::from_value(json!([
        {"name": "volume-name", "mountPath": "/v"}
    ]))
    .unwrap();
    let (volume, path) = failed_volume(dir.path(), "pod1uid");
    let (_mounter, shell) = kubelet(dir.path(), vec![p]).await;
    shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    shell
        .reconciler
        .set_volumes_failed_reconstruction(vec![volume]);

    shell.reconciler.clean_orphan_volumes().await;

    assert!(shell.reconciler.volumes_failed_reconstruction().is_empty());
    assert!(path.join("f").exists(), "a desired volume is left alone");
}

/// `TestReconstructVolumesMount` "reconstructed volume is mounted": a pod
/// that still wants a reconstructed volume gets it mounted again, which makes
/// the mount certain.
#[tokio::test]
async fn reconstructed_volume_is_mounted_again() {
    let dir = tempfile::tempdir().unwrap();
    previous_kubelet_mounts(dir.path(), &["uid-rr"]).await;

    let (mounter, shell) = kubelet(dir.path(), vec![pod("uid-rr")]).await;
    shell.reconciler.reconstruct_volumes().await;
    assert_eq!(mounter.asw.get_mounted_volumes().len(), 0);

    shell
        .desired_state_of_world_populator
        .find_and_add_new_pods()
        .await;
    shell.reconciler.reconcile().await;

    let mounted = mounter.asw.get_mounted_volumes();
    assert_eq!(mounted.len(), 1, "the uncertain mount became certain");
    assert_eq!(mounted[0].pod_name, get_unique_pod_name(&pod("uid-rr")));
}
