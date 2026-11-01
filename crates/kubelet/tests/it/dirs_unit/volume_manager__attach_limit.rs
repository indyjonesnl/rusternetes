//! `WaitForAttachAndMount` rejecting a pod whose volume cannot attach for lack
//! of room on the node (`volume_manager.go:425-460`,
//! `volumeAttachLimitExceededError`), and the kubelet's side of it
//! (`kubelet.go:2126-2131`).

use rusternetes_common::resources::Pod;
use rusternetes_kubelet::podcertificate::PodManager;
use rusternetes_kubelet::volume_manager::populator::PodStateProvider;
use rusternetes_kubelet::volume_manager::{VolumeAttachLimitExceededError, VolumeManager};
use rusternetes_kubelet::volume_plugins::csi::get_attachment_name;
use rusternetes_kubelet::volume_plugins::nodeinfomanager::NodeInfoInstaller;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DRIVER: &str = "limit.csi.example.com";
const NODE: &str = "node-lim";

struct Pods(Vec<Pod>);

impl PodManager for Pods {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod> {
        self.0.iter().find(|p| p.metadata.uid == uid).cloned()
    }
    fn get_pods(&self) -> Vec<Pod> {
        self.0.clone()
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

#[derive(Default)]
struct Installer(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl NodeInfoInstaller for Installer {
    async fn install_csi_driver(
        &self,
        _: &str,
        _: &str,
        _: i64,
        _: &HashMap<String, String>,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn update_csi_driver(
        &self,
        n: &str,
        _: &str,
        _: i64,
        _: &HashMap<String, String>,
    ) -> Result<(), String> {
        self.0.lock().unwrap().push(n.into());
        Ok(())
    }
    async fn uninstall_csi_driver(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "ns", "uid": "uid-lim"},
        "spec": {
            "nodeName": NODE,
            "containers": [{"name": "c", "image": "i",
                "volumeMounts": [{"name": "data", "mountPath": "/d"}]}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "claim"}}]
        }
    }))
    .unwrap()
}

/// A pod on a CSI PV whose VolumeAttachment reports `attach_error_code`.
async fn started(attach_error_code: i32) -> (Arc<VolumeManager>, Pod, Arc<Installer>) {
    let storage = Arc::new(StorageBackend::new_memory());
    let create = |key: String, v: serde_json::Value| {
        let storage = storage.clone();
        async move { storage.create(&key, &v).await.unwrap() }
    };
    create(
        build_key("persistentvolumes", None, "pv"),
        json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "claimRef": {"namespace": "ns", "name": "claim", "uid": "pvc-uid"},
                "csi": {"driver": DRIVER, "volumeHandle": "vol-1"}
            }
        }),
    )
    .await;
    create(
        build_key("persistentvolumeclaims", Some("ns"), "claim"),
        json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "claim", "namespace": "ns", "uid": "pvc-uid"},
            "spec": {"volumeName": "pv", "accessModes": ["ReadWriteOnce"]},
            "status": {"phase": "Bound"}
        }),
    )
    .await;
    create(
        build_key("csidrivers", None, DRIVER),
        json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": DRIVER},
            "spec": {"attachRequired": true, "nodeAllocatableUpdatePeriodSeconds": 10}
        }),
    )
    .await;
    let attachment = get_attachment_name("vol-1", DRIVER, NODE);
    create(
        build_key("volumeattachments", None, &attachment),
        json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
            "metadata": {"name": attachment},
            "spec": {"attacher": DRIVER, "nodeName": NODE, "source": {"persistentVolumeName": "pv"}},
            "status": {"attached": false,
                "attachError": {"message": "too many volumes", "errorCode": attach_error_code}}
        }),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let mounter = Arc::new(
        rusternetes_kubelet::volumes::VolumeManager::new(
            dir.path().display().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name(NODE),
    );
    std::mem::forget(dir);
    let pod = pod();
    let shell = Arc::new(VolumeManager::new(
        storage,
        Arc::new(Pods(vec![pod.clone()])),
        Arc::new(Running),
        mounter.clone(),
        mounter.plugin_mgr.clone(),
    ));
    let installer = Arc::new(Installer::default());
    shell.set_node_info_installer(installer.clone());
    shell.run(|| true);
    (shell, pod, installer)
}

/// gRPC `ResourceExhausted`: the attach failed for lack of room.
#[tokio::test]
async fn exhausted_attachment_rejects_the_pod() {
    let (shell, pod, _) = started(8).await;

    let err = shell
        .wait_for_attach_and_mount_within(&pod, Duration::from_millis(800))
        .await
        .unwrap_err();
    let limit = err
        .downcast_ref::<VolumeAttachLimitExceededError>()
        .unwrap_or_else(|| panic!("not an attach limit error: {err:#}"));
    assert_eq!(limit.unmounted_volumes, vec!["data"]);
    assert_eq!(limit.unattached_volumes, vec!["data"]);
    assert!(limit.to_string().starts_with(
        "Node has reached its volume attachment limit, rejecting pod. unmounted volumes=[\"data\"]"
    ));
    assert!(
        !rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err),
        "a rejection, not a wait"
    );
}

/// Any other attach failure is still a wait for the volume.
#[tokio::test]
async fn other_attach_failure_is_a_volume_wait() {
    let (shell, pod, _) = started(5).await;

    let err = shell
        .wait_for_attach_and_mount_within(&pod, Duration::from_millis(800))
        .await
        .unwrap_err();
    assert!(
        rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err),
        "{err:#}"
    );
    assert!(
        err.to_string().contains("unattached volumes=[\"data\"]"),
        "{err}"
    );
}
