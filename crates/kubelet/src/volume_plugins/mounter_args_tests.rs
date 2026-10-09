//! The pod's `fsGroup` reaches a mounter only through `MounterArgs` (#2900).
//!
//! Upstream: `operation_generator.go:582-589` is the single place the pod's
//! `FsGroup` is turned into `volume.MounterArgs{FsGroup: ..}`; every
//! plugin's `SetUpAt` then reads `mounterArgs.FsGroup`
//! (`empty_dir.go` `setupDir` -> `volume.SetVolumeOwnership(.., mounterArgs.FsGroup, ..)`,
//! `configmap.go:246-252`, `secret.go:232-238`) and never the pod.
//! `NewMounter` captures no fsGroup.
//!
//! Each test builds the mounter from a pod WITH an fsGroup and sets up with
//! EMPTY args: nothing may be chowned/chmodded (setgid stays off). The mirror
//! test builds it from a pod WITHOUT fsGroup and passes the group in args.

use super::plugin::{Mounter, MounterArgs, Spec, VolumePlugin};
use crate::volume_plugins::{
    config_map::ConfigMapPlugin, downward_api::DownwardApiPlugin, empty_dir::EmptyDirPlugin,
    projected::ProjectedPlugin, secret::SecretPlugin, KubeletVolumeHost,
};
use rusternetes_common::resources::{Pod, Volume};
use rusternetes_storage::{MemoryStorage, StorageBackend};
use serde_json::json;
use std::collections::HashMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::Arc;

fn host() -> Arc<KubeletVolumeHost> {
    Arc::new(KubeletVolumeHost::new(
        "/var/lib/rusternetes".to_string(),
        Some(Arc::new(StorageBackend::Memory(Arc::new(
            MemoryStorage::new(),
        )))),
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        HashMap::new(),
    ))
}

fn pod(fs_group: Option<i64>) -> Pod {
    let mut spec = json!({"containers": []});
    if let Some(g) = fs_group {
        spec["securityContext"] = json!({"fsGroup": g});
    }
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
        "spec": spec
    }))
    .unwrap()
}

fn volumes() -> Vec<(&'static str, Volume, Box<dyn VolumePlugin>)> {
    let v = |j| -> Volume { serde_json::from_value(j).unwrap() };
    vec![
        (
            "emptyDir",
            v(json!({"name": "v", "emptyDir": {}})),
            Box::new(EmptyDirPlugin::new(host())),
        ),
        (
            "configMap",
            v(json!({"name": "v", "configMap": {"name": "c", "optional": true}})),
            Box::new(ConfigMapPlugin::new(host())),
        ),
        (
            "secret",
            v(json!({"name": "v", "secret": {"secretName": "s", "optional": true}})),
            Box::new(SecretPlugin::new(host())),
        ),
        (
            "downwardAPI",
            v(json!({"name": "v", "downwardAPI": {"items": [
                {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}]}})),
            Box::new(DownwardApiPlugin::new(host())),
        ),
        (
            "projected",
            v(
                json!({"name": "v", "projected": {"sources": [{"downwardAPI": {"items": [
                {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}]}}]}}),
            ),
            Box::new(ProjectedPlugin::new(host())),
        ),
    ]
}

async fn mounter(plugin: &dyn VolumePlugin, v: &Volume, pod: &Pod) -> Box<dyn Mounter> {
    let spec = Spec {
        volume: v,
        persistent_volume: None,
        read_only: false,
    };
    plugin.new_mounter(&spec, pod).await.unwrap()
}

fn setgid(dir: &std::path::Path) -> bool {
    std::fs::metadata(dir).unwrap().permissions().mode() & 0o2000 != 0
}

#[tokio::test]
async fn pod_fs_group_does_not_leak_past_mounter_args() {
    let mut leaked = Vec::new();
    for (kind, v, plugin) in volumes() {
        let tmp = tempfile::tempdir().unwrap();
        let gid = std::fs::metadata(tmp.path()).unwrap().gid() as i64;
        let dir = tmp.path().join("vol");
        let m = mounter(plugin.as_ref(), &v, &pod(Some(gid))).await;
        m.set_up_at(dir.to_str().unwrap(), &MounterArgs::default())
            .await
            .unwrap();
        if setgid(&dir) {
            leaked.push(kind);
        }
    }
    assert!(
        leaked.is_empty(),
        "pod fsGroup applied without MounterArgs: {leaked:?}"
    );
}

#[tokio::test]
async fn mounter_args_fs_group_is_applied_without_a_pod_fs_group() {
    for (kind, v, plugin) in volumes() {
        let tmp = tempfile::tempdir().unwrap();
        let gid = std::fs::metadata(tmp.path()).unwrap().gid() as i64;
        let dir = tmp.path().join("vol");
        let m = mounter(plugin.as_ref(), &v, &pod(None)).await;
        let args = MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        };
        m.set_up_at(dir.to_str().unwrap(), &args).await.unwrap();
        assert!(setgid(&dir), "{kind}: MounterArgs.fs_group not applied");
    }
}
