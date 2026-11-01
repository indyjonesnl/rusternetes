//! Tests for the block-volume operations of
//! `pkg/volume/util/operationexecutor/operation_generator.go`
//! (`GenerateMapVolumeFunc`, `GenerateUnmapVolumeFunc`,
//! `GenerateUnmapDeviceFunc`), over a local block PV and a recording
//! `BlockVolumePathHandler`.

use anyhow::{anyhow, Result};
use rusternetes_common::resources::Pod;
use rusternetes_kubelet::volume_manager::cache::ActualStateOfWorld;
use rusternetes_kubelet::volume_plugins::plugin::OwnedSpec;
use rusternetes_kubelet::volume_plugins::util::block_operations::{
    map_volume_with, unmap_device_with, unmap_volume_with, BlockVolumePathHandler,
};
use rusternetes_kubelet::volume_plugins::util::get_unique_volume_name_from_spec;
use rusternetes_kubelet::volume_plugins::util::hostutil::HostUtil;
use rusternetes_kubelet::volume_plugins::util::operation_executor::{
    AttachedVolume, VolumeToMount,
};
use rusternetes_kubelet::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use rusternetes_kubelet::volumes::VolumeManager;
use serde_json::json;
use std::sync::{Arc, Mutex};

/// Records every call; `fail_map` makes the second `map_device` fail.
#[derive(Default)]
struct Blk {
    calls: Mutex<Vec<String>>,
    fail_map: bool,
    refs: Vec<String>,
}

impl Blk {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl BlockVolumePathHandler for Blk {
    fn map_device(&self, device: &str, map_path: &str, link: &str, bind: bool) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("map {device} {map_path} {link} bind={bind}"));
        if self.fail_map && !bind {
            return Err(anyhow!("boom"));
        }
        Ok(())
    }
    fn unmap_device(&self, map_path: &str, link: &str, bind: bool) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("unmap {map_path} {link} bind={bind}"));
        Ok(())
    }
    fn remove_map_path(&self, map_path: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("remove {map_path}"));
        Ok(())
    }
    fn attach_file_device(&self, path: &str) -> Result<String> {
        self.calls.lock().unwrap().push(format!("attach {path}"));
        Ok(String::new())
    }
    fn detach_file_device(&self, path: &str) -> Result<()> {
        self.calls.lock().unwrap().push(format!("detach {path}"));
        Ok(())
    }
    fn get_device_bind_mount_refs(&self, _dev: &str, _map: &str) -> Result<Vec<String>> {
        Ok(self.refs.clone())
    }
}

struct Fx {
    vm: VolumeManager,
    asw: Arc<ActualStateOfWorld>,
    spec: Arc<OwnedSpec>,
    volume_name: UniqueVolumeName,
    device: String,
    root: String,
    _dir: tempfile::TempDir,
}

fn fx(access_modes: serde_json::Value) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().display().to_string();
    let device = dir.path().join("sdb");
    std::fs::write(&device, "").unwrap();
    let device = device.display().to_string();
    let vm = VolumeManager::new_with_recording_empty_dir_mounts(
        root.clone(),
        None,
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
    );
    let pv = serde_json::from_value(json!({
        "metadata": {"name": "pv-block"},
        "spec": {"local": {"path": device}, "volumeMode": "Block",
                 "accessModes": access_modes, "capacity": {"storage": "1Gi"}}
    }))
    .unwrap();
    let volume = serde_json::from_value(json!({"name": "pv-block"})).unwrap();
    let spec = Arc::new(OwnedSpec {
        volume,
        persistent_volume: Some(pv),
        read_only: false,
        migrated: false,
        inline_volume_spec_for_csi_migration: false,
    });
    let plugin = vm.plugin_mgr.find_plugin_by_spec(&spec.as_spec()).unwrap();
    let volume_name = get_unique_volume_name_from_spec(plugin, &spec.as_spec()).unwrap();
    let asw = vm.asw.clone();
    asw.mark_volume_as_attached(Some(&volume_name), spec.clone(), "node-b", "")
        .unwrap();
    Fx {
        vm,
        asw,
        spec,
        volume_name,
        device,
        root,
        _dir: dir,
    }
}

fn pod(uid: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": format!("p-{uid}"), "namespace": "ns", "uid": uid},
        "spec": {"containers": [{"name": "c", "image": "i",
            "volumeDevices": [{"name": "pv-block", "devicePath": "/dev/xvda"}]}],
            "volumes": [{"name": "pv-block", "persistentVolumeClaim": {"claimName": "c"}}]}
    }))
    .unwrap()
}

fn to_mount(f: &Fx, uid: &str) -> VolumeToMount {
    let pod = pod(uid);
    VolumeToMount {
        volume_name: f.volume_name.clone(),
        pod_name: UniquePodName(uid.to_string()),
        volume_spec: f.spec.clone(),
        outer_volume_spec_names: vec!["pv-block".to_string()],
        pod: Arc::new(pod),
        plugin_is_attachable: false,
        plugin_is_device_mountable: false,
        volume_gid_value: String::new(),
        device_path: String::new(),
        reported_in_use: true,
        desired_size_limit: None,
        mount_request_time: std::time::SystemTime::now(),
        desired_persistent_volume_size: None,
        selinux_label: String::new(),
    }
}

/// `MapVolume`: the device is linked under the global map path (bind mount)
/// and the pod device map path, a file-device lock is taken, and the volume
/// and its device are recorded as mounted.
#[tokio::test]
async fn map_volume_maps_the_device_and_records_it() {
    let f = fx(json!(["ReadWriteOnce"]));
    let blk = Blk::default();
    let v = to_mount(&f, "uid-1");

    let ctx = map_volume_with(&f.vm.plugin_mgr, &v, &f.asw, &blk)
        .await
        .unwrap();

    assert!(ctx.event_err.is_none(), "{:?}", ctx.event_err);
    let global = format!(
        "{}/plugins/kubernetes.io~local-volume/volumeDevices/pv-block",
        f.root
    );
    let pod_dir = format!(
        "{}/pods/uid-1/volumeDevices/kubernetes.io~local-volume",
        f.root
    );
    let device = std::fs::canonicalize(&f.device)
        .unwrap()
        .display()
        .to_string();
    assert_eq!(
        blk.calls(),
        vec![
            format!("map {device} {global} uid-1 bind=true"),
            format!("map {device} {pod_dir} pv-block bind=false"),
            format!("attach {global}/uid-1"),
        ]
    );
    let mounted = f.asw.get_mounted_volumes();
    assert_eq!(mounted.len(), 1);
    assert!(mounted[0].block_volume_mapper.is_some());
    assert!(mounted[0].mounter.is_none());
    assert_eq!(
        f.asw
            .get_attached_volume(&f.volume_name)
            .unwrap()
            .device_mount_path,
        global
    );
}

/// A failed `MapBlockVolume` after `MapPodDevice` leaves the mount uncertain,
/// so the volume is unmapped when its pod is deleted (the `defer` at
/// `operation_generator.go:1048`).
#[tokio::test]
async fn failed_map_leaves_the_mount_uncertain() {
    let f = fx(json!(["ReadWriteOnce"]));
    let blk = Blk {
        fail_map: true,
        ..Default::default()
    };

    let ctx = map_volume_with(&f.vm.plugin_mgr, &to_mount(&f, "uid-1"), &f.asw, &blk)
        .await
        .unwrap();

    let err = ctx.event_err.expect("the map failed").to_string();
    assert!(err.contains("MapVolume.MapBlockVolume failed"), "{err}");
    assert!(f.asw.get_mounted_volumes().is_empty());
    assert_eq!(f.asw.get_all_mounted_volumes().len(), 1, "uncertain");
}

/// `IsVolumeMountedElsewhere` + a single ReadWriteOncePod access mode.
#[tokio::test]
async fn read_write_once_pod_volume_in_use_by_another_pod_is_refused() {
    let f = fx(json!(["ReadWriteOncePod"]));
    let blk = Blk::default();
    let first = map_volume_with(&f.vm.plugin_mgr, &to_mount(&f, "uid-1"), &f.asw, &blk)
        .await
        .unwrap();
    assert!(first.event_err.is_none());

    let second = map_volume_with(&f.vm.plugin_mgr, &to_mount(&f, "uid-2"), &f.asw, &blk)
        .await
        .unwrap();

    let err = second.event_err.expect("refused").to_string();
    assert!(err.contains("ReadWriteOncePod"), "{err}");
}

/// `UnmapVolume`: the file-device lock is released, the pod link and the
/// global bind mount are removed, and the volume leaves the actual state's
/// mounts.
#[tokio::test]
async fn unmap_volume_removes_the_links_and_the_mount() {
    let f = fx(json!(["ReadWriteOnce"]));
    let blk = Blk::default();
    let ctx = map_volume_with(&f.vm.plugin_mgr, &to_mount(&f, "uid-1"), &f.asw, &blk)
        .await
        .unwrap();
    assert!(ctx.event_err.is_none());
    let mounted = f.asw.get_all_mounted_volumes().remove(0);

    let unmap = Blk::default();
    let ctx = unmap_volume_with(&f.vm.plugin_mgr, &mounted, &f.asw, &unmap)
        .await
        .unwrap();

    assert!(ctx.event_err.is_none(), "{:?}", ctx.event_err);
    let global = format!(
        "{}/plugins/kubernetes.io~local-volume/volumeDevices/pv-block",
        f.root
    );
    let pod_dir = format!(
        "{}/pods/uid-1/volumeDevices/kubernetes.io~local-volume",
        f.root
    );
    assert_eq!(
        unmap.calls(),
        vec![
            format!("detach {global}/uid-1"),
            format!("unmap {pod_dir} pv-block bind=false"),
            format!("unmap {global} uid-1 bind=true"),
        ]
    );
    assert!(f.asw.get_all_mounted_volumes().is_empty());
}

fn attached(f: &Fx, device_mount_path: &str) -> AttachedVolume {
    AttachedVolume {
        volume_name: f.volume_name.clone(),
        volume_spec: f.spec.clone(),
        node_name: "node-b".to_string(),
        plugin_is_attachable: false,
        device_path: f.device.clone(),
        device_mount_path: device_mount_path.to_string(),
        plugin_name: "kubernetes.io/local-volume".to_string(),
        selinux_mount_context: String::new(),
    }
}

/// `UnmapDevice`: refused while another pod still references the device.
#[tokio::test]
async fn unmap_device_is_refused_while_referenced() {
    let f = fx(json!(["ReadWriteOnce"]));
    let blk = Blk {
        refs: vec!["/g/other-pod".to_string()],
        ..Default::default()
    };

    let ctx = unmap_device_with(
        &f.vm.plugin_mgr,
        &attached(&f, "/g"),
        &f.asw,
        &HostUtil,
        &blk,
    )
    .await
    .unwrap();

    let err = ctx.event_err.expect("referenced").to_string();
    assert!(err.contains("still referenced"), "{err}");
    assert!(blk.calls().is_empty());
}

/// `UnmapDevice` with no references removes the global map path and marks the
/// device unmounted.
#[tokio::test]
async fn unmap_device_removes_the_global_map_path() {
    let f = fx(json!(["ReadWriteOnce"]));
    f.asw
        .mark_device_as_mounted(&f.volume_name, &f.device, "/g", "")
        .unwrap();
    let blk = Blk::default();

    let ctx = unmap_device_with(
        &f.vm.plugin_mgr,
        &attached(&f, "/g"),
        &f.asw,
        &HostUtil,
        &blk,
    )
    .await
    .unwrap();

    assert!(ctx.event_err.is_none(), "{:?}", ctx.event_err);
    assert_eq!(blk.calls(), vec!["remove /g".to_string()]);
    assert!(!f
        .asw
        .get_attached_volume(&f.volume_name)
        .unwrap()
        .device_may_be_mounted());
}
