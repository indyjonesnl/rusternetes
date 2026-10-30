//! Unit tests moved from `src/volumes.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_common::resources::{
        ConfigMap, KeyToPath, PersistentVolume, PersistentVolumeClaim, Pod,
    };
    pub use rusternetes_kubelet::atomic_writer::FileProjection;
    pub use rusternetes_kubelet::volume_plugins::VolumePlugin;
    pub use rusternetes_storage::{build_key, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, info, warn};
}

#[cfg(unix)]
mod projected_mode_tests {
    use super::__parent::*;
    use rusternetes_common::resources::Secret;
    use rusternetes_kubelet::volumes::*;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    /// A projected configMap item with an explicit `mode` must have that mode
    /// applied to the file the resync path (re)writes — matching the
    /// initial-mount path and upstream's atomic writer. Regression guard for the
    /// resync gap where new keys landed at the umask default (#1050).
    #[tokio::test]
    async fn resync_projected_configmap_applies_item_mode() {
        let tmp = std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "cm"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let cm: rusternetes_common::resources::ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cfg", "namespace": "default"},
            "data": {"app.conf": "hello"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();

        // defaultMode 0644 (420), item mode 0400 (256).
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"configMap": {
                        "name": "cfg",
                        "items": [{"key": "app.conf", "path": "app.conf", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let file = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            rusternetes_kubelet::pod_dirs::plugin::PROJECTED,
            "proj",
        )
        .join("app.conf");
        let perms = std::fs::metadata(&file)
            .expect("projected file must exist after resync")
            .permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected configMap item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Stage a CSI device by hand under `<root>/plugins/kubernetes.io/csi`
    /// (`<dir>/globalmount` + `<dir>/vol_data.json`) for a driver nobody has
    /// registered, so `NodeUnstageVolume` fails transiently
    /// (`csi_attacher.go` `UnmountDevice`, kubernetes#120268).
    fn stage_unregistered_csi_device(root: &std::path::Path, handle: &str) {
        use sha2::Digest;
        let dir = root
            .join("plugins")
            .join(rusternetes_kubelet::pod_dirs::plugin::CSI)
            // `makeDeviceMountPath`: `<csi plugin dir>/<driver>/<sha256(handle)>/globalmount`.
            .join("gone.example.com")
            .join(format!("{:x}", sha2::Sha256::digest(handle.as_bytes())));
        std::fs::create_dir_all(dir.join("globalmount")).unwrap();
        std::fs::write(
            dir.join("vol_data.json"),
            format!(
                r#"{{"specVolID":"pv-{handle}","driverName":"gone.example.com","volumeHandle":"{handle}"}}"#
            ),
        )
        .unwrap();
    }

    /// #3820: the kubelet's node name reaches the world (`NewVolumeManager`'s
    /// `nodeName`, `volume_manager.go:202`), so a reconstructed device's
    /// `AttachedVolume` carries it (`newAttachedVolume`, `:1186-1210`).
    #[tokio::test]
    async fn with_node_name_reaches_reconstructed_attached_volumes() {
        let tmp = std::env::temp_dir().join(format!("rn-asw-node-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        stage_unregistered_csi_device(&tmp, "h1");
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
        .with_node_name("node-7");
        vm.unmount_csi_devices_not_in(&Default::default()).await;
        let attached = vm.asw.get_attached_volumes();
        assert_eq!(attached.len(), 1);
        assert_eq!(attached[0].node_name, "node-7");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #3601: the device-unmount sweep runs against ONE persistent
    /// `ActualStateOfWorld`. A failed `UnmountDevice` leaves the device
    /// `DeviceMountUncertain` (`operation_generator.go:859-868`) and the next
    /// sweep finds it there, instead of rebuilding a transient world that
    /// forgets it.
    #[tokio::test]
    async fn device_unmount_sweep_keeps_uncertain_state_across_sweeps() {
        use rusternetes_kubelet::volume_plugins::util::operation_executor::DeviceMountState;

        let tmp = std::env::temp_dir().join(format!("rn-asw-sweep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        stage_unregistered_csi_device(&tmp, "h1");
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        assert!(vm.asw.get_attached_volumes().is_empty());

        vm.unmount_csi_devices_not_in(&Default::default()).await;
        let after_first = vm.asw.get_attached_volumes();
        assert_eq!(after_first.len(), 1, "the staged device is reconstructed");
        assert_eq!(
            after_first[0].device_mount_state,
            DeviceMountState::DeviceMountUncertain
        );

        // `CompleteFunc` (`operation_generator.go:910`): the failed unstage is
        // observed under the driver-qualified plugin name.
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(
            body.contains("storage_operation_duration_seconds_count{migrated=\"false\",operation_name=\"unmount_device\",status=\"fail-unknown\",volume_plugin=\"kubernetes.io/csi:gone.example.com\"}"),
            "{body}"
        );

        // A second sweep must neither duplicate nor reset the entry.
        vm.unmount_csi_devices_not_in(&Default::default()).await;
        let after_second = vm.asw.get_attached_volumes();
        assert_eq!(after_second.len(), 1);
        assert_eq!(
            after_second[0].device_mount_state,
            DeviceMountState::DeviceMountUncertain
        );
        // Clones of the manager (one per runtime attach) share the world.
        assert_eq!(vm.clone().asw.get_attached_volumes().len(), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// `unmountDetachDevices` else-branch (`reconciler_common.go:299-303`): a
    /// volume the world knows whose device is no longer mounted and that
    /// nothing wants is marked detached, i.e. leaves the world.
    #[tokio::test]
    async fn device_unmount_sweep_detaches_unmounted_unwanted_volumes() {
        use rusternetes_kubelet::volume_plugins::plugin::OwnedSpec;

        let tmp = std::env::temp_dir().join(format!("rn-asw-detach-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pv = serde_json::from_value(json!({
            "metadata": {"name": "pv-x"},
            "spec": {"csi": {"driver": "gone.example.com", "volumeHandle": "x"}}
        }))
        .unwrap();
        let volume = serde_json::from_value(json!({"name": "pv-x"})).unwrap();
        let spec = Arc::new(OwnedSpec {
            volume,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        });
        let name = rusternetes_kubelet::volume_plugins::util::get_unique_volume_name_from_spec(
            vm.plugin_mgr
                .find_plugin_by_name(rusternetes_kubelet::pod_dirs::plugin::CSI)
                .unwrap(),
            &spec.as_spec(),
        )
        .unwrap();
        vm.asw
            .mark_volume_as_attached(Some(&name), spec, "", "")
            .unwrap();
        assert!(vm.asw.volume_exists(&name));

        let desired =
            std::collections::HashSet::from([("gone.example.com".to_string(), "x".to_string())]);
        vm.unmount_csi_devices_not_in(&desired).await;
        assert!(vm.asw.volume_exists(&name), "desired volumes are kept");

        vm.unmount_csi_devices_not_in(&Default::default()).await;
        assert!(!vm.asw.volume_exists(&name), "unwanted volume is detached");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1652 regression: re-projecting an UNCHANGED ConfigMap volume (via
    /// `refresh_volumes` on every sync AND `resync_volumes`) must be a true
    /// no-op — it must NOT rewrite the user-visible file in place through the
    /// AtomicWriter `..data` symlink. An in-place rewrite bumps the real file's
    /// ctime and fires an fsnotify Write, which crash-loops a config watcher
    /// such as kube-proxy ("content of the proxy server's configuration file
    /// was updated"). The user-visible file must stay a symlink and its target
    /// inode's ctime must be unchanged across re-projection.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_configmap_is_a_noop() {
        use std::os::unix::fs::MetadataExt;

        let tmp = std::env::temp_dir().join(format!("rn-cm-noop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "kube-proxy", "namespace": "kube-system"},
            "data": {"config.conf": "apiVersion: v1\nkind: Config\n", "kubeconfig.conf": "x"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("kube-system"), "kube-proxy"),
            &cm,
        )
        .await
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "kube-proxy-xyz", "namespace": "kube-system", "uid": "uid-kp"},
            "spec": {"containers": [], "volumes": [{
                "name": "kube-proxy",
                "configMap": {"name": "kube-proxy"}
            }]}
        }))
        .unwrap();

        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        // Initial projection (AtomicWriter layout: config.conf -> ..data/config.conf).
        vm.create_pod_volumes(&pod).await.unwrap();
        let visible = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-kp",
            rusternetes_kubelet::pod_dirs::plugin::CONFIG_MAP,
            "kube-proxy",
        )
        .join("config.conf");
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "config.conf must be a symlink (AtomicWriter layout)"
        );
        let real = std::fs::canonicalize(&visible).unwrap();
        let ctime_before = std::fs::metadata(&real).unwrap().ctime();
        let data_link_before = std::fs::read_link(
            rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
                &tmp.to_string_lossy(),
                "uid-kp",
                rusternetes_kubelet::pod_dirs::plugin::CONFIG_MAP,
                "kube-proxy",
            )
            .join("..data"),
        )
        .unwrap();

        // Re-project the UNCHANGED ConfigMap through BOTH sync-loop paths repeatedly.
        for _ in 0..3 {
            vm.refresh_volumes(&pod).await.unwrap();
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }

        // The user-visible file must still be a symlink, the real inode's ctime
        // must be untouched (no in-place write), and ..data must not have swapped.
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "config.conf must remain a symlink after re-projection"
        );
        let ctime_after = std::fs::metadata(&real).unwrap().ctime();
        assert_eq!(
            ctime_before, ctime_after,
            "unchanged re-projection must not rewrite the config file in place (would crash kube-proxy)"
        );
        let data_link_after = std::fs::read_link(
            rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
                &tmp.to_string_lossy(),
                "uid-kp",
                rusternetes_kubelet::pod_dirs::plugin::CONFIG_MAP,
                "kube-proxy",
            )
            .join("..data"),
        )
        .unwrap();
        assert_eq!(
            data_link_before, data_link_after,
            "..data must not swap when the ConfigMap is unchanged"
        );
        assert_eq!(
            std::fs::read_to_string(&visible).unwrap(),
            "apiVersion: v1\nkind: Config\n"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Drives a volume through the initial mount then repeated
    /// `refresh_volumes` + `resync_volumes`, asserting the visible file stays
    /// an AtomicWriter symlink and its real inode is never rewritten (#1656).
    #[cfg(unix)]
    async fn assert_reproject_is_noop(
        tag: &str,
        pod: Pod,
        secret: Option<Secret>,
        plugin_dir: &str,
        volume_name: &str,
        file: &str,
        expected: &str,
    ) {
        use std::os::unix::fs::MetadataExt;
        let tmp = std::env::temp_dir().join(format!("rn-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        if let Some(s) = secret {
            Storage::create(
                storage.as_ref(),
                &build_key("secrets", Some("default"), &s.metadata.name),
                &s,
            )
            .await
            .unwrap();
        }
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            &pod.metadata.uid,
            plugin_dir,
            volume_name,
        );
        let visible = dir.join(file);
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "{file} must be an AtomicWriter symlink after the initial mount"
        );
        let real = std::fs::canonicalize(&visible).unwrap();
        let ctime_before = std::fs::metadata(&real).unwrap().ctime();
        let link_before = std::fs::read_link(dir.join("..data")).unwrap();
        for _ in 0..3 {
            vm.refresh_volumes(&pod).await.unwrap();
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }
        assert!(std::fs::symlink_metadata(&visible)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            ctime_before,
            std::fs::metadata(&real).unwrap().ctime(),
            "unchanged re-projection must not rewrite {file} in place"
        );
        assert_eq!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read_to_string(&visible).unwrap(), expected);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1656: Secret re-projection (`refresh_volumes` + `resync_volumes`) must
    /// be a true no-op, like ConfigMap.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_secret_is_a_noop() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-sec"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds", "secret": {"secretName": "creds"}
            }]}
        }))
        .unwrap();
        let secret =
            Secret::new("creds", "default").with_data(std::collections::HashMap::from([(
                "password".to_string(),
                b"hunter2".to_vec(),
            )]));
        assert_reproject_is_noop(
            "sec-noop",
            pod,
            Some(secret),
            rusternetes_kubelet::pod_dirs::plugin::SECRET,
            "creds",
            "password",
            "hunter2",
        )
        .await;
    }

    /// #2390: the projected branch of `resync_volumes` must re-project through
    /// the AtomicWriter like the initial mount (`projected.go:208-221`) --
    /// never write through the `..data` symlinks. Unchanged => zero fs changes
    /// (ctime of every real file and the `..data` target untouched, bound SA
    /// token kept); a changed source swaps `..data`; a removed key is pruned.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_projected_is_a_noop() {
        use std::os::unix::fs::MetadataExt;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-proj"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {"defaultMode": 420, "sources": [
                    {"configMap": {"name": "cfg"}},
                    {"secret": {"name": "sec", "items": [{"key": "password", "path": "pw", "mode": 256}]}},
                    {"downwardAPI": {"items": [{"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}}]}},
                    {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}
                ]}
            }]}
        }))
        .unwrap();
        let tmp = std::env::temp_dir().join(format!("rn-proj-noop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        let cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]));
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();
        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-proj",
            rusternetes_kubelet::pod_dirs::plugin::PROJECTED,
            "proj",
        );
        let files = ["a", "b", "pw", "podname", "token"];
        let snapshot = |dir: &std::path::Path| -> Vec<(i64, i64, u32)> {
            files
                .iter()
                .map(|f| {
                    let real = std::fs::canonicalize(dir.join(f)).unwrap();
                    let m = std::fs::metadata(&real).unwrap();
                    (m.ctime(), m.ctime_nsec(), m.permissions().mode() & 0o777)
                })
                .collect()
        };
        for f in files {
            assert!(
                std::fs::symlink_metadata(dir.join(f))
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{f} must be an AtomicWriter symlink after the initial mount"
            );
        }
        let before = snapshot(&dir);
        let link_before = std::fs::read_link(dir.join("..data")).unwrap();
        let token_before = std::fs::read(dir.join("token")).unwrap();
        for _ in 0..3 {
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }
        assert_eq!(
            before,
            snapshot(&dir),
            "unchanged resync must not touch any file"
        );
        assert_eq!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read(dir.join("token")).unwrap(), token_before);
        assert_eq!(
            std::fs::metadata(dir.join("pw"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400,
            "items[].mode is kept"
        );

        // A changed ConfigMap key swaps `..data`; the removed key `b` is
        // pruned; the bound token and the other sources survive.
        let cm2 = ConfigMap::new("cfg", "default")
            .with_data(HashMap::from([("a".to_string(), "changed".to_string())]));
        Storage::update(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm2,
        )
        .await
        .unwrap();
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_ne!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read_to_string(dir.join("a")).unwrap(), "changed");
        assert!(std::fs::symlink_metadata(dir.join("b")).is_err());
        assert_eq!(std::fs::read_to_string(dir.join("pw")).unwrap(), "hunter2");
        assert_eq!(std::fs::read_to_string(dir.join("podname")).unwrap(), "p");
        assert_eq!(std::fs::read(dir.join("token")).unwrap(), token_before);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #2638: a `kube-api-access*` / `*-token` Secret volume is an ordinary
    /// Secret volume (upstream `secret.go` has no token special case), so the
    /// stored token is projected at mount and re-projected unchanged.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_sa_named_secret_projects_stored_token() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-sat"},
            "spec": {"containers": [], "volumes": [{
                "name": "kube-api-access-x", "secret": {"secretName": "default-token"}
            }]}
        }))
        .unwrap();
        let secret = Secret::new("default-token", "default").with_data(
            std::collections::HashMap::from([("token".to_string(), b"static-unbound".to_vec())]),
        );
        let tmp = std::env::temp_dir().join(format!("rn-sat-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "default-token"),
            &secret,
        )
        .await
        .unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let token = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-sat",
            rusternetes_kubelet::pod_dirs::plugin::SECRET,
            "kube-api-access-x",
        )
        .join("token");
        let before = std::fs::read(&token).unwrap();
        assert_eq!(before, b"static-unbound", "stored token projected verbatim");
        vm.refresh_volumes(&pod).await.unwrap();
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(&token).unwrap(), before);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1656: standalone downwardAPI resync must be a true no-op.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_downward_api_is_a_noop() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "dapi-pod", "namespace": "default", "uid": "uid-da"},
            "spec": {"containers": [], "volumes": [{
                "name": "podinfo", "downwardAPI": {"defaultMode": 420, "items": [
                    {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}
                ]}
            }]}
        }))
        .unwrap();
        assert_reproject_is_noop(
            "da-noop",
            pod,
            None,
            rusternetes_kubelet::pod_dirs::plugin::DOWNWARD_API,
            "podinfo",
            "name",
            "dapi-pod",
        )
        .await;
    }

    /// Projected secret resync must honor `items` mappings. If it writes every
    /// secret key at the volume root, stale cleanup can delete the mapped file
    /// that the container is waiting for.
    #[tokio::test]
    async fn resync_projected_secret_honors_items_mapping() {
        let tmp =
            std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "secret"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let secret: rusternetes_common::resources::Secret = serde_json::from_value(json!({
            "metadata": {"name": "sec", "namespace": "default"},
            "data": {"data-1": "dmFsdWUtMQ=="}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"secret": {
                        "name": "sec",
                        "items": [{"key": "data-1", "path": "new-path-data-1", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let volume_dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            rusternetes_kubelet::pod_dirs::plugin::PROJECTED,
            "proj",
        );
        std::fs::create_dir_all(&volume_dir).unwrap();
        std::fs::write(volume_dir.join("data-1"), b"stale-root").unwrap();

        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let mapped = volume_dir.join("new-path-data-1");
        assert_eq!(std::fs::read(&mapped).unwrap(), b"value-1");
        assert!(
            !volume_dir.join("data-1").exists(),
            "unmapped root secret key must be removed as stale"
        );
        let perms = std::fs::metadata(&mapped).unwrap().permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected secret item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// A pod mounting a PVC bound to a PV carrying the
    /// `pv.beta.kubernetes.io/gid` annotation must surface that GID via
    /// `volume_gids` (upstream `VolumeGIDValue`). A PVC-backed volume whose PV
    /// lacks the annotation contributes nothing.
    #[tokio::test]
    async fn volume_gids_reads_pv_gid_annotation_for_pvc() {
        use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};

        let storage = Arc::new(StorageBackend::new_memory());

        // PV `pv-gid` carries the GID annotation; PV `pv-nogid` does not.
        for (name, ann) in [
            ("pv-gid", json!({"pv.beta.kubernetes.io/gid": "7777"})),
            ("pv-nogid", json!({})),
        ] {
            let pv: PersistentVolume = serde_json::from_value(json!({
                "metadata": {"name": name, "annotations": ann},
                "spec": {
                    "capacity": {"storage": "1Gi"},
                    "accessModes": ["ReadWriteOnce"],
                    "hostPath": {"path": format!("/tmp/{name}")}
                }
            }))
            .unwrap();
            Storage::create(
                storage.as_ref(),
                &build_key("persistentvolumes", None, name),
                &pv,
            )
            .await
            .unwrap();
        }

        for (claim, pv) in [("claim-gid", "pv-gid"), ("claim-nogid", "pv-nogid")] {
            let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
                "metadata": {"name": claim, "namespace": "default"},
                "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": pv,
                         "resources": {"requests": {"storage": "1Gi"}}}
            }))
            .unwrap();
            Storage::create(
                storage.as_ref(),
                &build_key("persistentvolumeclaims", Some("default"), claim),
                &pvc,
            )
            .await
            .unwrap();
        }

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [
                {"name": "with-gid", "persistentVolumeClaim": {"claimName": "claim-gid"}},
                {"name": "no-gid", "persistentVolumeClaim": {"claimName": "claim-nogid"}}
            ]}
        }))
        .unwrap();

        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            std::env::temp_dir().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        assert_eq!(vm.volume_gids(&pod).await, vec![7777]);
    }

    /// Characterization test for the secret volume plugin's `set_up` body
    /// (moved verbatim from `create_volume` in #1970's Task 6). Goes through
    /// the public entry point, `VolumeManager::create_volume`, not the plugin
    /// directly, and asserts on the real on-disk result: the returned path,
    /// the written file contents, and the mode the body applies from
    /// `defaultMode`. This is the test the Task 6 review found missing — the
    /// safety argument for the highest-risk moved body in the refactor had
    /// rested entirely on the text diff. It was re-run unmodified against
    /// `991a503d` (the pre-move commit) to confirm it characterizes the OLD
    /// behaviour too, not just whatever the new code happens to do — see the
    /// task-6 report for both runs.
    /// #1983: the map `create_pod_volumes` returns — which both container
    /// start paths now consume, init-container restart included — holds a
    /// hostPath volume's host path itself, never a dir under the pod dir.
    /// Upstream records the mounted path per volume and the runtime reads
    /// that (`GetMountedVolumesForPod`, `volume_manager.go:320`).
    #[tokio::test]
    async fn create_pod_volumes_maps_a_host_path_volume_to_the_host_path() {
        let tmp = tempfile::tempdir().unwrap();
        let host = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [], "volumes": [
                {"name": "hp", "hostPath": {"path": host.path(), "type": "Directory"}}
            ]}
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        assert_eq!(paths["hp"], host.path().to_string_lossy());
    }

    #[tokio::test]
    async fn create_volume_writes_secret_data_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let secret = Secret::new("mysecret", "default").with_data(HashMap::from([
            ("username".to_string(), b"admin".to_vec()),
            ("password".to_string(), b"hunter2".to_vec()),
        ]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "mysecret"),
            &secret,
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "sec",
            "secret": {"secretName": "mysecret", "defaultMode": 0o400}
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~secret/sec",
                tmp.path().to_string_lossy()
            )
        );

        let username = std::fs::read(format!("{path}/username")).unwrap();
        assert_eq!(username, b"admin");
        let password = std::fs::read(format!("{path}/password")).unwrap();
        assert_eq!(password, b"hunter2");

        let mode = std::fs::metadata(format!("{path}/username"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o400,
            "file mode must come from the volume's defaultMode"
        );

        // The volume dir's mode no longer derives from defaultMode (#1975):
        // upstream's AtomicWriter creates the timestamped data dir 0755
        // (atomic_writer.go) and the volume dir comes from the emptyDir
        // wrapper, never from the Secret's defaultMode.
        let data_dir = std::fs::canonicalize(format!("{path}/..data")).unwrap();
        let dir_mode = std::fs::metadata(&data_dir).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o755);
    }

    /// Characterization test for the downwardAPI volume plugin's `set_up`
    /// body, written and run BEFORE that body moves out of `create_volume`
    /// (#1970 Task 7) — see the task-7 report for the equivalence run against
    /// the pre-move commit. Goes through the public entry point,
    /// `VolumeManager::create_volume`, not the plugin directly, and asserts
    /// on the real on-disk result: the returned path, a `fieldRef` item's
    /// contents, a `resourceFieldRef` item's contents, the mode a per-item
    /// `mode` applies, the mode `defaultMode` applies to an item with no
    /// override, and the directory mode (`defaultMode | 0o111`).
    #[tokio::test]
    async fn create_volume_writes_downward_api_data_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [{
                "name": "app",
                "resources": {"limits": {"cpu": "2"}}
            }]}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "podinfo",
            "downwardAPI": {
                "defaultMode": 416,
                "items": [
                    {"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}},
                    {
                        "path": "cpu_limit",
                        "resourceFieldRef": {"containerName": "app", "resource": "limits.cpu"},
                        "mode": 256
                    }
                ]
            }
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~downward-api/podinfo",
                tmp.path().to_string_lossy()
            )
        );

        let podname = std::fs::read_to_string(format!("{path}/podname")).unwrap();
        assert_eq!(podname, "p", "fieldRef item must contain metadata.name");
        let cpu_limit = std::fs::read_to_string(format!("{path}/cpu_limit")).unwrap();
        assert_eq!(
            cpu_limit, "2",
            "resourceFieldRef item must contain the container's limits.cpu, in whole cores"
        );

        let podname_mode = std::fs::metadata(format!("{path}/podname"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            podname_mode & 0o777,
            0o640,
            "file mode must come from the volume's defaultMode when the item has no mode"
        );

        let cpu_limit_mode = std::fs::metadata(format!("{path}/cpu_limit"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            cpu_limit_mode & 0o777,
            0o400,
            "file mode must come from the item's own mode when set"
        );

        // The wrapped emptyDir's setupDir leaves the root at 0777
        // (empty_dir.go:447-486); downwardapi.go never chmods it (#2323).
        let dir_mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o777);
    }

    /// Characterization test for the projected volume plugin's `set_up` body,
    /// written and run BEFORE that body moves out of `create_volume` (#1970
    /// Task 8) — see the task-8 report for the equivalence run against the
    /// pre-move commit. Goes through the public entry point,
    /// `VolumeManager::create_volume`, not the plugin directly, and combines
    /// the three source kinds a projected volume fans out to in production
    /// (configMap, secret, downwardAPI) in one volume, matching the whole
    /// point of the projected plugin: asserts on the real on-disk result —
    /// the returned path, each source's file contents, a per-item `mode`
    /// override (the configMap item), the `defaultMode` fallback (the secret
    /// and downwardAPI items, which set no `mode`), and the directory mode
    /// (`defaultMode | 0o111`).
    ///
    /// The serviceAccountToken source is NOT covered here — it mints a real
    /// token via `TokenManager` and reads ServiceAccount/Node uids from
    /// storage, which is a materially different code path from the other
    /// three. Left as a follow-up, same as the gap flagged for the secret
    /// plugin in Task 6.
    #[tokio::test]
    async fn create_volume_writes_projected_sources_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([(
            "app.conf".to_string(),
            "hello".to_string(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();

        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "proj",
            "projected": {
                "defaultMode": 416,
                "sources": [
                    {"configMap": {
                        "name": "cfg",
                        "items": [{"key": "app.conf", "path": "app.conf", "mode": 256}]
                    }},
                    {"secret": {
                        "name": "sec",
                        "items": [{"key": "password", "path": "password"}]
                    }},
                    {"downwardAPI": {
                        "items": [{"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}}]
                    }}
                ]
            }
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~projected/proj",
                tmp.path().to_string_lossy()
            )
        );

        let app_conf = std::fs::read_to_string(format!("{path}/app.conf")).unwrap();
        assert_eq!(
            app_conf, "hello",
            "configMap item must contain its key's value"
        );
        let password = std::fs::read(format!("{path}/password")).unwrap();
        assert_eq!(
            password, b"hunter2",
            "secret item must contain its key's value"
        );
        let podname = std::fs::read_to_string(format!("{path}/podname")).unwrap();
        assert_eq!(
            podname, "p",
            "downwardAPI fieldRef item must contain metadata.name"
        );

        let app_conf_mode = std::fs::metadata(format!("{path}/app.conf"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            app_conf_mode & 0o777,
            0o400,
            "configMap item's own mode must override the volume's defaultMode"
        );

        let password_mode = std::fs::metadata(format!("{path}/password"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            password_mode & 0o777,
            0o640,
            "secret item with no mode must fall back to the volume's defaultMode"
        );

        let podname_mode = std::fs::metadata(format!("{path}/podname"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            podname_mode & 0o777,
            0o640,
            "downwardAPI item with no mode must fall back to the volume's defaultMode"
        );

        // The volume directory itself is no longer chmod'd to
        // `defaultMode | 0o111`: upstream's projected SetUpAt hands the
        // payload to the AtomicWriter (`projected.go` -> `writer.Write`), which
        // never applies defaultMode to the directory, so it must simply stay
        // traversable.
        let dir_mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o100, 0o100, "volume dir must stay traversable");
        let ts = std::fs::read_link(format!("{path}/..data")).unwrap();
        let ts_mode = std::fs::metadata(format!("{path}/{}", ts.display()))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            ts_mode & 0o777,
            0o755,
            "timestamp dir is 0755 (atomic_writer.go:399-408)"
        );
    }

    /// With a pod `fsGroup`, a projected file written 0600 ends up 0640
    /// (`volume_linux.go:171-175`: `mode | roMask` for the projected volume's
    /// `GetAttributes().ReadOnly`) -- NOT 0660, which the generic owner->group
    /// mirror in `create_pod_volumes` would produce. The projected plugin owns
    /// its ownership step (`projected.go:200-214`), so the post-pass must skip
    /// it (#2333).
    #[tokio::test]
    async fn create_pod_volumes_projected_fs_group_is_ro_mask_not_mirror() {
        use std::os::unix::fs::MetadataExt;
        let storage = Arc::new(StorageBackend::new_memory());
        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let probe = tmp.path().join("probe");
        std::fs::write(&probe, b"").unwrap();
        let gid = std::fs::metadata(&probe).unwrap().gid();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "securityContext": {"fsGroup": gid},
                "containers": [],
                "volumes": [{
                    "name": "proj",
                    "projected": {"defaultMode": 384, "sources": [
                        {"secret": {"name": "sec"}}
                    ]}
                }]
            }
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        let file = std::path::Path::new(&paths["proj"]).join("password");
        let meta = std::fs::metadata(&file).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o640);
        assert_eq!(meta.gid(), gid);
    }

    /// configMap, secret and downwardAPI volumes with a pod `fsGroup` get
    /// `chmod(mode | roMask)` (`volume_linux.go:171-175`; each reports
    /// `ReadOnly: true`, `configmap.go:160`, `secret.go:166`,
    /// `downwardapi.go:156`), run from their own AtomicWriter `setPerms`
    /// (`configmap.go:246-252`, `secret.go:242-248`, `downwardapi.go:217-223`).
    /// A 0644 file stays 0644 (the owner->group mirror made it 0664) and a 0600
    /// file becomes 0640 (#2540).
    #[tokio::test]
    async fn create_pod_volumes_fs_group_is_ro_mask_for_cm_secret_downward_api() {
        use rusternetes_common::resources::ConfigMap;
        use std::os::unix::fs::MetadataExt;
        let storage = Arc::new(StorageBackend::new_memory());
        let secret = Secret::new("sec", "default")
            .with_data(HashMap::from([("k".to_string(), b"v".to_vec())]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cm", "namespace": "default"},
            "data": {"k": "v"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cm"),
            &cm,
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let probe = tmp.path().join("probe");
        std::fs::write(&probe, b"").unwrap();
        let gid = std::fs::metadata(&probe).unwrap().gid();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1",
                         "labels": {"a": "b"}},
            "spec": {
                "securityContext": {"fsGroup": gid},
                "containers": [],
                "volumes": [
                    {"name": "cm", "configMap": {"name": "cm"}},
                    {"name": "sec", "secret": {"secretName": "sec", "defaultMode": 384}},
                    {"name": "da", "downwardAPI": {"defaultMode": 420, "items": [
                        {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}
                    ]}}
                ]
            }
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        let check = |vol: &str, file: &str, want: u32| {
            let meta = std::fs::metadata(std::path::Path::new(&paths[vol]).join(file)).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, want, "{vol}/{file}");
            assert_eq!(meta.gid(), gid, "{vol}/{file} gid");
        };
        check("cm", "k", 0o644);
        check("da", "labels", 0o644);
        check("sec", "k", 0o640);
    }
}

/// #1620: `resync_volumes` / `refresh_volumes` must not run `std::fs` on an
/// async worker. Upstream runs each volume operation in its own goroutine
/// (`pkg/kubelet/volumemanager/reconciler/reconciler_common.go` ->
/// `operationexecutor`), never inline in the loop that serves everything else.
///
/// The probe is a FIFO standing in for a secret key's file: `std::fs::read` /
/// `write` on it blocks until the other end opens, exactly like IO stuck on a
/// wedged disk. On a single-threaded runtime an inline blocking call starves
/// every other task; offloaded, a heartbeat task keeps ticking.
#[cfg(unix)]
mod blocking_fs_offload_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volumes::*;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    async fn setup(tag: &str) -> (VolumeManager, Pod, Arc<StorageBackend>, std::path::PathBuf) {
        let tmp = std::env::temp_dir().join(format!("rn-offload-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        let secret: rusternetes_common::resources::Secret = serde_json::from_value(json!({
            "metadata": {"name": "s", "namespace": "default"},
            "data": {"k": "dg=="}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "s"),
            &secret,
        )
        .await
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{"name": "sv", "secret": {"secretName": "s"}}]}
        }))
        .unwrap();
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            rusternetes_kubelet::pod_dirs::plugin::SECRET,
            "sv",
        );
        std::fs::create_dir_all(&dir).unwrap();
        let fifo = dir.join("k");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        (vm, pod, storage, fifo)
    }

    /// Run `op` while a heartbeat ticks; the FIFO IO inside `op` blocks until
    /// we open the other end. Returns the ticks seen while it was blocked.
    async fn ticks_while_blocked<Fut>(fifo: std::path::PathBuf, op: Fut) -> usize
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let beats = Arc::new(AtomicUsize::new(0));
        let b = beats.clone();
        let hb = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                b.fetch_add(1, Ordering::SeqCst);
            }
        });
        let task = tokio::spawn(op);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let ticks = beats.load(Ordering::SeqCst);
        // Release the blocked FIFO IO (O_RDWR satisfies either end).
        let release = std::thread::spawn(move || {
            // The code under test may block on several FIFO opens/reads in a
            // row, so keep re-opening: each hold lets the peer's open() complete
            // and each drop gives its read an EOF.
            for _ in 0..30 {
                let _held = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fifo);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
        hb.abort();
        let _ = release.join();
        ticks
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resync_volumes_does_not_block_the_runtime() {
        let (vm, pod, storage, fifo) = setup("resync").await;
        let ticks = ticks_while_blocked(fifo, async move {
            let _ = vm.resync_volumes(&pod, storage.as_ref()).await;
        })
        .await;
        assert!(
            ticks >= 5,
            "runtime starved by blocking fs in resync_volumes: {ticks} ticks in 300ms"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_volumes_does_not_block_the_runtime() {
        let (vm, pod, _storage, fifo) = setup("refresh").await;
        let ticks = ticks_while_blocked(fifo, async move {
            let _ = vm.refresh_volumes(&pod).await;
        })
        .await;
        assert!(
            ticks >= 5,
            "runtime starved by blocking fs in refresh_volumes: {ticks} ticks in 300ms"
        );
    }
}

/// #2913: a mounted projected `podCertificate` volume must pick up a rotated
/// credential bundle on resync. Upstream: the projected plugin
/// `RequiresRemount` (`pkg/volume/projected/projected.go:100-102`) makes the
/// reconciler re-run `SetUpAt` (`reconciler_common.go:185,210`), whose
/// `collectData` re-asks the pod certificate manager
/// (`projected.go:391-428`) and rewrites through the AtomicWriter.
#[cfg(unix)]
mod pod_certificate_resync_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volumes::*;
    use rusternetes_storage::StorageBackend;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// A manager whose bundle the test rotates.
    struct RotatingManager(Mutex<(Vec<u8>, Vec<u8>)>);

    #[async_trait::async_trait]
    impl rusternetes_kubelet::podcertificate::Manager for RotatingManager {
        fn track_pod(&self, _pod: &Pod) {}
        fn forget_pod(&self, _pod: &Pod) {}
        async fn get_pod_certificate_credential_bundle(
            &self,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _volume_name: &str,
            _source_index: usize,
        ) -> Result<(Vec<u8>, Vec<u8>)> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn metric_report(&self) -> rusternetes_kubelet::podcertificate::MetricReport {
            Default::default()
        }
    }

    #[tokio::test]
    async fn resync_projects_rotated_pod_certificate() {
        let mgr = Arc::new(RotatingManager(Mutex::new((
            b"key1\n".to_vec(),
            b"cert1\n".to_vec(),
        ))));
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(StorageBackend::new_memory());
        let vm = VolumeManager::new_with_pod_certificate_manager_and_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            Some(mgr.clone()),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-pc"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds",
                "projected": {"defaultMode": 420, "sources": [
                    {"podCertificate": {
                        "signerName": "example.com/signer",
                        "keyType": "ED25519",
                        "keyPath": "key.pem",
                        "certificateChainPath": "chain.pem",
                        "credentialBundlePath": "bundle.pem"
                    }}
                ]}
            }]}
        }))
        .unwrap();
        let volume = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert1\n"
        );

        // An unchanged bundle must keep every file.
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(format!("{path}/key.pem")).unwrap(), b"key1\n");
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert1\n"
        );

        // Rotate: the manager now hands out a new bundle.
        *mgr.0.lock().unwrap() = (b"key2\n".to_vec(), b"cert2\n".to_vec());
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(format!("{path}/key.pem")).unwrap(), b"key2\n");
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert2\n"
        );
        assert_eq!(
            std::fs::read(format!("{path}/bundle.pem")).unwrap(),
            b"key2\ncert2\n"
        );
    }
    /// A manager error leaves the volume untouched, as a failed `SetUpAt`
    /// writes nothing (`projected.go:393-396` appends to `errlist`;
    /// `:337` returns the aggregate; `SetUpAt` bails before the writer).
    #[tokio::test]
    async fn resync_keeps_files_when_manager_errors() {
        struct Failing;
        #[async_trait::async_trait]
        impl rusternetes_kubelet::podcertificate::Manager for Failing {
            fn track_pod(&self, _pod: &Pod) {}
            fn forget_pod(&self, _pod: &Pod) {}
            async fn get_pod_certificate_credential_bundle(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: &str,
                _: usize,
            ) -> Result<(Vec<u8>, Vec<u8>)> {
                Err(anyhow::anyhow!("no credential bundle yet"))
            }
            fn metric_report(&self) -> rusternetes_kubelet::podcertificate::MetricReport {
                Default::default()
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(StorageBackend::new_memory());
        let dir = tmp
            .path()
            .join("pods/uid-pc/volumes/kubernetes.io~projected/creds");
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-pc"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds",
                "projected": {"sources": [
                    {"podCertificate": {
                        "signerName": "example.com/signer", "keyType": "ED25519",
                        "keyPath": "key.pem"
                    }}
                ]}
            }]}
        }))
        .unwrap();
        let ok = Arc::new(RotatingManager(Mutex::new((
            b"k\n".to_vec(),
            b"c\n".to_vec(),
        ))));
        let mk = |m: Arc<dyn rusternetes_kubelet::podcertificate::Manager>| {
            VolumeManager::new_with_pod_certificate_manager_and_recording_empty_dir_mounts(
                tmp.path().to_string_lossy().to_string(),
                Some(storage.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                Some(m),
            )
        };
        let volume = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        mk(ok).create_volume(&pod, &volume).await.unwrap();
        mk(Arc::new(Failing))
            .resync_volumes(&pod, storage.as_ref())
            .await
            .unwrap();
        assert_eq!(std::fs::read(dir.join("key.pem")).unwrap(), b"k\n");
    }
}
