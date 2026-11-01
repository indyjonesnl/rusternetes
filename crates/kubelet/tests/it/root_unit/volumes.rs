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

mod configmap_payload_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volumes::*;

    fn cm(pairs: &[(&str, &str)]) -> ConfigMap {
        let mut c = ConfigMap::new("cm", "default");
        c.data = Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        c
    }

    fn item(key: &str, path: &str, mode: Option<i32>) -> KeyToPath {
        KeyToPath {
            key: key.to_string(),
            path: path.to_string(),
            mode,
        }
    }

    /// `items[].mode` must reach the projected file; siblings without one keep
    /// the volume `defaultMode`.
    ///
    /// Pins the upstream Conformance spec "ConfigMap should be consumable from
    /// pods in volume with mappings and Item mode set [LinuxOnly]"
    /// (`test/e2e/common/storage/configmap_volume.go:99-102`), which maps a key
    /// to the nested path `path/to/data-2` with `Mode: 0400` and asserts the
    /// file mode. Upstream carries the mode per file in `FileProjection.Mode`
    /// (`pkg/volume/util/atomic_writer.go:64-68`); this projection used to
    /// collapse it to one mode for the whole volume, so the spec could not pass.
    #[test]
    fn item_mode_overrides_default_mode_per_file() {
        let configmap = cm(&[("data-1", "value-1"), ("data-2", "value-2")]);
        let items = vec![
            item("data-2", "path/to/data-2", Some(0o400)),
            item("data-1", "plain", None),
        ];

        let payload =
            build_configmap_payload(&configmap, Some(&items), "cm", false, 0o644).unwrap();

        assert_eq!(
            payload.get("path/to/data-2").map(|p| p.mode),
            Some(0o400),
            "items[].mode must be carried per file"
        );
        assert_eq!(
            payload.get("path/to/data-2").map(|p| p.data.as_slice()),
            Some(b"value-2".as_slice())
        );
        assert_eq!(
            payload.get("plain").map(|p| p.mode),
            Some(0o644),
            "an item without a mode falls back to the volume defaultMode"
        );
    }

    /// With no `items`, every key takes the volume `defaultMode`.
    #[test]
    fn without_items_every_key_takes_default_mode() {
        let configmap = cm(&[("a", "1"), ("b", "2")]);
        let payload = build_configmap_payload(&configmap, None, "cm", false, 0o400).unwrap();

        assert_eq!(payload.len(), 2);
        assert!(payload.values().all(|p| p.mode == 0o400));
    }
}

/// Orphaned pod directory sweep — port of `cleanupOrphanedPodDirs`
/// (`pkg/kubelet/kubelet_volumes.go:169-260`).
mod orphan_sweep_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volumes::*;
    use std::collections::HashSet;

    fn tmp(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!("rn-sweep-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn vm(root: &str) -> VolumeManager {
        VolumeManager::new_with_recording_empty_dir_mounts(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    /// Lay down `<root>/pods/<uid>/volumes/<plugin>/<volume>` as create_volume
    /// would, so the sweep sees a realistic tree.
    fn seed_pod(root: &str, uid: &str, volume: &str) -> std::path::PathBuf {
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            root,
            uid,
            rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
            volume,
        );
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Lay down a non-empty volume of `plugin`, as a running pod leaves it.
    fn seed_filled_volume(root: &str, uid: &str, plugin: &str, volume: &str) -> std::path::PathBuf {
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(root, uid, plugin, volume);
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/data"), "x").unwrap();
        dir
    }

    /// The gap this slice closes: `removeOrphanedPodVolumeDirs` `rmdir`s each
    /// volume (`kubelet_volumes.go:119-165`), which fails on a volume that
    /// still holds files, so a pod whose emptyDir was written to leaked its
    /// directory forever — upstream relies on the reconciler's `TearDown`
    /// having emptied it first.
    #[test]
    fn the_sweep_alone_cannot_remove_a_volume_that_holds_files() {
        let root = tmp("filled-sweep-only");
        seed_filled_volume(
            &root,
            "uid-gone",
            rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
            "scratch",
        );

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-gone").exists());
    }

    /// A pod known to the kubelet that mounts `volumes` (plugin, name) as
    /// emptyDirs the desired state of world can hold.
    fn pod_with_empty_dirs(uid: &str, names: &[&str]) -> Pod {
        let volumes: Vec<_> = names
            .iter()
            .map(|n| serde_json::json!({"name": n, "emptyDir": {}}))
            .collect();
        let mounts: Vec<_> = names
            .iter()
            .map(|n| serde_json::json!({"name": n, "mountPath": format!("/{n}")}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": uid, "namespace": "ns", "uid": uid},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": mounts}],
                     "volumes": volumes}
        }))
        .unwrap()
    }

    async fn world_of(vm: &VolumeManager, pods: Vec<Pod>) -> crate::volume_world::World {
        crate::volume_world::world(
            vm,
            "node-1",
            Arc::new(rusternetes_storage::StorageBackend::new_memory()),
            pods,
        )
        .await
    }

    /// The restart flow: `reconstructVolumes` finds the volumes on disk, and
    /// the reconciler's `unmountVolumes` -> `NewUnmounter` -> `TearDown` reaps
    /// those of pods the kubelet no longer has.
    #[tokio::test]
    async fn teardown_then_cleanup_reaps_every_non_csi_volume_kind() {
        let root = tmp("filled-teardown");
        use rusternetes_kubelet::pod_dirs::plugin;
        for (p, v) in [
            (plugin::EMPTY_DIR, "scratch"),
            (plugin::CONFIG_MAP, "cfg"),
            (plugin::SECRET, "sec"),
            (plugin::DOWNWARD_API, "dapi"),
            (plugin::PROJECTED, "proj"),
        ] {
            seed_filled_volume(&root, "uid-gone", p, v);
        }
        // A live pod's identical volumes must not be touched.
        let live_dir = seed_filled_volume(&root, "uid-live", plugin::EMPTY_DIR, "scratch");
        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();

        let vm = vm(&root);
        let w = world_of(&vm, vec![pod_with_empty_dirs("uid-live", &["scratch"])]).await;
        w.restart().await;
        w.reconcile(2).await;
        vm.cleanup_orphaned_pod_dirs(&live);

        assert!(
            !rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-gone").exists(),
            "the orphan's directory must be reaped once its volumes are torn down"
        );
        assert!(live_dir.join("sub/data").exists(), "live pod untouched");
    }

    /// A pod still in the API but terminated no longer wants its volumes
    /// (`ShouldPodRuntimeBeRemoved`), so they are torn down as for a deleted
    /// pod.
    #[tokio::test]
    async fn a_terminated_pods_volumes_are_torn_down() {
        let root = tmp("filled-terminated");
        let dir = seed_filled_volume(
            &root,
            "uid-done",
            rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
            "scratch",
        );

        let vm = vm(&root);
        let w = world_of(&vm, vec![pod_with_empty_dirs("uid-done", &["scratch"])]).await;
        w.terminate("uid-done");
        w.restart().await;
        w.reconcile(2).await;

        assert!(!dir.exists());
    }

    /// A hostPath volume's `TearDown` does nothing (`host_path.go:272-274`),
    /// and a directory no plugin owns is left for the cleanup.
    #[tokio::test]
    async fn host_path_and_unowned_dirs_are_left_alone_by_teardown() {
        let root = tmp("hostpath-unowned");
        let hp = seed_filled_volume(
            &root,
            "uid-gone",
            rusternetes_kubelet::pod_dirs::plugin::HOST_PATH,
            "hp",
        );
        let other = seed_filled_volume(
            &root,
            "uid-gone",
            rusternetes_kubelet::pod_dirs::UNSUPPORTED_PLUGIN,
            "x",
        );

        let vm = vm(&root);
        let w = world_of(&vm, vec![]).await;
        w.restart().await;
        w.reconcile(2).await;

        assert!(hp.join("sub/data").exists());
        assert!(other.join("sub/data").exists());
    }

    #[test]
    fn a_pod_dir_with_no_live_pod_is_removed() {
        let root = tmp("orphan");
        seed_pod(&root, "uid-gone", "scratch");

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(
            !rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-gone").exists(),
            "an orphaned pod dir must be reaped"
        );
    }

    #[test]
    fn a_live_pods_dir_is_left_alone() {
        let root = tmp("live");
        seed_pod(&root, "uid-live", "scratch");

        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();
        vm(&root).cleanup_orphaned_pod_dirs(&live);

        assert!(
            rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
                &root,
                "uid-live",
                rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
                "scratch"
            )
            .exists(),
            "a live pod's volumes must survive the sweep"
        );
    }

    /// Only the orphan goes; a live pod sharing the sweep is untouched.
    #[test]
    fn the_sweep_removes_only_the_orphans() {
        let root = tmp("mixed");
        seed_pod(&root, "uid-live", "scratch");
        seed_pod(&root, "uid-gone", "scratch");

        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();
        vm(&root).cleanup_orphaned_pod_dirs(&live);

        assert!(rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-live").exists());
        assert!(!rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-gone").exists());
    }

    /// The property that makes the sweep safe to run unattended: a volume dir
    /// holding real content is removed with `rmdir`, which fails, so the data
    /// stays. Upstream relies on exactly this rather than a recursive delete
    /// (`kubelet_volumes.go:114-118`).
    #[test]
    fn content_left_in_a_volume_dir_is_never_deleted() {
        let root = tmp("content");
        let vol = seed_pod(&root, "uid-data", "scratch");
        let payload = vol.join("important.txt");
        std::fs::write(&payload, b"do not delete me").unwrap();

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(payload.exists(), "file content must survive the sweep");
        assert_eq!(
            std::fs::read(&payload).unwrap(),
            b"do not delete me",
            "and must be unmodified"
        );
        assert!(
            rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-data").exists(),
            "the pod dir must be kept while content remains under it"
        );
    }

    /// Non-`volumes` subdirs of the pod dir are reaped with the recursive
    /// remover, matching upstream's RemoveAllOneFilesystem pass.
    #[test]
    fn other_pod_subdirs_are_reaped() {
        let root = tmp("subdirs");
        seed_pod(&root, "uid-sub", "scratch");
        let containers =
            rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-sub").join("containers");
        std::fs::create_dir_all(&containers).unwrap();
        std::fs::write(containers.join("log"), b"x").unwrap();

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(!rusternetes_kubelet::pod_dirs::get_pod_dir(&root, "uid-sub").exists());
    }

    #[test]
    fn a_missing_pods_dir_is_not_an_error() {
        let root = tmp("empty");
        // No pods/ dir at all — nothing has run yet.
        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());
        assert!(rusternetes_kubelet::pod_dirs::list_pods_from_disk(&root)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn list_pods_from_disk_returns_the_uids_on_disk() {
        let root = tmp("list");
        seed_pod(&root, "uid-a", "v");
        seed_pod(&root, "uid-b", "v");

        let mut found = rusternetes_kubelet::pod_dirs::list_pods_from_disk(&root).unwrap();
        found.sort();
        assert_eq!(found, vec!["uid-a".to_string(), "uid-b".to_string()]);
    }
}

/// Task 10 (#1970): `persistentVolumeClaim` and `ephemeral` have no plugin of
/// their own — they resolve to a `PersistentVolume` and the PV's SOURCE picks
/// the plugin (`host_path.go:98-101`). These tests cover
/// `VolumeManager::resolve_persistent_volume` and the dispatch that follows
/// it in `create_volume`, with particular attention to the error strings
/// `lifecycle.rs::is_volume_wait_error` matches on byte-for-byte.
mod pvc_resolution_tests {
    use super::__parent::*;
    use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, Pod, Volume};
    use rusternetes_kubelet::volumes::*;
    use rusternetes_storage::StorageBackend;
    use serde_json::json;

    fn vm(storage: Option<Arc<StorageBackend>>) -> VolumeManager {
        VolumeManager::new_with_recording_empty_dir_mounts(
            std::env::temp_dir().to_string_lossy().to_string(),
            storage,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    /// `createVolumeSpec` (`desired_state_of_world_populator.go:475`, `:511`):
    /// an inline in-tree gcePersistentDisk is `IsMigratable`, so the spec the
    /// plugin lookup sees is the translated CSI PV, flagged `Migrated` and
    /// `InlineVolumeSpecForCSIMigration`, and the CSI plugin claims it.
    #[test]
    fn create_volume_spec_translates_a_migratable_inline_volume() {
        let manager = vm(None);
        let volume: Volume = serde_json::from_value(json!({
            "name": "pd", "gcePersistentDisk": {"pdName": "disk-1", "fsType": "ext4"}
        }))
        .unwrap();
        let owned = manager
            .create_volume_spec("ns", &volume, None, false)
            .unwrap();
        assert!(owned.migrated && owned.inline_volume_spec_for_csi_migration);
        let csi = owned.persistent_volume.as_ref().unwrap().spec.csi.as_ref();
        assert_eq!(csi.unwrap().driver, "pd.csi.storage.gke.io");
        let spec = owned.as_spec();
        assert_eq!(
            manager
                .plugin_mgr
                .find_plugin_by_spec(&spec)
                .unwrap()
                .name(),
            "kubernetes.io/csi"
        );
    }

    /// The claim arm (`:475`): a bound in-tree PV is translated with
    /// `InlineVolumeSpecForCSIMigration` false and `ReadOnly` carried over.
    #[test]
    fn create_volume_spec_translates_a_migratable_pv() {
        let manager = vm(None);
        let volume: Volume = serde_json::from_value(json!({
            "name": "c", "persistentVolumeClaim": {"claimName": "c", "readOnly": true}
        }))
        .unwrap();
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {"gcePersistentDisk": {"pdName": "disk-1"}}
        }))
        .unwrap();
        let owned = manager
            .create_volume_spec("ns", &volume, Some(&pv), true)
            .unwrap();
        assert!(owned.migrated && !owned.inline_volume_spec_for_csi_migration);
        assert!(owned.read_only);
        let csi = owned.persistent_volume.as_ref().unwrap().spec.csi.as_ref();
        assert_eq!(
            csi.unwrap().volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/zones/UNSPECIFIED/disks/disk-1")
        );
    }

    /// A non-migratable spec passes through untouched.
    #[test]
    fn create_volume_spec_leaves_a_non_migratable_volume_alone() {
        let manager = vm(None);
        let volume: Volume = serde_json::from_value(json!({"name": "e", "emptyDir": {}})).unwrap();
        let owned = manager
            .create_volume_spec("ns", &volume, None, false)
            .unwrap();
        assert!(!owned.migrated && !owned.inline_volume_spec_for_csi_migration);
        assert!(owned.persistent_volume.is_none());
    }

    /// A PVC-backed volume must reach the plugin that owns the PV's SOURCE.
    /// There is no pvc plugin upstream: hostPath claims it via the
    /// PersistentVolume arm of `CanSupport` (`host_path.go:98-101`).
    #[tokio::test]
    async fn a_bound_pvc_resolves_to_its_pv_and_dispatches_on_the_pv_source() {
        let storage = Arc::new(StorageBackend::new_memory());

        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                // The API server defaults it (`SetDefaults_PersistentVolumeSpec`);
                // `MountVolume` reads it (`CheckVolumeModeFilesystem`).
                "volumeMode": "Filesystem",
                "hostPath": {"path": "/mnt/data"}
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();

        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let manager = vm(Some(storage));
        let resolved = manager
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap();
        let resolved = resolved.expect("a bound PVC resolves to a PV");
        let spec = rusternetes_kubelet::volume_plugins::Spec {
            volume: &volume,
            persistent_volume: Some(&resolved),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let plugin = manager.plugin_mgr.find_plugin_by_spec(&spec).unwrap();
        assert_eq!(
            plugin.name(),
            rusternetes_kubelet::pod_dirs::plugin::HOST_PATH
        );

        // And create_volume must dispatch to the same plugin end-to-end.
        let path = manager.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(path, "/mnt/data");
    }

    /// #2782: a `readOnly: true` PVC over a hostPath PV must reach the CRI
    /// mount as read-only (`makeMounts`: `mustMountRO`, kubelet_pods.go:392),
    /// while a read-write claim and an inline hostPath stay read-write.
    #[tokio::test]
    async fn read_only_pvc_over_host_path_pv_yields_read_only_mount_attrs() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                // The API server defaults it (`SetDefaults_PersistentVolumeSpec`);
                // `MountVolume` reads it (`CheckVolumeModeFilesystem`).
                "volumeMode": "Filesystem",
                "hostPath": {"path": "/mnt/data"}
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "volumes": [
                    {"name": "ro", "persistentVolumeClaim": {"claimName": "claim-1", "readOnly": true}},
                    {"name": "rw", "persistentVolumeClaim": {"claimName": "claim-1"}},
                    {"name": "inline", "hostPath": {"path": "/mnt/inline"}}
                ],
                "containers": [{
                    "name": "c", "image": "i",
                    "volumeMounts": [
                        {"name": "ro", "mountPath": "/ro"},
                        {"name": "rw", "mountPath": "/rw"},
                        {"name": "inline", "mountPath": "/inline"}
                    ]
                }]
            }
        }))
        .unwrap();
        let manager = vm(Some(storage));
        manager.create_pod_volumes(&pod).await.unwrap();
        let attrs = manager.mount_attributes(&pod, &pod.spec.as_ref().unwrap().containers[0]);
        assert!(attrs["ro"].read_only);
        assert!(!attrs["rw"].read_only);
        assert!(!attrs["inline"].read_only);
        // hostPath is not Managed: never relabelled (host_path.go:180).
        assert!(attrs.values().all(|a| !a.selinux_relabel));
    }

    /// #2801: after a kubelet restart nothing ran `create_volume`, so the
    /// attribute record is empty. Upstream rebuilds a Mounter per volume
    /// (`reconstructVolume`, `reconciler/reconstruct_common.go:352`
    /// `plugin.NewMounter(volumeSpec, pod)`) and `makeMounts` reads
    /// `GetAttributes()` from it.
    #[tokio::test]
    async fn mount_attributes_are_reconstructed_without_create_pod_volumes() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                     "hostPath": {"path": "/mnt/data"}}
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": "pv-1",
                     "resources": {"requests": {"storage": "1Gi"}}}
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "volumes": [
                    {"name": "ro", "persistentVolumeClaim": {"claimName": "claim-1", "readOnly": true}},
                    {"name": "rw", "persistentVolumeClaim": {"claimName": "claim-1"}}
                ],
                "containers": [{"name": "c", "image": "i", "volumeMounts": [
                    {"name": "ro", "mountPath": "/ro"}, {"name": "rw", "mountPath": "/rw"}]}]
            }
        }))
        .unwrap();
        // A fresh manager: the restarted kubelet. No create_pod_volumes.
        let manager = vm(Some(storage));
        manager.reconstruct_mount_attributes(&pod).await;
        let attrs = manager.mount_attributes(&pod, &pod.spec.as_ref().unwrap().containers[0]);
        assert!(attrs["ro"].read_only, "ReadOnly forcing survives a restart");
        assert!(!attrs["rw"].read_only);
    }

    /// A re-reconstruction must not clobber a live record (and its
    /// `SELinuxLabeled` bit) written by `create_volume`.
    #[tokio::test]
    async fn reconstruct_keeps_an_existing_record() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-2"},
            "spec": {"volumes": [{"name": "e", "emptyDir": {}}],
                "containers": [{"name": "c", "image": "i",
                    "volumeMounts": [{"name": "e", "mountPath": "/e"}]}]}
        }))
        .unwrap();
        let manager = vm(None);
        manager
            .mounted
            .lock()
            .unwrap()
            .entry(("uid-2".into(), "e".into()))
            .or_default()
            .selinux_labeled = true;
        manager.reconstruct_mount_attributes(&pod).await;
        assert!(
            manager.mounted.lock().unwrap()[&("uid-2".to_string(), "e".to_string())]
                .selinux_labeled
        );
    }

    /// `createVolumeSpec` hands `pvcSource.ReadOnly` to
    /// `NewSpecFromPersistentVolume(pv, pvcReadOnly)`
    /// (`desired_state_of_world_populator.go:461`, `:588`); an inline volume's
    /// `NewSpecFromVolume` leaves it false (`plugins.go:549-553`).
    #[test]
    fn spec_read_only_comes_from_the_claim_source() {
        let ro: Volume = serde_json::from_value(json!({
            "name": "c", "persistentVolumeClaim": {"claimName": "c", "readOnly": true}
        }))
        .unwrap();
        let rw: Volume = serde_json::from_value(json!({
            "name": "c", "persistentVolumeClaim": {"claimName": "c"}
        }))
        .unwrap();
        let inline: Volume =
            serde_json::from_value(json!({"name": "h", "hostPath": {"path": "/x"}})).unwrap();
        assert!(pvc_read_only(&ro));
        assert!(!pvc_read_only(&rw));
        assert!(!pvc_read_only(&inline));
    }

    #[tokio::test]
    async fn a_non_claim_volume_resolves_to_no_pv() {
        let volume: Volume = serde_json::from_value(json!({
            "name": "scratch",
            "emptyDir": {}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"containers": []}
        }))
        .unwrap();

        assert!(vm(None)
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap()
            .is_none());
    }

    /// Preserved byte-for-byte: `lifecycle.rs::is_volume_wait_error` matches
    /// "not found in namespace" together with "PersistentVolumeClaim" to keep
    /// a pod in `ContainerCreating` rather than failing it outright.
    #[tokio::test]
    async fn a_missing_pvc_yields_the_exact_not_found_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "missing"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim missing not found in namespace ns"
        );
        assert!(rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err));
    }

    /// Preserved byte-for-byte: `lifecycle.rs::is_volume_wait_error` matches
    /// this exact string to keep the pod waiting instead of failing it.
    #[tokio::test]
    async fn an_unbound_pvc_yields_the_exact_not_bound_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "ns"},
            "spec": {"accessModes": ["ReadWriteOnce"],
                     "resources": {"requests": {"storage": "1Gi"}}}
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("ns"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim is not bound to a volume"
        );
        assert!(rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err));
    }

    /// #1981: upstream handles a generic ephemeral volume "the same way as a
    /// PVC reference" (`desired_state_of_world_populator.go:432-441`), so an
    /// unbound ephemeral claim must be a retriable volume wait exactly like an
    /// unbound PVC — not a start failure.
    #[tokio::test]
    async fn an_unbound_ephemeral_pvc_is_a_volume_wait_error() {
        let storage = Arc::new(StorageBackend::new_memory());
        let volume: Volume = serde_json::from_value(json!({
            "name": "scratch",
            "ephemeral": {"volumeClaimTemplate": {"spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}}}}}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim p-scratch is not bound to a volume"
        );
        assert!(rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err));
    }

    /// Ruling 3: `ephemeral` set but `volumeClaimTemplate` absent falls
    /// through both arms silently — upstream's structure (nested `if let`)
    /// is reproduced verbatim, not converted into an error. `create_volume`
    /// then hits the unsupported-kind fallback and gets an empty directory.
    #[tokio::test]
    async fn an_ephemeral_volume_without_a_template_resolves_to_no_pv() {
        let volume: Volume = serde_json::from_value(json!({
            "name": "eph",
            "ephemeral": {}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"containers": []}
        }))
        .unwrap();

        assert!(vm(None)
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap()
            .is_none());
    }

    /// Ruling 2: a PV with no claimable source (today, anything but hostPath)
    /// must surface the pre-registry message verbatim, not the registry's own
    /// "no volume plugin matched" — `lifecycle.rs:486` documents this exact
    /// string as a worked example of a non-wait error.
    #[tokio::test]
    async fn a_pv_without_a_hostpath_source_yields_the_preserved_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        // No source field on the PV at all — deliberately not a CSI source
        // (that specific case is not this test's concern); any PV with no
        // claimable source must produce this message.
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"]
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();

        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .create_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to get Plugin from volumeSpec for volume \"claimed\" err=no volume plugin matched"
        );
    }
}

/// Task 11 (#1970): `create_volume`'s eight-branch `if let` chain becomes one
/// `find_plugin_by_spec` lookup. These pin the #1967 invariant directly: the
/// directory a volume is created in (`create_volume`) must be the directory
/// every other code path looks for it in (`plugin_name_for_volume`, hence
/// `pod_volume_dir` and the `kubelet.rs` init-container-restart path map).
mod dispatch_tests {
    use super::__parent::*;
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volumes::*;
    use serde_json::json;

    fn vm() -> VolumeManager {
        VolumeManager::new_with_recording_empty_dir_mounts(
            std::env::temp_dir().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    fn volume(name: &str, source: serde_json::Value) -> Volume {
        let mut fields = source;
        fields
            .as_object_mut()
            .unwrap()
            .insert("name".to_string(), json!(name));
        serde_json::from_value(fields).unwrap()
    }

    /// Every implemented kind reaches its own plugin through the registry.
    /// `plugin_name_for_volume` and `create_volume` build the identical
    /// `Spec` and call the identical `find_plugin_by_spec`, so this is also
    /// the "same name the registry dispatches to" half of the #1967
    /// invariant — the other half is `an_unimplemented_kind_falls_back_to_unsupported`.
    #[test]
    fn each_kind_resolves_to_its_own_plugin() {
        let vm = vm();
        let cases: Vec<(Volume, &str)> = vec![
            (
                volume("a", json!({"emptyDir": {}})),
                rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
            ),
            (
                volume("b", json!({"configMap": {"name": "cm"}})),
                rusternetes_kubelet::pod_dirs::plugin::CONFIG_MAP,
            ),
            (
                volume("c", json!({"secret": {"secretName": "s"}})),
                rusternetes_kubelet::pod_dirs::plugin::SECRET,
            ),
            (
                volume("d", json!({"downwardAPI": {"defaultMode": 420}})),
                rusternetes_kubelet::pod_dirs::plugin::DOWNWARD_API,
            ),
            (
                volume("e", json!({"projected": {}})),
                rusternetes_kubelet::pod_dirs::plugin::PROJECTED,
            ),
            (
                volume("f", json!({"csi": {"driver": "d"}})),
                rusternetes_kubelet::pod_dirs::plugin::CSI,
            ),
            (
                volume("g", json!({"hostPath": {"path": "/tmp"}})),
                rusternetes_kubelet::pod_dirs::plugin::HOST_PATH,
            ),
        ];
        for (v, want) in cases {
            assert_eq!(vm.plugin_name_for_volume(&v), want, "volume {}", v.name);
        }
    }

    /// The storage-free kinds (emptyDir, hostPath, downwardAPI, csi, and
    /// projected with no sources) prove `create_volume` itself — not just
    /// `plugin_name_for_volume` — routes through the same single
    /// `find_plugin_by_spec` call and lands the mount under the matched
    /// plugin's directory segment. configMap/secret/PVC dispatch through the
    /// identical call and are exercised with storage by
    /// `create_volume_writes_secret_data_to_disk`,
    /// `create_volume_writes_downward_api_data_to_disk`,
    /// `create_volume_writes_projected_sources_to_disk` and
    /// `pvc_resolution_tests`.
    #[tokio::test]
    async fn create_volume_dispatches_storage_free_kinds_to_their_plugin_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let cases: Vec<(Volume, &str)> = vec![
            (
                volume("a", json!({"emptyDir": {}})),
                rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR,
            ),
            (
                volume("d", json!({"downwardAPI": {"defaultMode": 420}})),
                rusternetes_kubelet::pod_dirs::plugin::DOWNWARD_API,
            ),
            // `projected` is not storage-free: upstream's SetUpAt fails with
            // "kube client is not configured" without a client, so its
            // plugin-dir dispatch is asserted by the projected tests.
            // An inline `csi` volume needs a registered driver (#3262), so
            // its dispatch is covered by the csi plugin tests.
            (
                volume("g", json!({"hostPath": {"path": "/tmp"}})),
                rusternetes_kubelet::pod_dirs::plugin::HOST_PATH,
            ),
        ];
        for (v, want) in cases {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default", "uid": format!("uid-{}", v.name)},
                "spec": {"containers": []}
            }))
            .unwrap();
            let path = vm.create_volume(&pod, &v).await.unwrap();
            // hostPath's mounter returns the host path itself rather than a
            // pod-dir-derived path, so this row asserts the literal instead
            // of the plugin-dir shape asserted below. The fixture is an
            // inline hostPath with no `$`, so its env expansion is a no-op.
            if want == rusternetes_kubelet::pod_dirs::plugin::HOST_PATH {
                assert_eq!(path, "/tmp");
            } else {
                assert!(
                    path.contains(&rusternetes_kubelet::pod_dirs::escape_qualified_name(want)),
                    "volume {} took path {path}, want it to contain the {want} plugin segment",
                    v.name
                );
            }
        }
    }

    /// `nfs.ProbeVolumePlugins` is registered by the kubelet
    /// (`cmd/kubelet/app/plugins.go`), so an `nfs` volume resolves to the
    /// `kubernetes.io/nfs` plugin (`pkg/volume/nfs/nfs.go:66`) instead of
    /// failing with `no volume plugin matched` (#3286).
    #[test]
    fn an_nfs_volume_is_claimed_by_the_nfs_plugin() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume(
            "n",
            json!({"nfs": {"server": "10.0.0.1", "path": "/export"}}),
        );
        assert_eq!(vm.plugin_name_for_volume(&v), "kubernetes.io/nfs");
        assert!(vm
            .plugin_mgr
            .find_plugin_by_name("kubernetes.io/nfs")
            .is_ok());
    }

    /// `git_repo.ProbeVolumePlugins` is registered by the kubelet
    /// (`cmd/kubelet/app/plugins.go:61`), so a `gitRepo` volume resolves to
    /// the `kubernetes.io/git-repo` plugin (`git_repo.go:53`) instead of
    /// failing with `no volume plugin matched` (#3289).
    #[test]
    fn a_git_repo_volume_is_claimed_by_the_git_repo_plugin() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume("g", json!({"gitRepo": {"repository": "https://x/y.git"}}));
        assert_eq!(vm.plugin_name_for_volume(&v), "kubernetes.io/git-repo");
        assert!(vm
            .plugin_mgr
            .find_plugin_by_name("kubernetes.io/git-repo")
            .is_ok());
    }

    /// `iscsi.ProbeVolumePlugins` is registered by the kubelet
    /// (`cmd/kubelet/app/plugins.go`), so an `iscsi` volume resolves to the
    /// `kubernetes.io/iscsi` plugin (`pkg/volume/iscsi/iscsi.go:59`) instead
    /// of failing with `no volume plugin matched` (#3287).
    #[test]
    fn an_iscsi_volume_is_claimed_by_the_iscsi_plugin() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume(
            "i",
            json!({"iscsi": {"targetPortal": "10.0.0.1", "iqn": "iqn.x", "lun": 0}}),
        );
        assert_eq!(vm.plugin_name_for_volume(&v), "kubernetes.io/iscsi");
        assert!(vm
            .plugin_mgr
            .find_plugin_by_name("kubernetes.io/iscsi")
            .is_ok());
    }

    /// #3492: a reconstructed iscsi global mount round-trips through
    /// `GetDeviceMountPath` to the very directory it came from (so the
    /// desired-state comparison and `UnmountDevice` act on the same path) and
    /// is recorded as globally mounted.
    #[tokio::test]
    async fn iscsi_global_mount_reconstructs() {
        use rusternetes_kubelet::volume_plugins::util::operation_executor::DeviceMountState;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_string_lossy().to_string();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            root.clone(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let global = std::path::Path::new(&root)
            .join("plugins/kubernetes.io/iscsi/iface-default")
            .join("10.0.0.1:3260-iqn.2017-05.com.example:test-lun-0");
        std::fs::create_dir_all(&global).unwrap();
        let global_str = global.to_string_lossy().to_string();

        vm.reconstruct_iscsi_device(&global_str);
        let attached = vm.asw.get_attached_volumes();
        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceGloballyMounted
        );
        assert_eq!(attached[0].device_mount_path, global_str);
        assert_eq!(attached[0].plugin_name, "kubernetes.io/iscsi");
        // Reconstructing again keeps the one entry.
        vm.reconstruct_iscsi_device(&global_str);
        assert_eq!(vm.asw.get_attached_volumes().len(), 1);
    }

    /// `fc.ProbeVolumePlugins` is registered by the kubelet
    /// (`cmd/kubelet/app/plugins.go`), so an `fc` volume resolves to the
    /// `kubernetes.io/fc` plugin (`pkg/volume/fc/fc.go:56`) instead of
    /// failing with `no volume plugin matched` (#3288).
    #[test]
    fn an_fc_volume_is_claimed_by_the_fc_plugin() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume(
            "f",
            json!({"fc": {"targetWWNs": ["500a0981891b8dc5"], "lun": 0}}),
        );
        assert_eq!(vm.plugin_name_for_volume(&v), "kubernetes.io/fc");
        assert!(vm
            .plugin_mgr
            .find_plugin_by_name("kubernetes.io/fc")
            .is_ok());
    }

    /// `image.ProbeVolumePlugins` is registered by the kubelet
    /// (`cmd/kubelet/app/plugins.go`), so an `image` volume resolves to the
    /// `kubernetes.io/image` stub plugin (`pkg/volume/image/image.go:41`)
    /// instead of failing with `no volume plugin matched` (#3290), and
    /// provisioning it makes no directory: `GetPath` is empty (`image.go:83`).
    #[tokio::test]
    async fn an_image_volume_is_claimed_by_the_image_plugin() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume("i", json!({"image": {"reference": "quay.io/a/b:1"}}));
        assert_eq!(vm.plugin_name_for_volume(&v), "kubernetes.io/image");
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-image"},
            "spec": {"containers": []}
        }))
        .unwrap();
        assert_eq!(vm.create_volume(&pod, &v).await.unwrap(), "");
        assert!(!tmp.path().join("pods").exists(), "no directory is made");
    }

    /// #3262: an unimplemented kind (rbd, gitRepo) matches no
    /// plugin. Upstream's desired-state populator fails the volume with
    /// `failed to get Plugin from volumeSpec for volume %q err=%v`
    /// (`desired_state_of_world.go:272-277`) over `ErrNoPluginMatched`
    /// (`plugins.go:72`), and the pod waits in ContainerCreating. No empty
    /// directory may be provisioned, and the error must classify as a wait.
    #[tokio::test]
    async fn an_unimplemented_kind_fails_with_no_volume_plugin_matched() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-rbd"},
            "spec": {"containers": [{"name": "c", "image": "i",
                "volumeMounts": [{"name": "rbd-vol", "mountPath": "/m"}]}],
                "volumes": [{"name": "rbd-vol", "rbd": {"monitors": ["10.0.0.1"], "image": "img"}}]}
        }))
        .unwrap();

        let err = vm.create_pod_volumes(&pod).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to get Plugin from volumeSpec for volume \"rbd-vol\" err=no volume plugin matched"
        );
        assert!(rusternetes_kubelet::lifecycle::is_volume_wait_anyhow(&err));
        assert!(!tmp.path().join("pods").exists(), "no fallback dir");
    }

    /// Upstream only processes volumes a container mounts
    /// (`desired_state_of_world_populator.go:293-298`, "Skipping unused
    /// volume"), so an unused unsupported volume does not fail the pod.
    #[tokio::test]
    async fn an_unused_unimplemented_kind_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-rbd"},
            "spec": {"containers": [{"name": "c", "image": "i"}],
                "volumes": [{"name": "rbd-vol", "rbd": {"monitors": ["s"], "image": "img"}}]}
        }))
        .unwrap();
        assert!(vm.create_pod_volumes(&pod).await.unwrap().is_empty());
    }

    /// The case the ordered if-else this replaced silently resolved: two
    /// sources set, first arm wins. The registry rejects it
    /// (`pkg/volume/plugins.go:661-663`), and `create_volume` now propagates
    /// that error instead of mounting either source.
    #[tokio::test]
    async fn a_volume_with_two_sources_is_rejected() {
        let mut v = volume("malformed", json!({"emptyDir": {}}));
        v.config_map = Some(serde_json::from_value(json!({"name": "cm"})).unwrap());

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-malformed"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm().create_volume(&pod, &v).await.unwrap_err();
        assert!(
            err.to_string().contains("multiple volume plugins matched"),
            "{err}"
        );
    }
}
