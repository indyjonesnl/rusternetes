//! Unit tests moved from `src/volume_plugins/config_map.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{ConfigMap, ConfigMapVolumeSource, Pod};
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, Spec, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_kubelet::volumes::build_configmap_payload;
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::sync::Arc;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volume_plugins::config_map::*;
    use serde_json::json;

    /// A host rooted beside `dir`, so the wrapped emptyDir's ready dir
    /// (`<root>/pods/<uid>/plugins/...`) never lands in the volume itself.
    fn test_host(dir: &std::path::Path) -> Arc<dyn VolumeHost> {
        let root = format!("{}-kubelet-root", dir.to_string_lossy());
        Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root,
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        )
    }

    fn plugin() -> ConfigMapPlugin {
        ConfigMapPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_config_map(name: &str) -> Volume {
        serde_json::from_value(json!({"name": "cfg", "configMap": {"name": name}})).unwrap()
    }

    fn secret_volume() -> Volume {
        serde_json::from_value(json!({"name": "s", "secret": {"secretName": "x"}})).unwrap()
    }

    #[test]
    fn supports_an_inline_config_map() {
        let v = inline_config_map("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = secret_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(
            plugin().name(),
            rusternetes_kubelet::pod_dirs::plugin::CONFIG_MAP
        );
    }

    fn cm(pairs: &[(&str, &str)]) -> ConfigMap {
        serde_json::from_value(json!({
            "metadata": {"name": "cm"},
            "data": pairs.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>()
        }))
        .unwrap()
    }

    fn ktp(key: &str, path: &str) -> rusternetes_common::resources::KeyToPath {
        serde_json::from_value(json!({"key": key, "path": path})).unwrap()
    }

    /// `MakePayload` (`configmap.go:222-229`): a mapped key that is absent
    /// from `data` and `binaryData` is an error unless the source is optional.
    /// `TestMakePayload` "no sources"/"missing key" cases pin this.
    #[test]
    fn missing_mapped_key_is_an_error_unless_optional() {
        let c = cm(&[("a", "1")]);
        let items = vec![ktp("nope", "p")];
        let err = build_configmap_payload(&c, Some(&items), "cm", false, 0o644).unwrap_err();
        assert_eq!(
            err.to_string(),
            "configmap references non-existent config key: nope"
        );
        let ok = build_configmap_payload(&c, Some(&items), "cm", true, 0o644).unwrap();
        assert!(ok.is_empty());
    }

    /// `if len(mappings) == 0` (`configmap.go:207`): an EMPTY items list means
    /// "project every key", exactly like an absent one.
    #[test]
    fn empty_items_list_projects_every_key() {
        let c = cm(&[("a", "1"), ("b", "2")]);
        let items: Vec<rusternetes_common::resources::KeyToPath> = vec![];
        let payload = build_configmap_payload(&c, Some(&items), "cm", false, 0o644).unwrap();
        assert_eq!(payload.len(), 2);
    }

    /// `volume.Mounter` embeds `MetricsProvider` (`volume.go:162`): the
    /// configMap mounter answers `NewCachedMetrics(NewMetricsDu(path))`
    /// (`configmap.go:100`), reachable through the trait object, and a second
    /// call returns the cached result even after the directory changes.
    #[test]
    fn mounter_exposes_cached_du_metrics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f"), vec![0u8; 4096]).unwrap();
        let m: Box<dyn Mounter> = Box::new(mounter(dir.path(), false));
        let first = m.get_metrics().unwrap();
        assert!(first.used.is_some());
        std::fs::write(dir.path().join("g"), vec![0u8; 1 << 20]).unwrap();
        assert_eq!(m.get_metrics().unwrap(), first);
    }

    /// `if !(errors.IsNotFound(err) && optional)` (`configmap.go:168`): only a
    /// NotFound is tolerated for an optional ConfigMap; any other error fails.
    #[test]
    fn only_not_found_is_tolerated_when_optional() {
        use rusternetes_common::Error;
        assert!(tolerate_get_error(&Error::NotFound("x".into()), true));
        assert!(!tolerate_get_error(&Error::NotFound("x".into()), false));
        assert!(!tolerate_get_error(&Error::Storage("boom".into()), true));
    }

    fn mounter(dir: &std::path::Path, optional: bool) -> ConfigMapMounter {
        ConfigMapMounter {
            metrics: rusternetes_kubelet::volume_plugins::metrics_cached::cached_du(
                &dir.to_string_lossy(),
            ),
            path: dir.to_string_lossy().into_owned(),
            volume_name: "cfg".into(),
            namespace: "ns".into(),
            config_map: serde_json::from_value(json!({"name": "missing", "optional": optional}))
                .unwrap(),
            storage: Some(Arc::new(StorageBackend::Memory(Arc::new(
                rusternetes_storage::MemoryStorage::new(),
            )))),
            host: test_host(dir),
            pod: serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
                "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": [
                    {"name": "cfg", "mountPath": "/etc/cfg"},
                    {"name": "other", "mountPath": "/etc/cfg/sub/dir"}
                ]}]}
            }))
            .unwrap(),
        }
    }

    /// `MakeNestedMountpoints` (`configmap.go:220`): a volume mounted beneath
    /// the configMap volume gets its mountpoint directory created inside it.
    #[tokio::test]
    async fn nested_mountpoints_are_created_inside_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        mounter(&dir, true).set_up().await.unwrap();
        assert!(dir.join("sub/dir").is_dir());
    }

    /// `TestPluginOptional` (`configmap_test.go:447-537`): an optional
    /// ConfigMap that is NotFound still runs the AtomicWriter over an empty
    /// payload, so `..data` exists and points at an empty directory
    /// (`configmap.go:168-176` builds an empty ConfigMap, no early return).
    #[tokio::test]
    async fn optional_missing_configmap_still_creates_data_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        mounter(&dir, true).set_up().await.unwrap();
        let target = std::fs::read_link(dir.join("..data")).expect("..data symlink");
        let entries: Vec<_> = std::fs::read_dir(dir.join(target)).unwrap().collect();
        assert!(entries.is_empty());
    }

    /// `SetUpAt`'s deferred cleanup (`configmap.go:222-237`): when the
    /// AtomicWriter fails after the wrapped emptyDir SetUp, the volume is torn
    /// down (emptyDir `TearDownAt` removes the directory).
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        // A non-empty `..data_tmp` directory makes the writer's symlink step fail.
        std::fs::create_dir_all(dir.join("..data_tmp/x")).unwrap();
        assert!(mounter(&dir, true).set_up().await.is_err());
        assert!(
            !dir.exists(),
            "volume dir must be removed after a failed write"
        );
    }

    /// `configMapVolume.GetAttributes` (`pkg/volume/configmap/configmap.go:158-164`).
    #[tokio::test]
    async fn attributes_are_read_only_managed_relabel() {
        let v = inline_config_map("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(
            json!({"metadata": {"name": "p", "namespace": "default", "uid": "u"}, "spec": {"containers": []}}),
        )
        .unwrap();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(
            m.get_attributes(),
            rusternetes_kubelet::volume_plugins::plugin::Attributes {
                read_only: true,
                managed: true,
                selinux_relabel: true
            }
        );
    }

    /// `TestPlugin` (`configmap_test.go:324-388`): `SetUp` with
    /// `mounterArgs.FsGroup` set projects the data, and `SetVolumeOwnership`
    /// runs over the whole volume (`configmap.go:246-252`, read-only
    /// attributes): root dir gets setgid + `roMask|execMask`, the symlinks stay
    /// symlinks, and the file reached through `..data` keeps `defaultMode`
    /// OR `roMask`.
    #[tokio::test]
    async fn fs_group_set_up_projects_data_and_applies_ownership() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        let gid = i64::from(std::fs::metadata(tmp.path()).unwrap().gid());
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cm", "namespace": "ns"},
            "data": {"data-1": "value-1", "data-2": "value-2"}
        }))
        .unwrap();
        storage
            .create(&build_key("configmaps", Some("ns"), "cm"), &cm)
            .await
            .unwrap();
        let m = ConfigMapMounter {
            metrics: rusternetes_kubelet::volume_plugins::metrics_cached::cached_du(""),
            path: dir.to_string_lossy().into_owned(),
            volume_name: "cfg".into(),
            namespace: "ns".into(),
            config_map: serde_json::from_value(json!({"name": "cm", "defaultMode": 0o600}))
                .unwrap(),
            storage: Some(storage),
            host: test_host(&dir),
            pod: serde_json::from_value(
                json!({"metadata": {"name": "p", "uid": "u"}, "spec": {"containers": []}}),
            )
            .unwrap(),
        };
        m.set_up_with(&rusternetes_kubelet::volume_plugins::MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("data-1")).unwrap(),
            "value-1"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("data-2")).unwrap(),
            "value-2"
        );
        assert!(std::fs::symlink_metadata(dir.join("data-1"))
            .unwrap()
            .file_type()
            .is_symlink());
        let root = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(root & 0o2000, 0o2000, "root dir must be setgid");
        assert_eq!(root & 0o550, 0o550, "root dir gets roMask|execMask");
        let real = std::fs::metadata(dir.join("data-1")).unwrap();
        assert_eq!(real.permissions().mode() & 0o777, 0o640, "0600 | roMask");
        assert_eq!(i64::from(real.gid()), gid);
    }
}
