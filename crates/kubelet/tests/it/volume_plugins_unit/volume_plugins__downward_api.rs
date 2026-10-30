//! Unit tests moved from `src/volume_plugins/downward_api.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, bail, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{Pod, Volume};
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, Spec, VolumeHost, VolumePlugin,
    };
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volume_plugins::downward_api::*;
    use serde_json::json;

    fn plugin() -> DownwardApiPlugin {
        DownwardApiPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::from([("memory".to_string(), "2Gi".to_string())]),
            ),
        ))
    }

    fn inline_downward_api() -> Volume {
        serde_json::from_value(json!({"name": "podinfo", "downwardAPI": {}})).unwrap()
    }

    fn empty_dir_volume() -> Volume {
        serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap()
    }

    #[test]
    fn supports_an_inline_downward_api() {
        let v = inline_downward_api();
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
        let v = empty_dir_volume();
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
            rusternetes_kubelet::pod_dirs::plugin::DOWNWARD_API
        );
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "u1",
                         "labels": {"key1": "value1"}},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap()
    }

    fn mounter_for(dir: &std::path::Path, items: serde_json::Value) -> DownwardApiMounter {
        DownwardApiMounter {
            metrics: rusternetes_kubelet::volume_plugins::metrics_cached::cached_du(""),
            host: std::sync::Arc::new(
                rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                    "/var/lib/rusternetes".to_string(),
                    None,
                    rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                    HashMap::new(),
                ),
            ),
            path: dir.to_string_lossy().into_owned(),
            volume: serde_json::from_value(
                json!({"name": "podinfo", "downwardAPI": {"items": items, "defaultMode": 420}}),
            )
            .unwrap(),
            pod: test_pod(),
            node_allocatable: HashMap::new(),
        }
    }

    // downwardapi_test.go: test_write_with_two_consecutive_slashes_in_the_path
    // (CollectData: `fPath := filepath.Clean(fileInfo.Path)`).
    #[test]
    fn collect_data_cleans_the_path() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([
                {"path": "this//labels", "fieldRef": {"fieldPath": "metadata.labels"}}
            ]))
            .unwrap();
        let data = collect_data(&items, &test_pod(), &HashMap::new(), Some(0o644)).unwrap();
        assert!(data.contains_key("this/labels"), "{:?}", data.keys());
    }

    // CollectData appends every failure to `errlist` and returns
    // `utilerrors.NewAggregate(errlist)` rather than stopping at the first.
    #[test]
    fn collect_data_aggregates_all_errors() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([
                {"path": "a", "fieldRef": {"fieldPath": "metadata.bogusA"}},
                {"path": "b", "fieldRef": {"fieldPath": "metadata.bogusB"}}
            ]))
            .unwrap();
        let err = collect_data(&items, &test_pod(), &HashMap::new(), Some(0o644))
            .unwrap_err()
            .to_string();
        assert!(err.contains("bogusA") && err.contains("bogusB"), "{err}");
    }

    // An item with neither ref still gets `data[fPath] = fileProjection`
    // (empty data, resolved mode) upstream; it is not an error.
    #[test]
    fn collect_data_item_without_ref_is_an_empty_file() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([{"path": "empty", "mode": 256}])).unwrap();
        let data = collect_data(&items, &test_pod(), &HashMap::new(), Some(0o644)).unwrap();
        assert_eq!(data["empty"].data, Vec::<u8>::new());
        assert_eq!(data["empty"].mode, 0o400);
    }

    // SetUpAt projects through volumeutil.NewAtomicWriter: user-visible
    // entries are symlinks through `..data`, and re-SetUp of unchanged data
    // makes no filesystem change.
    #[tokio::test]
    async fn set_up_uses_the_atomic_writer_and_is_inert_on_resetup() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "name_file", "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let link = tmp.path().join("..data");
        assert!(std::fs::symlink_metadata(&link).is_ok(), "no ..data link");
        assert!(std::fs::symlink_metadata(tmp.path().join("name_file"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("name_file")).unwrap(),
            "p"
        );
        let before = std::fs::read_link(&link).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(before, std::fs::read_link(&link).unwrap());
    }

    #[tokio::test]
    async fn set_up_applies_item_mode_over_default() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "f", "mode": 256, "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(tmp.path().join("f"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o400);
    }

    fn read(dir: &std::path::Path, f: &str) -> String {
        std::fs::read_to_string(dir.join(f)).unwrap()
    }

    // downwardapi_test.go TestDownwardAPI test_labels/test_annotations:
    // `fieldpath.FormatMap` — sorted `k="v"` lines, value `%q`-quoted.
    #[tokio::test]
    async fn set_up_projects_labels_and_annotations() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = mounter_for(
            tmp.path(),
            json!([
                {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}},
                {"path": "annotations", "fieldRef": {"fieldPath": "metadata.annotations"}}
            ]),
        );
        m.pod.metadata.labels = Some(
            [("key2", "value2"), ("key1", "value1")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .into(),
        );
        m.pod.metadata.annotations = Some(
            [("a1", "value1"), ("multiline", "c\nb\na")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .into(),
        );
        m.set_up().await.unwrap();
        assert_eq!(
            read(tmp.path(), "labels"),
            "key1=\"value1\"\nkey2=\"value2\""
        );
        assert_eq!(
            read(tmp.path(), "annotations"),
            "a1=\"value1\"\nmultiline=\"c\\nb\\na\""
        );
    }

    // test_namespace
    #[tokio::test]
    async fn set_up_projects_namespace() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "ns_file", "fieldRef": {"fieldPath": "metadata.namespace"}}]),
        );
        m.set_up().await.unwrap();
        assert_eq!(read(tmp.path(), "ns_file"), "ns");
    }

    // test_write_with_unix_path + two consecutive slashes
    #[tokio::test]
    async fn set_up_writes_nested_paths_and_cleans_double_slashes() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([
                {"path": "these/are/my/labels", "fieldRef": {"fieldPath": "metadata.labels"}},
                {"path": "this//name", "fieldRef": {"fieldPath": "metadata.name"}}
            ]),
        );
        m.set_up().await.unwrap();
        assert_eq!(read(tmp.path(), "these/are/my/labels"), "key1=\"value1\"");
        assert_eq!(read(tmp.path(), "this/name"), "p");
    }

    // test_write_twice_with_update: changed data moves the `..data` link.
    #[tokio::test]
    async fn re_set_up_with_changed_data_moves_the_link() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = mounter_for(
            tmp.path(),
            json!([{"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}]),
        );
        m.set_up().await.unwrap();
        let before = std::fs::read_link(tmp.path().join("..data")).unwrap();
        m.pod
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("key3".into(), "value3".into());
        m.set_up().await.unwrap();
        assert_ne!(
            before,
            std::fs::read_link(tmp.path().join("..data")).unwrap()
        );
        assert_eq!(
            read(tmp.path(), "labels"),
            "key1=\"value1\"\nkey3=\"value3\""
        );
    }

    // `CollectData`: `if defaultMode == nil { return nil, fmt.Errorf("no
    // defaultMode used, not even the default value for it") }`. The API server
    // (and the static-pod decoder) default it, so nil never reaches here.
    #[tokio::test]
    async fn set_up_nil_default_mode_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let m = DownwardApiMounter {
            metrics: rusternetes_kubelet::volume_plugins::metrics_cached::cached_du(""),
            host: std::sync::Arc::new(
                rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                    "/var/lib/rusternetes".to_string(),
                    None,
                    rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                    HashMap::new(),
                ),
            ),
            path: tmp.path().to_string_lossy().into_owned(),
            volume: serde_json::from_value(json!({"name": "podinfo", "downwardAPI": {"items": [
                {"path": "f", "fieldRef": {"fieldPath": "metadata.name"}}]}}))
            .unwrap(),
            pod: test_pod(),
            node_allocatable: HashMap::new(),
        };
        let err = m.set_up().await.unwrap_err();
        assert!(
            format!("{err:#}").contains("no defaultMode used, not even the default value for it"),
            "{err:#}"
        );
        assert!(!tmp.path().join("f").exists());
    }

    // test_default_mode: verifyMode 0644 on the file.
    #[tokio::test]
    async fn set_up_default_mode_is_0644() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "f", "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(tmp.path().join("f"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o644);
    }

    // The wrapped emptyDir's `setupDir` (empty_dir.go:447-486) leaves the
    // volume root at `perm` 0777; `ChangePermissions` with a nil fsGroup does
    // not touch it. The old `defaultMode|0111` is gone.
    #[tokio::test]
    async fn volume_root_is_the_wrapped_empty_dir_mode() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        let m = mounter_for(
            &dir,
            json!([{"path": "f", "mode": 256, "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o777);
    }

    // SetUpAt's deferred cleanup (downwardapi.go:195-208): a failed
    // AtomicWriter tears the volume down (emptyDir TearDownAt removes it).
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        std::fs::create_dir_all(dir.join("..data_tmp/x")).unwrap();
        let m = mounter_for(
            &dir,
            json!([{"path": "f", "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        assert!(m.set_up().await.is_err());
        assert!(
            !dir.exists(),
            "volume dir must be removed after a failed write"
        );
    }

    // `CollectData` runs before `wrapped.SetUpAt` (downwardapi.go:179-189):
    // a bad item must not even create the volume directory.
    #[tokio::test]
    async fn collect_failure_does_not_create_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        let m = mounter_for(
            &dir,
            json!([{"path": "f", "fieldRef": {"fieldPath": "metadata.bogus"}}]),
        );
        assert!(m.set_up().await.is_err());
        assert!(!dir.exists());
    }

    async fn plugin_mounter(base: &std::path::Path, pod: Pod) -> Box<dyn Mounter> {
        let plugin = DownwardApiPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                base.to_string_lossy().into_owned(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        ));
        let v: Volume = serde_json::from_value(json!({
            "name": "podinfo",
            "downwardAPI": {"items": [{"path": "f", "fieldRef": {"fieldPath": "metadata.name"}}],
                            "defaultMode": 420}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        plugin.new_mounter(&spec, &pod).await.unwrap()
    }

    /// `newDownwardAPITest` (downwardapi_test.go): "downwardapi volume should
    /// create its own empty wrapper path" -- the wrapped emptyDir's `SetReady`
    /// (empty_dir.go:284) creates
    /// `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`.
    #[tokio::test]
    async fn set_up_creates_the_wrapped_empty_dir_metadata_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let m = plugin_mounter(tmp.path(), test_pod()).await;
        m.set_up().await.unwrap();
        let wrapper = tmp
            .path()
            .join("pods/u1/plugins/kubernetes.io~empty-dir/wrapped_podinfo");
        assert!(wrapper.is_dir(), "missing {}", wrapper.display());
        assert!(wrapper.join("ready").exists());
    }

    /// `volumeutil.MakeNestedMountpoints(b.volName, dir, *b.pod)`
    /// (downwardapi.go:191): a volume mounted beneath the downwardAPI volume
    /// gets its mountpoint directory created inside it.
    #[tokio::test]
    async fn nested_mountpoints_are_created_inside_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u1"},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": [
                {"name": "podinfo", "mountPath": "/etc/podinfo"},
                {"name": "other", "mountPath": "/etc/podinfo/sub/dir"}
            ]}]}
        }))
        .unwrap();
        let m = plugin_mounter(tmp.path(), pod).await;
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        assert!(dir.join("sub/dir").is_dir());
        assert_eq!(std::fs::read(dir.join("f")).unwrap(), b"p");
    }

    /// `downwardAPIVolume.GetAttributes` (`pkg/volume/downwardapi/downwardapi.go:154-160`).
    #[test]
    fn attributes_are_read_only_managed_relabel() {
        let m = mounter_for(std::path::Path::new("/tmp/x"), json!([]));
        assert_eq!(
            m.get_attributes(),
            rusternetes_kubelet::volume_plugins::plugin::Attributes {
                read_only: true,
                managed: true,
                selinux_relabel: true
            }
        );
    }
}
