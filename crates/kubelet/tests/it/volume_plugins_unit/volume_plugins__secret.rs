//! Unit tests moved from `src/volume_plugins/secret.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{KeyToPath, Pod, Secret, SecretVolumeSource};
    pub use rusternetes_kubelet::atomic_writer::FileProjection;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, Spec, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::collections::BTreeMap;
    pub use std::sync::Arc;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{Pod, Volume};
    use rusternetes_kubelet::volume_plugins::secret::*;
    use serde_json::json;

    fn plugin() -> SecretPlugin {
        SecretPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_secret(name: &str) -> Volume {
        serde_json::from_value(json!({"name": "sec", "secret": {"secretName": name}})).unwrap()
    }

    fn config_map_volume() -> Volume {
        serde_json::from_value(json!({"name": "cfg", "configMap": {"name": "x"}})).unwrap()
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    #[test]
    fn supports_an_inline_secret() {
        let v = inline_secret("x");
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
        let v = config_map_volume();
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
            rusternetes_kubelet::pod_dirs::plugin::SECRET
        );
    }

    /// The mounter's path comes from the pod-directory layout the host
    /// provides, keyed on the plugin name — same contract as every other
    /// plugin (host.rs `pod_volume_dir_matches_pod_dirs`).
    #[tokio::test]
    async fn get_path_uses_the_secret_plugin_directory() {
        let v = inline_secret("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(
            m.get_path(),
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~secret/sec"
        );
    }

    // ---- set_up behaviour, ported from upstream secret_test.go ----

    /// Build a plugin whose host serves `secret` from an in-memory store,
    /// rooted at `base`.
    async fn plugin_with_secret(base: &str, secret: Option<Secret>) -> SecretPlugin {
        let storage = Arc::new(StorageBackend::new_memory());
        if let Some(s) = secret {
            let key = build_key("secrets", Some("default"), &s.metadata.name);
            storage.create(&key, &s).await.unwrap();
        }
        SecretPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                base.to_string(),
                Some(storage),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn opaque(name: &str, data: &[(&str, &[u8])]) -> Secret {
        let mut s = Secret::new(name, "default");
        s.data = Some(
            data.iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect(),
        );
        s
    }

    fn volume_with_items(items: serde_json::Value, optional: bool) -> Volume {
        serde_json::from_value(json!({"name": "sec", "secret": {
            "secretName": "s", "items": items, "optional": optional}}))
        .unwrap()
    }

    /// `MakePayload` (secret.go:219-232): a key named in `items` that the
    /// Secret lacks is an error unless the volume is optional.
    /// `TestPluginOptionalKeys` / `TestMakePayload` "no defaultMode" family.
    #[tokio::test]
    async fn required_item_missing_from_secret_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(json!([{"key": "nope", "path": "p"}]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(
            err.contains("references non-existent secret key: nope"),
            "{err}"
        );
    }

    /// Same, but optional: the missing key is skipped, setup succeeds
    /// (`TestPluginOptionalKeys`).
    #[tokio::test]
    async fn optional_item_missing_from_secret_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(
            json!([{"key": "nope", "path": "p"}, {"key": "a", "path": "a"}]),
            true,
        );
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
        assert!(!dir.join("p").exists());
    }

    /// `AtomicWriter` layout (atomic_writer.go): user-visible files are
    /// symlinks through `..data`, and re-running SetUp on unchanged content
    /// is a no-op, so the `..data` target must not change.
    #[tokio::test]
    async fn set_up_projects_through_atomic_writer_and_is_idempotent() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(json!([{"key": "a", "path": "a", "mode": 256}]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        let first = std::fs::read_link(dir.join("..data")).expect("..data symlink");
        assert!(std::fs::symlink_metadata(dir.join("a"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::metadata(dir.join("a"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read_link(dir.join("..data")).unwrap(), first);
    }

    async fn mounter_at(
        base: &std::path::Path,
        items: serde_json::Value,
        secret: Option<Secret>,
    ) -> Box<dyn Mounter> {
        let p = plugin_with_secret(base.to_str().unwrap(), secret).await;
        let v = volume_with_items(items, false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        p.new_mounter(&spec, &test_pod()).await.unwrap()
    }

    /// `TestInvalidPathSecret` (secret_test.go:365-417): `MakePayload` fails
    /// BEFORE `wrapped.SetUpAt` (secret.go:177-181), so the volume path must
    /// not exist afterwards ("Expected path %s to not exist").
    #[tokio::test]
    async fn invalid_path_secret_does_not_create_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "missing", "path": "missing"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        assert!(m.set_up().await.is_err());
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// `getSecret` fails before `wrapped.SetUpAt` too (secret.go:166-176).
    #[tokio::test]
    async fn missing_required_secret_does_not_create_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(tmp.path(), json!([]), None).await;
        assert!(m.set_up().await.is_err());
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// The wrapped emptyDir's `setupDir` (empty_dir.go:447-486) leaves the
    /// volume root at 0777 (same as downwardAPI).
    #[tokio::test]
    async fn volume_root_is_the_wrapped_empty_dir_mode() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a", "mode": 256}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(m.get_path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o777);
    }

    /// `TestPlugin` (secret_test.go:340-348): "secret volume should create its
    /// own empty wrapper path" -- the wrapped emptyDir's `SetReady`
    /// (empty_dir.go:284, `volumeutil.SetReady` util.go:93) creates
    /// `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`.
    #[tokio::test]
    async fn set_up_creates_the_wrapped_empty_dir_metadata_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        m.set_up().await.unwrap();
        let wrapper = tmp
            .path()
            .join("pods/uid-1/plugins/kubernetes.io~empty-dir/wrapped_sec");
        assert!(wrapper.is_dir(), "missing {}", wrapper.display());
        assert!(wrapper.join("ready").exists());
    }

    /// SetUpAt's `defer` (secret.go:183-200): a failed AtomicWriter runs
    /// `unmounter.TearDown()`, and emptyDir `TearDownAt` removes the volume.
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let dir = std::path::PathBuf::from(m.get_path());
        std::fs::create_dir_all(dir.join("..data_tmp/x")).unwrap();
        assert!(m.set_up().await.is_err());
        assert!(
            !dir.exists(),
            "volume dir must be removed after a failed write"
        );
    }

    /// `TestPluginReboot` (secret_test.go:419-470): the volume dir already
    /// exists (post-reboot state); SetUp still writes the data.
    #[tokio::test]
    async fn plugin_reboot_sets_up_over_an_existing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let dir = std::path::PathBuf::from(m.get_path());
        std::fs::create_dir_all(&dir).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
    }

    /// `MakeNestedMountpoints` (secret.go:181): a volume mounted beneath the
    /// secret volume gets its mountpoint directory created inside it.
    #[tokio::test]
    async fn nested_mountpoints_are_created_inside_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let p = plugin_with_secret(
            tmp.path().to_str().unwrap(),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let v = volume_with_items(json!([]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": [
                {"name": "sec", "mountPath": "/etc/sec"},
                {"name": "other", "mountPath": "/etc/sec/sub/dir"}
            ]}]}
        }))
        .unwrap();
        let m = p.new_mounter(&spec, &pod).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        assert!(dir.join("sub/dir").is_dir());
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
    }
}
