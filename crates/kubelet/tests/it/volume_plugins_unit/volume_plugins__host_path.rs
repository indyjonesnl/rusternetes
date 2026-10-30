//! Unit tests moved from `src/volume_plugins/host_path.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::volume::HostPathType;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::runtime::check_host_path_type_msg;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, Spec, VolumeHost, VolumePlugin,
    };
    pub use std::sync::Arc;
    pub use tracing::info;
}

mod tests {
    /// `hostPath` embeds `volume.MetricsNil` (`host_path.go:215`, `:344`):
    /// the unmounter reports "not supported" through the `Unmounter`
    /// supertrait.
    #[test]
    fn unmounter_reports_metrics_not_supported() {
        use rusternetes_kubelet::volume_plugins::plugin::MetricsProvider;
        let want = rusternetes_kubelet::volume_plugins::util::metrics::NOT_SUPPORTED;
        let m = rusternetes_kubelet::volume_plugins::host_path::HostPathUnmounter;
        assert_eq!(m.get_metrics().unwrap_err().to_string(), want);
    }

    use super::__parent::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use rusternetes_kubelet::runtime::{check_host_path_type, HostPathCheck};
    use rusternetes_kubelet::volume_plugins::host_path::*;
    use serde_json::json;

    fn plugin() -> HostPathPlugin {
        HostPathPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_host_path(path: &str) -> Volume {
        serde_json::from_value(json!({"name": "hp", "hostPath": {"path": path}})).unwrap()
    }

    fn claimed_volume() -> Volume {
        serde_json::from_value(
            json!({"name": "claimed", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap()
    }

    fn pv_host_path(path: &str) -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {"hostPath": {"path": path}}
        }))
        .unwrap()
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    /// Upstream's CanSupport checks BOTH arms — a hostPath PV and an inline
    /// hostPath volume are the same plugin's work
    /// (`pkg/volume/hostpath/host_path.go:98-101`).
    #[test]
    fn supports_an_inline_host_path() {
        let v = inline_host_path("/tmp/x");
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
    fn supports_a_host_path_persistent_volume() {
        let v = claimed_volume();
        let pv = pv_host_path("/mnt/data");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_a_volume_with_neither_arm() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    /// The path comes from the source, not from the pod directory.
    #[tokio::test]
    async fn get_path_is_the_host_path_for_a_pv() {
        let v = claimed_volume();
        let pv = pv_host_path("/mnt/data");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/mnt/data");
    }

    /// `hostPathMounter.GetPath` returns the volume source's path verbatim
    /// (`pkg/volume/hostpath/host_path.go:230-232`, asserted by `TestPlugin`,
    /// `host_path_test.go:254-257`). Upstream expands environment variables
    /// nowhere in `pkg/volume/` (#1984): an unset name used to expand to ""
    /// and silently mount a different, existing directory.
    #[tokio::test]
    async fn the_inline_arm_does_not_expand_environment_variables() {
        let v = inline_host_path("/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
    }

    /// `host_path.go:242-245`: the backstep check precedes the type check, so
    /// even with an unsatisfiable type the validation error wins.
    #[tokio::test]
    async fn set_up_rejects_a_backstep_before_checking_the_type() {
        let v: Volume = serde_json::from_value(
            json!({"name": "hp", "hostPath": {"path": "/tmp/../etc", "type": "File"}}),
        )
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = plugin().new_mounter(&spec, &test_pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err().to_string();
        assert_eq!(err, "invalid HostPath `/tmp/../etc`: must not contain '..'");
    }

    #[test]
    fn backstep_validation_only_rejects_whole_dotdot_elements() {
        assert!(validate_path_no_backsteps("/a/..b/c..").is_ok());
        assert!(validate_path_no_backsteps("/a/b/..").is_err());
        assert!(validate_path_no_backsteps("../a").is_err());
    }

    /// The PV arm's counterpart to the test above: same unset var, but
    /// sourced from `pv.spec.host_path` instead of the inline volume, must
    /// come through unexpanded.
    #[tokio::test]
    async fn the_pv_arm_does_not_expand_environment_variables() {
        let v = claimed_volume();
        let pv = pv_host_path("/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
    }

    async fn set_up_err(path: &str, ty: &str) -> String {
        let v: Volume =
            serde_json::from_value(json!({"name": "hp", "hostPath": {"path": path, "type": ty}}))
                .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = plugin().new_mounter(&spec, &test_pod()).await.unwrap();
        m.set_up().await.unwrap_err().to_string()
    }

    /// `checkTypeInternal` error strings (`host_path.go:463-494`). A MISSING
    /// path fails the same way as a wrong-kind one: `IsDir()`/`IsFile()`
    /// return false when the path does not exist (`host_path.go:395-401`).
    #[tokio::test]
    async fn set_up_uses_upstream_type_check_error_strings() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("d");
        std::fs::create_dir(&dir).unwrap();
        let file = d.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let missing = d.path().join("nope");
        let (dir, file, missing) = (
            dir.to_str().unwrap(),
            file.to_str().unwrap(),
            missing.to_str().unwrap(),
        );
        let fail = "hostPath type check failed";
        assert_eq!(
            set_up_err(file, "Directory").await,
            format!("{fail}: {file} is not a directory")
        );
        assert_eq!(
            set_up_err(missing, "Directory").await,
            format!("{fail}: {missing} is not a directory")
        );
        assert_eq!(
            set_up_err(file, "DirectoryOrCreate").await,
            format!("{fail}: {file} is not a directory")
        );
        assert_eq!(
            set_up_err(dir, "File").await,
            format!("{fail}: {dir} is not a file")
        );
        assert_eq!(
            set_up_err(missing, "File").await,
            format!("{fail}: {missing} is not a file")
        );
        assert_eq!(
            set_up_err(dir, "FileOrCreate").await,
            format!("{fail}: {dir} is not a file")
        );
        assert_eq!(
            set_up_err(file, "Socket").await,
            format!("{fail}: {file} is not a socket file")
        );
        assert_eq!(
            set_up_err(file, "CharDevice").await,
            format!("{fail}: {file} is not a character device")
        );
        assert_eq!(
            set_up_err(file, "BlockDevice").await,
            format!("{fail}: {file} is not a block device")
        );
    }

    /// `default:` arm (`host_path.go:492`).
    #[tokio::test]
    async fn set_up_rejects_an_unknown_type_with_upstream_wording() {
        assert_eq!(
            set_up_err("/tmp", "Bogus").await,
            "Bogus is an invalid volume type"
        );
    }

    /// `GetFileType` uses `os.Stat` and `PathExists` follows symlinks
    /// (`hostutil.go:82`, `hostutil_linux.go:152-154`): a symlink to a
    /// directory IS a directory, a symlink to a file IS a file.
    #[cfg(unix)]
    #[test]
    fn type_check_follows_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("d");
        std::fs::create_dir(&dir).unwrap();
        let file = d.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let ld = d.path().join("ld");
        let lf = d.path().join("lf");
        std::os::unix::fs::symlink(&dir, &ld).unwrap();
        std::os::unix::fs::symlink(&file, &lf).unwrap();
        assert_eq!(
            check_host_path_type(ld.to_str().unwrap(), Some("Directory")),
            HostPathCheck::Ok
        );
        assert_eq!(
            check_host_path_type(lf.to_str().unwrap(), Some("File")),
            HostPathCheck::Ok
        );
        assert_eq!(
            check_host_path_type(ld.to_str().unwrap(), Some("DirectoryOrCreate")),
            HostPathCheck::Ok
        );
    }

    /// `makeDir` 0755 / `makeFile` 0644 (`host_path.go:504-526`), observed
    /// under the process umask (Go applies it too).
    #[cfg(unix)]
    #[test]
    fn or_create_uses_upstream_modes() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let sub = d.path().join("a/b");
        let f = d.path().join("f");
        assert_eq!(
            check_host_path_type(sub.to_str().unwrap(), Some("DirectoryOrCreate")),
            HostPathCheck::Ok
        );
        assert_eq!(
            check_host_path_type(f.to_str().unwrap(), Some("FileOrCreate")),
            HostPathCheck::Ok
        );
        let m = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(m(&sub), 0o755 & !current_umask());
        assert_eq!(m(&f), 0o644 & !current_umask());
    }

    #[cfg(unix)]
    fn current_umask() -> u32 {
        // /proc/self/status "Umask:" avoids a racy umask() write.
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("Umask:"))
                    .and_then(|v| u32::from_str_radix(v.trim(), 8).ok())
            })
            .unwrap_or(0o022)
    }

    /// Port of `TestPersistentClaimReadOnlyFlag`
    /// (`pkg/volume/hostpath/host_path_test.go:276-320`): the readOnly of
    /// `NewSpecFromPersistentVolume(pv, true)` reaches
    /// `mounter.GetAttributes().ReadOnly`, and hostPath is never `Managed` nor
    /// `SELinuxRelabel` (`host_path.go:232-238`).
    #[tokio::test]
    async fn persistent_claim_read_only_flag() {
        let v = claimed_volume();
        let pv = pv_host_path("foo");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: true,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = plugin().new_mounter(&spec, &test_pod()).await.unwrap();
        assert_eq!(
            m.get_attributes(),
            Attributes {
                read_only: true,
                managed: false,
                selinux_relabel: false
            }
        );
    }

    /// `NewSpecFromVolume` leaves `ReadOnly` false (`plugins.go:549-553`).
    #[tokio::test]
    async fn inline_host_path_is_not_read_only() {
        let v = inline_host_path("/tmp/x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = plugin().new_mounter(&spec, &test_pod()).await.unwrap();
        assert!(!m.get_attributes().read_only);
    }
}
