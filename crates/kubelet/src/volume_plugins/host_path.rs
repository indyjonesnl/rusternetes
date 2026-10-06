use crate::runtime::check_host_path_type_msg;
use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::volume::HostPathType;
use rusternetes_common::resources::Pod;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/hostpath/host_path.go`.
pub struct HostPathPlugin {}

impl HostPathPlugin {
    /// Takes the host uniformly with every other plugin (Task 11's registry
    /// constructs plugins generically), but hostPath's path comes straight
    /// from the volume source, not from the pod directory layout the host
    /// provides — so nothing here reads it.
    pub fn new(_host: Arc<dyn VolumeHost>) -> Self {
        Self {}
    }
}

/// `HostPathType` (`common/src/resources/volume.rs:103-111`) has no
/// `#[serde(rename_all)]`, so its variant names already are the upstream
/// wire strings `check_host_path_type` matches on
/// (`v1.HostPathDirectoryOrCreate` = `"DirectoryOrCreate"`, etc. —
/// `staging/src/k8s.io/api/core/v1/types.go:797-825`). An explicit match is
/// used rather than a `serde_json::to_value` round trip: it is exhaustive
/// (a new variant fails to compile here instead of silently serialising to
/// something unexpected) and has no fallible/serialisation path to paper
/// over with `unwrap_or_default`.
fn host_path_type_as_str(t: &HostPathType) -> &'static str {
    match t {
        HostPathType::DirectoryOrCreate => "DirectoryOrCreate",
        HostPathType::Directory => "Directory",
        HostPathType::FileOrCreate => "FileOrCreate",
        HostPathType::File => "File",
        HostPathType::Socket => "Socket",
        HostPathType::CharDevice => "CharDevice",
        HostPathType::BlockDevice => "BlockDevice",
    }
}

#[async_trait]
impl VolumePlugin for HostPathPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::HOST_PATH
    }

    /// `GetVolumeName` (`host_path.go:89-96`): the host path itself, via
    /// `getVolumeSource` (`host_path.go:362-371`), which prefers the inline
    /// source and falls back to the PV's.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if let Some(source) = &spec.volume.host_path {
            return Ok(source.path.clone());
        }
        if let Some(source) = spec
            .persistent_volume
            .and_then(|pv| pv.spec.host_path.as_ref())
        {
            return Ok(source.path.clone());
        }
        Err(anyhow!("spec does not reference an HostPath volume type"))
    }

    /// `RequiresRemount` (`host_path.go:103-105`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`host_path.go:111-113`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`host_path.go:98-101`), both arms verbatim:
    ///
    /// ```go
    /// return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath != nil) ||
    ///     (spec.Volume != nil && spec.Volume.HostPath != nil)
    /// ```
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.persistent_volume
            .map(|pv| pv.spec.host_path.is_some())
            .unwrap_or(false)
            || spec.volume.host_path.is_some()
    }

    /// `NewMounter` (`host_path.go:121-145`) — `getVolumeSource`
    /// (`host_path.go:362-371`) reads the inline arm first and falls back to
    /// the PV arm; the two are never both set in practice (a PVC-backed
    /// `Volume` carries `persistentVolumeClaim`, not `hostPath`), so the
    /// order only matters for fidelity to upstream, not for behaviour here.
    async fn new_mounter(&self, spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
        let (path, path_type) = if let Some(hp) = spec.volume.host_path.as_ref() {
            // The path is used verbatim, as upstream does
            // (`host_path.go:139`: `&hostPath{path: path, ...}`
            // from `getVolumeSource`; no `os.ExpandEnv` anywhere in
            // `pkg/volume/` or `pkg/kubelet/`). The only upstream expansion
            // is `subPathExpr`, in `makeMounts`
            // (`pkg/kubelet/kubelet_pods.go:309-310`), with `$(VAR)`
            // syntax against the container env. The earlier kubelet-env
            // `$VAR` expansion defaulted unset names to "" (#1984).
            (hp.path.clone(), hp.type_.clone())
        } else if let Some(hp) = spec
            .persistent_volume
            .and_then(|pv| pv.spec.host_path.as_ref())
        {
            (
                hp.path.clone(),
                hp.r#type
                    .as_ref()
                    .map(host_path_type_as_str)
                    .map(String::from),
            )
        } else {
            return Err(anyhow::anyhow!(
                "hostPath plugin got a spec with no hostPath"
            ));
        };
        Ok(Box::new(HostPathMounter {
            path,
            path_type,
            volume_name: spec.volume.name.clone(),
        }))
    }
}

/// Port of `ValidatePathNoBacksteps`
/// (`pkg/volume/validation/pv_validation.go:62-71`): rejects any `/`-separated
/// element equal to `..`.
fn validate_path_no_backsteps(target_path: &str) -> std::result::Result<(), &'static str> {
    if target_path.split('/').any(|item| item == "..") {
        return Err("must not contain '..'");
    }
    Ok(())
}

struct HostPathMounter {
    path: String,
    path_type: Option<String>,
    volume_name: String,
}

#[async_trait]
impl Mounter for HostPathMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `hostPathMounter.SetUp` (`host_path.go:241-255`) — the type check runs
    /// for both arms, which is why a PV-backed hostPath is checked here where
    /// `create_volume`'s PVC branch did not check it. Sanctioned delta, see
    /// the plan.
    async fn set_up(&self) -> Result<()> {
        // `validation.ValidatePathNoBacksteps(b.GetPath())` runs BEFORE the
        // type check (`host_path.go:242-245`), so a path with a backstep is
        // rejected with this error, never a type error. #1973.
        if let Err(e) = validate_path_no_backsteps(&self.path) {
            return Err(anyhow!("invalid HostPath `{}`: {}", self.path, e));
        }
        // ---- moved verbatim from create_volume's hostPath branch
        //      (991a503d:crates/kubelet/src/volumes.rs:959-988) ----
        let path = &self.path;
        let host_path_type = self.path_type.as_deref();
        // `checkType` (`host_path.go:251-253`): upstream's exact error text.
        check_host_path_type_msg(path, host_path_type).map_err(|(_, msg)| anyhow!(msg))?;
        info!("Using hostPath volume {} at {}", self.volume_name, path);
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{check_host_path_type, HostPathCheck};
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;

    fn plugin() -> HostPathPlugin {
        HostPathPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
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
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_a_volume_with_neither_arm() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
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
}
