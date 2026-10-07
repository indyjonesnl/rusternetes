use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{EmptyDirVolumeSource, Pod};
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/emptydir/empty_dir.go`.
pub struct EmptyDirPlugin {
    host: Arc<dyn VolumeHost>,
}

impl EmptyDirPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for EmptyDirPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::EMPTY_DIR
    }

    /// `GetVolumeName` (`empty_dir.go:84-92`): the user-defined volume name,
    /// because this is an ephemeral volume type.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.empty_dir.is_none() {
            return Err(anyhow!("spec does not reference an emptyDir volume type"));
        }
        Ok(spec.name().to_string())
    }

    /// `RequiresRemount` (`empty_dir.go:98-100`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`empty_dir.go:106-108`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`empty_dir.go:94`). emptyDir has no PV form, so only the
    /// inline arm is checked.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.empty_dir.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(EmptyDirMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            empty_dir: spec
                .volume
                .empty_dir
                .clone()
                .expect("checked by can_support"),
        }))
    }
}

/// `perm` (`empty_dir.go:50`): `const perm os.FileMode = 0777`.
#[cfg(unix)]
const PERM: u32 = 0o777;

#[derive(Debug, PartialEq, Eq)]
enum Medium {
    Default,
    Memory,
    /// `v1helper.IsHugePageMedium`. Upstream mounts hugetlbfs (`setupHugepages`);
    /// we do not yet, so it gets a plain directory as before (tracked separately).
    HugePages,
}

/// The medium switch of `SetUpAt` (`empty_dir.go:268-277`):
/// `default: err = fmt.Errorf("unknown storage medium %q", ed.medium)`.
/// `IsHugePageMedium` is `HugePages` or the `HugePages-` prefix
/// (`pkg/apis/core/v1/helper/helpers.go`).
fn classify_medium(medium: Option<&str>) -> Result<Medium> {
    match medium.unwrap_or("") {
        "" => Ok(Medium::Default),
        "Memory" => Ok(Medium::Memory),
        m if m == "HugePages" || m.starts_with("HugePages-") => Ok(Medium::HugePages),
        m => Err(anyhow!("unknown storage medium {:?}", m)),
    }
}

/// `setupDir` (`empty_dir.go:447-486`): MkdirAll, Lstat, chmod to `perm` when
/// the mode differs (umask), and return every error. The old helper swallowed
/// them.
pub(crate) fn setup_dir(dir: &str) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::symlink_metadata(dir)?.permissions().mode() & 0o777;
        if mode != PERM {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(PERM))?;
            let after = std::fs::symlink_metadata(dir)?.permissions().mode() & 0o777;
            if after != PERM {
                tracing::error!(
                    "Expected directory {:?} permissions to be: {:o}; got: {:o}",
                    dir,
                    PERM,
                    after
                );
            }
        }
    }
    Ok(())
}

struct EmptyDirMounter {
    path: String,
    volume_name: String,
    empty_dir: EmptyDirVolumeSource,
}

#[async_trait]
impl Mounter for EmptyDirMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        let volume_dir = &self.path;
        let empty_dir = &self.empty_dir;
        // SetUpAt (empty_dir.go:268-277): the medium switch. Anything that is
        // not Default, Memory or a huge-page medium is an error.
        let medium = classify_medium(empty_dir.medium.as_deref())?;
        // setupDir's errors are returned by upstream, not swallowed.
        setup_dir(volume_dir)?;

        // Memory-medium emptyDir is a tmpfs. Mount it on the host volume dir
        // (propagated to the host daemon via the kubelet's rshared bind) so
        // it persists across container restarts AND reports fs_type=tmpfs.
        // K8s ref: pkg/volume/emptydir/empty_dir.go setupTmpfs.
        if medium == Medium::Memory {
            let size_bytes = empty_dir
                .size_limit
                .as_deref()
                .and_then(crate::runtime::parse_quantity_bytes);
            crate::runtime::mount_tmpfs_for_emptydir(volume_dir, size_bytes)?;
        }
        info!(
            "Created emptyDir volume {} at {}",
            self.volume_name, volume_dir
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Volume;
    use serde_json::json;

    fn plugin() -> EmptyDirPlugin {
        EmptyDirPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    #[test]
    fn supports_an_inline_empty_dir() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v: Volume =
            serde_json::from_value(json!({"name": "cfg", "configMap": {"name": "x"}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::EMPTY_DIR);
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("emptydir-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn unknown_medium_is_an_error() {
        let e = classify_medium(Some("Bogus")).unwrap_err().to_string();
        assert_eq!(e, "unknown storage medium \"Bogus\"");
        assert_eq!(classify_medium(None).unwrap(), Medium::Default);
        assert_eq!(classify_medium(Some("")).unwrap(), Medium::Default);
        assert_eq!(classify_medium(Some("Memory")).unwrap(), Medium::Memory);
        assert_eq!(
            classify_medium(Some("HugePages-2Mi")).unwrap(),
            Medium::HugePages
        );
    }

    #[test]
    fn setup_dir_returns_mkdir_errors() {
        let root = tmp("mkdirerr");
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("file");
        std::fs::write(&file, "x").unwrap();
        let target = file.join("vol");
        assert!(setup_dir(target.to_str().unwrap()).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn setup_dir_fixes_mode_to_0777() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmp("mode");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
        setup_dir(d.to_str().unwrap()).unwrap();
        let m = std::fs::metadata(&d).unwrap().permissions().mode() & 0o777;
        assert_eq!(m, 0o777);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `setupTmpfs` (`empty_dir.go:324-362`) returns the error from
    /// `MountSensitiveWithoutSystemd`; a failed tmpfs mount fails SetUp
    /// instead of silently degrading to a persistent directory. The mount
    /// point does not exist, so `mount(8)` fails whether or not we are root.
    #[test]
    fn tmpfs_mount_failure_is_returned() {
        let r = crate::runtime::mount_tmpfs_for_emptydir("/nonexistent/emptydir/mountpoint", None);
        assert!(r.is_err());
    }
}
