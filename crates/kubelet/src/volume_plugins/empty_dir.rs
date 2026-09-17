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
        // ---- moved verbatim from create_volume's emptyDir branch
        //      (991a503d:crates/kubelet/src/volumes.rs:931-953) ----
        let volume_dir = &self.path;
        let empty_dir = &self.empty_dir;
        // K8s setupDir does best-effort chmod on emptyDir directories.
        // A failed chmod must never block the volume mount.
        let _ = crate::runtime::setup_emptydir_dir(volume_dir);

        // Memory-medium emptyDir is a tmpfs. Mount it on the host volume dir
        // (propagated to the host daemon via the kubelet's rshared bind) so
        // it persists across container restarts AND reports fs_type=tmpfs.
        // K8s ref: pkg/volume/emptydir/empty_dir.go.
        let is_memory = empty_dir.medium.as_deref() == Some("Memory");
        if is_memory {
            let size_bytes = empty_dir
                .size_limit
                .as_deref()
                .and_then(crate::runtime::parse_quantity_bytes);
            crate::runtime::mount_tmpfs_for_emptydir(volume_dir, size_bytes);
        }
        info!(
            "Created emptyDir volume {} at {}",
            self.volume_name, volume_dir
        );
        // ---- end moved body ----
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
}
