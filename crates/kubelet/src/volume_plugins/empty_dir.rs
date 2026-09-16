use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::Result;
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Volume};
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
            volume: spec.volume.clone(),
        }))
    }
}

struct EmptyDirMounter {
    path: String,
    volume: Volume,
}

#[async_trait]
impl Mounter for EmptyDirMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's emptyDir branch (b22ed559) ----
        let volume_dir = &self.path;
        let volume = &self.volume;
        // K8s setupDir does best-effort chmod on emptyDir directories.
        // A failed chmod must never block the volume mount.
        let _ = crate::runtime::setup_emptydir_dir(volume_dir);

        // Memory-medium emptyDir is a tmpfs. Mount it on the host volume dir
        // (propagated to the host daemon via the kubelet's rshared bind) so
        // it persists across container restarts AND reports fs_type=tmpfs.
        // K8s ref: pkg/volume/emptydir/empty_dir.go.
        let is_memory =
            volume.empty_dir.as_ref().and_then(|e| e.medium.as_deref()) == Some("Memory");
        if is_memory {
            let size_bytes = volume
                .empty_dir
                .as_ref()
                .and_then(|e| e.size_limit.as_deref())
                .and_then(crate::runtime::parse_quantity_bytes);
            crate::runtime::mount_tmpfs_for_emptydir(volume_dir, size_bytes);
        }
        info!("Created emptyDir volume {} at {}", volume.name, volume_dir);
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
