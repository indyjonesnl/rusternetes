use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use std::sync::Arc;
use tracing::info;

/// Port of the shape in `pkg/volume/csi/csi_plugin.go`.
///
/// Only the ephemeral-inline path is implemented, matching what
/// `create_volume` did: a placeholder directory for the CSI driver to
/// populate.
///
/// **Deliberate deviation:** Upstream's `csiPlugin.CanSupport`
/// (`csi_plugin.go:447-455`) checks both the PersistentVolume arm and the
/// inline arm. We deliberately implement only the inline arm (see
/// `can_support` below). The reason: this mounter creates a placeholder
/// directory and never calls `NodePublishVolume`, so claiming a
/// CSI-sourced PV would convert today's loud failure ("PersistentVolume
/// does not have a hostPath volume source") into a pod starting with an
/// empty volume — a behaviour change this pure refactor forbids. The PV
/// arm and `NodePublishVolume` must land together in the CSI fidelity
/// follow-up.
pub struct CsiPlugin {
    host: Arc<dyn VolumeHost>,
}

impl CsiPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for CsiPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::CSI
    }

    /// `CanSupport` — inline arm only. Upstream checks both
    /// (`csi_plugin.go:447-455`), but we only support the ephemeral-inline
    /// path; see the struct-level doc comment for the reason.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.csi.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(CsiMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
        }))
    }
}

struct CsiMounter {
    path: String,
    volume_name: String,
}

#[async_trait]
impl Mounter for CsiMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's CSI branch
        //      (991a503d:crates/kubelet/src/volumes.rs:1449-1461) ----
        // CSI ephemeral inline volumes are managed by the CSI driver via the kubelet CSI plugin
        // For conformance, we create a placeholder directory and rely on the CSI driver to populate it
        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir).context("Failed to create CSI volume directory")?;

        info!(
            "Created CSI ephemeral volume {} at {} (managed by CSI driver)",
            self.volume_name, volume_dir
        );
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plugin() -> CsiPlugin {
        CsiPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    #[test]
    fn supports_an_inline_csi_volume() {
        let v: rusternetes_common::resources::Volume = serde_json::from_value(
            json!({"name": "csi-inline", "csi": {"driver": "example.com/csi"}}),
        )
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v: rusternetes_common::resources::Volume =
            serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::CSI);
    }
}
