use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
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

    /// `GetVolumeName` (`csi_plugin.go:437-445`): `"<driver>^<volumeHandle>"`,
    /// where `^` is upstream's `volNameSep` (`csi_plugin.go:57`).
    ///
    /// Upstream resolves the source with `getPVSourceFromSpec`
    /// (`csi_util.go:165-174`), which rejects an inline `CSIVolumeSource`
    /// outright — an inline CSI volume is ephemeral and pod-scoped, so it never
    /// reaches the unique-name-from-spec branch. That rejection is ported
    /// verbatim even though this plugin only supports the inline arm today
    /// (see the struct doc): the error is upstream's answer, not a gap.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.csi.is_some() {
            return Err(anyhow!(
                "plugin.GetVolumeName failed to extract volume source from spec: unexpected api.CSIVolumeSource found in volume.Spec"
            ));
        }
        let Some(source) = spec.persistent_volume.and_then(|pv| pv.spec.csi.as_ref()) else {
            return Err(anyhow!(
                "plugin.GetVolumeName failed to extract volume source from spec: volume source not found in volume.Spec"
            ));
        };
        Ok(format!(
            "{}^{}",
            source.driver,
            source.volume_handle.as_deref().unwrap_or_default()
        ))
    }

    /// `RequiresRemount` (`csi_plugin.go:457-460`): upstream consults the
    /// CSIDriver lister and returns `false` when it is nil. We have no lister,
    /// which is exactly that nil case.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`csi_plugin.go:637-...`): upstream looks
    /// the driver up in the CSIDriver lister to read its `SELinuxMount` field.
    /// With no lister we cannot answer `true` for any driver, so this reports
    /// `false` — the same answer upstream gives for a driver that does not
    /// declare `seLinuxMount: true`, which is the default.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
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
