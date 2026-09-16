use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Volume};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/downwardapi/downwardapi.go`.
pub struct DownwardApiPlugin {
    host: Arc<dyn VolumeHost>,
}

impl DownwardApiPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for DownwardApiPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::DOWNWARD_API
    }

    /// `CanSupport` (`downwardapi.go:79-81`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.DownwardAPI != nil
    /// ```
    ///
    /// downwardAPI has no PersistentVolume form — upstream's `CanSupport` has
    /// no `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as emptyDir/configMap/secret.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.downward_api.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(DownwardApiMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            // The whole Volume is cloned (not just its `downward_api` source)
            // because the final log line names `volume.name`, and the whole
            // Pod is cloned because a `fieldRef` can name any of its fields —
            // unlike Task 5/6's mounters, which read only namespace and name.
            volume: spec.volume.clone(),
            pod: pod.clone(),
            // `GetNodeAllocatable` (`pkg/volume/plugins.go:397`) — this is the
            // only plugin that calls it, since only a `resourceFieldRef` needs
            // the node's allocatable to default an unset limit.
            node_allocatable: self.host.get_node_allocatable().clone(),
        }))
    }
}

struct DownwardApiMounter {
    path: String,
    volume: Volume,
    pod: Pod,
    node_allocatable: HashMap<String, String>,
}

#[async_trait]
impl Mounter for DownwardApiMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's downwardAPI branch (d129cc63) ----
        let downward_api = self
            .volume
            .downward_api
            .as_ref()
            .expect("checked by can_support");

        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create DownwardAPI volume directory")?;

        // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
        let da_default_mode = downward_api.default_mode.unwrap_or(0o644);

        // Compute final directory permissions (applied after files are written)
        #[cfg(unix)]
        let da_dir_mode = da_default_mode as u32 | 0o111;

        if let Some(items) = &downward_api.items {
            for item in items {
                let file_path = format!("{}/{}", volume_dir, item.path);

                // Create parent directories if needed
                if let Some(parent) = std::path::Path::new(&file_path).parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Get the value from field_ref or resource_field_ref
                let value = if let Some(field_ref) = &item.field_ref {
                    crate::downward_api::resolve_pod_field(&self.pod, &field_ref.field_path)
                        .map_err(|e| anyhow::anyhow!("{e}"))?
                } else if let Some(resource_ref) = &item.resource_field_ref {
                    crate::downward_api::resolve_container_resource(
                        &self.pod,
                        resource_ref,
                        Some(&self.node_allocatable),
                    )
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                } else {
                    return Err(anyhow::anyhow!(
                        "DownwardAPI item must have either fieldRef or resourceFieldRef"
                    ));
                };

                std::fs::write(&file_path, value)
                    .with_context(|| format!("Failed to write DownwardAPI file {}", file_path))?;

                // Set file permissions: per-item mode overrides defaultMode
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = item.mode.unwrap_or(da_default_mode) as u32;
                    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(mode))?;
                }

                info!(
                    "Wrote DownwardAPI file {} with value from {}",
                    file_path, item.path
                );
            }
        }

        // Set directory permissions after files are written so that restrictive
        // defaultMode values don't prevent file creation.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(volume_dir, std::fs::Permissions::from_mode(da_dir_mode))?;
        }

        info!(
            "Created DownwardAPI volume {} at {}",
            self.volume.name, volume_dir
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

    fn plugin() -> DownwardApiPlugin {
        DownwardApiPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
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
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = empty_dir_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::DOWNWARD_API);
    }
}
