use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use crate::volumes::build_configmap_payload;
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{ConfigMap, ConfigMapVolumeSource, Pod};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/configmap/configmap.go`.
pub struct ConfigMapPlugin {
    host: Arc<dyn VolumeHost>,
}

impl ConfigMapPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for ConfigMapPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::CONFIG_MAP
    }

    /// `CanSupport` (`configmap.go:77-79`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.ConfigMap != nil
    /// ```
    ///
    /// configMap has no PersistentVolume form — upstream's `CanSupport` has no
    /// `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as emptyDir.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.config_map.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(ConfigMapMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            config_map: spec
                .volume
                .config_map
                .clone()
                .expect("checked by can_support"),
            storage: self.host.get_kube_client().cloned(),
        }))
    }
}

struct ConfigMapMounter {
    path: String,
    volume_name: String,
    namespace: String,
    config_map: ConfigMapVolumeSource,
    storage: Option<Arc<StorageBackend>>,
}

#[async_trait]
impl Mounter for ConfigMapMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's configMap branch (a1a39354) ----
        let storage = self
            .storage
            .as_ref()
            .context("Storage not available for ConfigMap volumes")?;

        let configmap_name = self
            .config_map
            .name
            .as_ref()
            .context("ConfigMap volume must specify name")?;

        let is_optional = self.config_map.optional.unwrap_or(false);

        let key = build_key("configmaps", Some(&self.namespace), configmap_name);
        let configmap_result: Result<ConfigMap, _> = storage.get(&key).await;

        // Create volume directory
        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create ConfigMap volume directory")?;

        // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
        let cm_default_mode = self.config_map.default_mode.unwrap_or(0o644);

        match configmap_result {
            Ok(configmap) => {
                // Build the projection payload (relative path -> bytes),
                // honoring `items` (specific keys → mapped paths) or all keys
                // from data + binaryData, then project it via the upstream
                // AtomicWriter. Re-projecting an unchanged payload is a no-op
                // (no write, no chmod, no symlink swap), so a running pod's
                // config watcher (kube-proxy) is never disturbed by the
                // kubelet's periodic re-SetUp. Each entry carries its own
                // mode: `items[].mode` when set, else the volume defaultMode.
                let payload = build_configmap_payload(
                    &configmap,
                    self.config_map.items.as_ref(),
                    configmap_name,
                    is_optional,
                    cm_default_mode as u32,
                );
                crate::atomic_writer::write_projected_payload(
                    std::path::Path::new(volume_dir),
                    &payload,
                )
                .with_context(|| format!("failed to project ConfigMap {configmap_name}"))?;
            }
            Err(e) => {
                if is_optional {
                    info!(
                        "Optional ConfigMap {} not found in namespace {}, creating empty volume",
                        configmap_name, &self.namespace
                    );
                } else {
                    // Required ConfigMap not found — abort pod start so kubelet
                    // retries on next reconciliation (when the ConfigMap exists).
                    return Err(anyhow::anyhow!(
                        "ConfigMap {} not found in namespace {}: {}",
                        configmap_name,
                        &self.namespace,
                        e
                    ));
                }
            }
        }

        info!(
            "Created ConfigMap volume {} at {}",
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

    fn plugin() -> ConfigMapPlugin {
        ConfigMapPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_config_map(name: &str) -> Volume {
        serde_json::from_value(json!({"name": "cfg", "configMap": {"name": name}})).unwrap()
    }

    fn secret_volume() -> Volume {
        serde_json::from_value(json!({"name": "s", "secret": {"secretName": "x"}})).unwrap()
    }

    #[test]
    fn supports_an_inline_config_map() {
        let v = inline_config_map("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = secret_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::CONFIG_MAP);
    }
}
