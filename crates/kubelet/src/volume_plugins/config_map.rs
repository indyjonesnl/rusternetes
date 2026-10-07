use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use crate::volumes::build_configmap_payload;
use anyhow::{anyhow, Context, Result};
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

    /// `GetVolumeName` (`configmap.go:65-75`): `"<spec name>/<configMap name>"`
    /// — the pair, not either alone, because one pod may mount two different
    /// ConfigMaps and two pods may mount the same one.
    ///
    /// `ConfigMapVolumeSource.name` is `Option<String>` here where upstream's
    /// embedded `LocalObjectReference.Name` is a plain string whose zero value
    /// is `""`; `unwrap_or_default` reproduces that zero value.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let Some(source) = &spec.volume.config_map else {
            return Err(anyhow!("Spec does not reference a ConfigMap volume type"));
        };
        Ok(format!(
            "{}/{}",
            spec.name(),
            source.name.as_deref().unwrap_or_default()
        ))
    }

    /// `RequiresRemount` (`configmap.go:81-83`): `true` — a ConfigMap volume
    /// tracks its API object, so an update must re-run the mount.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `SupportsSELinuxContextMount` (`configmap.go:89-91`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
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
            fs_group: crate::volume_plugins::util::fs_group_from(pod),
        }))
    }

    /// `NewUnmounter` (`configmap.go:108-130`): the wrapper plugins share one unmounter that
    /// delegates `TearDownAt` to emptyDir (`volumeutil.UnmountViaEmptyDir`).
    fn new_unmounter(
        &self,
        vol_name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn crate::volume_plugins::Unmounter>> {
        Ok(Box::new(
            crate::volume_plugins::util::WrappedEmptyDirUnmounter {
                host: self.host.clone(),
                plugin_name: self.name(),
                vol_name: vol_name.to_string(),
                pod_uid: pod_uid.to_string(),
            },
        ))
    }

    /// `ConstructVolumeSpec` (`configmap.go:108-130`): a bare `ConfigMapVolumeSource{}`
    /// named after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"configMap": {"name": ""}}),
        )
    }
}

/// `if !(errors.IsNotFound(err) && optional)` (`configmap.go:168`): the error
/// is tolerated (empty volume) only when the ConfigMap is optional AND the
/// failure is NotFound. Any other error (storage down, decode failure) fails
/// the mount rather than silently projecting nothing.
fn tolerate_get_error(err: &rusternetes_common::Error, optional: bool) -> bool {
    optional && matches!(err, rusternetes_common::Error::NotFound(_))
}

struct ConfigMapMounter {
    path: String,
    volume_name: String,
    namespace: String,
    config_map: ConfigMapVolumeSource,
    storage: Option<Arc<StorageBackend>>,
    /// `mounterArgs.FsGroup` (`volume.go:132`).
    fs_group: Option<i64>,
}

#[async_trait]
impl Mounter for ConfigMapMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's configMap branch
        //      (991a503d:crates/kubelet/src/volumes.rs:991-1059) ----
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

        // `configmap.go:166-181`: a NotFound on an optional ConfigMap is
        // replaced by an empty ConfigMap and setup CONTINUES into MakePayload
        // and the AtomicWriter (so `..data` exists, pointing at an empty dir);
        // it does not return early.
        let configmap = match configmap_result {
            Ok(configmap) => configmap,
            Err(e) if tolerate_get_error(&e, is_optional) => {
                info!(
                    "Optional ConfigMap {} not found in namespace {}, creating empty volume",
                    configmap_name, &self.namespace
                );
                serde_json::from_value(serde_json::json!({
                    "metadata": {"name": configmap_name, "namespace": &self.namespace}
                }))
                .context("building empty ConfigMap")?
            }
            Err(e) => {
                // Required ConfigMap not found — abort pod start so kubelet
                // retries on next reconciliation (when the ConfigMap exists).
                return Err(anyhow::anyhow!(
                    "ConfigMap {} not found in namespace {}: {}",
                    configmap_name,
                    self.namespace,
                    e
                ));
            }
        };

        // Build the projection payload (relative path -> bytes), honoring
        // `items` or all keys from data + binaryData, then project it via the
        // upstream AtomicWriter. Re-projecting an unchanged payload is a no-op
        // so a running pod's config watcher (kube-proxy) is never disturbed.
        let payload = build_configmap_payload(
            &configmap,
            self.config_map.items.as_ref(),
            configmap_name,
            is_optional,
            cm_default_mode as u32,
        )?;
        // `defer` at `configmap.go:222-237`: if the AtomicWriter fails after
        // the wrapped emptyDir SetUpAt, `unmounter.TearDown()` runs, and
        // emptyDir `TearDownAt` removes the volume directory.
        if let Err(e) = crate::volume_ownership::write_payload_with_ownership(
            std::path::Path::new(volume_dir),
            &payload,
            self.fs_group,
            true,
        ) {
            if let Err(td) = std::fs::remove_dir_all(volume_dir) {
                tracing::error!("Error tearing down volume {}: {}", self.volume_name, td);
            }
            return Err(anyhow::Error::new(e)
                .context(format!("failed to project ConfigMap {configmap_name}")));
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

    fn cm(pairs: &[(&str, &str)]) -> ConfigMap {
        serde_json::from_value(json!({
            "metadata": {"name": "cm"},
            "data": pairs.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>()
        }))
        .unwrap()
    }

    fn ktp(key: &str, path: &str) -> rusternetes_common::resources::KeyToPath {
        serde_json::from_value(json!({"key": key, "path": path})).unwrap()
    }

    /// `MakePayload` (`configmap.go:222-229`): a mapped key that is absent
    /// from `data` and `binaryData` is an error unless the source is optional.
    /// `TestMakePayload` "no sources"/"missing key" cases pin this.
    #[test]
    fn missing_mapped_key_is_an_error_unless_optional() {
        let c = cm(&[("a", "1")]);
        let items = vec![ktp("nope", "p")];
        let err = build_configmap_payload(&c, Some(&items), "cm", false, 0o644).unwrap_err();
        assert_eq!(
            err.to_string(),
            "configmap references non-existent config key: nope"
        );
        let ok = build_configmap_payload(&c, Some(&items), "cm", true, 0o644).unwrap();
        assert!(ok.is_empty());
    }

    /// `if len(mappings) == 0` (`configmap.go:207`): an EMPTY items list means
    /// "project every key", exactly like an absent one.
    #[test]
    fn empty_items_list_projects_every_key() {
        let c = cm(&[("a", "1"), ("b", "2")]);
        let items: Vec<rusternetes_common::resources::KeyToPath> = vec![];
        let payload = build_configmap_payload(&c, Some(&items), "cm", false, 0o644).unwrap();
        assert_eq!(payload.len(), 2);
    }

    /// `if !(errors.IsNotFound(err) && optional)` (`configmap.go:168`): only a
    /// NotFound is tolerated for an optional ConfigMap; any other error fails.
    #[test]
    fn only_not_found_is_tolerated_when_optional() {
        use rusternetes_common::Error;
        assert!(tolerate_get_error(&Error::NotFound("x".into()), true));
        assert!(!tolerate_get_error(&Error::NotFound("x".into()), false));
        assert!(!tolerate_get_error(&Error::Storage("boom".into()), true));
    }

    fn mounter(dir: &std::path::Path, optional: bool) -> ConfigMapMounter {
        ConfigMapMounter {
            path: dir.to_string_lossy().into_owned(),
            volume_name: "cfg".into(),
            namespace: "ns".into(),
            config_map: serde_json::from_value(json!({"name": "missing", "optional": optional}))
                .unwrap(),
            storage: Some(Arc::new(StorageBackend::Memory(Arc::new(
                rusternetes_storage::MemoryStorage::new(),
            )))),
            fs_group: None,
        }
    }

    /// `TestPluginOptional` (`configmap_test.go:447-537`): an optional
    /// ConfigMap that is NotFound still runs the AtomicWriter over an empty
    /// payload, so `..data` exists and points at an empty directory
    /// (`configmap.go:168-176` builds an empty ConfigMap, no early return).
    #[tokio::test]
    async fn optional_missing_configmap_still_creates_data_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        mounter(&dir, true).set_up().await.unwrap();
        let target = std::fs::read_link(dir.join("..data")).expect("..data symlink");
        let entries: Vec<_> = std::fs::read_dir(dir.join(target)).unwrap().collect();
        assert!(entries.is_empty());
    }

    /// `SetUpAt`'s deferred cleanup (`configmap.go:222-237`): when the
    /// AtomicWriter fails after the wrapped emptyDir SetUp, the volume is torn
    /// down (emptyDir `TearDownAt` removes the directory).
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        // A non-empty `..data_tmp` directory makes the writer's symlink step fail.
        std::fs::create_dir_all(dir.join("..data_tmp/x")).unwrap();
        assert!(mounter(&dir, true).set_up().await.is_err());
        assert!(
            !dir.exists(),
            "volume dir must be removed after a failed write"
        );
    }
}
