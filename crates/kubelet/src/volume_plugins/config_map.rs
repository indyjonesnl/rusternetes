use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
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
        let path = self
            .host
            .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name);
        Ok(Box::new(ConfigMapMounter {
            metrics: crate::volume_plugins::metrics_cached::cached_du(&path),
            path,
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
            pod: pod.clone(),
            host: self.host.clone(),
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
            crate::volume_plugins::util::WrappedEmptyDirUnmounter::new(
                self.host.clone(),
                self.name(),
                vol_name,
                pod_uid,
            ),
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
pub fn tolerate_get_error(err: &rusternetes_common::Error, optional: bool) -> bool {
    optional && matches!(err, rusternetes_common::Error::NotFound(_))
}

pub struct ConfigMapMounter {
    pub path: String,
    pub volume_name: String,
    pub namespace: String,
    pub config_map: ConfigMapVolumeSource,
    pub storage: Option<Arc<StorageBackend>>,
    /// `configMapVolumeMounter.pod` (`configmap.go:134`), read by
    /// `MakeNestedMountpoints`.
    pub pod: Pod,
    /// `b.plugin.host`, for `NewWrapperMounter` / `NewWrapperUnmounter`.
    pub host: Arc<dyn VolumeHost>,
    /// Embedded `volume.MetricsProvider` (`configmap.go:100`).
    pub metrics: crate::volume_plugins::metrics_cached::CachedMetrics,
}

impl crate::volume_plugins::plugin::MetricsProvider for ConfigMapMounter {
    /// `volume.NewCachedMetrics(volume.NewMetricsDu(getPath(..)))` (`configmap.go:100`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        self.metrics.get_metrics()
    }
}

#[async_trait]
impl Mounter for ConfigMapMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `configMapVolume.GetAttributes` (`pkg/volume/configmap/configmap.go:158-164`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: true,
            selinux_relabel: true,
        }
    }

    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
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

        // `wrapped.SetUpAt(dir, mounterArgs)` (`configmap.go:213`): configMap's
        // `wrappedVolumeSpec()` is a plain (disk) emptyDir (`:165-172`), so
        // this is `setupDir` + `SetReady(getMetaDir())`, no tmpfs.
        let volume_dir = dir;
        crate::volume_plugins::util::new_wrapper_mounter(
            &self.host,
            &self.volume_name,
            None,
            &self.pod,
        )
        .await?
        .set_up_at(volume_dir, args)
        .await
        .context("Failed to create ConfigMap volume directory")?;
        // `volumeutil.MakeNestedMountpoints(b.volName, dir, b.pod)`
        // (`configmap.go:220-222`), after the wrapped emptyDir SetUpAt and
        // before the failure-teardown `defer` is armed.
        crate::volume_plugins::util::nested_volumes::make_nested_mountpoints(
            &self.volume_name,
            std::path::Path::new(volume_dir),
            &self.pod,
        )?;

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
                return Err(crate::lifecycle::VolumeWaitError(format!(
                    "ConfigMap {} not found in namespace {}: {}",
                    configmap_name, self.namespace, e
                ))
                .into());
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
            args.fs_group,
            true,
        ) {
            // `defer` teardown: `unmounter.TearDown()` -> `UnmountViaEmptyDir`
            // (ready dir, umount of the tmpfs, volume dir).
            if let Err(td) = crate::volume_plugins::util::unmount_via_empty_dir(
                &self.host,
                volume_dir,
                &self.volume_name,
                &self.pod.metadata.uid,
            )
            .await
            {
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
