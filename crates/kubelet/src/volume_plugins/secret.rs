use crate::atomic_writer::FileProjection;
use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{KeyToPath, Pod, Secret, SecretVolumeSource};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/secret/secret.go`.
pub struct SecretPlugin {
    host: Arc<dyn VolumeHost>,
}

impl SecretPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for SecretPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::SECRET
    }

    /// `GetVolumeName` (`secret.go:72-79`): the referenced Secret's name.
    ///
    /// `SecretVolumeSource.secret_name` is `Option<String>` here where
    /// upstream's is a plain string whose zero value is `""`;
    /// `unwrap_or_default` reproduces that zero value.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let Some(source) = &spec.volume.secret else {
            return Err(anyhow!("Spec does not reference a Secret volume type"));
        };
        Ok(source.secret_name.clone().unwrap_or_default())
    }

    /// `RequiresRemount` (`secret.go:85-87`): `true`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `SupportsSELinuxContextMount` (`secret.go:93-95`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`secret.go:81-83`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.Secret != nil
    /// ```
    ///
    /// secret has no PersistentVolume form — upstream's `CanSupport` has no
    /// `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as emptyDir and configMap.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.secret.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        let path = self
            .host
            .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name);
        Ok(Box::new(SecretMounter {
            host: self.host.clone(),
            metrics: crate::volume_plugins::metrics_cached::cached_du(&path),
            path,
            volume_name: spec.volume.name.clone(),
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            secret: spec.volume.secret.clone().expect("checked by can_support"),
            storage: self.host.get_kube_client().cloned(),
            pod: pod.clone(),
        }))
    }

    /// `NewUnmounter` (`secret.go:112-136`): the wrapper plugins share one unmounter that
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

    /// `ConstructVolumeSpec` (`secret.go:112-136`): a bare `SecretVolumeSource{SecretName: volName}`
    /// named after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"secret": {"secretName": vol_name}}),
        )
    }
}

/// `MakePayload` (`pkg/volume/secret/secret.go:210-247`).
///
/// With no `items`, every Secret key becomes a file at `defaultMode`. With
/// `items`, only the listed keys are projected, at `items[].path`, with
/// `items[].mode` or else `defaultMode`. A listed key the Secret lacks is
/// an error unless the volume is optional (secret.go:228-233).
pub(crate) fn make_payload(
    mappings: Option<&[KeyToPath]>,
    secret: &Secret,
    default_mode: u32,
    optional: bool,
) -> Result<BTreeMap<String, FileProjection>> {
    let empty = Default::default();
    let data = secret.data.as_ref().unwrap_or(&empty);
    let mut payload = BTreeMap::new();
    match mappings {
        None | Some([]) => {
            for (name, bytes) in data {
                payload.insert(
                    name.clone(),
                    FileProjection {
                        fs_user: None,
                        data: bytes.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
        Some(items) => {
            for ktp in items {
                let Some(content) = data.get(&ktp.key) else {
                    if optional {
                        continue;
                    }
                    return Err(anyhow!("references non-existent secret key: {}", ktp.key));
                };
                payload.insert(
                    ktp.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: content.clone(),
                        mode: ktp.mode.map(|m| m as u32).unwrap_or(default_mode),
                    },
                );
            }
        }
    }
    Ok(payload)
}

struct SecretMounter {
    /// `plugin.host` (`secret.go:151`), for the wrapped emptyDir's metadata dir.
    host: Arc<dyn VolumeHost>,
    path: String,
    volume_name: String,
    namespace: String,
    secret: SecretVolumeSource,
    storage: Option<Arc<StorageBackend>>,
    /// `secretVolumeMounter.pod` (`secret.go:155`), read by
    /// `MakeNestedMountpoints`.
    pod: Pod,
    /// Embedded `volume.MetricsProvider` (`secret.go:104`).
    metrics: crate::volume_plugins::metrics_cached::CachedMetrics,
}

impl crate::volume_plugins::plugin::MetricsProvider for SecretMounter {
    /// `volume.NewCachedMetrics(volume.NewMetricsDu(getPath(..)))` (`secret.go:104`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        self.metrics.get_metrics()
    }
}

#[async_trait]
impl Mounter for SecretMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `secretVolume.GetAttributes` (`pkg/volume/secret/secret.go:164-170`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: true,
            selinux_relabel: true,
        }
    }

    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        // ---- moved verbatim from create_volume's secret branch
        //      (991a503d:crates/kubelet/src/volumes.rs:1062-1325) ----
        let storage = self
            .storage
            .as_ref()
            .context("Storage not available for Secret volumes")?;

        let secret_source = &self.secret;

        let secret_name = secret_source
            .secret_name
            .as_ref()
            .context("Secret volume must specify secret_name")?;

        let is_optional = secret_source.optional.unwrap_or(false);

        let key = build_key("secrets", Some(&self.namespace), secret_name);
        let secret_result: Result<Secret, _> = storage.get(&key).await;

        let volume_dir = dir;

        let secret = match secret_result {
            Ok(s) => Some(s),
            Err(e) => {
                if is_optional {
                    info!(
                        "Optional Secret {} not found in namespace {}, creating empty volume",
                        secret_name, &self.namespace
                    );
                    None
                } else {
                    // Required secret not found — return error so the kubelet
                    // retries on next sync. K8s leaves the pod in Pending with
                    // ContainerCreating and retries until the secret exists.
                    // K8s ref: pkg/kubelet/kubelet_pods.go — makeVolumes
                    return Err(crate::lifecycle::VolumeWaitError(format!(
                        "Secret {} not found in namespace {}: {}",
                        secret_name, self.namespace, e
                    ))
                    .into());
                }
            }
        };

        // Upstream's API defaulting always fills `defaultMode` (0644); a
        // volume that reaches us without it takes the same default.
        let secret_default_mode = secret_source.default_mode.unwrap_or(0o644);

        // `SetUpAt` (secret.go:167-172): an optional Secret that is not found
        // is replaced by an empty one, so the volume is still projected.
        let empty = Secret::new(secret_name.clone(), self.namespace.clone());
        let secret_ref = secret.as_ref().unwrap_or(&empty);

        // `MakePayload` (secret.go:210-247).
        let payload = make_payload(
            secret_source.items.as_deref(),
            secret_ref,
            secret_default_mode as u32,
            is_optional,
        )?;

        // No service-account token or CA special case here: upstream's secret
        // plugin projects the Secret verbatim (secret.go:162-208, MakePayload
        // :210-247). SA tokens and the cluster CA reach pods through the
        // projected `kube-api-access` volume (serviceAccountToken source +
        // `kube-root-ca.crt` ConfigMap; plugin/pkg/admission/serviceaccount/
        // admission.go TokenVolumeSource), injected at admission (#2638).

        // `wrapped.SetUpAt(dir, ...)` (secret.go:179-181) runs AFTER `getSecret`
        // and `MakePayload` (:166-177), so a missing Secret or bad item leaves
        // no volume behind (`TestInvalidPathSecret`, secret_test.go:365). The
        // wrapped emptyDir's `setupDir` creates the root at 0777. Its fsGroup
        // ownership runs in the AtomicWriter's `setPerms` (:242-247) below.
        // `wrappedVolumeSpec()` is a `Medium: Memory` emptyDir (secret.go:52-56),
        // so this mounts a tmpfs on the volume dir and ends with
        // `SetReady(getMetaDir())` (empty_dir.go:284), which creates
        // `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`
        // (`TestPlugin`, secret_test.go:340-348).
        let wrapped = crate::volume_plugins::util::new_wrapper_mounter(
            &self.host,
            &self.volume_name,
            Some("Memory"),
            &self.pod,
        )
        .await?;
        wrapped
            .set_up_at(volume_dir, args)
            .await
            .context("Failed to create Secret volume directory")?;
        // `volumeutil.MakeNestedMountpoints(b.volName, dir, b.pod)`
        // (secret.go:181-183), before the failure-teardown `defer` is armed.
        crate::volume_plugins::util::nested_volumes::make_nested_mountpoints(
            &self.volume_name,
            std::path::Path::new(volume_dir),
            &self.pod,
        )?;

        // `writer.Write(payload, setPerms)` (secret.go:196-204) via the
        // upstream AtomicWriter port: unchanged content is a no-op. The
        // `defer` at secret.go:183-200 runs `unmounter.TearDown()` when the
        // write fails; emptyDir `TearDownAt` removes the volume directory
        // (same as configMap/downwardAPI).
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
            return Err(
                anyhow::Error::new(e).context(format!("failed to project Secret {secret_name}"))
            );
        }

        info!(
            "Created Secret volume {} at {}",
            self.volume_name, volume_dir
        );
        // ---- end moved body ----
        Ok(())
    }
}
