use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{ConfigMap, Pod, Secret, Volume};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Port of `pkg/volume/projected/projected.go`.
pub struct ProjectedPlugin {
    host: Arc<dyn VolumeHost>,
}

impl ProjectedPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for ProjectedPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::PROJECTED
    }

    /// `GetVolumeName` (`projected.go:87-94`): the user-defined volume name,
    /// after `getVolumeSource` (`projected.go:459-465`) confirms the spec is
    /// actually a projected volume.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.projected.is_none() {
            return Err(anyhow!("spec does not reference a projected volume type"));
        }
        Ok(spec.name().to_string())
    }

    /// `RequiresRemount` (`projected.go:100-102`): `true`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `SupportsSELinuxContextMount` (`projected.go:108-110`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`projected.go:96-98`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.Projected != nil
    /// ```
    ///
    /// projected has no PersistentVolume form — upstream's `CanSupport` has no
    /// `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as configMap/secret/downwardAPI. A projected volume
    /// nests configMap and secret sources inside `sources[]`, but those live
    /// under `Volume.Projected`, never as siblings of it on the same
    /// `Volume` — so `spec.volume.config_map`/`.secret` are never set at the
    /// same time as `spec.volume.projected`, and the configMap/secret
    /// plugins can never also claim a projected volume.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.projected.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(ProjectedMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            // The whole Volume is cloned (not just its `projected` source)
            // because the final log line names `volume.name`, same reason as
            // Task 7's downwardAPI mounter.
            volume: spec.volume.clone(),
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            // Carries the pod's NAME, used only for the service-account
            // token audience/claims. On-disk paths never use it — `path` is
            // already resolved.
            pod_name: pod.metadata.name.clone(),
            // The whole Pod is cloned because a projected downwardAPI item's
            // `fieldRef` can name any of the pod's fields, and the
            // serviceAccountToken source reads the pod's own spec
            // (serviceAccountName, nodeName) and metadata (uid) directly.
            pod: pod.clone(),
            storage: self.host.get_kube_client().cloned(),
            token_manager: self.host.get_service_account_token_func().clone(),
            // `GetNodeAllocatable` (`pkg/volume/plugins.go:397`) — needed
            // only when a downwardAPI source item has a `resourceFieldRef`,
            // same as Task 7's downwardAPI plugin.
            node_allocatable: self.host.get_node_allocatable().clone(),
        }))
    }
}

struct ProjectedMounter {
    path: String,
    volume: Volume,
    namespace: String,
    pod_name: String,
    pod: Pod,
    storage: Option<Arc<StorageBackend>>,
    token_manager: rusternetes_common::auth::TokenManager,
    node_allocatable: HashMap<String, String>,
}

#[async_trait]
impl Mounter for ProjectedMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's projected branch
        //      (991a503d:crates/kubelet/src/volumes.rs:1554-1933) ----
        let projected = self
            .volume
            .projected
            .as_ref()
            .expect("checked by can_support");
        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create projected volume directory")?;

        // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
        let proj_default_mode = projected.default_mode.unwrap_or(0o644);

        // Compute final directory permissions (will be applied after files are written)
        #[cfg(unix)]
        let proj_dir_mode = proj_default_mode as u32 | 0o111;

        if let Some(sources) = &projected.sources {
            let storage = self.storage.as_ref();

            for source in sources {
                // ConfigMap projection
                if let Some(cm_proj) = &source.config_map {
                    if let Some(cm_name) = &cm_proj.name {
                        let key = build_key("configmaps", Some(&self.namespace), cm_name);
                        if let Some(storage) = storage {
                            match storage.get::<ConfigMap>(&key).await {
                                Ok(cm) => {
                                    // Helper to write a projected file with permissions
                                    let write_proj_file =
                                        |path: &str, content: &[u8], mode: i32| -> Result<()> {
                                            if let Some(parent) =
                                                std::path::Path::new(path).parent()
                                            {
                                                std::fs::create_dir_all(parent)?;
                                            }
                                            std::fs::write(path, content)?;
                                            #[cfg(unix)]
                                            {
                                                use std::os::unix::fs::PermissionsExt;
                                                std::fs::set_permissions(
                                                    path,
                                                    std::fs::Permissions::from_mode(mode as u32),
                                                )?;
                                            }
                                            Ok(())
                                        };

                                    if let Some(items) = &cm_proj.items {
                                        for item in items {
                                            let mode = item.mode.unwrap_or(proj_default_mode);
                                            let file_path = format!("{}/{}", volume_dir, item.path);
                                            // Try data first, then binaryData
                                            if let Some(value) =
                                                cm.data.as_ref().and_then(|d| d.get(&item.key))
                                            {
                                                write_proj_file(
                                                    &file_path,
                                                    value.as_bytes(),
                                                    mode,
                                                )?;
                                            } else if let Some(value) = cm
                                                .binary_data
                                                .as_ref()
                                                .and_then(|d| d.get(&item.key))
                                            {
                                                write_proj_file(&file_path, value, mode)?;
                                            }
                                        }
                                    } else {
                                        // Mount all keys from data
                                        if let Some(data) = &cm.data {
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                write_proj_file(
                                                    &file_path,
                                                    v.as_bytes(),
                                                    proj_default_mode,
                                                )?;
                                            }
                                        }
                                        // Mount all keys from binaryData
                                        if let Some(binary_data) = &cm.binary_data {
                                            for (k, v) in binary_data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                write_proj_file(&file_path, v, proj_default_mode)?;
                                            }
                                        }
                                    }
                                }
                                Err(_) if cm_proj.optional.unwrap_or(false) => {
                                    // Optional configmap not found, skip
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to get ConfigMap {} for projected volume: {}. Skipping.",
                                        cm_name, e
                                    );
                                }
                            }
                        }
                    }
                }

                // Secret projection
                if let Some(secret_proj) = &source.secret {
                    if let Some(secret_name) = &secret_proj.name {
                        let key = build_key("secrets", Some(&self.namespace), secret_name);
                        if let Some(storage) = storage {
                            match storage.get::<Secret>(&key).await {
                                Ok(secret) => {
                                    if let Some(data) = &secret.data {
                                        if let Some(items) = &secret_proj.items {
                                            for item in items {
                                                if let Some(value) = data.get(&item.key) {
                                                    let file_path =
                                                        format!("{}/{}", volume_dir, item.path);
                                                    if let Some(parent) =
                                                        std::path::Path::new(&file_path).parent()
                                                    {
                                                        std::fs::create_dir_all(parent)?;
                                                    }
                                                    std::fs::write(&file_path, value)?;
                                                    #[cfg(unix)]
                                                    {
                                                        use std::os::unix::fs::PermissionsExt;
                                                        let mode =
                                                            item.mode.unwrap_or(proj_default_mode)
                                                                as u32;
                                                        std::fs::set_permissions(
                                                            &file_path,
                                                            std::fs::Permissions::from_mode(mode),
                                                        )?;
                                                    }
                                                }
                                            }
                                        } else {
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                std::fs::write(&file_path, v)?;
                                                #[cfg(unix)]
                                                {
                                                    use std::os::unix::fs::PermissionsExt;
                                                    std::fs::set_permissions(
                                                        &file_path,
                                                        std::fs::Permissions::from_mode(
                                                            proj_default_mode as u32,
                                                        ),
                                                    )?;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(_) if secret_proj.optional.unwrap_or(false) => {
                                    // Optional secret not found, skip
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to get Secret {} for projected volume: {}. Skipping.",
                                        secret_name, e
                                    );
                                }
                            }
                        }
                    }
                }

                // DownwardAPI projection
                if let Some(downward_api) = &source.downward_api {
                    if let Some(items) = &downward_api.items {
                        for item in items {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            let value = if let Some(ref field_ref) = item.field_ref {
                                crate::downward_api::resolve_pod_field(
                                    &self.pod,
                                    &field_ref.field_path,
                                )
                                .map_err(|e| anyhow::anyhow!("{e}"))
                                .unwrap_or_default()
                            } else if let Some(ref resource_ref) = item.resource_field_ref {
                                crate::downward_api::resolve_container_resource(
                                    &self.pod,
                                    resource_ref,
                                    Some(&self.node_allocatable),
                                )
                                .map_err(|e| anyhow::anyhow!("{e}"))
                                .unwrap_or_default()
                            } else {
                                String::new()
                            };
                            std::fs::write(&file_path, &value)?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                let mode = item.mode.unwrap_or(proj_default_mode) as u32;
                                std::fs::set_permissions(
                                    &file_path,
                                    std::fs::Permissions::from_mode(mode),
                                )?;
                            }
                        }
                    }
                }

                // ServiceAccountToken projection
                if let Some(sa_token) = &source.service_account_token {
                    let token_path = format!("{}/{}", volume_dir, sa_token.path);
                    if let Some(parent) = std::path::Path::new(&token_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    // Generate a real JWT token bound to this pod
                    let sa_name = self
                        .pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.service_account_name.as_deref())
                        .unwrap_or("default");
                    let sa_uid = if let Some(storage) = storage {
                        let sa_key = build_key("serviceaccounts", Some(&self.namespace), sa_name);
                        match storage
                            .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
                            .await
                        {
                            Ok(sa) => sa.metadata.uid.clone(),
                            Err(_) => String::new(),
                        }
                    } else {
                        String::new()
                    };
                    // TokenRequest requires expirationSeconds >= 600 (10m).
                    let expiration_seconds = sa_token.expiration_seconds.unwrap_or(3600).max(600);
                    let now = chrono::Utc::now();
                    let exp = now.timestamp() + expiration_seconds;
                    // Audience to REQUEST from the api-server: exactly what the
                    // projection asked for (empty => the api-server's own
                    // default api-audience, which it will then accept — do NOT
                    // force "rusternetes", or a vanilla api-server issues a
                    // token whose audience it rejects on use).
                    let requested_audiences: Vec<String> =
                        sa_token.audience.iter().cloned().collect();
                    // Audience baked into the self-mint FALLBACK claims (native
                    // storage-mode only): default to "rusternetes".
                    let mut audiences = vec!["rusternetes".to_string()];
                    if let Some(ref aud) = sa_token.audience {
                        audiences = vec![aud.clone()];
                    }
                    let node_name = self.pod.spec.as_ref().and_then(|s| s.node_name.clone());
                    let node_uid = if let (Some(ref nn), Some(st)) = (&node_name, storage) {
                        let node_key = build_key("nodes", None::<&str>, nn);
                        st.get::<serde_json::Value>(&node_key)
                            .await
                            .ok()
                            .and_then(|v| {
                                v.pointer("/metadata/uid")
                                    .and_then(|u| u.as_str())
                                    .map(|s| s.to_string())
                            })
                    } else {
                        None
                    };
                    let claims = rusternetes_common::auth::ServiceAccountClaims {
                        sub: format!("system:serviceaccount:{}:{}", &self.namespace, sa_name),
                        namespace: self.namespace.to_string(),
                        uid: sa_uid.clone(),
                        iat: now.timestamp(),
                        exp,
                        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                        aud: audiences.clone(),
                        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                            namespace: self.namespace.to_string(),
                            svcacct: rusternetes_common::auth::KubeRef {
                                name: sa_name.to_string(),
                                uid: sa_uid,
                            },
                            pod: Some(rusternetes_common::auth::KubeRef {
                                name: self.pod_name.clone(),
                                uid: self.pod.metadata.uid.clone(),
                            }),
                            node: node_name
                                .as_ref()
                                .map(|nn| rusternetes_common::auth::KubeRef {
                                    name: nn.clone(),
                                    uid: node_uid.clone().unwrap_or_default(),
                                }),
                        }),
                        pod_name: Some(self.pod_name.clone()),
                        pod_uid: Some(self.pod.metadata.uid.clone()),
                        node_name,
                        node_uid,
                    };
                    // Reuse a still-fresh token: re-mint only when the file is
                    // missing or past ~80% of its lifetime. The per-sync volume
                    // re-creation would otherwise hit the api-server TokenRequest
                    // endpoint every few seconds per pod and churn the token file.
                    let refresh_after = (expiration_seconds * 8 / 10).max(60);
                    let token_fresh = std::fs::metadata(&token_path)
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|mt| mt.elapsed().ok())
                        .map(|age| (age.as_secs() as i64) < refresh_after)
                        .unwrap_or(false);
                    if !token_fresh {
                        // Prefer an api-server-issued bound token (TokenRequest),
                        // matching the upstream kubelet (pkg/kubelet/token) — it
                        // never self-signs. A vanilla api-server only trusts
                        // tokens IT signed, so a self-minted token is 401-rejected
                        // for in-cluster clients (kindnet et al.). Self-mint only
                        // as a fallback for the storage-direct backends
                        // (all-in-one), whose co-located api-server trusts our key.
                        let mut issued: Option<String> = None;
                        if let Some(st) = storage {
                            // Bind the token to this pod, as upstream's
                            // projected volume plugin does
                            // (pkg/volume/projected/projected.go). The
                            // api-server derives the pod/node claims from
                            // the ref, and a TokenReview on the mounted
                            // token then reports the
                            // authentication.kubernetes.io/pod-name,
                            // pod-uid and node-name extras (#1684).
                            match st
                                .create_sa_token(
                                    &self.namespace,
                                    sa_name,
                                    &requested_audiences,
                                    expiration_seconds,
                                    Some((self.pod_name.as_str(), self.pod.metadata.uid.as_str())),
                                )
                                .await
                            {
                                Ok(Some(t)) => issued = Some(t),
                                Ok(None) => {}
                                Err(e) => warn!(
                                    "TokenRequest for {}/{} failed: {}; self-minting",
                                    &self.namespace, sa_name, e
                                ),
                            }
                        }
                        let token = match issued {
                            Some(t) => t,
                            None => match self.token_manager.generate_token(claims) {
                                Ok(t) => t,
                                Err(e) => {
                                    warn!(
                                    "Failed to generate SA token for pod {}: {}, using placeholder",
                                    self.pod_name, e
                                );
                                    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.placeholder".to_string()
                                }
                            },
                        };
                        std::fs::write(&token_path, &token)?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            std::fs::set_permissions(
                                &token_path,
                                std::fs::Permissions::from_mode(proj_default_mode as u32),
                            )?;
                        }
                    } // end if !token_fresh
                }
            }
        }

        // Set directory permissions after files are written so that restrictive
        // defaultMode values don't prevent file creation.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(volume_dir, std::fs::Permissions::from_mode(proj_dir_mode))?;
        }

        info!(
            "Created projected volume {} at {}",
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

    fn plugin() -> ProjectedPlugin {
        ProjectedPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_projected() -> Volume {
        serde_json::from_value(serde_json::json!({"name": "proj", "projected": {}})).unwrap()
    }

    fn config_map_volume() -> Volume {
        serde_json::from_value(serde_json::json!({"name": "cfg", "configMap": {"name": "x"}}))
            .unwrap()
    }

    #[test]
    fn supports_an_inline_projected_volume() {
        let v = inline_projected();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    /// A projected volume nests configMap and secret sources, but the
    /// configMap and secret plugins must NOT claim it — otherwise
    /// find_plugin_by_spec sees three matches and errors.
    #[test]
    fn rejects_a_plain_config_map() {
        let v = config_map_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::PROJECTED);
    }
}
