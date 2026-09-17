use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Secret, SecretVolumeSource};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::sync::Arc;
use tracing::{info, warn};

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
        Ok(Box::new(SecretMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            secret: spec.volume.secret.clone().expect("checked by can_support"),
            // Carries the pod's NAME, used only for the service-account token
            // audience. On-disk paths never use it — `path` is already resolved.
            pod_name: pod.metadata.name.clone(),
            pod_uid: pod.metadata.uid.clone(),
            service_account_name: pod
                .spec
                .as_ref()
                .and_then(|s| s.service_account_name.clone()),
            node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
            storage: self.host.get_kube_client().cloned(),
            volumes_base_path: self.host.get_volumes_base_path().to_string(),
            token_manager: self.host.get_service_account_token_func().clone(),
        }))
    }
}

struct SecretMounter {
    path: String,
    volume_name: String,
    namespace: String,
    secret: SecretVolumeSource,
    /// Carries the pod's NAME, used only for the service-account token
    /// audience. On-disk paths never use it — `path` is already resolved.
    pod_name: String,
    pod_uid: String,
    service_account_name: Option<String>,
    node_name: Option<String>,
    storage: Option<Arc<StorageBackend>>,
    volumes_base_path: String,
    token_manager: rusternetes_common::auth::TokenManager,
}

#[async_trait]
impl Mounter for SecretMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
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

        // For SA token volumes, generate a bound token with pod reference
        // instead of using the static token from the Secret.
        let is_sa_token_volume =
            self.volume_name.contains("kube-api-access") || secret_name.ends_with("-token");
        let bound_token: Option<String> = if is_sa_token_volume {
            let sa_name = self.service_account_name.as_deref().unwrap_or("default");
            let sa_key = build_key("serviceaccounts", Some(&self.namespace), sa_name);
            let sa_uid = storage
                .get::<serde_json::Value>(&sa_key)
                .await
                .ok()
                .and_then(|v| {
                    v.pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let node_name = self.node_name.clone();
            let node_uid = if let Some(ref nn) = node_name {
                let node_key = build_key("nodes", None::<&str>, nn);
                storage
                    .get::<serde_json::Value>(&node_key)
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
            let now = chrono::Utc::now();
            let claims = rusternetes_common::auth::ServiceAccountClaims {
                sub: format!("system:serviceaccount:{}:{}", &self.namespace, sa_name),
                namespace: self.namespace.to_string(),
                uid: sa_uid.clone(),
                iat: now.timestamp(),
                exp: (now + chrono::Duration::hours(1)).timestamp(),
                iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                aud: vec!["rusternetes".to_string()],
                kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                    namespace: self.namespace.to_string(),
                    svcacct: rusternetes_common::auth::KubeRef {
                        name: sa_name.to_string(),
                        uid: sa_uid,
                    },
                    pod: Some(rusternetes_common::auth::KubeRef {
                        name: self.pod_name.to_string(),
                        uid: self.pod_uid.clone(),
                    }),
                    node: node_name
                        .as_ref()
                        .map(|nn| rusternetes_common::auth::KubeRef {
                            name: nn.clone(),
                            uid: node_uid.clone().unwrap_or_default(),
                        }),
                }),
                pod_name: Some(self.pod_name.to_string()),
                pod_uid: Some(self.pod_uid.clone()),
                node_name,
                node_uid,
            };
            self.token_manager.generate_token(claims).ok()
        } else {
            None
        };

        let key = build_key("secrets", Some(&self.namespace), secret_name);
        let secret_result: Result<Secret, _> = storage.get(&key).await;

        // Create volume directory
        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir).context("Failed to create Secret volume directory")?;

        // Compute final directory permissions (will be applied after files are written)
        #[cfg(unix)]
        let secret_dir_mode = secret_source.default_mode.unwrap_or(0o644) as u32 | 0o111;

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
                    return Err(anyhow::anyhow!(
                        "Secret {} not found in namespace {}: {}",
                        secret_name,
                        &self.namespace,
                        e
                    ));
                }
            }
        };

        // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
        let secret_default_mode = secret_source.default_mode.unwrap_or(0o644);

        // Write secret data as files
        if let Some(data) = secret.as_ref().and_then(|s| s.data.as_ref()) {
            if let Some(ref items) = secret_source.items {
                // Only mount the specified keys
                for item in items {
                    if let Some(value) = data.get(&item.key) {
                        let file_path = format!("{}/{}", volume_dir, item.path);
                        // Create parent directories if needed
                        if let Some(parent) = std::path::Path::new(&file_path).parent() {
                            std::fs::create_dir_all(parent).with_context(|| {
                                format!("Failed to create directory for Secret item {}", item.path)
                            })?;
                        }
                        // For SA token volumes, substitute the bound token
                        let write_value: &[u8] = if item.key == "token" {
                            if let Some(ref bt) = bound_token {
                                bt.as_bytes()
                            } else {
                                value
                            }
                        } else {
                            value
                        };
                        std::fs::write(&file_path, write_value).with_context(|| {
                            format!("Failed to write Secret key {} to file", item.key)
                        })?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            let mode = item.mode.unwrap_or(secret_default_mode) as u32;
                            std::fs::set_permissions(
                                &file_path,
                                std::fs::Permissions::from_mode(mode),
                            )?;
                        }
                        if bound_token.is_some() && item.key == "token" {
                            info!(
                                "Wrote bound SA token for pod {} to {}",
                                self.pod_name, file_path
                            );
                        } else {
                            info!("Wrote Secret key {} to {}", item.key, file_path);
                        }
                    }
                }
            } else {
                // Mount all keys
                for (key, value) in data {
                    let file_path = format!("{}/{}", volume_dir, key);
                    // For SA token volumes, substitute the bound token
                    let write_value: &[u8] = if key == "token" {
                        if let Some(ref bt) = bound_token {
                            bt.as_bytes()
                        } else {
                            value.as_slice()
                        }
                    } else {
                        value.as_slice()
                    };
                    std::fs::write(&file_path, write_value)
                        .with_context(|| format!("Failed to write Secret key {} to file", key))?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(
                            &file_path,
                            std::fs::Permissions::from_mode(secret_default_mode as u32),
                        )?;
                    }
                    info!("Wrote Secret key {} to {}", key, file_path);
                }
            }
        }

        // Special handling for service account token secrets - add ca.crt
        // Service account secrets are identified by having a "token" key or by name pattern
        let is_service_account_secret = secret
            .as_ref()
            .and_then(|s| s.data.as_ref())
            .map(|data| data.contains_key("token"))
            .unwrap_or(false)
            || secret_name.ends_with("-token");

        if is_service_account_secret {
            // Check if ca.crt already exists in the secret data
            let has_ca_cert = secret
                .as_ref()
                .and_then(|s| s.data.as_ref())
                .map(|data| data.contains_key("ca.crt"))
                .unwrap_or(false);

            if !has_ca_cert {
                // Inject ca.crt from the cluster CA certificate
                // Try multiple locations: environment variable, volumes/_certs, then fallback to .rusternetes/certs
                let ca_cert_source = std::env::var("CA_CERT_PATH").unwrap_or_else(|_| {
                    // First try volumes/_certs (accessible from kubelet container)
                    let volumes_cert_path = format!("{}/_certs/ca.crt", self.volumes_base_path);
                    if std::path::Path::new(&volumes_cert_path).exists() {
                        volumes_cert_path
                    } else {
                        // Fallback to .rusternetes/certs (for host-based kubelet)
                        format!(
                            "{}/.rusternetes/certs/ca.crt",
                            std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())
                        )
                    }
                });

                let ca_path = format!("{}/ca.crt", volume_dir);
                if let Ok(ca_content) = std::fs::read(&ca_cert_source) {
                    std::fs::write(&ca_path, ca_content)
                        .context("Failed to write CA certificate")?;
                    info!(
                        "Injected CA certificate into service account secret volume at {} (from {})",
                        ca_path, ca_cert_source
                    );
                } else {
                    warn!(
                        "CA certificate not found at {}, pods may not be able to verify API server",
                        ca_cert_source
                    );
                }
            }
        }

        // Set directory permissions after files are written so that restrictive
        // defaultMode values (e.g., 0o400) don't prevent file creation.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(volume_dir, std::fs::Permissions::from_mode(secret_dir_mode))?;
        }

        info!(
            "Created Secret volume {} at {}",
            self.volume_name, volume_dir
        );
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Pod, Volume};
    use serde_json::json;

    fn plugin() -> SecretPlugin {
        SecretPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_secret(name: &str) -> Volume {
        serde_json::from_value(json!({"name": "sec", "secret": {"secretName": name}})).unwrap()
    }

    fn config_map_volume() -> Volume {
        serde_json::from_value(json!({"name": "cfg", "configMap": {"name": "x"}})).unwrap()
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    #[test]
    fn supports_an_inline_secret() {
        let v = inline_secret("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = config_map_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::SECRET);
    }

    /// The mounter's path comes from the pod-directory layout the host
    /// provides, keyed on the plugin name — same contract as every other
    /// plugin (host.rs `pod_volume_dir_matches_pod_dirs`).
    #[tokio::test]
    async fn get_path_uses_the_secret_plugin_directory() {
        let v = inline_secret("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(
            m.get_path(),
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~secret/sec"
        );
    }
}
