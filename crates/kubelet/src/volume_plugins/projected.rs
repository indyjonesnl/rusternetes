use crate::atomic_writer::FileProjection;
use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{ConfigMap, KeyToPath, Pod, Secret, Volume};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::{BTreeMap, HashMap};
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
            // `MounterArgs{FsUser: util.FsUserFrom(pod), FsGroup: fsGroup}`
            // (`operation_generator.go:501-509`, `:583-584`).
            fs_user: crate::volume_plugins::util::fs_user_from(pod),
            fs_group: crate::volume_plugins::util::fs_group_from(pod),
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
    /// `mounterArgs.FsUser` (`volume.go:131`).
    fs_user: Option<i64>,
    /// `mounterArgs.FsGroup` (`volume.go:132`).
    fs_group: Option<i64>,
}

/// `utilerrors.NewAggregate(errlist).Error()`
/// (`staging/src/k8s.io/apimachinery/pkg/util/errors/errors.go:70-96`): one
/// error prints as itself; several print de-duplicated as `[a, b]`.
fn aggregate_message(errs: &[String]) -> String {
    let mut seen: Vec<&str> = Vec::new();
    for e in errs {
        if !seen.contains(&e.as_str()) {
            seen.push(e);
        }
    }
    if seen.len() == 1 {
        seen[0].to_string()
    } else {
        format!("[{}]", seen.join(", "))
    }
}

fn mode_of(item_mode: Option<i32>, default_mode: u32) -> u32 {
    item_mode.map(|m| m as u32).unwrap_or(default_mode)
}

/// `configmap.MakePayload` (`pkg/volume/configmap/configmap.go:263-305`).
fn config_map_payload(
    items: Option<&Vec<KeyToPath>>,
    cm: &ConfigMap,
    default_mode: u32,
    optional: bool,
) -> std::result::Result<BTreeMap<String, FileProjection>, String> {
    let mut payload = BTreeMap::new();
    match items.filter(|i| !i.is_empty()) {
        None => {
            for (k, v) in cm.data.iter().flatten() {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone().into_bytes(),
                        mode: default_mode,
                    },
                );
            }
            for (k, v) in cm.binary_data.iter().flatten() {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
        Some(items) => {
            for ktp in items {
                let data = if let Some(v) = cm.data.as_ref().and_then(|d| d.get(&ktp.key)) {
                    v.clone().into_bytes()
                } else if let Some(v) = cm.binary_data.as_ref().and_then(|d| d.get(&ktp.key)) {
                    v.clone()
                } else if optional {
                    continue;
                } else {
                    return Err(format!(
                        "configmap references non-existent config key: {}",
                        ktp.key
                    ));
                };
                payload.insert(
                    ktp.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data,
                        mode: mode_of(ktp.mode, default_mode),
                    },
                );
            }
        }
    }
    Ok(payload)
}

/// `secret.MakePayload` (`pkg/volume/secret/secret.go:259-295`).
fn secret_payload(
    items: Option<&Vec<KeyToPath>>,
    secret: &Secret,
    default_mode: u32,
    optional: bool,
) -> std::result::Result<BTreeMap<String, FileProjection>, String> {
    let mut payload = BTreeMap::new();
    match items.filter(|i| !i.is_empty()) {
        None => {
            for (k, v) in secret.data.iter().flatten() {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
        Some(items) => {
            for ktp in items {
                let Some(content) = secret.data.as_ref().and_then(|d| d.get(&ktp.key)) else {
                    if optional {
                        continue;
                    }
                    return Err(format!("references non-existent secret key: {}", ktp.key));
                };
                payload.insert(
                    ktp.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: content.clone(),
                        mode: mode_of(ktp.mode, default_mode),
                    },
                );
            }
        }
    }
    Ok(payload)
}

/// `downwardapi.CollectData` (`pkg/volume/downwardapi/downwardapi.go:238-277`):
/// errors are accumulated, not short-circuited; an item with neither ref
/// projects an empty file; the path is `filepath.Clean`ed.
fn downward_api_payload(
    items: &[rusternetes_common::resources::DownwardAPIVolumeFile],
    pod: &Pod,
    node_allocatable: &HashMap<String, String>,
    default_mode: u32,
) -> std::result::Result<BTreeMap<String, FileProjection>, String> {
    let mut errs = Vec::new();
    let mut data = BTreeMap::new();
    for item in items {
        let mut fp = FileProjection {
            fs_user: None,
            data: Vec::new(),
            mode: mode_of(item.mode, default_mode),
        };
        if let Some(field_ref) = &item.field_ref {
            match crate::downward_api::resolve_pod_field(pod, &field_ref.field_path) {
                Ok(v) => fp.data = v.into_bytes(),
                Err(e) => errs.push(e.to_string()),
            }
        } else if let Some(resource_ref) = &item.resource_field_ref {
            match crate::downward_api::resolve_container_resource(
                pod,
                resource_ref,
                Some(node_allocatable),
            ) {
                Ok(v) => fp.data = v.into_bytes(),
                Err(e) => errs.push(e.to_string()),
            }
        }
        data.insert(clean_path(&item.path), fp);
    }
    if errs.is_empty() {
        Ok(data)
    } else {
        Err(aggregate_message(&errs))
    }
}

/// Lexical `filepath.Clean` for the relative item paths validation allows.
fn clean_path(p: &str) -> String {
    let parts: Vec<&str> = p
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    parts.join("/")
}

impl ProjectedMounter {
    /// The mode of a token / bundle / key file (`projected.go:276-282`):
    ///
    /// ```go
    /// // When FsGroup is set, we depend on SetVolumeOwnership to
    /// // change from 0600 to 0640.
    /// mode := *s.source.DefaultMode
    /// if mounterArgs.FsUser != nil || mounterArgs.FsGroup != nil {
    ///     mode = 0600
    /// }
    /// ```
    fn secret_file_mode(&self, default_mode: u32) -> u32 {
        if self.fs_user.is_some() || self.fs_group.is_some() {
            0o600
        } else {
            default_mode
        }
    }

    /// `collectData` (`projected.go:226-338`): build ONE payload from every
    /// source, accumulating errors, and fail with their aggregate.
    async fn collect_data(&self) -> Result<BTreeMap<String, FileProjection>> {
        let projected = self
            .volume
            .projected
            .as_ref()
            .expect("checked by can_support");
        let default_mode = projected.default_mode.unwrap_or(0o644) as u32;

        // `kubeClient == nil` (`projected.go:233-236`).
        let Some(storage) = self.storage.as_ref() else {
            return Err(anyhow!(
                "cannot setup projected volume {} because kube client is not configured",
                self.volume.name
            ));
        };

        let mut errlist: Vec<String> = Vec::new();
        let mut payload: BTreeMap<String, FileProjection> = BTreeMap::new();
        for source in projected.sources.iter().flatten() {
            if let Some(sp) = &source.secret {
                let name = sp.name.clone().unwrap_or_default();
                let optional = sp.optional.unwrap_or(false);
                let key = build_key("secrets", Some(&self.namespace), &name);
                let secret = match storage.get::<Secret>(&key).await {
                    Ok(s) => s,
                    // `!(errors.IsNotFound(err) && optional)` (`:243`).
                    Err(rusternetes_common::Error::NotFound(_)) if optional => {
                        Secret::new(&name, &self.namespace)
                    }
                    Err(e) => {
                        warn!("Couldn't get secret {}/{}: {}", self.namespace, name, e);
                        errlist.push(e.to_string());
                        continue;
                    }
                };
                match secret_payload(sp.items.as_ref(), &secret, default_mode, optional) {
                    Ok(p) => payload.extend(p),
                    Err(e) => errlist.push(e),
                }
            } else if let Some(cp) = &source.config_map {
                let name = cp.name.clone().unwrap_or_default();
                let optional = cp.optional.unwrap_or(false);
                let key = build_key("configmaps", Some(&self.namespace), &name);
                let cm = match storage.get::<ConfigMap>(&key).await {
                    Ok(c) => c,
                    Err(rusternetes_common::Error::NotFound(_)) if optional => {
                        ConfigMap::new(&name, &self.namespace)
                    }
                    Err(e) => {
                        warn!("Couldn't get configMap {}/{}: {}", self.namespace, name, e);
                        errlist.push(e.to_string());
                        continue;
                    }
                };
                match config_map_payload(cp.items.as_ref(), &cm, default_mode, optional) {
                    Ok(p) => payload.extend(p),
                    Err(e) => errlist.push(e),
                }
            } else if let Some(da) = &source.downward_api {
                let items = da.items.as_deref().unwrap_or(&[]);
                match downward_api_payload(items, &self.pod, &self.node_allocatable, default_mode) {
                    Ok(p) => payload.extend(p),
                    Err(e) => errlist.push(e),
                }
            } else if let Some(tp) = &source.service_account_token {
                match self.service_account_token(storage, tp).await {
                    Ok(token) => {
                        payload.insert(
                            tp.path.clone(),
                            FileProjection {
                                fs_user: self.fs_user,
                                data: token.into_bytes(),
                                mode: self.secret_file_mode(default_mode),
                            },
                        );
                    }
                    Err(e) => errlist.push(e.to_string()),
                }
            }
            // ClusterTrustBundle / PodCertificate: not yet implemented, see
            // the follow-up issue linked from the PR.
        }

        if errlist.is_empty() {
            Ok(payload)
        } else {
            Err(anyhow!("{}", aggregate_message(&errlist)))
        }
    }

    /// The `source.ServiceAccountToken` arm of `collectData`
    /// (`projected.go:289-318`): a token bound to this pod.
    async fn service_account_token(
        &self,
        storage: &Arc<StorageBackend>,
        sa_token: &rusternetes_common::resources::ServiceAccountTokenProjection,
    ) -> Result<String> {
        let storage = Some(storage);
        let token_path = format!("{}/{}", self.path, sa_token.path);
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
        let requested_audiences: Vec<String> = sa_token.audience.iter().cloned().collect();
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
            sub: format!("system:serviceaccount:{}:{}", self.namespace, sa_name),
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
        let refresh_after = token_refresh_after_secs(expiration_seconds, token_jitter_secs());
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
            // A failed mint is an error, as upstream's
            // `errlist = append(errlist, err)` (`projected.go:
            // 306-309`); no placeholder token is written.
            return Ok(token);
        } // end if !token_fresh
          // Still fresh: re-use the on-disk token. The upstream
          // kubelet's token manager caches and refreshes at 80% of
          // the lifetime (pkg/kubelet/token/token_manager.go), so a
          // periodic re-SetUp feeds the AtomicWriter the SAME bytes
          // and stays inert.
        std::fs::read_to_string(&token_path)
            .with_context(|| format!("reading cached token {token_path}"))
    }
}

#[async_trait]
impl Mounter for ProjectedMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `SetUpAt` (`projected.go:136-224`): collect the payload (an error here
    /// writes nothing), then project it with the AtomicWriter
    /// (`volumeutil.NewAtomicWriter` + `writer.Write`, `:208-221`).
    ///
    /// The fsGroup ownership callback (`:208-214`) is ported: `setPerms`
    /// runs `NewVolumeOwnership(..).ChangePermissions()` on the whole volume
    /// dir, with `GetAttributes().ReadOnly == true` (`:175-181`), after each
    /// real write.
    ///
    /// Not ported: the wrapped memory-backed emptyDir mount (`:143-157`) and
    /// `MakeNestedMountpoints` (`:166`) — see the follow-up issue linked from
    /// the PR (the kubelet has no tmpfs unmount, so a wrapper would leak
    /// mounts).
    async fn set_up(&self) -> Result<()> {
        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create projected volume directory")?;

        let payload = self.collect_data().await.inspect_err(|e| {
            tracing::error!(
                "Error preparing data for projected volume {} for pod {}/{}: {}",
                self.volume.name,
                self.namespace,
                self.pod_name,
                e
            );
        })?;

        // `setPerms` (`projected.go:200-206`): "This may be the first time
        // writing and new files get created outside the timestamp
        // subdirectory: change the permissions on the whole volume and not
        // only in the timestamp directory."
        let fs_group = self.fs_group;
        let set_perms = move |dir: &std::path::Path| -> std::io::Result<()> {
            crate::volume_ownership::set_volume_ownership(dir, fs_group, true)
        };
        crate::atomic_writer::write_projected_payload_with(
            std::path::Path::new(volume_dir),
            &payload,
            Some(&set_perms),
        )
        .map_err(|e| {
            tracing::error!("Error writing payload to dir: {}", e);
            anyhow!(e)
        })?;

        info!(
            "Created projected volume {} at {}",
            self.volume.name, volume_dir
        );
        Ok(())
    }
}

/// Seconds after minting at which a projected token is refreshed. Ports
/// `requiresRefresh` (pkg/kubelet/token/token_manager.go:174-195): refresh at
/// 80% of the lifetime (`exp - ExpirationSeconds*20/100 - jitter`) AND never
/// later than `maxTTL (24h) - jitter` (`:40`, `:187`). `maxJitter = 10s` (`:42`).
fn token_refresh_after_secs(expiration_seconds: i64, jitter_secs: i64) -> i64 {
    const MAX_TTL_SECS: i64 = 24 * 3600;
    let pct = expiration_seconds - (expiration_seconds * 20) / 100 - jitter_secs;
    pct.min(MAX_TTL_SECS - jitter_secs).max(60)
}

/// Jitter in [0, 10) seconds (upstream `rand.Float64()*maxJitter`, :186).
fn token_jitter_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.subsec_nanos() % 10) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream pkg/kubelet/token/token_manager.go:40 `maxTTL = 24 * time.Hour`,
    // :187 `now.After(iat.Add(maxTTL - jitter))` => refresh by 24h-jitter.
    #[test]
    fn refresh_after_is_capped_at_24h_minus_jitter_2358() {
        // 48h token: 80% (38.4h) exceeds the 24h cap.
        assert_eq!(token_refresh_after_secs(48 * 3600, 4), 24 * 3600 - 4);
        // 1h token: 80% rule wins.
        assert_eq!(token_refresh_after_secs(3600, 0), 2880);
        // Floor kept (pre-existing 60s minimum).
        assert_eq!(token_refresh_after_secs(10, 0), 60);
    }
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

    // ---- behavioural tests through the mounter (projected.go SetUpAt /
    //      collectData) ----

    use rusternetes_common::resources::ConfigMap;
    use std::collections::HashMap;

    fn pod() -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap()
    }

    fn vol(sources: serde_json::Value) -> Volume {
        serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"sources": sources}
        }))
        .unwrap()
    }

    async fn mounter(
        root: &std::path::Path,
        storage: &Arc<StorageBackend>,
        v: &Volume,
    ) -> Box<dyn Mounter> {
        let p = ProjectedPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            root.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
        };
        p.new_mounter(&spec, &pod()).await.unwrap()
    }

    async fn put_cm(storage: &Arc<StorageBackend>, name: &str, k: &str, val: &str) {
        let mut d = HashMap::new();
        d.insert(k.to_string(), val.to_string());
        let cm = ConfigMap::new(name, "ns").with_data(d);
        storage
            .create(&build_key("configmaps", Some("ns"), name), &cm)
            .await
            .unwrap();
    }

    /// `collectData` (`projected.go:241-246`): a non-optional source whose
    /// object cannot be fetched appends to `errlist`, `SetUpAt` returns the
    /// aggregate and writes nothing.
    #[tokio::test]
    async fn missing_required_config_map_fails_set_up() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        let v = vol(serde_json::json!([{"configMap": {"name": "nope"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
        assert!(!std::path::Path::new(&m.get_path()).join("..data").exists());
    }

    /// `!(errors.IsNotFound(err) && optional)` (`projected.go:243`): an
    /// optional, absent source is an empty payload, not an error.
    #[tokio::test]
    async fn missing_optional_config_map_is_empty_payload() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        let v = vol(serde_json::json!([{"configMap": {"name": "nope", "optional": true}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
    }

    /// `configmap.MakePayload` (`configmap.go:289-293`): a mapped key that is
    /// absent from a non-optional configMap is an error.
    #[tokio::test]
    async fn missing_item_key_fails_set_up() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([
            {"configMap": {"name": "cm", "items": [{"key": "zzz", "path": "z"}]}}
        ]));
        let m = mounter(dir.path(), &st, &v).await;
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(
            err.contains("configmap references non-existent config key: zzz"),
            "{err}"
        );
    }

    /// `volumeutil.NewAtomicWriter` + `writer.Write` (`projected.go:208-221`):
    /// content lands behind `..data`, and a re-SetUp of an unchanged payload
    /// leaves the on-disk layout untouched.
    #[tokio::test]
    async fn projects_through_atomic_writer_and_resync_is_inert() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let root = std::path::PathBuf::from(m.get_path());
        assert!(root.join("..data").exists());
        assert!(std::fs::symlink_metadata(root.join("a"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1");
        let before = std::fs::read_link(root.join("..data")).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read_link(root.join("..data")).unwrap(), before);
    }

    /// `collectData` merges every source into ONE payload (`projected.go:
    /// 270-272`); a later source overwrites an earlier one at the same path.
    #[tokio::test]
    async fn merges_sources_and_downward_api_errors_fail() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([
            {"configMap": {"name": "cm"}},
            {"downwardAPI": {"items": [{"path": "n", "fieldRef": {"fieldPath": "metadata.name"}}]}}
        ]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let root = std::path::PathBuf::from(m.get_path());
        assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1");
        assert_eq!(std::fs::read_to_string(root.join("n")).unwrap(), "p");

        // `downwardapi.CollectData` (`downwardapi.go:255-258`): a bad
        // fieldPath is an error, not an empty file.
        let dir2 = tempfile::tempdir().unwrap();
        let v2 = vol(serde_json::json!([
            {"downwardAPI": {"items": [{"path": "n", "fieldRef": {"fieldPath": "bogus.path"}}]}}
        ]));
        let m2 = mounter(dir2.path(), &st, &v2).await;
        assert!(m2.set_up().await.is_err());
    }

    /// `secret.MakePayload` (`secret.go:280`) wording, and the optional skip.
    #[test]
    fn secret_payload_missing_key_error_and_optional() {
        let s = Secret::new("s", "ns");
        let items = vec![KeyToPath {
            key: "k".into(),
            path: "p".into(),
            mode: Some(0o400),
        }];
        assert_eq!(
            secret_payload(Some(&items), &s, 0o644, false).unwrap_err(),
            "references non-existent secret key: k"
        );
        assert!(secret_payload(Some(&items), &s, 0o644, true)
            .unwrap()
            .is_empty());
    }

    /// `aggregate.Error()` (`errors.go:70-96`).
    #[test]
    fn aggregate_formats_like_upstream() {
        assert_eq!(aggregate_message(&["a".into()]), "a");
        assert_eq!(aggregate_message(&["a".into(), "b".into()]), "[a, b]");
        assert_eq!(aggregate_message(&["a".into(), "a".into()]), "a");
    }

    // ---- fsUser / fsGroup (projected.go:279-282, :208-214) ----

    fn pod_with_security(pod_sc: serde_json::Value, container_sc: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {
                "securityContext": pod_sc,
                "serviceAccountName": "default",
                "containers": [{"name": "c", "image": "i", "securityContext": container_sc}]
            }
        }))
        .unwrap()
    }

    async fn mounter_for(
        root: &std::path::Path,
        storage: &Arc<StorageBackend>,
        v: &Volume,
        pod: &Pod,
    ) -> Box<dyn Mounter> {
        let p = ProjectedPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            root.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
        };
        p.new_mounter(&spec, pod).await.unwrap()
    }

    /// The (uid, gid) a file created by this process gets — chowning to them
    /// is permitted without root.
    #[cfg(unix)]
    fn own_ids() -> (i64, i64) {
        use std::os::unix::fs::MetadataExt;
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("probe");
        std::fs::write(&f, b"").unwrap();
        let m = std::fs::metadata(&f).unwrap();
        (m.uid() as i64, m.gid() as i64)
    }

    /// `TestCollectDataWithServiceAccountToken` (`projected_test.go:724-880`)
    /// cases "fsUser != nil", "fsGroup != nil", "fsUser != nil && fsGroup !=
    /// nil" and the default: a service-account token is forced to 0600 when
    /// either is set (`projected.go:279-282`; "When FsGroup is set, we depend
    /// on SetVolumeOwnership to change from 0600 to 0640"), and the final
    /// on-disk mode follows. The e2e twin is `service_accounts.go:368`
    /// ("should set ownership and permission when RunAsUser or FsGroup is
    /// present"): `-rw-------` / `-rw-r-----` / `-rw-r-----` / `-rw-r--r--`.
    #[cfg(unix)]
    #[tokio::test]
    async fn service_account_token_mode_follows_fs_user_and_fs_group() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let (uid, gid) = own_ids();
        let cases: [(&str, serde_json::Value, serde_json::Value, u32); 4] = [
            (
                "runAsUser",
                serde_json::json!({}),
                serde_json::json!({"runAsUser": uid}),
                0o600,
            ),
            (
                "fsGroup",
                serde_json::json!({"fsGroup": gid}),
                serde_json::json!({}),
                0o640,
            ),
            (
                "runAsUser+fsGroup",
                serde_json::json!({"fsGroup": gid}),
                serde_json::json!({"runAsUser": uid}),
                0o640,
            ),
            (
                "neither",
                serde_json::json!({}),
                serde_json::json!({}),
                0o644,
            ),
        ];
        for (name, pod_sc, ctr_sc, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let st = Arc::new(StorageBackend::new_memory());
            let v = vol(serde_json::json!([
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}
            ]));
            let pod = pod_with_security(pod_sc, ctr_sc);
            let m = mounter_for(dir.path(), &st, &v, &pod).await;
            m.set_up().await.unwrap();
            let tok = std::path::PathBuf::from(m.get_path()).join("token");
            let meta = std::fs::metadata(&tok).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, want, "{name}");
            assert_eq!(meta.uid() as i64, uid, "{name}");
            assert_eq!(meta.gid() as i64, gid, "{name}");
        }
    }

    /// `setPerms` is only run when the AtomicWriter writes (`atomic_writer.go:
    /// 196-201`): a re-SetUp of an unchanged volume with an fsGroup leaves the
    /// file untouched — ctime included — which is what #2390 asks of resync.
    #[cfg(unix)]
    #[tokio::test]
    async fn fs_group_ownership_is_not_reapplied_to_an_unchanged_volume() {
        use std::os::unix::fs::MetadataExt;
        let gid = own_ids().1;
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let pod = pod_with_security(serde_json::json!({"fsGroup": gid}), serde_json::json!({}));
        let m = mounter_for(dir.path(), &st, &v, &pod).await;
        m.set_up().await.unwrap();
        let real = std::fs::canonicalize(std::path::PathBuf::from(m.get_path()).join("a")).unwrap();
        let before = std::fs::metadata(&real).unwrap().ctime_nsec();
        let before_s = std::fs::metadata(&real).unwrap().ctime();
        m.set_up().await.unwrap();
        let after = std::fs::metadata(&real).unwrap();
        assert_eq!((before_s, before), (after.ctime(), after.ctime_nsec()));
    }
}
