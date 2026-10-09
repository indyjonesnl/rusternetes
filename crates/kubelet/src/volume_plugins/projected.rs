use crate::atomic_writer::FileProjection;
use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
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
        Ok(Box::new(self.build_mounter(spec, pod)))
    }

    /// `NewUnmounter` (`projected.go:126-148`): the wrapper plugins share one unmounter that
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

    /// `ConstructVolumeSpec` (`projected.go:126-148`): a bare `ProjectedVolumeSource{}`
    /// named after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"projected": {"sources": []}}),
        )
    }
}

impl ProjectedPlugin {
    /// `NewMounter` (`projected.go:112-124`), concrete so tests can reach
    /// `collect_data`.
    fn build_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> ProjectedMounter {
        ProjectedMounter {
            host: self.host.clone(),
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
        }
    }
}

struct ProjectedMounter {
    /// `s.plugin.kvHost` (`projected.go`), for the pod certificate manager.
    host: Arc<dyn VolumeHost>,
    path: String,
    volume: Volume,
    namespace: String,
    pod_name: String,
    pod: Pod,
    storage: Option<Arc<StorageBackend>>,
    token_manager: rusternetes_common::auth::TokenManager,
    node_allocatable: HashMap<String, String>,
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
fn secret_file_mode(fs_user: Option<i64>, fs_group: Option<i64>, default_mode: u32) -> u32 {
    if fs_user.is_some() || fs_group.is_some() {
        0o600
    } else {
        default_mode
    }
}

impl ProjectedMounter {
    /// `mounterArgs.FsUser` / `.FsGroup` (`volume.go:131-132`), read per call
    /// as `projected.go:276-282` does - never captured by `NewMounter`.
    fn secret_file_mode(
        &self,
        args: &crate::volume_plugins::MounterArgs,
        default_mode: u32,
    ) -> u32 {
        secret_file_mode(args.fs_user, args.fs_group, default_mode)
    }

    /// `collectData` (`projected.go:226-338`): build ONE payload from every
    /// source, accumulating errors, and fail with their aggregate.
    async fn collect_data(
        &self,
        args: &crate::volume_plugins::MounterArgs,
    ) -> Result<BTreeMap<String, FileProjection>> {
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
        for (source_index, source) in projected.sources.iter().flatten().enumerate() {
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
                                fs_user: args.fs_user,
                                data: token.into_bytes(),
                                mode: self.secret_file_mode(args, default_mode),
                            },
                        );
                    }
                    Err(e) => errlist.push(e.to_string()),
                }
            } else if let Some(ctb) = &source.cluster_trust_bundle {
                match trust_anchors_for(storage.as_ref(), ctb).await {
                    Ok(trust_anchors) => {
                        payload.insert(
                            ctb.path.clone(),
                            FileProjection {
                                fs_user: args.fs_user,
                                data: trust_anchors,
                                mode: self.secret_file_mode(args, default_mode),
                            },
                        );
                    }
                    Err(e) => errlist.push(e.to_string()),
                }
            } else if let Some(pc) = &source.pod_certificate {
                // `source.PodCertificate` arm (`projected.go:357-398`).
                let Some(manager) = self.host.pod_certificate_manager() else {
                    // `NoOpManager.GetPodCertificateCredentialBundle`
                    // (`podcertificatemanager.go:994`).
                    errlist.push("unimplemented".to_string());
                    continue;
                };
                let (key, certificates) = match manager
                    .get_pod_certificate_credential_bundle(
                        &self.namespace,
                        &self.pod_name,
                        &self.pod.metadata.uid,
                        &self.volume.name,
                        source_index,
                    )
                    .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        errlist.push(e.to_string());
                        continue;
                    }
                };
                pod_certificate_files(
                    pc,
                    key,
                    certificates,
                    args.fs_user,
                    self.secret_file_mode(args, default_mode),
                    &mut payload,
                );
            }
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
                secret: None,
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

    /// `projectedVolume.GetAttributes` (`pkg/volume/projected/projected.go:173-179`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: true,
            selinux_relabel: true,
        }
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
    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        let volume_dir = dir;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create projected volume directory")?;

        let payload = self.collect_data(args).await.inspect_err(|e| {
            tracing::error!(
                "Error preparing data for projected volume {} for pod {}/{}: {}",
                self.volume.name,
                self.namespace,
                self.pod_name,
                e
            );
        })?;

        write_payload(std::path::Path::new(volume_dir), &payload, args.fs_group).map_err(|e| {
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

/// The `source.ClusterTrustBundle` lookup of `collectData`
/// (`projected.go:320-345`): by name, else by signer name + label selector,
/// else an error. `allowEmpty` is `optional` (`:321-324`).
pub(crate) async fn trust_anchors_for<S: Storage + ?Sized>(
    storage: &S,
    ctb: &rusternetes_common::resources::ClusterTrustBundleProjection,
) -> Result<Vec<u8>> {
    let allow_empty = ctb.optional.unwrap_or(false);
    if let Some(name) = &ctb.name {
        crate::clustertrustbundle::get_trust_anchors_by_name(storage, name, allow_empty).await
    } else if let Some(signer) = &ctb.signer_name {
        crate::clustertrustbundle::get_trust_anchors_by_signer(
            storage,
            signer,
            ctb.label_selector.as_ref(),
            allow_empty,
        )
        .await
    } else {
        Err(anyhow!(
            "ClusterTrustBundle projection requires either name or signerName to be set"
        ))
    }
}

/// `volumeutil.NewAtomicWriter(..).Write(data, setPerms)` (`projected.go:
/// 200-221`). `setPerms` (`:200-206`): "This may be the first time writing and
/// new files get created outside the timestamp subdirectory: change the
/// permissions on the whole volume and not only in the timestamp directory."
/// The volume is read-only (`GetAttributes().ReadOnly`, `:175-181`).
pub(crate) fn write_payload(
    dir: &std::path::Path,
    payload: &BTreeMap<String, FileProjection>,
    fs_group: Option<i64>,
) -> std::io::Result<()> {
    crate::volume_ownership::write_payload_with_ownership(dir, payload, fs_group, true)
}

/// `(key, certificate chain)` of one `podCertificate` source, or the manager's
/// error text.
pub(crate) type BundleResult = std::result::Result<(Vec<u8>, Vec<u8>), String>;

/// The three file projections of a `podCertificate` source
/// (`projected.go:403-427`): `credentialBundlePath` is key followed by chain.
/// Shared by the mount path ([`ProjectedMounter::collect_data`]) and the
/// resync path ([`resync_payload`]), which must project identical bytes.
fn pod_certificate_files(
    pc: &rusternetes_common::resources::pod::PodCertificateProjection,
    key: Vec<u8>,
    certificates: Vec<u8>,
    fs_user: Option<i64>,
    mode: u32,
    payload: &mut BTreeMap<String, FileProjection>,
) {
    let mut put = |path: &Option<String>, data: Vec<u8>| {
        if let Some(path) = path.as_deref().filter(|p| !p.is_empty()) {
            payload.insert(
                path.to_string(),
                FileProjection {
                    fs_user,
                    data,
                    mode,
                },
            );
        }
    };
    let mut bundle = key.clone();
    bundle.extend_from_slice(&certificates);
    put(&pc.credential_bundle_path, bundle);
    put(&pc.key_path, key);
    put(&pc.certificate_chain_path, certificates);
}

/// The payload a periodic re-SetUp of a projected volume projects
/// (`collectData`, `projected.go:226-338`), built from objects the caller has
/// already fetched, so it can run on the blocking pool (#2390).
///
/// Same per-source rules as [`ProjectedMounter::collect_data`]: errors are
/// accumulated and returned as their aggregate (the volume is then left
/// untouched, as upstream's failed `SetUpAt` writes nothing); an optional,
/// absent source is an empty payload.
///
/// The service-account-token source keeps the bytes already on disk: minting
/// is the mounter's job (an async `TokenRequest`, refreshed at 80% of its
/// lifetime, `token_manager.go:174-195`), and re-projecting the SAME bytes is
/// what keeps the AtomicWriter inert.
#[allow(clippy::too_many_arguments)] // one lookup closure per fetched source kind
pub(crate) fn resync_payload<'a>(
    projected: &rusternetes_common::resources::ProjectedVolumeSource,
    pod: &Pod,
    node_allocatable: &HashMap<String, String>,
    volume_dir: &std::path::Path,
    secret: impl Fn(&str) -> Option<&'a Secret>,
    config_map: impl Fn(&str) -> Option<&'a ConfigMap>,
    trust_anchors: impl Fn(usize) -> Option<&'a std::result::Result<Vec<u8>, String>>,
    pod_certificates: impl Fn(usize) -> Option<&'a BundleResult>,
) -> std::result::Result<BTreeMap<String, FileProjection>, String> {
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
    let default_mode = projected.default_mode.unwrap_or(0o644) as u32;
    let fs_user = crate::volume_plugins::util::fs_user_from(pod);
    let fs_group = crate::volume_plugins::util::fs_group_from(pod);
    let mut errlist: Vec<String> = Vec::new();
    let mut payload: BTreeMap<String, FileProjection> = BTreeMap::new();
    for (index, source) in projected.sources.iter().flatten().enumerate() {
        if let Some(sp) = &source.secret {
            let name = sp.name.clone().unwrap_or_default();
            let optional = sp.optional.unwrap_or(false);
            let empty;
            let secret = match secret(&name) {
                Some(s) => s,
                None if optional => {
                    empty = Secret::new(&name, namespace);
                    &empty
                }
                None => {
                    errlist.push(format!("secret \"{name}\" not found"));
                    continue;
                }
            };
            match secret_payload(sp.items.as_ref(), secret, default_mode, optional) {
                Ok(p) => payload.extend(p),
                Err(e) => errlist.push(e),
            }
        } else if let Some(cp) = &source.config_map {
            let name = cp.name.clone().unwrap_or_default();
            let optional = cp.optional.unwrap_or(false);
            let empty;
            let cm = match config_map(&name) {
                Some(c) => c,
                None if optional => {
                    empty = ConfigMap::new(&name, namespace);
                    &empty
                }
                None => {
                    errlist.push(format!("configmap \"{name}\" not found"));
                    continue;
                }
            };
            match config_map_payload(cp.items.as_ref(), cm, default_mode, optional) {
                Ok(p) => payload.extend(p),
                Err(e) => errlist.push(e),
            }
        } else if let Some(da) = &source.downward_api {
            let items = da.items.as_deref().unwrap_or(&[]);
            match downward_api_payload(items, pod, node_allocatable, default_mode) {
                Ok(p) => payload.extend(p),
                Err(e) => errlist.push(e),
            }
        } else if let Some(tp) = &source.service_account_token {
            match std::fs::read(volume_dir.join(&tp.path)) {
                Ok(token) => {
                    payload.insert(
                        tp.path.clone(),
                        FileProjection {
                            fs_user,
                            data: token,
                            mode: secret_file_mode(fs_user, fs_group, default_mode),
                        },
                    );
                }
                Err(e) => errlist.push(format!(
                    "service account token {} is not projected yet: {e}",
                    tp.path
                )),
            }
        } else if let Some(ctb) = &source.cluster_trust_bundle {
            // Fetched by the async half of the resync (`FetchedSources`).
            match trust_anchors(index) {
                Some(Ok(data)) => {
                    payload.insert(
                        ctb.path.clone(),
                        FileProjection {
                            fs_user,
                            data: data.clone(),
                            mode: secret_file_mode(fs_user, fs_group, default_mode),
                        },
                    );
                }
                Some(Err(e)) => errlist.push(e.clone()),
                None => errlist.push("ClusterTrustBundle was not fetched".to_string()),
            }
        } else if let Some(pc) = &source.pod_certificate {
            // `source.PodCertificate` arm (`projected.go:391-428`); the bundle
            // was asked of the pod certificate manager by the async half of
            // the resync (`FetchedSources`), as `SetUpAt` re-runs
            // `collectData` on every remount (`RequiresRemount`).
            match pod_certificates(index) {
                Some(Ok((key, certificates))) => pod_certificate_files(
                    pc,
                    key.clone(),
                    certificates.clone(),
                    fs_user,
                    secret_file_mode(fs_user, fs_group, default_mode),
                    &mut payload,
                ),
                Some(Err(e)) => errlist.push(e.clone()),
                None => errlist.push("pod certificate bundle was not fetched".to_string()),
            }
        }
    }
    if errlist.is_empty() {
        Ok(payload)
    } else {
        Err(aggregate_message(&errlist))
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

    /// The args the operation generator builds from the pod
    /// (`operation_generator.go:582-589`).
    fn args_for(pod: &Pod) -> crate::volume_plugins::MounterArgs {
        crate::volume_plugins::MounterArgs {
            fs_user: crate::volume_plugins::util::fs_user_from(pod),
            fs_group: crate::volume_plugins::util::fs_group_from(pod),
            ..Default::default()
        }
    }

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
            read_only: false,
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
            read_only: false,
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
            read_only: false,
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
            read_only: false,
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
            m.set_up_with(&args_for(&pod)).await.unwrap();
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
        m.set_up_with(&args_for(&pod)).await.unwrap();
        let real = std::fs::canonicalize(std::path::PathBuf::from(m.get_path()).join("a")).unwrap();
        let before = std::fs::metadata(&real).unwrap().ctime_nsec();
        let before_s = std::fs::metadata(&real).unwrap().ctime();
        m.set_up_with(&args_for(&pod)).await.unwrap();
        let after = std::fs::metadata(&real).unwrap();
        assert_eq!((before_s, before), (after.ctime(), after.ctime_nsec()));
    }

    // ---- ClusterTrustBundle source (projected.go:320-355) ----

    fn test_root(tag: &str) -> String {
        pem::encode_config(
            &pem::Pem::new("CERTIFICATE", tag.as_bytes().to_vec()),
            pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
        )
    }

    async fn put_ctb(
        st: &Arc<StorageBackend>,
        name: &str,
        signer: Option<&str>,
        labels: serde_json::Value,
        bundle: &str,
    ) {
        let mut spec = serde_json::json!({"trustBundle": bundle});
        if let Some(s) = signer {
            spec["signerName"] = s.into();
        }
        let ctb = serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1beta1", "kind": "ClusterTrustBundle",
            "metadata": {"name": name, "labels": labels}, "spec": spec
        });
        st.create(&build_key("clustertrustbundles", None, name), &ctb)
            .await
            .unwrap();
    }

    async fn collect(
        st: &Arc<StorageBackend>,
        v: &Volume,
        pod: &Pod,
    ) -> Result<BTreeMap<String, FileProjection>> {
        let root = tempfile::tempdir().unwrap();
        let plugin = ProjectedPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            root.path().to_string_lossy().to_string(),
            Some(st.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
        };
        plugin
            .build_mounter(&spec, pod)
            .collect_data(&args_for(pod))
            .await
    }

    fn payload_bundle(p: &BTreeMap<String, FileProjection>, path: &str) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = pem::parse_many(&p[path].data)
            .unwrap()
            .into_iter()
            .map(|b| b.into_contents())
            .collect();
        v.sort();
        v
    }

    /// `TestCollectDataWithClusterTrustBundle` (`projected_test.go:882-1039`),
    /// the three upstream cases: by name, by signer name + label selector, and
    /// by name with a non-default `defaultMode`.
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_upstream_cases() {
        let st = Arc::new(StorageBackend::new_memory());
        let root1 = test_root("root1");
        put_ctb(&st, "foo", None, serde_json::json!({}), &root1).await;
        put_ctb(
            &st,
            "foo:example:bar",
            Some("foo.example/bar"),
            serde_json::json!({"key": "value"}),
            &root1,
        )
        .await;
        let want = vec![b"root1".to_vec()];

        // "single ClusterTrustBundle by name"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(payload_bundle(&got, "bundle.pem"), want);
        assert_eq!(got["bundle.pem"].mode, 0o644);
        assert_eq!(got["bundle.pem"].fs_user, None);

        // "single ClusterTrustBundle by signer name"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {
                    "signerName": "foo.example/bar",
                    "labelSelector": {"matchLabels": {"key": "value"}},
                    "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(payload_bundle(&got, "bundle.pem"), want);
        assert_eq!(got["bundle.pem"].mode, 0o644);

        // "single ClusterTrustBundle by name, non-default mode"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 384, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
    }

    /// `projected.go:344-352`: 0600 + `FsUser` when FsUser/FsGroup is set.
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_fs_user_and_group() {
        let st = Arc::new(StorageBackend::new_memory());
        put_ctb(&st, "foo", None, serde_json::json!({}), &test_root("r")).await;
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(
            &st,
            &v,
            &pod_with_security(serde_json::json!({"fsGroup": 1000}), serde_json::json!({})),
        )
        .await
        .unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
        assert_eq!(got["bundle.pem"].fs_user, None);
        let got = collect(
            &st,
            &v,
            &pod_with_security(
                serde_json::json!({}),
                serde_json::json!({"runAsUser": 1000}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
        assert_eq!(got["bundle.pem"].fs_user, Some(1000));
    }

    /// `projected.go:312-317` and the `allowEmpty` handling (`:321-324`).
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_errors_and_optional() {
        let st = Arc::new(StorageBackend::new_memory());
        let neither: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"path": "b.pem"}}
            ]}
        }))
        .unwrap();
        assert_eq!(
            collect(&st, &neither, &pod())
                .await
                .unwrap_err()
                .to_string(),
            "ClusterTrustBundle projection requires either name or signerName to be set"
        );
        let missing: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "nope", "path": "b.pem"}}
            ]}
        }))
        .unwrap();
        assert!(collect(&st, &missing, &pod()).await.is_err());
        let optional: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "nope", "optional": true, "path": "b.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &optional, &pod()).await.unwrap();
        assert!(got["b.pem"].data.is_empty());
    }

    /// `FakeKubeletVolumeHost.GetPodCertificateCredentialBundle`
    /// (`pkg/volume/testing/testing.go`): hardcoded `"key\n"`, `"cert\n"`.
    struct FakePodCertificateManager;

    #[async_trait]
    impl crate::podcertificate::Manager for FakePodCertificateManager {
        fn track_pod(&self, _pod: &Pod) {}
        fn forget_pod(&self, _pod: &Pod) {}
        async fn get_pod_certificate_credential_bundle(
            &self,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _volume_name: &str,
            _source_index: usize,
        ) -> Result<(Vec<u8>, Vec<u8>)> {
            Ok((b"key\n".to_vec(), b"cert\n".to_vec()))
        }
        fn metric_report(&self) -> crate::podcertificate::MetricReport {
            Default::default()
        }
    }

    async fn collect_pc(
        v: &Volume,
        pod: &Pod,
        with_manager: bool,
    ) -> Result<BTreeMap<String, FileProjection>> {
        let root = tempfile::tempdir().unwrap();
        let mut host = crate::volume_plugins::KubeletVolumeHost::new(
            root.path().to_string_lossy().to_string(),
            Some(Arc::new(StorageBackend::new_memory())),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        );
        if with_manager {
            host = host.with_pod_certificate_manager(Arc::new(FakePodCertificateManager));
        }
        let plugin = ProjectedPlugin::new(Arc::new(host));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
        };
        plugin
            .build_mounter(&spec, pod)
            .collect_data(&args_for(pod))
            .await
    }

    /// `TestCollectDataWithPodCertificate` (`projected_test.go:1041-1142`),
    /// case "credential bundle".
    #[tokio::test]
    async fn collect_data_pod_certificate_credential_bundle() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "credentialBundlePath": "credbundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect_pc(&v, &pod(), true).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got["credbundle.pem"].data, b"key\ncert\n");
        assert_eq!(got["credbundle.pem"].mode, 0o644);
    }

    /// Same test, case "key and cert bundle".
    #[tokio::test]
    async fn collect_data_pod_certificate_key_and_cert() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "keyPath": "key.pem",
                                    "certificateChainPath": "certificates.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect_pc(&v, &pod(), true).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got["key.pem"].data, b"key\n");
        assert_eq!(got["certificates.pem"].data, b"cert\n");
        assert_eq!(got["key.pem"].mode, 0o644);
    }

    /// `projected.go:372-375`: fsGroup set forces mode 0600, same as the
    /// other credential-bearing sources.
    #[tokio::test]
    async fn collect_data_pod_certificate_fs_group_forces_0600() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "keyPath": "key.pem"}}
            ]}
        }))
        .unwrap();
        let mut p = pod();
        p.spec.as_mut().unwrap().security_context =
            Some(serde_json::from_value(serde_json::json!({"fsGroup": 1000})).unwrap());
        let got = collect_pc(&v, &p, true).await.unwrap();
        assert_eq!(got["key.pem"].mode, 0o600);
    }

    /// The source index passed to the manager is the position in `sources`
    /// (`for sourceIndex, source := range s.source.Sources`), so it counts the
    /// non-certificate sources before it.
    #[tokio::test]
    async fn collect_data_pod_certificate_passes_source_index() {
        use std::sync::Mutex;
        struct Recorder(Mutex<Vec<usize>>);
        #[async_trait]
        impl crate::podcertificate::Manager for Recorder {
            fn track_pod(&self, _pod: &Pod) {}
            fn forget_pod(&self, _pod: &Pod) {}
            async fn get_pod_certificate_credential_bundle(
                &self,
                _n: &str,
                _p: &str,
                _u: &str,
                _v: &str,
                source_index: usize,
            ) -> Result<(Vec<u8>, Vec<u8>)> {
                self.0.lock().unwrap().push(source_index);
                Ok((b"k".to_vec(), b"c".to_vec()))
            }
            fn metric_report(&self) -> crate::podcertificate::MetricReport {
                Default::default()
            }
        }
        let rec = Arc::new(Recorder(Mutex::new(vec![])));
        let root = tempfile::tempdir().unwrap();
        let host = crate::volume_plugins::KubeletVolumeHost::new(
            root.path().to_string_lossy().to_string(),
            Some(Arc::new(StorageBackend::new_memory())),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )
        .with_pod_certificate_manager(rec.clone());
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"downwardAPI": {"items": []}},
                {"podCertificate": {"signerName": "a/b", "keyType": "ED25519", "keyPath": "k"}}
            ]}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        ProjectedPlugin::new(Arc::new(host))
            .build_mounter(&spec, &pod())
            .collect_data(&args_for(&pod()))
            .await
            .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), vec![1]);
    }

    /// Without a manager the host behaves as upstream's `NoOpManager`
    /// (`podcertificatemanager.go:994`): "unimplemented".
    #[tokio::test]
    async fn collect_data_pod_certificate_without_manager_errors() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "a/b", "keyType": "ED25519", "keyPath": "k"}}
            ]}
        }))
        .unwrap();
        let err = collect_pc(&v, &pod(), false).await.unwrap_err();
        assert_eq!(err.to_string(), "unimplemented");
    }
}
