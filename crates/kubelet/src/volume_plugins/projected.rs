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
        let mut unmounter = crate::volume_plugins::util::WrappedEmptyDirUnmounter::new(
            self.host.clone(),
            self.name(),
            vol_name,
            pod_uid,
        );
        // `c.plugin.deleteServiceAccountToken(c.podUID)` (`projected.go:454`).
        unmounter.delete_service_account_token = true;
        Ok(Box::new(unmounter))
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
    pub fn build_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> ProjectedMounter {
        let path = self
            .host
            .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name);
        ProjectedMounter {
            host: self.host.clone(),
            metrics: crate::volume_plugins::metrics_cached::cached_du(&path),
            path,
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
            // `GetNodeAllocatable` (`pkg/volume/plugins.go:397`) — needed
            // only when a downwardAPI source item has a `resourceFieldRef`,
            // same as Task 7's downwardAPI plugin.
            node_allocatable: self.host.get_node_allocatable().clone(),
        }
    }
}

pub struct ProjectedMounter {
    /// `s.plugin.kvHost` (`projected.go`), for the pod certificate manager.
    host: Arc<dyn VolumeHost>,
    path: String,
    volume: Volume,
    namespace: String,
    pod_name: String,
    pod: Pod,
    storage: Option<Arc<StorageBackend>>,
    node_allocatable: HashMap<String, String>,
    /// Embedded `volume.MetricsProvider` (`projected.go:119`).
    metrics: crate::volume_plugins::metrics_cached::CachedMetrics,
}

/// `utilerrors.NewAggregate(errlist).Error()`
/// (`staging/src/k8s.io/apimachinery/pkg/util/errors/errors.go:70-96`): one
/// error prints as itself; several print de-duplicated as `[a, b]`.
pub fn aggregate_message(errs: &[String]) -> String {
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
pub fn secret_payload(
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
    pub async fn collect_data(
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
                match self.service_account_token(tp).await {
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
        sa_token: &rusternetes_common::resources::ServiceAccountTokenProjection,
    ) -> Result<String> {
        let sa_name = self
            .pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.as_deref())
            .unwrap_or("default");
        // `auds = []string{tp.Audience}` when set: exactly what the projection
        // asked for. Empty means the api-server's own default audience, which
        // it then accepts - do NOT force "rusternetes", or a vanilla
        // api-server issues a token whose audience it rejects on use.
        let audiences: Vec<String> = sa_token.audience.iter().cloned().collect();
        // TokenRequest requires expirationSeconds >= 600 (10m).
        let expiration_seconds = sa_token.expiration_seconds.unwrap_or(3600).max(600);
        // `s.plugin.getServiceAccountToken(ns, saName, &TokenRequest{..})`
        // (`projected.go:334-346`): the kubelet token manager caches the token
        // and refreshes it at 80% of its lifetime, so a periodic re-SetUp feeds
        // the AtomicWriter the same bytes and stays inert.
        let tr = self
            .host
            .service_account_token_manager()
            .get_service_account_token(
                &self.namespace,
                sa_name,
                crate::token_manager::TokenRequest {
                    uid: String::new(),
                    spec: crate::token_manager::TokenRequestSpec {
                        audiences,
                        expiration_seconds: Some(expiration_seconds),
                        bound_object_ref: Some(crate::token_manager::BoundObjectReference {
                            api_version: "v1".to_string(),
                            kind: "Pod".to_string(),
                            name: self.pod_name.clone(),
                            uid: self.pod.metadata.uid.clone(),
                        }),
                    },
                    status: Default::default(),
                },
            )
            .await?;
        Ok(tr.status.token)
    }
}

impl crate::volume_plugins::plugin::MetricsProvider for ProjectedMounter {
    /// `volume.NewCachedMetrics(volume.NewMetricsDu(getPath(..)))` (`projected.go:119`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        self.metrics.get_metrics()
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
    /// The wrapped emptyDir (`:189-203`, `NewWrapperMounter`) is ported as in
    /// secret.rs: `setupDir` (0777 root) + `SetReady` metadata dir. It runs AFTER
    /// `collectData` (`:194-203`) so a failed collect leaves no volume behind
    /// (`TestInvalidPathProjected`). `MakeNestedMountpoints` (`:205`) follows,
    /// and a failed write runs the `defer` teardown (`:209-222`).
    ///
    /// The wrapped emptyDir is the `Medium: Memory` one (`:59-67`), so the
    /// volume is a tmpfs. Not ported (tracked on #2537):
    /// `deleteServiceAccountToken(podUID)` on `TearDownAt` (`:454`).
    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        let volume_dir = dir;

        let payload = self.collect_data(args).await.inspect_err(|e| {
            tracing::error!(
                "Error preparing data for projected volume {} for pod {}/{}: {}",
                self.volume.name,
                self.namespace,
                self.pod_name,
                e
            );
        })?;

        // `wrappedVolumeSpec()` is a `Medium: Memory` emptyDir
        // (projected.go:59-67): a tmpfs on the volume dir, then
        // `SetReady(getMetaDir())` (empty_dir.go:284).
        crate::volume_plugins::util::new_wrapper_mounter(
            &self.host,
            &self.volume.name,
            Some("Memory"),
            &self.pod,
        )
        .await?
        .set_up_at(volume_dir, args)
        .await
        .context("Failed to create projected volume directory")?;
        crate::volume_plugins::util::nested_volumes::make_nested_mountpoints(
            &self.volume.name,
            std::path::Path::new(volume_dir),
            &self.pod,
        )?;

        if let Err(e) = write_payload(std::path::Path::new(volume_dir), &payload, args.fs_group) {
            tracing::error!("Error writing payload to dir: {}", e);
            // `defer` teardown: `unmounter.TearDown()` -> `UnmountViaEmptyDir`
            // (ready dir, umount of the tmpfs, volume dir).
            if let Err(td) = crate::volume_plugins::util::unmount_via_empty_dir(
                &self.host,
                volume_dir,
                &self.volume.name,
                &self.pod.metadata.uid,
            )
            .await
            {
                tracing::error!("Error tearing down volume {}: {}", self.volume.name, td);
            }
            return Err(anyhow!(e));
        }

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
