//! The CSI external-attacher (#1460): a port of kubernetes-csi/external-attacher
//! `pkg/controller/csi_handler.go` (`csiHandler`), `trivial_handler.go`
//! (`trivialHandler`), `util.go` and `pkg/attacher/attacher.go`, with the
//! handler selection of `cmd/csi-attacher/main.go:227-262`.
//!
//! One [`CsiAttacher`] serves one CSI driver, as one external-attacher sidecar
//! does: it handles the `VolumeAttachment`s whose `spec.attacher` is its
//! driver name (`syncVA`, `controller.go:257-260`) and talks to the driver's
//! Controller service over its unix socket ([`CsiControllerClient`]).
//!
//! A driver that reports `PUBLISH_UNPUBLISH_VOLUME` gets the real handler:
//! `ControllerPublishVolume` / `ControllerUnpublishVolume` gated by the
//! `external-attacher/<driver>` finalizer on the `VolumeAttachment` and on the
//! `PersistentVolume`. Any other driver gets the trivial handler (every
//! attachment is marked attached, no finalizer, no RPC).
//!
//! NOT ported here, each a tracked follow-up on #1460: `ReconcileVA`
//! (`--reconcile-sync`, needs `ListVolumes` which the CSI proto subset lacks),
//! CSI migration of in-tree PVs (`translator`), and the creation of
//! `VolumeAttachment`s themselves (the AttachDetachController half, upstream
//! `pkg/volume/csi/csi_attacher.go` `Attach`).

use super::csi_provisioner::to_csi_access_mode;
use super::pvc_protection::update_or_reap;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use rusternetes_common::resources::csi::{
    CSINode, VolumeAttachment, VolumeAttachmentStatus, VolumeError,
};
use rusternetes_common::resources::volume::{
    CSIVolumeSource, PersistentVolumeMode, PersistentVolumeSpec,
};
use rusternetes_common::resources::{PersistentVolume, Secret};
use rusternetes_csi::controller_client::{
    ControllerError, CsiControllerClient, DriverCapabilities,
};
use rusternetes_csi::proto::controller_service_capability::rpc::Type as ControllerRpc;
use rusternetes_csi::proto::plugin_capability::service::Type as PluginService;
use rusternetes_csi::proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use rusternetes_csi::proto::{
    ControllerPublishVolumeRequest, ControllerUnpublishVolumeRequest, VolumeCapability,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// `vaNodeIDAnnotation` (`util.go:124`).
const VA_NODE_ID_ANNOTATION: &str = "csi.alpha.kubernetes.io/node-id";
/// `--retry-interval-start` / `--retry-interval-max` (`main.go:69-70`), the
/// per-item exponential limiter of the VA and PV queues (`main.go:275-276`).
const RETRY_INTERVAL_START: Duration = Duration::from_secs(1);
const RETRY_INTERVAL_MAX: Duration = Duration::from_secs(300);
/// `--timeout` (`main.go:68`): the budget of one attach or detach call.
pub const ATTACH_TIMEOUT: Duration = Duration::from_secs(15);
/// `--worker-threads` (`main.go:71`).
const WORKERS: usize = 10;
/// Resync when no event arrives; also bounds how late a backed-off retry runs.
const RESYNC: Duration = Duration::from_secs(15);

/// `SanitizeDriverName` (`util.go:128-136`).
pub fn sanitize_driver_name(driver: &str) -> String {
    let mut name: String = driver
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if name.ends_with('-') {
        // name must not end with '-'
        name.push('X');
    }
    name
}

/// `GetFinalizerName` (`util.go:139-141`).
pub fn finalizer_name(driver: &str) -> String {
    format!("external-attacher/{}", sanitize_driver_name(driver))
}

/// The message `err.Error()` yields upstream, and the gRPC code
/// `saveAttachError` records (`status.FromError`, `csi_handler.go:613-618`;
/// `MutableCSINodeAllocatableCount` is Beta and on by default in 1.35).
fn describe(err: &anyhow::Error) -> (String, Option<i32>) {
    match err
        .downcast_ref::<ControllerError>()
        .and_then(ControllerError::status)
    {
        Some(s) => (
            format!("rpc error: code = {:?} desc = {}", s.code(), s.message()),
            Some(s.code() as i32),
        ),
        None => (format!("{err:#}"), None),
    }
}

#[derive(Clone, Copy, Debug)]
struct Caps {
    /// `supportsAttach`: the driver implements `ControllerPublishVolume`.
    attach: bool,
    read_only: bool,
    single_node_multi_writer: bool,
}

impl Caps {
    /// `supportsPluginControllerService` + `supportsControllerCapabilities`
    /// (`main.go:227-262`, `:316-338`).
    fn of(c: &DriverCapabilities) -> Self {
        let service = c.plugin.contains(&PluginService::ControllerService);
        Self {
            attach: service
                && c.controller
                    .contains(&ControllerRpc::PublishUnpublishVolume),
            read_only: c.controller.contains(&ControllerRpc::PublishReadonly),
            single_node_multi_writer: c.controller.contains(&ControllerRpc::SingleNodeMultiWriter),
        }
    }
}

pub struct CsiAttacher<S: Storage> {
    storage: Arc<S>,
    driver_name: String,
    client: CsiControllerClient,
    /// `--default-fstype`.
    default_fs_type: String,
    capabilities: tokio::sync::OnceCell<Caps>,
    /// The VA queue's `ItemExponentialFailureRateLimiter`: failures and the
    /// earliest next attempt per VolumeAttachment name.
    retries: Mutex<HashMap<String, (u32, Instant)>>,
    retry_start: Duration,
    retry_max: Duration,
}

impl<S: Storage + 'static> CsiAttacher<S> {
    pub fn new(
        storage: Arc<S>,
        driver_name: impl Into<String>,
        client: CsiControllerClient,
    ) -> Self {
        Self {
            storage,
            driver_name: driver_name.into(),
            client: client.with_timeout(ATTACH_TIMEOUT),
            default_fs_type: String::new(),
            capabilities: tokio::sync::OnceCell::new(),
            retries: Mutex::new(HashMap::new()),
            retry_start: RETRY_INTERVAL_START,
            retry_max: RETRY_INTERVAL_MAX,
        }
    }

    /// `--default-fstype`.
    pub fn with_default_fs_type(mut self, fs_type: impl Into<String>) -> Self {
        self.default_fs_type = fs_type.into();
        self
    }

    // ---- loop ---------------------------------------------------------------

    /// Watch VolumeAttachments and PersistentVolumes; every event, and the
    /// resync tick, runs one reconcile pass.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting CSI attacher for driver {}", self.driver_name);
        loop {
            if let Err(e) = self.reconcile_all().await {
                error!("CSI attacher {}: reconcile failed: {e:#}", self.driver_name);
            }
            let va_prefix = build_prefix("volumeattachments", None);
            let pv_prefix = build_prefix("persistentvolumes", None);
            let (mut va_watch, mut pv_watch) = match (
                self.storage.watch(&va_prefix).await,
                self.storage.watch(&pv_prefix).await,
            ) {
                (Ok(a), Ok(b)) => (a, b),
                (a, b) => {
                    let e = a.err().or(b.err());
                    warn!("CSI attacher: watch failed: {e:?}; retrying");
                    tokio::time::sleep(RESYNC).await;
                    continue;
                }
            };
            loop {
                let wake = self.next_wake();
                let again = tokio::select! {
                    ev = va_watch.next() => matches!(ev, Some(Ok(_))),
                    ev = pv_watch.next() => matches!(ev, Some(Ok(_))),
                    _ = tokio::time::sleep(wake) => true,
                };
                if !again {
                    break;
                }
                if let Err(e) = self.reconcile_all().await {
                    error!("CSI attacher {}: reconcile failed: {e:#}", self.driver_name);
                }
            }
        }
    }

    /// Time until the earliest backed-off retry, at most [`RESYNC`].
    fn next_wake(&self) -> Duration {
        let now = Instant::now();
        let retries = self.retries.lock().unwrap();
        retries
            .values()
            .map(|(_, at)| at.saturating_duration_since(now))
            .min()
            .map_or(RESYNC, |d| d.clamp(Duration::from_millis(50), RESYNC))
    }

    /// One pass over every VolumeAttachment of this driver
    /// (`SyncNewOrUpdatedVolumeAttachment`) and every PersistentVolume
    /// (`SyncNewOrUpdatedPersistentVolume`).
    pub async fn reconcile_all(&self) -> Result<()> {
        let vas: Vec<VolumeAttachment> = self
            .storage
            .list(&build_prefix("volumeattachments", None))
            .await?;
        futures::stream::iter(
            vas.into_iter()
                .filter(|v| v.spec.attacher == self.driver_name),
        )
        .for_each_concurrent(WORKERS, |va| async move {
            let name = va.metadata.name.clone();
            if !self.due(&name) {
                return;
            }
            self.sync_new_or_updated_volume_attachment(&va).await;
        })
        .await;
        self.sync_persistent_volumes().await
    }

    fn due(&self, name: &str) -> bool {
        self.retries
            .lock()
            .unwrap()
            .get(name)
            .is_none_or(|(_, at)| Instant::now() >= *at)
    }

    /// `SyncNewOrUpdatedVolumeAttachment` (`csi_handler.go:234-253`): an error
    /// re-queues with exponential backoff, success resets it.
    async fn sync_new_or_updated_volume_attachment(&self, va: &VolumeAttachment) {
        let name = va.metadata.name.clone();
        let res = if va.metadata.deletion_timestamp.is_none() {
            self.sync_attach(va).await
        } else {
            self.sync_detach(va).await
        };
        match res {
            Ok(()) => {
                self.retries.lock().unwrap().remove(&name);
            }
            Err(e) => {
                debug!("CSI attacher: error processing VolumeAttachment {name}: {e:#}");
                let mut retries = self.retries.lock().unwrap();
                let n = retries.get(&name).map_or(0, |(n, _)| *n);
                let delay = self
                    .retry_start
                    .saturating_mul(2u32.saturating_pow(n.min(30)))
                    .min(self.retry_max);
                retries.insert(name, (n + 1, Instant::now() + delay));
            }
        }
    }

    async fn caps(&self) -> Result<Caps> {
        self.capabilities
            .get_or_try_init(|| async {
                Ok::<_, ControllerError>(Caps::of(&self.client.get_driver_capabilities().await?))
            })
            .await
            .copied()
            .map_err(Into::into)
    }

    fn va_key(name: &str) -> String {
        build_key("volumeattachments", None, name)
    }

    // ---- attach -------------------------------------------------------------

    /// `syncAttach` (`csi_handler.go:255-284`), or `trivialHandler`
    /// (`trivial_handler.go:49-65`) for a driver that cannot publish.
    async fn sync_attach(&self, va: &VolumeAttachment) -> Result<()> {
        let attached = va.status.as_ref().is_some_and(|s| s.attached);
        if attached {
            // Volume is attached and no force sync, there is nothing to be done.
            return Ok(());
        }
        let caps = self.caps().await?;
        if !caps.attach {
            self.mark_as_attached(&va.metadata.name, None).await?;
            info!("Marked VolumeAttachment {} as attached", va.metadata.name);
            return Ok(());
        }

        match self.csi_attach(va, &caps).await {
            Ok(publish_context) => {
                self.mark_as_attached(&va.metadata.name, Some(publish_context))
                    .await
                    .map_err(|e| anyhow!("failed to mark as attached: {e}"))?;
                Ok(())
            }
            Err(err) => {
                if let Err(save) = self.save_error(&va.metadata.name, &err, true).await {
                    debug!("Failed to save attach error to VolumeAttachment: {save}");
                }
                Err(anyhow!("failed to attach: {err:#}"))
            }
        }
    }

    /// `csiAttach` (`csi_handler.go:429-534`).
    async fn csi_attach(
        &self,
        va: &VolumeAttachment,
        caps: &Caps,
    ) -> Result<HashMap<String, String>> {
        // Check as much as possible before adding VA finalizer - it would
        // block deletion of VA on error.
        let spec = match (
            &va.spec.source.persistent_volume_name,
            &va.spec.source.inline_volume_spec,
        ) {
            (Some(_), Some(_)) => {
                return Err(anyhow!(
                    "both InlineCSIVolumeSource and PersistentVolumeName specified in VA source"
                ))
            }
            (Some(pv_name), None) => {
                let pv = self.get_pv(pv_name).await?;
                // Refuse to attach volumes that are marked for deletion.
                if pv.metadata.deletion_timestamp.is_some() {
                    return Err(anyhow!(
                        "PersistentVolume {:?} is marked for deletion",
                        pv.metadata.name
                    ));
                }
                let pv = self
                    .add_pv_finalizer(pv)
                    .await
                    .map_err(|e| anyhow!("could not add PersistentVolume finalizer: {e}"))?;
                pv.spec
            }
            (None, Some(inline)) => {
                if inline.csi.is_none() {
                    return Err(anyhow!("inline volume spec contains nil CSI source"));
                }
                inline.clone()
            }
            (None, None) => {
                return Err(anyhow!(
                    "neither InlineCSIVolumeSource nor PersistentVolumeName specified in VA source"
                ))
            }
        };
        let csi = csi_source(&spec)?;
        let volume_handle = csi.volume_handle.clone().unwrap_or_default();
        // "CO MUST set this field to false if SP does not have the
        // PUBLISH_READONLY controller capability"
        let read_only = caps.read_only && csi.read_only.unwrap_or(false);
        let volume_capability = self.volume_capability(&spec, caps.single_node_multi_writer)?;
        let secrets = self.credentials_from_pv(csi).await?;
        let node_id = self.node_id(&va.spec.node_name, None).await?;

        self.prepare_va(&va.metadata.name, &node_id).await?;

        let publish_context = self
            .client
            .controller_publish_volume(ControllerPublishVolumeRequest {
                volume_id: volume_handle,
                node_id,
                volume_capability: Some(volume_capability),
                readonly: read_only,
                secrets,
                volume_context: csi.volume_attributes.clone().unwrap_or_default(),
            })
            .await?;
        Ok(publish_context)
    }

    /// `prepareVAFinalizer` + `prepareVANodeID` + `patchVA`
    /// (`csi_handler.go:312-340`, `:513-521`).
    async fn prepare_va(&self, name: &str, node_id: &str) -> Result<()> {
        let key = Self::va_key(name);
        let mut va: VolumeAttachment = self.storage.get(&key).await?;
        let finalizer = finalizer_name(&self.driver_name);
        let mut modified = false;
        let finalizers = va.metadata.finalizers.get_or_insert_with(Vec::new);
        if !finalizers.contains(&finalizer) {
            finalizers.push(finalizer);
            modified = true;
        }
        let annotations = va.metadata.annotations.get_or_insert_with(HashMap::new);
        if annotations.get(VA_NODE_ID_ANNOTATION).map(String::as_str) != Some(node_id) {
            annotations.insert(VA_NODE_ID_ANNOTATION.to_string(), node_id.to_string());
            modified = true;
        }
        if modified {
            self.storage
                .update(&key, &va)
                .await
                .map_err(|e| anyhow!("could not save VolumeAttachment: {e}"))?;
        }
        Ok(())
    }

    /// `markAsAttached` (`util.go:38-56`).
    async fn mark_as_attached(
        &self,
        name: &str,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let key = Self::va_key(name);
        let mut va: VolumeAttachment = self.storage.get(&key).await?;
        let status = va.status.get_or_insert_with(empty_status);
        status.attached = true;
        status.attachment_metadata = metadata.filter(|m| !m.is_empty());
        status.attach_error = None;
        self.storage.update_status(&key, &va).await?;
        Ok(())
    }

    /// `saveAttachError` / `saveDetachError` (`csi_handler.go:603-645`).
    async fn save_error(&self, name: &str, err: &anyhow::Error, attach: bool) -> Result<()> {
        let key = Self::va_key(name);
        let mut va: VolumeAttachment = self.storage.get(&key).await?;
        let (message, code) = describe(err);
        let status = va.status.get_or_insert_with(empty_status);
        let volume_error = VolumeError {
            time: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            message: Some(message),
            // Only the attach error carries the code upstream.
            error_code: if attach { code } else { None },
        };
        if attach {
            status.attach_error = Some(volume_error);
        } else {
            status.detach_error = Some(volume_error);
        }
        self.storage.update_status(&key, &va).await?;
        Ok(())
    }

    // ---- detach -------------------------------------------------------------

    /// `syncDetach` (`csi_handler.go:286-310`).
    async fn sync_detach(&self, va: &VolumeAttachment) -> Result<()> {
        let finalizer = finalizer_name(&self.driver_name);
        let has_finalizer = va
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.contains(&finalizer));
        if !has_finalizer {
            // The attachment is already detached.
            return Ok(());
        }
        match self.csi_detach(va).await {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(save) = self.save_error(&va.metadata.name, &err, false).await {
                    debug!("Failed to save detach error to VolumeAttachment: {save}");
                }
                Err(anyhow!("failed to detach: {err:#}"))
            }
        }
    }

    /// `csiDetach` (`csi_handler.go:536-601`).
    async fn csi_detach(&self, va: &VolumeAttachment) -> Result<()> {
        let spec = match (
            &va.spec.source.persistent_volume_name,
            &va.spec.source.inline_volume_spec,
        ) {
            (Some(_), Some(_)) => {
                return Err(anyhow!(
                    "both InlineCSIVolumeSource and PersistentVolumeName specified in VA source"
                ))
            }
            (Some(pv_name), None) => self.get_pv(pv_name).await?.spec,
            (None, Some(inline)) => {
                if inline.csi.is_none() {
                    return Err(anyhow!("inline volume spec contains nil CSI source"));
                }
                inline.clone()
            }
            (None, None) => {
                return Err(anyhow!(
                    "neither InlineCSIVolumeSource nor PersistentVolumeName specified in VA source"
                ))
            }
        };
        let csi = csi_source(&spec)?;
        let volume_handle = csi.volume_handle.clone().unwrap_or_default();
        let secrets = self.credentials_from_pv(csi).await?;
        let node_id = self.node_id(&va.spec.node_name, Some(va)).await?;

        self.client
            .controller_unpublish_volume(ControllerUnpublishVolumeRequest {
                volume_id: volume_handle,
                node_id,
                secrets,
            })
            .await?;
        self.mark_as_detached(&va.metadata.name)
            .await
            .map_err(|e| anyhow!("could not mark as detached: {e}"))
    }

    /// `markAsDetached` (`util.go:58-121`): the finalizer is removed FIRST, so
    /// a deleted VolumeAttachment never triggers a second Unpublish.
    async fn mark_as_detached(&self, name: &str) -> Result<()> {
        let key = Self::va_key(name);
        let mut va: VolumeAttachment = match self.storage.get(&key).await {
            Ok(v) => v,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let finalizer = finalizer_name(&self.driver_name);
        let finalizers = va.metadata.finalizers.take().unwrap_or_default();
        let found = finalizers.contains(&finalizer);
        let attached = va.status.as_ref().is_some_and(|s| s.attached);
        if !found && !attached {
            // Finalizer was not present, nothing to update.
            return Ok(());
        }
        let rest: Vec<String> = finalizers.into_iter().filter(|f| *f != finalizer).collect();
        va.metadata.finalizers = if rest.is_empty() { None } else { Some(rest) };
        if found {
            // The API server (here: `update_or_reap`) deletes an object that is
            // being deleted and has no finalizer left.
            update_or_reap(&*self.storage, &key, &va).await?;
        }
        let status = va.status.get_or_insert_with(empty_status);
        status.attached = false;
        status.detach_error = None;
        status.attachment_metadata = None;
        match self.storage.update_status(&key, &va).await {
            Ok(_) => Ok(()),
            // "The VolumeAttachment does not have any finalizer, it might
            // have been deleted by the API server."
            Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    // ---- PersistentVolume ---------------------------------------------------

    async fn get_pv(&self, name: &str) -> Result<PersistentVolume> {
        Ok(self
            .storage
            .get(&build_key("persistentvolumes", None, name))
            .await?)
    }

    /// `addPVFinalizer` (`csi_handler.go:342-363`).
    async fn add_pv_finalizer(&self, mut pv: PersistentVolume) -> Result<PersistentVolume> {
        let finalizer = finalizer_name(&self.driver_name);
        if pv
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.contains(&finalizer))
        {
            return Ok(pv);
        }
        pv.metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push(finalizer);
        let key = build_key("persistentvolumes", None, &pv.metadata.name);
        Ok(self.storage.update(&key, &pv).await?)
    }

    /// `SyncNewOrUpdatedPersistentVolume` (`csi_handler.go:647-722`), minus the
    /// migration branch: a PV being deleted that still carries this driver's
    /// finalizer loses it once no VolumeAttachment references the PV.
    async fn sync_persistent_volumes(&self) -> Result<()> {
        let finalizer = finalizer_name(&self.driver_name);
        let pvs: Vec<PersistentVolume> = self
            .storage
            .list(&build_prefix("persistentvolumes", None))
            .await?;
        let candidates: Vec<PersistentVolume> = pvs
            .into_iter()
            .filter(|pv| {
                pv.metadata.deletion_timestamp.is_some()
                    && pv
                        .metadata
                        .finalizers
                        .as_ref()
                        .is_some_and(|f| f.contains(&finalizer))
            })
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let vas: Vec<VolumeAttachment> = self
            .storage
            .list(&build_prefix("volumeattachments", None))
            .await?;
        for mut pv in candidates {
            let needed = vas.iter().any(|va| {
                va.spec.source.persistent_volume_name.as_deref() == Some(&pv.metadata.name)
            });
            if needed {
                // This PV is needed by this VA, don't remove finalizer.
                continue;
            }
            if let Some(f) = pv.metadata.finalizers.as_mut() {
                f.retain(|x| *x != finalizer);
                if f.is_empty() {
                    pv.metadata.finalizers = None;
                }
            }
            let key = build_key("persistentvolumes", None, &pv.metadata.name);
            if let Err(e) = update_or_reap(&*self.storage, &key, &pv).await {
                warn!("Failed to remove finalizer from PersistentVolume: {e}");
            } else {
                info!(
                    "Removed finalizer from PersistentVolume {}",
                    pv.metadata.name
                );
            }
        }
        Ok(())
    }

    // ---- request pieces -----------------------------------------------------

    /// `getNodeID` (`csi_handler.go:747-775`): the CSINode first; the VA's
    /// node-id annotation only when the caller passes the VA (detach).
    async fn node_id(&self, node_name: &str, va: Option<&VolumeAttachment>) -> Result<String> {
        let from_csi_node: Result<String> = match self
            .storage
            .get::<CSINode>(&build_key("csinodes", None, node_name))
            .await
        {
            Ok(n) => match n.spec.drivers.iter().find(|d| d.name == self.driver_name) {
                Some(d) => return Ok(d.node_id.clone()),
                // The CSI pod may not be running, e.g. the node is shut down;
                // don't block the controller unpublish in that scenario.
                None => Err(anyhow!(
                    "CSINode {node_name} does not contain driver {}",
                    self.driver_name
                )),
            },
            Err(e) => Err(e.into()),
        };
        if let Some(id) = va
            .and_then(|v| v.metadata.annotations.as_ref())
            .and_then(|a| a.get(VA_NODE_ID_ANNOTATION))
        {
            return Ok(id.clone());
        }
        from_csi_node
    }

    /// `getCredentialsFromPV` (`csi_handler.go:724-743`).
    async fn credentials_from_pv(&self, csi: &CSIVolumeSource) -> Result<HashMap<String, String>> {
        let Some(r) = csi.controller_publish_secret_ref.as_ref() else {
            return Ok(HashMap::new());
        };
        let ns = r.namespace.as_deref().unwrap_or_default();
        let name = r.name.as_deref().unwrap_or_default();
        let secret: Secret = self
            .storage
            .get(&build_key("secrets", Some(ns), name))
            .await
            .map_err(|e| anyhow!("failed to load secret \"{ns}/{name}\": {e}"))?;
        Ok(secret
            .data
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
            .collect())
    }

    /// `GetVolumeCapabilities` (`util.go:153-194`).
    fn volume_capability(
        &self,
        spec: &PersistentVolumeSpec,
        single_node_multi_writer: bool,
    ) -> Result<VolumeCapability> {
        let csi = csi_source(spec)?;
        let access_type = if matches!(spec.volume_mode, Some(PersistentVolumeMode::Block)) {
            AccessType::Block(BlockVolume {})
        } else {
            let fs_type = match csi.fs_type.as_deref() {
                Some(f) if !f.is_empty() => f.to_string(),
                _ => self.default_fs_type.clone(),
            };
            AccessType::Mount(MountVolume {
                fs_type,
                mount_flags: spec.mount_options.clone().unwrap_or_default(),
                volume_mount_group: String::new(),
            })
        };
        let mode = to_csi_access_mode(&spec.access_modes, single_node_multi_writer)
            .map_err(|e| anyhow!(e))?;
        Ok(VolumeCapability {
            access_type: Some(access_type),
            access_mode: Some(AccessMode { mode: mode as i32 }),
        })
    }
}

fn empty_status() -> VolumeAttachmentStatus {
    VolumeAttachmentStatus {
        attached: false,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    }
}

/// `getCSISource` (`csi_handler.go:389-397`).
fn csi_source(spec: &PersistentVolumeSpec) -> Result<&CSIVolumeSource> {
    spec.csi
        .as_ref()
        .ok_or_else(|| anyhow!("pv spec contained non-csi source that was not migrated"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TestSanitizeDriverName` (`util_test.go`).
    #[test]
    fn driver_names_sanitize_like_upstream() {
        assert_eq!(sanitize_driver_name("csi/example.com"), "csi-example-com");
        assert_eq!(sanitize_driver_name("a."), "a-X");
        assert_eq!(
            finalizer_name("test.csi.example.com"),
            "external-attacher/test-csi-example-com"
        );
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        // 1s, 2s, 4s ... capped at 5m (`ItemExponentialFailureRateLimiter`).
        let d = |n: u32| {
            RETRY_INTERVAL_START
                .saturating_mul(2u32.saturating_pow(n))
                .min(RETRY_INTERVAL_MAX)
        };
        assert_eq!(d(0), Duration::from_secs(1));
        assert_eq!(d(3), Duration::from_secs(8));
        assert_eq!(d(20), RETRY_INTERVAL_MAX);
    }
}
