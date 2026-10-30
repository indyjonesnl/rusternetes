use anyhow::Result;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    get_default_class, NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeMode,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode,
    VolumeNodeAffinity,
};
use rusternetes_common::resources::{
    EventSource, EventType, Node, PersistentVolume, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus, Pod,
};
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

pub const ANN_BOUND_BY_CONTROLLER: &str = "pv.kubernetes.io/bound-by-controller";
/// `storagehelpers.AnnBindCompleted` (component-helpers pv_helpers.go:39).
pub const ANN_BIND_COMPLETED: &str = "pv.kubernetes.io/bind-completed";
pub const ANN_DYNAMICALLY_PROVISIONED: &str = "pv.kubernetes.io/provisioned-by";
/// `storagehelpers.AnnMigratedTo` (component-helpers pv_helpers.go).
pub const ANN_MIGRATED_TO: &str = "pv.kubernetes.io/migrated-to";
/// `util.AnnPreResizeCapacity` (`pkg/volume/util/resize_util.go:52`).
const ANN_PRE_RESIZE_CAPACITY: &str = "volume.alpha.kubernetes.io/pre-resize-capacity";
/// `storagehelpers.AnnStorageProvisioner` (pv_helpers.go:75).
pub const ANN_STORAGE_PROVISIONER: &str = "volume.kubernetes.io/storage-provisioner";
/// `storagehelpers.AnnBetaStorageProvisioner` (pv_helpers.go:76).
pub const ANN_BETA_STORAGE_PROVISIONER: &str = "volume.beta.kubernetes.io/storage-provisioner";

/// A Kubernetes `resource.Quantity` as an exact integer count of nano-units,
/// or `None` when it does not parse (`resource.ParseQuantity`). Enough to
/// implement `Quantity.Cmp` equality, so `1Gi` equals `1024Mi`.
pub fn parse_quantity_nano(q: &str) -> Option<i128> {
    let q = q.trim();
    let (sign, q) = match q.strip_prefix('-') {
        Some(r) => (-1i128, r),
        None => (1, q.strip_prefix('+').unwrap_or(q)),
    };
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let (int_s, frac_s) = num.split_once('.').unwrap_or((num, ""));
    if int_s.is_empty() && frac_s.is_empty() {
        return None;
    }
    let digits: i128 = format!("{int_s}{frac_s}").parse().ok()?;
    let scale = 10i128.checked_pow(u32::try_from(frac_s.len()).ok()?)?;
    let nano: i128 = match suffix {
        "n" => 1,
        "u" => 1_000,
        "m" => 1_000_000,
        "" => 1_000_000_000,
        "k" => 10i128.pow(12),
        "M" => 10i128.pow(15),
        "G" => 10i128.pow(18),
        "T" => 10i128.pow(21),
        "P" => 10i128.pow(24),
        "E" => 10i128.pow(27),
        "Ki" => 1024 * 10i128.pow(9),
        "Mi" => 1024i128.pow(2) * 10i128.pow(9),
        "Gi" => 1024i128.pow(3) * 10i128.pow(9),
        "Ti" => 1024i128.pow(4) * 10i128.pow(9),
        "Pi" => 1024i128.pow(5) * 10i128.pow(9),
        "Ei" => 1024i128.pow(6) * 10i128.pow(9),
        exp => {
            let e: i32 = exp.strip_prefix(['e', 'E'])?.parse().ok()?;
            if !(-9..=18).contains(&e) {
                return None;
            }
            10i128.pow((9 + e) as u32)
        }
    };
    Some(sign * digits.checked_mul(nano)? / scale)
}

/// `volumeCap.Cmp(claimCap) == 0` (`pv_controller.go:835`); unparsable
/// quantities compare by their text.
pub fn quantity_eq(a: &str, b: &str) -> bool {
    match (parse_quantity_nano(a), parse_quantity_nano(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// `AnnSelectedNode` (component-helpers `pv_helpers.go:51`).
const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
/// `v1.BetaStorageClassAnnotation`.
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";

/// `util.IsPodTerminated` (`pkg/volume/util/util.go:303-310`).
pub(crate) fn is_pod_terminated(pod: &Pod) -> bool {
    use rusternetes_common::resources::pod::{
        ContainerState, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus,
    };
    use rusternetes_common::types::Phase;
    let Some(status) = pod.status.as_ref() else {
        return false;
    };
    if matches!(status.phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
        return true;
    }
    // `notRunning` (util.go:312-319): every status is Terminated or Waiting.
    let not_running = |s: &Option<Vec<ContainerStatus>>| {
        s.iter().flatten().all(|c| {
            matches!(
                c.state,
                Some(ContainerState {
                    terminated: Some(ContainerStateTerminated { .. }),
                    ..
                }) | Some(ContainerState {
                    waiting: Some(ContainerStateWaiting { .. }),
                    ..
                })
            )
        })
    };
    pod.metadata.deletion_timestamp.is_some()
        && not_running(&status.init_container_statuses)
        && not_running(&status.container_statuses)
        && not_running(&status.ephemeral_container_statuses)
}

/// `hostPathDeleter.Delete` (`pkg/volume/hostpath/host_path.go:354-360`):
///
/// ```go
/// regexp := regexp.MustCompile("/tmp/.+")
/// if !regexp.MatchString(r.GetPath()) {
///     return fmt.Errorf("host_path deleter only supports /tmp/.+ but received provided %s", r.GetPath())
/// }
/// return os.RemoveAll(r.GetPath())
/// ```
///
/// Deliberate hardening (destructive operation): upstream's regex is
/// unanchored, so `/x/tmp/y` and `/tmp/a/../../etc` pass. Here the path must
/// START with `/tmp/` followed by something and contain no `..` component.
/// The error text is unchanged. `os.RemoveAll` semantics: a missing path is
/// success, and a symlink is removed itself, never followed.
async fn host_path_delete(path: &str) -> std::result::Result<(), String> {
    let refused =
        || format!("host_path deleter only supports /tmp/.+ but received provided {path}");
    let has_tail = path
        .strip_prefix("/tmp/")
        .is_some_and(|rest| !rest.is_empty());
    let traverses = std::path::Path::new(path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir));
    if !has_tail || traverses {
        return Err(refused());
    }
    let meta = match tokio::fs::symlink_metadata(path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("failed to remove {path}: {e}")),
    };
    let res = if meta.is_dir() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    };
    match res {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(format!("failed to remove {path}: {e}"))
        }
        _ => Ok(()),
    }
}

/// `FindDeletablePluginByName` names this controller has a deleter for. The
/// upstream in-tree one is hostPath (`kubernetes.io/host-path`,
/// `pkg/volume/hostpath/host_path.go:77`); the rest are the names
/// `DynamicProvisioner` stamps on the PVs it creates (Rusternetes-only).
const BUILTIN_DELETER_PLUGINS: &[&str] = &[
    "kubernetes.io/host-path",
    "kubernetes.io/hostpath",
    "rusternetes.io/hostpath",
    "hostpath",
];

/// `findDeletablePlugin` (`pv_controller.go:1954-1991`): `Ok(true)` a deleter
/// plugin handles the volume, `Ok(false)` an external deleter does (do
/// nothing), `Err` no deleter (the message becomes the event text).
pub fn find_deletable_plugin(pv: &PersistentVolume) -> std::result::Result<bool, String> {
    const NO_PLUGIN: &str = "no deletable volume plugin matched";
    let ann = |k: &str| pv.metadata.annotations.as_ref().and_then(|a| a.get(k));
    if let Some(name) = ann(ANN_DYNAMICALLY_PROVISIONED).filter(|n| !n.is_empty()) {
        if BUILTIN_DELETER_PLUGINS.contains(&name.as_str()) {
            return Ok(true);
        }
        if !name.starts_with("kubernetes.io/") {
            // External provisioner is requested, do not report error.
            return Ok(false);
        }
        return Err(NO_PLUGIN.to_string());
    }
    // CSI migration scenario / CSI volume source: external provisioner.
    if ann(ANN_MIGRATED_TO).is_some() || pv.spec.csi.is_some() {
        return Ok(false);
    }
    // "Try to find a plugin by spec."
    if pv.spec.host_path.is_some() {
        return Ok(true);
    }
    Err(format!(
        "error getting deleter volume plugin for volume {:?}: {}",
        pv.metadata.name, NO_PLUGIN
    ))
}

/// Whether `pod` references `claim_name` (`PodPVCIndex`: a
/// `persistentVolumeClaim` source or an ephemeral volume's `<pod>-<volume>`).
fn pod_uses_claim(pod: &Pod, claim_name: &str) -> bool {
    pod.spec.as_ref().is_some_and(|spec| {
        spec.volumes.iter().flatten().any(|v| {
            if let Some(src) = v.persistent_volume_claim.as_ref() {
                src.claim_name == claim_name
            } else if v.ephemeral.is_some() {
                format!("{}-{}", pod.metadata.name, v.name) == claim_name
            } else {
                false
            }
        })
    })
}

pub struct PVBinderController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    /// `ctrl.runningOperations` (`goroutinemap`): names of the volume
    /// operations currently running, so one is never started twice
    /// (`scheduleOperation`, `pv_controller.go:1895-1914`).
    running_operations: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// The recycler `VolumeConfig` of the hostPath and NFS plugins
    /// (`ProbeProvisionableRecyclableVolumePlugins`, `plugins.go:100-120`).
    recycler_config: Arc<super::pv_recycler::RecyclerConfig>,
}

impl<S: Storage + 'static> PVBinderController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            running_operations: Arc::default(),
            recycler_config: Arc::default(),
        }
    }

    /// Use the recycler plugin config built from the `--pv-recycler-*` flags
    /// (`newPersistentVolumeBinderController`, `core.go:322`).
    pub fn with_recycler_config(mut self, config: super::pv_recycler::RecyclerConfig) -> Self {
        self.recycler_config = Arc::new(config);
        self
    }

    /// A handle for a detached volume operation: shares storage, recorder and
    /// the running-operation set (upstream's operations are goroutines closing
    /// over `ctrl`).
    fn operation_handle(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            recorder: self.recorder.clone(),
            running_operations: Arc::clone(&self.running_operations),
            recycler_config: Arc::clone(&self.recycler_config),
        }
    }

    /// `scheduleOperation` (`pv_controller.go:1895-1914`) with
    /// `goroutinemap.Run`: start `op` detached unless one with the same name
    /// is running ("Operation is already running, skipping").
    fn schedule_operation<F>(&self, name: String, op: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        {
            let mut running = self
                .running_operations
                .lock()
                .expect("running operations mutex poisoned");
            if !running.insert(name.clone()) {
                debug!("Operation {} is already running, skipping", name);
                return;
            }
        }
        let running = Arc::clone(&self.running_operations);
        tokio::spawn(async move {
            op.await;
            running
                .lock()
                .expect("running operations mutex poisoned")
                .remove(&name);
        });
    }

    /// Whether any volume operation is still running (test synchronisation:
    /// upstream's tests wait on `runningOperations`).
    pub fn operations_running(&self) -> bool {
        !self
            .running_operations
            .lock()
            .expect("running operations mutex poisoned")
            .is_empty()
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting PV/PVC Binder Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Upstream's volume informer drives syncVolume on every PV event.
            let pv_prefix = rusternetes_storage::build_prefix("persistentvolumes", None);
            let mut pv_watch = match self.storage.watch(&pv_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish PV watch: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pv_watch.next() => {
                        match event {
                            Some(Ok(_)) => {
                                if let Err(e) = self.sync_volumes(&queue).await {
                                    error!("PV availability pass failed: {}", e);
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("PV watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("PV watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("persistentvolumeclaims", Some(ns), name);
            match self
                .storage
                .get::<PersistentVolumeClaim>(&storage_key)
                .await
            {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.bind_pvc(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        // Volume side of the sync (upstream syncVolume).
        if let Err(e) = self.sync_volumes(queue).await {
            error!("PV availability pass failed: {}", e);
        }
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("persistentvolumeclaims/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list persistentvolumeclaims for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        // Get all PVCs
        let pvcs: Vec<PersistentVolumeClaim> = self
            .storage
            .list("/registry/persistentvolumeclaims/")
            .await?;

        for mut pvc in pvcs {
            if let Err(e) = self.bind_pvc(&mut pvc).await {
                error!("Failed to bind PVC {}: {}", pvc.metadata.name, e);
            }
        }

        // Volume side of the sync (upstream syncVolume): release/reclaim,
        // Bound, unbind. Claims it enqueues go to a throwaway queue here.
        if let Err(e) = self.sync_volumes(&WorkQueue::new()).await {
            error!("PV sync pass failed: {}", e);
        }

        Ok(())
    }

    /// Volume side of the controller: run [`Self::sync_volume`] over every PV.
    /// Upstream's volume informer invokes `syncVolume` per PV event/resync
    /// (`pv_controller_base.go` `volumeWorker`); this controller has no
    /// per-key PV queue, so a PV event (or the resync) sweeps all PVs. A
    /// failure on one PV is logged and does not stop the others — the next
    /// event/resync retries it ("Nothing was saved; we will fall back into the
    /// same condition in the next call").
    ///
    /// `claim_queue` is upstream's `ctrl.claimQueue`: `syncVolume` enqueues a
    /// claim there to speed up its `syncClaim` (`pv_controller.go:701-706`).
    pub async fn sync_volumes(&self, claim_queue: &WorkQueue) -> Result<()> {
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;
        for pv in pvs {
            // Being deleted: nothing to advance.
            if pv.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let name = pv.metadata.name.clone();
            if let Err(e) = self.sync_volume(pv, claim_queue).await {
                error!("Failed to sync PV {}: {}", name, e);
            }
        }
        Ok(())
    }

    /// Port of upstream `PersistentVolumeController.syncVolume`
    /// (`pkg/controller/volume/persistentvolume/pv_controller.go:556-770`),
    /// the decision table of binder_test.go "4-1".."4-12" and
    /// "14-13".."14-14":
    ///
    /// * no `claimRef`, or `claimRef.uid` empty (pre-bound, not yet bound):
    ///   `Available` (`:576-596`).
    /// * claim not found, or found with a different UID (the old one was
    ///   deleted and recreated): `Released` unless already `Released`/`Failed`
    ///   ("do not overwrite previous Failed state", `:668-681`), then
    ///   `reclaimVolume` (`:682-690`).
    /// * claim found but `spec.volumeName` empty: on a volumeMode mismatch emit
    ///   `VolumeMismatch` on the PV and the claim and skip; otherwise enqueue
    ///   the claim so `syncClaim` finishes the bind (`:694-728`).
    /// * claim bound to this volume: `Bound` (`:729-736`).
    /// * claim bound elsewhere: a dynamically provisioned `Delete` volume is
    ///   released and reclaimed; otherwise `unbindVolume` (`:737-770`).
    ///
    async fn sync_volume(&self, pv: PersistentVolume, claim_queue: &WorkQueue) -> Result<()> {
        // "Set correct "migrated-to" annotations and modify finalizers on PV and
        // update in API server if necessary" (`pv_controller.go:565-573`).
        let pv = self
            .update_volume_migration_annotations_and_finalizers(pv)
            .await?;
        // `volume.Spec.ClaimRef == nil` and `claimRef.UID == ""`: unused, or
        // reserved for a claim that has not yet bound (`:576-596`).
        let claim_ref = match pv.spec.claim_ref.clone() {
            Some(cr) if !cr.uid.as_deref().unwrap_or("").is_empty() => cr,
            _ => {
                self.update_volume_phase(pv, PersistentVolumePhase::Available)
                    .await?;
                return Ok(());
            }
        };

        // Get the PVC by name; a claim whose UID differs from the claimRef's
        // is a recreation of the one the PV was bound to, i.e. "treat the
        // volume as bound to a missing claim" (`:633-657`). The storage read is
        // authoritative, so upstream's informer-cache-then-apiserver double
        // check collapses to this one read.
        let claim = match (claim_ref.namespace.as_deref(), claim_ref.name.as_deref()) {
            (Some(ns), Some(name)) => {
                let key = build_key("persistentvolumeclaims", Some(ns), name);
                match self.storage.get::<PersistentVolumeClaim>(&key).await {
                    Ok(c) => Some(c),
                    Err(rusternetes_common::Error::NotFound(_)) => None,
                    Err(e) => return Err(e.into()),
                }
            }
            _ => None,
        };
        let claim = claim.filter(|c| Some(c.metadata.uid.as_str()) == claim_ref.uid.as_deref());

        let Some(claim) = claim else {
            // The claim must have been deleted; reclaimVolume may release the
            // PV back into the pool, recycle it or do nothing (retain).
            let pv = self.release_if_needed(pv).await?;
            return self.reclaim_volume(&pv).await;
        };

        let claim_volume_name = claim.spec.volume_name.as_deref().unwrap_or("");
        if claim_volume_name.is_empty() {
            if volume_mode_mismatches(&claim.spec, &pv.spec) {
                // Binding won't be called in syncUnboundClaim, because
                // findBestMatchForClaim won't return the volume due to
                // volumeMode mismatch (`:684-692`).
                let volume_msg = format!(
                    "Cannot bind PersistentVolume to requested PersistentVolumeClaim \"{}\" due to incompatible volumeMode.",
                    claim.metadata.name
                );
                self.warn(object_ref_for_pv(&pv), "VolumeMismatch", &volume_msg)
                    .await;
                let claim_msg = format!(
                    "Cannot bind PersistentVolume \"{}\" to requested PersistentVolumeClaim due to incompatible volumeMode.",
                    pv.metadata.name
                );
                self.warn(object_ref_for_pvc(&claim), "VolumeMismatch", &claim_msg)
                    .await;
                // Skipping syncClaim.
                return Ok(());
            }
            // The volume is Bound and the claim is Pending (either the binding
            // is not completed, or it was unbound by the user). Enqueue the
            // claim so syncClaim fixes it shortly (`:694-706`).
            claim_queue
                .add(format!(
                    "persistentvolumeclaims/{}/{}",
                    claim.metadata.namespace.as_deref().unwrap_or(""),
                    claim.metadata.name
                ))
                .await;
            return Ok(());
        }

        if claim_volume_name == pv.metadata.name {
            // Volume is bound to a claim properly, update status if necessary.
            self.update_volume_phase(pv, PersistentVolumePhase::Bound)
                .await?;
            return Ok(());
        }

        // Volume is bound to a claim, but the claim is bound elsewhere.
        let dynamically_provisioned = pv
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_DYNAMICALLY_PROVISIONED));
        if dynamically_provisioned
            && pv.spec.persistent_volume_reclaim_policy
                == Some(PersistentVolumeReclaimPolicy::Delete)
        {
            // Dynamically provisioned for this claim, which got bound
            // elsewhere: this volume is not needed. Mark it Released for
            // external deleters (don't overwrite Failed) and delete it.
            let pv = self.release_if_needed(pv).await?;
            return self.reclaim_volume(&pv).await;
        }
        // Bound by the controller: it tried to use this volume for a claim
        // that was fulfilled by another volume, so roll back. Bound by the
        // user: only clear the binding UID and leave the pre-bind.
        self.unbind_volume(pv).await
    }

    /// `updateVolumePhase` (`pv_controller.go:912-937`): a no-op when the phase
    /// is already set, otherwise `UpdateStatus` with the phase set and
    /// `status.message` cleared. Plain `updateVolumePhase` records no event.
    async fn update_volume_phase(
        &self,
        pv: PersistentVolume,
        phase: PersistentVolumePhase,
    ) -> Result<PersistentVolume> {
        self.update_volume_phase_with_message(pv, phase, None).await
    }

    /// `updateVolumePhaseWithEvent` (`pv_controller.go:942-962`): set the
    /// phase and message, and emit a Warning event only when the phase
    /// actually changes ("not every time syncClaim is called").
    async fn update_volume_phase_with_event(
        &self,
        pv: PersistentVolume,
        phase: PersistentVolumePhase,
        reason: &str,
        message: &str,
    ) -> Result<PersistentVolume> {
        if pv.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(pv);
        }
        let new_pv = self
            .update_volume_phase_with_message(pv, phase, Some(message.to_string()))
            .await?;
        self.warn(object_ref_for_pv(&new_pv), reason, message).await;
        Ok(new_pv)
    }

    /// `updateVolumePhase` with an explicit `message` (`:912-937`).
    async fn update_volume_phase_with_message(
        &self,
        mut pv: PersistentVolume,
        phase: PersistentVolumePhase,
        message: Option<String>,
    ) -> Result<PersistentVolume> {
        if pv.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(pv);
        }
        let mut status = pv.status.take().unwrap_or_default();
        status.phase = phase.clone();
        status.message = message;
        pv.status = Some(status);
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        // Phase-only write -> status subresource (pv_controller.go:925),
        // conditional on the resourceVersion of the object we were handed
        // (`UpdateStatus(volumeClone)`; a lost race is a Conflict the caller
        // requeues) (#2160).
        let new_pv = self.storage.update_status_cas(&pv_key, &pv).await?;
        info!("Volume {} entered phase {:?}", new_pv.metadata.name, phase);
        Ok(new_pv)
    }

    /// `Released` unless already `Released` or `Failed`: "Do not overwrite
    /// previous Failed state - let the user see that something went wrong,
    /// while we still re-try to reclaim the volume" (`pv_controller.go:672-681`
    /// and the same guard at `:741-749`).
    async fn release_if_needed(&self, pv: PersistentVolume) -> Result<PersistentVolume> {
        match pv.status.as_ref().map(|s| &s.phase) {
            Some(PersistentVolumePhase::Released) | Some(PersistentVolumePhase::Failed) => Ok(pv),
            _ => {
                info!(
                    "Volume {} is released and reclaim policy {:?} will be executed",
                    pv.metadata.name, pv.spec.persistent_volume_reclaim_policy
                );
                self.update_volume_phase(pv, PersistentVolumePhase::Released)
                    .await
            }
        }
    }

    /// `updateVolumeMigrationAnnotationsAndFinalizers`
    /// (`pv_controller_base.go:361-381`): one `Update` when either the
    /// `migrated-to` annotation or the deletion finalizers need fixing.
    async fn update_volume_migration_annotations_and_finalizers(
        &self,
        mut pv: PersistentVolume,
    ) -> Result<PersistentVolume> {
        let ann_modified = update_migration_annotations(pv.metadata.annotations.as_mut(), false);
        let (finalizers, finalizers_modified) = modify_deletion_finalizers(&pv);
        if !ann_modified && !finalizers_modified {
            return Ok(pv);
        }
        if finalizers_modified {
            pv.metadata.finalizers = finalizers;
        }
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        Ok(self.storage.update(&pv_key, &pv).await?)
    }

    /// `reclaimVolume` (`pv_controller.go:1180-1223`). `Retain` does nothing;
    /// `Recycle` runs `recycleVolumeOperation` (hostPath/NFS volumes are
    /// scrubbed by a recycler pod, [`super::pv_recycler`], in a detached
    /// operation, `:1192-1198`); `Delete` runs `deleteVolumeOperation`; an
    /// unknown policy fails the volume (`:1216-1221`). A PV carrying the
    /// `pv.kubernetes.io/migrated-to` annotation is left to the external
    /// provisioner (`:1182-1186`).
    ///
    /// Upstream schedules both operations on a goroutine keyed by
    /// `recycle-/delete-<name>[<uid>]` (`scheduleOperation`); recycling keeps
    /// that, because the recycler pod can run for minutes. The delete is
    /// inline.
    pub async fn reclaim_volume(&self, pv: &PersistentVolume) -> Result<()> {
        if pv
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANN_MIGRATED_TO))
            .is_some_and(|v| !v.is_empty())
        {
            return Ok(());
        }
        match pv.spec.persistent_volume_reclaim_policy {
            // An unset policy is defaulted to Retain by the API server.
            Some(PersistentVolumeReclaimPolicy::Retain) | None => {}
            Some(PersistentVolumeReclaimPolicy::Recycle) => {
                // `case v1.PersistentVolumeReclaimRecycle` (`:1192-1198`).
                let opname = format!("recycle-{}[{}]", pv.metadata.name, pv.metadata.uid);
                let handle = self.operation_handle();
                let pv = pv.clone();
                self.schedule_operation(opname, async move {
                    handle.recycle_volume_operation(pv).await;
                });
            }
            Some(PersistentVolumeReclaimPolicy::Delete) => {
                self.delete_volume_operation(pv).await?;
            }
            _ => {
                // `default:` branch (`pv_controller.go:1216-1221`).
                self.update_volume_phase_with_event(
                    pv.clone(),
                    PersistentVolumePhase::Failed,
                    "VolumeUnknownReclaimPolicy",
                    "Volume has unrecognized PersistentVolumeReclaimPolicy",
                )
                .await?;
            }
        }
        Ok(())
    }

    /// `isVolumeReleased` (`pv_controller.go:1395-1438`): the volume is bound
    /// to a claim that no longer exists, or exists with another UID, or is
    /// bound to a different volume.
    async fn is_volume_released(&self, pv: &PersistentVolume) -> Result<bool> {
        let Some(claim_ref) = pv.spec.claim_ref.as_ref() else {
            return Ok(false);
        };
        let claim_uid = claim_ref.uid.as_deref().unwrap_or("");
        if claim_uid.is_empty() {
            // Bound by the user; the controller has not finished binding.
            return Ok(false);
        }
        let claim = self.get_claim_by_ref(claim_ref).await?;
        if let Some(claim) = claim.filter(|c| c.metadata.uid == claim_uid) {
            return Ok(match claim.spec.volume_name.as_deref() {
                // the claim is bound to another PV, this PV *is* released
                Some(v) if !v.is_empty() && v != pv.metadata.name => true,
                _ => false,
            });
        }
        Ok(true)
    }

    /// `claims.GetByKey(claimrefToClaimKey(ref))` (`pv_controller.go:1410`).
    async fn get_claim_by_ref(
        &self,
        claim_ref: &ObjectReference,
    ) -> Result<Option<PersistentVolumeClaim>> {
        let (Some(ns), Some(name)) = (claim_ref.namespace.as_deref(), claim_ref.name.as_deref())
        else {
            return Ok(None);
        };
        let key = build_key("persistentvolumeclaims", Some(ns), name);
        match self.storage.get::<PersistentVolumeClaim>(&key).await {
            Ok(c) => Ok(Some(c)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// `isVolumeUsed` (`pv_controller.go:1457-1474`): the sorted
    /// `namespace/name` of every non-terminated pod using the claim the
    /// volume was bound to.
    async fn is_volume_used(&self, pv: &PersistentVolume) -> Result<Vec<String>> {
        let Some(claim_ref) = pv.spec.claim_ref.as_ref() else {
            return Ok(Vec::new());
        };
        let namespace = claim_ref.namespace.as_deref().unwrap_or("");
        let claim_name = claim_ref.name.as_deref().unwrap_or("");
        let pods: Vec<Pod> = self
            .storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await?;
        let mut names: Vec<String> = pods
            .iter()
            .filter(|p| pod_uses_claim(p, claim_name) && !is_pod_terminated(p))
            .map(|p| format!("{}/{}", namespace, p.metadata.name))
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// `deleteVolumeOperation` (`pv_controller.go:1323-1390`). Re-reads the
    /// volume, skips it unless still released (`:1342-1350`; the
    /// `HonorPVReclaimPolicy` branch at `:1336` is taken, so a volume with a
    /// deletion timestamp is still processed), then `doDeleteVolume`. A
    /// failing deleter marks the volume `Failed` with `Warning
    /// VolumeFailedDelete` (`:1354-1372`); the next sync retries.
    async fn delete_volume_operation(&self, volume: &PersistentVolume) -> Result<()> {
        let Some(new_volume) = self.get_volume(&volume.metadata.name).await? else {
            return Ok(());
        };
        if !self.is_volume_released(&new_volume).await? {
            debug!(
                "Volume {} no longer needs deletion, skipping",
                new_volume.metadata.name
            );
            return Ok(());
        }
        match find_deletable_plugin(volume) {
            // "External deleter is requested, do nothing" (`:1508-1512`).
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(msg) => {
                debug!(
                    "Deletion of volume {} failed: {}",
                    volume.metadata.name, msg
                );
                self.update_volume_phase_with_event(
                    volume.clone(),
                    PersistentVolumePhase::Failed,
                    "VolumeFailedDelete",
                    &msg,
                )
                .await?;
                return Ok(());
            }
        }
        // `plugin.NewDeleter(...).Delete()` (`doDeleteVolume`, `:1499-1531`):
        // the in-tree hostPath deleter wipes the directory first; a failure
        // marks the volume Failed with `VolumeFailedDelete` and keeps the PV
        // (`deleteVolumeOperation`, `:1354-1372`); the next sync retries.
        if let Some(hp) = volume.spec.host_path.as_ref() {
            if let Err(msg) = host_path_delete(&hp.path).await {
                self.update_volume_phase_with_event(
                    volume.clone(),
                    PersistentVolumePhase::Failed,
                    "VolumeFailedDelete",
                    &msg,
                )
                .await?;
                return Ok(());
            }
        }
        // "Remove in-tree delete finalizer on the PV as the volume has been
        // deleted from the underlying storage" (`:1532-1538`).
        self.remove_deletion_protection_finalizer(&volume.metadata.name)
            .await?;
        let pv_key = build_key("persistentvolumes", None, &volume.metadata.name);
        match self.storage.delete(&pv_key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        info!(
            "Reclaim(Delete): deleted released PV {}",
            volume.metadata.name
        );
        Ok(())
    }

    /// `removeDeletionProtectionFinalizer` (`pv_controller.go:1542-1572`).
    async fn remove_deletion_protection_finalizer(&self, name: &str) -> Result<()> {
        let Some(mut pv) = self.get_volume(name).await? else {
            return Ok(());
        };
        let Some(finalizers) = pv.metadata.finalizers.as_mut() else {
            return Ok(());
        };
        if !finalizers
            .iter()
            .any(|f| f == IN_TREE_PV_DELETION_PROTECTION_FINALIZER)
        {
            return Ok(());
        }
        finalizers.retain(|f| f != IN_TREE_PV_DELETION_PROTECTION_FINALIZER);
        if finalizers.is_empty() {
            pv.metadata.finalizers = None;
        }
        let pv_key = build_key("persistentvolumes", None, name);
        self.storage.update(&pv_key, &pv).await?;
        Ok(())
    }

    /// `recycleVolumeOperation` (`pv_controller.go:1227-1321`), the part after
    /// the `isVolumeReleased`/`isVolumeUsed` guards: find the recycler
    /// plugin, run it, then either mark the volume Failed or make it
    /// Available again via `unbindVolume`. Runs detached, see
    /// [`Self::schedule_operation`].
    pub async fn recycle_volume_operation(&self, volume: PersistentVolume) {
        debug!(
            "RecycleVolumeOperation started for {}",
            volume.metadata.name
        );

        // "This method may have been waiting for a volume lock for some time.
        // Previous recycleVolumeOperation might just have saved an updated
        // version, so read current volume state now."
        let pv_key = build_key("persistentvolumes", None, &volume.metadata.name);
        let volume = match self.storage.get::<PersistentVolume>(&pv_key).await {
            Ok(v) => v,
            Err(e) => {
                debug!(
                    "Error reading persistent volume {}: {}",
                    volume.metadata.name, e
                );
                return;
            }
        };

        // The guards before the plugin lookup (`:1240-1263`). A read error
        // leaves the volume untouched; the next sync retries.
        match self.is_volume_released(&volume).await {
            Ok(true) => {}
            Ok(false) => {
                debug!(
                    "Volume {} no longer needs recycling, skipping",
                    volume.metadata.name
                );
                return;
            }
            Err(e) => {
                debug!("RecycleVolumeOperation: isVolumeReleased failed: {}", e);
                return;
            }
        }
        let pods = match self.is_volume_used(&volume).await {
            Ok(p) => p,
            Err(e) => {
                debug!("RecycleVolumeOperation: isVolumeUsed failed: {}", e);
                return;
            }
        };
        // "Verify the claim is in cache: if so, then it is a different PVC with
        // the same name since the volume is known to be released at this
        // moment ... So the old PV is safe to be recycled." (`:1254-1263`)
        let claim_cached = match volume.spec.claim_ref.as_ref() {
            Some(cr) => match self.get_claim_by_ref(cr).await {
                Ok(c) => c.is_some(),
                Err(e) => {
                    debug!("RecycleVolumeOperation: reading claim failed: {}", e);
                    return;
                }
            },
            None => false,
        };
        if !pods.is_empty() && !claim_cached {
            let msg = format!("Volume is used by pods: {}", pods.join(","));
            debug!("Can't recycle volume {}: {}", volume.metadata.name, msg);
            self.event(object_ref_for_pv(&volume), "VolumeFailedRecycle", &msg)
                .await;
            return;
        }

        // Find a plugin.
        let Some(pod) = super::pv_recycler::recycler_pod_for_volume(&volume, &self.recycler_config)
        else {
            // No recycler found. Emit an event and mark the volume Failed.
            if let Err(e) = self
                .update_volume_phase_with_event(
                    volume.clone(),
                    PersistentVolumePhase::Failed,
                    "VolumeFailedRecycle",
                    "No recycler plugin found for the volume!",
                )
                .await
            {
                debug!(
                    "RecycleVolumeOperation: failed to mark volume {} as failed: {}",
                    volume.metadata.name, e
                );
            }
            // Despite the volume being Failed, the controller will retry
            // recycling the volume in every syncVolume() call.
            return;
        };

        // Plugin found: `plugin.Recycle(volume.Name, spec, recorder)`.
        let recycled = match pod {
            Ok(pod) => {
                // `newRecyclerEventRecorder` (`:1916-1922`).
                let recorder = self.recorder.clone();
                let involved = object_ref_for_pv(&volume);
                let handle = tokio::runtime::Handle::current();
                let client = super::pv_recycler::StorageRecyclerClient::new(
                    Arc::clone(&self.storage),
                    Box::new(move |event_type, message| {
                        let recorder = recorder.clone();
                        let involved = involved.clone();
                        let event_type = if event_type == "Warning" {
                            EventType::Warning
                        } else {
                            EventType::Normal
                        };
                        let message = format!("Recycler pod: {message}");
                        handle.spawn(async move {
                            let source = EventSource {
                                component: "persistentvolume-controller".to_string(),
                                host: None,
                            };
                            if let Err(e) = recorder
                                .event(&involved, &source, event_type, "RecyclerPod", &message)
                                .await
                            {
                                tracing::warn!("Failed to record RecyclerPod event: {}", e);
                            }
                        });
                    }),
                );
                super::pv_recycler::recycle_volume_by_watching_pod_until_completion(
                    &volume.metadata.name,
                    pod,
                    &client,
                )
                .await
            }
            Err(e) => Err(e),
        };

        if let Err(e) = recycled {
            // Recycler failed.
            let strerr = format!("Recycle failed: {e}");
            if let Err(e) = self
                .update_volume_phase_with_event(
                    volume.clone(),
                    PersistentVolumePhase::Failed,
                    "VolumeFailedRecycle",
                    &strerr,
                )
                .await
            {
                debug!(
                    "RecycleVolumeOperation: failed to mark volume {} as failed: {}",
                    volume.metadata.name, e
                );
            }
            // Despite the volume being Failed, the controller will retry
            // recycling the volume in every syncVolume() call.
            return;
        }

        info!("Volume {} recycled", volume.metadata.name);
        self.event(
            object_ref_for_pv(&volume),
            "VolumeRecycled",
            "Volume recycled",
        )
        .await;
        // Make the volume available again.
        if let Err(e) = self.unbind_volume(volume.clone()).await {
            // "Oops, could not save the volume and therefore the controller
            // will recycle the volume again on next update."
            debug!(
                "RecycleVolumeOperation: failed to make recycled volume {} 'Available', we will recycle the volume again: {}",
                volume.metadata.name, e
            );
        }
    }

    /// `unbindVolume` (`pv_controller.go:1140-1178`): roll back a binding. A
    /// volume bound by the controller loses its `claimRef` and the
    /// bound-by-controller annotation; one pre-bound by the user only loses the
    /// claimRef UID. The spec goes through `Update`, then the phase returns to
    /// `Available` via `updateVolumePhase`.
    async fn unbind_volume(&self, mut pv: PersistentVolume) -> Result<()> {
        let bound_by_controller = pv
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BOUND_BY_CONTROLLER));
        if bound_by_controller {
            pv.spec.claim_ref = None;
            if let Some(a) = pv.metadata.annotations.as_mut() {
                a.remove(ANN_BOUND_BY_CONTROLLER);
                if a.is_empty() {
                    // "No annotations look better than empty annotation map".
                    pv.metadata.annotations = None;
                }
            }
        } else if let Some(cr) = pv.spec.claim_ref.as_mut() {
            cr.uid = None;
        }
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        let new_pv = self.storage.update(&pv_key, &pv).await?;
        self.update_volume_phase(new_pv, PersistentVolumePhase::Available)
            .await?;
        Ok(())
    }

    /// `ctrl.eventRecorder.Event(obj, v1.EventTypeWarning, reason, msg)`.
    /// Recording is fire-and-forget upstream; a failure is only logged.
    async fn warn(&self, involved: ObjectReference, reason: &str, message: &str) {
        let source = EventSource {
            component: "persistentvolume-controller".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Warning, reason, message)
            .await
        {
            tracing::warn!("Failed to record {} event: {}", reason, e);
        }
    }

    /// `eventRecorder.Event(obj, v1.EventTypeNormal, reason, message)`.
    async fn event(&self, involved: ObjectReference, reason: &str, message: &str) {
        let source = EventSource {
            component: "persistentvolume-controller".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Normal, reason, message)
            .await
        {
            tracing::warn!("Failed to record {} event: {}", reason, e);
        }
    }

    /// Port of upstream `syncClaim` dispatch (`pv_controller.go:251-255`): a
    /// claim without `pv.kubernetes.io/bind-completed` goes to
    /// `syncUnboundClaim`, one with it to `syncBoundClaim`.
    ///
    /// Not ported here (tracked separately): the `provisionClaim` call in the
    /// `volumeName == ""` branch (done by the dynamic-provisioner controller)
    /// and `unbindClaim`.
    pub async fn bind_pvc(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        // "Set correct "migrated-to" annotations on PVC and update in API
        // server if necessary" (`syncClaim`, pv_controller.go:240-249).
        self.update_claim_migration_annotations(pvc).await?;
        let bind_completed = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BIND_COMPLETED));
        if bind_completed {
            return self.sync_bound_claim(pvc).await;
        }
        // `claim.Spec.VolumeName != ""` (`:411`): the user asked for a
        // specific PV.
        if pvc
            .spec
            .volume_name
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        {
            return self.sync_prebound_claim(pvc).await;
        }
        self.sync_unbound_claim(pvc).await
    }

    /// `updateClaimMigrationAnnotations` (`pv_controller_base.go:336-359`):
    /// when the claim's provisioner annotation says it is (no longer)
    /// CSI-migrated, anneal `pv.kubernetes.io/migrated-to` and `Update` the
    /// claim.
    async fn update_claim_migration_annotations(
        &self,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        let mut annotated = pvc.clone();
        if !update_migration_annotations(annotated.metadata.annotations.as_mut(), true) {
            return Ok(());
        }
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        let updated = self.storage.update(&key, &annotated).await.map_err(|e| {
            anyhow::anyhow!("persistent Volume Controller can't anneal migration annotations: {e}")
        })?;
        *pvc = updated;
        Ok(())
    }

    /// `PersistentVolumeController.syncBoundClaim`
    /// (`pv_controller.go:492-560`; binder_test.go "3-1".."3-7").
    async fn sync_bound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let volume_name = pvc.spec.volume_name.clone().unwrap_or_default();
        if volume_name.is_empty() {
            // Claim was bound before but not any more.
            return self
                .update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimLost",
                    "Bound claim has lost reference to PersistentVolume. Data on the volume is lost!",
                )
                .await;
        }
        let Some(pv) = self.get_volume(&volume_name).await? else {
            // Claim is bound to a non-existing volume.
            return self
                .update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimLost",
                    "Bound claim has lost its PersistentVolume. Data on the volume is lost!",
                )
                .await;
        };
        match pv.spec.claim_ref.as_ref() {
            // Claim is bound but volume has come unbound (or the controller
            // has not seen the updated volume yet; the two cannot be told
            // apart): bind the volume again and set all states to Bound.
            None => self.bind(pv, pvc).await,
            // All is well; `bind` does nothing when everything is set.
            Some(cr) if cr.uid.as_deref() == Some(pvc.metadata.uid.as_str()) => {
                self.bind(pv, pvc).await
            }
            // Claim is bound but volume has a different claimant: `Lost` is a
            // terminal phase.
            Some(_) => {
                self.update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimMisbound",
                    "Two claims are bound to the same volume, this one is bound incorrectly",
                )
                .await
            }
        }
    }

    /// The `claim.Spec.VolumeName != ""` branch of `syncUnboundClaim`
    /// (`pv_controller.go:411-490`; binder_test.go "2-1".."2-10"): the user
    /// asked for a specific PV.
    async fn sync_prebound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let volume_name = pvc.spec.volume_name.clone().unwrap_or_default();
        let Some(pv) = self.get_volume(&volume_name).await? else {
            // User asked for a PV that does not exist; retry later.
            return self
                .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                .await;
        };
        if pv.spec.claim_ref.is_none() {
            // User asked for a PV that is not claimed.
            if let Err(e) = self.check_volume_satisfy_claim(&pv, pvc) {
                let msg = format!(
                    "Cannot bind to requested volume {:?}: {}",
                    pv.metadata.name, e
                );
                self.warn(object_ref_for_pvc(pvc), "VolumeMismatch", &msg)
                    .await;
                return self
                    .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                    .await;
            }
            return self.bind(pv, pvc).await;
        }
        if is_volume_bound_to_claim(&pv, pvc) {
            // User asked for a PV that is claimed by this PVC: finish the
            // binding by adding the claim UID.
            return self.bind(pv, pvc).await;
        }
        // User asked for a PV that is claimed by someone else.
        let claim_msg = format!(
            "volume {:?} already bound to a different claim.",
            pv.metadata.name
        );
        self.warn(object_ref_for_pvc(pvc), "FailedBinding", &claim_msg)
            .await;
        let bound_by_controller = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BOUND_BY_CONTROLLER));
        if !bound_by_controller {
            // User asked for a specific PV, retry later.
            return self
                .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                .await;
        }
        // "This should never happen because someone had to remove
        // AnnBindCompleted annotation on the claim."
        let other = pv.spec.claim_ref.as_ref();
        anyhow::bail!(
            "invalid binding of claim \"{}/{}\" to volume \"{}\": volume already claimed by \"{}/{}\"",
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name,
            volume_name,
            other.and_then(|c| c.namespace.as_deref()).unwrap_or(""),
            other.and_then(|c| c.name.as_deref()).unwrap_or(""),
        )
    }

    /// `PersistentVolumeController.bind` (`pv_controller.go:1000-1035`) as far
    /// as this controller models it: a no-op when volume and claim already
    /// agree (upstream's `bindVolumeToClaim`/`bindClaimToVolume` each write
    /// only when dirty), else [`Self::complete_binding`].
    async fn bind(&self, pv: PersistentVolume, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let bound = pv.status.as_ref().map(|s| &s.phase) == Some(&PersistentVolumePhase::Bound)
            && pv
                .spec
                .claim_ref
                .as_ref()
                .is_some_and(|cr| cr.uid.as_deref() == Some(pvc.metadata.uid.as_str()))
            && is_volume_bound_to_claim(&pv, pvc)
            && pvc.spec.volume_name.as_deref() == Some(pv.metadata.name.as_str())
            && pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(ANN_BIND_COMPLETED))
            && pvc.status.as_ref().is_some_and(|s| {
                s.phase == PersistentVolumeClaimPhase::Bound && s.capacity.is_some()
            });
        if bound {
            return Ok(());
        }
        self.complete_binding(pv, pvc).await
    }

    async fn get_volume(&self, name: &str) -> Result<Option<PersistentVolume>> {
        match self
            .storage
            .get::<PersistentVolume>(&build_key("persistentvolumes", None, name))
            .await
        {
            Ok(pv) => Ok(Some(pv)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// `checkVolumeSatisfyClaim` (`pv_controller.go:262-301`) for a volume the
    /// user named explicitly. Error strings are upstream's verbatim.
    fn check_volume_satisfy_claim(
        &self,
        pv: &PersistentVolume,
        pvc: &PersistentVolumeClaim,
    ) -> std::result::Result<(), String> {
        if pv.metadata.deletion_timestamp.is_some() {
            return Err(format!(
                "the volume is marked for deletion {:?}",
                pv.metadata.name
            ));
        }
        if let Some(requested) = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
        {
            let sufficient = pv
                .spec
                .capacity
                .get("storage")
                .is_some_and(|have| self.storage_sufficient(have, requested));
            if !sufficient {
                return Err("requested PV is too small".to_string());
            }
        }
        if class_of(
            pvc.spec.storage_class_name.as_deref(),
            pvc.metadata.annotations.as_ref(),
        ) != class_of(
            pv.spec.storage_class_name.as_deref(),
            pv.metadata.annotations.as_ref(),
        ) {
            return Err("storageClassName does not match".to_string());
        }
        if pvc
            .spec
            .volume_attributes_class_name
            .as_deref()
            .unwrap_or("")
            != pv
                .spec
                .volume_attributes_class_name
                .as_deref()
                .unwrap_or("")
        {
            return Err("volumeAttributesClassName does not match".to_string());
        }
        if volume_mode_mismatches(&pvc.spec, &pv.spec) {
            return Err("incompatible volumeMode".to_string());
        }
        if !pvc
            .spec
            .access_modes_slice()
            .iter()
            .all(|m| pv.spec.access_modes.contains(m))
        {
            return Err("incompatible accessMode".to_string());
        }
        Ok(())
    }

    /// `updateClaimStatus` (`pv_controller.go:784-858`). With `volume ==
    /// None` (`:796-806`) the phase is set and accessModes / capacity /
    /// currentVolumeAttributesClassName are reset; with a volume (`:823-858`)
    /// accessModes follow the volume, capacity is copied only on a phase
    /// transition (honouring the pre-resize-capacity annotation) and
    /// currentVolumeAttributesClassName only on Pending -> Bound. A no-op when
    /// nothing changes; otherwise written through the status subresource.
    pub async fn update_claim_status(
        &self,
        pvc: &mut PersistentVolumeClaim,
        phase: PersistentVolumeClaimPhase,
        volume: Option<&PersistentVolume>,
    ) -> Result<()> {
        let old_phase = pvc.status.as_ref().map(|s| s.phase.clone());
        let mut status = pvc.status.clone().unwrap_or_default();
        let mut dirty = pvc.status.is_none() || status.phase != phase;
        status.phase = phase.clone();
        match volume {
            None => {
                dirty |= status.access_modes.take().is_some();
                dirty |= status.capacity.take().is_some();
                dirty |= status.current_volume_attributes_class_name.take().is_some();
            }
            Some(volume) => {
                if status.access_modes.as_ref() != Some(&volume.spec.access_modes) {
                    status.access_modes = Some(volume.spec.access_modes.clone());
                    dirty = true;
                }
                // "Update Capacity if the claim is becoming Bound, not if it
                // was already. A discrepancy can be intentional to mean that
                // the PVC filesystem size doesn't match the PV block device
                // size, so don't clobber it" (`:817-819`).
                if old_phase.as_ref() != Some(&phase) {
                    let Some(volume_cap) = volume.spec.capacity.get("storage") else {
                        anyhow::bail!(
                            "PersistentVolume {:?} is without a storage capacity",
                            volume.metadata.name
                        );
                    };
                    let pre_resize = volume
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get(ANN_PRE_RESIZE_CAPACITY));
                    if let Some(pre) = pre_resize {
                        // `resource.ParseQuantity` failing falls back to the
                        // volume's capacity (`:830-833`).
                        let qty = if parse_quantity_nano(pre).is_some() {
                            pre.clone()
                        } else {
                            volume_cap.clone()
                        };
                        status
                            .capacity
                            .get_or_insert_with(Default::default)
                            .insert("storage".to_string(), qty);
                        dirty = true;
                    } else if status
                        .capacity
                        .as_ref()
                        .and_then(|c| c.get("storage"))
                        .is_none_or(|c| !quantity_eq(volume_cap, c))
                    {
                        status.capacity = Some(volume.spec.capacity.clone());
                        dirty = true;
                    }
                }
                // `:841-857`: only during binding, never afterwards.
                if old_phase == Some(PersistentVolumeClaimPhase::Pending)
                    && phase == PersistentVolumeClaimPhase::Bound
                    && status.current_volume_attributes_class_name
                        != volume.spec.volume_attributes_class_name
                {
                    status.current_volume_attributes_class_name =
                        volume.spec.volume_attributes_class_name.clone();
                    dirty = true;
                }
            }
        }
        if !dirty {
            return Ok(());
        }
        pvc.status = Some(status);
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        // `UpdateStatus(claimClone)` (pv_controller.go:866): conditional on the
        // claim's resourceVersion, Conflict -> requeue (#2160).
        let new_pvc = self.storage.update_status_cas(&key, &*pvc).await?;
        *pvc = new_pvc;
        Ok(())
    }

    /// `updateClaimStatusWithEvent` (`pv_controller.go:889-909`): nothing when
    /// the phase is already set; otherwise the status write, then the event, so
    /// it is emitted on a change and not on every sync.
    async fn update_claim_status_with_event(
        &self,
        pvc: &mut PersistentVolumeClaim,
        phase: PersistentVolumeClaimPhase,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        if pvc.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(());
        }
        self.update_claim_status(pvc, phase, None).await?;
        self.warn(object_ref_for_pvc(pvc), reason, message).await;
        Ok(())
    }

    /// The `claim.Spec.VolumeName == ""` branch of `syncUnboundClaim`
    /// (`pv_controller.go:331-409`), first-match binding only.
    async fn sync_unbound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        let pvc_spec = &pvc.spec;

        debug!("Looking for PV to bind to PVC {}/{}", namespace, pvc_name);
        debug!(
            "PVC requirements: storage_class={:?}, capacity={:?}, access_modes={:?}",
            pvc_spec.storage_class_name,
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
            pvc_spec.access_modes
        );

        // Get all available PVs
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;

        debug!("Found {} PVs to check for binding", pvs.len());

        // Lazily-loaded node inventory, only fetched when a candidate PV
        // actually declares node affinity (upstream `volume_scheduling`).
        let mut nodes_cache: Option<Vec<Node>> = None;

        // Phase 1 (#1095): honor a pre-bound PV. A dynamically provisioned PV
        // carries a `claimRef` naming the exact PVC it was provisioned for, so
        // it must bind to THAT PVC and no other — otherwise two PVCs provisioned
        // at the same time cross-bind to each other's PV. A PV whose claimRef
        // points to this PVC was provisioned for us; one pointing elsewhere is
        // unavailable. The pre-bound PV already satisfies the request (it was
        // sized for it), so it wins over any unclaimed first-match candidate.
        if let Some(pv) = pvs.iter().find(|pv| {
            pv.spec
                .claim_ref
                .as_ref()
                .is_some_and(|cr| claim_ref_points_to(cr, namespace, pvc_name, &pvc.metadata.uid))
        }) {
            info!(
                "Completing pre-bound PVC {}/{} ↔ PV {} (claimRef)",
                namespace, pvc_name, pv.metadata.name
            );
            return self.complete_binding(pv.clone(), pvc).await;
        }

        // `FindMatchingVolume` (component-helpers `pv_helpers.go:279-283`,
        // called from `findByClaim` `index.go:97` with node == nil):
        // `if node == nil && delayBinding { continue }` — after the
        // IsVolumeBoundToClaim return (phase 1 above), the PV controller does
        // not bind unclaimed PVs for a delay-binding class; the scheduler's
        // VolumeBinding plugin (BindPodVolumes) binds them with a node chosen.
        // `delayBinding` is computed once up front exactly as
        // `syncUnboundClaim` does (`pv_controller.go:337`), errors propagate.
        let delay_binding = self
            .is_delay_binding_mode(class_of(
                pvc.spec.storage_class_name.as_deref(),
                pvc.metadata.annotations.as_ref(),
            ))
            .await?;

        // Phase 2: first-match among genuinely unclaimed PVs (static volumes /
        // legacy PVs with no claimRef).
        for pv in pvs {
            if delay_binding {
                continue;
            }
            debug!("Checking PV {} (storage_class={:?}, capacity={:?}, access_modes={:?}, claim_ref={:?})",
                pv.metadata.name,
                pv.spec.storage_class_name,
                pv.spec.capacity,
                pv.spec.access_modes,
                pv.spec.claim_ref.is_some());

            // Skip if PV is already bound/pre-bound (a pre-bind to this PVC was
            // handled in phase 1; anything else belongs to another claim).
            if pv.spec.claim_ref.is_some() {
                continue;
            }

            // Check if PV matches PVC requirements
            let matches = self.pv_matches_pvc(&pv.spec, pvc_spec);
            debug!(
                "PV {} matches PVC requirements: {}",
                pv.metadata.name, matches
            );
            if !matches {
                continue;
            }

            // Honor the PV's required node affinity (upstream parity with
            // `pkg/controller/volume/scheduling`): a PV constrained to a node
            // topology must not bind unless at least one node in the cluster
            // satisfies that constraint. Otherwise the bound volume could never
            // be mounted by any pod.
            if let Some(node_affinity) = pv.spec.node_affinity.as_ref() {
                if node_affinity.required.is_some() {
                    let nodes = match nodes_cache.as_ref() {
                        Some(n) => n,
                        None => {
                            let listed: Vec<Node> = self.storage.list("/registry/nodes/").await?;
                            nodes_cache = Some(listed);
                            nodes_cache.as_ref().unwrap()
                        }
                    };
                    if !node_affinity_satisfied(node_affinity, nodes) {
                        debug!(
                            "Skipping PV {}: required nodeAffinity matches no node",
                            pv.metadata.name
                        );
                        continue;
                    }
                }
            }

            info!(
                "Binding PVC {}/{} to PV {}",
                namespace, pvc_name, pv.metadata.name
            );
            return self.complete_binding(pv, pvc).await;
        }

        debug!("No matching PV found for PVC {}/{}", namespace, pvc_name);
        self.sync_unbound_claim_without_volume(pvc).await
    }

    /// The `volume == nil` arm of `syncUnboundClaim`
    /// (`pv_controller.go:345-390`): try `assignDefaultStorageClass`, then
    /// either explain why a delay-binding claim waits, hand a classed claim to
    /// provisioning, or report that nothing can serve a classless claim.
    ///
    /// Deviation: `provisionClaim` (`:372`) is not called here; provisioning is
    /// done by the separate dynamic-provisioner controller, so a claim with a
    /// class simply returns (as upstream does after `provisionClaim`).
    async fn sync_unbound_claim_without_volume(
        &self,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        if self.assign_default_storage_class(pvc).await? {
            // "PersistentVolumeClaim update successful, restarting claim sync"
            return Ok(());
        }
        let class = class_of(
            pvc.spec.storage_class_name.as_deref(),
            pvc.metadata.annotations.as_ref(),
        )
        .to_string();
        let selected_node = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_SELECTED_NODE));
        if self.is_delay_binding_mode(&class).await? && !selected_node {
            // Scheduler does not observe any pod using this claim.
            self.emit_event_for_unbound_delay_binding_claim(pvc).await?;
        } else if !class.is_empty() {
            return Ok(());
        } else {
            self.event(
                object_ref_for_pvc(pvc),
                "FailedBinding",
                "no persistent volumes available for this claim and no storage class is set",
            )
            .await;
        }
        // Mark the claim as Pending and try to find a match in the next
        // periodic syncClaim.
        self.update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
            .await
    }

    /// `storagehelpers.IsDelayBindingMode` (component-helpers
    /// `pv_helpers.go:96-115`): no class or a missing class is not delay
    /// binding; a class without `volumeBindingMode` is an error.
    async fn is_delay_binding_mode(&self, class_name: &str) -> Result<bool> {
        if class_name.is_empty() {
            return Ok(false);
        }
        let class: StorageClass = match self
            .storage
            .get(&build_key("storageclasses", None, class_name))
            .await
        {
            Ok(c) => c,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        match class.volume_binding_mode {
            None => anyhow::bail!(
                "VolumeBindingMode not set for StorageClass {:?}",
                class_name
            ),
            Some(m) => Ok(m == VolumeBindingMode::WaitForFirstConsumer),
        }
    }

    /// `assignDefaultStorageClass` (`pv_controller.go:967-994`): a claim that
    /// asks for no class gets the default one, written to the API server.
    /// Returns whether the claim was updated.
    async fn assign_default_storage_class(&self, pvc: &mut PersistentVolumeClaim) -> Result<bool> {
        // `PersistentVolumeClaimHasClass` (component-helpers helpers.go:28-39).
        let has_class = pvc.spec.storage_class_name.is_some()
            || pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(ANN_BETA_STORAGE_CLASS));
        if has_class {
            return Ok(false);
        }
        let classes: Vec<StorageClass> = self.storage.list("/registry/storageclasses/").await?;
        let Some(class) = get_default_class(classes) else {
            return Ok(false);
        };
        pvc.spec.storage_class_name = Some(class.metadata.name);
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        self.storage.update(&key, &*pvc).await?;
        Ok(true)
    }

    /// `emitEventForUnboundDelayBindingClaim` (`pv_controller.go:306-326`).
    async fn emit_event_for_unbound_delay_binding_claim(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<()> {
        let mut reason = "WaitForFirstConsumer";
        let mut message = "waiting for first consumer to be created before binding".to_string();
        let pod_names = self.find_non_scheduled_pods_by_pvc(pvc).await?;
        if !pod_names.is_empty() {
            reason = "WaitForPodScheduled";
            message = if pod_names.len() > 1 {
                format!("waiting for pods {} to be scheduled", pod_names.join(","))
            } else {
                format!("waiting for pod {} to be scheduled", pod_names[0])
            };
        }
        self.event(object_ref_for_pvc(pvc), reason, &message).await;
        Ok(())
    }

    /// `findNonScheduledPodsByPVC` (`pv_controller.go:1477-1493`) over
    /// `PodPVCIndexFunc` (`pkg/controller/volume/common/common.go:35-55`).
    async fn find_non_scheduled_pods_by_pvc(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<Vec<String>> {
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("");
        let pods: Vec<Pod> = self
            .storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await?;
        let mut names = Vec::new();
        for pod in pods {
            let Some(spec) = pod.spec.as_ref() else {
                continue;
            };
            let uses_claim = spec.volumes.iter().flatten().any(|v| {
                if let Some(src) = v.persistent_volume_claim.as_ref() {
                    src.claim_name == pvc.metadata.name
                } else if v.ephemeral.is_some() {
                    // `ephemeral.VolumeClaimName`: `<pod>-<volume>`.
                    format!("{}-{}", pod.metadata.name, v.name) == pvc.metadata.name
                } else {
                    false
                }
            });
            if !uses_claim || is_pod_terminated(&pod) {
                continue;
            }
            if spec.node_name.as_deref().unwrap_or("").is_empty() {
                names.push(pod.metadata.name.clone());
            }
        }
        Ok(names)
    }

    /// Complete a PVC↔PV binding: pin the PV's `claimRef` to this PVC, mark both
    /// Bound, and persist them. Shared by the pre-bound (claimRef) and the
    /// first-match (unclaimed) paths in [`Self::bind_pvc`].
    async fn complete_binding(
        &self,
        mut pv: PersistentVolume,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        let namespace = pvc.metadata.namespace.clone().unwrap_or("default".into());
        let pvc_name = pvc.metadata.name.clone();

        let pv_name = pv.metadata.name.clone();

        // GetBindVolumeToClaim (component-helpers pv_helpers.go:120-154): a PV
        // not already bound/pre-bound to this claim is bound BY the controller,
        // recorded in `pv.kubernetes.io/bound-by-controller` so that
        // `unbindVolume` knows it may clear the whole claimRef.
        if !is_volume_bound_to_claim(&pv, pvc) {
            pv.metadata
                .annotations
                .get_or_insert_with(Default::default)
                .entry(ANN_BOUND_BY_CONTROLLER.to_string())
                .or_insert_with(|| "yes".to_string());
        }
        // Pin the PV to this PVC (idempotent for an already pre-bound PV).
        pv.spec.claim_ref = Some(
            rusternetes_common::resources::service_account::ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some(namespace.clone()),
                name: Some(pvc_name.clone()),
                uid: Some(pvc.metadata.uid.clone()),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            },
        );
        pv.status = Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        });
        let pv_key = build_key("persistentvolumes", None, &pv_name);
        // Upstream binds in two writes, and so must we: the spec half
        // (`claimRef`) goes through the main resource
        // (`PersistentVolumes().Update`, pv_controller.go:1019) and the phase
        // through the status subresource (`UpdateStatus`, pv_controller.go:925).
        // A single full-object PUT would have its `.status` stripped by a
        // conformant api-server, leaving the PV bound but never Bound (#1723).
        // The status write is built on the object `Update` returned
        // (`bind`: `volume = updatedVolume` then `updateVolumePhase`, :1110), so it carries
        // that resourceVersion and a concurrent writer is a Conflict (#2160).
        let mut pv = self.storage.update(&pv_key, &pv).await?;
        pv.status = Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        });
        self.storage.update_status_cas(&pv_key, &pv).await?;

        bind_claim_to_volume(pvc, &pv_name);
        let pvc_key = build_key("persistentvolumeclaims", Some(&namespace), &pvc_name);
        // Same two-write split for the claim: `spec.volumeName` via the main
        // resource (`bindClaimToVolume`), then phase/capacity/VAC via the
        // status subresource (`updateClaimStatus(claim, ClaimBound, volume)`,
        // `bind` at pv_controller.go:1122).
        // `updateClaimStatus(ctx, claim=updatedClaim, ...)` (`bind`, :1122): the claim
        // `Update` returned carries the fresh resourceVersion.
        *pvc = self.storage.update(&pvc_key, pvc).await?;
        self.update_claim_status(pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await?;

        info!(
            "Successfully bound PVC {}/{} to PV {}",
            namespace, pvc_name, pv_name
        );
        Ok(())
    }

    /// Check if a PV matches the requirements of a PVC
    fn pv_matches_pvc(
        &self,
        pv_spec: &rusternetes_common::resources::PersistentVolumeSpec,
        pvc_spec: &rusternetes_common::resources::PersistentVolumeClaimSpec,
    ) -> bool {
        // Check storage class match
        if let (Some(pv_class), Some(pvc_class)) =
            (&pv_spec.storage_class_name, &pvc_spec.storage_class_name)
        {
            if pv_class != pvc_class {
                return false;
            }
        }

        // Check capacity
        if let (Some(pv_storage), Some(pvc_storage)) = (
            pv_spec.capacity.get("storage"),
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        ) {
            // Simple string comparison - in real Kubernetes, this would parse quantities
            // For now, we'll just check if PV storage >= PVC storage
            if !self.storage_sufficient(pv_storage, pvc_storage) {
                return false;
            }
        }

        // Check access modes - PV must support all modes requested by PVC
        for pvc_mode in pvc_spec.access_modes_slice() {
            if !pv_spec.access_modes.contains(pvc_mode) {
                return false;
            }
        }

        true
    }

    /// Check if PV storage is sufficient for PVC
    /// This is a simple string comparison for now
    pub fn storage_sufficient(&self, pv_storage: &str, pvc_storage: &str) -> bool {
        // Parse the numeric part and unit from storage strings like "10Gi", "5Gi"
        let parse_storage = |s: &str| -> Option<(f64, String)> {
            let numeric_end = s.chars().position(|c| !c.is_numeric() && c != '.')?;
            let (num_str, unit) = s.split_at(numeric_end);
            let num = num_str.parse::<f64>().ok()?;
            Some((num, unit.to_string()))
        };

        match (parse_storage(pv_storage), parse_storage(pvc_storage)) {
            (Some((pv_num, pv_unit)), Some((pvc_num, pvc_unit))) => {
                // Units must match
                if pv_unit != pvc_unit {
                    debug!(
                        "Storage units don't match: PV has {}, PVC needs {}",
                        pv_unit, pvc_unit
                    );
                    return false;
                }
                // PV must have at least as much storage as PVC
                let sufficient = pv_num >= pvc_num;
                debug!(
                    "Storage comparison: PV has {}{}, PVC needs {}{} -> sufficient: {}",
                    pv_num, pv_unit, pvc_num, pvc_unit, sufficient
                );
                sufficient
            }
            _ => {
                debug!(
                    "Failed to parse storage values: PV='{}', PVC='{}'",
                    pv_storage, pvc_storage
                );
                // Fall back to string comparison if parsing fails
                pv_storage >= pvc_storage
            }
        }
    }
}

/// True if at least one node satisfies the PV's required node affinity.
///
/// `required.nodeSelectorTerms` are ORed; within a term every match expression
/// and match field must hold (AND). Mirrors upstream
/// `pkg/apis/core/v1/helper.MatchNodeSelectorTerms` semantics for the operators
/// rusternetes models.
/// True when a PV's `claimRef` names this exact PVC (namespace + name), with a
/// matching UID when the ref carries one. A pre-bound PV from the dynamic
/// provisioner sets all three; the UID guard prevents binding to a PV that was
/// pinned to an earlier, since-deleted PVC of the same name (#1095).
/// `storagehelpers.CheckVolumeModeMismatches`
/// (`staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go:331-343`):
/// a nil volumeMode defaults to Filesystem on both sides.
/// Claim-side metadata of `bindClaimToVolume`
/// (`pv_controller.go:1037-1092`): when the claim is not yet bound to this
/// volume (`:1044-1047`) set `spec.volumeName` and, unless already present,
/// `pv.kubernetes.io/bound-by-controller: "yes"` (`:1058-1066`); then set
/// `pv.kubernetes.io/bind-completed: "yes"` unless already present
/// (`:1064-1068`), which is what `syncClaim` (`:251`) and the scheduler's
/// `isPVCBound` (`volumebinding/binder.go:776`) key on.
/// `storagehelpers.PVDeletionInTreeProtectionFinalizer`
/// (component-helpers pv_helpers.go:82).
pub const IN_TREE_PV_DELETION_PROTECTION_FINALIZER: &str = "kubernetes.io/pv-controller";
/// `storagehelpers.PVDeletionProtectionFinalizer` (pv_helpers.go:79).
pub const EXTERNAL_PV_DELETION_PROTECTION_FINALIZER: &str =
    "external-provisioner.volume.kubernetes.io/finalizer";

/// `csilibplugins.*InTreePluginName` -> `*DriverName`, the entries of
/// `inTreePlugins` in `csi-translation-lib/translate.go:30-38` (names from
/// `plugins/{aws_ebs,gce_pd,azure_file,azure_disk,openstack_cinder,
/// vsphere_volume,portworx}.go`). Only the name mapping is ported; Rusternetes
/// has no volume-source translation.
const IN_TREE_TO_CSI_DRIVER: &[(&str, &str)] = &[
    ("kubernetes.io/gce-pd", "pd.csi.storage.gke.io"),
    ("kubernetes.io/aws-ebs", "ebs.csi.aws.com"),
    ("kubernetes.io/cinder", "cinder.csi.openstack.org"),
    ("kubernetes.io/azure-disk", "disk.csi.azure.com"),
    ("kubernetes.io/azure-file", "file.csi.azure.com"),
    ("kubernetes.io/vsphere-volume", "csi.vsphere.vmware.com"),
    ("kubernetes.io/portworx-volume", "pxd.portworx.com"),
];

/// `CSITranslator.GetCSINameFromInTreeName` (`translate.go:168-175`).
pub fn csi_name_from_in_tree_name(plugin: &str) -> Option<&'static str> {
    IN_TREE_TO_CSI_DRIVER
        .iter()
        .find(|(p, _)| *p == plugin)
        .map(|(_, d)| *d)
}

/// `PluginManager.IsMigrationEnabledForPlugin`
/// (`pkg/volume/csimigration/plugin_manager.go:85-107`). In 1.35 every
/// per-plugin gate is GA and locked on (`CSIMigrationPortworx`:
/// `kube_features.go:1178`), so each translator plugin is migrated.
pub fn is_migration_enabled_for_plugin(plugin: &str) -> bool {
    csi_name_from_in_tree_name(plugin).is_some()
}

/// `updateMigrationAnnotations` (`pv_controller_base.go:445-496`): the one
/// helper for volumes (`claim == false`, provisioner key
/// `AnnDynamicallyProvisioned`) and claims (`claim == true`, key
/// `AnnStorageProvisioner`, else the beta key; `:455-470`). Adds
/// `migrated-to` for a migrated plugin (`:477-485`) and removes it when
/// migration is off (`:486-490`). Returns whether the map changed.
pub fn update_migration_annotations(
    ann: Option<&mut std::collections::HashMap<String, String>>,
    claim: bool,
) -> bool {
    let Some(ann) = ann else { return false };
    let provisioner = if claim {
        ann.get(ANN_STORAGE_PROVISIONER)
            .or_else(|| ann.get(ANN_BETA_STORAGE_PROVISIONER))
    } else {
        ann.get(ANN_DYNAMICALLY_PROVISIONED)
    };
    let Some(provisioner) = provisioner.cloned() else {
        return false;
    };
    let migrated_to = ann.get(ANN_MIGRATED_TO).cloned().unwrap_or_default();
    if is_migration_enabled_for_plugin(&provisioner) {
        let Some(driver) = csi_name_from_in_tree_name(&provisioner) else {
            return false;
        };
        if migrated_to != driver {
            ann.insert(ANN_MIGRATED_TO.to_string(), driver.to_string());
            return true;
        }
    } else if !migrated_to.is_empty() {
        ann.remove(ANN_MIGRATED_TO);
        return true;
    }
    false
}

/// `modifyDeletionFinalizers` (`pv_controller_base.go:398-443`) with CSI
/// migration per `is_migration_enabled_for_plugin`;
/// `HonorPVReclaimPolicy` is GA and locked on in 1.35. Returns the new
/// finalizers and whether they changed.
pub fn modify_deletion_finalizers(pv: &PersistentVolume) -> (Option<Vec<String>>, bool) {
    let unchanged = || (pv.metadata.finalizers.clone(), false);
    let Some(provisioner) = pv
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_DYNAMICALLY_PROVISIONED))
    else {
        // Supported only for dynamically provisioned volumes.
        return unchanged();
    };
    let mut out = pv.metadata.finalizers.clone().unwrap_or_default();
    let mut modified = false;
    let has = |out: &[String], f: &str| out.iter().any(|x| x == f);
    if is_migration_enabled_for_plugin(provisioner) {
        // `:411-420`: remove the in-tree delete finalizer, migration is on.
        if has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER) {
            out.retain(|f| f != IN_TREE_PV_DELETION_PROTECTION_FINALIZER);
            modified = true;
        }
        return (
            if modified {
                Some(out).filter(|o| !o.is_empty())
            } else {
                pv.metadata.finalizers.clone()
            },
            modified,
        );
    }
    if !provisioner.starts_with("kubernetes.io/") {
        return unchanged();
    }
    let policy = pv.spec.persistent_volume_reclaim_policy.as_ref();
    if policy == Some(&PersistentVolumeReclaimPolicy::Delete)
        && !has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER)
    {
        out.push(IN_TREE_PV_DELETION_PROTECTION_FINALIZER.to_string());
        modified = true;
    } else if matches!(
        policy,
        Some(PersistentVolumeReclaimPolicy::Retain | PersistentVolumeReclaimPolicy::Recycle)
    ) && has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER)
    {
        out.retain(|f| f != IN_TREE_PV_DELETION_PROTECTION_FINALIZER);
        modified = true;
    }
    if has(&out, EXTERNAL_PV_DELETION_PROTECTION_FINALIZER) {
        out.retain(|f| f != EXTERNAL_PV_DELETION_PROTECTION_FINALIZER);
        modified = true;
    }
    if !modified {
        return unchanged();
    }
    (if out.is_empty() { None } else { Some(out) }, true)
}

pub fn bind_claim_to_volume(pvc: &mut PersistentVolumeClaim, volume_name: &str) {
    if pvc.spec.volume_name.as_deref() != Some(volume_name) {
        pvc.spec.volume_name = Some(volume_name.to_string());
        pvc.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .entry(ANN_BOUND_BY_CONTROLLER.to_string())
            .or_insert_with(|| "yes".to_string());
    }
    pvc.metadata
        .annotations
        .get_or_insert_with(Default::default)
        .entry(ANN_BIND_COMPLETED.to_string())
        .or_insert_with(|| "yes".to_string());
}

/// `storagehelpers.GetPersistentVolumeClaimClass` / `GetPersistentVolumeClass`
/// (pv_helpers.go): `spec.storageClassName`, else the legacy beta annotation,
/// else "".
fn class_of<'a>(
    spec_class: Option<&'a str>,
    annotations: Option<&'a std::collections::HashMap<String, String>>,
) -> &'a str {
    spec_class
        .or_else(|| {
            annotations
                .and_then(|a| a.get("volume.beta.kubernetes.io/storage-class"))
                .map(String::as_str)
        })
        .unwrap_or("")
}

fn volume_mode_mismatches(claim: &PersistentVolumeClaimSpec, pv: &PersistentVolumeSpec) -> bool {
    let requested = claim
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    let have = pv
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    requested != have
}

/// `storagehelpers.IsVolumeBoundToClaim` (pv_helpers.go:159-169): name and
/// namespace must match, and so must the UID when the claimRef carries one.
fn is_volume_bound_to_claim(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> bool {
    let Some(cr) = pv.spec.claim_ref.as_ref() else {
        return false;
    };
    if cr.name.as_deref() != Some(pvc.metadata.name.as_str())
        || cr.namespace.as_deref() != pvc.metadata.namespace.as_deref()
    {
        return false;
    }
    match cr.uid.as_deref() {
        Some(uid) if !uid.is_empty() => uid == pvc.metadata.uid,
        _ => true,
    }
}

fn object_ref_for_pv(pv: &PersistentVolume) -> ObjectReference {
    ObjectReference {
        kind: Some("PersistentVolume".to_string()),
        namespace: None,
        name: Some(pv.metadata.name.clone()),
        uid: Some(pv.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pv.metadata.resource_version.clone(),
        field_path: None,
    }
}

fn object_ref_for_pvc(pvc: &PersistentVolumeClaim) -> ObjectReference {
    ObjectReference {
        kind: Some("PersistentVolumeClaim".to_string()),
        namespace: pvc.metadata.namespace.clone(),
        name: Some(pvc.metadata.name.clone()),
        uid: Some(pvc.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pvc.metadata.resource_version.clone(),
        field_path: None,
    }
}

pub fn claim_ref_points_to(
    claim_ref: &rusternetes_common::resources::service_account::ObjectReference,
    namespace: &str,
    pvc_name: &str,
    pvc_uid: &str,
) -> bool {
    claim_ref.name.as_deref() == Some(pvc_name)
        && claim_ref.namespace.as_deref() == Some(namespace)
        && claim_ref.uid.as_ref().is_none_or(|u| u == pvc_uid)
}

fn node_affinity_satisfied(node_affinity: &VolumeNodeAffinity, nodes: &[Node]) -> bool {
    let required = match &node_affinity.required {
        Some(r) => r,
        None => return true,
    };
    nodes.iter().any(|node| {
        required
            .node_selector_terms
            .iter()
            .any(|term| volume_term_matches(node, term))
    })
}

/// A single node-selector term matches when all of its match expressions (over
/// node labels) and match fields (over `metadata.name`) hold.
fn volume_term_matches(node: &Node, term: &NodeSelectorTerm) -> bool {
    let labels = node.metadata.labels.as_ref();
    if let Some(exprs) = term.match_expressions.as_ref() {
        for req in exprs {
            let value = labels.and_then(|l| l.get(&req.key)).map(|s| s.as_str());
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    if let Some(fields) = term.match_fields.as_ref() {
        for req in fields {
            let value = match req.key.as_str() {
                "metadata.name" => Some(node.metadata.name.as_str()),
                _ => None,
            };
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    true
}

/// Evaluate one selector requirement against a (possibly absent) node value.
fn requirement_matches(value: Option<&str>, operator: &str, values: Option<&[String]>) -> bool {
    let values = values.unwrap_or(&[]);
    match operator {
        "In" => value
            .map(|v| values.iter().any(|x| x == v))
            .unwrap_or(false),
        "NotIn" => value
            .map(|v| !values.iter().any(|x| x == v))
            .unwrap_or(true),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" | "Lt" => {
            let (node_val, req_val) = match (value, values.first()) {
                (Some(v), Some(r)) => match (v.parse::<i64>(), r.parse::<i64>()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => return false,
                },
                _ => return false,
            };
            if operator == "Gt" {
                node_val > req_val
            } else {
                node_val < req_val
            }
        }
        _ => false,
    }
}
