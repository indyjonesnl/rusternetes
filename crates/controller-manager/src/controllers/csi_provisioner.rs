//! The CSI external-provisioner loop (#2882): a port of
//! kubernetes-csi/external-provisioner `pkg/controller/controller.go`
//! (`csiProvisioner`: `ShouldProvision`, `Provision`, `prepareProvision`,
//! `cleanupVolume`) running inside sig-storage-lib-external-provisioner
//! `controller/controller.go` (`syncClaim`, `shouldProvision`,
//! `provisionClaimOperation`, `provisionVolumeErrorHandling`) and its
//! `volume_store.go` (`queueStore`), the store `csi-provisioner` selects
//! (`cmd/csi-provisioner/csi-provisioner.go:406`,
//! `CreateProvisionedPVLimiter`).
//!
//! One [`CsiProvisioner`] serves one CSI driver, as one external-provisioner
//! sidecar does: it handles claims annotated
//! `volume.kubernetes.io/storage-provisioner` (or the beta key) with its
//! driver name and talks to that driver's Controller service over its unix
//! socket ([`CsiControllerClient`]).
//!
//! Slice 1 (this file): a claim without a data source. NOT yet ported, each
//! tracked on #2882 and rejected loudly rather than silently ignored:
//! `dataSource` / `dataSourceRef` of kind VolumeSnapshot or PVC
//! (`getVolumeContentSource`), provisioner/other secret parameters
//! (`getSecretReference`), topology (`GenerateAccessibilityRequirements`,
//! `GenerateVolumeNodeAffinity`), VolumeAttributesClass, the delete path
//! (`syncVolume` / `deleteVolumeOperation`) and the slow-retry set for
//! infeasible (`InvalidArgument`) requests.

use anyhow::{anyhow, Context, Result};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    CSIVolumeSource, PersistentVolumeAccessMode, PersistentVolumeMode, PersistentVolumePhase,
    StorageClass, VolumeBindingMode,
};
use rusternetes_common::resources::{
    EventSource, EventType, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
    PersistentVolumeStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_csi::controller_client::{
    check_driver_capabilities, ControllerError, CsiControllerClient, DriverCapabilities,
    ProvisioningState, RequiredCapabilities,
};
use rusternetes_csi::proto::volume_capability::access_mode::Mode as AccessModeKind;
use rusternetes_csi::proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use rusternetes_csi::proto::{
    CapacityRange, CreateVolumeRequest, DeleteVolumeRequest, VolumeCapability,
};
use rusternetes_storage::WorkQueueConfig;
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// `annStorageProvisioner` (lib controller.go:70).
const ANN_STORAGE_PROVISIONER: &str = "volume.kubernetes.io/storage-provisioner";
/// `annBetaStorageProvisioner` (lib controller.go:69).
const ANN_BETA_STORAGE_PROVISIONER: &str = "volume.beta.kubernetes.io/storage-provisioner";
/// `annMigratedTo` (lib controller.go:66).
const ANN_MIGRATED_TO: &str = "pv.kubernetes.io/migrated-to";
/// `annSelectedNode` / `annAlphaSelectedNode` (lib controller.go:74, :77).
const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
const ANN_ALPHA_SELECTED_NODE: &str = "volume.alpha.kubernetes.io/selected-node";
/// `annDynamicallyProvisioned` (lib controller.go:62).
const ANN_DYNAMICALLY_PROVISIONED: &str = "pv.kubernetes.io/provisioned-by";
/// `finalizerPV` (lib controller.go:80).
pub const FINALIZER_PV: &str = "external-provisioner.volume.kubernetes.io/finalizer";
/// `annDeletionProvisionerSecretRefName` / `...Namespace`
/// (external-provisioner controller.go:151-152).
const ANN_DELETION_SECRET_NAME: &str = "volume.kubernetes.io/provisioner-deletion-secret-name";
const ANN_DELETION_SECRET_NAMESPACE: &str =
    "volume.kubernetes.io/provisioner-deletion-secret-namespace";
/// `provisionerIDKey` (external-provisioner controller.go:315).
const PROVISIONER_ID_KEY: &str = "storage.kubernetes.io/csiProvisionerIdentity";
/// `csiParameterPrefix` and the well-known keys under it (controller.go:83-109).
const CSI_PARAMETER_PREFIX: &str = "csi.storage.k8s.io/";
const PREFIXED_FSTYPE_KEY: &str = "csi.storage.k8s.io/fstype";
const PREFIXED_KNOWN_KEYS: &[&str] = &[
    "fstype",
    "secret-name",
    "secret-namespace",
    "provisioner-secret-name",
    "provisioner-secret-namespace",
    "controller-publish-secret-name",
    "controller-publish-secret-namespace",
    "node-stage-secret-name",
    "node-stage-secret-namespace",
    "node-publish-secret-name",
    "node-publish-secret-namespace",
    "controller-expand-secret-name",
    "controller-expand-secret-namespace",
    "node-expand-secret-name",
    "node-expand-secret-namespace",
    "controller-modify-secret-name",
    "controller-modify-secret-namespace",
];
const PVC_NAME_KEY: &str = "csi.storage.k8s.io/pvc/name";
const PVC_NAMESPACE_KEY: &str = "csi.storage.k8s.io/pvc/namespace";
const PV_NAME_KEY: &str = "csi.storage.k8s.io/pv/name";
/// `snapshotKind` / `snapshotAPIGroup` / `pvcKind` (controller.go:130-132).
const SNAPSHOT_KIND: &str = "VolumeSnapshot";
const SNAPSHOT_API_GROUP: &str = "snapshot.storage.k8s.io";
const PVC_KIND: &str = "PersistentVolumeClaim";
/// `deleteVolumeRetryCount` (controller.go:141).
const DELETE_VOLUME_RETRY_COUNT: usize = 5;
/// `--volume-name-prefix` default (csi-provisioner.go:81).
const VOLUME_NAME_PREFIX: &str = "pvc";
/// `--retry-interval-start` / `--retry-interval-max` (csi-provisioner.go:83-84).
const RETRY_INTERVAL_START: Duration = Duration::from_secs(1);
const RETRY_INTERVAL_MAX: Duration = Duration::from_secs(300);
/// `DefaultTypedControllerRateLimiter`'s per-item exponential limiter
/// (`5ms` base, `1000s` cap), used by the background PV store.
const PV_SAVE_BASE: Duration = Duration::from_millis(5);
const PV_SAVE_MAX: Duration = Duration::from_secs(1000);
/// Concurrent claim workers (`--worker-threads`, csi-provisioner.go:85,
/// default 100; this process runs fewer, the claims are cheap).
const WORKERS: usize = 8;

/// Why a provisioning step stopped without an error to retry.
#[derive(Debug)]
enum Failure {
    /// `controller.IgnoredError`: another provisioner/populator owns it.
    Ignored(String),
    /// `errStopProvision`.
    Stop,
    Error(anyhow::Error),
}

impl From<anyhow::Error> for Failure {
    fn from(e: anyhow::Error) -> Self {
        Failure::Error(e)
    }
}

type Outcome<T> = std::result::Result<T, (ProvisioningState, Failure)>;

/// A provisioned PV the API server has not accepted yet (`queueStore`).
struct Unsaved {
    pv: PersistentVolume,
    attempts: u32,
    next_try: Instant,
}

/// `GetPersistentVolumeClaimClass` (component-helpers `helpers.go`).
fn claim_class(claim: &PersistentVolumeClaim) -> String {
    if let Some(c) = claim.spec.storage_class_name.as_ref() {
        return c.clone();
    }
    claim
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("volume.beta.kubernetes.io/storage-class"))
        .cloned()
        .unwrap_or_default()
}

fn annotation<'a>(meta: &'a ObjectMeta, key: &str) -> Option<&'a String> {
    meta.annotations.as_ref().and_then(|a| a.get(key))
}

/// `makeVolumeName` with `--volume-name-uuid-length=-1` (controller.go:483-498).
pub fn make_volume_name(prefix: &str, pvc_uid: &str) -> std::result::Result<String, String> {
    if prefix.is_empty() {
        return Err("volume name prefix cannot be of length 0".into());
    }
    if pvc_uid.is_empty() {
        return Err("corrupted PVC object, it is missing UID".into());
    }
    Ok(format!("{prefix}-{pvc_uid}"))
}

/// `accessmodes.ToCSIAccessMode` (kubernetes-csi/csi-lib-utils
/// `accessmodes/access_modes.go`), for one claim access mode at a time as
/// `getVolumeCapability` calls it (controller.go:522).
pub fn to_csi_access_mode(
    modes: &[PersistentVolumeAccessMode],
    supports_single_node_multi_writer: bool,
) -> std::result::Result<AccessModeKind, String> {
    use PersistentVolumeAccessMode::*;
    let has = |m: PersistentVolumeAccessMode| modes.contains(&m);
    let (rwop, rwx, rox, rwo) = (
        has(ReadWriteOncePod),
        has(ReadWriteMany),
        has(ReadOnlyMany),
        has(ReadWriteOnce),
    );
    let unique: HashSet<String> = modes.iter().map(|m| format!("{m:?}")).collect();
    if rwop {
        if unique.len() > 1 {
            return Err("Kubernetes does not support use of ReadWriteOncePod with other access modes on the same PersistentVolume".into());
        }
        return Ok(if supports_single_node_multi_writer {
            AccessModeKind::SingleNodeSingleWriter
        } else {
            AccessModeKind::SingleNodeWriter
        });
    }
    if rwx {
        return Ok(AccessModeKind::MultiNodeMultiWriter);
    }
    if rox && rwo {
        return Err(
            "CSI does not support ReadOnlyMany and ReadWriteOnce on the same PersistentVolume"
                .into(),
        );
    }
    if rox {
        return Ok(AccessModeKind::MultiNodeReaderOnly);
    }
    if rwo {
        return Ok(if supports_single_node_multi_writer {
            AccessModeKind::SingleNodeMultiWriter
        } else {
            AccessModeKind::SingleNodeWriter
        });
    }
    Err(format!("unsupported AccessMode combination: {modes:?}"))
}

/// `removePrefixedParameters` (controller.go:1145-1182).
pub fn remove_prefixed_parameters(
    param: &HashMap<String, String>,
) -> std::result::Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    for (k, v) in param {
        if let Some(rest) = k.strip_prefix(CSI_PARAMETER_PREFIX) {
            if !PREFIXED_KNOWN_KEYS.contains(&rest) {
                return Err(format!(
                    "found unknown parameter key \"{k}\" with reserved namespace {CSI_PARAMETER_PREFIX}"
                ));
            }
        } else {
            out.insert(k.clone(), v.clone());
        }
    }
    Ok(out)
}

/// `CheckPersistentVolumeClaimModeBlock`.
fn is_block(claim: &PersistentVolumeClaim) -> bool {
    matches!(claim.spec.volume_mode, Some(PersistentVolumeMode::Block))
}

/// What `prepareProvision` returns.
struct Prepared {
    fs_type: String,
    req: CreateVolumeRequest,
}

pub struct CsiProvisioner<S: Storage> {
    storage: Arc<S>,
    driver_name: String,
    client: CsiControllerClient,
    /// `identity` (csi-provisioner.go:298), stamped into every volume's
    /// attributes as `storage.kubernetes.io/csiProvisionerIdentity`.
    identity: String,
    default_fs_type: String,
    extra_create_metadata: bool,
    controller_publish_read_only: bool,
    recorder: EventRecorder<S>,
    /// `claimsInProgress`: claims whose CreateVolume may still be running in
    /// the driver; kept so a claim deleted meanwhile is still driven to
    /// completion (or cleaned up).
    claims_in_progress: Mutex<HashMap<String, PersistentVolumeClaim>>,
    /// `queueStore.volumes`.
    unsaved: Mutex<HashMap<String, Unsaved>>,
    capabilities: tokio::sync::OnceCell<DriverCapabilities>,
}

impl<S: Storage + 'static> CsiProvisioner<S> {
    pub fn new(
        storage: Arc<S>,
        driver_name: impl Into<String>,
        client: CsiControllerClient,
    ) -> Self {
        let driver_name = driver_name.into();
        // csi-provisioner.go:298: `<unix ts>-<rand 0..9999>-<provisioner name>`.
        let identity = format!(
            "{}-{}-{}",
            chrono::Utc::now().timestamp(),
            rand::random::<u32>() % 10000,
            driver_name
        );
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            driver_name,
            client,
            identity,
            default_fs_type: String::new(),
            extra_create_metadata: false,
            controller_publish_read_only: false,
            claims_in_progress: Mutex::new(HashMap::new()),
            unsaved: Mutex::new(HashMap::new()),
            capabilities: tokio::sync::OnceCell::new(),
        }
    }

    /// `--default-fstype`.
    pub fn with_default_fs_type(mut self, fs_type: impl Into<String>) -> Self {
        self.default_fs_type = fs_type.into();
        self
    }

    /// `--extra-create-metadata`.
    pub fn with_extra_create_metadata(mut self, on: bool) -> Self {
        self.extra_create_metadata = on;
        self
    }

    /// `--controller-publish-readonly`.
    pub fn with_controller_publish_read_only(mut self, on: bool) -> Self {
        self.controller_publish_read_only = on;
        self
    }

    // ---- events -----------------------------------------------------------

    fn claim_ref(claim: &PersistentVolumeClaim) -> ObjectReference {
        ObjectReference {
            kind: Some("PersistentVolumeClaim".to_string()),
            namespace: claim.metadata.namespace.clone(),
            name: Some(claim.metadata.name.clone()),
            uid: Some(claim.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: claim.metadata.resource_version.clone(),
            field_path: None,
        }
    }

    async fn event(
        &self,
        claim: &PersistentVolumeClaim,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let source = EventSource {
            component: format!("{}_{}", self.driver_name, self.identity),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(
                &Self::claim_ref(claim),
                &source,
                event_type,
                reason,
                message,
            )
            .await
        {
            warn!("failed to record {reason} event: {e}");
        }
    }

    // ---- sync -------------------------------------------------------------

    async fn driver_capabilities(
        &self,
    ) -> std::result::Result<&DriverCapabilities, ControllerError> {
        self.capabilities
            .get_or_try_init(|| self.client.get_driver_capabilities())
            .await
    }

    async fn get_storage_class(&self, name: &str) -> Result<StorageClass> {
        self.storage
            .get(&build_key("storageclasses", None, name))
            .await
            .map_err(|_| anyhow!("storageClass {name:?} not found"))
    }

    /// `shouldProvision` (lib controller.go:1242-1283) behind the
    /// `Qualifier` (`ShouldProvision`, external-provisioner
    /// controller.go:1557-1592; no node deployment, so `checkNode` is always
    /// "owned").
    async fn should_provision(&self, claim: &PersistentVolumeClaim) -> Result<bool> {
        if claim
            .spec
            .volume_name
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        {
            return Ok(false);
        }
        let provisioner = annotation(&claim.metadata, ANN_STORAGE_PROVISIONER)
            .or_else(|| annotation(&claim.metadata, ANN_BETA_STORAGE_PROVISIONER));
        let migrated_to = annotation(&claim.metadata, ANN_MIGRATED_TO);
        if provisioner.map(String::as_str) != Some(self.driver_name.as_str())
            && migrated_to.map(String::as_str) != Some(self.driver_name.as_str())
        {
            return Ok(false);
        }
        let class = self.get_storage_class(&claim_class(claim)).await?;
        if matches!(
            class.volume_binding_mode,
            Some(VolumeBindingMode::WaitForFirstConsumer)
        ) {
            // "annSelectedNode is required to provision volume."
            return Ok(
                annotation(&claim.metadata, ANN_SELECTED_NODE).is_some_and(|n| !n.is_empty())
            );
        }
        Ok(true)
    }

    /// `syncClaim` (lib controller.go:1098-1147).
    pub async fn sync_claim(&self, claim: &PersistentVolumeClaim) -> Result<()> {
        if !self.should_provision(claim).await? {
            return Ok(());
        }
        let uid = claim.metadata.uid.clone();
        match self.provision_claim_operation(claim).await {
            Ok(()) => {
                self.claims_in_progress.lock().unwrap().remove(&uid);
                Ok(())
            }
            Err((state, failure)) => {
                let result = match failure {
                    // "Our caller would requeue if we pass on this special
                    // error; return nil instead."
                    Failure::Stop | Failure::Ignored(_) => Ok(()),
                    Failure::Error(e) => Err(e),
                };
                match state {
                    ProvisioningState::Finished => {
                        self.claims_in_progress.lock().unwrap().remove(&uid);
                    }
                    ProvisioningState::InBackground => {
                        self.claims_in_progress
                            .lock()
                            .unwrap()
                            .insert(uid, claim.clone());
                    }
                    // Don't change claimsInProgress.
                    ProvisioningState::NoChange | ProvisioningState::Reschedule => {}
                }
                result
            }
        }
    }

    /// `provisionClaimOperation` (lib controller.go:1436-1543).
    async fn provision_claim_operation(&self, claim: &PersistentVolumeClaim) -> Outcome<()> {
        let class_name = claim_class(claim);
        let ns = claim.metadata.namespace.as_deref().unwrap_or("");

        // "A previous doProvisionClaim may just have finished ... Check that
        // PV (with deterministic name) hasn't been provisioned yet."
        let pv_name = make_volume_name(VOLUME_NAME_PREFIX, &claim.metadata.uid)
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(anyhow!(e))))?;
        let pv_key = build_key("persistentvolumes", None, &pv_name);
        if self.storage.get::<PersistentVolume>(&pv_key).await.is_ok() {
            debug!("PersistentVolume {pv_name} already exists, skipping");
            return Err((ProvisioningState::Finished, Failure::Stop));
        }

        // canProvision: the CSI provisioner supports block (`SupportsBlock`).

        let class = self
            .get_storage_class(&class_name)
            .await
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(e)))?;
        if class.provisioner != self.driver_name {
            error!(
                "Unknown provisioner {:?} requested in claim's StorageClass",
                class.provisioner
            );
            return Err((ProvisioningState::Finished, Failure::Stop));
        }

        let selected_node = annotation(&claim.metadata, ANN_SELECTED_NODE)
            .or_else(|| annotation(&claim.metadata, ANN_ALPHA_SELECTED_NODE))
            .cloned()
            .unwrap_or_default();

        self.event(
            claim,
            EventType::Normal,
            "Provisioning",
            &format!(
                "External provisioner is provisioning volume for claim \"{ns}/{}\"",
                claim.metadata.name
            ),
        )
        .await;

        let mut volume = match self
            .provision(claim, &class, &selected_node, &pv_name)
            .await
        {
            Ok(v) => v,
            Err((_, Failure::Ignored(reason))) => {
                // "Provision ignored, do nothing and hope another provisioner
                // will provision it."
                debug!("Volume provision ignored: {reason}");
                return Err((ProvisioningState::Finished, Failure::Stop));
            }
            Err((state, Failure::Error(err))) => {
                // isInfeasibleError (lib :1587-1599): gRPC InvalidArgument.
                if err
                    .downcast_ref::<ControllerError>()
                    .and_then(ControllerError::status)
                    .is_some_and(|s| s.code() == tonic::Code::InvalidArgument)
                {
                    info!("Detected infeasible volume provisioning request: {err}");
                    self.event(
                        claim,
                        EventType::Warning,
                        "ProvisioningFailed",
                        &format!(
                            "Volume provisioning failed with infeasible error. Retries will be delayed. {err}"
                        ),
                    )
                    .await;
                    return Err((ProvisioningState::Finished, Failure::Error(err)));
                }
                return self
                    .provision_volume_error_handling(state, err, claim)
                    .await;
            }
            Err((state, Failure::Stop)) => return Err((state, Failure::Stop)),
        };

        // "Set ClaimRef and the PV controller will bind and set
        // annBoundByController for us"
        volume.spec.claim_ref = Some(Self::claim_ref(claim));
        // `addFinalizer` is true for csi-provisioner (csi-provisioner.go:409).
        let finalizers = volume.metadata.finalizers.get_or_insert_with(Vec::new);
        if !finalizers.iter().any(|f| f == FINALIZER_PV) {
            finalizers.push(FINALIZER_PV.to_string());
        }
        volume
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(
                ANN_DYNAMICALLY_PROVISIONED.to_string(),
                class.provisioner.clone(),
            );
        volume.spec.storage_class_name = Some(class_name);

        self.store_volume(claim, volume).await;
        Ok(())
    }

    /// `provisionVolumeErrorHandling` (lib controller.go:1601-1634).
    async fn provision_volume_error_handling(
        &self,
        mut state: ProvisioningState,
        err: anyhow::Error,
        claim: &PersistentVolumeClaim,
    ) -> Outcome<()> {
        self.event(
            claim,
            EventType::Warning,
            "ProvisioningFailed",
            &err.to_string(),
        )
        .await;
        if annotation(&claim.metadata, ANN_SELECTED_NODE).is_some()
            && state == ProvisioningState::Reschedule
        {
            // "`selectedNode` must be removed to notify scheduler to schedule
            // again." (`rescheduleProvisioning`, lib :1393).
            return match self.reschedule_provisioning(claim).await {
                Ok(()) => {
                    info!("Volume rescheduled because: {err}");
                    Err((ProvisioningState::Finished, Failure::Stop))
                }
                Err(e) => {
                    warn!("Volume rescheduling failed: {e}");
                    Err((ProvisioningState::Finished, Failure::Error(err)))
                }
            };
        }
        // "ProvisioningReschedule shouldn't have been returned for volumes
        // without selected node ... treat it like ProvisioningFinished."
        if state == ProvisioningState::Reschedule {
            state = ProvisioningState::Finished;
        }
        Err((state, Failure::Error(err)))
    }

    /// `rescheduleProvisioning` (lib controller.go:1393-1434).
    async fn reschedule_provisioning(&self, claim: &PersistentVolumeClaim) -> Result<()> {
        let ns = claim.metadata.namespace.as_deref().unwrap_or("");
        let key = build_key("persistentvolumeclaims", Some(ns), &claim.metadata.name);
        let mut c = claim.clone();
        if let Some(a) = c.metadata.annotations.as_mut() {
            a.remove(ANN_SELECTED_NODE);
        }
        self.storage.update(&key, &c).await?;
        Ok(())
    }

    // ---- Provision ----------------------------------------------------------

    /// `getVolumeCapabilities` / `getVolumeCapability` (controller.go:515-577).
    fn volume_capabilities(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        fs_type: &str,
        single_node_multi_writer: bool,
    ) -> std::result::Result<Vec<VolumeCapability>, String> {
        let mut caps = Vec::new();
        for mode in &claim.spec.access_modes {
            let kind = to_csi_access_mode(std::slice::from_ref(mode), single_node_multi_writer)?;
            let access_type = if is_block(claim) {
                AccessType::Block(BlockVolume {})
            } else {
                AccessType::Mount(MountVolume {
                    fs_type: fs_type.to_string(),
                    mount_flags: sc.mount_options.clone().unwrap_or_default(),
                    volume_mount_group: String::new(),
                })
            };
            caps.push(VolumeCapability {
                access_type: Some(access_type),
                access_mode: Some(AccessMode { mode: kind as i32 }),
            });
        }
        Ok(caps)
    }

    /// `prepareProvision` (controller.go:579-830), minus the parts listed in
    /// the module docs.
    async fn prepare_provision(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        pv_name: &str,
    ) -> Outcome<Prepared> {
        let fin = |e: String| (ProvisioningState::Finished, Failure::Error(anyhow!(e)));

        // dataSource normalisation + required capabilities (:586-:625).
        let rc = RequiredCapabilities::default();
        let data_source = claim
            .spec
            .data_source
            .as_ref()
            .map(|d| {
                (
                    d.api_group.clone().unwrap_or_default(),
                    d.kind.clone(),
                    d.name.clone(),
                    None,
                )
            })
            .or_else(|| {
                claim.spec.data_source_ref.as_ref().map(|d| {
                    (
                        d.api_group.clone().unwrap_or_default(),
                        d.kind.clone(),
                        d.name.clone(),
                        d.namespace.clone(),
                    )
                })
            });
        if let Some((api_group, kind, name, namespace)) = &data_source {
            if name.is_empty() {
                return Err(fin(format!(
                    "the PVC source not found for PVC {}",
                    claim.metadata.name
                )));
            }
            match kind.as_str() {
                SNAPSHOT_KIND => {
                    if api_group != SNAPSHOT_API_GROUP {
                        return Err(fin(format!(
                            "the PVC source does not belong to the right APIGroup. Expected {SNAPSHOT_API_GROUP}, Got {api_group}"
                        )));
                    }
                }
                PVC_KIND => {}
                other => {
                    // "Assume external data populator to create the volume,
                    // and there is no more work for us to do"
                    self.event(
                        claim,
                        EventType::Normal,
                        "Provisioning",
                        "Assuming an external populator will provision the volume",
                    )
                    .await;
                    return Err((
                        ProvisioningState::Finished,
                        Failure::Ignored(format!(
                            "data source ({other}) is not handled by the provisioner, assuming an external populator will provision it"
                        )),
                    ));
                }
            }
            let _ = namespace;
            return Err(fin(format!(
                "dataSource {kind} {name} is not supported by this provisioner yet (#2882: VolumeContentSource)"
            )));
        }
        if claim
            .spec
            .volume_attributes_class_name
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(fin(
                "VolumeAttributesClass is not supported by this provisioner yet (#2882)".into(),
            ));
        }

        let caps = self.driver_capabilities().await.map_err(|e| {
            (
                e.provisioning_state(false),
                Failure::Error(anyhow::Error::new(e)),
            )
        })?;
        check_driver_capabilities(caps, &rc)
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(e.into())))?;

        if claim.spec.selector.is_some() {
            return Err(fin("claim Selector is not supported".into()));
        }

        // fstype from the class parameters (:665-:683).
        let params = sc.parameters.clone().unwrap_or_default();
        let mut fs_types_found = 0;
        let mut fs_type = String::new();
        for (k, v) in &params {
            if k.to_lowercase() == "fstype" || k == PREFIXED_FSTYPE_KEY {
                fs_type = v.clone();
                fs_types_found += 1;
            }
        }
        if fs_types_found > 1 {
            return Err(fin(format!(
                "fstype specified in parameters with both \"fstype\" and \"{PREFIXED_FSTYPE_KEY}\" keys"
            )));
        }
        if fs_type.is_empty() && !self.default_fs_type.is_empty() {
            fs_type = self.default_fs_type.clone();
        }

        // Secrets are not ported yet: refuse rather than silently omit.
        if let Some(k) = params.keys().find(|k| {
            k.strip_prefix(CSI_PARAMETER_PREFIX)
                .is_some_and(|r| r.contains("secret-"))
        }) {
            return Err(fin(format!(
                "StorageClass parameter {k:?} (secrets) is not supported by this provisioner yet (#2882)"
            )));
        }

        let requested = claim
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .map(|q| Quantity::parse(q))
            .transpose()
            .map_err(|e| fin(format!("invalid storage request: {e}")))?;
        let required_bytes = requested.map_or(0, |q| q.value() as i64);

        let volume_caps = self
            .volume_capabilities(
                claim,
                sc,
                &fs_type,
                caps.controller.contains(
                    &rusternetes_csi::proto::controller_service_capability::rpc::Type::SingleNodeMultiWriter,
                ),
            )
            .map_err(fin)?;

        let mut parameters = remove_prefixed_parameters(&params).map_err(|e| {
            fin(format!(
                "failed to strip CSI Parameters of prefixed keys: {e}"
            ))
        })?;
        if self.extra_create_metadata {
            parameters.insert(PVC_NAME_KEY.into(), claim.metadata.name.clone());
            parameters.insert(
                PVC_NAMESPACE_KEY.into(),
                claim.metadata.namespace.clone().unwrap_or_default(),
            );
            parameters.insert(PV_NAME_KEY.into(), pv_name.to_string());
        }

        let req = CreateVolumeRequest {
            name: pv_name.to_string(),
            parameters,
            volume_capabilities: volume_caps,
            capacity_range: Some(CapacityRange {
                required_bytes,
                limit_bytes: 0,
            }),
            ..Default::default()
        };
        Ok(Prepared { fs_type, req })
    }

    /// `cleanupVolume` (controller.go:2074-2086).
    async fn cleanup_volume(&self, volume_id: &str) -> Result<()> {
        let mut last = None;
        for _ in 0..DELETE_VOLUME_RETRY_COUNT {
            match self
                .client
                .delete_volume(DeleteVolumeRequest {
                    volume_id: volume_id.to_string(),
                    ..Default::default()
                })
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => last = Some(e),
            }
        }
        Err(anyhow::Error::new(last.expect("retry count is non-zero")))
    }

    /// `Provision` (controller.go:838-1052).
    async fn provision(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        selected_node: &str,
        pv_name: &str,
    ) -> Outcome<PersistentVolume> {
        let provisioner = annotation(&claim.metadata, ANN_STORAGE_PROVISIONER)
            .or_else(|| annotation(&claim.metadata, ANN_BETA_STORAGE_PROVISIONER));
        if provisioner.map(String::as_str) != Some(self.driver_name.as_str())
            && annotation(&claim.metadata, ANN_MIGRATED_TO).map(String::as_str)
                != Some(self.driver_name.as_str())
        {
            return Err((
                ProvisioningState::Finished,
                Failure::Ignored(format!(
                    "PVC annotated with external-provisioner name {} does not match provisioner driver name {}. This could mean the PVC is not migrated",
                    provisioner.map(String::as_str).unwrap_or(""),
                    self.driver_name
                )),
            ));
        }

        // prepareProvision returns ProvisioningNoChange alongside its
        // non-final failures; a `?` keeps the state it chose.
        let Prepared { fs_type, req } = self.prepare_provision(claim, sc, pv_name).await?;
        let vol_size_bytes = req.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
        let vol_caps = req.volume_capabilities.clone();

        let supports_topology = self
            .driver_capabilities()
            .await
            .map(|c| c.supports_topology())
            .unwrap_or(false);
        let vol = match self.client.create_volume(req).await {
            Ok(v) => v,
            Err(e) => {
                // "Giving up after an error and telling the pod scheduler to
                // retry with a different node only makes sense if" the driver
                // supports topology and a node was selected (:884-:900).
                let may_reschedule = supports_topology && !selected_node.is_empty();
                let state = e.provisioning_state(may_reschedule);
                debug!("CreateVolume failed, may reschedule = {may_reschedule} => state = {state:?}: {e}");
                return Err((state, Failure::Error(anyhow::Error::new(e))));
            }
        };

        let mut volume_attributes: HashMap<String, String> =
            HashMap::from([(PROVISIONER_ID_KEY.to_string(), self.identity.clone())]);
        volume_attributes.extend(vol.volume_context.clone());
        let mut resp_cap = vol.capacity_bytes;

        // "According to CSI spec CreateVolume should be able to return
        // capacity = 0, which means it is unknown."
        if resp_cap == 0 {
            resp_cap = vol_size_bytes;
        } else if resp_cap < vol_size_bytes {
            let mut cap_err = format!(
                "created volume capacity {resp_cap} less than requested capacity {vol_size_bytes}"
            );
            if let Err(e) = self.cleanup_volume(&vol.volume_id).await {
                cap_err = format!(
                    "{cap_err}. Cleanup of volume {pv_name} failed, volume is orphaned: {e}"
                );
            }
            // "use InBackground to retry the call, hoping the volume is
            // deleted correctly next time."
            return Err((
                ProvisioningState::InBackground,
                Failure::Error(anyhow!(cap_err)),
            ));
        }

        // `ReadOnlyMany` on its own with --controller-publish-readonly (:965-:971).
        let pv_read_only = vol_caps.len() == 1
            && vol_caps[0].access_mode.as_ref().map(|m| m.mode)
                == Some(AccessModeKind::MultiNodeReaderOnly as i32)
            && self.controller_publish_read_only;

        let block = is_block(claim);
        let mut annotations = HashMap::new();
        // No provisioner secret ref: upstream stamps empty strings (:988-:989).
        annotations.insert(ANN_DELETION_SECRET_NAME.to_string(), String::new());
        annotations.insert(ANN_DELETION_SECRET_NAMESPACE.to_string(), String::new());

        let mut meta = ObjectMeta::new(pv_name);
        meta.annotations = Some(annotations);
        let pv = PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: PersistentVolumeSpec {
                access_modes: claim.spec.access_modes.clone(),
                mount_options: sc.mount_options.clone(),
                capacity: HashMap::from([(
                    "storage".to_string(),
                    // `bytesToQuantity`: `resource.NewQuantity(bytes, BinarySI)`.
                    Quantity::from_value(resp_cap, Format::BinarySI).canonical_string(),
                )]),
                persistent_volume_reclaim_policy: sc.reclaim_policy.clone(),
                volume_mode: claim.spec.volume_mode.clone(),
                csi: Some(CSIVolumeSource {
                    driver: self.driver_name.clone(),
                    volume_handle: Some(vol.volume_id.clone()),
                    read_only: Some(pv_read_only),
                    // "Set FSType if PV is not Block Volume"
                    fs_type: if block { None } else { Some(fs_type) },
                    volume_attributes: Some(volume_attributes),
                    ..Default::default()
                }),
                ..Default::default()
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Pending,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        };
        info!(
            "successfully created PV {} for PVC {} and csi volume name {}",
            pv.metadata.name, claim.metadata.name, vol.volume_id
        );
        Ok(pv)
    }

    // ---- volume store (`queueStore`) -----------------------------------------

    /// `queueStore.StoreVolume`: try once; on failure keep the PV and retry in
    /// the background, "consum[ing] any error".
    async fn store_volume(&self, claim: &PersistentVolumeClaim, pv: PersistentVolume) {
        match self.save_volume(&pv).await {
            Ok(()) => {
                self.send_success_event(claim, &pv.metadata.name).await;
            }
            Err(e) => {
                error!("Failed to save volume {}: {e}", pv.metadata.name);
                self.unsaved.lock().unwrap().insert(
                    pv.metadata.name.clone(),
                    Unsaved {
                        pv,
                        attempts: 1,
                        next_try: Instant::now() + backoff(1),
                    },
                );
            }
        }
    }

    /// `queueStore.doSaveVolume`: create the PV, `AlreadyExists` is success.
    async fn save_volume(&self, pv: &PersistentVolume) -> Result<()> {
        let key = build_key("persistentvolumes", None, &pv.metadata.name);
        match self.storage.create(&key, pv).await {
            Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => Ok(()),
            Err(e) => Err(anyhow!("error saving volume {}: {e}", pv.metadata.name)),
        }
    }

    async fn send_success_event(&self, claim: &PersistentVolumeClaim, pv_name: &str) {
        self.event(
            claim,
            EventType::Normal,
            "ProvisioningSucceeded",
            &format!("Successfully provisioned volume {pv_name}"),
        )
        .await;
    }

    /// `queueStore.processNextWorkItem`, one pass over the unsaved PVs that
    /// are due.
    pub async fn save_unsaved_volumes(&self) {
        let due: Vec<PersistentVolume> = {
            let now = Instant::now();
            self.unsaved
                .lock()
                .unwrap()
                .values()
                .filter(|u| u.next_try <= now)
                .map(|u| u.pv.clone())
                .collect()
        };
        for pv in due {
            let name = pv.metadata.name.clone();
            match self.save_volume(&pv).await {
                Ok(()) => {
                    self.unsaved.lock().unwrap().remove(&name);
                    if let Some(claim) = self.claim_for_volume(&pv).await {
                        self.send_success_event(&claim, &name).await;
                    }
                }
                Err(e) => {
                    warn!("{e}");
                    if let Some(u) = self.unsaved.lock().unwrap().get_mut(&name) {
                        u.attempts += 1;
                        u.next_try = Instant::now() + backoff(u.attempts);
                    }
                }
            }
        }
    }

    async fn claim_for_volume(&self, pv: &PersistentVolume) -> Option<PersistentVolumeClaim> {
        let r = pv.spec.claim_ref.as_ref()?;
        let claim: PersistentVolumeClaim = self
            .storage
            .get(&build_key(
                "persistentvolumeclaims",
                r.namespace.as_deref(),
                r.name.as_deref()?,
            ))
            .await
            .ok()?;
        (Some(&claim.metadata.uid) == r.uid.as_ref()).then_some(claim)
    }

    // ---- run loop ------------------------------------------------------------

    fn queue_key(claim: &PersistentVolumeClaim) -> String {
        format!(
            "persistentvolumeclaims/{}/{}",
            claim.metadata.namespace.as_deref().unwrap_or(""),
            claim.metadata.name
        )
    }

    /// `syncClaimHandler` (lib controller.go:1063-1081): a key whose claim is
    /// gone is driven from `claimsInProgress`, else dropped.
    async fn sync_key(&self, key: &str) -> Result<()> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() != 3 {
            return Ok(());
        }
        let storage_key = build_key("persistentvolumeclaims", Some(parts[1]), parts[2]);
        let claim = match self
            .storage
            .get::<PersistentVolumeClaim>(&storage_key)
            .await
        {
            Ok(c) => c,
            Err(_) => {
                let in_progress = self
                    .claims_in_progress
                    .lock()
                    .unwrap()
                    .values()
                    .find(|c| Self::queue_key(c) == key)
                    .cloned();
                match in_progress {
                    Some(c) => c,
                    None => return Ok(()),
                }
            }
        };
        self.sync_claim(&claim)
            .await
            .with_context(|| format!("error syncing claim {key:?}"))
    }

    async fn worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_key(&key).await {
                // failedProvisionThreshold(0): retry without limit
                // (csi-provisioner.go:402).
                Err(e) => {
                    error!("{e:#}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
                Ok(()) => queue.forget(&key).await,
            }
            queue.done(&key).await;
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting CSI provisioner for driver {}", self.driver_name);
        let queue = WorkQueue::with_config(WorkQueueConfig {
            base_delay: RETRY_INTERVAL_START,
            max_delay: RETRY_INTERVAL_MAX,
        });
        for _ in 0..WORKERS {
            tokio::spawn(Arc::clone(&self).worker(queue.clone()));
        }
        let saver = Arc::clone(&self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                saver.save_unsaved_volumes().await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;
            let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", None);
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            let mut broken = false;
            while !broken {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => queue.add(extract_key(&ev)).await,
                        Some(Err(e)) => { warn!("Watch error: {e}, reconnecting"); broken = true; }
                        None => { warn!("Watch stream ended, reconnecting"); broken = true; }
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for c in &items {
                    queue.add(Self::queue_key(c)).await;
                }
            }
            Err(e) => error!("Failed to list persistentvolumeclaims: {e}"),
        }
    }
}

/// `NewItemExponentialFailureRateLimiter(5ms, 1000s)`: `base * 2^(n-1)`.
fn backoff(attempts: u32) -> Duration {
    PV_SAVE_BASE
        .checked_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
        .map_or(PV_SAVE_MAX, |d| d.min(PV_SAVE_MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use PersistentVolumeAccessMode::*;

    /// `access_modes_test.go`'s table, both mappings.
    #[test]
    fn access_modes_map_like_csi_lib_utils() {
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOnce], false),
            Ok(AccessModeKind::SingleNodeWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOnce], true),
            Ok(AccessModeKind::SingleNodeMultiWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOncePod], false),
            Ok(AccessModeKind::SingleNodeWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOncePod], true),
            Ok(AccessModeKind::SingleNodeSingleWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteMany, ReadOnlyMany], true),
            Ok(AccessModeKind::MultiNodeMultiWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadOnlyMany], false),
            Ok(AccessModeKind::MultiNodeReaderOnly)
        );
        assert!(to_csi_access_mode(&[ReadOnlyMany, ReadWriteOnce], false).is_err());
        assert!(to_csi_access_mode(&[ReadWriteOncePod, ReadWriteOnce], false).is_err());
        assert!(to_csi_access_mode(&[], false).is_err());
    }

    #[test]
    fn prefixed_parameters_are_stripped_and_unknown_ones_rejected() {
        let p: HashMap<String, String> = [
            ("type", "ssd"),
            ("csi.storage.k8s.io/fstype", "xfs"),
            ("csi.storage.k8s.io/provisioner-secret-name", "s"),
            ("csiFoo", "kept: not under the reserved prefix"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .into();
        let out = remove_prefixed_parameters(&p).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.contains_key("type") && out.contains_key("csiFoo"));
        let bad: HashMap<String, String> =
            [("csi.storage.k8s.io/bogus".to_string(), "x".to_string())].into();
        assert!(remove_prefixed_parameters(&bad)
            .unwrap_err()
            .contains("found unknown parameter key"));
    }

    #[test]
    fn volume_names_follow_make_volume_name() {
        assert_eq!(make_volume_name("pvc", "abc-1").unwrap(), "pvc-abc-1");
        assert!(make_volume_name("", "abc").is_err());
        assert!(make_volume_name("pvc", "").is_err());
    }

    #[test]
    fn pv_save_backoff_doubles_from_5ms_to_1000s() {
        assert_eq!(backoff(1), Duration::from_millis(5));
        assert_eq!(backoff(3), Duration::from_millis(20));
        assert_eq!(backoff(60), PV_SAVE_MAX);
    }
}
