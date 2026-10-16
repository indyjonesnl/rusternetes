//! The CSI half of the AttachDetachController (#3048): creates the
//! `VolumeAttachment` a scheduled pod's CSI volume needs and deletes it again
//! once nothing needs it. [`CsiAttacher`](super::csi_attacher) (the
//! external-attacher, #1460) is the consumer of these objects.
//!
//! Ported from:
//! - `pkg/volume/csi/csi_attacher.go` `Attach` (`:63-123`: name, create,
//!   AlreadyExists tolerated, source = PV name) and `Detach` (`:417-456`:
//!   delete by name, NotFound is done), `getAttachmentName` (`:593-596`);
//! - `pkg/volume/csi/csi_plugin.go` `CanAttach` (`:675-699`) and `skipAttach`
//!   (`:856-871`: no CSIDriver => attach; `attachRequired == false` => skip);
//! - `pkg/controller/volume/attachdetach/util/util.go` `ProcessPodVolumes`
//!   (`:194`), `getPVCFromCache` (bound + `volumeName`), `getPVSpecFromCache`
//!   (claimRef UID must match), generic-ephemeral claim name
//!   (`ephemeral.VolumeClaimName`), and `IsMultiAttachAllowed`
//!   (`pkg/volume/util/util.go:599`);
//! - `pkg/controller/volume/attachdetach/populator/desired_state_of_world_populator.go`
//!   `findAndAddActivePods` (`:179`: terminated pods are skipped);
//! - `.../reconciler/reconciler.go` `reconcile` (`:165`: detaches before
//!   attaches; a volume still mounted by the node, i.e. in
//!   `node.status.volumesInUse`, is not detached; a node unknown to the
//!   controller is ignored, `ProcessPodVolumes` `NodeExists`) and
//!   `attachDesiredVolumes` (`:319`: a single-attach volume attached to
//!   another node is not attached again).
//!
//! - `.../statusupdater/node_status_updater.go` `updateNodeStatus` (`:121-131`,
//!   `node.Status.VolumesAttached = attachedVolumes`) fed by
//!   `cache/actual_state_of_world.go` `getAttachedVolumeFromUpdateObject`
//!   (`:713-726`); `reconciler.go` `:245-265` removes the volume from the
//!   report and updates the node BEFORE detaching, skipping the detach when
//!   that update fails. Deviation: upstream replaces the whole list; this
//!   controller only owns the `kubernetes.io/csi/` entries and leaves any
//!   other entry alone. Upstream's `devicePath` is `""` for CSI (`Attach`
//!   returns `""`, `csi_attacher.go:138`).
//!
//! - force detach (`reconciler.go:205-260`): `SetDetachRequestTime` starts a
//!   per-attachment timer; a volume still in `volumesInUse` is detached anyway
//!   when the node is not Ready and `maxWaitForUnmountDuration` (default 6m,
//!   `attach_detach_controller.go:92`) expired (unless
//!   `disableForceDetachOnTimeout`), or the node has the
//!   `node.kubernetes.io/out-of-service` taint. Re-desired attached volumes
//!   reset the timer (`:342-346`). `verifySafeToDetach` has no equivalent
//!   here (`VerifyVolumesAreAttached` is not ported).
//!
//! - `.../reconciler/reconciler.go` `reportMultiAttachError` (`:376`), reported
//!   once per volume/node (`MultiAttachErrorReported`, `:352`, reset when the
//!   volume leaves the desired state of world), message from
//!   `operation_executor.go` `GenerateMsg` (`:361`) / `generateVolumeMsg`
//!   (`:320`); tests `reconciler_test.go` `Test_ReportMultiAttachError` (`:1279`).
//!
//! - CSI migration (#3801): `util.go` `CreateVolumeSpecWithNodeMigration` (`:108`),
//!   `translateInTreeSpecToCSIOnNodeIfNeeded` (`:251`), `isCSIMigrationSupportedOnNode`
//!   (`:292`); `attach_detach_controller.go:722-735`; `csi_attacher.go:84-103`.
//!
//! NOT ported (tracked on #3053): ephemeral
//! inline CSI volumes (never attachable, `CanAttach`).
//! Off by default; enabled with `--attach-detach-controller`.

use super::pv_binder::is_pod_terminated;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::csi_migration::{translate_in_tree_spec_to_csi, PluginManager, VolumeSpec};
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::resources::csi::{
    CSIDriver, CSINode, VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec,
};
use rusternetes_common::resources::node::AttachedVolume;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeSpec,
};
use rusternetes_common::resources::{EventSource, EventType};
use rusternetes_common::resources::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Resync when no event arrives (upstream `reconcilerLoopPeriod`, 100ms, is a
/// poll over an informer cache; this one lists storage, so it is coarser).
const RESYNC: Duration = Duration::from_secs(5);

/// `getAttachmentName` (`csi_attacher.go:593-596`).
pub fn attachment_name(volume_handle: &str, driver: &str, node: &str) -> String {
    let result = Sha256::digest(format!("{volume_handle}{driver}{node}").as_bytes());
    format!("csi-{result:x}")
}

/// `GetUniqueVolumeName(CSIPluginName, driver^handle)`
/// (`csi_plugin.go:54,57,437-445`): the key of `node.status.volumesInUse`.
fn unique_volume_name(driver: &str, handle: &str) -> String {
    format!("kubernetes.io/csi/{driver}^{handle}")
}

const CSI_PREFIX: &str = "kubernetes.io/csi/";

/// One CSI volume a pod needs attached to a node.
#[derive(Clone)]
struct Wanted {
    pv_name: String,
    driver: String,
    handle: String,
    node: String,
    /// `IsMultiAttachAllowed(spec)`.
    multi_attach: bool,
    /// `VolumeToAttach.ScheduledPods`: the pods that need this volume on `node`.
    scheduled_pods: Vec<ObjectReference>,
    /// `VolumeSpec.Name()` (the PV name), used in event messages.
    spec_name: String,
    /// `spec.InlineVolumeSpecForCSIMigration`: the translated inline volume's
    /// PV spec, which becomes the VolumeAttachment's `inlineVolumeSpec`
    /// (`csi_attacher.go:84-92`).
    inline_spec: Option<PersistentVolumeSpec>,
}

/// `v1.MigratedPluginsAnnotationKey` (`staging/src/k8s.io/api/core/v1/annotation_key_constants.go`).
const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

/// `isCSIMigrationSupportedOnNode` (`attachdetach/util/util.go:292-346`) after
/// the plugin name is known. `csi_node` is the node's CSINode (`None` = the
/// lister returned NotFound, which upstream returns as an error).
fn migration_supported_on_node(
    pm: &PluginManager,
    plugin_name: &str,
    csi_node: Option<&CSINode>,
) -> Result<bool, String> {
    if plugin_name.is_empty() {
        // Could not find a plugin name from translation directory, assume not translated
        return Ok(false);
    }
    if pm.is_migration_complete_for_plugin(plugin_name) {
        // All nodes are expected to have migrated CSI plugin installed and
        // configured when CSI Migration Complete flag is enabled for a plugin.
        return Ok(true);
    }
    let node = csi_node.ok_or_else(|| "csinode not found".to_string())?;
    let Some(ann) = node.metadata.annotations.as_ref() else {
        return Ok(false);
    };
    let mpa = ann
        .get(MIGRATED_PLUGINS_ANNOTATION_KEY)
        .map(String::as_str)
        .unwrap_or_default();
    Ok(mpa.split(',').any(|t| t == plugin_name))
}

/// `translateInTreeSpecToCSIOnNodeIfNeeded` (`attachdetach/util/util.go:251-272`).
fn translate_in_tree_spec_to_csi_on_node_if_needed(
    spec: VolumeSpec,
    csi_node: Option<&CSINode>,
    pm: &PluginManager,
    translator: &CsiTranslator,
    pod_namespace: &str,
) -> Result<VolumeSpec, String> {
    if !pm.is_migratable(Some(&spec))? {
        // Jump out of translation fast so we don't check the node if the spec itself is not migratable
        return Ok(spec);
    }
    let plugin_name = translator
        .get_in_tree_plugin_name_from_spec(spec.persistent_volume.as_ref(), spec.volume.as_ref())?;
    if migration_supported_on_node(pm, &plugin_name, csi_node)? {
        return translate_in_tree_spec_to_csi(&spec, pod_namespace, translator);
    }
    Ok(spec)
}

/// The CSI driver and volume handle of an attached volume.
struct CsiRef {
    driver: String,
    handle: String,
}

fn pod_ref(pod: &Pod) -> ObjectReference {
    ObjectReference {
        kind: Some("Pod".to_string()),
        namespace: pod.metadata.namespace.clone(),
        name: Some(pod.metadata.name.clone()),
        uid: Some(pod.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pod.metadata.resource_version.clone(),
        field_path: None,
    }
}

/// The event message each of `scheduled` (namespace, name) gets from
/// `reportMultiAttachError` (`reconciler.go:376-433`). `holders` are the
/// `(namespace, name)` of the pods using the volume on the other nodes
/// (`GetVolumePodsOnNodes`). Messages are built by `GenerateMsg`
/// (`operation_executor.go:361`, `generateVolumeMsg` `:320`).
fn multi_attach_messages(
    spec_name: &str,
    scheduled: &[(String, String)],
    holders: &[(String, String)],
) -> Vec<String> {
    let generate = |suffix: &str| format!("Multi-Attach error for volume \"{spec_name}\" {suffix}");
    if holders.is_empty() {
        // "We did not find any pods that requests the volume."
        let simple = generate(
            "Volume is already exclusively attached to one node and can't be attached to another",
        );
        return scheduled.iter().map(|_| simple.clone()).collect();
    }
    scheduled
        .iter()
        .map(|(ns, _)| {
            // Each pod gets a custom message: a namespace's user must not see
            // pod names from other namespaces.
            let local: Vec<&str> = holders
                .iter()
                .filter(|(n, _)| n == ns)
                .map(|(_, name)| name.as_str())
                .collect();
            let other = holders.len() - local.len();
            let msg = if !local.is_empty() {
                let mut m = format!("Volume is already used by pod(s) {}", local.join(", "));
                if other > 0 {
                    m = format!("{m} and {other} pod(s) in different namespaces");
                }
                m
            } else {
                format!("Volume is already used by {other} pod(s) in different namespaces")
            };
            generate(&msg)
        })
        .collect()
}

/// `IsMultiAttachAllowed` for a persistent volume (`util.go:615-628`).
fn multi_attach_allowed(pv: &PersistentVolume) -> bool {
    let modes = &pv.spec.access_modes;
    if modes.is_empty() {
        return true;
    }
    modes.iter().any(|m| {
        matches!(
            m,
            PersistentVolumeAccessMode::ReadWriteMany | PersistentVolumeAccessMode::ReadOnlyMany
        )
    })
}

/// `ReconcilerMaxWaitForUnmountDuration` default
/// (`attach_detach_controller.go:92`).
pub const DEFAULT_MAX_WAIT_FOR_UNMOUNT: Duration = Duration::from_secs(6 * 60);

pub struct AttachDetachController<S: Storage> {
    storage: Arc<S>,
    /// `maxWaitForUnmountDuration`.
    max_wait_for_unmount: Duration,
    /// `disableForceDetachOnTimeout`.
    disable_force_detach_on_timeout: bool,
    /// `actualStateOfWorld` `detachRequestedTime`, keyed by VolumeAttachment name.
    detach_requested: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    recorder: EventRecorder<S>,
    /// `MultiAttachErrorReported`: attachment names already reported. Pruned to
    /// the desired state each reconcile (upstream's flag lives on the DSW entry).
    multi_attach_reported: Mutex<HashSet<String>>,
    /// `csiMigratedPluginManager` (`attach_detach_controller.go`).
    plugin_manager: PluginManager,
    /// `intreeToCSITranslator`.
    translator: CsiTranslator,
}

impl<S: Storage + 'static> AttachDetachController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            multi_attach_reported: Mutex::new(HashSet::new()),
            max_wait_for_unmount: DEFAULT_MAX_WAIT_FOR_UNMOUNT,
            disable_force_detach_on_timeout: false,
            detach_requested: Default::default(),
            plugin_manager: PluginManager::new(),
            translator: CsiTranslator::new(),
        }
    }

    pub fn with_max_wait_for_unmount(mut self, d: Duration) -> Self {
        self.max_wait_for_unmount = d;
        self
    }

    pub fn with_disable_force_detach_on_timeout(mut self, v: bool) -> Self {
        self.disable_force_detach_on_timeout = v;
        self
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting AttachDetachController");
        loop {
            if let Err(e) = self.reconcile_all().await {
                warn!("AttachDetachController: reconcile failed: {e:#}");
            }
            let (mut pods, mut pvs, mut vas) = match (
                self.storage.watch(&build_prefix("pods", None)).await,
                self.storage
                    .watch(&build_prefix("persistentvolumes", None))
                    .await,
                self.storage
                    .watch(&build_prefix("volumeattachments", None))
                    .await,
            ) {
                (Ok(a), Ok(b), Ok(c)) => (a, b, c),
                _ => {
                    tokio::time::sleep(RESYNC).await;
                    continue;
                }
            };
            loop {
                let again = tokio::select! {
                    ev = pods.next() => matches!(ev, Some(Ok(_))),
                    ev = pvs.next() => matches!(ev, Some(Ok(_))),
                    ev = vas.next() => matches!(ev, Some(Ok(_))),
                    _ = tokio::time::sleep(RESYNC) => true,
                };
                if !again {
                    break;
                }
                if let Err(e) = self.reconcile_all().await {
                    warn!("AttachDetachController: reconcile failed: {e:#}");
                }
            }
        }
    }

    /// `ProcessPodVolumes` over every active pod: the desired state of world.
    #[allow(clippy::too_many_arguments)]
    fn desired(
        pods: &[Pod],
        pvcs: &[PersistentVolumeClaim],
        pvs: &HashMap<String, PersistentVolume>,
        drivers: &HashMap<String, CSIDriver>,
        nodes: &HashSet<String>,
        csi_nodes: &HashMap<String, CSINode>,
        pm: &PluginManager,
        translator: &CsiTranslator,
    ) -> HashMap<String, Wanted> {
        let mut out = HashMap::new();
        for pod in pods {
            // findAndAddActivePods: terminated pods add nothing.
            if is_pod_terminated(pod) {
                continue;
            }
            let node = pod.spec.as_ref().and_then(|s| s.node_name.clone());
            let Some(node) = node.filter(|n| !n.is_empty()) else {
                continue; // "it is not scheduled to a node"
            };
            if !nodes.contains(&node) {
                continue; // "not managed by the controller" (NodeExists)
            }
            let ns = pod.metadata.namespace.as_deref().unwrap_or_default();
            for vol in pod
                .spec
                .as_ref()
                .and_then(|s| s.volumes.as_ref())
                .into_iter()
                .flatten()
            {
                // createInTreeVolumeSpec (util.go:36-85).
                let (claim, read_only) = if let Some(p) = &vol.persistent_volume_claim {
                    (Some(p.claim_name.clone()), p.read_only.unwrap_or(false))
                } else if vol.ephemeral.is_some() {
                    // ephemeral.VolumeClaimName
                    (Some(format!("{}-{}", pod.metadata.name, vol.name)), false)
                } else {
                    (None, false)
                };
                let spec = match claim {
                    // "In-line volume": volume.NewSpecFromVolume.
                    None => VolumeSpec {
                        volume: Some(vol.clone()),
                        ..Default::default()
                    },
                    Some(claim) => {
                        let Some(pvc) = pvcs.iter().find(|c| {
                            c.metadata.name == claim && c.metadata.namespace.as_deref() == Some(ns)
                        }) else {
                            continue;
                        };
                        // getPVCFromCache: Bound phase and non-empty volumeName.
                        let bound = pvc.status.as_ref().map(|s| &s.phase)
                            == Some(&PersistentVolumeClaimPhase::Bound);
                        let Some(pv_name) = pvc.spec.volume_name.clone().filter(|n| !n.is_empty())
                        else {
                            continue;
                        };
                        if !bound {
                            continue;
                        }
                        let Some(pv) = pvs.get(&pv_name) else {
                            continue;
                        };
                        // getPVSpecFromCache: claimRef set and pointing at this claim.
                        let uid_ok = pv.spec.claim_ref.as_ref().is_some_and(|r| {
                            r.uid.as_deref().unwrap_or_default() == pvc.metadata.uid
                        });
                        if !uid_ok {
                            continue;
                        }
                        VolumeSpec {
                            persistent_volume: Some(pv.clone()),
                            read_only,
                            ..Default::default()
                        }
                    }
                };
                // CreateVolumeSpecWithNodeMigration (util.go:108-122): an
                // error skips the volume ("Error processing volume for pod").
                let spec = match translate_in_tree_spec_to_csi_on_node_if_needed(
                    spec,
                    csi_nodes.get(&node),
                    pm,
                    translator,
                    ns,
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        debug!(
                            "Error processing volume {} for pod {}/{ns}: {e}",
                            vol.name, pod.metadata.name
                        );
                        continue;
                    }
                };
                // FindAttachablePluginBySpec: only a CSI PV is attachable (an
                // inline CSI volume is ephemeral, `CanAttach` is false).
                let Some(pv) = spec.persistent_volume.as_ref() else {
                    continue;
                };
                let pv_name = pv.metadata.name.clone();
                let Some(csi) = pv.spec.csi.as_ref() else {
                    continue; // not a CSI volume
                };
                // CanAttach / skipAttach: only an explicit false skips.
                if drivers
                    .get(&csi.driver)
                    .and_then(|d| d.spec.attach_required)
                    == Some(false)
                {
                    continue;
                }
                let handle = csi.volume_handle.clone().unwrap_or_default();
                let w = out
                    .entry(attachment_name(&handle, &csi.driver, &node))
                    .or_insert_with(|| Wanted {
                        spec_name: pv_name.clone(),
                        inline_spec: spec
                            .inline_volume_spec_for_csi_migration
                            .then(|| pv.spec.clone()),
                        pv_name,
                        driver: csi.driver.clone(),
                        handle,
                        node: node.clone(),
                        multi_attach: multi_attach_allowed(pv),
                        scheduled_pods: vec![],
                    });
                w.scheduled_pods.push(pod_ref(pod));
            }
        }
        out
    }

    /// `nodeStatusUpdater.updateNodeStatus`: make the CSI entries of
    /// `node.status.volumesAttached` equal `report`. No-op when already equal
    /// (`PatchNodeStatus` sends nothing for an empty diff); a missing node is
    /// not an error (`processNodeVolumes`, `:87-92`).
    async fn update_node_status(&self, node_name: &str, report: &BTreeSet<String>) -> Result<()> {
        let key = build_key("nodes", None, node_name);
        let node: Node = match self.storage.get(&key).await {
            Ok(n) => n,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let current = node
            .status
            .as_ref()
            .and_then(|s| s.volumes_attached.clone())
            .unwrap_or_default();
        let mut want: Vec<AttachedVolume> = current
            .iter()
            .filter(|v| !v.name.starts_with(CSI_PREFIX))
            .cloned()
            .collect();
        want.extend(report.iter().map(|n| AttachedVolume {
            name: n.clone(),
            device_path: String::new(),
        }));
        let same = want.len() == current.len()
            && want
                .iter()
                .all(|w| current.iter().any(|c| c.name == w.name));
        if same {
            return Ok(());
        }
        let list = serde_json::to_value(&want)?;
        let patch = serde_json::json!({"status": {
            "volumesAttached": if want.is_empty() { serde_json::Value::Null } else { list }
        }});
        let mut full = node;
        full.status
            .get_or_insert_with(Default::default)
            .volumes_attached = if want.is_empty() { None } else { Some(want) };
        match self
            .storage
            .patch_status_strategic_merge::<Node>(&key, &patch, &full)
            .await
        {
            Ok(_) | Err(Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// One pass of `reconciler.reconcile`: detaches, then attaches.
    /// `reportMultiAttachError` (`reconciler.go:376-433`): a Warning
    /// `FailedAttachVolume` event on every pod waiting for `w`, naming the pods
    /// that hold the volume on `other_nodes`.
    async fn report_multi_attach_error(
        &self,
        w: &Wanted,
        other_nodes: &[&str],
        desired: &HashMap<String, Wanted>,
    ) {
        // GetVolumePodsOnNodes(otherNodes, volumeName).
        let mut holders: Vec<(String, String)> = vec![];
        for node in other_nodes {
            for other in desired.values() {
                if other.pv_name == w.pv_name && other.node == *node {
                    for p in &other.scheduled_pods {
                        holders.push((
                            p.namespace.clone().unwrap_or_default(),
                            p.name.clone().unwrap_or_default(),
                        ));
                    }
                }
            }
        }
        let scheduled: Vec<(String, String)> = w
            .scheduled_pods
            .iter()
            .map(|p| {
                (
                    p.namespace.clone().unwrap_or_default(),
                    p.name.clone().unwrap_or_default(),
                )
            })
            .collect();
        let msgs = multi_attach_messages(&w.spec_name, &scheduled, &holders);
        let source = EventSource {
            component: "attachdetach-controller".to_string(),
            host: None,
        };
        for (pod, msg) in w.scheduled_pods.iter().zip(msgs) {
            if let Err(e) = self
                .recorder
                .event(pod, &source, EventType::Warning, "FailedAttachVolume", &msg)
                .await
            {
                warn!("Failed to record FailedAttachVolume event: {e}");
            }
        }
        // "Log all pods for system admin."
        warn!(
            "Multi-Attach error: volume {} is already attached to {:?}, used by pods {:?}",
            w.spec_name, other_nodes, holders
        );
    }

    /// The CSI source an existing VolumeAttachment points at.
    /// `populateActualStateOfWorld` (`attach_detach_controller.go:700-736`):
    /// the PV is looked up by `source.persistentVolumeName`, and a PV whose
    /// in-tree plugin is migrated is translated to CSI first
    /// (`:722-735`, `podNamespace` is empty); a failed translation skips the
    /// VolumeAttachment. Deviation: upstream skips inline-source
    /// VolumeAttachments (its actual state of world is in memory and already
    /// holds them); this controller is stateless, so it reads the CSI source
    /// off `source.inlineVolumeSpec` to be able to detach them.
    fn va_csi_ref(
        &self,
        va: &VolumeAttachment,
        pvs: &HashMap<String, PersistentVolume>,
    ) -> Option<CsiRef> {
        if let Some(inline) = va.spec.source.inline_volume_spec.as_ref() {
            let csi = inline.csi.as_ref()?;
            return Some(CsiRef {
                driver: csi.driver.clone(),
                handle: csi.volume_handle.clone().unwrap_or_default(),
            });
        }
        let pv = pvs.get(va.spec.source.persistent_volume_name.as_ref()?)?;
        let mut spec = VolumeSpec {
            persistent_volume: Some(pv.clone()),
            ..Default::default()
        };
        if let Ok(name) = self
            .translator
            .get_in_tree_plugin_name_from_spec(Some(pv), None)
        {
            if self.plugin_manager.is_migration_enabled_for_plugin(&name) {
                spec = match translate_in_tree_spec_to_csi(&spec, "", &self.translator) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            "Failed to translate intree volumeSpec to CSI volumeSpec for volume {name}: VolumeAttachment {}: {e}",
                            va.metadata.name
                        );
                        return None;
                    }
                };
            }
        }
        let csi = spec.persistent_volume?.spec.csi?;
        Some(CsiRef {
            driver: csi.driver,
            handle: csi.volume_handle.unwrap_or_default(),
        })
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        let pods: Vec<Pod> = self.storage.list(&build_prefix("pods", None)).await?;
        let pvcs: Vec<PersistentVolumeClaim> = self
            .storage
            .list(&build_prefix("persistentvolumeclaims", None))
            .await?;
        let pvs: HashMap<String, PersistentVolume> = self
            .storage
            .list::<PersistentVolume>(&build_prefix("persistentvolumes", None))
            .await?
            .into_iter()
            .map(|p| (p.metadata.name.clone(), p))
            .collect();
        let drivers: HashMap<String, CSIDriver> = self
            .storage
            .list::<CSIDriver>(&build_prefix("csidrivers", None))
            .await?
            .into_iter()
            .map(|d| (d.metadata.name.clone(), d))
            .collect();
        let csi_nodes: HashMap<String, CSINode> = self
            .storage
            .list::<CSINode>(&build_prefix("csinodes", None))
            .await?
            .into_iter()
            .map(|n| (n.metadata.name.clone(), n))
            .collect();
        let node_list: Vec<Node> = self.storage.list(&build_prefix("nodes", None)).await?;
        let nodes: HashSet<String> = node_list.iter().map(|n| n.metadata.name.clone()).collect();
        let vas: Vec<VolumeAttachment> = self
            .storage
            .list(&build_prefix("volumeattachments", None))
            .await?;

        let desired = Self::desired(
            &pods,
            &pvcs,
            &pvs,
            &drivers,
            &nodes,
            &csi_nodes,
            &self.plugin_manager,
            &self.translator,
        );

        // Actual state: the VolumeAttachments this controller owns, i.e. whose
        // name is what Attach() would have generated for their own source.
        let mut attached: Vec<(&VolumeAttachment, CsiRef)> = vec![];
        for va in &vas {
            let Some(r) = self.va_csi_ref(va, &pvs) else {
                continue;
            };
            if va.metadata.name != attachment_name(&r.handle, &r.driver, &va.spec.node_name) {
                continue;
            }
            attached.push((va, r));
        }

        // The volumes each node reports as attached
        // (`GetVolumesToReportAttached`): every owned attachment not already
        // being deleted.
        let mut report: HashMap<String, BTreeSet<String>> = HashMap::new();
        for (va, r) in &attached {
            if va.metadata.deletion_timestamp.is_none() {
                report
                    .entry(va.spec.node_name.clone())
                    .or_default()
                    .insert(unique_volume_name(&r.driver, &r.handle));
            }
        }

        // Detach first (reconciler.go:167-170).
        for (va, r) in &attached {
            if desired.contains_key(&va.metadata.name) || va.metadata.deletion_timestamp.is_some() {
                continue;
            }
            // SetDetachRequestTime (reconciler.go:205): first request starts
            // the timer, later ones return the elapsed time.
            let elapsed = {
                let mut m = self.detach_requested.lock().unwrap();
                m.entry(va.metadata.name.clone())
                    .or_insert_with(std::time::Instant::now)
                    .elapsed()
            };
            let max_wait_expired = elapsed > self.max_wait_for_unmount;
            let node = node_list
                .iter()
                .find(|n| n.metadata.name == va.spec.node_name);
            // nodeIsHealthy (:151-157): util/node.IsNodeReady; a node that
            // cannot be read is not healthy.
            let healthy = node.is_some_and(|n| {
                n.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .and_then(|c| c.iter().find(|c| c.condition_type == "Ready"))
                    .is_some_and(|c| c.status == "True")
            });
            // :223-231
            let force_timeout_expired = max_wait_expired && !self.disable_force_detach_on_timeout;
            let force_detach = !healthy && force_timeout_expired;
            // hasOutOfServiceTaint (:143-149), v1.TaintNodeOutOfService.
            let out_of_service = node
                .and_then(|n| n.spec.as_ref())
                .and_then(|s| s.taints.as_ref())
                .is_some_and(|t| {
                    t.iter()
                        .any(|t| t.key == "node.kubernetes.io/out-of-service")
                });
            // MountedByNode: node.status.volumesInUse (reconciler.go:231-239).
            let unique = unique_volume_name(&r.driver, &r.handle);
            let mounted = node
                .and_then(|n| n.status.as_ref())
                .and_then(|s| s.volumes_in_use.as_ref())
                .is_some_and(|u| u.contains(&unique));
            if mounted && !force_detach && !out_of_service {
                debug!(
                    "Cannot detach volume {unique} from {}: still mounted",
                    va.spec.node_name
                );
                continue;
            }
            if mounted {
                info!(
                    "force detaching volume {unique} from {}: {}",
                    va.spec.node_name,
                    if force_detach {
                        "maxWaitForUnmountDuration expired on an unhealthy node"
                    } else {
                        "node has out-of-service taint"
                    }
                );
            }
            // reconciler.go:245-265: drop the volume from the report and
            // update the node first; if that fails skip the detach and keep
            // reporting it.
            let node_report = report.entry(va.spec.node_name.clone()).or_default();
            node_report.remove(&unique);
            let snapshot = node_report.clone();
            if let Err(e) = self.update_node_status(&va.spec.node_name, &snapshot).await {
                warn!(
                    "UpdateNodeStatusForNode failed for {}: {e:#}",
                    va.spec.node_name
                );
                report
                    .entry(va.spec.node_name.clone())
                    .or_default()
                    .insert(unique);
                continue;
            }
            // Detach (csi_attacher.go:442): delete; NotFound is done.
            self.storage
                .delete_gracefully(&build_key("volumeattachments", None, &va.metadata.name))
                .await?;
            self.detach_requested
                .lock()
                .unwrap()
                .remove(&va.metadata.name);
            info!(
                "deleted VolumeAttachment {} (volume {unique}, node {})",
                va.metadata.name, va.spec.node_name
            );
        }

        // attachDesiredVolumes (reconciler.go:319).
        let existing: HashSet<&str> = vas.iter().map(|v| v.metadata.name.as_str()).collect();
        let mut attached_on: HashMap<String, Vec<&str>> = HashMap::new();
        for (va, r) in &attached {
            attached_on
                .entry(unique_volume_name(&r.driver, &r.handle))
                .or_default()
                .push(va.spec.node_name.as_str());
        }
        // The flag lives on the DSW entry upstream: it goes when the volume does.
        self.multi_attach_reported
            .lock()
            .unwrap()
            .retain(|n| desired.contains_key(n));
        let mut names: Vec<_> = desired.keys().collect();
        names.sort();
        for name in names {
            let w = &desired[name];
            if existing.contains(name.as_str()) {
                // AttachStateAttached: "touch it to reset detachRequestedTime"
                // (reconciler.go:342-346).
                self.detach_requested.lock().unwrap().remove(name);
                continue;
            }
            if !w.multi_attach
                && attached_on
                    .get(&unique_volume_name(&w.driver, &w.handle))
                    .is_some_and(|n| n.iter().any(|n| *n != w.node))
            {
                let first = self
                    .multi_attach_reported
                    .lock()
                    .unwrap()
                    .insert(name.clone());
                if first {
                    let mut other_nodes: Vec<&str> = attached_on
                        .get(&unique_volume_name(&w.driver, &w.handle))
                        .map(|n| n.iter().copied().filter(|n| *n != w.node).collect())
                        .unwrap_or_default();
                    other_nodes.sort();
                    self.report_multi_attach_error(w, &other_nodes, &desired)
                        .await;
                }
                continue;
            }
            let va = VolumeAttachment {
                type_meta: TypeMeta {
                    kind: "VolumeAttachment".to_string(),
                    api_version: "storage.k8s.io/v1".to_string(),
                },
                metadata: ObjectMeta::new(name.clone()),
                spec: VolumeAttachmentSpec {
                    attacher: w.driver.clone(),
                    node_name: w.node.clone(),
                    source: VolumeAttachmentSource {
                        // csi_attacher.go:84-103: an inline volume migrated to
                        // CSI carries its translated PV spec, a PV its name.
                        persistent_volume_name: w.inline_spec.is_none().then(|| w.pv_name.clone()),
                        inline_volume_spec: w.inline_spec.clone(),
                    },
                },
                status: None,
            };
            match self
                .storage
                .create(&build_key("volumeattachments", None, name), &va)
                .await
            {
                Ok(_) => info!(
                    "created VolumeAttachment {name} (volume {}, node {})",
                    w.handle, w.node
                ),
                // csi_attacher.go:111-113: already exists, not recreated.
                Err(Error::AlreadyExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
            // MarkVolumeAsAttached once Attach() returned.
            report
                .entry(w.node.clone())
                .or_default()
                .insert(unique_volume_name(&w.driver, &w.handle));
        }

        // UpdateNodeStatuses: every node we report for, plus any node still
        // carrying CSI entries we no longer report.
        let mut failed = 0;
        for node in &node_list {
            let name = &node.metadata.name;
            let has_csi = node
                .status
                .as_ref()
                .and_then(|s| s.volumes_attached.as_ref())
                .is_some_and(|v| v.iter().any(|v| v.name.starts_with(CSI_PREFIX)));
            let empty = BTreeSet::new();
            let r = report.get(name).unwrap_or(&empty);
            if r.is_empty() && !has_csi {
                continue;
            }
            if let Err(e) = self.update_node_status(name, r).await {
                warn!("could not update volumesAttached of node {name}: {e:#}");
                failed += 1;
            }
        }
        if failed > 0 {
            anyhow::bail!("unable to update {failed} nodes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        migration_supported_on_node, multi_attach_messages, MIGRATED_PLUGINS_ANNOTATION_KEY,
    };
    use rusternetes_common::csi_migration::PluginManager;
    use rusternetes_common::resources::csi::CSINode;

    fn csi_node(ann: Option<&str>) -> CSINode {
        let mut n: CSINode = serde_json::from_value(
            serde_json::json!({"metadata": {"name": "n"}, "spec": {"drivers": []}}),
        )
        .unwrap();
        if let Some(a) = ann {
            n.metadata.annotations =
                Some([(MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(), a.to_string())].into());
        }
        n
    }

    /// `isCSIMigrationSupportedOnNode` (`attachdetach/util/util.go:292-346`):
    /// complete plugins need no CSINode, others consult the annotation, a
    /// missing CSINode is an error.
    #[test]
    fn migration_supported_on_node_matches_upstream() {
        let pm = PluginManager::new();
        let gce = "kubernetes.io/gce-pd";
        assert_eq!(migration_supported_on_node(&pm, gce, None), Ok(true));
        assert_eq!(migration_supported_on_node(&pm, "", None), Ok(false));
        // Portworx migration enabled but not complete: the CSINode decides.
        let px = "kubernetes.io/portworx-volume";
        let mut pm = PluginManager::new();
        pm.csi_migration_portworx = true;
        assert!(migration_supported_on_node(&pm, px, None).is_err());
        assert_eq!(
            migration_supported_on_node(&pm, px, Some(&csi_node(None))),
            Ok(false)
        );
        assert_eq!(
            migration_supported_on_node(
                &pm,
                px,
                Some(&csi_node(Some("a,kubernetes.io/portworx-volume")))
            ),
            Ok(true)
        );
        assert_eq!(
            migration_supported_on_node(&pm, px, Some(&csi_node(Some("kubernetes.io/aws-ebs")))),
            Ok(false)
        );
    }

    /// `Test_ReportMultiAttachError` (`reconciler_test.go:1279`): ns1/pod1 is
    /// the pod being attached, `others` use the volume on other nodes.
    fn run(others: &[(&str, &str)]) -> Vec<String> {
        let holders: Vec<(String, String)> = others
            .iter()
            .map(|(n, p)| (n.to_string(), p.to_string()))
            .collect();
        multi_attach_messages("volume-name", &[("ns1".into(), "pod1".into())], &holders)
    }

    const PFX: &str = "Multi-Attach error for volume \"volume-name\" ";

    #[test]
    fn no_pods_use_the_volume() {
        assert_eq!(
            run(&[]),
            vec![format!("{PFX}Volume is already exclusively attached to one node and can't be attached to another")]
        );
    }

    #[test]
    fn pods_in_the_same_namespace() {
        assert_eq!(
            run(&[("ns1", "pod2")]),
            vec![format!("{PFX}Volume is already used by pod(s) pod2")]
        );
    }

    #[test]
    fn pods_in_another_namespace() {
        assert_eq!(
            run(&[("ns2", "pod2")]),
            vec![format!(
                "{PFX}Volume is already used by 1 pod(s) in different namespaces"
            )]
        );
    }

    #[test]
    fn pods_in_same_and_another_namespace() {
        assert_eq!(
            run(&[("ns2", "pod2"), ("ns1", "pod3")]),
            vec![format!(
                "{PFX}Volume is already used by pod(s) pod3 and 1 pod(s) in different namespaces"
            )]
        );
    }
}
