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
//! NOT ported (tracked on #3048): `node.status.volumesAttached` updates
//! (`statusupdater`), force-detach on unhealthy node / `out-of-service` taint,
//! the multi-attach event, CSI migration of in-tree volumes, ephemeral inline
//! CSI volumes (never attachable, `CanAttach`), `maxWaitForUnmountDuration`.
//! Off by default; enabled with `--attach-detach-controller`.

use super::pv_binder::is_pod_terminated;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::csi::{
    CSIDriver, VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec,
};
use rusternetes_common::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaimPhase,
};
use rusternetes_common::resources::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, Storage};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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

/// One CSI volume a pod needs attached to a node.
#[derive(Clone)]
struct Wanted {
    pv_name: String,
    driver: String,
    handle: String,
    node: String,
    /// `IsMultiAttachAllowed(spec)`.
    multi_attach: bool,
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

pub struct AttachDetachController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> AttachDetachController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
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
    fn desired(
        pods: &[Pod],
        pvcs: &[PersistentVolumeClaim],
        pvs: &HashMap<String, PersistentVolume>,
        drivers: &HashMap<String, CSIDriver>,
        nodes: &HashSet<String>,
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
                let claim = if let Some(p) = &vol.persistent_volume_claim {
                    p.claim_name.clone()
                } else if vol.ephemeral.is_some() {
                    // ephemeral.VolumeClaimName
                    format!("{}-{}", pod.metadata.name, vol.name)
                } else {
                    continue; // inline CSI is never attachable (CanAttach)
                };
                let Some(pvc) = pvcs.iter().find(|c| {
                    c.metadata.name == claim && c.metadata.namespace.as_deref() == Some(ns)
                }) else {
                    continue;
                };
                // getPVCFromCache: Bound phase and non-empty volumeName.
                let bound = pvc.status.as_ref().map(|s| &s.phase)
                    == Some(&PersistentVolumeClaimPhase::Bound);
                let Some(pv_name) = pvc.spec.volume_name.clone().filter(|n| !n.is_empty()) else {
                    continue;
                };
                if !bound {
                    continue;
                }
                let Some(pv) = pvs.get(&pv_name) else {
                    continue;
                };
                // getPVSpecFromCache: claimRef set and pointing at this claim.
                let uid_ok = pv
                    .spec
                    .claim_ref
                    .as_ref()
                    .is_some_and(|r| r.uid.as_deref().unwrap_or_default() == pvc.metadata.uid);
                if !uid_ok {
                    continue;
                }
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
                out.insert(
                    attachment_name(&handle, &csi.driver, &node),
                    Wanted {
                        pv_name,
                        driver: csi.driver.clone(),
                        handle,
                        node: node.clone(),
                        multi_attach: multi_attach_allowed(pv),
                    },
                );
            }
        }
        out
    }

    /// One pass of `reconciler.reconcile`: detaches, then attaches.
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
        let node_list: Vec<Node> = self.storage.list(&build_prefix("nodes", None)).await?;
        let nodes: HashSet<String> = node_list.iter().map(|n| n.metadata.name.clone()).collect();
        let vas: Vec<VolumeAttachment> = self
            .storage
            .list(&build_prefix("volumeattachments", None))
            .await?;

        let desired = Self::desired(&pods, &pvcs, &pvs, &drivers, &nodes);

        // Actual state: the VolumeAttachments this controller owns, i.e. whose
        // name is what Attach() would have generated for their own source.
        let mut attached: Vec<(&VolumeAttachment, &PersistentVolume, String)> = vec![];
        for va in &vas {
            let Some(pv) = va
                .spec
                .source
                .persistent_volume_name
                .as_ref()
                .and_then(|n| pvs.get(n))
            else {
                continue;
            };
            let Some(csi) = pv.spec.csi.as_ref() else {
                continue;
            };
            let handle = csi.volume_handle.clone().unwrap_or_default();
            if va.metadata.name != attachment_name(&handle, &csi.driver, &va.spec.node_name) {
                continue;
            }
            attached.push((va, pv, handle));
        }

        // Detach first (reconciler.go:167-170).
        for (va, pv, handle) in &attached {
            if desired.contains_key(&va.metadata.name) || va.metadata.deletion_timestamp.is_some() {
                continue;
            }
            // MountedByNode: node.status.volumesInUse (reconciler.go:231).
            let driver = pv.spec.csi.as_ref().map_or("", |c| c.driver.as_str());
            let unique = unique_volume_name(driver, handle);
            let mounted = node_list
                .iter()
                .find(|n| n.metadata.name == va.spec.node_name)
                .and_then(|n| n.status.as_ref())
                .and_then(|s| s.volumes_in_use.as_ref())
                .is_some_and(|u| u.contains(&unique));
            if mounted {
                debug!(
                    "Cannot detach volume {unique} from {}: still mounted",
                    va.spec.node_name
                );
                continue;
            }
            // Detach (csi_attacher.go:442): delete; NotFound is done.
            self.storage
                .delete_gracefully(&build_key("volumeattachments", None, &va.metadata.name))
                .await?;
            info!(
                "deleted VolumeAttachment {} (volume {unique}, node {})",
                va.metadata.name, va.spec.node_name
            );
        }

        // attachDesiredVolumes (reconciler.go:319).
        let existing: HashSet<&str> = vas.iter().map(|v| v.metadata.name.as_str()).collect();
        let mut attached_on: HashMap<&str, Vec<&str>> = HashMap::new();
        for (va, pv, _) in &attached {
            attached_on
                .entry(pv.metadata.name.as_str())
                .or_default()
                .push(va.spec.node_name.as_str());
        }
        let mut names: Vec<_> = desired.keys().collect();
        names.sort();
        for name in names {
            let w = &desired[name];
            if existing.contains(name.as_str()) {
                continue;
            }
            if !w.multi_attach
                && attached_on
                    .get(w.pv_name.as_str())
                    .is_some_and(|n| n.iter().any(|n| *n != w.node))
            {
                warn!(
                    "Multi-Attach: volume {} is already attached to another node, not attaching to {}",
                    w.pv_name, w.node
                );
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
                        persistent_volume_name: Some(w.pv_name.clone()),
                        inline_volume_spec: None,
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
        }
        Ok(())
    }
}
