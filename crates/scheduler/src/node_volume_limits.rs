//! NodeVolumeLimits (`CSILimits`) scheduler plugin: PreFilter + Filter.
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/nodevolumelimits/{csi.go,utils.go}`.
//! Not yet ported: the `EventsToRegister` queueing hints (`csi.go:86-215`).

use crate::volume_binding::{
    claim_class, ephemeral_volume_is_for_pod, pod_volumes, volume_pvc_name,
};
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::resources::{
    CSIDriver, CSINode, Node, PersistentVolume, PersistentVolumeClaim, Pod, StorageClass, Volume,
    VolumeAttachment,
};
use std::collections::HashMap;

/// `ErrReasonMaxVolumeCountExceeded` (`csi.go:44`).
pub const ERR_REASON_MAX_VOLUME_COUNT_EXCEEDED: &str = "node(s) exceed max volume count";

/// `v1.MigratedPluginsAnnotationKey`.
pub const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

/// The listers `CSILimits` reads (`pvLister`, `pvcLister`, `scLister`,
/// `vaLister`, `csiDriverLister` and the `csiManager` CSINode lister). Unlike
/// the VolumeBinding snapshot, PVCs are cluster-wide: existing pods on the node
/// may live in any namespace.
#[derive(Default, Clone)]
pub struct NodeVolumeLimitsSnapshot {
    pub pvcs: Vec<PersistentVolumeClaim>,
    pub pvs: Vec<PersistentVolume>,
    pub classes: Vec<StorageClass>,
    pub csi_drivers: Vec<CSIDriver>,
    pub csi_nodes: Vec<CSINode>,
    pub volume_attachments: Vec<VolumeAttachment>,
}

/// The `*fwk.Status` codes `Filter` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterStatus {
    Success,
    Unschedulable(String),
    UnschedulableAndUnresolvable(String),
    Error(String),
}

/// An error from `filterAttachableVolumes`; `NotFound` is
/// `apierrors.IsNotFound`.
enum LimitsError {
    NotFound(String),
    Other(String),
}

pub struct CsiLimits<'a> {
    pub snap: &'a NodeVolumeLimitsSnapshot,
    pub enable_csi_migration_portworx: bool,
    pub enable_volume_limit_scaling: bool,
    pub random_volume_id_prefix: String,
    pub translator: CsiTranslator,
}

/// `PreFilter` without the lister snapshot (`csi.go:219-229`), so the caller
/// can skip listing volume objects for a pod that would be `Skip`ped.
pub fn pod_needs_volume_limits(pod: &Pod) -> bool {
    let translator = CsiTranslator::new();
    pod_volumes(pod).iter().any(|vol| {
        vol.persistent_volume_claim.is_some()
            || vol.ephemeral.is_some()
            || translator.is_inline_migratable(vol)
    })
}

impl<'a> CsiLimits<'a> {
    pub fn new(snap: &'a NodeVolumeLimitsSnapshot) -> Self {
        Self {
            snap,
            enable_csi_migration_portworx: false,
            // `fts.EnableVolumeLimitScaling` (plugins/feature/feature.go).
            enable_volume_limit_scaling: rusternetes_common::feature_gates::gate_enabled(
                "VolumeLimitScaling",
            )
            .unwrap_or(false),
            // `rand.String(32)` (csi.go NewCSI).
            random_volume_id_prefix: uuid::Uuid::new_v4().simple().to_string(),
            translator: CsiTranslator::new(),
        }
    }

    fn csi_node(&self, name: &str) -> Option<&CSINode> {
        self.snap.csi_nodes.iter().find(|n| n.metadata.name == name)
    }
    fn pvc(&self, ns: &str, name: &str) -> Option<&PersistentVolumeClaim> {
        self.snap.pvcs.iter().find(|c| {
            c.metadata.name == name && c.metadata.namespace.as_deref().unwrap_or("") == ns
        })
    }
    fn pv(&self, name: &str) -> Option<&PersistentVolume> {
        self.snap.pvs.iter().find(|v| v.metadata.name == name)
    }

    /// `PreFilter` (`csi.go:219-229`): `false` is `Skip` ("If the pod haven't
    /// those types of volumes, we'll skip the Filter phase").
    pub fn pre_filter(pod: &Pod) -> bool {
        pod_needs_volume_limits(pod)
    }

    /// `Filter` (`csi.go:236-345`). `existing_pods` is `nodeInfo.GetPods()`.
    pub fn filter(&self, pod: &Pod, node: &Node, existing_pods: &[&Pod]) -> FilterStatus {
        // If the new pod doesn't have any volume attached to it, the predicate
        // will always be true.
        if pod_volumes(pod).is_empty() {
            return FilterStatus::Success;
        }
        // A missing CSINode is only logged upstream (csi.go:249-252).
        let csi_node = self.csi_node(&node.metadata.name);

        // Count CSI volumes from the new pod.
        let mut new_volumes: HashMap<String, String> = HashMap::new();
        if let Err(e) = self.filter_attachable_volumes(pod, csi_node, true, &mut new_volumes) {
            return match e {
                // PVC is not found. This Pod will never be schedulable until
                // PVC is created.
                LimitsError::NotFound(m) => FilterStatus::UnschedulableAndUnresolvable(m),
                LimitsError::Other(m) => FilterStatus::Error(m),
            };
        }
        // If the pod doesn't have any new CSI volumes, the predicate will
        // always be true.
        if new_volumes.is_empty() {
            return FilterStatus::Success;
        }

        if self.enable_volume_limit_scaling {
            for driver_name in new_volumes.values() {
                if !self.check_csi_driver_on_node(driver_name, csi_node) {
                    return FilterStatus::Unschedulable(format!(
                        "{driver_name} CSI driver is not installed on the node"
                    ));
                }
            }
        }

        // If the node doesn't have volume limits, the predicate will always be
        // true.
        let node_volume_limits = get_volume_limits(csi_node);
        if node_volume_limits.is_empty() {
            return FilterStatus::Success;
        }

        // Count CSI volumes from existing pods.
        let mut attached_volumes: HashMap<String, String> = HashMap::new();
        for existing in existing_pods {
            if let Err(e) =
                self.filter_attachable_volumes(existing, csi_node, false, &mut attached_volumes)
            {
                return FilterStatus::Error(match e {
                    LimitsError::NotFound(m) | LimitsError::Other(m) => m,
                });
            }
        }

        let mut attached_volume_count: HashMap<&str, i64> = HashMap::new();
        for (unique_name, driver_name) in &attached_volumes {
            // Don't count single volume used in multiple pods more than once.
            new_volumes.remove(unique_name);
            *attached_volume_count.entry(driver_name).or_default() += 1;
        }

        // Count CSI volumes from VolumeAttachments.
        let volume_attachments = self.get_node_volume_attachment_info(&node.metadata.name);
        for (unique_name, driver_name) in &volume_attachments {
            // Avoid double-counting volumes already used by existing pods.
            if !attached_volumes.contains_key(unique_name) {
                *attached_volume_count.entry(driver_name).or_default() += 1;
            }
        }

        // Count the new volumes count per driver.
        let mut new_volume_count: HashMap<&str, i64> = HashMap::new();
        for driver_name in new_volumes.values() {
            *new_volume_count.entry(driver_name).or_default() += 1;
        }

        for (driver_name, count) in new_volume_count {
            if let Some(max) = node_volume_limits.get(driver_name) {
                let current = attached_volume_count.get(driver_name).copied().unwrap_or(0);
                if current + count > *max {
                    return FilterStatus::Unschedulable(
                        ERR_REASON_MAX_VOLUME_COUNT_EXCEEDED.to_string(),
                    );
                }
            }
        }
        FilterStatus::Success
    }

    /// `checkCSIDriverOnNode` (`csi.go:347-369`).
    fn check_csi_driver_on_node(&self, plugin_name: &str, csi_node: Option<&CSINode>) -> bool {
        // The registered driver must be a CSI driver to enforce this limit; if
        // we can't find the driver, we assume it may not be a CSI driver and
        // allow the pod to be scheduled.
        if !self
            .snap
            .csi_drivers
            .iter()
            .any(|d| d.metadata.name == plugin_name)
        {
            return true;
        }
        csi_node.is_some_and(|n| n.spec.drivers.iter().any(|d| d.name == plugin_name))
    }

    /// `filterAttachableVolumes` (`csi.go:371-441`): adds `driverName/volumeHandle
    /// -> driverName` for the pod's attachable volumes to `result`.
    fn filter_attachable_volumes(
        &self,
        pod: &Pod,
        csi_node: Option<&CSINode>,
        new_pod: bool,
        result: &mut HashMap<String, String>,
    ) -> Result<(), LimitsError> {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        for vol in pod_volumes(pod) {
            let Some((pvc_name, is_ephemeral)) = volume_pvc_name(pod, vol) else {
                // Inline volume does not have a PVC: count it if it is
                // migratable and CSI migration is enabled; otherwise it is
                // counted in the non_csi filter.
                self.check_attachable_inline_volume(vol, csi_node, pod, result)?;
                continue;
            };
            if pvc_name.is_empty() {
                return Err(LimitsError::Other(
                    "PersistentVolumeClaim had no name".into(),
                ));
            }
            let Some(pvc) = self.pvc(ns, &pvc_name) else {
                if new_pod {
                    // The PVC is required to proceed with scheduling of a new
                    // pod because it cannot run without it.
                    return Err(LimitsError::NotFound(format!(
                        "looking up PVC {ns}/{pvc_name}: persistentvolumeclaims \"{pvc_name}\" not found"
                    )));
                }
                // If the PVC is invalid, we don't count the volume because
                // there's no guarantee that it belongs to the running predicate.
                continue;
            };
            // The PVC for an ephemeral volume must be owned by the pod.
            if is_ephemeral {
                ephemeral_volume_is_for_pod(pod, pvc).map_err(LimitsError::Other)?;
            }
            let (driver_name, volume_handle) = self.get_csi_driver_info(csi_node, pvc);
            if driver_name.is_empty() || volume_handle.is_empty() {
                continue;
            }
            result.insert(
                get_volume_unique_name(&driver_name, &volume_handle),
                driver_name,
            );
        }
        Ok(())
    }

    /// `checkAttachableInlineVolume` (`csi.go:443-481`).
    fn check_attachable_inline_volume(
        &self,
        vol: &Volume,
        csi_node: Option<&CSINode>,
        pod: &Pod,
        result: &mut HashMap<String, String>,
    ) -> Result<(), LimitsError> {
        if !self.translator.is_inline_migratable(vol) {
            return Ok(());
        }
        // Check if the intree provisioner CSI migration has been enabled.
        let in_tree_provisioner_name = self
            .translator
            .get_in_tree_plugin_name_from_spec(None, Some(vol))
            .map_err(|e| {
                LimitsError::Other(format!(
                    "looking up provisioner name for volume {}: {e}",
                    vol.name
                ))
            })?;
        if !is_csi_migration_on(
            csi_node,
            &in_tree_provisioner_name,
            self.enable_csi_migration_portworx,
        ) {
            return Ok(());
        }
        // Do translation for the in-tree volume.
        let translated_pv = self
            .translator
            .translate_in_tree_inline_volume_to_csi(
                vol,
                pod.metadata.namespace.as_deref().unwrap_or(""),
            )
            .map_err(|e| {
                LimitsError::Other(format!(
                    "converting volume({}) from inline to csi: {e}",
                    vol.name
                ))
            })?;
        let driver_name = self
            .translator
            .get_csi_name_from_in_tree_name(&in_tree_provisioner_name)
            .map_err(|e| {
                LimitsError::Other(format!(
                    "looking up CSI driver name for provisioner {in_tree_provisioner_name}: {e}"
                ))
            })?;
        // TranslateInTreeInlineVolumeToCSI should translate inline volume to
        // CSI. If it is not set, the volume does not support inline. Skip the
        // count.
        let Some(csi) = translated_pv.spec.csi.as_ref() else {
            return Ok(());
        };
        let handle = csi.volume_handle.clone().unwrap_or_default();
        result.insert(get_volume_unique_name(&driver_name, &handle), driver_name);
        Ok(())
    }

    /// `getCSIDriverInfo` (`csi.go:483-534`): the CSI driver name and volume
    /// handle of a PVC, translated when it is from a migrated in-tree plugin.
    fn get_csi_driver_info(
        &self,
        csi_node: Option<&CSINode>,
        pvc: &PersistentVolumeClaim,
    ) -> (String, String) {
        let pv_name = pvc.spec.volume_name.as_deref().unwrap_or("");
        if pv_name.is_empty() {
            return self.get_csi_driver_info_from_sc(csi_node, pvc);
        }
        // If we can't fetch the PV associated with the PVC, maybe it got
        // deleted or the PVC was prebound to a PV that hasn't been created
        // yet: fall back to the StorageClass for volume counting.
        let Some(pv) = self.pv(pv_name) else {
            return self.get_csi_driver_info_from_sc(csi_node, pvc);
        };
        if let Some(csi) = pv.spec.csi.as_ref() {
            return (
                csi.driver.clone(),
                csi.volume_handle.clone().unwrap_or_default(),
            );
        }
        // Fast path for non-CSI volumes that aren't migratable.
        if !self.translator.is_pv_migratable(pv) {
            return Default::default();
        }
        let Ok(plugin_name) = self
            .translator
            .get_in_tree_plugin_name_from_spec(Some(pv), None)
        else {
            return Default::default();
        };
        if !is_csi_migration_on(csi_node, &plugin_name, self.enable_csi_migration_portworx) {
            return Default::default();
        }
        let Ok(csi_pv) = self.translator.translate_in_tree_pv_to_csi(pv) else {
            return Default::default();
        };
        match csi_pv.spec.csi {
            Some(csi) => (csi.driver, csi.volume_handle.unwrap_or_default()),
            None => Default::default(),
        }
    }

    /// `getCSIDriverInfoFromSC` (`csi.go:536-579`): the CSI driver name and a
    /// random volume ID of a PVC's StorageClass.
    fn get_csi_driver_info_from_sc(
        &self,
        csi_node: Option<&CSINode>,
        pvc: &PersistentVolumeClaim,
    ) -> (String, String) {
        let sc_name = claim_class(pvc);
        // If StorageClass is not set or not found, then PVC must be using
        // immediate binding mode and hence it must be bound before scheduling.
        // So it is safe to not count it.
        if sc_name.is_empty() {
            return Default::default();
        }
        let Some(storage_class) = self
            .snap
            .classes
            .iter()
            .find(|c| c.metadata.name == sc_name)
        else {
            return Default::default();
        };
        // A random prefix avoids conflicts with volume IDs.
        let volume_handle = format!(
            "{}-{}/{}",
            self.random_volume_id_prefix,
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name
        );
        let provisioner = storage_class.provisioner.as_str();
        if self
            .translator
            .is_migratable_in_tree_plugin_by_name(provisioner)
        {
            if !is_csi_migration_on(csi_node, provisioner, self.enable_csi_migration_portworx) {
                return Default::default();
            }
            return match self.translator.get_csi_name_from_in_tree_name(provisioner) {
                Ok(driver_name) => (driver_name, volume_handle),
                Err(_) => Default::default(),
            };
        }
        (provisioner.to_string(), volume_handle)
    }

    /// `getNodeVolumeAttachmentInfo` (`csi.go:631-663`): unique volume name to
    /// driver name for the VolumeAttachments of `node_name`.
    fn get_node_volume_attachment_info(&self, node_name: &str) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for va in &self.snap.volume_attachments {
            if va.spec.node_name != node_name || va.spec.attacher.is_empty() {
                continue;
            }
            let Some(pv_name) = va.spec.source.persistent_volume_name.as_deref() else {
                continue;
            };
            let Some(pv) = self.pv(pv_name) else { continue };
            let Some(csi) = pv.spec.csi.as_ref() else {
                continue;
            };
            let volume_id = get_volume_unique_name(
                &va.spec.attacher,
                csi.volume_handle.as_deref().unwrap_or(""),
            );
            out.insert(volume_id, va.spec.attacher.clone());
        }
        out
    }
}

/// `getVolumeLimits` (`csi.go:610-624`): driver name to the node's attach
/// limit; a driver without `allocatable.count` has no entry (no limit).
fn get_volume_limits(csi_node: Option<&CSINode>) -> HashMap<String, i64> {
    let mut limits = HashMap::new();
    if let Some(n) = csi_node {
        for d in &n.spec.drivers {
            if let Some(count) = d.allocatable.as_ref().and_then(|a| a.count) {
                limits.insert(d.name.clone(), i64::from(count));
            }
        }
    }
    limits
}

/// `getVolumeUniqueName` (`csi.go:665-667`).
fn get_volume_unique_name(driver_name: &str, volume_handle: &str) -> String {
    format!("{driver_name}/{volume_handle}")
}

/// `isCSIMigrationOn` (`utils.go:28-70`). Like upstream, the in-tree plugins
/// that are always migrated (AWS EBS, GCE PD, Azure Disk, Cinder) return true
/// without consulting the CSINode annotation; only Portworx (gated) does.
fn is_csi_migration_on(
    csi_node: Option<&CSINode>,
    plugin_name: &str,
    enable_csi_migration_portworx: bool,
) -> bool {
    let Some(csi_node) = csi_node else {
        return false;
    };
    if plugin_name.is_empty() {
        return false;
    }
    match plugin_name {
        "kubernetes.io/aws-ebs"
        | "kubernetes.io/gce-pd"
        | "kubernetes.io/azure-disk"
        | "kubernetes.io/cinder" => return true,
        "kubernetes.io/portworx-volume" => {
            if !enable_csi_migration_portworx {
                return false;
            }
        }
        _ => return false,
    }
    // The plugin name should be listed in the CSINode object annotation. This
    // indicates that the plugin has been migrated to a CSI driver in the node.
    csi_node
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(MIGRATED_PLUGINS_ANNOTATION_KEY))
        .is_some_and(|mpa| !mpa.is_empty() && mpa.split(',').any(|t| t == plugin_name))
}
