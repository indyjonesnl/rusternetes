//! VolumeBinding scheduler plugin: PreFilter + Filter (`FindPodVolumes`).
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/volumebinding/{volume_binding.go,binder.go}`
//! and `staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go`.

use rusternetes_common::affinity::matches_node_selector;
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::{
    PersistentVolumeClaimPhase, PersistentVolumeMode, PersistentVolumePhase, TopologySelectorTerm,
    VolumeBindingMode,
};
use rusternetes_common::resources::{
    CSIDriver, CSINode, CSIStorageCapacity, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    StorageClass, Volume,
};
use rusternetes_common::types::{label_selector_as_selector, LabelSelector};
use std::collections::HashMap;

pub const ERR_REASON_BIND_CONFLICT: &str =
    "node(s) didn't find available persistent volumes to bind";
pub const ERR_REASON_NODE_CONFLICT: &str = "node(s) didn't match PersistentVolume's node affinity";
pub const ERR_REASON_NOT_ENOUGH_SPACE: &str = "node(s) did not have enough free storage";
pub const ERR_REASON_PV_NOT_EXIST: &str =
    "node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)";

/// Listers the plugin reads (upstream: PVC/PV/StorageClass/CSIDriver/
/// CSIStorageCapacity listers).
#[derive(Default, Clone)]
pub struct VolumeSnapshot {
    pub pvcs: Vec<PersistentVolumeClaim>,
    pub pvs: Vec<PersistentVolume>,
    pub classes: Vec<StorageClass>,
    pub csi_drivers: Vec<CSIDriver>,
    pub csi_capacities: Vec<CSIStorageCapacity>,
    /// CSINode lister (binder.go `csiNodeLister`); read by checkBoundClaims.
    pub csi_nodes: Vec<CSINode>,
}

#[derive(Default, Clone)]
pub struct PodVolumeClaims {
    pub bound_claims: Vec<PersistentVolumeClaim>,
    pub unbound_claims_immediate: Vec<PersistentVolumeClaim>,
    pub unbound_claims_delay_binding: Vec<PersistentVolumeClaim>,
    pub unbound_volumes_delay_binding: HashMap<String, Vec<PersistentVolume>>,
}

pub enum PreFilterOutcome {
    Skip,
    Unresolvable(String),
    Error(String),
    Ready(PodVolumeClaims),
}

#[derive(Clone)]

pub struct BindingInfo {
    pub pv: PersistentVolume,
    pub pvc: PersistentVolumeClaim,
}

#[derive(Clone)]

pub struct DynamicProvision {
    pub pvc: PersistentVolumeClaim,
    pub node_capacity: Option<CSIStorageCapacity>,
}

#[derive(Default, Clone)]
pub struct PodVolumes {
    pub static_bindings: Vec<BindingInfo>,
    pub dynamic_provisions: Vec<DynamicProvision>,
}

pub(crate) const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
const ANN_BIND_COMPLETED: &str = "pv.kubernetes.io/bind-completed";
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";
/// `volume.NotSupportedProvisioner` (pkg/volume/util/storageclass.go).
const NOT_SUPPORTED_PROVISIONER: &str = "kubernetes.io/no-provisioner";

pub(crate) fn pvc_key(pvc: &PersistentVolumeClaim) -> String {
    format!(
        "{}/{}",
        pvc.metadata.namespace.as_deref().unwrap_or(""),
        pvc.metadata.name
    )
}

/// `GetPersistentVolumeClaimClass` (component-helpers `helpers.go:43-53`):
/// beta annotation first, then `spec.storageClassName`.
fn claim_class(pvc: &PersistentVolumeClaim) -> String {
    if let Some(c) = pvc
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_BETA_STORAGE_CLASS))
    {
        return c.clone();
    }
    pvc.spec.storage_class_name.clone().unwrap_or_default()
}

/// `GetPersistentVolumeClass` (`helpers.go:56-63`).
fn volume_class(pv: &PersistentVolume) -> String {
    if let Some(c) = pv
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_BETA_STORAGE_CLASS))
    {
        return c.clone();
    }
    pv.spec.storage_class_name.clone().unwrap_or_default()
}

fn storage_request(pvc: &PersistentVolumeClaim) -> Option<Quantity> {
    pvc.spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
        .and_then(|q| Quantity::parse(q).ok())
}

fn zero_quantity() -> Quantity {
    Quantity::from_value(0, Format::BinarySI)
}

/// `ephemeral.VolumeClaimName` (`ephemeral.go:41-43`).
fn ephemeral_claim_name(pod: &Pod, vol_name: &str) -> String {
    format!("{}-{}", pod.metadata.name, vol_name)
}

/// `ephemeral.VolumeIsForPod` (`ephemeral.go:49-56`): the PVC must be in the
/// pod's namespace and controlled (`metav1.IsControlledBy`) by the pod.
pub(crate) fn ephemeral_volume_is_for_pod(
    pod: &Pod,
    pvc: &PersistentVolumeClaim,
) -> Result<(), String> {
    let controlled = pvc.metadata.namespace == pod.metadata.namespace
        && pvc.metadata.owner_references.as_ref().is_some_and(|refs| {
            refs.iter()
                .any(|r| r.controller == Some(true) && r.uid == pod.metadata.uid)
        });
    if controlled {
        Ok(())
    } else {
        Err(format!(
            "PVC {}/{} was not created for pod {}/{} (pod is not owner)",
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name,
            pod.metadata.namespace.as_deref().unwrap_or(""),
            pod.metadata.name
        ))
    }
}

/// The PVC name a pod volume refers to, and whether it is a generic ephemeral
/// volume. `None` for volumes that do not use a PVC.
pub(crate) fn volume_pvc_name(pod: &Pod, vol: &Volume) -> Option<(String, bool)> {
    if let Some(p) = vol.persistent_volume_claim.as_ref() {
        Some((p.claim_name.clone(), false))
    } else if vol.ephemeral.is_some() {
        Some((ephemeral_claim_name(pod, &vol.name), true))
    } else {
        None
    }
}

impl VolumeSnapshot {
    fn pvc(&self, ns: &str, name: &str) -> Option<&PersistentVolumeClaim> {
        self.pvcs
            .iter()
            .find(|c| c.metadata.name == name && c.metadata.namespace.as_deref() == Some(ns))
    }
    fn pv(&self, name: &str) -> Option<&PersistentVolume> {
        self.pvs.iter().find(|v| v.metadata.name == name)
    }
    fn csi_node(&self, name: &str) -> Option<&CSINode> {
        self.csi_nodes.iter().find(|c| c.metadata.name == name)
    }
    fn class(&self, name: &str) -> Option<&StorageClass> {
        self.classes.iter().find(|c| c.metadata.name == name)
    }
}

/// Whether the pod has at least one PVC-backed volume. Cheap pre-check so the
/// caller can avoid listing volume objects for the (common) PVC-less pod.
pub fn pod_references_pvcs(pod: &Pod) -> bool {
    pod_volumes(pod)
        .iter()
        .any(|v| v.persistent_volume_claim.is_some() || v.ephemeral.is_some())
}

pub(crate) fn pod_volumes(pod: &Pod) -> &[Volume] {
    pod.spec
        .as_ref()
        .and_then(|s| s.volumes.as_deref())
        .unwrap_or(&[])
}

/// `podHasPVCs` (volume_binding.go:313-358).
fn pod_has_pvcs(pod: &Pod, snap: &VolumeSnapshot) -> Result<bool, String> {
    let ns = pod.metadata.namespace.as_deref().unwrap_or("");
    let mut has_pvc = false;
    for vol in pod_volumes(pod) {
        let Some((name, is_ephemeral)) = volume_pvc_name(pod, vol) else {
            continue;
        };
        has_pvc = true;
        let Some(pvc) = snap.pvc(ns, &name) else {
            return Err(if is_ephemeral {
                format!(
                    "waiting for ephemeral volume controller to create the persistentvolumeclaim \"{name}\""
                )
            } else {
                format!("persistentvolumeclaim \"{name}\" not found")
            });
        };
        if pvc.status.as_ref().map(|s| &s.phase) == Some(&PersistentVolumeClaimPhase::Lost) {
            return Err(format!(
                "persistentvolumeclaim \"{}\" bound to non-existent persistentvolume \"{}\"",
                pvc.metadata.name,
                pvc.spec.volume_name.as_deref().unwrap_or("")
            ));
        }
        if pvc.metadata.deletion_timestamp.is_some() {
            return Err(format!(
                "persistentvolumeclaim \"{}\" is being deleted",
                pvc.metadata.name
            ));
        }
        if is_ephemeral {
            ephemeral_volume_is_for_pod(pod, pvc)?;
        }
    }
    Ok(has_pvc)
}

/// `isPVCFullyBound` (binder.go:775-777).
pub(crate) fn is_pvc_fully_bound(pvc: &PersistentVolumeClaim) -> bool {
    pvc.spec
        .volume_name
        .as_deref()
        .is_some_and(|v| !v.is_empty())
        && pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BIND_COMPLETED))
}

/// `IsDelayBindingMode` (pv_helpers.go:96-115).
fn is_delay_binding_mode(
    pvc: &PersistentVolumeClaim,
    snap: &VolumeSnapshot,
) -> Result<bool, String> {
    let class_name = claim_class(pvc);
    if class_name.is_empty() {
        return Ok(false);
    }
    let Some(class) = snap.class(&class_name) else {
        return Ok(false);
    };
    match &class.volume_binding_mode {
        None => Err(format!(
            "VolumeBindingMode not set for StorageClass \"{class_name}\""
        )),
        Some(m) => Ok(*m == VolumeBindingMode::WaitForFirstConsumer),
    }
}

/// `GetPodVolumeClaims` (binder.go:792-837).
fn get_pod_volume_claims(pod: &Pod, snap: &VolumeSnapshot) -> Result<PodVolumeClaims, String> {
    let ns = pod.metadata.namespace.as_deref().unwrap_or("");
    let mut out = PodVolumeClaims::default();
    for vol in pod_volumes(pod) {
        let Some((name, is_ephemeral)) = volume_pvc_name(pod, vol) else {
            continue; // isVolumeBound: non-PVC volumes are "bound", pvc nil
        };
        let Some(pvc) = snap.pvc(ns, &name) else {
            return Err(format!("error getting PVC \"{ns}/{name}\": not found"));
        };
        if is_ephemeral {
            ephemeral_volume_is_for_pod(pod, pvc)?;
        }
        if is_pvc_fully_bound(pvc) {
            out.bound_claims.push(pvc.clone());
        } else {
            let delay = is_delay_binding_mode(pvc, snap)?;
            // Prebound PVCs are treated as unbound immediate binding.
            if delay && pvc.spec.volume_name.as_deref().unwrap_or("").is_empty() {
                out.unbound_claims_delay_binding.push(pvc.clone());
            } else {
                out.unbound_claims_immediate.push(pvc.clone());
            }
        }
    }
    for pvc in &out.unbound_claims_delay_binding {
        let class = claim_class(pvc);
        let pvs: Vec<PersistentVolume> = snap
            .pvs
            .iter()
            .filter(|v| volume_class(v) == class)
            .cloned()
            .collect();
        out.unbound_volumes_delay_binding.insert(class, pvs);
    }
    Ok(out)
}

/// `PreFilter` (volume_binding.go:360-391).
pub fn pre_filter(pod: &Pod, snap: &VolumeSnapshot) -> PreFilterOutcome {
    match pod_has_pvcs(pod, snap) {
        Err(e) => return PreFilterOutcome::Unresolvable(e),
        Ok(false) => return PreFilterOutcome::Skip,
        Ok(true) => {}
    }
    let claims = match get_pod_volume_claims(pod, snap) {
        Ok(c) => c,
        Err(e) => return PreFilterOutcome::Error(e),
    };
    if !claims.unbound_claims_immediate.is_empty() {
        return PreFilterOutcome::Unresolvable(
            "pod has unbound immediate PersistentVolumeClaims".to_string(),
        );
    }
    PreFilterOutcome::Ready(claims)
}

/// `volume.CheckNodeAffinity` (helpers.go:68-83). Upstream matches against a
/// node carrying only labels, so `matchFields` never sees a name.
pub(crate) fn check_pv_node_affinity(pv: &PersistentVolume, node: &Node) -> bool {
    let Some(required) = pv
        .spec
        .node_affinity
        .as_ref()
        .and_then(|a| a.required.as_ref())
    else {
        return true;
    };
    let mut label_only = node.clone();
    label_only.metadata.name = String::new();
    // The PV's `volume::NodeSelector` is wire-identical to the pod-affinity
    // `NodeSelector` the shared matcher takes.
    match serde_json::to_value(required).and_then(serde_json::from_value) {
        Ok(selector) => matches_node_selector(&label_only, &selector),
        Err(_) => false,
    }
}

/// `IsVolumeBoundToClaim` (pv_helpers.go:159-169).
pub(crate) fn is_volume_bound_to_claim(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> bool {
    let Some(cr) = pv.spec.claim_ref.as_ref() else {
        return false;
    };
    if cr.name.as_deref() != Some(pvc.metadata.name.as_str())
        || cr.namespace.as_deref() != pvc.metadata.namespace.as_deref()
    {
        return false;
    }
    cr.uid
        .as_deref()
        .is_none_or(|u| u.is_empty() || u == pvc.metadata.uid)
}

fn volume_mode_mismatch(pvc: &PersistentVolumeClaim, pv: &PersistentVolume) -> bool {
    let want = pvc
        .spec
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    let have = pv
        .spec
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    want != have
}

/// `FindMatchingVolume` scheduler path (`node != nil`), pv_helpers.go:186-329.
/// VolumeAttributesClass is GA in 1.34+, so `vacEnabled` is true.
fn find_matching_volume<'a>(
    pvc: &PersistentVolumeClaim,
    volumes: &'a [PersistentVolume],
    node: &Node,
    excluded: &HashMap<String, &PersistentVolume>,
) -> Result<Option<&'a PersistentVolume>, String> {
    let requested = storage_request(pvc).unwrap_or_else(zero_quantity);
    let requested_class = claim_class(pvc);
    let selector = match pvc.spec.selector.as_ref() {
        Some(s) => {
            let converted: LabelSelector = serde_json::to_value(s)
                .and_then(serde_json::from_value)
                .map_err(|e| e.to_string())?;
            Some(label_selector_as_selector(Some(&converted)).map_err(|e| {
                format!(
                    "error creating internal label selector for claim: {}: {e}",
                    pvc_key(pvc)
                )
            })?)
        }
        None => None,
    };
    let claim_vac = pvc
        .spec
        .volume_attributes_class_name
        .as_deref()
        .unwrap_or("");
    let mut smallest: Option<(&PersistentVolume, Quantity)> = None;
    for volume in volumes {
        if excluded.contains_key(&volume.metadata.name) {
            continue;
        }
        if volume.spec.claim_ref.is_some() && !is_volume_bound_to_claim(volume, pvc) {
            continue;
        }
        let Some(volume_qty) = volume
            .spec
            .capacity
            .get("storage")
            .and_then(|q| Quantity::parse(q).ok())
        else {
            continue;
        };
        if volume_qty.cmp_value(&requested) == std::cmp::Ordering::Less {
            continue;
        }
        if volume_mode_mismatch(pvc, volume) {
            continue;
        }
        if claim_vac
            != volume
                .spec
                .volume_attributes_class_name
                .as_deref()
                .unwrap_or("")
        {
            continue;
        }
        if volume.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let node_affinity_valid = check_pv_node_affinity(volume, node);
        if is_volume_bound_to_claim(volume, pvc) {
            // A prebound PV whose affinity rejects the node: no match.
            return Ok(if node_affinity_valid {
                Some(volume)
            } else {
                None
            });
        }
        let available =
            volume.status.as_ref().map(|s| &s.phase) == Some(&PersistentVolumePhase::Available);
        if !available {
            continue;
        }
        if let Some(sel) = &selector {
            if !sel.matches(volume.metadata.labels.as_ref()) {
                continue;
            }
        }
        if volume_class(volume) != requested_class {
            continue;
        }
        if !node_affinity_valid {
            continue;
        }
        // CheckAccessModes (pv_helpers.go:346-358)
        if !pvc
            .spec
            .access_modes
            .iter()
            .all(|m| volume.spec.access_modes.contains(m))
        {
            continue;
        }
        if smallest
            .as_ref()
            .is_none_or(|(_, q)| q.cmp_value(&volume_qty) == std::cmp::Ordering::Greater)
        {
            smallest = Some((volume, volume_qty));
        }
    }
    Ok(smallest.map(|(v, _)| v))
}

/// `v1.MigratedPluginsAnnotationKey` (staging/src/k8s.io/api/core/v1/annotation_key_constants.go).
const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

/// `isCSIMigrationOnForPlugin` (binder.go:1069-1083). Portworx is gated by
/// `enableCSIMigrationPortworx`, which this scheduler never enables; the shared
/// translator has no Portworx plugin either.
fn is_csi_migration_on_for_plugin(plugin_name: &str) -> bool {
    matches!(
        plugin_name,
        "kubernetes.io/aws-ebs"
            | "kubernetes.io/gce-pd"
            | "kubernetes.io/azure-disk"
            | "kubernetes.io/cinder"
    )
}

/// `isPluginMigratedToCSIOnNode` (binder.go:1086-1106).
fn is_plugin_migrated_to_csi_on_node(plugin_name: &str, csi_node: Option<&CSINode>) -> bool {
    csi_node
        .and_then(|n| n.metadata.annotations.as_ref())
        .and_then(|a| a.get(MIGRATED_PLUGINS_ANNOTATION_KEY))
        .is_some_and(|mpa| !mpa.is_empty() && mpa.split(',').any(|t| t == plugin_name))
}

/// `tryTranslatePVToCSI` (binder.go:1108-1139): translate the in-tree PV to
/// CSI when its plugin is migrated on this node, else return it unmodified.
pub(crate) fn try_translate_pv_to_csi<'a>(
    pv: &'a PersistentVolume,
    csi_node: Option<&CSINode>,
) -> Result<std::borrow::Cow<'a, PersistentVolume>, String> {
    let translator = CsiTranslator::new();
    if !translator.is_pv_migratable(pv) {
        return Ok(std::borrow::Cow::Borrowed(pv));
    }
    let plugin_name = translator
        .get_in_tree_plugin_name_from_pv(pv)
        .map_err(|e| format!("could not get plugin name from pv: {e}"))?;
    if !is_csi_migration_on_for_plugin(&plugin_name)
        || !is_plugin_migrated_to_csi_on_node(&plugin_name, csi_node)
    {
        return Ok(std::borrow::Cow::Borrowed(pv));
    }
    translator
        .translate_in_tree_pv_to_csi(pv)
        .map(std::borrow::Cow::Owned)
        .map_err(|e| format!("could not translate pv: {e}"))
}

/// `checkBoundClaims` (binder.go:839-873). Returns
/// (boundVolumesSatisfied, boundPVsFound); a missing CSINode is not an error.
fn check_bound_claims(
    claims: &[PersistentVolumeClaim],
    node: &Node,
    snap: &VolumeSnapshot,
) -> Result<(bool, bool), String> {
    let csi_node = snap.csi_node(&node.metadata.name);
    for pvc in claims {
        let pv_name = pvc.spec.volume_name.as_deref().unwrap_or("");
        let Some(pv) = snap.pv(pv_name) else {
            return Ok((true, false));
        };
        let pv = try_translate_pv_to_csi(pv, csi_node)?;
        if !check_pv_node_affinity(&pv, node) {
            return Ok((false, true));
        }
    }
    Ok((true, true))
}

/// `findMatchingVolumes` (binder.go:875-914).
fn find_matching_volumes(
    claims_to_bind: &mut [PersistentVolumeClaim],
    unbound_volumes: &HashMap<String, Vec<PersistentVolume>>,
    node: &Node,
) -> Result<(bool, Vec<BindingInfo>, Vec<PersistentVolumeClaim>), String> {
    // byPVCSize: smallest requests first (binder.go:1052-1066).
    claims_to_bind.sort_by(|a, b| {
        let qa = storage_request(a).unwrap_or_else(zero_quantity);
        let qb = storage_request(b).unwrap_or_else(zero_quantity);
        qa.cmp_value(&qb)
    });
    let mut chosen: HashMap<String, &PersistentVolume> = HashMap::new();
    let mut found_matches = true;
    let mut bindings = Vec::new();
    let mut unbound = Vec::new();
    for pvc in claims_to_bind.iter() {
        let class = claim_class(pvc);
        let pvs = unbound_volumes
            .get(&class)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        match find_matching_volume(pvc, pvs, node, &chosen)? {
            None => {
                unbound.push(pvc.clone());
                found_matches = false;
            }
            Some(pv) => {
                chosen.insert(pv.metadata.name.clone(), pv);
                bindings.push(BindingInfo {
                    pv: pv.clone(),
                    pvc: pvc.clone(),
                });
            }
        }
    }
    Ok((found_matches, bindings, unbound))
}

/// `MatchTopologySelectorTerms` (apis/core/v1/helper/helpers.go:241-261):
/// terms are ORed, an empty term never matches, an empty list matches all.
fn match_topology_selector_terms(terms: &[TopologySelectorTerm], node: &Node) -> bool {
    if terms.is_empty() {
        return true;
    }
    let labels = node.metadata.labels.as_ref();
    terms.iter().any(|term| {
        let Some(exprs) = term
            .match_label_expressions
            .as_ref()
            .filter(|e| !e.is_empty())
        else {
            return false;
        };
        exprs.iter().all(|e| {
            labels
                .and_then(|l| l.get(&e.key))
                .is_some_and(|v| e.values.contains(v))
        })
    })
}

/// `hasEnoughCapacity` (binder.go:978-1022).
fn has_enough_capacity(
    provisioner: &str,
    pvc: &PersistentVolumeClaim,
    class: &StorageClass,
    node: &Node,
    snap: &VolumeSnapshot,
) -> (bool, Option<CSIStorageCapacity>) {
    let Some(quantity) = storage_request(pvc) else {
        return (true, None);
    };
    let Some(driver) = snap
        .csi_drivers
        .iter()
        .find(|d| d.metadata.name == provisioner)
    else {
        return (true, None);
    };
    if driver.spec.storage_capacity != Some(true) {
        return (true, None);
    }
    let size = quantity.value();
    for capacity in &snap.csi_capacities {
        if capacity.storage_class_name != class.metadata.name {
            continue;
        }
        // capacitySufficient / volumeLimit (binder.go:1024-1035)
        let limit = capacity
            .maximum_volume_size
            .as_ref()
            .or(capacity.capacity.as_ref())
            .and_then(|q| Quantity::parse(q).ok());
        if !limit.is_some_and(|l| l.value() >= size) {
            continue;
        }
        // nodeHasAccess (binder.go:1037-1050)
        let Some(topology) = capacity.node_topology.as_ref() else {
            continue;
        };
        let Ok(converted) =
            serde_json::to_value(topology).and_then(serde_json::from_value::<LabelSelector>)
        else {
            continue;
        };
        let Ok(sel) = label_selector_as_selector(Some(&converted)) else {
            continue;
        };
        if sel.matches(node.metadata.labels.as_ref()) {
            return (true, Some(capacity.clone()));
        }
    }
    (false, None)
}

/// `checkVolumeProvisions` (binder.go:916-962). Returns
/// (provisionSatisfied, sufficientStorage, dynamicProvisions).
fn check_volume_provisions(
    claims: &[PersistentVolumeClaim],
    node: &Node,
    snap: &VolumeSnapshot,
) -> Result<(bool, bool, Vec<DynamicProvision>), String> {
    let mut out = Vec::new();
    for claim in claims {
        let class_name = claim_class(claim);
        if class_name.is_empty() {
            return Err(format!("no class for claim \"{}\"", pvc_key(claim)));
        }
        let Some(class) = snap.class(&class_name) else {
            return Err(format!("failed to find storage class \"{class_name}\""));
        };
        let provisioner = class.provisioner.as_str();
        if provisioner.is_empty() || provisioner == NOT_SUPPORTED_PROVISIONER {
            return Ok((false, true, Vec::new()));
        }
        let topologies = class.allowed_topologies.as_deref().unwrap_or(&[]);
        if !match_topology_selector_terms(topologies, node) {
            return Ok((false, true, Vec::new()));
        }
        let (sufficient, capacity) = has_enough_capacity(provisioner, claim, class, node, snap);
        if !sufficient {
            return Ok((true, false, Vec::new()));
        }
        out.push(DynamicProvision {
            pvc: claim.clone(),
            node_capacity: capacity,
        });
    }
    Ok((true, true, out))
}

/// `FindPodVolumes` (binder.go:285-391).
pub fn find_pod_volumes(
    _pod: &Pod,
    claims: &PodVolumeClaims,
    node: &Node,
    snap: &VolumeSnapshot,
) -> Result<(PodVolumes, Vec<&'static str>), String> {
    let mut unbound_satisfied = true;
    let mut bound_satisfied = true;
    let mut sufficient_storage = true;
    let mut bound_pvs_found = true;
    let mut vols = PodVolumes::default();

    let reasons = |b: bool, u: bool, s: bool, f: bool| {
        let mut r = Vec::new();
        if !b {
            r.push(ERR_REASON_NODE_CONFLICT);
        }
        if !u {
            r.push(ERR_REASON_BIND_CONFLICT);
        }
        if !s {
            r.push(ERR_REASON_NOT_ENOUGH_SPACE);
        }
        if !f {
            r.push(ERR_REASON_PV_NOT_EXIST);
        }
        r
    };

    if !claims.bound_claims.is_empty() {
        (bound_satisfied, bound_pvs_found) = check_bound_claims(&claims.bound_claims, node, snap)?;
    }

    if !claims.unbound_claims_delay_binding.is_empty() {
        let mut to_find = Vec::new();
        let mut to_provision = Vec::new();
        for claim in &claims.unbound_claims_delay_binding {
            let selected = claim
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(ANN_SELECTED_NODE));
            match selected {
                Some(n) if *n != node.metadata.name => {
                    // Fast path, skip unmatched node.
                    return Ok((
                        vols,
                        reasons(bound_satisfied, false, sufficient_storage, bound_pvs_found),
                    ));
                }
                Some(_) => to_provision.push(claim.clone()),
                None => to_find.push(claim.clone()),
            }
        }
        if !to_find.is_empty() {
            let (ok, bindings, unbound) =
                find_matching_volumes(&mut to_find, &claims.unbound_volumes_delay_binding, node)?;
            unbound_satisfied = ok;
            vols.static_bindings = bindings;
            to_provision.extend(unbound);
        }
        if !to_provision.is_empty() {
            let (ok, sufficient, dynamic) = check_volume_provisions(&to_provision, node, snap)?;
            unbound_satisfied = ok;
            sufficient_storage = sufficient;
            vols.dynamic_provisions = dynamic;
        }
    }

    Ok((
        vols,
        reasons(
            bound_satisfied,
            unbound_satisfied,
            sufficient_storage,
            bound_pvs_found,
        ),
    ))
}

// ---------------------------------------------------------------------------
// Score: volume_binding.go PreScore/Score (:456-526), binder.go
// StorageResource (:87-102) and scorer.go (whole file).
// ---------------------------------------------------------------------------

/// `maxUtilization` (volume_binding.go:47).
pub const MAX_UTILIZATION: i64 = 100;
/// `fwk.MaxNodeScore` (staging/src/k8s.io/kube-scheduler/framework/interface.go:289).
pub const MAX_NODE_SCORE: i64 = 100;
/// `config.MaxCustomPriorityScore` (pkg/scheduler/apis/config/types.go:220).
pub const MAX_CUSTOM_PRIORITY_SCORE: i64 = 10;

/// `helper.FunctionShapePoint` (framework/plugins/helper/shape_score.go).
#[derive(Clone, Copy, Debug)]
pub struct FunctionShapePoint {
    pub utilization: i64,
    pub score: i64,
}

/// `StorageResource` (binder.go:87-91).
#[derive(Clone, Copy, Debug, Default)]
pub struct StorageResource {
    pub requested: i64,
    pub capacity: i64,
}

/// `classResourceMap` (scorer.go:26).
pub type ClassResourceMap = HashMap<String, StorageResource>;

/// `BindingInfo.StorageResource` (binder.go:93-102): both fields mandatory,
/// a missing quantity reads as zero (Go's zero-value `resource.Quantity`).
fn binding_storage_resource(b: &BindingInfo) -> StorageResource {
    let requested = storage_request(&b.pvc).map_or(0, |q| q.value() as i64);
    let capacity =
        b.pv.spec
            .capacity
            .get("storage")
            .and_then(|q| Quantity::parse(q).ok())
            .map_or(0, |q| q.value() as i64);
    StorageResource {
        requested,
        capacity,
    }
}

/// The shape the scheduler config defaults to when `StorageCapacityScoring`
/// is on and `shape` is empty: `SetDefaults_VolumeBindingArgs`
/// (pkg/scheduler/apis/config/v1/defaults.go:196-207), scaled to
/// `MaxNodeScore` by `New` (volume_binding.go:647-651).
pub fn default_shape() -> Vec<FunctionShapePoint> {
    [(0, MAX_CUSTOM_PRIORITY_SCORE), (100, 0)]
        .iter()
        .map(|&(utilization, score)| FunctionShapePoint {
            utilization,
            score: score * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
        })
        .collect()
}

/// `helper.BuildBrokenLinearFunction` (framework/plugins/helper/shape_score.go:40-52).
pub(crate) fn build_broken_linear_function(shape: Vec<FunctionShapePoint>) -> impl Fn(i64) -> i64 {
    move |p| {
        for i in 0..shape.len() {
            if p <= shape[i].utilization {
                if i == 0 {
                    return shape[0].score;
                }
                return shape[i - 1].score
                    + (shape[i].score - shape[i - 1].score) * (p - shape[i - 1].utilization)
                        / (shape[i].utilization - shape[i - 1].utilization);
            }
        }
        shape[shape.len() - 1].score
    }
}

/// `buildScorerFunction` (scorer.go:31-53).
pub fn build_scorer_function(shape: &[FunctionShapePoint]) -> impl Fn(&ClassResourceMap) -> i64 {
    let raw = build_broken_linear_function(shape.to_vec());
    let f = move |requested: i64, capacity: i64| -> i64 {
        if capacity == 0 || requested > capacity {
            return raw(MAX_UTILIZATION);
        }
        raw(requested * MAX_UTILIZATION / capacity)
    };
    move |class_resources: &ClassResourceMap| {
        // in alpha stage, all classes have the same weight
        let weight_sum = class_resources.len();
        if weight_sum == 0 {
            return 0;
        }
        let node_score: i64 = class_resources
            .values()
            .map(|r| f(r.requested, r.capacity))
            .sum();
        // math.Round rounds half away from zero, as does f64::round.
        (node_score as f64 / weight_sum as f64).round() as i64
    }
}

/// `PreScore` (volume_binding.go:456-468): true = `fwk.Skip`. The caller maps
/// "no scorer" (gate off) to skip itself, as `pl.scorer == nil` does.
pub fn pre_score_skip(by_node: &HashMap<String, PodVolumes>, scoring_enabled: bool) -> bool {
    // `state.hasStaticBindings` is the OR over every Filter'd node
    // (volume_binding.go:446).
    let has_static_bindings = by_node.values().any(|v| !v.static_bindings.is_empty());
    !(has_static_bindings || scoring_enabled)
}

/// `Score` (volume_binding.go:471-523) for one node's `PodVolumes`
/// (`None` = node absent from `podVolumesByNode` => 0).
pub fn score(
    pod_volumes: Option<&PodVolumes>,
    scorer: &dyn Fn(&ClassResourceMap) -> i64,
    scoring_enabled: bool,
) -> i64 {
    let Some(pod_volumes) = pod_volumes else {
        return 0;
    };
    let mut class_resources = ClassResourceMap::new();
    if !pod_volumes.static_bindings.is_empty() || !scoring_enabled {
        // group static binding volumes by storage class
        for b in &pod_volumes.static_bindings {
            let r = binding_storage_resource(b);
            let e = class_resources.entry(volume_class(&b.pv)).or_default();
            e.requested += r.requested;
            e.capacity += r.capacity;
        }
    } else {
        // group dynamic binding volumes by storage class
        for provision in &pod_volumes.dynamic_provisions {
            let Some(node_capacity) = provision.node_capacity.as_ref() else {
                continue;
            };
            let e = class_resources
                .entry(claim_class(&provision.pvc))
                .or_default();
            // Assigned, not `+=`: two 50GB claims on a 100GB class must not
            // read as 200GB (volume_binding.go:508-512).
            e.capacity = node_capacity
                .capacity
                .as_ref()
                .and_then(|q| Quantity::parse(q).ok())
                .map_or(0, |q| q.value() as i64);
            e.requested += storage_request(&provision.pvc).map_or(0, |q| q.value() as i64);
        }
    }
    scorer(&class_resources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn pod(volumes: Value) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "pod-uid"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "volumes": volumes}
        }))
        .unwrap()
    }
    fn pod_with_pvc(claim: &str) -> Pod {
        pod(json!([{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]))
    }
    fn node(name: &str, labels: Value) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}, "spec": {}
        }))
        .unwrap()
    }
    fn pvc(name: &str, extra_spec: Value, ann: Value, status: Value) -> PersistentVolumeClaim {
        let mut spec = json!({"resources": {"requests": {"storage": "1Gi"}}});
        for (k, v) in extra_spec.as_object().unwrap() {
            spec[k] = v.clone();
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "ns", "uid": format!("{name}-uid"), "annotations": ann},
            "spec": spec, "status": status
        }))
        .unwrap()
    }
    fn bound_pvc(name: &str, pv: &str) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"volumeName": pv, "storageClassName": "std"}),
            json!({"pv.kubernetes.io/bind-completed": "yes"}),
            json!({"phase": "Bound"}),
        )
    }
    fn wffc_pvc(name: &str, class: &str, ann: Value) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"storageClassName": class}),
            ann,
            json!({"phase": "Pending"}),
        )
    }
    fn pv(name: &str, class: &str, size: &str, aff: Option<Value>) -> PersistentVolume {
        let mut spec = json!({
            "capacity": {"storage": size}, "accessModes": ["ReadWriteOnce"],
            "storageClassName": class, "hostPath": {"path": "/x"}
        });
        if let Some(a) = aff {
            spec["nodeAffinity"] = a;
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": spec, "status": {"phase": "Available"}
        }))
        .unwrap()
    }
    fn zone_affinity(zone: &str) -> Value {
        json!({"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "zone", "operator": "In", "values": [zone]}]}]}})
    }
    fn class(name: &str, mode: &str, provisioner: &str, topo: Option<Value>) -> StorageClass {
        let mut v = json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": name}, "provisioner": provisioner, "volumeBindingMode": mode
        });
        if let Some(t) = topo {
            v["allowedTopologies"] = t;
        }
        serde_json::from_value(v).unwrap()
    }
    fn ready(p: &Pod, s: &VolumeSnapshot) -> PodVolumeClaims {
        match pre_filter(p, s) {
            PreFilterOutcome::Ready(c) => c,
            PreFilterOutcome::Skip => panic!("got Skip"),
            PreFilterOutcome::Unresolvable(m) | PreFilterOutcome::Error(m) => panic!("got {m}"),
        }
    }
    fn filter(p: &Pod, n: &Node, s: &VolumeSnapshot) -> (PodVolumes, Vec<&'static str>) {
        find_pod_volumes(p, &ready(p, s), n, s).unwrap()
    }

    #[test]
    fn pod_without_pvcs_is_skipped() {
        let p = pod(json!([{"name": "v", "emptyDir": {}}]));
        assert!(matches!(
            pre_filter(&p, &VolumeSnapshot::default()),
            PreFilterOutcome::Skip
        ));
    }

    #[test]
    fn missing_pvc_is_unresolvable() {
        match pre_filter(&pod_with_pvc("foo"), &VolumeSnapshot::default()) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "persistentvolumeclaim \"foo\" not found")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn lost_pvc_is_unresolvable() {
        let c = pvc(
            "foo",
            json!({"volumeName": "pv1"}),
            json!({}),
            json!({"phase": "Lost"}),
        );
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => assert_eq!(
                m,
                "persistentvolumeclaim \"foo\" bound to non-existent persistentvolume \"pv1\""
            ),
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn deleting_pvc_is_unresolvable() {
        let mut c = bound_pvc("foo", "pv1");
        c.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "persistentvolumeclaim \"foo\" is being deleted")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn unbound_immediate_pvc_is_unresolvable() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "imm", json!({}))],
            classes: vec![class("imm", "Immediate", "p", None)],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "pod has unbound immediate PersistentVolumeClaims")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn bound_pvc_checks_pv_node_affinity() {
        let s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "pv1")],
            pvs: vec![pv("pv1", "std", "1Gi", Some(zone_affinity("a")))],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (_, r) = filter(&p, &node("n1", json!({"zone": "a"})), &s);
        assert!(r.is_empty());
        let (_, r) = filter(&p, &node("n2", json!({"zone": "b"})), &s);
        assert_eq!(r, vec![ERR_REASON_NODE_CONFLICT]);
    }

    fn gce_bound_snapshot(csi_node_annotations: Option<Value>) -> VolumeSnapshot {
        let mut pv = pv(
            "pv1",
            "std",
            "1Gi",
            Some(json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "topology.kubernetes.io/zone", "operator": "In", "values": ["a"]}]}]}})),
        );
        pv.spec.host_path = None;
        pv.spec.gce_persistent_disk = serde_json::from_value(json!({"pdName": "d1"})).unwrap();
        let mut s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "pv1")],
            pvs: vec![pv],
            ..Default::default()
        };
        if let Some(ann) = csi_node_annotations {
            s.csi_nodes = vec![serde_json::from_value(json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
                "metadata": {"name": "n1", "annotations": ann}, "spec": {"drivers": []}
            }))
            .unwrap()];
        }
        s
    }

    // binder.go checkBoundClaims -> tryTranslatePVToCSI: the PV's zone
    // affinity is rewritten to the CSI topology key when the node's CSINode
    // lists the plugin as migrated, so a node carrying only the CSI key matches.
    #[test]
    fn migrated_in_tree_pv_affinity_is_translated_for_csi_node() {
        let s = gce_bound_snapshot(Some(json!({
            "storage.alpha.kubernetes.io/migrated-plugins": "kubernetes.io/gce-pd"
        })));
        let p = pod_with_pvc("foo");
        let (_, r) = filter(&p, &node("n1", json!({"topology.gke.io/zone": "a"})), &s);
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn unmigrated_in_tree_pv_affinity_is_untouched() {
        let p = pod_with_pvc("foo");
        let n = node("n1", json!({"topology.gke.io/zone": "a"}));
        for s in [
            gce_bound_snapshot(None),
            gce_bound_snapshot(Some(json!({}))),
            gce_bound_snapshot(Some(json!({
                "storage.alpha.kubernetes.io/migrated-plugins": "kubernetes.io/aws-ebs"
            }))),
        ] {
            let (_, r) = filter(&p, &n, &s);
            assert_eq!(r, vec![ERR_REASON_NODE_CONFLICT]);
        }
    }

    #[test]
    fn bound_pvc_with_missing_pv_reports_pv_not_exist() {
        let s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "gone")],
            ..Default::default()
        };
        let (_, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_PV_NOT_EXIST]);
    }

    #[test]
    fn wffc_claim_finds_matching_pv_on_node() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            pvs: vec![pv("pv1", "wait", "2Gi", Some(zone_affinity("a")))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (vols, r) = filter(&p, &node("n1", json!({"zone": "a"})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.static_bindings.len(), 1);
        assert_eq!(vols.static_bindings[0].pv.metadata.name, "pv1");
        let (vols, r) = filter(&p, &node("n2", json!({"zone": "b"})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
        assert!(vols.static_bindings.is_empty());
    }

    #[test]
    fn smallest_fitting_pv_chosen_and_not_reused_across_claims() {
        let s = VolumeSnapshot {
            pvcs: vec![
                wffc_pvc("a", "wait", json!({})),
                wffc_pvc("b", "wait", json!({})),
            ],
            pvs: vec![
                pv("big", "wait", "10Gi", None),
                pv("small", "wait", "2Gi", None),
            ],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let p = pod(json!([
            {"name": "va", "persistentVolumeClaim": {"claimName": "a"}},
            {"name": "vb", "persistentVolumeClaim": {"claimName": "b"}}
        ]));
        let (vols, r) = filter(&p, &node("n1", json!({})), &s);
        assert!(r.is_empty());
        let mut names: Vec<_> = vols
            .static_bindings
            .iter()
            .map(|b| b.pv.metadata.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["big", "small"]);
    }

    #[test]
    fn no_provisioner_class_without_pv_is_bind_conflict() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let (_, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
    }

    #[test]
    fn provisionable_class_without_pv_yields_dynamic_provision() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            ..Default::default()
        };
        let (vols, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.dynamic_provisions.len(), 1);
        assert!(vols.static_bindings.is_empty());
    }

    #[test]
    fn selected_node_annotation_pins_provisioning_to_that_node() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc(
                "foo",
                "wait",
                json!({"volume.kubernetes.io/selected-node": "n1"}),
            )],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (vols, r) = filter(&p, &node("n1", json!({})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.dynamic_provisions.len(), 1);
        let (_, r) = filter(&p, &node("n2", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
    }

    #[test]
    fn allowed_topologies_restrict_provisioning_nodes() {
        let topo = json!([{"matchLabelExpressions": [{"key": "zone", "values": ["a"]}]}]);
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                Some(topo),
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        assert!(filter(&p, &node("n1", json!({"zone": "a"})), &s)
            .1
            .is_empty());
        assert_eq!(
            filter(&p, &node("n2", json!({"zone": "b"})), &s).1,
            vec![ERR_REASON_BIND_CONFLICT]
        );
    }

    fn csi_driver(storage_capacity: bool) -> CSIDriver {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": "example.com/csi"},
            "spec": {"storageCapacity": storage_capacity}
        }))
        .unwrap()
    }
    fn capacity(zone: &str, size: &str) -> CSIStorageCapacity {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
            "metadata": {"name": format!("cap-{zone}"), "namespace": "ns"},
            "storageClassName": "wait", "capacity": size,
            "nodeTopology": {"matchLabels": {"zone": zone}}
        }))
        .unwrap()
    }

    #[test]
    fn csi_capacity_gates_dynamic_provisioning() {
        let base = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            csi_drivers: vec![csi_driver(true)],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let n = node("n1", json!({"zone": "a"}));
        // Driver opts in but no capacity object: not enough space.
        assert_eq!(filter(&p, &n, &base).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Too small.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("a", "500Mi")];
        assert_eq!(filter(&p, &n, &s).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Wrong topology.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("b", "5Gi")];
        assert_eq!(filter(&p, &n, &s).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Sufficient and reachable.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("a", "5Gi")];
        let (vols, r) = filter(&p, &n, &s);
        assert!(r.is_empty());
        assert!(vols.dynamic_provisions[0].node_capacity.is_some());
        // Driver without storageCapacity: capacity not checked.
        let mut s = base;
        s.csi_drivers = vec![csi_driver(false)];
        assert!(filter(&p, &n, &s).1.is_empty());
    }

    #[test]
    fn ephemeral_volume_pvc_must_be_owned_by_pod() {
        let p =
            pod(json!([{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]));
        let mut c = bound_pvc("p-scratch", "pv1");
        let s = VolumeSnapshot {
            pvcs: vec![c.clone()],
            ..Default::default()
        };
        match pre_filter(&p, &s) {
            PreFilterOutcome::Error(m) | PreFilterOutcome::Unresolvable(m) => {
                assert!(m.contains("was not created for pod ns/p"), "{m}")
            }
            _ => panic!("expected failure for un-owned ephemeral PVC"),
        }
        c.metadata.owner_references = Some(vec![serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "name": "p", "uid": "pod-uid", "controller": true
        }))
        .unwrap()]);
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        assert!(matches!(pre_filter(&p, &s), PreFilterOutcome::Ready(_)));
    }

    #[test]
    fn missing_ephemeral_pvc_waits_for_controller() {
        let p =
            pod(json!([{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]));
        match pre_filter(&p, &VolumeSnapshot::default()) {
            PreFilterOutcome::Unresolvable(m) => assert_eq!(
                m,
                "waiting for ephemeral volume controller to create the persistentvolumeclaim \"p-scratch\""
            ),
            _ => panic!("expected Unresolvable"),
        }
    }

    // ---- scorer_test.go TestScore ----

    fn scaled(points: &[(i64, i64)]) -> Vec<FunctionShapePoint> {
        points
            .iter()
            .map(|&(u, sc)| FunctionShapePoint {
                utilization: u,
                score: sc * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
            })
            .collect()
    }
    /// `defaultShapePoint` of volume_binding_test.go:80-89 (prefer high utilization).
    fn prefer_utilization() -> Vec<FunctionShapePoint> {
        scaled(&[(0, 0), (100, 10)])
    }
    fn res(requested: i64, capacity: i64) -> StorageResource {
        StorageResource {
            requested,
            capacity,
        }
    }
    fn classes(items: &[(&str, i64, i64)]) -> ClassResourceMap {
        items
            .iter()
            .map(|&(c, r, cap)| (c.to_string(), res(r, cap)))
            .collect()
    }

    #[test]
    fn scorer_default_shape_single_class() {
        let f = build_scorer_function(&prefer_utilization());
        for (req, want) in [(0, 0), (30, 30), (50, 50), (100, 100)] {
            assert_eq!(f(&classes(&[("hdd", req, 100)])), want, "req {req}");
        }
    }

    #[test]
    fn scorer_default_shape_multiple_classes() {
        let f = build_scorer_function(&prefer_utilization());
        for (hdd, ssd, want) in [
            (0, 0, 0),
            (0, 30, 15),
            (30, 30, 30),
            (30, 60, 45),
            (50, 50, 50),
            (50, 100, 75),
            (100, 100, 100),
        ] {
            assert_eq!(
                f(&classes(&[("hdd", hdd, 100), ("ssd", ssd, 100)])),
                want,
                "hdd {hdd} ssd {ssd}"
            );
        }
    }

    #[test]
    fn scorer_custom_shape_multiple_classes() {
        let f = build_scorer_function(&[
            FunctionShapePoint {
                utilization: 50,
                score: 0,
            },
            FunctionShapePoint {
                utilization: 80,
                score: 30,
            },
            FunctionShapePoint {
                utilization: 100,
                score: 50,
            },
        ]);
        for (hdd, ssd, want) in [
            (0, 0, 0),
            (0, 30, 0),
            (30, 30, 0),
            (30, 60, 5),
            (50, 100, 25),
            (90, 90, 40),
            (100, 100, 50),
        ] {
            assert_eq!(
                f(&classes(&[("hdd", hdd, 100), ("ssd", ssd, 100)])),
                want,
                "hdd {hdd} ssd {ssd}"
            );
        }
    }

    #[test]
    fn scorer_empty_map_zero_capacity_and_overcommit() {
        let f = build_scorer_function(&prefer_utilization());
        assert_eq!(f(&ClassResourceMap::new()), 0);
        // capacity == 0 || requested > capacity => rawScoringFunction(maxUtilization)
        assert_eq!(f(&classes(&[("hdd", 10, 0)])), 100);
        assert_eq!(f(&classes(&[("hdd", 200, 100)])), 100);
    }

    #[test]
    fn default_shape_is_max_score_at_zero_utilization() {
        // SetDefaults_VolumeBindingArgs (apis/config/v1/defaults.go:196-207)
        let f = build_scorer_function(&default_shape());
        assert_eq!(f(&classes(&[("c", 0, 100)])), 100);
        assert_eq!(f(&classes(&[("c", 100, 100)])), 0);
    }

    // ---- volume_binding_test.go score cases ----

    fn node_named(name: &str) -> Node {
        node(name, json!({"kubernetes.io/hostname": name, "node": name}))
    }
    fn sized_pvc(name: &str, class: &str, size: &str) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"storageClassName": class, "resources": {"requests": {"storage": size}}}),
            json!({}),
            json!({"phase": "Pending"}),
        )
    }
    fn host_affinity(node: &str) -> Value {
        json!({"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}})
    }
    fn node_capacity(node: &str, class: &str, size: &str) -> CSIStorageCapacity {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
            "metadata": {"name": format!("cap-{node}"), "namespace": "ns"},
            "storageClassName": class, "capacity": size,
            "nodeTopology": {"matchLabels": {"node": node}}
        }))
        .unwrap()
    }
    fn pod_with_claims(claims: &[&str]) -> Pod {
        pod(serde_json::Value::Array(
            claims
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({"name": format!("v{i}"), "persistentVolumeClaim": {"claimName": c}})
                })
                .collect(),
        ))
    }
    /// Filter every node (keeping `PodVolumes` per node like `podVolumesByNode`),
    /// then PreScore + Score. Filter-failed nodes score 0 (upstream's test
    /// expects 0 for them).
    fn run_scores(
        p: &Pod,
        nodes: &[Node],
        s: &VolumeSnapshot,
        shape: &[FunctionShapePoint],
    ) -> Vec<i64> {
        let claims = ready(p, s);
        let mut by_node = HashMap::new();
        for n in nodes {
            let (vols, reasons) = find_pod_volumes(p, &claims, n, s).unwrap();
            if reasons.is_empty() {
                by_node.insert(n.metadata.name.clone(), vols);
            }
        }
        assert!(!pre_score_skip(&by_node, true));
        let scorer = build_scorer_function(shape);
        nodes
            .iter()
            .map(|n| match by_node.get(&n.metadata.name) {
                Some(v) => score(Some(v), &scorer, true),
                None => 0,
            })
            .collect()
    }
    fn wait_class(name: &str, provisioner: &str) -> StorageClass {
        class(name, "WaitForFirstConsumer", provisioner, None)
    }
    fn local_pvs(size_a: &str, size_b: &str, class: &str, tag: &str) -> Vec<PersistentVolume> {
        vec![
            pv(
                &format!("pv-a-{tag}0"),
                class,
                size_a,
                Some(host_affinity("node-a")),
            ),
            pv(
                &format!("pv-a-{tag}1"),
                class,
                size_a,
                Some(host_affinity("node-a")),
            ),
            pv(
                &format!("pv-b-{tag}0"),
                class,
                size_b,
                Some(host_affinity("node-b")),
            ),
            pv(
                &format!("pv-b-{tag}1"),
                class,
                size_b,
                Some(host_affinity("node-b")),
            ),
        ]
    }
    const NOPROV: &str = "kubernetes.io/no-provisioner";

    fn three_nodes() -> [Node; 3] {
        [
            node_named("node-a"),
            node_named("node-b"),
            node_named("node-c"),
        ]
    }

    #[test]
    fn score_local_volumes_with_close_capacity_are_preferred() {
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("pvc-a", "wait", "50Gi")],
            pvs: local_pvs("200Gi", "100Gi", "wait", ""),
            classes: vec![wait_class("wait", NOPROV)],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-a"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![25, 50, 0]);
    }

    #[test]
    fn score_local_volumes_with_close_capacity_multiple_pvcs() {
        let mut pvs = local_pvs("200Gi", "100Gi", "wait", "");
        pvs.extend(local_pvs("200Gi", "100Gi", "wait-hdd", "h"));
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-0", "wait", "50Gi"),
                sized_pvc("pvc-1", "wait-hdd", "100Gi"),
            ],
            pvs,
            classes: vec![wait_class("wait", NOPROV), wait_class("wait-hdd", NOPROV)],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-0", "pvc-1"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![38, 75, 0]);
    }

    fn dynamic_snapshot(sizes: &[(&str, &str)], request: &str) -> (VolumeSnapshot, Vec<Node>) {
        let nodes: Vec<Node> = sizes.iter().map(|(n, _)| node_named(n)).collect();
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("pvc-dynamic", "wait-cap", request)],
            classes: vec![wait_class("wait-cap", "example.com/csi")],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: sizes
                .iter()
                .map(|(n, sz)| node_capacity(n, "wait-cap", sz))
                .collect(),
            ..Default::default()
        };
        (s, nodes)
    }

    #[test]
    fn score_storage_capacity_prefers_least_free() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "50Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![10, 20, 100]);
    }

    #[test]
    fn score_prefer_node_with_least_allocatable() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "20Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![10, 50, 100]);
    }

    #[test]
    fn score_prefer_node_with_maximum_allocatable_custom_shape() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "20Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &default_shape(),
        );
        assert_eq!(got, vec![90, 50, 0]);
    }

    #[test]
    fn score_dynamic_provisioning_same_class_uses_capacity_once() {
        // Capacity is assigned (not summed) per class: 2 x 50Gi on 100Gi => 100.
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-dynamic-0", "wait-cap", "50Gi"),
                sized_pvc("pvc-dynamic-1", "wait-cap", "50Gi"),
            ],
            classes: vec![wait_class("wait-cap", "example.com/csi")],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: vec![node_capacity("node-a", "wait-cap", "100Gi")],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic-0", "pvc-dynamic-1"]),
            &[node_named("node-a")],
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![100]);
    }

    #[test]
    fn score_with_static_binds_ignores_dynamic_provisions() {
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-dynamic", "wait-cap", "10Gi"),
                sized_pvc("pvc-static", "wait", "50Gi"),
            ],
            pvs: ["a", "b", "c"]
                .iter()
                .map(|n| {
                    pv(
                        &format!("pv-static-{n}"),
                        "wait",
                        "100Gi",
                        Some(host_affinity(&format!("node-{n}"))),
                    )
                })
                .collect(),
            classes: vec![
                wait_class("wait", NOPROV),
                wait_class("wait-cap", "example.com/csi"),
            ],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: vec![
                node_capacity("node-a", "wait-cap", "100Gi"),
                node_capacity("node-b", "wait-cap", "50Gi"),
                node_capacity("node-c", "wait-cap", "10Gi"),
            ],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic", "pvc-static"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![50, 50, 50]);
    }

    #[test]
    fn pre_score_skip_rules() {
        // volume_binding.go PreScore: skip unless static bindings exist or
        // StorageCapacityScoring is on.
        let mut by_node = HashMap::new();
        by_node.insert("n".to_string(), PodVolumes::default());
        assert!(pre_score_skip(&by_node, false));
        assert!(!pre_score_skip(&by_node, true));
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("a", "wait", "1Gi")],
            pvs: vec![pv("pv", "wait", "2Gi", None)],
            classes: vec![wait_class("wait", NOPROV)],
            ..Default::default()
        };
        let (vols, _) = filter(&pod_with_claims(&["a"]), &node_named("n"), &s);
        let mut with_static = HashMap::new();
        with_static.insert("n".to_string(), vols);
        assert!(!pre_score_skip(&with_static, false));
    }

    #[test]
    fn score_without_pod_volumes_for_node_is_zero() {
        let scorer = build_scorer_function(&prefer_utilization());
        assert_eq!(score(None, &scorer, true), 0);
    }
}
