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

pub const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
const ANN_BIND_COMPLETED: &str = "pv.kubernetes.io/bind-completed";
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";
/// `volume.NotSupportedProvisioner` (pkg/volume/util/storageclass.go).
const NOT_SUPPORTED_PROVISIONER: &str = "kubernetes.io/no-provisioner";

pub fn pvc_key(pvc: &PersistentVolumeClaim) -> String {
    format!(
        "{}/{}",
        pvc.metadata.namespace.as_deref().unwrap_or(""),
        pvc.metadata.name
    )
}

/// `GetPersistentVolumeClaimClass` (component-helpers `helpers.go:43-53`):
/// beta annotation first, then `spec.storageClassName`.
pub fn claim_class(pvc: &PersistentVolumeClaim) -> String {
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
pub(crate) fn ephemeral_claim_name(pod: &Pod, vol_name: &str) -> String {
    format!("{}-{}", pod.metadata.name, vol_name)
}

/// `ephemeral.VolumeIsForPod` (`ephemeral.go:49-56`): the PVC must be in the
/// pod's namespace and controlled (`metav1.IsControlledBy`) by the pod.
pub fn ephemeral_volume_is_for_pod(pod: &Pod, pvc: &PersistentVolumeClaim) -> Result<(), String> {
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
pub fn volume_pvc_name(pod: &Pod, vol: &Volume) -> Option<(String, bool)> {
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

pub fn pod_volumes(pod: &Pod) -> &[Volume] {
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
pub fn is_pvc_fully_bound(pvc: &PersistentVolumeClaim) -> bool {
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
pub fn check_pv_node_affinity(pv: &PersistentVolume, node: &Node) -> bool {
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
pub fn is_volume_bound_to_claim(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> bool {
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
            .access_modes_slice()
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
pub fn try_translate_pv_to_csi<'a>(
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
pub fn build_broken_linear_function(shape: Vec<FunctionShapePoint>) -> impl Fn(i64) -> i64 {
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
