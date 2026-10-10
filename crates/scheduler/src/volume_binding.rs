//! VolumeBinding scheduler plugin: PreFilter + Filter (`FindPodVolumes`).
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/volumebinding/{volume_binding.go,binder.go}`
//! and `staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go`.

use rusternetes_common::affinity::matches_node_selector;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::{
    PersistentVolumeClaimPhase, PersistentVolumeMode, PersistentVolumePhase, TopologySelectorTerm,
    VolumeBindingMode,
};
use rusternetes_common::resources::{
    CSIDriver, CSIStorageCapacity, Node, PersistentVolume, PersistentVolumeClaim, Pod,
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
#[allow(dead_code)] // read by AssumePodVolumes/BindPodVolumes (follow-up #3017 steps 2-3)
pub struct BindingInfo {
    pub pv: PersistentVolume,
    pub pvc: PersistentVolumeClaim,
}

#[derive(Clone)]
#[allow(dead_code)] // read by AssumePodVolumes/BindPodVolumes (follow-up #3017 steps 2-3)
pub struct DynamicProvision {
    pub pvc: PersistentVolumeClaim,
    pub node_capacity: Option<CSIStorageCapacity>,
}

#[derive(Default, Clone)]
pub struct PodVolumes {
    pub static_bindings: Vec<BindingInfo>,
    pub dynamic_provisions: Vec<DynamicProvision>,
}

const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
const ANN_BIND_COMPLETED: &str = "pv.kubernetes.io/bind-completed";
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";
/// `volume.NotSupportedProvisioner` (pkg/volume/util/storageclass.go).
const NOT_SUPPORTED_PROVISIONER: &str = "kubernetes.io/no-provisioner";

fn pvc_key(pvc: &PersistentVolumeClaim) -> String {
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
fn ephemeral_volume_is_for_pod(pod: &Pod, pvc: &PersistentVolumeClaim) -> Result<(), String> {
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
fn volume_pvc_name(pod: &Pod, vol: &Volume) -> Option<(String, bool)> {
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

fn pod_volumes(pod: &Pod) -> &[Volume] {
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
fn is_pvc_fully_bound(pvc: &PersistentVolumeClaim) -> bool {
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
fn check_pv_node_affinity(pv: &PersistentVolume, node: &Node) -> bool {
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
fn is_volume_bound_to_claim(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> bool {
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

/// `checkBoundClaims` (binder.go:839-873) without the CSI-migration
/// translation. Returns (boundVolumesSatisfied, boundPVsFound).
fn check_bound_claims(
    claims: &[PersistentVolumeClaim],
    node: &Node,
    snap: &VolumeSnapshot,
) -> (bool, bool) {
    for pvc in claims {
        let pv_name = pvc.spec.volume_name.as_deref().unwrap_or("");
        let Some(pv) = snap.pv(pv_name) else {
            return (true, false);
        };
        if !check_pv_node_affinity(pv, node) {
            return (false, true);
        }
    }
    (true, true)
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
        (bound_satisfied, bound_pvs_found) = check_bound_claims(&claims.bound_claims, node, snap);
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
}
