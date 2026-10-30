//! NodeVolumeLimits queueing hints.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/nodevolumelimits/csi.go`:
//! `EventsToRegister` (`:88-98`), `isSchedulableAfterPodDeleted` (`:100-122`),
//! `isSchedulableAfterPVCAdded` (`:124-154`),
//! `isSchedulableAfterVolumeAttachmentDeleted` (`:156-192`),
//! `isSchedulableAfterCSINodeUpdated` (`:194-236`);
//! tests: `csi_test.go` `TestCSILimitsQHint` (`:668`),
//! `TestCSILimitsAddedPVCQHint` (`:733`),
//! `TestCSILimitsDeletedVolumeAttachmentQHint` (`:810`),
//! `TestCSILimitsAfterCSINodeUpdatedQHint` (`:983`).

use super::events::*;
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::resources::{
    CSINode, CSINodeDriver, PersistentVolumeClaim, Pod, Volume, VolumeAttachment,
};
use std::collections::HashMap;
use tracing::debug;

fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

fn volumes(pod: &Pod) -> &[Volume] {
    crate::volume_binding::pod_volumes(pod)
}

/// `isSchedulableAfterPodDeleted` (`csi.go:100-122`): a deleted pod that was
/// scheduled (or nominated) and used PVCs, ephemeral or inline-migratable
/// volumes may have freed attach slots.
pub fn is_schedulable_after_pod_deleted(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (deleted, _) = as_typed::<Pod>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterPodDeleted: {e}"))?;
    let deleted = deleted.ok_or("unexpected objects in isSchedulableAfterPodDeleted: no pod")?;

    if volumes(deleted).is_empty() {
        return Ok(QueueingHint::QueueSkip);
    }
    let node_name = deleted
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("");
    let nominated = deleted
        .status
        .as_ref()
        .and_then(|s| s.nominated_node_name.as_deref())
        .unwrap_or("");
    if node_name.is_empty() && nominated.is_empty() {
        return Ok(QueueingHint::QueueSkip);
    }
    let translator = CsiTranslator::new();
    for vol in volumes(deleted) {
        if vol.persistent_volume_claim.is_some()
            || vol.ephemeral.is_some()
            || translator.is_inline_migratable(vol)
        {
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), deleted_pod = %pod_key(deleted), "The deleted pod does not impact the scheduling of the unscheduled pod");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterPVCAdded` (`csi.go:124-154`): an added PVC in the pod's
/// namespace that one of its PVC / ephemeral volumes refers to.
pub fn is_schedulable_after_pvc_added(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (_, added) = as_typed::<PersistentVolumeClaim>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterPVCAdded: {e}"))?;
    let added = added.ok_or("unexpected objects in isSchedulableAfterPVCAdded: no PVC")?;

    if added.metadata.namespace != pod.metadata.namespace {
        return Ok(QueueingHint::QueueSkip);
    }
    for vol in volumes(pod) {
        let pvc_name = if let Some(p) = &vol.persistent_volume_claim {
            p.claim_name.clone()
        } else if vol.ephemeral.is_some() {
            // `ephemeral.VolumeClaimName` (ephemeral.go:41-43).
            crate::volume_binding::ephemeral_claim_name(pod, &vol.name)
        } else {
            // Volume is not using a PVC, ignore
            continue;
        };
        if pvc_name == added.metadata.name {
            debug!(pod = %pod_key(pod), pvc = %added.metadata.name, "PVC that is referred from the pod was created, which might make this pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), pvc = %added.metadata.name, "PVC irrelevant to the Pod was created, which doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterVolumeAttachmentDeleted` (`csi.go:156-192`): any PVC
/// volume, or an inline-migratable volume whose translated CSI driver is the
/// deleted attachment's attacher.
pub fn is_schedulable_after_volume_attachment_deleted(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (deleted, _) = as_typed::<VolumeAttachment>(old_obj, new_obj).map_err(|e| {
        format!("unexpected objects in isSchedulableAfterVolumeAttachmentDeleted: {e}")
    })?;
    let deleted = deleted
        .ok_or("unexpected objects in isSchedulableAfterVolumeAttachmentDeleted: no object")?;

    let translator = CsiTranslator::new();
    for vol in volumes(pod) {
        // Check if the pod volume uses a PVC. If it does, return Queue.
        if vol.persistent_volume_claim.is_some() {
            return Ok(QueueingHint::Queue);
        }
        if !translator.is_inline_migratable(vol) {
            continue;
        }
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        let translated = translator
            .translate_in_tree_inline_volume_to_csi(vol, ns)
            .map_err(|e| format!("converting volume({}) from inline to csi: {e}", vol.name))?;
        if let Some(csi) = translated.spec.csi.as_ref() {
            if deleted.spec.attacher == csi.driver {
                // deleted VolumeAttachment Attacher matches the translated PV CSI driver
                return Ok(QueueingHint::Queue);
            }
        }
    }
    debug!(pod = %pod_key(pod), volume_attachment = %deleted.metadata.name, "the VolumeAttachment deletion wouldn't make this pod schedulable because the pod has no volume related to a deleted VolumeAttachment");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterCSINodeUpdated` (`csi.go:194-236`): Queue only when a
/// driver's `allocatable.count` rose (a missing count or driver is 0).
pub fn is_schedulable_after_csi_node_updated(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (old, new) = as_typed::<CSINode>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterCSINodeUpdated: {e}"))?;
    let old =
        old.ok_or("unexpected objects in isSchedulableAfterCSINodeUpdated: no old CSINode")?;
    let new =
        new.ok_or("unexpected objects in isSchedulableAfterCSINodeUpdated: no new CSINode")?;

    let count = |d: &CSINodeDriver| d.allocatable.as_ref().and_then(|a| a.count).unwrap_or(0);
    let old_limits: HashMap<&str, i32> = old
        .spec
        .drivers
        .iter()
        .map(|d| (d.name.as_str(), count(d)))
        .collect();
    // Compare new driver limits vs. old. If limit increased, queue pod.
    for d in &new.spec.drivers {
        let old_limit = old_limits.get(d.name.as_str()).copied().unwrap_or(0);
        let new_limit = count(d);
        if new_limit > old_limit {
            debug!(pod = %pod_key(pod), driver = %d.name, old_limit, new_limit, "CSINode driver limit increased, might make this pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    // If no driver limit was increased, skip queueing.
    Ok(QueueingHint::QueueSkip)
}
