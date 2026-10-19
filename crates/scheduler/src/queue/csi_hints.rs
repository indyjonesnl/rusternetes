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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn pod(ns: &str, name: &str, node: &str, vols: Value) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": ns},
            "spec": {"nodeName": node, "volumes": vols}
        }))
        .unwrap()
    }
    fn pvc_vol(claim: &str) -> Value {
        json!({"name": "v", "persistentVolumeClaim": {"claimName": claim}})
    }
    fn ebs_vol() -> Value {
        json!({"name": "i", "awsElasticBlockStore": {"volumeID": "test"}})
    }
    fn gce_vol() -> Value {
        json!({"name": "g", "gcePersistentDisk": {"pdName": "test"}})
    }
    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }
    fn pvc(ns: &str, name: &str) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": ns}, "spec": {}
        }))
        .unwrap()
    }
    fn va(attacher: &str) -> VolumeAttachment {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
            "metadata": {"name": "volumeattachment1"},
            "spec": {"attacher": attacher, "nodeName": "fake-node",
                     "source": {"persistentVolumeName": "pv1"}}
        }))
        .unwrap()
    }

    /// `TestCSILimitsQHint` (csi_test.go:668).
    #[test]
    fn pod_deleted() {
        let target = pod("", "t", "", json!([pvc_vol("csi-ebs.csi.aws.com-2")]));
        let cases = [
            (
                "deleted pod has a PVC",
                pod("", "d", "n", json!([pvc_vol("placeholder")])),
                QueueingHint::Queue,
            ),
            (
                "deleted pod has an inline migratable volume",
                pod("", "d", "n", json!([ebs_vol()])),
                QueueingHint::Queue,
            ),
            (
                "deleted pod has no volume",
                pod("", "d", "n", json!([])),
                QueueingHint::QueueSkip,
            ),
            (
                "deleted pod is not scheduled",
                pod("", "d", "", json!([pvc_vol("csi-ebs.csi.aws.com-0")])),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, deleted, want) in cases {
            let o = obj(deleted);
            assert_eq!(
                is_schedulable_after_pod_deleted(&target, Some(&*o), None).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `TestCSILimitsAddedPVCQHint` (csi_test.go:733).
    #[test]
    fn pvc_added() {
        let eph = json!([{"name": "ephemeral", "ephemeral": {}}]);
        let cases = [
            (
                "different namespace",
                pod("ns1", "p", "", json!([])),
                pvc("ns2", ""),
                QueueingHint::QueueSkip,
            ),
            (
                "same namespace, same PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                pvc("ns1", "pvc1"),
                QueueingHint::Queue,
            ),
            (
                "ephemeral volume related to the added PVC",
                pod("ns1", "pod1", "", eph),
                pvc("ns1", "pod1-ephemeral"),
                QueueingHint::Queue,
            ),
            (
                "pod doesn't have the same PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                pvc("ns1", "pvc2"),
                QueueingHint::QueueSkip,
            ),
            (
                "pod has no PVC",
                pod("ns1", "p", "", json!([])),
                pvc("ns1", "pvc2"),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, p, added, want) in cases {
            let o = obj(added);
            assert_eq!(
                is_schedulable_after_pvc_added(&p, None, Some(&*o)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `TestCSILimitsDeletedVolumeAttachmentQHint` (csi_test.go:810).
    #[test]
    fn volume_attachment_deleted() {
        let cases = [
            (
                "pod has PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                "test.storage.gke.io",
                QueueingHint::Queue,
            ),
            (
                "inline migratable (EBS) matching attacher",
                pod("ns1", "p", "", json!([ebs_vol()])),
                "ebs.csi.aws.com",
                QueueingHint::Queue,
            ),
            (
                "inline migratable (GCE PD) vs EBS attacher",
                pod("ns1", "p", "", json!([gce_vol()])),
                "ebs.csi.aws.com",
                QueueingHint::QueueSkip,
            ),
            (
                "inline migratable and PVC",
                pod("ns1", "p", "", json!([ebs_vol(), pvc_vol("pvc1")])),
                "test.storage.gke.io",
                QueueingHint::Queue,
            ),
            (
                "no PVC",
                pod("ns1", "p", "", json!([])),
                "test.storage.gke.io",
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, p, attacher, want) in cases {
            let o = obj(va(attacher));
            assert_eq!(
                is_schedulable_after_volume_attachment_deleted(&p, Some(&*o), None).unwrap(),
                want,
                "{name}"
            );
        }
    }

    fn csi_node(drivers: Value) -> CSINode {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
            "metadata": {"name": "test-node"}, "spec": {"drivers": drivers}
        }))
        .unwrap()
    }
    fn driver(name: &str, count: Option<i32>) -> Value {
        let mut d = json!({"name": name, "nodeID": "test-node"});
        if let Some(c) = count {
            d["allocatable"] = json!({"count": c});
        }
        d
    }

    /// `TestCSILimitsAfterCSINodeUpdatedQHint` (csi_test.go:983).
    #[test]
    fn csi_node_updated() {
        let p = pod(
            "ns1",
            "test-pod",
            "",
            json!([pvc_vol("csi-ebs.csi.aws.com-0")]),
        );
        let e = "ebs.csi.aws.com";
        let cases = [
            (
                "limit raised",
                vec![driver(e, Some(1))],
                vec![driver(e, Some(2))],
                QueueingHint::Queue,
            ),
            (
                "limit decreased",
                vec![driver(e, Some(2))],
                vec![driver(e, Some(1))],
                QueueingHint::QueueSkip,
            ),
            (
                "limit unchanged",
                vec![driver(e, Some(1))],
                vec![driver(e, Some(1))],
                QueueingHint::QueueSkip,
            ),
            (
                "driver added with limit",
                vec![],
                vec![driver(e, Some(1))],
                QueueingHint::Queue,
            ),
            (
                "driver added without limit",
                vec![],
                vec![driver(e, None)],
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, old, new, want) in cases {
            let (o, n) = (obj(csi_node(json!(old))), obj(csi_node(json!(new))));
            assert_eq!(
                is_schedulable_after_csi_node_updated(&p, Some(&*o), Some(&*n)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn wrong_object_type_is_an_error() {
        let p = pod("ns1", "p", "", json!([]));
        let o = obj(p.clone());
        assert!(is_schedulable_after_pvc_added(&p, None, Some(&*o)).is_err());
    }
}
