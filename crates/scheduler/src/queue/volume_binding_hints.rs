//! VolumeBinding queueing hints.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go`:
//! `EventsToRegister` (`:105-141`, wired in `hints.rs`),
//! `isSchedulableAfterCSINodeChange` (`:143-168`),
//! `isSchedulableAfterPersistentVolumeClaimChange` (`:170-210`),
//! `isSchedulableAfterStorageClassChange` (`:212-245`),
//! `isSchedulableAfterCSIStorageCapacityChange` (`:247-293`),
//! `isSchedulableAfterCSIDriverChange` (`:295-322`) and
//! `volumeLimit` (`binder.go:1029-1035`);
//! tests: `volume_binding_test.go` `TestIsSchedulableAfterCSINodeChange`
//! (`:1192`), `TestIsSchedulableAfterPersistentVolumeClaimChange` (`:1298`),
//! `TestIsSchedulableAfterStorageClassChange` (`:1395`),
//! `TestIsSchedulableAfterCSIStorageCapacityChange` (`:1483`),
//! `TestIsSchedulableAfterCSIDriverChange` (`:1712`).
//!
//! The PersistentVolume and Node events carry no hint upstream either.

use super::events::*;
use crate::volume_binding::{pod_volumes, volume_pvc_name};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{
    CSIDriver, CSINode, CSIStorageCapacity, PersistentVolumeClaim, Pod, StorageClass,
};
use tracing::debug;

/// `v1.MigratedPluginsAnnotationKey`.
pub const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `isSchedulableAfterCSINodeChange`.
pub fn is_schedulable_after_csi_node_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    if old_obj.is_none() {
        debug!("CSINode creation could make the pod schedulable");
        return Ok(QueueingHint::Queue);
    }
    let (old, modified) = as_typed::<CSINode>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterCSINodeChange: {e}"))?;
    let (Some(old), Some(modified)) = (old, modified) else {
        return Err("unexpected objects in isSchedulableAfterCSINodeChange: nil".to_string());
    };
    let annotation = |n: &CSINode| {
        n.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(MIGRATED_PLUGINS_ANNOTATION_KEY))
            .cloned()
            .unwrap_or_default()
    };
    if annotation(old) != annotation(modified) {
        debug!(pod = %pod_key(pod), csi_node = %modified.metadata.name, "CSINode's migrated plugins annotation is updated and that may make the pod schedulable");
        return Ok(QueueingHint::Queue);
    }
    debug!(pod = %pod_key(pod), csi_node = %modified.metadata.name, "CSINode was created or updated but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterPersistentVolumeClaimChange`.
pub fn is_schedulable_after_pvc_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (_, new_pvc) = as_typed::<PersistentVolumeClaim>(old_obj, new_obj).map_err(|e| {
        format!("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: {e}")
    })?;
    let new_pvc = new_pvc.ok_or_else(|| {
        "unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: nil".to_string()
    })?;
    if pod.metadata.namespace != new_pvc.metadata.namespace {
        debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable because the PVC belongs to a different namespace");
        return Ok(QueueingHint::QueueSkip);
    }
    for vol in pod_volumes(pod) {
        let Some((pvc_name, _)) = volume_pvc_name(pod, vol) else {
            continue;
        };
        if pvc_name == new_pvc.metadata.name {
            // Return Queue because, in this case, all PVC creations and
            // almost all PVC updates could make the Pod schedulable.
            debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim the pod requires was created or updated, potentially making the target Pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `apiequality.Semantic.DeepEqual` on `AllowedTopologies`: nil and empty
/// slices are equal.
fn allowed_topologies_equal(a: &StorageClass, b: &StorageClass) -> bool {
    type Terms = Vec<Vec<(String, Vec<String>)>>;
    fn norm(sc: &StorageClass) -> Terms {
        sc.allowed_topologies
            .iter()
            .flatten()
            .map(|t| {
                t.match_label_expressions
                    .iter()
                    .flatten()
                    .map(|r| (r.key.clone(), r.values.clone()))
                    .collect()
            })
            .collect()
    }
    norm(a) == norm(b)
}

/// `isSchedulableAfterStorageClassChange`: any StorageClass addition and a
/// StorageClass update to allowedTopologies might make a Pod schedulable.
pub fn is_schedulable_after_storage_class_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (old, new) = as_typed::<StorageClass>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterStorageClassChange: {e}"))?;
    let new = new.ok_or("unexpected objects in isSchedulableAfterStorageClassChange: nil")?;
    let Some(old) = old else {
        // No further filtering can be made for a creation event.
        debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "A new StorageClass was created, which could make a Pod schedulable");
        return Ok(QueueingHint::Queue);
    };
    if !allowed_topologies_equal(old, new) {
        debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "StorageClass got an update in AllowedTopologies");
        return Ok(QueueingHint::Queue);
    }
    debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "StorageClass was updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `volumeLimit` (`binder.go:1029-1035`): prefer `maximumVolumeSize`, it is
/// more precise.
fn volume_limit(capacity: &CSIStorageCapacity) -> Option<Quantity> {
    capacity
        .maximum_volume_size
        .as_deref()
        .or(capacity.capacity.as_deref())
        .and_then(|q| Quantity::parse(q).ok())
}

/// `isSchedulableAfterCSIStorageCapacityChange`: any CSIStorageCapacity
/// addition and an update that raises the volume limit might make a Pod
/// schedulable.
pub fn is_schedulable_after_csi_storage_capacity_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (old, new) = as_typed::<CSIStorageCapacity>(old_obj, new_obj).map_err(|e| {
        format!("unexpected objects in isSchedulableAfterCSIStorageCapacityChange: {e}")
    })?;
    let new = new.ok_or("unexpected objects in isSchedulableAfterCSIStorageCapacityChange: nil")?;
    let Some(old) = old else {
        debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "A new CSIStorageCapacity was created, which could make a Pod schedulable");
        return Ok(QueueingHint::Queue);
    };
    let old_limit = volume_limit(old);
    let new_limit = volume_limit(new);
    if let Some(new_limit) = new_limit {
        if old_limit.is_none_or(|o| new_limit.value() > o.value()) {
            debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "VolumeLimit was increased, which could make a Pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "CSIStorageCapacity was updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterCSIDriverChange`: disabling storage capacity tracking
/// on a driver the pod uses may make the pod schedulable.
pub fn is_schedulable_after_csi_driver_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (original, modified) = as_typed::<CSIDriver>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterCSIDriverChange: {e}"))?;
    let (Some(original), Some(modified)) = (original, modified) else {
        return Err("unexpected objects in isSchedulableAfterCSIDriverChange: nil".to_string());
    };
    for vol in pod_volumes(pod) {
        let Some(csi) = vol.csi.as_ref() else {
            continue;
        };
        if csi.driver != modified.metadata.name {
            continue;
        }
        if original.spec.storage_capacity == Some(true)
            && modified.spec.storage_capacity != Some(true)
        {
            debug!(pod = %pod_key(pod), driver = %modified.metadata.name, "CSIDriver was updated and storage capacity got disabled, which may make the pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), driver = %modified.metadata.name, "CSIDriver was created or updated but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}
