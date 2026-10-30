//! The `StorageObjectInUseProtection` admission plugin.
//!
//! Port of `plugin/pkg/admission/storage/storageobjectinuseprotection/
//! admission.go` (`Admit`, `admitPV`, `admitPVC`): on CREATE of a core
//! PersistentVolume or PersistentVolumeClaim (not a subresource) add the
//! `kubernetes.io/pv-protection` / `kubernetes.io/pvc-protection` finalizer
//! unless it is already present. The finalizer is removed by the
//! pv-protection / pvc-protection controllers once the object is unused
//! (`pkg/controller/volume/{pvprotection,pvcprotection}`).
//!
//! `admitVAC` (admission.go:132-151) does the same for a `storage.k8s.io`
//! VolumeAttributesClass, gated by the `VolumeAttributesClass` feature gate
//! (admission.go:76-80); the vac-protection controller removes the finalizer.

use rusternetes_common::resources::volume::{
    PVC_PROTECTION_FINALIZER, PV_PROTECTION_FINALIZER, VAC_PROTECTION_FINALIZER,
};
use rusternetes_common::types::ObjectMeta;

/// `admitPV` / `admitPVC` body: append `finalizer` unless present.
pub fn add_protection_finalizer(meta: &mut ObjectMeta, finalizer: &str) {
    let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
    if !finalizers.iter().any(|f| f == finalizer) {
        finalizers.push(finalizer.to_string());
    }
}

/// The finalizer `Admit` adds for `group`/`resource` (the switch on
/// `a.GetResource().GroupResource()`, admission.go:71-84), if any.
/// `vac_enabled` is the `VolumeAttributesClass` gate.
pub fn finalizer_for(group: &str, resource: &str, vac_enabled: bool) -> Option<&'static str> {
    match (group, resource) {
        ("", "persistentvolumes") => Some(PV_PROTECTION_FINALIZER),
        ("", "persistentvolumeclaims") => Some(PVC_PROTECTION_FINALIZER),
        ("storage.k8s.io", "volumeattributesclasses") if vac_enabled => {
            Some(VAC_PROTECTION_FINALIZER)
        }
        _ => None,
    }
}
