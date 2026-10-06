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
//! Not ported: the `VolumeAttributesClass` branch (`admitVAC`); there is no
//! VAC protection controller here.

use rusternetes_common::resources::volume::{PVC_PROTECTION_FINALIZER, PV_PROTECTION_FINALIZER};
use rusternetes_common::types::ObjectMeta;

/// `admitPV` / `admitPVC` body: append `finalizer` unless present.
pub fn add_protection_finalizer(meta: &mut ObjectMeta, finalizer: &str) {
    let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
    if !finalizers.iter().any(|f| f == finalizer) {
        finalizers.push(finalizer.to_string());
    }
}

/// The finalizer `Admit` adds for core resource `resource`, if any.
pub fn finalizer_for(resource: &str) -> Option<&'static str> {
    match resource {
        "persistentvolumes" => Some(PV_PROTECTION_FINALIZER),
        "persistentvolumeclaims" => Some(PVC_PROTECTION_FINALIZER),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_finalizer_when_absent() {
        let mut m = ObjectMeta::new("pvc");
        add_protection_finalizer(&mut m, PVC_PROTECTION_FINALIZER);
        assert_eq!(
            m.finalizers,
            Some(vec!["kubernetes.io/pvc-protection".to_string()])
        );
    }

    #[test]
    fn appends_after_existing_finalizers() {
        let mut m = ObjectMeta::new("pv");
        m.finalizers = Some(vec!["example.com/x".to_string()]);
        add_protection_finalizer(&mut m, PV_PROTECTION_FINALIZER);
        assert_eq!(
            m.finalizers,
            Some(vec![
                "example.com/x".to_string(),
                "kubernetes.io/pv-protection".to_string()
            ])
        );
    }

    #[test]
    fn does_not_duplicate_finalizer() {
        let mut m = ObjectMeta::new("pv");
        m.finalizers = Some(vec!["kubernetes.io/pv-protection".to_string()]);
        add_protection_finalizer(&mut m, PV_PROTECTION_FINALIZER);
        assert_eq!(m.finalizers.unwrap().len(), 1);
    }

    #[test]
    fn finalizer_for_resource() {
        assert_eq!(
            finalizer_for("persistentvolumes"),
            Some("kubernetes.io/pv-protection")
        );
        assert_eq!(
            finalizer_for("persistentvolumeclaims"),
            Some("kubernetes.io/pvc-protection")
        );
        assert_eq!(finalizer_for("pods"), None);
    }
}
