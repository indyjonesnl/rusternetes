//! Unit tests moved from `src/admission/storage_object_in_use_protection.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::volume::{
        PVC_PROTECTION_FINALIZER, PV_PROTECTION_FINALIZER, VAC_PROTECTION_FINALIZER,
    };
    pub use rusternetes_common::types::ObjectMeta;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::storage_object_in_use_protection::*;

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
            finalizer_for("", "persistentvolumes", true),
            Some("kubernetes.io/pv-protection")
        );
        assert_eq!(
            finalizer_for("", "persistentvolumeclaims", true),
            Some("kubernetes.io/pvc-protection")
        );
        assert_eq!(finalizer_for("", "pods", true), None);
    }

    #[test]
    fn finalizer_for_vac_needs_group_and_gate() {
        let vac = "volumeattributesclasses";
        assert_eq!(
            finalizer_for("storage.k8s.io", vac, true),
            Some("kubernetes.io/vac-protection")
        );
        assert_eq!(finalizer_for("storage.k8s.io", vac, false), None);
        assert_eq!(finalizer_for("", vac, true), None);
    }

    #[test]
    fn finalizer_for_pv_wrong_group_is_none() {
        assert_eq!(
            finalizer_for("example.com", "persistentvolumes", true),
            None
        );
    }
}
