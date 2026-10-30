//! Unit tests moved from `src/validation/namespace.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Namespace;
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_qualified_name;
    pub use rusternetes_common::validation::objectmeta::{
        validate_namespace_name, validate_object_meta, validate_object_meta_update,
    };
}

mod tests {
    use super::__parent::*;
    use chrono::Utc;
    use rusternetes_common::resources::NamespaceStatus;
    use rusternetes_common::validation::namespace::*;

    fn ns_with(phase: Option<Phase>, deleting: bool) -> Namespace {
        let mut ns = Namespace::new("test");
        ns.metadata.resource_version = Some("1".to_string());
        ns.status = Some(NamespaceStatus {
            phase,
            conditions: None,
        });
        ns.metadata.deletion_timestamp = if deleting { Some(Utc::now()) } else { None };
        ns
    }

    #[test]
    fn active_with_no_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Active), false);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn terminating_with_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Terminating), true);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn non_active_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Terminating), false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn missing_phase_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn non_terminating_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Active), true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }

    #[test]
    fn missing_phase_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }
}
