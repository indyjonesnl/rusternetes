//! Unit tests moved from `src/validation/controllerrevision.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::ControllerRevision;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_immutable_field, validate_nonnegative_field,
        validate_object_meta, validate_object_meta_update,
    };
}

mod update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::controllerrevision::*;

    fn cr(rev: i64, data: serde_json::Value) -> ControllerRevision {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "rev1", "namespace": "default", "resourceVersion": "1"},
            "revision": rev,
            "data": data,
        }))
        .unwrap()
    }

    #[test]
    fn revision_may_change_data_may_not() {
        let old = cr(1, serde_json::json!({"k": "v"}));
        // Only revision changes -> allowed.
        let bumped = cr(2, serde_json::json!({"k": "v"}));
        assert!(validate_controller_revision_update(&bumped, &old).is_empty());
        // data changes -> immutable error.
        let changed = cr(1, serde_json::json!({"k": "w"}));
        let errs = validate_controller_revision_update(&changed, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "data" && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    /// `validateControllerRevision` starts with `ValidateObjectMeta`
    /// (validation.go:334): a namespaced DNS-subdomain name.
    #[test]
    fn metadata_is_validated() {
        let mut bad = cr(1, serde_json::json!({"k": "v"}));
        bad.metadata.name = "Not_A_Subdomain".into();
        assert!(validate_controller_revision(&bad)
            .iter()
            .any(|e| e.field == "metadata.name"));
        let mut unscoped = cr(1, serde_json::json!({"k": "v"}));
        unscoped.metadata.namespace = None;
        assert!(validate_controller_revision(&unscoped)
            .iter()
            .any(|e| e.field == "metadata.namespace"));
    }

    /// `ValidateControllerRevisionUpdate` (validation.go:361-367) does not
    /// re-run the create-only `data` check, and validates the metadata update.
    #[test]
    fn update_skips_the_data_shape_check_and_checks_metadata() {
        let mut old = cr(1, serde_json::Value::Null);
        old.data = None;
        let new = old.clone();
        assert!(validate_controller_revision(&new)
            .iter()
            .any(|e| e.field == "data"));
        assert!(validate_controller_revision_update(&new, &old).is_empty());

        let mut renamespaced = cr(1, serde_json::json!({"k": "v"}));
        renamespaced.metadata.namespace = Some("other".into());
        let errs = validate_controller_revision_update(
            &renamespaced,
            &cr(1, serde_json::json!({"k": "v"})),
        );
        assert!(
            errs.iter().any(|e| e.field == "metadata.namespace"),
            "{errs:?}"
        );
    }
}
