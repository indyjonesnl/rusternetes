//! Unit tests moved from `src/validation/storageversionmigration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::storageversionmigration::{
        StorageVersionMigration, MIGRATION_FAILED, MIGRATION_RUNNING, MIGRATION_SUCCEEDED,
    };
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1035_label, is_dns1123_subdomain, validate_conditions,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_immutable_field, validate_object_meta,
        validate_object_meta_update,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::storageversionmigration::GroupResource;
    use rusternetes_common::types::Condition;
    use rusternetes_common::validation::storageversionmigration::*;

    fn agg(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn svm(group: &str, resource: &str) -> StorageVersionMigration {
        let mut s = StorageVersionMigration::default();
        s.metadata.name = "test-svm".to_string();
        s.spec.resource = GroupResource {
            group: group.to_string(),
            resource: resource.to_string(),
        };
        s
    }

    // validation_test.go TestValidateStorageVersionMigration
    #[test]
    fn create_cases() {
        assert_eq!(
            agg(&validate_storage_version_migration(&svm(
                "non-empty",
                "non-empty"
            ))),
            ""
        );
        let required = "spec.resource.resource: Required value: resource is required to be set";
        assert_eq!(
            agg(&validate_storage_version_migration(&svm("", ""))),
            required
        );
        assert_eq!(
            agg(&validate_storage_version_migration(&svm("non-empty", ""))),
            required
        );
        let msg = is_dns1035_label(".").join(",");
        assert_eq!(
            agg(&validate_storage_version_migration(&svm("non-empty", "."))),
            format!("spec.resource.resource: Invalid value: \".\": {msg}")
        );
        let msg = is_dns1123_subdomain("-").join(",");
        assert_eq!(
            agg(&validate_storage_version_migration(&svm("-", "non-empty"))),
            format!("spec.resource.group: Invalid value: \"-\": {msg}")
        );
    }

    // validation_test.go TestValidateStorageVersionMigrationUpdate
    #[test]
    fn update_cases() {
        let mut valid = svm("example.com", "myresources");
        valid.metadata.resource_version = Some("1".to_string());
        assert!(validate_storage_version_migration_update(&valid, &valid).is_empty());

        let mut relabeled = valid.clone();
        relabeled.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        assert!(validate_storage_version_migration_update(&relabeled, &valid).is_empty());

        let mut spec_changed = valid.clone();
        spec_changed.spec.resource.group = "new.example.com".to_string();
        let out = agg(&validate_storage_version_migration_update(
            &spec_changed,
            &valid,
        ));
        assert!(out.contains("spec: Invalid value"), "{out}");

        let mut invalid = valid.clone();
        invalid.spec.resource.resource.clear();
        let out = agg(&validate_storage_version_migration_update(&invalid, &valid));
        assert!(
            out.contains("spec.resource.resource: Required value"),
            "{out}"
        );
    }

    fn cond(ty: &str, status: &str) -> Condition {
        Condition {
            condition_type: ty.to_string(),
            status: status.to_string(),
            observed_generation: None,
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("R".to_string()),
            message: None,
        }
    }

    fn with(rv: &str, conds: Vec<Condition>) -> StorageVersionMigration {
        let mut s = svm("example.com", "myresources");
        s.metadata.resource_version = Some("1".to_string());
        s.status.resource_version = rv.to_string();
        s.status.conditions = conds;
        s
    }

    fn check(new: StorageVersionMigration, old: StorageVersionMigration, want: &str) {
        let out = agg(&validate_storage_version_migration_status_update(
            &new, &old,
        ));
        if want.is_empty() {
            assert_eq!(out, "");
        } else {
            assert!(out.contains(want), "want {want:?}, got {out}");
        }
    }

    // validation_test.go TestValidateStorageVersionMigrationStatusUpdate
    #[test]
    fn status_update_cases() {
        let run = || cond("Running", "True");
        let ok = || cond("Succeeded", "True");
        let fail = || cond("Failed", "True");
        // valid
        check(with("123", vec![run()]), with("", vec![]), "");
        check(
            with("123", vec![cond("Running", "False"), ok()]),
            with("123", vec![run()]),
            "",
        );
        check(
            with("123", vec![cond("Running", "False"), fail()]),
            with("123", vec![run()]),
            "",
        );
        check(with("123", vec![ok()]), with("123", vec![ok()]), "");
        check(with("123", vec![fail()]), with("123", vec![fail()]), "");
        // invalid
        check(
            with("456", vec![run()]),
            with("123", vec![run()]),
            "status.resourceVersion: Invalid value: \"456\": field is immutable",
        );
        check(
            with("abc", vec![run()]),
            with("", vec![]),
            "status.resourceVersion: Invalid value: \"abc\": resource version is not well formed: abc",
        );
        check(
            with("123", vec![ok(), fail()]),
            with("123", vec![run()]),
            "Both success and failed conditions cannot be true at the same time",
        );
        check(
            with("123", vec![ok(), run()]),
            with("123", vec![run()]),
            "Running condition cannot be true when success condition is true",
        );
        check(
            with("123", vec![fail(), run()]),
            with("123", vec![run()]),
            "Running condition cannot be true when failed condition is true",
        );
        check(
            with("123", vec![cond("Succeeded", "False")]),
            with("123", vec![ok()]),
            "Success condition cannot be set to false once it is true",
        );
        check(
            with("123", vec![cond("Failed", "False")]),
            with("123", vec![fail()]),
            "Failed condition cannot be set to false once it is true",
        );
    }

    #[test]
    fn compare_resource_version_matches_upstream() {
        assert_eq!(compare_resource_version("9", "10"), Ok(-1));
        assert_eq!(compare_resource_version("10", "9"), Ok(1));
        assert_eq!(compare_resource_version("12", "12"), Ok(0));
        assert!(compare_resource_version("0", "1").is_err());
        assert!(compare_resource_version("01", "1").is_err());
        assert!(compare_resource_version("", "1").is_err());
    }
}
