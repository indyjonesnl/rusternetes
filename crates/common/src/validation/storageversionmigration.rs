//! StorageVersionMigration validation — port of upstream
//! `pkg/apis/storagemigration/validation/validation.go` (release-1.35) and
//! the `resourceversion.CompareResourceVersion` it calls
//! (`staging/src/k8s.io/apimachinery/pkg/util/resourceversion/resourceversion.go`).

use crate::resources::storageversionmigration::{
    StorageVersionMigration, MIGRATION_FAILED, MIGRATION_RUNNING, MIGRATION_SUCCEEDED,
};
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1035_label, is_dns1123_subdomain, validate_conditions};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field, validate_object_meta,
    validate_object_meta_update,
};

/// `resourceversion.isWellFormed`: a positive integer with no leading zero.
fn is_well_formed(s: &str) -> bool {
    !s.is_empty() && !s.starts_with('0') && s.bytes().all(|b| b.is_ascii_digit())
}

/// `resourceversion.CompareResourceVersion`: `-1`/`0`/`1`, or the
/// `InvalidResourceVersion` message for a malformed input.
pub fn compare_resource_version(a: &str, b: &str) -> Result<i32, String> {
    for rv in [a, b] {
        if !is_well_formed(rv) {
            return Err(format!("resource version is not well formed: {rv}"));
        }
    }
    // Shorter is less; equal length compares lexically.
    Ok(match a.len().cmp(&b.len()).then_with(|| a.cmp(b)) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    })
}

/// `ValidateStorageVersionMigration` (validation.go:33-53).
pub fn validate_storage_version_migration(svm: &StorageVersionMigration) -> ErrorList {
    let mut errs = validate_object_meta(
        &svm.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );

    let resource_path = Path::new("spec").child("resource");
    let resource = &svm.spec.resource.resource;
    if resource.is_empty() {
        errs.push(Error::required(
            &resource_path.child("resource"),
            "resource is required to be set",
        ));
    } else {
        // Same validations as APIService: Group must be a DNS1123 Subdomain
        // and Resource must be DNS1035.
        let msgs = is_dns1035_label(resource);
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &resource_path.child("resource"),
                resource.clone(),
                msgs.join(","),
            ));
        }
    }

    let group = &svm.spec.resource.group;
    if !group.is_empty() {
        let msgs = is_dns1123_subdomain(group);
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &resource_path.child("group"),
                group.clone(),
                msgs.join(","),
            ));
        }
    }
    errs
}

/// `ValidateStorageVersionMigrationUpdate` (validation.go:55-63).
pub fn validate_storage_version_migration_update(
    new: &StorageVersionMigration,
    old: &StorageVersionMigration,
) -> ErrorList {
    let mut errs = validate_storage_version_migration(new);
    errs.extend(validate_object_meta_update(
        &new.metadata,
        &old.metadata,
        &Path::new("metadata"),
    ));
    // Prevent changes to the spec.
    errs.extend(validate_immutable_field(
        &new.spec,
        &old.spec,
        &Path::new("spec"),
    ));
    errs
}

fn condition_true(svm: &StorageVersionMigration, ty: &str) -> bool {
    svm.status
        .conditions
        .iter()
        .find(|c| c.condition_type == ty)
        .is_some_and(|c| c.status == "True")
}

/// `ValidateStorageVersionMigrationStatusUpdate` (validation.go:65-109).
pub fn validate_storage_version_migration_status_update(
    new: &StorageVersionMigration,
    old: &StorageVersionMigration,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    let fld = Path::new("status");
    let new_rv = &new.status.resource_version;

    // resource version should be a non-negative integer. Upstream compares
    // the new value with itself (validation.go:72), so an empty status
    // resourceVersion is rejected as malformed.
    match compare_resource_version(new_rv, new_rv) {
        Ok(0) => {}
        Ok(_) => errs.push(Error::invalid(
            &fld.child("resourceVersion"),
            new_rv.clone(),
            format!("unable to compare resource versions, {new_rv} is not equal to {new_rv}"),
        )),
        Err(msg) => errs.push(Error::invalid(
            &fld.child("resourceVersion"),
            new_rv.clone(),
            msg,
        )),
    }

    errs.extend(validate_conditions(
        &new.status.conditions,
        &fld.child("conditions"),
    ));

    // resource version should not change once it has been set
    if !old.status.resource_version.is_empty() && old.status.resource_version != *new_rv {
        errs.push(Error::invalid(
            &fld.child("resourceVersion"),
            new_rv.clone(),
            "field is immutable",
        ));
    }

    let conds = fld.child("conditions");
    let invalid =
        |msg: &str| Error::invalid(&conds, BadValue::marshal(&new.status.conditions), msg);
    let succeeded = condition_true(new, MIGRATION_SUCCEEDED);
    let failed = condition_true(new, MIGRATION_FAILED);
    let running = condition_true(new, MIGRATION_RUNNING);

    // at most one of success or failed may be true
    if succeeded && failed {
        errs.push(invalid(
            "Both success and failed conditions cannot be true at the same time",
        ));
    }
    // running must be false when success is true or failed is true
    if succeeded && running {
        errs.push(invalid(
            "Running condition cannot be true when success condition is true",
        ));
    }
    if failed && running {
        errs.push(invalid(
            "Running condition cannot be true when failed condition is true",
        ));
    }
    // success / failed cannot be set to false once true
    if condition_true(old, MIGRATION_SUCCEEDED) && !succeeded {
        errs.push(invalid(
            "Success condition cannot be set to false once it is true",
        ));
    }
    if condition_true(old, MIGRATION_FAILED) && !failed {
        errs.push(invalid(
            "Failed condition cannot be set to false once it is true",
        ));
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::storageversionmigration::GroupResource;
    use crate::types::Condition;

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
