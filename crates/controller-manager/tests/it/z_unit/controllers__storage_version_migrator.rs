//! Ports of the cases of `TestIsResourceMigratable`
//! (`pkg/controller/storageversionmigrator/resourceversion_test.go`) that
//! exercise the verb check, and of `setStatusConditions` (`util.go`).
use rusternetes_common::resources::storageversionmigration::*;
use rusternetes_controller_manager::controllers::storage_version_migrator::*;

fn verbs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn migratable_resource() {
    assert!(has_required_verbs(&verbs(&[
        "get", "list", "watch", "create", "update", "patch", "delete"
    ])));
}

#[test]
fn non_updatable_resource() {
    assert!(!has_required_verbs(&verbs(&[
        "get", "list", "watch", "create", "delete"
    ])));
}

#[test]
fn non_patchable_resource() {
    assert!(!has_required_verbs(&verbs(&[
        "get", "list", "watch", "create", "update", "delete"
    ])));
}

#[test]
fn non_listable_resource() {
    assert!(!has_required_verbs(&verbs(&[
        "get", "watch", "create", "delete", "update", "patch"
    ])));
}

#[test]
fn no_verbs() {
    assert!(!has_required_verbs(&[]));
}

#[test]
fn running_then_succeeded_flips_running_to_false() {
    let mut svm = StorageVersionMigration::default();
    set_status_conditions(
        &mut svm,
        MIGRATION_RUNNING,
        MIGRATION_RUNNING_STATUS_REASON,
        "",
    );
    assert_eq!(svm.status.conditions.len(), 1);
    assert_eq!(svm.status.conditions[0].status, "True");

    set_status_conditions(
        &mut svm,
        MIGRATION_SUCCEEDED,
        MIGRATION_SUCCESS_STATUS_REASON,
        "",
    );
    assert_eq!(svm.status.conditions.len(), 2);
    assert_eq!(svm.status.conditions[0].condition_type, MIGRATION_RUNNING);
    assert_eq!(svm.status.conditions[0].status, "False");
    assert_eq!(svm.status.conditions[1].condition_type, MIGRATION_SUCCEEDED);
    assert_eq!(svm.status.conditions[1].status, "True");
    assert_eq!(
        svm.status.conditions[1].reason.as_deref(),
        Some("StorageVersionMigrationSucceeded")
    );
}

#[test]
fn a_condition_cannot_be_set_twice() {
    let mut svm = StorageVersionMigration::default();
    set_status_conditions(
        &mut svm,
        MIGRATION_FAILED,
        MIGRATION_FAILED_STATUS_REASON,
        "a",
    );
    set_status_conditions(
        &mut svm,
        MIGRATION_FAILED,
        MIGRATION_FAILED_STATUS_REASON,
        "b",
    );
    assert_eq!(svm.status.conditions.len(), 1);
    assert_eq!(svm.status.conditions[0].message.as_deref(), Some("a"));
}
