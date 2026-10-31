//! Storage version migrator: shared pieces (slice 1 of #3825).
//!
//! Port of `pkg/controller/storageversionmigrator` (release-1.35):
//! `util.go` (`setStatusConditions`) and the verb check of
//! `resourceversion.go` (`isResourceMigratable`, `verbsRequiredForMigration`).
//! The two controllers (`ResourceVersionController`, `SVMController`) that use
//! them follow in later slices; nothing here is wired into the controller
//! manager yet.

use chrono::Utc;
use rusternetes_common::resources::storageversionmigration::{
    StorageVersionMigration, MIGRATION_FAILED, MIGRATION_RUNNING, MIGRATION_SUCCEEDED,
};
use rusternetes_common::types::Condition;

/// `storageversionmigrator.go:46`: `migrationSuccessStatusReason`.
pub const MIGRATION_SUCCESS_STATUS_REASON: &str = "StorageVersionMigrationSucceeded";
/// `migrationRunningStatusReason`.
pub const MIGRATION_RUNNING_STATUS_REASON: &str = "StorageVersionMigrationInProgress";
/// `migrationFailedStatusReason`.
pub const MIGRATION_FAILED_STATUS_REASON: &str = "StorageVersionMigrationFailed";

/// `verbsRequiredForMigration` (`resourceversion.go:51`).
pub const VERBS_REQUIRED_FOR_MIGRATION: [&str; 3] = ["update", "patch", "list"];

/// `isResourceMigratable`'s verb test (`resourceversion.go:313-316`):
/// `sets.NewString(resource.Verbs...).HasAll(verbsRequiredForMigration...)`.
pub fn has_required_verbs(verbs: &[String]) -> bool {
    VERBS_REQUIRED_FOR_MIGRATION
        .iter()
        .all(|required| verbs.iter().any(|v| v == required))
}

fn is_status_condition_set(svm: &StorageVersionMigration, condition_type: &str) -> bool {
    // meta.IsStatusConditionTrue || meta.IsStatusConditionFalse
    svm.status
        .conditions
        .iter()
        .any(|c| c.condition_type == condition_type && (c.status == "True" || c.status == "False"))
}

/// `setStatusConditions` (`util.go:29-58`).
pub fn set_status_conditions(
    svm: &mut StorageVersionMigration,
    condition_type: &str,
    reason: &str,
    message: &str,
) {
    // Cannot set the condition twice.
    if is_status_condition_set(svm, condition_type) {
        return;
    }

    if condition_type == MIGRATION_SUCCEEDED || condition_type == MIGRATION_FAILED {
        // set running condition to false if we're finished
        if let Some(running) = svm
            .status
            .conditions
            .iter_mut()
            .find(|c| c.condition_type == MIGRATION_RUNNING)
        {
            running.status = "False".to_string();
        }
    }

    svm.status.conditions.push(Condition {
        condition_type: condition_type.to_string(),
        status: "True".to_string(),
        observed_generation: None,
        last_transition_time: Some(Utc::now()),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
    });
}
