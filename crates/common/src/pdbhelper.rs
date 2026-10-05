//! Port of `k8s.io/component-helpers/apps/poddisruptionbudget/helpers.go` and
//! the `apimeta.SetStatusCondition` it builds on.
//!
//! Shared by the two writers of a PodDisruptionBudget's status: the
//! controller-manager's disruption controller and the api-server's pod
//! eviction subresource (`pkg/registry/core/pod/storage/eviction.go`
//! `checkAndDecrement`), exactly as upstream shares one helper module between
//! `pkg/controller/disruption` and `pkg/registry/core/pod/storage`.

use crate::resources::{PodDisruptionBudget, PodDisruptionBudgetCondition};
use chrono::Utc;

/// `policy.DisruptionAllowedCondition` and its reasons
/// (`staging/src/k8s.io/api/policy/v1/types.go:153-165`).
pub const DISRUPTION_ALLOWED_CONDITION: &str = "DisruptionAllowed";
pub const SYNC_FAILED_REASON: &str = "SyncFailed";
pub const SUFFICIENT_PODS_REASON: &str = "SufficientPods";
pub const INSUFFICIENT_PODS_REASON: &str = "InsufficientPods";

/// `apimeta.SetStatusCondition` (`apimachinery/pkg/api/meta/conditions.go:31-68`).
pub fn set_status_condition(
    conditions: &mut Vec<PodDisruptionBudgetCondition>,
    mut new: PodDisruptionBudgetCondition,
) {
    let now = || Some(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    match conditions
        .iter_mut()
        .find(|c| c.condition_type == new.condition_type)
    {
        None => {
            if new.last_transition_time.is_none() {
                new.last_transition_time = now();
            }
            conditions.push(new);
        }
        Some(existing) => {
            if existing.status != new.status {
                existing.status = new.status;
                existing.last_transition_time = new.last_transition_time.or_else(now);
            }
            existing.reason = new.reason;
            existing.message = new.message;
            existing.observed_generation = new.observed_generation;
        }
    }
}

/// `apimeta.FindStatusCondition` (`conditions.go:92-100`).
pub fn find_status_condition<'a>(
    conditions: &'a [PodDisruptionBudgetCondition],
    condition_type: &str,
) -> Option<&'a PodDisruptionBudgetCondition> {
    conditions
        .iter()
        .find(|c| c.condition_type == condition_type)
}

/// `pdbhelper.UpdateDisruptionAllowedCondition`
/// (`component-helpers/apps/poddisruptionbudget/helpers.go:28-46`).
pub fn update_disruption_allowed_condition(pdb: &mut PodDisruptionBudget) {
    let Some(status) = pdb.status.as_mut() else {
        return;
    };
    let (cond_status, reason) = if status.disruptions_allowed > 0 {
        ("True", SUFFICIENT_PODS_REASON)
    } else {
        ("False", INSUFFICIENT_PODS_REASON)
    };
    let observed_generation = status.observed_generation;
    set_status_condition(
        status.conditions.get_or_insert_with(Vec::new),
        PodDisruptionBudgetCondition {
            condition_type: DISRUPTION_ALLOWED_CONDITION.to_string(),
            status: cond_status.to_string(),
            reason: Some(reason.to_string()),
            message: None,
            observed_generation,
            last_transition_time: None,
        },
    );
}

/// `pdbhelper.ConditionsAreUpToDate` (`helpers.go:50-65`).
pub fn conditions_are_up_to_date(pdb: &PodDisruptionBudget) -> bool {
    let Some(status) = &pdb.status else {
        return false;
    };
    let Some(cond) = status
        .conditions
        .as_ref()
        .and_then(|c| find_status_condition(c, DISRUPTION_ALLOWED_CONDITION))
    else {
        return false;
    };
    if status.observed_generation.unwrap_or(0) != pdb.metadata.generation.unwrap_or(0) {
        return false;
    }
    if status.disruptions_allowed > 0 {
        cond.status == "True" && cond.reason.as_deref() == Some(SUFFICIENT_PODS_REASON)
    } else {
        cond.status == "False" && cond.reason.as_deref() == Some(INSUFFICIENT_PODS_REASON)
    }
}
