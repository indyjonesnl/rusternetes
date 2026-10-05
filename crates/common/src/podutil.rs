//! Port of the pod helpers in `pkg/api/v1/pod/util.go` that both the
//! api-server and the controllers read.

use crate::resources::{Pod, PodCondition, PodStatus};

/// `podutil.IsPodReady` (`pkg/api/v1/pod/util.go:297`): the `Ready` condition
/// is `True`.
pub fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| c.condition_type == "Ready" && c.status == "True")
        })
}

/// `podutil.UpdatePodCondition` (`pkg/api/v1/pod/util.go`): replace the
/// condition of the same type, or append it. The transition time moves only
/// when the status flips.
pub fn update_pod_condition(status: &mut PodStatus, mut condition: PodCondition) {
    condition.last_transition_time = Some(chrono::Utc::now());
    let conditions = status.conditions.get_or_insert_with(Vec::new);
    match conditions
        .iter_mut()
        .find(|c| c.condition_type == condition.condition_type)
    {
        None => conditions.push(condition),
        Some(old) => {
            if condition.status == old.status {
                condition.last_transition_time = old.last_transition_time;
            }
            *old = condition;
        }
    }
}
