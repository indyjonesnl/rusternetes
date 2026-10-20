//! DefaultPreemption queueing hint.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go`:
//! `EventsToRegister` (`:146-156`) and `isPodSchedulableAfterPodDeletion`
//! (`:158-168`). Upstream has no unit test for either.

use super::events::*;
use rusternetes_common::resources::Pod;

/// `isPodSchedulableAfterPodDeletion` (`default_preemption.go:158-168`):
/// always `QueueSkip`. Pods rejected by DefaultPreemption (PreEnqueue, async
/// preemption in flight) also carry other plugins in `UnschedulablePlugins`
/// whose failure the preemption resolves; those plugins decide when to
/// requeue, not this one.
pub fn is_pod_schedulable_after_pod_deletion(
    _pod: &Pod,
    _old_obj: Option<&EventObject>,
    _new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    Ok(QueueingHint::QueueSkip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_skips() {
        let pod: Pod =
            serde_json::from_value(serde_json::json!({"metadata": {"name": "p"}})).unwrap();
        assert_eq!(
            is_pod_schedulable_after_pod_deletion(&pod, Some(&pod), None).unwrap(),
            QueueingHint::QueueSkip
        );
    }
}
