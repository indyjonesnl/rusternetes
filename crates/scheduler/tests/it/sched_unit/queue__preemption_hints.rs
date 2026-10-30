//! Unit tests moved from `src/queue/preemption_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_scheduler::queue::events::*;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::preemption_hints::*;

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
