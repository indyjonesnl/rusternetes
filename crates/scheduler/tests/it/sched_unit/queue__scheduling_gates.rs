//! Unit tests moved from `src/queue/scheduling_gates.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_scheduler::framework_runtime::{names, ExtensionPoint};
    pub use rusternetes_scheduler::profile::Profile;
    pub use rusternetes_scheduler::queue::scheduling_queue::{
        PreEnqueuePlugin, PreEnqueuePluginMap,
    };
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::PodSchedulingGate;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_scheduler::queue::scheduling_gates::*;

    fn pod_with_gates(gates: Option<&[&str]>) -> Pod {
        let spec = PodSpec {
            scheduling_gates: gates.map(|g| {
                g.iter()
                    .map(|n| PodSchedulingGate {
                        name: n.to_string(),
                    })
                    .collect()
            }),
            ..Default::default()
        };
        Pod::new("p", spec)
    }

    /// `TestPreEnqueue` (scheduling_gates_test.go:32-64).
    #[test]
    fn test_pre_enqueue() {
        let pl = SchedulingGates;
        assert_eq!(pl.pre_enqueue(&pod_with_gates(None)), Ok(()));
        assert_eq!(
            pl.pre_enqueue(&pod_with_gates(Some(&["foo", "bar"]))),
            Err("waiting for scheduling gates: [foo bar]".to_string())
        );
        // An empty (non-nil) list carries no gates.
        assert_eq!(pl.pre_enqueue(&pod_with_gates(Some(&[]))), Ok(()));
    }

    /// `profile.PreEnqueuePlugins()` over the default plugin set.
    #[test]
    fn default_profile_enables_scheduling_gates() {
        let profile = Profile::named("default-scheduler");
        let map = build_pre_enqueue_plugin_map([&profile]);
        let plugins = &map["default-scheduler"];
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name(), "SchedulingGates");
    }

    /// End to end through the queue: a gated pod never reaches activeQ
    /// (`runPreEnqueuePlugins` in `moveToActiveQ`) and is released by the
    /// `UpdatePodSchedulingGatesEliminated` event once its gates are gone.
    #[tokio::test]
    async fn gated_pod_stays_out_of_active_q_until_ungated() {
        use rusternetes_scheduler::queue::hints::build_queueing_hint_map_per_profile;
        use rusternetes_scheduler::queue::scheduling_queue::{
            priority_sort_less, PriorityQueue, PriorityQueueOptions,
        };
        let profile = Profile::named("default-scheduler");
        let queue = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                queueing_hint_map: build_queueing_hint_map_per_profile(
                    [&profile],
                    &Default::default(),
                ),
                pre_enqueue_plugin_map: build_pre_enqueue_plugin_map([&profile]),
                ..Default::default()
            },
        );
        let mut gated = pod_with_gates(Some(&["foo"]));
        gated.metadata.uid = "uid".to_string();
        queue.add(gated.clone());
        assert!(queue.pods_in_active_q().is_empty());
        assert_eq!(queue.unschedulable_pods().len(), 1);

        let mut ungated = pod_with_gates(Some(&[]));
        ungated.metadata.uid = "uid".to_string();
        queue.update(Some(&gated), &ungated);
        assert_eq!(queue.pods_in_active_q().len(), 1);
        assert!(queue.unschedulable_pods().is_empty());
    }
}
