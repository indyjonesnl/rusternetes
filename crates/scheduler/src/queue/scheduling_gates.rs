//! `SchedulingGates` PreEnqueue plugin — port of
//! `pkg/scheduler/framework/plugins/schedulinggates/scheduling_gates.go`
//! (`PreEnqueue`, `:43-51`), plus the per-profile map construction from
//! `pkg/scheduler/scheduler.go:388-402`.
//!
//! Its queueing hint (`isSchedulableAfterUpdatePodSchedulingGatesEliminated`)
//! lives in [`super::hints`].

use super::scheduling_queue::{PreEnqueuePlugin, PreEnqueuePluginMap};
use crate::framework_runtime::{names, ExtensionPoint};
use crate::profile::Profile;
use rusternetes_common::resources::Pod;
use std::sync::Arc;

/// `SchedulingGates` checks if a Pod carries `.spec.schedulingGates`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SchedulingGates;

impl PreEnqueuePlugin for SchedulingGates {
    fn name(&self) -> &str {
        names::SCHEDULING_GATES
    }

    /// `PreEnqueue` (`scheduling_gates.go:43-51`): UnschedulableAndUnresolvable
    /// while the pod carries gates.
    fn pre_enqueue(&self, pod: &Pod) -> Result<(), String> {
        let gates = match pod.spec.as_ref().and_then(|s| s.scheduling_gates.as_ref()) {
            Some(g) if !g.is_empty() => g,
            _ => return Ok(()),
        };
        // Go's `%v` of a []string: `[foo bar]`.
        let names: Vec<&str> = gates.iter().map(|g| g.name.as_str()).collect();
        Err(format!(
            "waiting for scheduling gates: [{}]",
            names.join(" ")
        ))
    }
}

/// `preEnqueuePluginMap` of `scheduler.New` (`scheduler.go:388-402`): for each
/// profile, the in-tree plugins enabled at the PreEnqueue extension point
/// (`profile.PreEnqueuePlugins()`, `framework.go:732`), keyed by name.
/// Only plugins with a PreEnqueue implementation are instantiated.
pub fn build_pre_enqueue_plugin_map<'a>(
    profiles: impl IntoIterator<Item = &'a Profile>,
) -> PreEnqueuePluginMap {
    let mut map = PreEnqueuePluginMap::new();
    for p in profiles {
        let plugins: Vec<Arc<dyn PreEnqueuePlugin>> = p
            .framework
            .plugins_at(ExtensionPoint::PreEnqueue)
            .iter()
            .filter_map(|name| match name.as_str() {
                names::SCHEDULING_GATES => {
                    Some(Arc::new(SchedulingGates) as Arc<dyn PreEnqueuePlugin>)
                }
                _ => None,
            })
            .collect();
        map.insert(p.scheduler_name.clone(), plugins);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::PodSchedulingGate;
    use rusternetes_common::resources::PodSpec;

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
        use super::super::hints::build_queueing_hint_map_per_profile;
        use super::super::scheduling_queue::{
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
