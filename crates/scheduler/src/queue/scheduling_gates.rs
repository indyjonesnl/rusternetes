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
