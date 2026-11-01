//! Victim selection for tests, on the scheduler's own dry run.
//!
//! `check_preemption*` answer "would this pod preempt on this node, and whom"
//! with `SelectVictimsOnNode` (`default_preemption.go:207-307`) run over the
//! default profile's plugins, as `try_preempt` does. The node is taken as the
//! kubelet reports it: `NodeResourcesFit` counts `pods` against allocatable, so
//! fixtures that omit it get the kubelet's usual 110.
use rusternetes_common::resources::{Node, Pod, PodDisruptionBudget};
use rusternetes_scheduler::default_preemption::select_victims_on_node;
use rusternetes_scheduler::fwk::NoHandle;
use rusternetes_scheduler::profile::Profile;

fn as_reported_by_kubelet(node: &Node) -> Node {
    let mut node = node.clone();
    if let Some(status) = node.status.as_mut() {
        for q in [status.allocatable.as_mut()].into_iter().flatten() {
            q.entry("pods".to_string())
                .or_insert_with(|| "110".to_string());
        }
    }
    node
}

/// Whether the scheduler's PostFilter would preempt for `pod` on `node`, the
/// `preemptionPolicy` gate included (`try_preempt`), for a node every filter
/// rejected as Unschedulable.
pub fn try_preempt(node: &Node, pod: &Pod, all_pods: &[Pod]) -> Option<(String, Vec<Pod>)> {
    use rusternetes_scheduler::diagnosis::Diagnosis;
    use rusternetes_scheduler::fwk::Code;
    use rusternetes_scheduler::scheduler::Scheduler;
    use rusternetes_storage::memory::MemoryStorage;
    let scheduler = Scheduler::new_with_name(
        std::sync::Arc::new(MemoryStorage::new()),
        1,
        "default-scheduler".to_string(),
    );
    let diag = Diagnosis {
        absent_nodes_code: Some(Code::Unschedulable),
        ..Default::default()
    };
    futures::executor::block_on(scheduler.try_preempt(
        pod,
        &[as_reported_by_kubelet(node)],
        all_pods,
        &Default::default(),
        &[],
        &diag,
    ))
}

/// `(should_preempt, victim names)`; `(true, [])` is a pod that fits as is.
pub fn check_preemption_with_pdbs(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
) -> (bool, Vec<String>) {
    let mut node = node.clone();
    if let Some(status) = node.status.as_mut() {
        for q in [status.allocatable.as_mut()].into_iter().flatten() {
            q.entry("pods".to_string())
                .or_insert_with(|| "110".to_string());
        }
    }
    let profile = Profile::named("default-scheduler");
    let nodes = [node.clone()];
    let selected = futures::executor::block_on(select_victims_on_node(
        &profile.framework,
        &NoHandle,
        pod,
        &node,
        all_pods,
        &nodes,
        pdbs,
    ));
    match selected {
        Ok(v) => (
            true,
            v.victims.into_iter().map(|p| p.metadata.name).collect(),
        ),
        Err(_) => (false, Vec::new()),
    }
}

/// [`check_preemption_with_pdbs`] without budgets.
pub fn check_preemption(node: &Node, pod: &Pod, all_pods: &[Pod]) -> (bool, Vec<String>) {
    check_preemption_with_pdbs(node, pod, all_pods, &[])
}
