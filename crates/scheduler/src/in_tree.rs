//! In-tree scheduler plugin objects dispatched through the framework
//! (`fwk` traits). Plugins migrate here from the inline pipeline in
//! `scheduler.rs` one at a time (#3455).

use crate::advanced::toleration_matches_taint;
use crate::framework_runtime::names;
use crate::fwk::{Code, CycleState, FilterPlugin, NodeInfo, Plugin, Status};
use async_trait::async_trait;
use rusternetes_common::resources::pod::{NodeAffinity, NodeSelector};
use rusternetes_common::resources::{Pod, Taint};
use std::sync::Arc;

/// The per-profile plugin arguments the Filter plugin factories read
/// (`runtime.PluginFactory`'s `args`).
#[derive(Default)]
pub struct FilterPluginArgs<'a> {
    /// `NodeAffinityArgs.AddedAffinity`.
    pub added_affinity: Option<&'a NodeAffinity>,
}

/// The factory for an in-tree Filter plugin by name, `None` for names
/// that are not (yet) plugin objects.
pub fn new_filter_plugin(name: &str, args: &FilterPluginArgs<'_>) -> Option<Arc<dyn FilterPlugin>> {
    match name {
        names::NODE_RESOURCES_FIT => Some(Arc::new(NodeResourcesFit)),
        names::NODE_PORTS => Some(Arc::new(NodePorts)),
        names::NODE_AFFINITY => Some(Arc::new(NodeAffinityPlugin::new(args.added_affinity))),
        names::NODE_UNSCHEDULABLE => Some(Arc::new(NodeUnschedulable)),
        names::NODE_NAME => Some(Arc::new(NodeName)),
        names::TAINT_TOLERATION => Some(Arc::new(TaintToleration)),
        _ => None,
    }
}

/// `nodeunschedulable.ErrReasonUnschedulable`.
pub const ERR_REASON_UNSCHEDULABLE: &str = "node(s) were unschedulable";

/// `nodeunschedulable.NodeUnschedulable`
/// (`pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable.go`):
/// filters nodes that set `node.Spec.Unschedulable=true` unless the pod
/// tolerates the `{key=node.kubernetes.io/unschedulable, effect=NoSchedule}`
/// taint.
pub struct NodeUnschedulable;

impl Plugin for NodeUnschedulable {
    fn name(&self) -> &'static str {
        names::NODE_UNSCHEDULABLE
    }
}

fn pod_tolerates_unschedulable(pod: &Pod) -> bool {
    let taint = Taint {
        key: "node.kubernetes.io/unschedulable".to_string(),
        value: None,
        effect: "NoSchedule".to_string(),
        time_added: None,
    };
    pod.spec
        .as_ref()
        .and_then(|s| s.tolerations.as_deref())
        .unwrap_or(&[])
        .iter()
        .any(|t| toleration_matches_taint(t, &taint))
}

#[async_trait]
impl FilterPlugin for NodeUnschedulable {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        let unschedulable = node_info
            .node()
            .spec
            .as_ref()
            .and_then(|s| s.unschedulable)
            .unwrap_or(false);
        if !unschedulable {
            return Status::success();
        }
        // If the pod tolerates the unschedulable taint, it also tolerates
        // `node.Spec.Unschedulable` (node_unschedulable.go:Filter).
        if !pod_tolerates_unschedulable(pod) {
            return Status::new(
                Code::UnschedulableAndUnresolvable,
                vec![ERR_REASON_UNSCHEDULABLE.to_string()],
            );
        }
        Status::success()
    }
}

/// `nodename.ErrReason`.
pub const ERR_REASON_NODE_NAME: &str = "node(s) didn't match the requested node name";

/// `nodename.NodeName` (`plugins/nodename/node_name.go`): filters nodes that
/// do not match the name in `pod.Spec.NodeName`.
pub struct NodeName;

impl Plugin for NodeName {
    fn name(&self) -> &'static str {
        names::NODE_NAME
    }
}

#[async_trait]
impl FilterPlugin for NodeName {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        // `nodeInfo.Node() == nil` is an Error upstream; a Node here always exists.
        let wanted = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("");
        if !wanted.is_empty() && wanted != node_info.node().metadata.name {
            return Status::new(
                Code::UnschedulableAndUnresolvable,
                vec![ERR_REASON_NODE_NAME.to_string()],
            );
        }
        Status::success()
    }
}

/// `tainttoleration.TaintToleration` Filter
/// (`plugins/tainttoleration/taint_toleration.go:119-132`): a node with a
/// NoSchedule/NoExecute taint the pod does not tolerate is rejected
/// (`FindMatchingUntoleratedTaint` with `DoNotScheduleTaintsFilterFunc`).
pub struct TaintToleration;

impl Plugin for TaintToleration {
    fn name(&self) -> &'static str {
        names::TAINT_TOLERATION
    }
}

#[async_trait]
impl FilterPlugin for TaintToleration {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        let taints = node_info
            .node()
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_deref())
            .unwrap_or(&[]);
        let tolerations = pod
            .spec
            .as_ref()
            .and_then(|s| s.tolerations.as_deref())
            .unwrap_or(&[]);
        if !crate::queue::hints::taints::has_untolerated_do_not_schedule_taint(taints, tolerations)
        {
            return Status::success();
        }
        Status::new(
            Code::UnschedulableAndUnresolvable,
            vec!["node(s) had untolerated taint(s)".to_string()],
        )
    }
}

/// `errReasonEnforced` (node_affinity.go:66-67).
pub const ERR_REASON_ENFORCED: &str = "node(s) didn't match scheduler-enforced node affinity";
/// `ErrReasonPod` (node_affinity.go:63-64).
pub const ERR_REASON_POD: &str = "node(s) didn't match Pod's node affinity/selector";

/// `nodeaffinity.NodeAffinity` Filter (`plugins/nodeaffinity/node_affinity.go:217-238`):
/// the scheduler-enforced `addedAffinity` first, then the pod's
/// `spec.nodeSelector` and required node affinity
/// (`GetRequiredNodeAffinity(pod).Match(node)`).
///
/// Deviation: upstream's PreFilter (narrowing candidate nodes by
/// `matchFields` on `metadata.name`, :146-215) is not ported; Filter alone is
/// complete, as upstream's own fallback when no PreFilter state exists
/// (:225-231).
pub struct NodeAffinityPlugin {
    /// `addedNodeSelector`.
    added_node_selector: Option<NodeSelector>,
}

impl NodeAffinityPlugin {
    pub fn new(added_affinity: Option<&NodeAffinity>) -> Self {
        Self {
            added_node_selector: added_affinity.and_then(|a| {
                a.required_during_scheduling_ignored_during_execution
                    .clone()
            }),
        }
    }
}

impl Plugin for NodeAffinityPlugin {
    fn name(&self) -> &'static str {
        names::NODE_AFFINITY
    }
}

#[async_trait]
impl FilterPlugin for NodeAffinityPlugin {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        let node = node_info.node();
        if let Some(added) = &self.added_node_selector {
            if !rusternetes_common::affinity::matches_node_selector(node, added) {
                return Status::new(
                    Code::UnschedulableAndUnresolvable,
                    vec![ERR_REASON_ENFORCED.to_string()],
                );
            }
        }
        let spec = pod.spec.as_ref();
        // `RequiredNodeAffinity.Match`: the nodeSelector labels ...
        if let Some(selector) = spec.and_then(|s| s.node_selector.as_ref()) {
            let labels = node.metadata.labels.as_ref();
            let ok = match labels {
                None => selector.is_empty(),
                Some(l) => selector.iter().all(|(k, v)| l.get(k) == Some(v)),
            };
            if !ok {
                return Status::new(
                    Code::UnschedulableAndUnresolvable,
                    vec![ERR_REASON_POD.to_string()],
                );
            }
        }
        // ... and the required node affinity terms.
        if let Some(required) = spec
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| {
                na.required_during_scheduling_ignored_during_execution
                    .as_ref()
            })
        {
            if !rusternetes_common::affinity::matches_node_selector(node, required) {
                return Status::new(
                    Code::UnschedulableAndUnresolvable,
                    vec![ERR_REASON_POD.to_string()],
                );
            }
        }
        Status::success()
    }
}

/// `nodeports.ErrReason`.
pub const ERR_REASON_NODE_PORTS: &str =
    "node(s) didn't have free ports for the requested pod ports";

/// `nodeports.NodePorts` Filter (`plugins/nodeports/node_ports.go`): checks
/// that the pod's hostPorts are free on the node.
///
/// Deviation: PreFilter (caching the wanted ports in CycleState, and Skip for
/// pods without any so Filter is not run) is not ported; Filter computes the
/// wanted ports itself, which has the same outcome (a pod without hostPorts
/// fits every node). Upstream's Filter returns an Error status if PreFilter
/// state is missing; that case cannot arise here.
pub struct NodePorts;

impl Plugin for NodePorts {
    fn name(&self) -> &'static str {
        names::NODE_PORTS
    }
}

#[async_trait]
impl FilterPlugin for NodePorts {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        if crate::advanced::host_ports_fit(&node_info.node().metadata.name, pod, node_info.pods()) {
            return Status::success();
        }
        Status::new(Code::Unschedulable, vec![ERR_REASON_NODE_PORTS.to_string()])
    }
}

/// `noderesources.Fit` Filter (`plugins/noderesources/fit.go:586-619`,
/// `fitsRequest` :649-736): the node must have room for the pod's requests
/// (cpu, memory, ephemeral-storage, scalar/extended resources) and for one
/// more pod.
///
/// Deviations:
/// - PreFilter (computing the request once into CycleState) is not ported;
///   Filter computes it per node.
/// - A node that reports no `status.allocatable` at all is not filtered
///   (upstream would reject every pod there); the kubelet always reports it.
/// - `NodeResourcesFitArgs.IgnoredResources`/`IgnoredResourceGroups` and the
///   DRA extended-resource delegation (`shouldDelegateResourceToDRA`) are not
///   honoured yet.
/// - Terminating pods do not count against the node, as in the scheduler's
///   other accounting here.
pub struct NodeResourcesFit;

impl Plugin for NodeResourcesFit {
    fn name(&self) -> &'static str {
        names::NODE_RESOURCES_FIT
    }
}

fn counts_against_node(pod: &Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(rusternetes_common::types::Phase::Succeeded)
            | Some(rusternetes_common::types::Phase::Failed)
    ) && pod.metadata.deletion_timestamp.is_none()
}

#[async_trait]
impl FilterPlugin for NodeResourcesFit {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        use crate::advanced::{parse_resource_quantity, pod_request_amounts};
        use crate::score::is_scalar_resource_name;
        use std::collections::HashMap;

        let Some(allocatable) = node_info
            .node()
            .status
            .as_ref()
            .and_then(|s| s.allocatable.as_ref())
        else {
            return Status::success();
        };
        let alloc = |name: &str| -> i64 {
            allocatable
                .get(name)
                .map(|q| parse_resource_quantity(q, name))
                .unwrap_or(0)
        };

        // `NodeInfo.Requested` and `len(GetPods())` over the node's pods.
        let mut requested: HashMap<String, i64> = HashMap::new();
        let mut pod_count = 0i64;
        for p in node_info.pods().filter(|p| counts_against_node(p)) {
            pod_count += 1;
            for (k, v) in pod_request_amounts(p) {
                *requested.entry(k).or_insert(0) += v;
            }
        }
        let used = |name: &str| requested.get(name).copied().unwrap_or(0);

        // (reason, unresolvable)
        let mut insufficient: Vec<(String, bool)> = Vec::new();

        let allowed_pod_number = alloc("pods");
        if pod_count + 1 > allowed_pod_number {
            insufficient.push(("Too many pods".to_string(), false));
        }

        let request = pod_request_amounts(pod);
        let want = |name: &str| request.get(name).copied().unwrap_or(0);
        let scalars: Vec<(&String, &i64)> = {
            let mut v: Vec<_> = request
                .iter()
                .filter(|(k, v)| is_scalar_resource_name(k) && **v != 0)
                .collect();
            v.sort();
            v
        };
        if want("cpu") == 0
            && want("memory") == 0
            && want("ephemeral-storage") == 0
            && scalars.is_empty()
        {
            return Self::status(insufficient);
        }

        for (name, reason) in [
            ("cpu", "Insufficient cpu"),
            ("memory", "Insufficient memory"),
            ("ephemeral-storage", "Insufficient ephemeral-storage"),
        ] {
            let w = want(name);
            if w > 0 && w > alloc(name) - used(name) {
                insufficient.push((reason.to_string(), w > alloc(name)));
            }
        }
        for (name, w) in scalars {
            if *w > alloc(name) - used(name) {
                insufficient.push((format!("Insufficient {name}"), *w > alloc(name)));
            }
        }
        Self::status(insufficient)
    }
}

impl NodeResourcesFit {
    /// The status `Filter` builds from the insufficient resources
    /// (fit.go:604-617): all reasons are kept, and any unresolvable one makes
    /// the status UnschedulableAndUnresolvable.
    fn status(insufficient: Vec<(String, bool)>) -> Status {
        if insufficient.is_empty() {
            return Status::success();
        }
        let code = if insufficient.iter().any(|(_, unresolvable)| *unresolvable) {
            Code::UnschedulableAndUnresolvable
        } else {
            Code::Unschedulable
        };
        Status::new(code, insufficient.into_iter().map(|(r, _)| r).collect())
    }
}
