//! In-tree scheduler plugin objects dispatched through the framework
//! (`fwk` traits). Plugins migrate here from the inline pipeline in
//! `scheduler.rs` one at a time (#3455).

use crate::advanced::toleration_matches_taint;
use crate::framework_runtime::names;
use crate::fwk::{
    Code, CycleState, FilterPlugin, Handle, NodeInfo, Plugin, PreFilterPlugin, PreFilterResult,
    Status,
};
use async_trait::async_trait;
use rusternetes_common::resources::pod::{NodeAffinity, NodeSelector};
use rusternetes_common::resources::{Node, Pod, Taint};
use std::collections::{HashMap, HashSet};
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
        names::NODE_VOLUME_LIMITS => Some(Arc::new(NodeVolumeLimits)),
        names::POD_TOPOLOGY_SPREAD => Some(Arc::new(PodTopologySpread)),
        names::INTER_POD_AFFINITY => Some(Arc::new(InterPodAffinity)),
        names::TAINT_TOLERATION => Some(Arc::new(TaintToleration)),
        _ => None,
    }
}

/// The factory for an in-tree PreFilter plugin by name, `None` for names
/// that are not (yet) plugin objects.
pub fn new_pre_filter_plugin(
    name: &str,
    args: &FilterPluginArgs<'_>,
) -> Option<Arc<dyn PreFilterPlugin>> {
    match name {
        names::NODE_AFFINITY => Some(Arc::new(NodeAffinityPlugin::new(args.added_affinity))),
        names::NODE_PORTS => Some(Arc::new(NodePorts)),
        names::NODE_RESOURCES_FIT => Some(Arc::new(NodeResourcesFit)),
        names::NODE_VOLUME_LIMITS => Some(Arc::new(NodeVolumeLimits)),
        _ => None,
    }
}

/// `framework.ErrNotFound`-style failure reading PreFilter state: PreFilter
/// was not invoked for the plugin (`getPreFilterState`).
fn missing_state(key: &str) -> Status {
    Status::from_error(format!("reading {key:?} from cycleState: not found"))
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

/// `errReasonConflict` (node_affinity.go:69-70).
pub const ERR_REASON_CONFLICT: &str = "pod affinity terms conflict";

/// `preFilterStateKey` of the NodeAffinity plugin (`"PreFilter" + Name`).
const NODE_AFFINITY_STATE_KEY: &str = "PreFilterNodeAffinity";

/// `nodeaffinity.RequiredNodeAffinity` (`component-helpers/scheduling/corev1/nodeaffinity`):
/// the pod's `spec.nodeSelector` and required node affinity, which a node
/// must both match. Also the NodeAffinity plugin's `preFilterState`.
struct RequiredNodeAffinity {
    node_selector: Option<HashMap<String, String>>,
    required_affinity: Option<NodeSelector>,
}

impl RequiredNodeAffinity {
    /// `GetRequiredNodeAffinity(pod)`.
    fn from_pod(pod: &Pod) -> Self {
        let spec = pod.spec.as_ref();
        Self {
            node_selector: spec.and_then(|s| s.node_selector.clone()),
            required_affinity: spec
                .and_then(|s| s.affinity.as_ref())
                .and_then(|a| a.node_affinity.as_ref())
                .and_then(|na| {
                    na.required_during_scheduling_ignored_during_execution
                        .clone()
                }),
        }
    }

    /// `RequiredNodeAffinity.Match`.
    fn matches(&self, node: &Node) -> bool {
        if let Some(selector) = &self.node_selector {
            let ok = match node.metadata.labels.as_ref() {
                None => selector.is_empty(),
                Some(l) => selector.iter().all(|(k, v)| l.get(k) == Some(v)),
            };
            if !ok {
                return false;
            }
        }
        match &self.required_affinity {
            Some(required) => rusternetes_common::affinity::matches_node_selector(node, required),
            None => true,
        }
    }
}

/// `nodeaffinity.NodeAffinity` (`plugins/nodeaffinity/node_affinity.go`).
///
/// PreFilter (:159-209) skips pods the plugin has nothing to do with,
/// caches `GetRequiredNodeAffinity(pod)` in CycleState, and narrows the
/// candidate nodes when every required term pins `metadata.name` through a
/// `matchFields` `In` requirement. Filter (:217-238) checks the
/// scheduler-enforced `addedAffinity` first, then the cached pod
/// selector/affinity (recomputed from the pod when PreFilter did not run, as
/// upstream does at :225-231).
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
impl PreFilterPlugin for NodeAffinityPlugin {
    async fn pre_filter(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        _nodes: &[Node],
        _handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status) {
        let spec = pod.spec.as_ref();
        let required = spec
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| {
                na.required_during_scheduling_ignored_during_execution
                    .as_ref()
            });
        if required.is_none()
            && self.added_node_selector.is_none()
            && spec.is_none_or(|s| s.node_selector.is_none())
        {
            // NodeAffinity Filter has nothing to do with the Pod.
            return (None, Status::new(Code::Skip, Vec::new()));
        }

        state.write(NODE_AFFINITY_STATE_KEY, RequiredNodeAffinity::from_pod(pod));

        let Some(required) = required.filter(|r| !r.node_selector_terms.is_empty()) else {
            return (None, Status::success());
        };

        // Check if there is affinity to a specific node and return it.
        let mut node_names: Option<HashSet<String>> = None;
        for term in &required.node_selector_terms {
            let mut term_node_names: Option<HashSet<String>> = None;
            for r in term.match_fields.iter().flatten() {
                if r.key == "metadata.name" && r.operator == "In" {
                    // The requirements are ANDed, so intersect the nodes.
                    let s: HashSet<String> = r.values.iter().flatten().cloned().collect();
                    term_node_names = Some(match term_node_names {
                        None => s,
                        Some(t) => t.intersection(&s).cloned().collect(),
                    });
                }
            }
            let Some(term_node_names) = term_node_names else {
                // This term has no node.Name field affinity, so all nodes
                // are eligible because the terms are ORed.
                return (None, Status::success());
            };
            node_names
                .get_or_insert_with(HashSet::new)
                .extend(term_node_names);
        }
        match node_names {
            // Every term has conflicting affinity to node.Name: the pod will
            // not match any node.
            Some(n) if n.is_empty() => (
                None,
                Status::new(
                    Code::UnschedulableAndUnresolvable,
                    vec![ERR_REASON_CONFLICT.to_string()],
                ),
            ),
            Some(n) => (
                Some(PreFilterResult {
                    node_names: Some(n),
                }),
                Status::success(),
            ),
            None => (None, Status::success()),
        }
    }
}

#[async_trait]
impl FilterPlugin for NodeAffinityPlugin {
    async fn filter(&self, state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        let node = node_info.node();
        if let Some(added) = &self.added_node_selector {
            if !rusternetes_common::affinity::matches_node_selector(node, added) {
                return Status::new(
                    Code::UnschedulableAndUnresolvable,
                    vec![ERR_REASON_ENFORCED.to_string()],
                );
            }
        }
        // Fall back to computing the required selector here when PreFilter
        // is disabled.
        let matched = match state.read::<RequiredNodeAffinity>(NODE_AFFINITY_STATE_KEY) {
            Some(s) => s.matches(node),
            None => RequiredNodeAffinity::from_pod(pod).matches(node),
        };
        if !matched {
            return Status::new(
                Code::UnschedulableAndUnresolvable,
                vec![ERR_REASON_POD.to_string()],
            );
        }
        Status::success()
    }
}

/// `nodeports.ErrReason`.
pub const ERR_REASON_NODE_PORTS: &str =
    "node(s) didn't have free ports for the requested pod ports";

/// `preFilterStateKey` of the NodePorts plugin.
const NODE_PORTS_STATE_KEY: &str = "PreFilterNodePorts";

/// `nodeports.preFilterState`: the pod's wanted (hostPort, protocol, hostIP).
struct NodePortsState(Vec<(u16, String, String)>);

/// `nodeports.NodePorts` (`plugins/nodeports/node_ports.go`): PreFilter
/// (:75-83) caches the pod's host ports in CycleState, and returns Skip when
/// it has none so Filter does not run; Filter (:162-174) checks that they
/// are free on the node, and is an Error if PreFilter state is missing.
pub struct NodePorts;

impl Plugin for NodePorts {
    fn name(&self) -> &'static str {
        names::NODE_PORTS
    }
}

#[async_trait]
impl PreFilterPlugin for NodePorts {
    async fn pre_filter(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        _nodes: &[Node],
        _handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status) {
        let wanted = crate::advanced::collect_host_ports(pod);
        // Skip if a pod has no ports.
        if wanted.is_empty() {
            return (None, Status::new(Code::Skip, Vec::new()));
        }
        state.write(NODE_PORTS_STATE_KEY, NodePortsState(wanted));
        (None, Status::success())
    }
}

#[async_trait]
impl FilterPlugin for NodePorts {
    async fn filter(&self, state: &CycleState, _pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        let Some(NodePortsState(wanted)) = state.read(NODE_PORTS_STATE_KEY) else {
            return missing_state(NODE_PORTS_STATE_KEY);
        };
        if crate::advanced::fits_host_ports(
            &node_info.node().metadata.name,
            wanted,
            node_info.pods(),
        ) {
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
/// PreFilter (:304-318) computes the pod's request once into CycleState;
/// Filter (:586-619) reads it, and is an Error if it is missing.
///
/// Deviations:
/// - PreFilter's SidecarContainers-disabled rejection and pod-level
///   resources are not ported (restartable init containers are always
///   supported here).
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

/// `preFilterStateKey` of the NodeResourcesFit plugin.
const NODE_RESOURCES_FIT_STATE_KEY: &str = "PreFilterNodeResourcesFit";

/// `noderesources.preFilterState`: the pod's computed resource request.
struct FitState(HashMap<String, i64>);

#[async_trait]
impl PreFilterPlugin for NodeResourcesFit {
    async fn pre_filter(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        _nodes: &[Node],
        _handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status) {
        state.write(
            NODE_RESOURCES_FIT_STATE_KEY,
            FitState(crate::advanced::pod_request_amounts(pod)),
        );
        (None, Status::success())
    }
}

#[async_trait]
impl FilterPlugin for NodeResourcesFit {
    async fn filter(&self, state: &CycleState, _pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        use crate::advanced::{parse_resource_quantity, pod_request_amounts};
        use crate::score::is_scalar_resource_name;

        let Some(FitState(request)) = state.read(NODE_RESOURCES_FIT_STATE_KEY) else {
            return missing_state(NODE_RESOURCES_FIT_STATE_KEY);
        };
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

/// `podtopologyspread.ErrReasonConstraintsNotMatch` (plugin.go:41).
pub const ERR_REASON_CONSTRAINTS_NOT_MATCH: &str =
    "node(s) didn't match pod topology spread constraints";

/// `podtopologyspread.PodTopologySpread` Filter
/// (`plugins/podtopologyspread/filtering.go:309-358`): a node violating a
/// `DoNotSchedule` constraint is Unschedulable.
///
/// Deviations: the skew is recomputed per node by
/// [`crate::advanced::check_topology_spread_constraints`] instead of from
/// PreFilter-computed `preFilterState` (:150-280); the node-label-missing
/// UnschedulableAndUnresolvable case (`ErrReasonNodeLabelNotMatch`) is not
/// distinguished; nominated pods are not counted. PreFilter, the
/// default-constraints args and `PreFilterExtensions` (preemption what-if)
/// are not ported.
pub struct PodTopologySpread;

impl Plugin for PodTopologySpread {
    fn name(&self) -> &'static str {
        names::POD_TOPOLOGY_SPREAD
    }
}

#[async_trait]
impl FilterPlugin for PodTopologySpread {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        if pod.spec.is_none() {
            return Status::success();
        }
        let (ok, _) = crate::advanced::check_topology_spread_constraints(
            node_info.node(),
            pod,
            node_info.all_pods(),
            node_info.all_nodes(),
        );
        if ok {
            return Status::success();
        }
        Status::new(
            Code::Unschedulable,
            vec![ERR_REASON_CONSTRAINTS_NOT_MATCH.to_string()],
        )
    }
}

/// `interpodaffinity.ErrReasonAffinityRulesNotMatch` (filtering.go:39).
pub const ERR_REASON_AFFINITY_RULES_NOT_MATCH: &str = "node(s) didn't match pod affinity rules";
/// `interpodaffinity.ErrReasonAntiAffinityRulesNotMatch` (filtering.go:41).
pub const ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH: &str =
    "node(s) didn't match pod anti-affinity rules";

/// `interpodaffinity.InterPodAffinity` Filter (filtering.go:410-432): required
/// pod affinity unmet is UnschedulableAndUnresolvable, required pod
/// anti-affinity violated is Unschedulable.
///
/// Deviations: the terms are evaluated per node by
/// [`crate::advanced::check_pod_affinity`] /
/// `check_pod_anti_affinity` instead of PreFilter-computed topology counts
/// (:150-260); existing pods' anti-affinity against the incoming pod
/// (`ErrReasonExistingAntiAffinityRulesNotMatch`) is not checked; nominated
/// pods are not counted. PreFilter, namespace selectors' defaulting and
/// `PreFilterExtensions` are not ported.
pub struct InterPodAffinity;

impl Plugin for InterPodAffinity {
    fn name(&self) -> &'static str {
        names::INTER_POD_AFFINITY
    }
}

#[async_trait]
impl FilterPlugin for InterPodAffinity {
    async fn filter(&self, _state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        if pod.spec.is_none() {
            return Status::success();
        }
        let (node, pods, nodes) = (
            node_info.node(),
            node_info.all_pods(),
            node_info.all_nodes(),
        );
        let (affinity_ok, _) = crate::advanced::check_pod_affinity(node, pod, pods, nodes);
        if !affinity_ok {
            return Status::new(
                Code::UnschedulableAndUnresolvable,
                vec![ERR_REASON_AFFINITY_RULES_NOT_MATCH.to_string()],
            );
        }
        let (anti_ok, _) = crate::advanced::check_pod_anti_affinity(node, pod, pods, nodes);
        if !anti_ok {
            return Status::new(
                Code::Unschedulable,
                vec![ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH.to_string()],
            );
        }
        Status::success()
    }
}

/// `preFilterStateKey` of the NodeVolumeLimits plugin.
const NODE_VOLUME_LIMITS_STATE_KEY: &str = "PreFilterNodeVolumeLimits";

/// The listers Filter reads, snapshotted once per cycle by PreFilter.
struct NodeVolumeLimitsState(crate::node_volume_limits::NodeVolumeLimitsSnapshot);

/// `nodevolumelimits.CSILimits` (`plugins/nodevolumelimits/csi.go`):
/// PreFilter (:219-229) returns Skip for a pod without PVC, ephemeral or
/// inline-migratable volumes; Filter (:236-345) counts the node's attached
/// CSI volumes against the driver limits.
///
/// Deviation: upstream's plugin holds informer-backed listers; here PreFilter
/// takes one snapshot of them through the [`Handle`] and Filter reads it from
/// CycleState. A Filter without that state (PreFilter not enabled for the
/// profile) does not check, as before this plugin was an object. A failed
/// snapshot is a PreFilter error, which aborts the cycle
/// (schedule_one.go:496-497).
pub struct NodeVolumeLimits;

impl Plugin for NodeVolumeLimits {
    fn name(&self) -> &'static str {
        names::NODE_VOLUME_LIMITS
    }
}

#[async_trait]
impl PreFilterPlugin for NodeVolumeLimits {
    async fn pre_filter(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        _nodes: &[Node],
        handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status) {
        if !crate::node_volume_limits::CsiLimits::pre_filter(pod) {
            return (None, Status::new(Code::Skip, Vec::new()));
        }
        match handle.node_volume_limits_snapshot().await {
            Ok(snap) => {
                state.write(NODE_VOLUME_LIMITS_STATE_KEY, NodeVolumeLimitsState(snap));
                (None, Status::success())
            }
            Err(e) => (None, Status::from_error(e)),
        }
    }
}

#[async_trait]
impl FilterPlugin for NodeVolumeLimits {
    async fn filter(&self, state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        use crate::node_volume_limits::{CsiLimits, FilterStatus};
        let Some(NodeVolumeLimitsState(snap)) = state.read(NODE_VOLUME_LIMITS_STATE_KEY) else {
            return Status::success();
        };
        let on_node: Vec<&Pod> = node_info.pods().collect();
        match CsiLimits::new(snap).filter(pod, node_info.node(), &on_node) {
            FilterStatus::Success => Status::success(),
            FilterStatus::Unschedulable(m) => Status::new(Code::Unschedulable, vec![m]),
            FilterStatus::UnschedulableAndUnresolvable(m) => {
                Status::new(Code::UnschedulableAndUnresolvable, vec![m])
            }
            FilterStatus::Error(m) => Status::from_error(m),
        }
    }
}
