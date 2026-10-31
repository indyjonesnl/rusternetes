//! In-tree scheduler plugin objects dispatched through the framework
//! (`fwk` traits). Plugins migrate here from the inline pipeline in
//! `scheduler.rs` one at a time (#3455).

use crate::advanced::toleration_matches_taint;
use crate::framework_runtime::names;
use crate::fwk::{Code, CycleState, FilterPlugin, NodeInfo, Plugin, Status};
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Taint};
use std::sync::Arc;

/// The factory for an in-tree Filter plugin by name, `None` for names
/// that are not (yet) plugin objects.
pub fn new_filter_plugin(name: &str) -> Option<Arc<dyn FilterPlugin>> {
    match name {
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
