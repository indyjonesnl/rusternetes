//! PodTopologySpread PreFilter state and Filter.
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/podtopologyspread/filtering.go`
//! (`preFilterState` :39-90, `criticalPaths` :92-152, `PreFilter` :154-163,
//! `calPreFilterState` :244-319, `Filter` :321-358) and `plugin.go`
//! (`getConstraints` :273-294).
//!
//! `NodeInclusionPolicyInPodTopologySpread` and `MatchLabelKeysInPodTopologySpread`
//! are GA and always on. Deviation: `PreFilterExtensions` (`AddPod`/`RemovePod`,
//! :186-243) are not ported, so nominated pods and preemption what-ifs do not
//! update the counts.

use std::collections::HashMap;

use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::Selector;

use crate::pod_topology_spread_score::{
    build_default_constraints, count_pods_match_selector, filter_topology_spread_constraints,
    has_own_constraints, node_labels_match_spread_constraints, Args, Constraint,
};

/// `ErrReasonConstraintsNotMatch` (plugin.go:41).
pub const ERR_REASON_CONSTRAINTS_NOT_MATCH: &str =
    "node(s) didn't match pod topology spread constraints";
/// `ErrReasonNodeLabelNotMatch` (plugin.go:43).
pub const ERR_REASON_NODE_LABEL_NOT_MATCH: &str =
    "node(s) didn't match pod topology spread constraints (missing required label)";

const MAX_INT32: i64 = i32::MAX as i64;

/// `criticalPaths` (filtering.go:104-152): the two topology values with the
/// fewest matching pods; `[0]` always holds the minimum.
#[derive(Debug, Clone)]
struct CriticalPaths([(String, i64); 2]);

impl CriticalPaths {
    fn new() -> Self {
        Self([(String::new(), MAX_INT32), (String::new(), MAX_INT32)])
    }

    fn update(&mut self, tp_val: &str, num: i64) {
        let p = &mut self.0;
        // first verify if `tp_val` exists or not
        let i = if tp_val == p[0].0 {
            Some(0)
        } else if tp_val == p[1].0 {
            Some(1)
        } else {
            None
        };
        match i {
            Some(i) => {
                p[i].1 = num;
                if p[0].1 > p[1].1 {
                    p.swap(0, 1);
                }
            }
            None => {
                if num < p[0].1 {
                    p[1] = std::mem::replace(&mut p[0], (tp_val.to_string(), num));
                } else if num < p[1].1 {
                    p[1] = (tp_val.to_string(), num);
                }
            }
        }
    }
}

/// `preFilterState` (filtering.go:39-56).
#[derive(Debug)]
pub struct PreFilterState {
    constraints: Vec<Constraint>,
    /// Indexed by constraint.
    critical_paths: Vec<CriticalPaths>,
    /// Indexed by constraint: topology value -> number of matching pods.
    tp_value_to_match_num: Vec<HashMap<String, i64>>,
}

impl PreFilterState {
    /// `minMatchNum` (filtering.go:58-72): the global minimum, 0 when fewer
    /// than `minDomains` eligible domains exist.
    fn min_match_num(&self, constraint_id: usize, min_domains: i32) -> i64 {
        let mut min = self.critical_paths[constraint_id].0[0].1;
        if (self.tp_value_to_match_num[constraint_id].len() as i64) < i64::from(min_domains) {
            min = 0;
        }
        min
    }
}

/// `getConstraints` (plugin.go:273-294): the pod's hard constraints, or the
/// hard default constraints when it has none.
fn get_constraints(
    pod: &Pod,
    args: &Args,
    default_selector: Option<&Selector>,
) -> Result<Vec<Constraint>, String> {
    if has_own_constraints(pod) {
        let own = pod
            .spec
            .as_ref()
            .and_then(|s| s.topology_spread_constraints.as_deref())
            .unwrap_or(&[]);
        filter_topology_spread_constraints(own, pod.metadata.labels.as_ref(), "DoNotSchedule")
            .map_err(|e| format!("obtaining pod's hard topology spread constraints: {e}"))
    } else {
        build_default_constraints(pod, args, default_selector, "DoNotSchedule")
            .map_err(|e| format!("setting default hard topology spread constraints: {e}"))
    }
}

/// `PodTopologySpread.PreFilter` / `calPreFilterState` (filtering.go:154-163,
/// :244-319). `Ok(None)` is the `Skip` status (no hard constraints);
/// `pods_by_node` is `nodeInfo.GetPods()` per node.
pub fn pre_filter(
    pod: &Pod,
    all_nodes: &[Node],
    pods_by_node: &HashMap<&str, Vec<&Pod>>,
    args: &Args,
    default_selector: Option<&Selector>,
) -> Result<Option<PreFilterState>, String> {
    let constraints = get_constraints(pod, args, default_selector)
        .map_err(|e| format!("get constraints from pod: {e}"))?;
    if constraints.is_empty() {
        return Ok(None);
    }
    let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
    let mut tp_value_to_match_num: Vec<HashMap<String, i64>> =
        vec![HashMap::new(); constraints.len()];
    let no_pods: Vec<&Pod> = Vec::new();
    for node in all_nodes {
        // Ensure current node's labels contains all topologyKeys in 'Constraints'.
        if !node_labels_match_spread_constraints(node, &constraints) {
            continue;
        }
        let node_pods = pods_by_node
            .get(node.metadata.name.as_str())
            .unwrap_or(&no_pods);
        for (i, c) in constraints.iter().enumerate() {
            if !c.match_node_inclusion_policies(pod, node) {
                continue;
            }
            let value = node
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(&c.topology_key))
                .cloned()
                .unwrap_or_default();
            *tp_value_to_match_num[i].entry(value).or_default() +=
                count_pods_match_selector(node_pods, &c.selector, ns);
        }
    }
    // calculate min match for each constraint and topology value
    let critical_paths = tp_value_to_match_num
        .iter()
        .map(|m| {
            let mut paths = CriticalPaths::new();
            for (value, num) in m {
                paths.update(value, *num);
            }
            paths
        })
        .collect();
    Ok(Some(PreFilterState {
        constraints,
        critical_paths,
        tp_value_to_match_num,
    }))
}

/// `Filter` (filtering.go:321-358): the reason `node` fails, with whether it
/// is `UnschedulableAndUnresolvable` (a missing topology label).
pub fn filter(state: &PreFilterState, pod: &Pod, node: &Node) -> Option<(&'static str, bool)> {
    for (i, c) in state.constraints.iter().enumerate() {
        let Some(tp_val) = node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&c.topology_key))
        else {
            return Some((ERR_REASON_NODE_LABEL_NOT_MATCH, true));
        };
        // judging criteria:
        // 'existing matching num' + 'if self-match (1 or 0)' - 'global minimum' <= 'maxSkew'
        let min_match_num = state.min_match_num(i, c.min_domains);
        let self_match_num = i64::from(c.selector.matches(pod.metadata.labels.as_ref()));
        let match_num = state.tp_value_to_match_num[i]
            .get(tp_val)
            .copied()
            .unwrap_or(0);
        let skew = match_num + self_match_num - min_match_num;
        if skew > i64::from(c.max_skew) {
            return Some((ERR_REASON_CONSTRAINTS_NOT_MATCH, false));
        }
    }
    None
}
