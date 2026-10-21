//! The Score extension point as upstream runs it.
//!
//! Ported from:
//! - `pkg/scheduler/framework/runtime/framework.go` `RunScorePlugins`
//!   :1273-1382 (per-plugin scores -> `NormalizeScore` -> `score * weight`,
//!   summed per node);
//! - `pkg/scheduler/schedule_one.go` `prioritizeNodes` :791-901 and
//!   `nodeScoreHeap` / `newSortedNodeScores` :904-948 (the node is the heap
//!   root; ties keep heap order, `Randomizer` is only set for extenders);
//! - `pkg/scheduler/framework/plugins/helper/normalize_score.go`
//!   `DefaultNormalizeScore`;
//! - `noderesources/{resource_allocation,least_allocated,most_allocated,
//!   requested_to_capacity_ratio,balanced_allocation,fit}.go`;
//! - `tainttoleration/taint_toleration.go` `Score` / `NormalizeScore`;
//! - `imagelocality/image_locality.go`;
//! - `interpodaffinity/scoring.go` `NormalizeScore` (the rest of both
//!   InterPodAffinity and PodTopologySpread scoring lives in
//!   `inter_pod_affinity_score.rs` / `pod_topology_spread_score.rs`).

use std::collections::HashMap;

use rusternetes_common::resources::{Node, Pod, Toleration};

use crate::advanced::{
    non_zero_request_amounts, parse_resource_quantity, pod_request_amounts,
    toleration_matches_taint,
};
use crate::scheduler_config::{NodeResourcesFitArgs, ResourceSpec, ScoringStrategy};
use crate::volume_binding::{build_broken_linear_function, FunctionShapePoint};

/// `fwk.MaxNodeScore`.
pub const MAX_NODE_SCORE: i64 = 100;
/// `fwk.MinNodeScore`.
pub const MIN_NODE_SCORE: i64 = 0;
/// `config.MaxCustomPriorityScore` (`pkg/scheduler/apis/config/types.go`).
const MAX_CUSTOM_PRIORITY_SCORE: i64 = 10;
/// `maxUtilization` (requested_to_capacity_ratio.go).
const MAX_UTILIZATION: i64 = 100;

/// `helper.DefaultNormalizeScore`: scale `[0, max(scores)]` to
/// `[0, max_priority]`; `reverse` subtracts the result from `max_priority`.
pub fn default_normalize_score(max_priority: i64, reverse: bool, scores: &mut [i64]) {
    let max_count = scores.iter().copied().fold(0, i64::max);
    if max_count == 0 {
        if reverse {
            scores.iter_mut().for_each(|s| *s = max_priority);
        }
        return;
    }
    for s in scores.iter_mut() {
        let mut score = max_priority * *s / max_count;
        if reverse {
            score = max_priority - score;
        }
        *s = score;
    }
}

/// `interpodaffinity.NormalizeScore` (scoring.go:258-290): min/max scaling.
pub fn normalize_inter_pod_affinity(scores: &mut [i64]) {
    if scores.is_empty() {
        return;
    }
    let max = scores.iter().copied().max().unwrap_or(0);
    let min = scores.iter().copied().min().unwrap_or(0);
    let diff = max - min;
    for s in scores.iter_mut() {
        let f = if diff > 0 {
            MAX_NODE_SCORE as f64 * ((*s - min) as f64 / diff as f64)
        } else {
            0.0
        };
        *s = f as i64;
    }
}

/// `leastRequestedScore` (least_allocated.go).
fn least_requested_score(requested: i64, capacity: i64) -> i64 {
    if capacity == 0 || requested > capacity {
        return 0;
    }
    (capacity - requested) * MAX_NODE_SCORE / capacity
}

/// `mostRequestedScore` (most_allocated.go).
fn most_requested_score(requested: i64, capacity: i64) -> i64 {
    if capacity == 0 {
        return 0;
    }
    // `requested` might be greater than `capacity` because pods with no
    // requests get minimum values.
    let requested = requested.min(capacity);
    requested * MAX_NODE_SCORE / capacity
}

/// `balancedResourceScorer` (balanced_allocation.go).
fn balanced_resource_scorer(requested: &[i64], allocable: &[i64]) -> i64 {
    let mut fractions: Vec<f64> = Vec::new();
    let mut total = 0.0;
    for i in 0..requested.len() {
        if allocable[i] == 0 {
            continue;
        }
        let mut f = requested[i] as f64 / allocable[i] as f64;
        if f > 1.0 {
            f = 1.0;
        }
        total += f;
        fractions.push(f);
    }
    let mut std = 0.0f64;
    if fractions.len() == 2 {
        std = ((fractions[0] - fractions[1]) / 2.0).abs();
    } else if fractions.len() > 2 {
        let mean = total / fractions.len() as f64;
        let sum: f64 = fractions.iter().map(|f| (f - mean) * (f - mean)).sum();
        std = (sum / fractions.len() as f64).sqrt();
    }
    ((1.0 - std) * MAX_NODE_SCORE as f64) as i64
}

/// The `scorer` of a `resourceAllocationScorer`.
#[derive(Clone, Debug)]
pub enum Strategy {
    LeastAllocated,
    MostAllocated,
    /// `requestedToCapacityRatioScorer`: the shape, already scaled to the
    /// scheduler's score range (requested_to_capacity_ratio.go:60-69).
    RequestedToCapacityRatio(Vec<FunctionShapePoint>),
    /// `NodeResourcesBalancedAllocation`.
    Balanced,
}

/// `resourceAllocationScorer` (resource_allocation.go).
#[derive(Clone, Debug)]
pub struct ResourceAllocationScorer {
    pub strategy: Strategy,
    pub resources: Vec<ResourceSpec>,
    /// "used to decide whether to use Requested or NonZeroRequested for cpu
    /// and memory": BalancedAllocation sets it, Fit does not.
    pub use_requested: bool,
}

impl ResourceAllocationScorer {
    /// `NewFit`'s `nodeResourceStrategyTypeMap[strategy](args)` (fit.go:63-92),
    /// on defaulted args. `None` for an unknown strategy type.
    pub fn from_fit_args(args: &NodeResourcesFitArgs) -> Option<Self> {
        let s: &ScoringStrategy = args.scoring_strategy.as_ref()?;
        let strategy = match s.strategy_type.as_str() {
            "LeastAllocated" => Strategy::LeastAllocated,
            "MostAllocated" => Strategy::MostAllocated,
            "RequestedToCapacityRatio" => {
                let shape = s
                    .requested_to_capacity_ratio
                    .as_ref()
                    .map(|r| r.shape.as_slice())
                    .unwrap_or(&[]);
                Strategy::RequestedToCapacityRatio(
                    shape
                        .iter()
                        .map(|p| FunctionShapePoint {
                            utilization: i64::from(p.utilization),
                            score: i64::from(p.score)
                                * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
                        })
                        .collect(),
                )
            }
            _ => return None,
        };
        Some(Self {
            strategy,
            resources: s.resources.clone(),
            use_requested: false,
        })
    }

    /// `NewBalancedAllocation` (balanced_allocation.go).
    pub fn balanced(resources: Vec<ResourceSpec>) -> Self {
        Self {
            strategy: Strategy::Balanced,
            resources,
            use_requested: true,
        }
    }

    /// The default `NodeResourcesFit` scorer (`SetDefaults_NodeResourcesFitArgs`).
    pub fn default_fit() -> Self {
        let mut args = NodeResourcesFitArgs::default();
        args.set_defaults();
        Self::from_fit_args(&args).expect("default scoring strategy is valid")
    }

    /// The default `NodeResourcesBalancedAllocation` scorer.
    pub fn default_balanced() -> Self {
        let mut args = crate::scheduler_config::NodeResourcesBalancedAllocationArgs::default();
        args.set_defaults();
        Self::balanced(args.resources)
    }

    /// `calculatePodResourceRequestList` (resource_allocation.go:239-246).
    pub fn pod_requests(&self, pod: &Pod) -> Vec<i64> {
        let raw = pod_request_amounts(pod);
        let (cpu0, mem0) = non_zero_request_amounts(pod);
        self.resources
            .iter()
            .map(|r| match r.name.as_str() {
                "cpu" if !self.use_requested => cpu0,
                "memory" if !self.use_requested => mem0,
                n => raw.get(n).copied().unwrap_or(0),
            })
            .collect()
    }

    /// `isBestEffortPod`.
    pub fn is_best_effort(pod_requests: &[i64]) -> bool {
        pod_requests.iter().all(|r| *r == 0)
    }

    fn scorer(&self, requested: &[i64], allocable: &[i64]) -> i64 {
        match &self.strategy {
            Strategy::LeastAllocated => {
                self.weighted(requested, allocable, least_requested_score, false)
            }
            Strategy::MostAllocated => {
                self.weighted(requested, allocable, most_requested_score, false)
            }
            Strategy::RequestedToCapacityRatio(shape) => {
                if shape.is_empty() {
                    return 0;
                }
                let raw = build_broken_linear_function(shape.clone());
                let f = |requested: i64, capacity: i64| {
                    if capacity == 0 || requested > capacity {
                        return raw(MAX_UTILIZATION);
                    }
                    raw(requested * MAX_UTILIZATION / capacity)
                };
                let mut node_score = 0i64;
                let mut weight_sum = 0i64;
                for i in 0..requested.len() {
                    if allocable[i] == 0 {
                        continue;
                    }
                    let weight = self.resources[i].weight;
                    let s = f(requested[i], allocable[i]);
                    if s > 0 {
                        node_score += s * weight;
                        weight_sum += weight;
                    }
                }
                if weight_sum == 0 {
                    return 0;
                }
                (node_score as f64 / weight_sum as f64).round() as i64
            }
            Strategy::Balanced => balanced_resource_scorer(requested, allocable),
        }
    }

    /// `leastResourceScorer` / `mostResourceScorer`.
    fn weighted(
        &self,
        requested: &[i64],
        allocable: &[i64],
        per_resource: fn(i64, i64) -> i64,
        _unused: bool,
    ) -> i64 {
        let mut node_score = 0i64;
        let mut weight_sum = 0i64;
        for i in 0..requested.len() {
            if allocable[i] == 0 {
                continue;
            }
            let weight = self.resources[i].weight;
            node_score += per_resource(requested[i], allocable[i]) * weight;
            weight_sum += weight;
        }
        if weight_sum == 0 {
            return 0;
        }
        node_score / weight_sum
    }

    /// `resourceAllocationScorer.score` (resource_allocation.go:128-165) with
    /// `calculateResourceAllocatableRequest` (:176-216). `Err` is the
    /// "resources not found" error status.
    pub fn score(
        &self,
        node: &Node,
        usage: &NodeUsage,
        pod_requests: &[i64],
    ) -> Result<i64, String> {
        if self.resources.is_empty() {
            return Err("resources not found".to_string());
        }
        let allocatable = node.status.as_ref().and_then(|s| s.allocatable.as_ref());
        let mut requested = vec![0i64; self.resources.len()];
        let mut allocable = vec![0i64; self.resources.len()];
        for (i, r) in self.resources.iter().enumerate() {
            let name = r.name.as_str();
            let pod_req = pod_requests[i];
            // "If it's an extended resource, and the pod doesn't request it.
            // We return (0, 0) as an implication to bypass scoring".
            if pod_req == 0 && is_scalar_resource_name(name) {
                continue;
            }
            let alloc = allocatable
                .and_then(|a| a.get(name))
                .map(|q| parse_resource_quantity(q, name))
                .unwrap_or(0);
            let used = match name {
                "cpu" => {
                    if self.use_requested {
                        usage.requested.get("cpu").copied().unwrap_or(0)
                    } else {
                        usage.non0_cpu
                    }
                }
                "memory" => {
                    if self.use_requested {
                        usage.requested.get("memory").copied().unwrap_or(0)
                    } else {
                        usage.non0_mem
                    }
                }
                "ephemeral-storage" => usage.requested.get(name).copied().unwrap_or(0),
                n if is_scalar_resource_name(n) => usage.requested.get(n).copied().unwrap_or(0),
                // Not tracked by `framework.Resource`: omitted from scoring.
                _ => continue,
            };
            // "Only fill the extended resource entry when it's non-zero."
            if alloc == 0 {
                continue;
            }
            allocable[i] = alloc;
            requested[i] = used + pod_req;
        }
        Ok(self.scorer(&requested, &allocable))
    }
}

/// `schedutil.IsScalarResourceName` (util/utils.go:142-145).
pub fn is_scalar_resource_name(name: &str) -> bool {
    use rusternetes_common::quota::{
        is_extended_resource_name, is_hugepage_resource_name, is_prefixed_native_resource,
    };
    is_extended_resource_name(name)
        || is_hugepage_resource_name(name)
        || is_prefixed_native_resource(name)
        || name.starts_with("attachable-volumes-")
}

/// The slice of `NodeInfo` the resource scorers read: `GetRequested` and
/// `GetNonZeroRequested` over the node's pods.
#[derive(Clone, Debug, Default)]
pub struct NodeUsage {
    /// `Requested` (milli-cpu for `cpu`, base units otherwise).
    pub requested: HashMap<String, i64>,
    /// `NonZeroRequested.MilliCPU`.
    pub non0_cpu: i64,
    /// `NonZeroRequested.Memory`.
    pub non0_mem: i64,
}

/// `NodeInfo.AddPod` for every non-terminal pod bound to a node.
pub fn node_usages(all_pods: &[Pod]) -> HashMap<String, NodeUsage> {
    let mut out: HashMap<String, NodeUsage> = HashMap::new();
    for p in all_pods {
        let Some(node_name) = p.spec.as_ref().and_then(|s| s.node_name.as_ref()) else {
            continue;
        };
        if node_name.is_empty() {
            continue;
        }
        if matches!(
            p.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(rusternetes_common::types::Phase::Succeeded)
                | Some(rusternetes_common::types::Phase::Failed)
        ) {
            continue;
        }
        let u = out.entry(node_name.clone()).or_default();
        let (c0, m0) = non_zero_request_amounts(p);
        u.non0_cpu += c0;
        u.non0_mem += m0;
        for (k, v) in pod_request_amounts(p) {
            *u.requested.entry(k).or_insert(0) += v;
        }
    }
    out
}

/// `getAllTolerationPreferNoSchedule` (taint_toleration.go).
pub fn tolerations_prefer_no_schedule(pod: &Pod) -> Vec<Toleration> {
    pod.spec
        .as_ref()
        .and_then(|s| s.tolerations.as_ref())
        .map(|ts| {
            ts.iter()
                .filter(|t| {
                    t.effect
                        .as_deref()
                        .is_none_or(|e| e.is_empty() || e == "PreferNoSchedule")
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// `countIntolerableTaintsPreferNoSchedule` (taint_toleration.go).
pub fn count_intolerable_taints_prefer_no_schedule(node: &Node, tolerations: &[Toleration]) -> i64 {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|taints| {
            taints
                .iter()
                .filter(|t| t.effect == "PreferNoSchedule")
                .filter(|t| {
                    !tolerations
                        .iter()
                        .any(|tol| toleration_matches_taint(tol, t))
                })
                .count() as i64
        })
        .unwrap_or(0)
}

/// `imagelocality` constants (image_locality.go:34-42).
const MB: i64 = 1024 * 1024;
const MIN_THRESHOLD: i64 = 23 * MB;
const MAX_CONTAINER_THRESHOLD: i64 = 1000 * MB;

/// `normalizedImageName`.
pub fn normalized_image_name(name: &str) -> String {
    let colon = name.rfind(':').map(|i| i as isize).unwrap_or(-1);
    let slash = name.rfind('/').map(|i| i as isize).unwrap_or(-1);
    if colon <= slash {
        format!("{name}:latest")
    } else {
        name.to_string()
    }
}

/// `ImageStateSummary` per image name: size and how many nodes have it
/// (cache `addNodeImageStates`).
pub fn image_states(nodes: &[&Node]) -> HashMap<String, (i64, usize)> {
    let mut states: HashMap<String, (i64, usize)> = HashMap::new();
    for n in nodes {
        let mut seen = std::collections::HashSet::new();
        for image in n
            .status
            .as_ref()
            .and_then(|s| s.images.as_ref())
            .into_iter()
            .flatten()
        {
            for name in image.names.iter().flatten() {
                if seen.insert(name.clone()) {
                    let e = states.entry(name.clone()).or_insert((image_size(image), 0));
                    e.0 = image_size(image);
                    e.1 += 1;
                }
            }
        }
    }
    states
}

fn image_size(image: &rusternetes_common::resources::ContainerImage) -> i64 {
    image.size_bytes.unwrap_or(0)
}

/// `ImageLocality.Score` (image_locality.go): `sumImageScores` over the pod's
/// init and regular containers, then `calculatePriority`.
pub fn image_locality_score(
    node: &Node,
    pod: &Pod,
    states: &HashMap<String, (i64, usize)>,
    total_num_nodes: usize,
) -> i64 {
    let Some(spec) = pod.spec.as_ref() else {
        return 0;
    };
    let node_images: std::collections::HashSet<&String> = node
        .status
        .as_ref()
        .and_then(|s| s.images.as_ref())
        .into_iter()
        .flatten()
        .flat_map(|i| i.names.iter().flatten())
        .collect();
    let containers: Vec<&str> = spec
        .init_containers
        .iter()
        .flatten()
        .map(|c| c.image.as_str())
        .chain(spec.containers.iter().map(|c| c.image.as_str()))
        .collect();
    let mut sum = 0i64;
    for image in &containers {
        let name = normalized_image_name(image);
        if !node_images.contains(&name) {
            continue;
        }
        if let Some((size, num_nodes)) = states.get(&name) {
            let spread = *num_nodes as f64 / total_num_nodes as f64;
            sum += (*size as f64 * spread) as i64;
        }
    }
    calculate_priority(sum, containers.len())
}

/// `calculatePriority` (image_locality.go).
fn calculate_priority(sum_scores: i64, num_containers: usize) -> i64 {
    let max_threshold = MAX_CONTAINER_THRESHOLD * num_containers as i64;
    let sum = sum_scores.clamp(MIN_THRESHOLD, max_threshold.max(MIN_THRESHOLD));
    if max_threshold == MIN_THRESHOLD {
        return 0;
    }
    MAX_NODE_SCORE * (sum - MIN_THRESHOLD) / (max_threshold - MIN_THRESHOLD)
}

/// One scored plugin: its name, weight and normalised per-node scores
/// (`pluginToNodeScores`), in node order.
pub struct PluginScores {
    pub name: &'static str,
    pub weight: i64,
    pub scores: Vec<i64>,
}

/// The weighted-sum phase of `RunScorePlugins` (framework.go:1352-1378):
/// every normalised score must be within `[0, 100]`.
pub fn total_scores(plugins: &[PluginScores], num_nodes: usize) -> Result<Vec<i64>, String> {
    let mut totals = vec![0i64; num_nodes];
    for p in plugins {
        for (i, &s) in p.scores.iter().enumerate() {
            if !(MIN_NODE_SCORE..=MAX_NODE_SCORE).contains(&s) {
                return Err(format!(
                    "plugin \"{}\" returns an invalid score {}, it should in the range of [{}, {}] after normalizing",
                    p.name, s, MIN_NODE_SCORE, MAX_NODE_SCORE
                ));
            }
            totals[i] += s * p.weight;
        }
    }
    Ok(totals)
}

/// `container/heap` over `nodeScoreHeap`: `Less` is a higher total score
/// (`Randomizer` is always 0 without extenders, schedule_one.go:938).
/// Returns the index of the node `heap.Pop` would return after `heap.Init`.
pub fn select_host(totals: &[i64]) -> Option<usize> {
    if totals.is_empty() {
        return None;
    }
    let mut h: Vec<usize> = (0..totals.len()).collect();
    let less = |h: &[usize], i: usize, j: usize| totals[h[i]] > totals[h[j]];
    // heap.down
    fn down(h: &mut [usize], i0: usize, n: usize, less: &dyn Fn(&[usize], usize, usize) -> bool) {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && less(h, j2, j1) {
                j = j2;
            }
            if !less(h, j, i) {
                break;
            }
            h.swap(i, j);
            i = j;
        }
    }
    let n = h.len();
    // heap.Init
    for i in (0..n / 2).rev() {
        down(&mut h, i, n, &less);
    }
    // heap.Pop: swap(0, n-1), down(0, n-1), pop last.
    h.swap(0, n - 1);
    down(&mut h, 0, n - 1, &less);
    h.pop()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(name: &str, cpu: &str, mem: &str) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name},
            "status": {"allocatable": {"cpu": cpu, "memory": mem}}
        }))
        .unwrap()
    }

    fn pod(node_name: Option<&str>, cpu: &str, mem: &str) -> Pod {
        let mut containers = json!([{"name": "c", "image": "i"}]);
        if !cpu.is_empty() || !mem.is_empty() {
            containers[0]["resources"] = json!({"requests": {"cpu": cpu, "memory": mem}});
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "x", "namespace": "d"},
            "spec": {"nodeName": node_name, "containers": containers}
        }))
        .unwrap()
    }

    fn spec(name: &str, weight: i64) -> ResourceSpec {
        ResourceSpec {
            name: name.to_string(),
            weight,
        }
    }

    fn scorer(strategy: Strategy, use_requested: bool) -> ResourceAllocationScorer {
        ResourceAllocationScorer {
            strategy,
            resources: vec![spec("cpu", 1), spec("memory", 1)],
            use_requested,
        }
    }

    fn run(s: &ResourceAllocationScorer, n: &Node, existing: &[Pod], p: &Pod) -> i64 {
        let u = node_usages(existing);
        let usage = u.get(&n.metadata.name).cloned().unwrap_or_default();
        s.score(n, &usage, &s.pod_requests(p)).unwrap()
    }

    // least_allocated_test.go "nothing scheduled, resources requested,
    // differently sized nodes": node1 37, node2 50 (cpu in milli: "1000" cores
    // is not meaningful here; the test uses raw milli via Req).
    #[test]
    fn least_allocated_differently_sized_nodes() {
        // 4000m/10000 and 6000m/10000 nodes; requests 3000m / 5000.
        let s = scorer(Strategy::LeastAllocated, false);
        let n1 = node("node1", "4", "10000");
        let n2 = node("node2", "6", "10000");
        let p = pod(None, "3", "5000");
        assert_eq!(run(&s, &n1, &[], &p), 37);
        assert_eq!(run(&s, &n2, &[], &p), 50);
    }

    // "no resources requested, pods scheduled": existing pods with no requests
    // count at the non-zero defaults (100m / 200Mi) on huge nodes -> 99
    // (integer division: just under full capacity free).
    #[test]
    fn least_allocated_nonzero_defaults_on_huge_node() {
        let s = scorer(Strategy::LeastAllocated, false);
        let n = node("node1", "4000", "10000000000000");
        let existing = [pod(Some("node1"), "", ""), pod(Some("node1"), "", "")];
        assert_eq!(run(&s, &n, &existing, &pod(None, "", "")), 99);
    }

    // most_allocated_test.go: same shape, inverted: 4000m node with 3000m/5000
    // requested out of 10000 -> (75 + 50) / 2 = 62.
    #[test]
    fn most_allocated_differently_sized_nodes() {
        let s = scorer(Strategy::MostAllocated, false);
        let n1 = node("node1", "4", "10000");
        let n2 = node("node2", "6", "10000");
        let p = pod(None, "3", "5000");
        assert_eq!(run(&s, &n1, &[], &p), 62);
        assert_eq!(run(&s, &n2, &[], &p), 50);
    }

    // "resources requested equal node capacity" / requested > capacity.
    #[test]
    fn most_allocated_clamps_requested_to_capacity() {
        assert_eq!(most_requested_score(12, 10), 100);
        assert_eq!(least_requested_score(12, 10), 0);
        assert_eq!(least_requested_score(1, 0), 0);
    }

    // "resource weight" cases: weights bias the average.
    #[test]
    fn weights_bias_the_average() {
        let mut s = scorer(Strategy::LeastAllocated, true);
        s.resources = vec![spec("cpu", 3), spec("memory", 1)];
        // cpu 25, memory 75 -> (25*3 + 75*1)/4 = 37
        assert_eq!(s.scorer(&[3000, 2500], &[4000, 10000]), 37);
    }

    // requested_to_capacity_ratio_test.go: shape {0:0, 100:10} is bin packing
    // (score scaled x10), and BuildBrokenLinearFunction interpolates.
    #[test]
    fn requested_to_capacity_ratio_linear() {
        let shape = vec![
            FunctionShapePoint {
                utilization: 0,
                score: 0,
            },
            FunctionShapePoint {
                utilization: 100,
                score: 100,
            },
        ];
        let s = scorer(Strategy::RequestedToCapacityRatio(shape), false);
        assert_eq!(s.scorer(&[2000, 5000], &[4000, 10000]), 50);
        assert_eq!(s.scorer(&[4000, 10000], &[4000, 10000]), 100);
        // requested > capacity -> raw(maxUtilization)
        assert_eq!(s.scorer(&[8000, 10000], &[4000, 10000]), 100);
        // resource with allocatable 0 is skipped
        assert_eq!(s.scorer(&[0, 5000], &[0, 10000]), 50);
    }

    // balanced_allocation_test.go: two resources std = |f1-f2|/2.
    #[test]
    fn balanced_two_resources() {
        // fractions 0.5 / 0.5 -> 100; 0.75 / 0.25 -> 75; 1.0 / 0.0 -> 50
        assert_eq!(balanced_resource_scorer(&[2, 5], &[4, 10]), 100);
        assert_eq!(balanced_resource_scorer(&[3, 25], &[4, 100]), 75);
        assert_eq!(balanced_resource_scorer(&[4, 0], &[4, 10]), 50);
        // > 2 resources use the standard deviation
        let v = balanced_resource_scorer(&[1, 1, 4], &[4, 4, 4]);
        assert!(v < 100 && v > 0);
    }

    #[test]
    fn best_effort_pod_requests_all_zero_for_balanced() {
        let b = ResourceAllocationScorer::default_balanced();
        let reqs = b.pod_requests(&pod(None, "", ""));
        assert!(ResourceAllocationScorer::is_best_effort(&reqs));
        // Fit uses non-zero defaults instead.
        let f = ResourceAllocationScorer::default_fit();
        assert_eq!(
            f.pod_requests(&pod(None, "", "")),
            vec![100, 200 * 1024 * 1024]
        );
    }

    // fit_test / least_allocated_test "bypass extended resource if the pod does
    // not request" and "honor extended resource if the pod request".
    #[test]
    fn extended_resources_bypass_unless_requested() {
        let mut s = scorer(Strategy::LeastAllocated, false);
        s.resources = vec![
            spec("cpu", 1),
            spec("memory", 1),
            spec("example.com/gpu", 1),
        ];
        let mut n = node("n", "4", "10000");
        n.status
            .as_mut()
            .unwrap()
            .allocatable
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".to_string(), "4".to_string());
        let p = pod(None, "3", "5000");
        // Not requested: (25 + 50) / 2 = 37.
        assert_eq!(run(&s, &n, &[], &p), 37);
        // Requested 2 of 4: (25 + 50 + 50) / 3 = 41.
        let mut with_gpu = p.clone();
        with_gpu.spec.as_mut().unwrap().containers[0]
            .resources
            .as_mut()
            .unwrap()
            .requests
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".to_string(), "2".to_string());
        assert_eq!(run(&s, &n, &[], &with_gpu), 41);
    }

    #[test]
    fn empty_resources_is_an_error() {
        let mut s = scorer(Strategy::LeastAllocated, false);
        s.resources.clear();
        let n = node("n", "4", "10000");
        assert!(s.score(&n, &NodeUsage::default(), &[]).is_err());
    }

    // helper normalize_score_test.go.
    #[test]
    fn default_normalize() {
        let mut v = vec![10, 5, 0];
        default_normalize_score(100, false, &mut v);
        assert_eq!(v, vec![100, 50, 0]);
        let mut v = vec![10, 5, 0];
        default_normalize_score(100, true, &mut v);
        assert_eq!(v, vec![0, 50, 100]);
        let mut v = vec![0, 0];
        default_normalize_score(100, true, &mut v);
        assert_eq!(v, vec![100, 100]);
        let mut v = vec![0, 0];
        default_normalize_score(100, false, &mut v);
        assert_eq!(v, vec![0, 0]);
    }

    #[test]
    fn inter_pod_affinity_normalize() {
        let mut v = vec![-5, 0, 5];
        normalize_inter_pod_affinity(&mut v);
        assert_eq!(v, vec![0, 50, 100]);
        let mut v = vec![3, 3];
        normalize_inter_pod_affinity(&mut v);
        assert_eq!(v, vec![0, 0]);
    }

    // taint_toleration_test.go TestTaintTolerationScore.
    #[test]
    fn taint_toleration_counts_and_normalizes() {
        let mk = |name: &str, taints: serde_json::Value| -> Node {
            serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Node",
                "metadata": {"name": name},
                "spec": {"taints": taints}
            }))
            .unwrap()
        };
        let a = mk(
            "a",
            json!([{"key": "foo", "value": "bar", "effect": "PreferNoSchedule"}]),
        );
        let b = mk(
            "b",
            json!([
                {"key": "foo", "value": "bar", "effect": "PreferNoSchedule"},
                {"key": "foo2", "value": "bar2", "effect": "PreferNoSchedule"}
            ]),
        );
        let c = mk("c", json!([]));
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"containers": [], "tolerations": [
                {"key": "foo", "operator": "Equal", "value": "bar", "effect": "PreferNoSchedule"}
            ]}
        }))
        .unwrap();
        let tols = tolerations_prefer_no_schedule(&pod);
        let mut scores: Vec<i64> = [&a, &b, &c]
            .iter()
            .map(|n| count_intolerable_taints_prefer_no_schedule(n, &tols))
            .collect();
        assert_eq!(scores, vec![0, 1, 0]);
        default_normalize_score(MAX_NODE_SCORE, true, &mut scores);
        assert_eq!(scores, vec![100, 0, 100]);
    }

    // image_locality_test.go calculatePriority / sumImageScores.
    #[test]
    fn image_locality_priority() {
        assert_eq!(calculate_priority(0, 1), 0);
        assert_eq!(calculate_priority(MAX_CONTAINER_THRESHOLD, 1), 100);
        assert_eq!(calculate_priority(2 * MAX_CONTAINER_THRESHOLD, 1), 100);
        // 1 container: (500MB-23MB)*100/(1000MB-23MB) = 48
        assert_eq!(calculate_priority(500 * MB, 1), 48);
        assert_eq!(normalized_image_name("busybox"), "busybox:latest");
        assert_eq!(normalized_image_name("r:5000/app"), "r:5000/app:latest");
        assert_eq!(normalized_image_name("r:5000/app:v1"), "r:5000/app:v1");
    }

    // schedule_one.go newSortedNodeScores + Pop: highest total wins, a tie
    // follows container/heap order (heap.Init does not reorder equal items).
    #[test]
    fn select_host_picks_highest_and_first_of_ties() {
        assert_eq!(select_host(&[]), None);
        assert_eq!(select_host(&[7]), Some(0));
        assert_eq!(select_host(&[1, 9, 3]), Some(1));
        assert_eq!(select_host(&[5, 5, 5]), Some(0));
        assert_eq!(select_host(&[3, 8, 8, 1]), Some(1));
    }

    #[test]
    fn total_scores_weighted_sum_and_range_check() {
        let p = vec![
            PluginScores {
                name: "A",
                weight: 3,
                scores: vec![100, 0],
            },
            PluginScores {
                name: "B",
                weight: 1,
                scores: vec![10, 90],
            },
        ];
        assert_eq!(total_scores(&p, 2).unwrap(), vec![310, 90]);
        let bad = vec![PluginScores {
            name: "A",
            weight: 1,
            scores: vec![101],
        }];
        assert!(total_scores(&bad, 1)
            .unwrap_err()
            .contains("returns an invalid score 101"));
    }
}
