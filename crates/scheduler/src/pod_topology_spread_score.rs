//! The PodTopologySpread plugin's PreScore / Score / NormalizeScore.
//!
//! Ported from `pkg/scheduler/framework/plugins/podtopologyspread/`:
//! - `scoring.go`: `preScoreState`, `initPreScoreState`, `PreScore`, `Score`,
//!   `NormalizeScore`, `topologyNormalizingWeight`, `scoreForCount`;
//! - `common.go`: `topologySpreadConstraint`, `matchNodeInclusionPolicies`,
//!   `buildDefaultConstraints`, `nodeLabelsMatchSpreadConstraints`,
//!   `filterTopologySpreadConstraints`, `countPodsMatchSelector`;
//! - `plugin.go`: `systemDefaultConstraints`, `New` (`defaultingType`,
//!   `systemDefaulted`);
//! - `plugins/helper/spread.go`: `DefaultSelector`, `GetPodServices`;
//! - `component-helpers/scheduling/corev1/nodeaffinity` `GetRequiredNodeAffinity`
//!   / `Match` and `v1helper.FindMatchingUntoleratedTaint` (for the node
//!   inclusion policies).
//!
//! Tests are ported from `podtopologyspread/scoring_test.go`
//! (`TestPreScoreSkip`, `TestPreScoreStateEmptyNodes`,
//! `TestPodTopologySpreadScore`).
//!
//! Feature gates are at their 1.35 defaults: `NodeInclusionPolicyInPodTopologySpread`
//! and `MatchLabelKeysInPodTopologySpread` are on (so the
//! `!enableNodeInclusionPolicyInPodTopologySpread` branch of `PreScore` and the
//! gate-off cases of the upstream tests do not apply);
//! `TaintTolerationComparisonOperators` is Alpha/off.
//!
//! Deviations: `PreScore`'s `parallelizer.Until` over all nodes runs
//! sequentially; the per-node pod list (`nodeInfo.GetPods()`) is the non-terminal
//! pods of `all_pods` bound to the node (the pod informer drops Succeeded/Failed).

use std::collections::{HashMap, HashSet};

use rusternetes_common::resources::pod::TopologySpreadConstraint;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::{
    label_selector_as_selector, LabelSelector, LabelSelectorRequirement, Phase, Selector,
};

use crate::queue::topology_spread_hints::merge_label_set_with_selector;
use crate::score::MAX_NODE_SCORE;

/// `v1.LabelHostname`.
pub const LABEL_HOSTNAME: &str = "kubernetes.io/hostname";
/// `v1.LabelTopologyZone`.
pub const LABEL_TOPOLOGY_ZONE: &str = "topology.kubernetes.io/zone";
/// `invalidScore` (scoring.go:34).
const INVALID_SCORE: i64 = -1;

/// `systemDefaultConstraints` (plugin.go:46-57).
fn system_default_constraints() -> Vec<TopologySpreadConstraint> {
    let c = |key: &str, max_skew: i32| TopologySpreadConstraint {
        max_skew,
        topology_key: key.to_string(),
        when_unsatisfiable: "ScheduleAnyway".to_string(),
        label_selector: None,
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    vec![c(LABEL_HOSTNAME, 3), c(LABEL_TOPOLOGY_ZONE, 5)]
}

/// The plugin's resolved configuration (`New`, plugin.go:96-120): the default
/// constraints and whether they are the `System` ones.
#[derive(Clone, Debug)]
pub struct Args {
    pub default_constraints: Vec<TopologySpreadConstraint>,
    pub system_defaulted: bool,
}

impl Default for Args {
    /// `SetDefaults_PodTopologySpreadArgs`: `defaultingType: System`.
    fn default() -> Self {
        Self {
            default_constraints: system_default_constraints(),
            system_defaulted: true,
        }
    }
}

impl Args {
    /// `New`: `args.DefaultingType == SystemDefaulting` replaces the configured
    /// constraints with `systemDefaultConstraints`.
    pub fn from_config(args: &crate::scheduler_config::PodTopologySpreadArgs) -> Self {
        if args.defaulting_type == "List" {
            Self {
                default_constraints: args.default_constraints.clone(),
                system_defaulted: false,
            }
        } else {
            Self::default()
        }
    }
}

/// `topologySpreadConstraint` (common.go:32-39), without `MinDomains`, which
/// only the Filter reads.
#[derive(Clone, Debug)]
pub struct Constraint {
    pub max_skew: i32,
    pub topology_key: String,
    pub selector: Selector,
    /// `NodeAffinityPolicy == Honor`.
    pub node_affinity_honor: bool,
    /// `NodeTaintsPolicy == Honor`.
    pub node_taints_honor: bool,
}

/// `filterTopologySpreadConstraints` (common.go:92-130).
fn filter_topology_spread_constraints(
    constraints: &[TopologySpreadConstraint],
    pod_labels: Option<&HashMap<String, String>>,
    action: &str,
) -> Result<Vec<Constraint>, String> {
    let mut result = Vec::new();
    for c in constraints {
        if c.when_unsatisfiable != action {
            continue;
        }
        let mut selector = label_selector_as_selector(c.label_selector.as_ref())?;
        if let Some(keys) = c.match_label_keys.as_ref().filter(|k| !k.is_empty()) {
            let match_labels: HashMap<String, String> = keys
                .iter()
                .filter_map(|k| {
                    pod_labels
                        .and_then(|l| l.get(k))
                        .map(|v| (k.clone(), v.clone()))
                })
                .collect();
            if !match_labels.is_empty() {
                selector = merge_label_set_with_selector(
                    match_labels,
                    c.label_selector.as_ref(),
                    selector,
                )?;
            }
        }
        result.push(Constraint {
            max_skew: c.max_skew,
            topology_key: c.topology_key.clone(),
            selector,
            // nil NodeAffinityPolicy is Honor, nil NodeTaintsPolicy is Ignore.
            node_affinity_honor: c.node_affinity_policy.as_deref().unwrap_or("Honor") == "Honor",
            node_taints_honor: c.node_taints_policy.as_deref() == Some("Honor"),
        });
    }
    Ok(result)
}

fn pod_namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("default")
}

/// What `helper.DefaultSelector` reads besides the pod: the Services of the
/// pod's namespace and the pod's controller, as the listers return them.
pub struct DefaultSelectorInputs {
    /// `Service.Spec.Selector` of every Service in the pod's namespace.
    pub service_selectors: Vec<Option<HashMap<String, String>>>,
    /// The controller's selector, when it is a ReplicationController,
    /// ReplicaSet or StatefulSet that exists.
    pub owner: Option<OwnerSelector>,
}

/// The selector of the pod's controller (`DefaultSelector` switch).
pub enum OwnerSelector {
    /// `rc.Spec.Selector` (a label map, merged into the label set).
    ReplicationController(Option<HashMap<String, String>>),
    /// `rs.Spec.Selector` / `ss.Spec.Selector` (requirements added to the selector).
    Selector(Option<LabelSelector>),
}

/// The kind of controller whose selector `DefaultSelector` consults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerKind {
    ReplicationController,
    ReplicaSet,
    StatefulSet,
}

/// `metav1.GetControllerOfNoCopy` + the `gvk` switch of `DefaultSelector`:
/// `Some((kind, name))` when the pod's controller is an RC, RS or StatefulSet.
pub fn default_selector_owner(pod: &Pod) -> Option<(OwnerKind, String)> {
    let owner = pod
        .metadata
        .owner_references
        .iter()
        .flatten()
        .find(|o| o.controller == Some(true))?;
    // `schema.ParseGroupVersion(owner.APIVersion)`: "v1" or "apps/v1".
    let kind = match (owner.api_version.as_str(), owner.kind.as_str()) {
        ("v1", "ReplicationController") => OwnerKind::ReplicationController,
        ("apps/v1", "ReplicaSet") => OwnerKind::ReplicaSet,
        ("apps/v1", "StatefulSet") => OwnerKind::StatefulSet,
        _ => return None,
    };
    Some((kind, owner.name.clone()))
}

/// `helper.DefaultSelector` (spread.go:35-84): a selector deduced from the
/// Services, ReplicationControllers, ReplicaSets and StatefulSets matching the
/// pod. An empty selector is `Selector::Everything` (`Empty()`).
pub fn default_selector(pod: &Pod, inputs: &DefaultSelectorInputs) -> Selector {
    let pod_labels = pod.metadata.labels.as_ref();
    // GetPodServices: services with a nil selector match nothing.
    let mut label_set: HashMap<String, String> = HashMap::new();
    for sel in inputs.service_selectors.iter().flatten() {
        let matches = sel
            .iter()
            .all(|(k, v)| pod_labels.and_then(|l| l.get(k)) == Some(v));
        if matches {
            label_set.extend(sel.clone());
        }
    }
    let mut expressions: Vec<LabelSelectorRequirement> = Vec::new();
    match &inputs.owner {
        Some(OwnerSelector::ReplicationController(sel)) => {
            // labelSet = Merge(labelSet, rc.Spec.Selector)
            label_set.extend(sel.iter().flatten().map(|(k, v)| (k.clone(), v.clone())));
        }
        Some(OwnerSelector::Selector(Some(other))) => {
            // `LabelSelectorAsSelector(rs.Spec.Selector)` then `Add(r...)`: a
            // nil selector is `Nothing()` (`Requirements()` ok=false, skipped).
            if let Ok(s) = label_selector_as_selector(Some(other)) {
                if !s.is_everything() && !matches!(s, Selector::Nothing) {
                    for (k, v) in other.match_labels.iter().flatten() {
                        expressions.push(LabelSelectorRequirement {
                            key: k.clone(),
                            operator: "In".to_string(),
                            values: Some(vec![v.clone()]),
                        });
                    }
                    expressions.extend(other.match_expressions.iter().flatten().cloned());
                }
            }
        }
        Some(OwnerSelector::Selector(None)) | None => {}
    }
    if label_set.is_empty() && expressions.is_empty() {
        return Selector::Everything;
    }
    let combined = LabelSelector {
        match_labels: if label_set.is_empty() {
            None
        } else {
            Some(label_set)
        },
        match_expressions: if expressions.is_empty() {
            None
        } else {
            Some(expressions)
        },
    };
    // `labelSet.AsSelector()` does not validate; an unusable selector selects
    // nothing useful, so treat it as empty (the constraints are then dropped).
    label_selector_as_selector(Some(&combined)).unwrap_or(Selector::Everything)
}

fn has_own_constraints(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.topology_spread_constraints.as_ref())
        .is_some_and(|c| !c.is_empty())
}

/// Whether `PreScore` will call `buildDefaultConstraints` with a non-empty
/// default list, i.e. whether the Services/controller of the pod must be read.
pub fn needs_default_selector(pod: &Pod, args: &Args) -> bool {
    !has_own_constraints(pod)
        && args
            .default_constraints
            .iter()
            .any(|c| c.when_unsatisfiable == "ScheduleAnyway")
}

/// `buildDefaultConstraints` (common.go:57-71).
fn build_default_constraints(
    pod: &Pod,
    args: &Args,
    default_selector: Option<&Selector>,
    action: &str,
) -> Result<Vec<Constraint>, String> {
    let mut constraints = filter_topology_spread_constraints(
        &args.default_constraints,
        pod.metadata.labels.as_ref(),
        action,
    )?;
    if constraints.is_empty() {
        return Ok(Vec::new());
    }
    let selector = default_selector.cloned().unwrap_or(Selector::Everything);
    if selector.is_everything() {
        return Ok(Vec::new());
    }
    for c in &mut constraints {
        c.selector = selector.clone();
    }
    Ok(constraints)
}

/// `nodeLabelsMatchSpreadConstraints` (common.go:73-81).
fn node_labels_match_spread_constraints(node: &Node, constraints: &[Constraint]) -> bool {
    constraints.iter().all(|c| {
        node.metadata
            .labels
            .as_ref()
            .is_some_and(|l| l.contains_key(&c.topology_key))
    })
}

/// `nodeaffinity.GetRequiredNodeAffinity(pod).Match(node)`: `spec.nodeSelector`
/// AND the required node affinity terms.
fn required_node_affinity_matches(pod: &Pod, node: &Node) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return true;
    };
    if let Some(sel) = spec.node_selector.as_ref() {
        let labels = node.metadata.labels.as_ref();
        if !sel
            .iter()
            .all(|(k, v)| labels.and_then(|l| l.get(k)) == Some(v))
        {
            return false;
        }
    }
    if let Some(required) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
    {
        if !rusternetes_common::affinity::matches_node_selector(node, required) {
            return false;
        }
    }
    true
}

/// `FindMatchingUntoleratedTaint(.., DoNotScheduleTaintsFilterFunc())` as a
/// bool: some NoSchedule/NoExecute taint of the node is not tolerated.
fn has_untolerated_taint(pod: &Pod, node: &Node) -> bool {
    let tolerations = pod
        .spec
        .as_ref()
        .and_then(|s| s.tolerations.as_deref())
        .unwrap_or(&[]);
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .into_iter()
        .flatten()
        .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
        .any(|t| {
            !tolerations
                .iter()
                .any(|tol| crate::advanced::toleration_matches_taint(tol, t))
        })
}

impl Constraint {
    /// `matchNodeInclusionPolicies` (common.go:41-55).
    fn match_node_inclusion_policies(&self, pod: &Pod, node: &Node) -> bool {
        if self.node_affinity_honor && !required_node_affinity_matches(pod, node) {
            return false;
        }
        if self.node_taints_honor && has_untolerated_taint(pod, node) {
            return false;
        }
        true
    }
}

/// `nodeInfo.GetPods()` for every node: the non-terminal pods bound to it.
pub fn pods_by_node(all_pods: &[Pod]) -> HashMap<&str, Vec<&Pod>> {
    let mut m: HashMap<&str, Vec<&Pod>> = HashMap::new();
    for p in all_pods {
        let Some(name) = p.spec.as_ref().and_then(|s| s.node_name.as_deref()) else {
            continue;
        };
        if matches!(
            p.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Succeeded) | Some(Phase::Failed)
        ) {
            continue;
        }
        m.entry(name).or_default().push(p);
    }
    m
}

/// `countPodsMatchSelector` (common.go:148-163): terminating pods and pods of
/// another namespace are skipped.
fn count_pods_match_selector(pods: &[&Pod], selector: &Selector, ns: &str) -> i64 {
    if selector.is_everything() {
        // `selector.Empty()`
        return 0;
    }
    pods.iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none() && pod_namespace(p) == ns)
        .filter(|p| selector.matches(p.metadata.labels.as_ref()))
        .count() as i64
}

/// `preScoreState` (scoring.go:36-49).
#[derive(Debug)]
pub struct PreScoreState {
    pub constraints: Vec<Constraint>,
    /// Node names missing some `constraints[*].topologyKey`.
    pub ignored_nodes: HashSet<String>,
    /// Indexed by constraint: topology value -> number of matching pods. A
    /// value's presence is upstream's non-nil `*int64`.
    pub topology_value_to_pod_counts: Vec<HashMap<String, i64>>,
    /// `TopologyNormalizingWeight`.
    pub topology_normalizing_weight: Vec<f64>,
}

/// `topologyNormalizingWeight` (scoring.go:271-273).
pub fn topology_normalizing_weight(size: usize) -> f64 {
    ((size + 2) as f64).ln()
}

/// `scoreForCount` (scoring.go:279-281).
fn score_for_count(cnt: i64, max_skew: i32, tp_weight: f64) -> f64 {
    cnt as f64 * tp_weight + f64::from(max_skew - 1)
}

/// `initPreScoreState` (scoring.go:61-123). Returns the state with empty
/// `constraints` when there is nothing to score.
fn init_pre_score_state(
    pod: &Pod,
    filtered_nodes: &[&Node],
    require_all_topologies: bool,
    args: &Args,
    default_selector: Option<&Selector>,
) -> Result<PreScoreState, String> {
    let mut s = PreScoreState {
        constraints: Vec::new(),
        ignored_nodes: HashSet::new(),
        topology_value_to_pod_counts: Vec::new(),
        topology_normalizing_weight: Vec::new(),
    };
    s.constraints = if has_own_constraints(pod) {
        let own = pod
            .spec
            .as_ref()
            .and_then(|sp| sp.topology_spread_constraints.as_deref())
            .unwrap_or(&[]);
        filter_topology_spread_constraints(own, pod.metadata.labels.as_ref(), "ScheduleAnyway")
            .map_err(|e| format!("obtaining pod's soft topology spread constraints: {e}"))?
    } else {
        build_default_constraints(pod, args, default_selector, "ScheduleAnyway")
            .map_err(|e| format!("setting default soft topology spread constraints: {e}"))?
    };
    if s.constraints.is_empty() {
        return Ok(s);
    }
    s.topology_value_to_pod_counts = vec![HashMap::new(); s.constraints.len()];
    let mut topo_size = vec![0usize; s.constraints.len()];
    for node in filtered_nodes {
        if require_all_topologies && !node_labels_match_spread_constraints(node, &s.constraints) {
            // Nodes which don't have all required topologyKeys present are
            // ignored when scoring later.
            s.ignored_nodes.insert(node.metadata.name.clone());
            continue;
        }
        for (i, c) in s.constraints.iter().enumerate() {
            // per-node counts are calculated during Score.
            if c.topology_key == LABEL_HOSTNAME {
                continue;
            }
            let value = node
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(&c.topology_key))
                .cloned()
                .unwrap_or_default();
            if let std::collections::hash_map::Entry::Vacant(e) =
                s.topology_value_to_pod_counts[i].entry(value)
            {
                e.insert(0);
                topo_size[i] += 1;
            }
        }
    }
    s.topology_normalizing_weight = s
        .constraints
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut sz = topo_size[i];
            if c.topology_key == LABEL_HOSTNAME {
                sz = filtered_nodes.len() - s.ignored_nodes.len();
            }
            topology_normalizing_weight(sz)
        })
        .collect();
    Ok(s)
}

/// `PodTopologySpread.PreScore` (scoring.go:126-199). `filtered_nodes` are the
/// nodes that passed the filters; `all_nodes` is the snapshot. `Ok(None)` is
/// the `Skip` status.
pub fn pre_score(
    pod: &Pod,
    filtered_nodes: &[&Node],
    all_nodes: &[Node],
    pods_by_node: &HashMap<&str, Vec<&Pod>>,
    args: &Args,
    default_selector: Option<&Selector>,
) -> Result<Option<PreScoreState>, String> {
    if all_nodes.is_empty() {
        // No need to score.
        return Ok(None);
    }
    // Only require that nodes have all the topology labels if using
    // non-system-default spreading rules. This allows nodes that don't have a
    // zone label to still have hostname spreading.
    let require_all_topologies = has_own_constraints(pod) || !args.system_defaulted;
    let mut state = init_pre_score_state(
        pod,
        filtered_nodes,
        require_all_topologies,
        args,
        default_selector,
    )
    .map_err(|e| format!("calculating preScoreState: {e}"))?;

    // return Skip if incoming pod doesn't have soft topology spread Constraints.
    if state.constraints.is_empty() {
        return Ok(None);
    }

    let ns = pod_namespace(pod);
    let no_pods: Vec<&Pod> = Vec::new();
    for node in all_nodes {
        // All topologyKeys need to be present in `node`
        if require_all_topologies && !node_labels_match_spread_constraints(node, &state.constraints)
        {
            continue;
        }
        let node_pods = pods_by_node
            .get(node.metadata.name.as_str())
            .unwrap_or(&no_pods);
        for (i, c) in state.constraints.iter().enumerate() {
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
            // If current topology pair is not associated with any candidate
            // node, continue to avoid unnecessary calculation. Per-node counts
            // are also skipped, as they are done during Score.
            let Some(tp_count) = state.topology_value_to_pod_counts[i].get_mut(&value) else {
                continue;
            };
            *tp_count += count_pods_match_selector(node_pods, &c.selector, ns);
        }
    }
    Ok(Some(state))
}

/// `PodTopologySpread.Score` (scoring.go:204-231). `node_pods` is
/// `nodeInfo.GetPods()`.
pub fn score(state: &PreScoreState, pod: &Pod, node: &Node, node_pods: &[&Pod]) -> i64 {
    // Return if the node is not qualified.
    if state.ignored_nodes.contains(&node.metadata.name) {
        return 0;
    }
    // For each present <pair>, current node gets a credit of <matchSum>. And we
    // sum up <matchSum> and return it as this node's score.
    let mut score = 0f64;
    for (i, c) in state.constraints.iter().enumerate() {
        if let Some(tp_val) = node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&c.topology_key))
        {
            let cnt = if c.topology_key == LABEL_HOSTNAME {
                count_pods_match_selector(node_pods, &c.selector, pod_namespace(pod))
            } else {
                state.topology_value_to_pod_counts[i]
                    .get(tp_val)
                    .copied()
                    .unwrap_or(0)
            };
            score += score_for_count(cnt, c.max_skew, state.topology_normalizing_weight[i]);
        }
    }
    score.round() as i64
}

/// `PodTopologySpread.NormalizeScore` (scoring.go:234-271): ignored nodes score
/// 0, the rest are inverted against `maxScore + minScore`. `names[i]` is the
/// node of `scores[i]`.
pub fn normalize_score(state: &PreScoreState, names: &[&str], scores: &mut [i64]) {
    // Calculate <minScore> and <maxScore>
    let mut min_score = i64::MAX;
    let mut max_score = 0i64;
    for (i, score) in scores.iter_mut().enumerate() {
        // it's mandatory to check if <score.Name> is present in m.IgnoredNodes
        if state.ignored_nodes.contains(names[i]) {
            *score = INVALID_SCORE;
            continue;
        }
        if *score < min_score {
            min_score = *score;
        }
        if *score > max_score {
            max_score = *score;
        }
    }
    for score in scores.iter_mut() {
        if *score == INVALID_SCORE {
            *score = 0;
            continue;
        }
        if max_score == 0 {
            *score = MAX_NODE_SCORE;
            continue;
        }
        *score = MAX_NODE_SCORE * (max_score + min_score - *score) / max_score;
    }
}

/// Read the inputs of [`default_selector`] for `pod` (the Service, RC, RS and
/// StatefulSet listers of `DefaultSelector`). Errors from the lists propagate;
/// a missing controller is the lister's NotFound, which `DefaultSelector`
/// ignores.
pub async fn read_default_selector_inputs<S>(
    data: &crate::data_plane::DataPlane<S>,
    pod: &Pod,
) -> rusternetes_common::Result<DefaultSelectorInputs>
where
    S: rusternetes_storage::Storage + Send + Sync + 'static,
{
    use rusternetes_common::resources::{ReplicaSet, ReplicationController, Service, StatefulSet};
    let ns = pod_namespace(pod);
    let services: Vec<Service> = data.list_namespaced("services", "/api/v1", ns).await?;
    let service_selectors = services.into_iter().map(|s| s.spec.selector).collect();
    let owner = match default_selector_owner(pod) {
        None => None,
        Some((OwnerKind::ReplicationController, name)) => data
            .get_namespaced::<ReplicationController>("replicationcontrollers", "/api/v1", ns, &name)
            .await?
            .map(|rc| OwnerSelector::ReplicationController(rc.spec.selector)),
        Some((OwnerKind::ReplicaSet, name)) => data
            .get_namespaced::<ReplicaSet>("replicasets", "/apis/apps/v1", ns, &name)
            .await?
            .map(|rs| OwnerSelector::Selector(Some(rs.spec.selector))),
        Some((OwnerKind::StatefulSet, name)) => data
            .get_namespaced::<StatefulSet>("statefulsets", "/apis/apps/v1", ns, &name)
            .await?
            .map(|ss| OwnerSelector::Selector(Some(ss.spec.selector))),
    };
    Ok(DefaultSelectorInputs {
        service_selectors,
        owner,
    })
}
