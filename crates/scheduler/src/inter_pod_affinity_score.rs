//! The InterPodAffinity plugin's PreScore / Score / NormalizeScore.
//!
//! Ported from `pkg/scheduler/framework/plugins/interpodaffinity/scoring.go`
//! (`PreScore`, `processExistingPod`, `Score`, `NormalizeScore`), with the
//! term processing of `plugin.go` (`mergeAffinityTermNamespacesIfNotEmpty`,
//! `GetNamespaceLabelsSnapshot`) and of
//! `staging/src/k8s.io/kube-scheduler/framework/types.go`
//! (`AffinityTerm`, `newAffinityTerm`, `getNamespacesFromPodAffinityTerm`).
//! Normalization is [`crate::score::normalize_inter_pod_affinity`].
//!
//! Tests are ported from `interpodaffinity/scoring_test.go`
//! (`TestPreferredAffinity`, `TestPreferredAffinityWithHardPodAffinitySymmetricWeight`).
//!
//! Deviation: `PreScore` fans the nodes out over `parallelizer.Until`
//! upstream; here the pods are visited sequentially (the sum is
//! order-independent). The snapshot is `nodes` plus the non-terminal pods of
//! `all_pods` bound to one of them (the scheduler's pod informer drops
//! Succeeded/Failed pods, `scheduler.go` `assignedPod` + the field selector).

use std::collections::{HashMap, HashSet};

use rusternetes_common::resources::pod::{PodAffinityTerm, WeightedPodAffinityTerm};
use rusternetes_common::resources::{Namespace, Node, Pod};
use rusternetes_common::types::{label_selector_as_selector, Phase, Selector};

/// `config.InterPodAffinityArgs` as the plugin consumes it (`pl.args`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Args {
    /// `HardPodAffinityWeight` (default 1, `v1.DefaultHardPodAffinitySymmetricWeight`).
    pub hard_pod_affinity_weight: i32,
    /// `IgnorePreferredTermsOfExistingPods`.
    pub ignore_preferred_terms_of_existing_pods: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            hard_pod_affinity_weight: 1,
            ignore_preferred_terms_of_existing_pods: false,
        }
    }
}

/// `fwk.AffinityTerm`.
#[derive(Clone, Debug)]
struct AffinityTerm {
    namespaces: HashSet<String>,
    selector: Selector,
    topology_key: String,
    namespace_selector: Selector,
}

/// `fwk.WeightedAffinityTerm`.
#[derive(Clone, Debug)]
struct WeightedAffinityTerm {
    term: AffinityTerm,
    weight: i32,
}

fn pod_namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("default")
}

impl AffinityTerm {
    /// `newAffinityTerm` + `getNamespacesFromPodAffinityTerm`.
    fn new(pod: &Pod, term: &PodAffinityTerm) -> Result<Self, String> {
        let selector = label_selector_as_selector(term.label_selector.as_ref())?;
        let mut namespaces = HashSet::new();
        let term_namespaces = term.namespaces.as_deref().unwrap_or(&[]);
        if term_namespaces.is_empty() && term.namespace_selector.is_none() {
            namespaces.insert(pod_namespace(pod).to_string());
        } else {
            namespaces.extend(term_namespaces.iter().cloned());
        }
        let namespace_selector = label_selector_as_selector(term.namespace_selector.as_ref())?;
        Ok(Self {
            namespaces,
            selector,
            topology_key: term.topology_key.clone(),
            namespace_selector,
        })
    }

    /// `AffinityTerm.Matches`.
    fn matches(&self, pod: &Pod, ns_labels: Option<&HashMap<String, String>>) -> bool {
        if self.namespaces.contains(pod_namespace(pod))
            || self.namespace_selector.matches(ns_labels)
        {
            return self.selector.matches(pod.metadata.labels.as_ref());
        }
        false
    }

    /// `mergeAffinityTermNamespacesIfNotEmpty` (plugin.go:144-157): the
    /// namespaces selected by a non-empty `NamespaceSelector` are unrolled
    /// into `Namespaces` and the selector becomes `Nothing()`.
    fn merge_namespaces_if_not_empty(&mut self, namespaces: &[Namespace]) {
        // `labels.Selector.Empty()`: only `Everything()` is empty; `Nothing()`
        // reports false but selects no namespace, so it merges nothing.
        if self.namespace_selector.is_everything() {
            return;
        }
        for ns in namespaces {
            if self.namespace_selector.matches(ns.metadata.labels.as_ref()) {
                self.namespaces.insert(ns.metadata.name.clone());
            }
        }
        self.namespace_selector = Selector::Nothing;
    }
}

/// The parsed inter-pod affinity terms of a pod (`framework.PodInfo.Update`).
/// A category whose terms fail to parse is left empty, as upstream leaves the
/// `PodInfo` field nil and returns the aggregate error.
#[derive(Default)]
struct PodInfo {
    required_affinity_terms: Vec<AffinityTerm>,
    preferred_affinity_terms: Vec<WeightedAffinityTerm>,
    preferred_anti_affinity_terms: Vec<WeightedAffinityTerm>,
    errors: Vec<String>,
}

fn weighted_terms(
    pod: &Pod,
    v1_terms: &[WeightedPodAffinityTerm],
) -> Result<Vec<WeightedAffinityTerm>, String> {
    v1_terms
        .iter()
        .map(|t| {
            Ok(WeightedAffinityTerm {
                term: AffinityTerm::new(pod, &t.pod_affinity_term)?,
                weight: t.weight,
            })
        })
        .collect()
}

impl PodInfo {
    fn new(pod: &Pod) -> Self {
        let mut info = PodInfo::default();
        let Some(affinity) = pod.spec.as_ref().and_then(|s| s.affinity.as_ref()) else {
            return info;
        };
        if let Some(a) = &affinity.pod_affinity {
            // GetPodAffinityTerms: the required terms.
            let required = a
                .required_during_scheduling_ignored_during_execution
                .as_deref()
                .unwrap_or(&[]);
            match required
                .iter()
                .map(|t| AffinityTerm::new(pod, t))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(t) => info.required_affinity_terms = t,
                Err(e) => info.errors.push(format!("requiredAffinityTerms: {e}")),
            }
            let preferred = a
                .preferred_during_scheduling_ignored_during_execution
                .as_deref()
                .unwrap_or(&[]);
            match weighted_terms(pod, preferred) {
                Ok(t) => info.preferred_affinity_terms = t,
                Err(e) => info.errors.push(format!("preferredAffinityTerms: {e}")),
            }
        }
        if let Some(a) = &affinity.pod_anti_affinity {
            let preferred = a
                .preferred_during_scheduling_ignored_during_execution
                .as_deref()
                .unwrap_or(&[]);
            match weighted_terms(pod, preferred) {
                Ok(t) => info.preferred_anti_affinity_terms = t,
                Err(e) => info.errors.push(format!("preferredAntiAffinityTerms: {e}")),
            }
            // The required anti-affinity terms are parsed upstream
            // (`requiredAntiAffinityTerms`) so that a bad selector fails
            // PreScore; they are not otherwise read here.
            let required = a
                .required_during_scheduling_ignored_during_execution
                .as_deref()
                .unwrap_or(&[]);
            if let Err(e) = required
                .iter()
                .map(|t| AffinityTerm::new(pod, t))
                .collect::<Result<Vec<_>, _>>()
            {
                info.errors.push(format!("requiredAntiAffinityTerms: {e}"));
            }
        }
        info
    }
}

/// `scoreMap`: topologyKey -> topologyValue -> score.
type ScoreMap = HashMap<String, HashMap<String, i64>>;

/// `preScoreState` (scoring.go:36-43). Only the `topologyScore` is read by
/// `Score`/`NormalizeScore`.
#[derive(Debug, Default)]
pub struct PreScoreState {
    topology_score: ScoreMap,
}

/// `scoreMap.processTerm`.
fn process_term(
    m: &mut ScoreMap,
    term: &AffinityTerm,
    weight: i32,
    pod: &Pod,
    ns_labels: Option<&HashMap<String, String>>,
    node: &Node,
    multiplier: i32,
) {
    if term.matches(pod, ns_labels) {
        if let Some(tp_value) = node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&term.topology_key))
        {
            *m.entry(term.topology_key.clone())
                .or_default()
                .entry(tp_value.clone())
                .or_insert(0) += i64::from(weight.wrapping_mul(multiplier));
        }
    }
}

/// `scoreMap.processTerms`.
fn process_terms(
    m: &mut ScoreMap,
    terms: &[WeightedAffinityTerm],
    pod: &Pod,
    ns_labels: Option<&HashMap<String, String>>,
    node: &Node,
    multiplier: i32,
) {
    for t in terms {
        process_term(m, &t.term, t.weight, pod, ns_labels, node, multiplier);
    }
}

/// `processExistingPod` (scoring.go:90-132).
#[allow(clippy::too_many_arguments)]
fn process_existing_pod(
    args: &Args,
    incoming: &PodInfo,
    incoming_pod: &Pod,
    ns_labels: &HashMap<String, String>,
    existing_pod: &Pod,
    existing_node: &Node,
    topo_score: &mut ScoreMap,
) {
    if existing_node
        .metadata
        .labels
        .as_ref()
        .is_none_or(|l| l.is_empty())
    {
        return;
    }

    // Soft affinity / anti-affinity terms of the incoming pod against the
    // existing pod. The incoming pod's terms have the namespaceSelector merged
    // into the namespaces, so no namespace labels are looked up (nil).
    process_terms(
        topo_score,
        &incoming.preferred_affinity_terms,
        existing_pod,
        None,
        existing_node,
        1,
    );
    process_terms(
        topo_score,
        &incoming.preferred_anti_affinity_terms,
        existing_pod,
        None,
        existing_node,
        -1,
    );

    let existing = PodInfo::new(existing_pod);

    // Hard affinity terms of the existing pod against the incoming pod, by the
    // constant hardPodAffinityWeight.
    if args.hard_pod_affinity_weight > 0 {
        for t in &existing.required_affinity_terms {
            process_term(
                topo_score,
                t,
                args.hard_pod_affinity_weight,
                incoming_pod,
                Some(ns_labels),
                existing_node,
                1,
            );
        }
    }

    // Soft affinity / anti-affinity terms of the existing pod against the
    // incoming pod.
    process_terms(
        topo_score,
        &existing.preferred_affinity_terms,
        incoming_pod,
        Some(ns_labels),
        existing_node,
        1,
    );
    process_terms(
        topo_score,
        &existing.preferred_anti_affinity_terms,
        incoming_pod,
        Some(ns_labels),
        existing_node,
        -1,
    );
}

fn has_pod_affinity(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.affinity.as_ref())
        .is_some_and(|a| a.pod_affinity.is_some() || a.pod_anti_affinity.is_some())
}

/// Whether any term `PreScore` evaluates carries a `namespaceSelector`, i.e.
/// whether the namespace list can influence the result. The data plane only
/// lists namespaces when this holds (upstream reads the informer cache).
pub fn needs_namespaces(pod: &Pod, all_pods: &[Pod]) -> bool {
    let has_ns_selector = |p: &Pod| {
        let Some(a) = p.spec.as_ref().and_then(|s| s.affinity.as_ref()) else {
            return false;
        };
        let pa = a.pod_affinity.iter().flat_map(|x| {
            x.required_during_scheduling_ignored_during_execution
                .iter()
                .flatten()
                .map(|t| t.namespace_selector.is_some())
                .chain(
                    x.preferred_during_scheduling_ignored_during_execution
                        .iter()
                        .flatten()
                        .map(|t| t.pod_affinity_term.namespace_selector.is_some()),
                )
        });
        let paa = a.pod_anti_affinity.iter().flat_map(|x| {
            x.preferred_during_scheduling_ignored_during_execution
                .iter()
                .flatten()
                .map(|t| t.pod_affinity_term.namespace_selector.is_some())
        });
        pa.chain(paa).any(|b| b)
    };
    has_pod_affinity(pod) && has_ns_selector(pod)
        || all_pods
            .iter()
            .any(|p| has_pod_affinity(p) && has_ns_selector(p))
}

/// `InterPodAffinity.PreScore` (scoring.go:135-244). `Ok(None)` is the `Skip`
/// status (the plugin is left out of `RunScorePlugins`); `Err` is an `Error`
/// status.
pub fn pre_score(
    pod: &Pod,
    nodes: &[Node],
    all_pods: &[Pod],
    namespaces: &[Namespace],
    args: &Args,
) -> Result<Option<PreScoreState>, String> {
    let affinity = pod.spec.as_ref().and_then(|s| s.affinity.as_ref());
    let has_preferred_affinity = affinity
        .and_then(|a| a.pod_affinity.as_ref())
        .and_then(|a| {
            a.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .is_some_and(|t| !t.is_empty());
    let has_preferred_anti_affinity = affinity
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| {
            a.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .is_some_and(|t| !t.is_empty());
    let has_constraints = has_preferred_affinity || has_preferred_anti_affinity;

    // Optionally ignore the preferences of existing pods' affinity rules if the
    // incoming pod has no inter-pod affinities.
    if args.ignore_preferred_terms_of_existing_pods && !has_constraints {
        return Ok(None);
    }

    let mut incoming = PodInfo::new(pod);
    if let Some(e) = incoming.errors.first() {
        return Err(format!("failed to parse pod: {e}"));
    }
    for t in &mut incoming.preferred_affinity_terms {
        t.term.merge_namespaces_if_not_empty(namespaces);
    }
    for t in &mut incoming.preferred_anti_affinity_terms {
        t.term.merge_namespaces_if_not_empty(namespaces);
    }
    // `GetNamespaceLabelsSnapshot`: a missing namespace is an empty label set.
    let ns_labels: HashMap<String, String> = namespaces
        .iter()
        .find(|n| n.metadata.name == pod_namespace(pod))
        .and_then(|n| n.metadata.labels.clone())
        .unwrap_or_default();

    let node_by_name: HashMap<&str, &Node> = nodes
        .iter()
        .map(|n| (n.metadata.name.as_str(), n))
        .collect();

    let mut topo_score = ScoreMap::new();
    for existing in all_pods {
        let Some(node_name) = existing.spec.as_ref().and_then(|s| s.node_name.as_deref()) else {
            continue;
        };
        let Some(node) = node_by_name.get(node_name) else {
            continue;
        };
        if matches!(
            existing.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Succeeded) | Some(Phase::Failed)
        ) {
            continue;
        }
        // Unless the pod being scheduled has preferred affinity terms, only
        // the pods with affinity need processing (`GetPodsWithAffinity`).
        if !has_constraints && !has_pod_affinity(existing) {
            continue;
        }
        process_existing_pod(
            args,
            &incoming,
            pod,
            &ns_labels,
            existing,
            node,
            &mut topo_score,
        );
    }

    // `topoScores` keeps only the non-empty per-node maps; `index == -1` is
    // "nothing was scored anywhere".
    if topo_score.is_empty() {
        return Ok(None);
    }
    Ok(Some(PreScoreState {
        topology_score: topo_score,
    }))
}

/// `InterPodAffinity.Score` (scoring.go:259-275): the sum of the weights of
/// the node's `(topologyKey, value)` pairs; positive for affinity, negative for
/// anti-affinity. Normalized later.
pub fn score(state: &PreScoreState, node: &Node) -> i64 {
    let mut score = 0i64;
    for (tp_key, tp_values) in &state.topology_score {
        if let Some(v) = node.metadata.labels.as_ref().and_then(|l| l.get(tp_key)) {
            score += tp_values.get(v).copied().unwrap_or(0);
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::score::{normalize_inter_pod_affinity, MAX_NODE_SCORE};
    use serde_json::{json, Value};

    fn ns(name: &str, team: &str) -> Namespace {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "labels": {"team": team}}
        }))
        .unwrap()
    }

    /// scoring_test.go `namespaces`.
    fn namespaces() -> Vec<Namespace> {
        vec![
            ns("subteam1.team1", "team1"),
            ns("subteam2.team1", "team1"),
            ns("subteam1.team2", "team2"),
            ns("subteam2.team2", "team2"),
        ]
    }

    fn node(name: &str, labels: Value) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}
        }))
        .unwrap()
    }

    fn pod(node_name: &str, namespace: &str, labels: Value, affinity: Option<&Value>) -> Pod {
        let mut spec = json!({"containers": []});
        if !node_name.is_empty() {
            spec["nodeName"] = json!(node_name);
        }
        if let Some(a) = affinity {
            spec["affinity"] = a.clone();
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": namespace, "labels": labels},
            "spec": spec
        }))
        .unwrap()
    }

    fn sel(key: &str, op: &str, values: &[&str]) -> Value {
        json!({"key": key, "operator": op, "values": values})
    }

    fn weighted(weight: i32, exprs: Vec<Value>, topology_key: &str) -> Value {
        json!({
            "weight": weight,
            "podAffinityTerm": {
                "labelSelector": {"matchExpressions": exprs},
                "topologyKey": topology_key
            }
        })
    }

    fn pref_affinity(terms: Vec<Value>) -> Value {
        json!({"podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": terms}})
    }

    fn pref_anti(terms: Vec<Value>) -> Value {
        json!({"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": terms}})
    }

    fn s1() -> Value {
        json!({"security": "S1"})
    }
    fn s2() -> Value {
        json!({"security": "S2"})
    }

    struct Affinities {
        stay_with_s1_in_region: Value,
        stay_with_s2_in_region: Value,
        affinity3: Value,
        hard_affinity: Value,
        away_from_s1_in_az: Value,
        away_from_s2_in_az: Value,
        stay_with_s1_away_from_s2: Value,
        affinity_namespace_selector: Value,
        anti_affinity_namespace_selector: Value,
        invalid_affinity: Value,
        invalid_anti_affinity: Value,
    }

    fn affinities() -> Affinities {
        let ns_term = |anti: bool| {
            let t = json!({
                "weight": 5,
                "podAffinityTerm": {
                    "labelSelector": {"matchExpressions": [sel("security", "In", &["S1"])]},
                    "topologyKey": "region",
                    "namespaces": ["subteam2.team2"],
                    "namespaceSelector": {"matchExpressions": [sel("team", "In", &["team1"])]}
                }
            });
            if anti {
                pref_anti(vec![t])
            } else {
                pref_affinity(vec![t])
            }
        };
        Affinities {
            stay_with_s1_in_region: pref_affinity(vec![weighted(
                5,
                vec![sel("security", "In", &["S1"])],
                "region",
            )]),
            stay_with_s2_in_region: pref_affinity(vec![weighted(
                6,
                vec![sel("security", "In", &["S2"])],
                "region",
            )]),
            affinity3: pref_affinity(vec![
                weighted(
                    8,
                    vec![
                        sel("security", "NotIn", &["S1"]),
                        sel("security", "In", &["S2"]),
                    ],
                    "region",
                ),
                weighted(
                    2,
                    vec![
                        sel("security", "Exists", &[]),
                        sel("wrongkey", "DoesNotExist", &[]),
                    ],
                    "region",
                ),
            ]),
            hard_affinity: json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
                {"labelSelector": {"matchExpressions": [sel("security", "In", &["S1", "value2"])]},
                 "topologyKey": "region"},
                {"labelSelector": {"matchExpressions": [
                    sel("security", "Exists", &[]), sel("wrongkey", "DoesNotExist", &[])]},
                 "topologyKey": "region"},
            ]}}),
            away_from_s1_in_az: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["S1"])],
                "az",
            )]),
            away_from_s2_in_az: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["S2"])],
                "az",
            )]),
            stay_with_s1_away_from_s2: json!({
                "podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
                    weighted(8, vec![sel("security", "In", &["S1"])], "region")]},
                "podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
                    weighted(5, vec![sel("security", "In", &["S2"])], "az")]},
            }),
            affinity_namespace_selector: ns_term(false),
            anti_affinity_namespace_selector: ns_term(true),
            invalid_affinity: pref_affinity(vec![weighted(
                8,
                vec![sel("security", "In", &["{{.bad-value.}}"])],
                "region",
            )]),
            invalid_anti_affinity: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["{{.bad-value.}}"])],
                "az",
            )]),
        }
    }

    /// PreScore + Score for every node + NormalizeScore, as the test harness
    /// does. `None` is the Skip status.
    fn run(
        incoming: &Pod,
        pods: &[Pod],
        nodes: &[Node],
        args: Args,
    ) -> Result<Option<Vec<i64>>, String> {
        let nss = namespaces();
        let Some(state) = pre_score(incoming, nodes, pods, &nss, &args)? else {
            return Ok(None);
        };
        let mut scores: Vec<i64> = nodes.iter().map(|n| score(&state, n)).collect();
        normalize_inter_pod_affinity(&mut scores);
        Ok(Some(scores))
    }

    const MAX: i64 = MAX_NODE_SCORE;

    fn rg(v: &str) -> Value {
        json!({"region": v})
    }
    fn az(v: &str) -> Value {
        json!({"az": v})
    }

    #[test]
    fn affinity_nil_skips() {
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", az("az1")),
        ];
        let p = pod("", "default", s1(), None);
        assert_eq!(run(&p, &[], &nodes, Args::default()), Ok(None));
    }

    #[test]
    fn preferred_affinity_cases() {
        let a = affinities();
        let d = "default";
        let nodes3 = || {
            vec![
                node("node1", rg("China")),
                node("node2", rg("India")),
                node("node3", az("az1")),
            ]
        };
        let args = Args::default();

        // topology key + matching pods score high.
        let p = pod("", d, s1(), Some(&a.stay_with_s1_in_region));
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s1(), None),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, 0, 0])
        );

        // all nodes in the same domain share the score.
        let p = pod("", d, json!({}), Some(&a.stay_with_s1_in_region));
        let pods = [pod("node1", d, s1(), None)];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", json!({"region": "China", "az": "az1"})),
            node("node3", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, MAX, 0])
        );

        // more matches in a region.
        let p = pod("", d, s1(), Some(&a.stay_with_s2_in_region));
        let pods = [
            pod("node1", d, s2(), None),
            pod("node1", d, s2(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s2(), None),
            pod("node4", d, s2(), None),
            pod("node5", d, s2(), None),
        ];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", rg("China")),
            node("node4", rg("China")),
            node("node5", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, MAX, 0])
        );

        // different label operators.
        let p = pod("", d, s1(), Some(&a.affinity3));
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s1(), None),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![20, MAX, 0])
        );

        // symmetry: only the preferred terms of existing pods.
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.stay_with_s1_in_region)),
            pod("node2", d, s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![0, MAX, 0])
        );

        // symmetry with namespace selector.
        let p = pod("", "subteam1.team1", s1(), None);
        let pods = [
            pod("node1", "", s1(), Some(&a.affinity_namespace_selector)),
            pod("node2", "", s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, 0, 0])
        );

        // anti-affinity symmetry with namespace selector.
        let pods = [
            pod("node1", "", s1(), Some(&a.anti_affinity_namespace_selector)),
            pod("node2", "", s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![0, MAX, MAX])
        );

        // required terms of existing pods count by the hard weight.
        let p = pod("", d, s1(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.hard_affinity)),
            pod("node2", d, s2(), Some(&a.hard_affinity)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
    }

    #[test]
    fn preferred_anti_affinity_cases() {
        let a = affinities();
        let d = "default";
        let args = Args::default();

        let p = pod("", d, s1(), Some(&a.away_from_s1_in_az));
        let pods = [pod("node1", d, s1(), None), pod("node2", d, s2(), None)];
        let nodes = vec![node("node1", az("az1")), node("node2", rg("China"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        let pods = [pod("node1", d, s1(), None), pod("node2", d, s1(), None)];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        let pods = [
            pod("node1", d, s1(), None),
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
        ];
        let nodes = vec![node("node1", az("az1")), node("node2", rg("India"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        // symmetry.
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.away_from_s2_in_az)),
            pod("node2", d, s2(), Some(&a.away_from_s1_in_az)),
        ];
        let nodes = vec![node("node1", az("az1")), node("node2", az("az2"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));
    }

    #[test]
    fn affinity_and_anti_affinity_cases() {
        let a = affinities();
        let d = "default";
        let args = Args::default();

        let p = pod("", d, s1(), Some(&a.stay_with_s1_away_from_s2));
        let pods = [pod("node1", d, s1(), None), pod("node2", d, s1(), None)];
        let nodes = vec![node("node1", rg("China")), node("node2", az("az1"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![MAX, 0]));

        // same labels as the existing pods (one RC/service).
        let pods = [
            pod("node1", d, s1(), None),
            pod("node1", d, s1(), None),
            pod("node2", d, s1(), None),
            pod("node3", d, s1(), None),
            pod("node3", d, s1(), None),
            pod("node4", d, s1(), None),
            pod("node5", d, s1(), None),
        ];
        let nodes = vec![
            node("node1", json!({"region": "China", "az": "az1"})),
            node("node2", rg("India")),
            node("node3", rg("China")),
            node("node4", rg("China")),
            node("node5", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, MAX, 0])
        );

        // affinity, anti-affinity and both symmetries together.
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, json!({}), Some(&a.stay_with_s1_away_from_s2)),
            pod("node4", d, json!({}), Some(&a.away_from_s1_in_az)),
        ];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", az("az1")),
            node("node3", rg("India")),
            node("node4", az("az2")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, 0])
        );

        // Avoid panic when partial nodes in a topology don't have pods with
        // affinity (kubernetes#82796).
        let p = pod("", d, s1(), None);
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, json!({}), Some(&a.stay_with_s1_away_from_s2)),
        ];
        let nodes = vec![node("node1", rg("China")), node("node2", rg("China"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, 0]));
    }

    #[test]
    fn invalid_affinity_fails_pre_score() {
        let a = affinities();
        let nodes = vec![node("node1", rg("China")), node("node2", rg("China"))];
        for aff in [&a.invalid_affinity, &a.invalid_anti_affinity] {
            let p = pod("", "default", json!({}), Some(aff));
            let err = run(&p, &[], &nodes, Args::default()).unwrap_err();
            assert!(err.contains("Invalid value: \"{{.bad-value.}}\""), "{err}");
        }
    }

    #[test]
    fn namespace_selector_cases() {
        let a = affinities();
        let nodes = vec![node("node1", rg("China")), node("node2", rg("India"))];
        let args = Args::default();
        let incoming = |aff: &Value| pod("", "subteam1.team1", s1(), Some(aff));
        let pods_a = [
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team2", s1(), None),
            pod("node2", "subteam2.team1", s1(), None),
        ];
        assert_eq!(
            run(
                &incoming(&a.affinity_namespace_selector),
                &pods_a,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![MAX, 0])
        );
        // The Namespaces field is unioned with the selector's namespaces.
        let pods_b = [
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam2.team2", s1(), None),
            pod("node2", "subteam2.team1", s1(), None),
        ];
        assert_eq!(
            run(
                &incoming(&a.affinity_namespace_selector),
                &pods_b,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![MAX, 0])
        );
        assert_eq!(
            run(
                &incoming(&a.anti_affinity_namespace_selector),
                &pods_a,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![0, MAX])
        );
        assert_eq!(
            run(
                &incoming(&a.anti_affinity_namespace_selector),
                &pods_b,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![0, MAX])
        );
    }

    #[test]
    fn ignore_preferred_terms_of_existing_pods() {
        let a = affinities();
        let d = "default";
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.stay_with_s1_away_from_s2)),
            pod("node2", d, s2(), Some(&a.stay_with_s2_in_region)),
        ];
        let nodes = vec![node("node1", rg("China")), node("node2", rg("India"))];
        let ignore = Args {
            ignore_preferred_terms_of_existing_pods: true,
            ..Args::default()
        };
        assert_eq!(run(&p, &pods, &nodes, ignore), Ok(None));
        assert_eq!(
            run(&p, &pods, &nodes, Args::default()).unwrap(),
            Some(vec![0, MAX])
        );
    }

    #[test]
    fn no_nodes_to_score_skips() {
        let p = pod("", "default", s2(), None);
        assert_eq!(run(&p, &[], &[], Args::default()), Ok(None));
    }

    /// TestPreferredAffinityWithHardPodAffinitySymmetricWeight.
    #[test]
    fn hard_pod_affinity_symmetric_weight() {
        let hard = json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
            "labelSelector": {"matchExpressions": [sel("service", "In", &["S1"])]},
            "namespaces": ["", "subteam2.team2"],
            "namespaceSelector": {"matchExpressions": [sel("team", "In", &["team1"])]},
            "topologyKey": "region"
        }]}});
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", az("az1")),
        ];
        let pods = [
            pod("node1", "", json!({}), Some(&hard)),
            pod("node2", "", json!({}), Some(&hard)),
        ];
        let incoming = |ns: &str| pod("", ns, json!({"service": "S1"}), None);
        let with = |w: i32| Args {
            hard_pod_affinity_weight: w,
            ..Args::default()
        };
        // default weight, NamespaceSelector match ("subteam1.team1").
        assert_eq!(
            run(&incoming("subteam1.team1"), &pods, &nodes, with(1)).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
        // zero weight: Skip.
        assert_eq!(
            run(&incoming("subteam1.team1"), &pods, &nodes, with(0)),
            Ok(None)
        );
        // no matching namespace: Skip.
        assert_eq!(
            run(&incoming("subteam1.team2"), &pods, &nodes, with(1)),
            Ok(None)
        );
        // matching Namespaces field.
        assert_eq!(
            run(&incoming("subteam2.team2"), &pods, &nodes, with(1)).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
    }

    #[test]
    fn needs_namespaces_only_with_a_namespace_selector() {
        let a = affinities();
        let plain = pod("", "default", s1(), Some(&a.stay_with_s1_in_region));
        let with_sel = pod("n", "default", s1(), Some(&a.affinity_namespace_selector));
        assert!(!needs_namespaces(&plain, &[]));
        assert!(needs_namespaces(&with_sel, &[]));
        assert!(needs_namespaces(&plain, &[with_sel]));
    }
}
