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
pub(crate) struct AffinityTerm {
    pub(crate) namespaces: HashSet<String>,
    pub(crate) selector: Selector,
    pub(crate) topology_key: String,
    pub(crate) namespace_selector: Selector,
}

/// `fwk.WeightedAffinityTerm`.
#[derive(Clone, Debug)]
pub(crate) struct WeightedAffinityTerm {
    term: AffinityTerm,
    weight: i32,
}

pub(crate) fn pod_namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("default")
}

impl AffinityTerm {
    /// `newAffinityTerm` + `getNamespacesFromPodAffinityTerm`.
    pub(crate) fn new(pod: &Pod, term: &PodAffinityTerm) -> Result<Self, String> {
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
    pub(crate) fn matches(&self, pod: &Pod, ns_labels: Option<&HashMap<String, String>>) -> bool {
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
    pub(crate) fn merge_namespaces_if_not_empty(&mut self, namespaces: &[Namespace]) {
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
#[derive(Default, Clone)]
pub(crate) struct PodInfo {
    pub(crate) required_affinity_terms: Vec<AffinityTerm>,
    pub(crate) required_anti_affinity_terms: Vec<AffinityTerm>,
    preferred_affinity_terms: Vec<WeightedAffinityTerm>,
    preferred_anti_affinity_terms: Vec<WeightedAffinityTerm>,
    pub(crate) errors: Vec<String>,
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
    pub(crate) fn new(pod: &Pod) -> Self {
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
            let required = a
                .required_during_scheduling_ignored_during_execution
                .as_deref()
                .unwrap_or(&[]);
            match required
                .iter()
                .map(|t| AffinityTerm::new(pod, t))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(t) => info.required_anti_affinity_terms = t,
                Err(e) => info.errors.push(format!("requiredAntiAffinityTerms: {e}")),
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

pub(crate) fn has_pod_affinity(pod: &Pod) -> bool {
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
