//! InterPodAffinity PreFilter state and Filter.
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/interpodaffinity/filtering.go`
//! (`preFilterState` :48-95, `getExistingAntiAffinityCounts` :203-228,
//! `getIncomingAffinityAntiAffinityCounts` :234-274, `PreFilter` :277-310,
//! `satisfyExistingPodsAntiAffinity` :357-372, `satisfyPodAntiAffinity`
//! :375-387, `satisfyPodAffinity` :390-420, `Filter` :424-432) and
//! `plugin.go` (`mergeAffinityTermNamespacesIfNotEmpty` :144-157).
//!
//! Deviation: `PreFilterExtensions` (`AddPod`/`RemovePod`, :312-342) are not
//! ported, so nominated pods and preemption what-ifs do not update the
//! counts (the legacy per-node helpers did not count them either).

use std::collections::HashMap;

use rusternetes_common::resources::{Namespace, Node, Pod};

use crate::inter_pod_affinity_score::{pod_namespace, AffinityTerm, PodInfo};
use crate::pod_topology_spread_score::pods_by_node;

/// `ErrReasonExistingAntiAffinityRulesNotMatch` (filtering.go:36).
pub const ERR_REASON_EXISTING_ANTI_AFFINITY_RULES_NOT_MATCH: &str =
    "node(s) didn't satisfy existing pods anti-affinity rules";
/// `ErrReasonAffinityRulesNotMatch` (filtering.go:38).
pub const ERR_REASON_AFFINITY_RULES_NOT_MATCH: &str = "node(s) didn't match pod affinity rules";
/// `ErrReasonAntiAffinityRulesNotMatch` (filtering.go:40).
pub const ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH: &str =
    "node(s) didn't match pod anti-affinity rules";

/// `topologyToMatchedTermCount`: (topology key, value) -> number of matches.
type Counts = HashMap<(String, String), i64>;

/// `preFilterState` (filtering.go:48-60).
#[derive(Debug)]
pub struct PreFilterState {
    existing_anti_affinity_counts: Counts,
    affinity_counts: Counts,
    anti_affinity_counts: Counts,
    required_affinity_terms: Vec<AffinityTerm>,
    required_anti_affinity_terms: Vec<AffinityTerm>,
    /// The incoming pod, for the self-affinity exception (`state.podInfo.GetPod()`).
    pod: Pod,
}

/// `PreFilter`'s outcome: `Skip` (`Ok(None)`), or the status of a failure.
pub enum PreFilterError {
    /// `UnschedulableAndUnresolvable`: "parsing pod: ...".
    Unresolvable(String),
    Error(String),
}

fn add(m: &mut Counts, node: &Node, topology_key: &str) {
    if let Some(tv) = node
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(topology_key))
    {
        *m.entry((topology_key.to_string(), tv.clone())).or_default() += 1;
    }
}

/// `podMatchesAllAffinityTerms` (filtering.go:182-196).
fn pod_matches_all_affinity_terms(terms: &[AffinityTerm], pod: &Pod) -> bool {
    if terms.is_empty() {
        return false;
    }
    // The incoming pod's NamespaceSelector was merged into the Namespaces
    // set, so no namespace labels are passed.
    terms.iter().all(|t| t.matches(pod, None))
}

/// Whether any required (anti-)affinity term of `pod` carries a
/// `namespaceSelector`, i.e. whether the namespace list can influence
/// PreFilter. Upstream reads the informer cache; here the list is a data-plane
/// read, so it is only made when it matters.
fn has_required_namespace_selector(pod: &Pod) -> bool {
    let Some(a) = pod.spec.as_ref().and_then(|s| s.affinity.as_ref()) else {
        return false;
    };
    let aff = a.pod_affinity.iter().flat_map(|x| {
        x.required_during_scheduling_ignored_during_execution
            .iter()
            .flatten()
    });
    let anti = a.pod_anti_affinity.iter().flat_map(|x| {
        x.required_during_scheduling_ignored_during_execution
            .iter()
            .flatten()
    });
    aff.chain(anti).any(|t| t.namespace_selector.is_some())
}

fn has_required_anti_affinity(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.affinity.as_ref())
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .is_some_and(|t| !t.is_empty())
}

/// Whether PreFilter has to read the namespace list: a term of the incoming
/// pod, or of an existing pod with required anti-affinity (which is matched
/// against the incoming pod's namespace labels), selects namespaces by label.
pub fn needs_namespaces(pod: &Pod, all_pods: &[Pod]) -> bool {
    has_required_namespace_selector(pod)
        || all_pods
            .iter()
            .any(|p| has_required_anti_affinity(p) && has_required_namespace_selector(p))
}

/// `InterPodAffinity.PreFilter` (filtering.go:277-310). `Ok(None)` is the
/// `Skip` status. `all_nodes` and `all_pods` are the snapshot, `namespaces`
/// the namespace lister.
pub fn pre_filter(
    pod: &Pod,
    all_nodes: &[Node],
    all_pods: &[Pod],
    namespaces: &[Namespace],
) -> Result<Option<PreFilterState>, PreFilterError> {
    let mut info = PodInfo::new(pod);
    if let Some(e) = info.errors.first() {
        return Err(PreFilterError::Unresolvable(format!("parsing pod: {e}")));
    }
    for t in info
        .required_affinity_terms
        .iter_mut()
        .chain(info.required_anti_affinity_terms.iter_mut())
    {
        t.merge_namespaces_if_not_empty(namespaces);
    }
    // `GetNamespaceLabelsSnapshot`: a missing namespace is an empty label set.
    let ns_labels: HashMap<String, String> = namespaces
        .iter()
        .find(|n| n.metadata.name == pod_namespace(pod))
        .and_then(|n| n.metadata.labels.clone())
        .unwrap_or_default();

    let by_node = pods_by_node(all_pods);
    let existing_anti_affinity_counts =
        existing_anti_affinity_counts(pod, &ns_labels, all_nodes, &by_node);
    let (affinity_counts, anti_affinity_counts) = incoming_counts(&info, all_nodes, &by_node);

    if existing_anti_affinity_counts.is_empty()
        && info.required_affinity_terms.is_empty()
        && info.required_anti_affinity_terms.is_empty()
    {
        return Ok(None);
    }
    Ok(Some(PreFilterState {
        existing_anti_affinity_counts,
        affinity_counts,
        anti_affinity_counts,
        required_affinity_terms: info.required_affinity_terms,
        required_anti_affinity_terms: info.required_anti_affinity_terms,
        pod: pod.clone(),
    }))
}

/// `getExistingAntiAffinityCounts` (filtering.go:203-228): for each existing
/// pod with required anti-affinity, the topology pairs of the terms that
/// match the incoming pod.
fn existing_anti_affinity_counts(
    pod: &Pod,
    ns_labels: &HashMap<String, String>,
    all_nodes: &[Node],
    by_node: &HashMap<&str, Vec<&Pod>>,
) -> Counts {
    let mut counts = Counts::new();
    for node in all_nodes {
        let Some(pods) = by_node.get(node.metadata.name.as_str()) else {
            continue;
        };
        for existing in pods.iter().filter(|p| has_required_anti_affinity(p)) {
            for t in &PodInfo::new(existing).required_anti_affinity_terms {
                if t.matches(pod, Some(ns_labels)) {
                    add(&mut counts, node, &t.topology_key);
                }
            }
        }
    }
    counts
}

/// `getIncomingAffinityAntiAffinityCounts` (filtering.go:234-274).
fn incoming_counts(
    info: &PodInfo,
    all_nodes: &[Node],
    by_node: &HashMap<&str, Vec<&Pod>>,
) -> (Counts, Counts) {
    let mut affinity = Counts::new();
    let mut anti_affinity = Counts::new();
    if info.required_affinity_terms.is_empty() && info.required_anti_affinity_terms.is_empty() {
        return (affinity, anti_affinity);
    }
    for node in all_nodes {
        let Some(pods) = by_node.get(node.metadata.name.as_str()) else {
            continue;
        };
        for existing in pods {
            if pod_matches_all_affinity_terms(&info.required_affinity_terms, existing) {
                for t in &info.required_affinity_terms {
                    add(&mut affinity, node, &t.topology_key);
                }
            }
            // The incoming pod's terms have the namespaceSelector merged into
            // the namespaces, so the existing pod's namespace labels are not
            // looked up.
            for t in &info.required_anti_affinity_terms {
                if t.matches(existing, None) {
                    add(&mut anti_affinity, node, &t.topology_key);
                }
            }
        }
    }
    (affinity, anti_affinity)
}

/// `satisfyExistingPodsAntiAffinity` (filtering.go:357-372).
fn satisfy_existing_pods_anti_affinity(state: &PreFilterState, node: &Node) -> bool {
    if !state.existing_anti_affinity_counts.is_empty() {
        for (k, v) in node.metadata.labels.iter().flatten() {
            if state
                .existing_anti_affinity_counts
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0)
                > 0
            {
                return false;
            }
        }
    }
    true
}

/// `satisfyPodAntiAffinity` (filtering.go:375-387).
fn satisfy_pod_anti_affinity(state: &PreFilterState, node: &Node) -> bool {
    if !state.anti_affinity_counts.is_empty() {
        for term in &state.required_anti_affinity_terms {
            if let Some(tv) = node
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(&term.topology_key))
            {
                if state
                    .anti_affinity_counts
                    .get(&(term.topology_key.clone(), tv.clone()))
                    .copied()
                    .unwrap_or(0)
                    > 0
                {
                    return false;
                }
            }
        }
    }
    true
}

/// `satisfyPodAffinity` (filtering.go:390-420).
fn satisfy_pod_affinity(state: &PreFilterState, node: &Node) -> bool {
    let mut pods_exist = true;
    for term in &state.required_affinity_terms {
        match node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&term.topology_key))
        {
            Some(tv) => {
                if state
                    .affinity_counts
                    .get(&(term.topology_key.clone(), tv.clone()))
                    .copied()
                    .unwrap_or(0)
                    <= 0
                {
                    pods_exist = false;
                }
            }
            // All topology labels must exist on the node.
            None => return false,
        }
    }
    if !pods_exist {
        // This pod may be the first pod in a series that have affinity to
        // themselves. To not leave such pods pending forever, allow the pod
        // when no other pod in the cluster matches its terms, it matches its
        // own terms, and the node has all the requested topologies.
        return state.affinity_counts.is_empty()
            && pod_matches_all_affinity_terms(&state.required_affinity_terms, &state.pod);
    }
    true
}

/// The reason a node fails `Filter` (filtering.go:424-432), with whether it is
/// `UnschedulableAndUnresolvable`.
pub fn filter(state: &PreFilterState, node: &Node) -> Option<(&'static str, bool)> {
    if !satisfy_pod_affinity(state, node) {
        return Some((ERR_REASON_AFFINITY_RULES_NOT_MATCH, true));
    }
    if !satisfy_pod_anti_affinity(state, node) {
        return Some((ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH, false));
    }
    if !satisfy_existing_pods_anti_affinity(state, node) {
        return Some((ERR_REASON_EXISTING_ANTI_AFFINITY_RULES_NOT_MATCH, false));
    }
    None
}
