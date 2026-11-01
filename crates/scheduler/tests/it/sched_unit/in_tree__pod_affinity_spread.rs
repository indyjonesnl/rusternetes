//! InterPodAffinity and PodTopologySpread Filter plugin objects: the codes
//! and reasons of `interpodaffinity/filtering.go:410-432` and
//! `podtopologyspread/filtering.go:309-358`.
use super::listers::EmptyListers;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, NodeInfo, PreFilterPlugin, Status,
};
use rusternetes_scheduler::in_tree::{
    InterPodAffinity, PodTopologySpreadFilter, ERR_REASON_AFFINITY_RULES_NOT_MATCH,
    ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH, ERR_REASON_CONSTRAINTS_NOT_MATCH,
    ERR_REASON_EXISTING_ANTI_AFFINITY_RULES_NOT_MATCH, ERR_REASON_NODE_LABEL_NOT_MATCH,
};
use serde_json::{json, Value};

fn node(name: &str, zone: &str) -> Node {
    serde_json::from_value(json!({"metadata": {"name": name, "labels": {"zone": zone}}})).unwrap()
}

fn pod(name: &str, node_name: &str, spec: Value) -> Pod {
    let mut spec = spec;
    spec["nodeName"] = json!(node_name);
    spec["containers"] = json!([{"name": "c"}]);
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "web"}},
        "spec": spec
    }))
    .unwrap()
}

/// PreFilter then Filter, as the framework runs them: a `Skip` skips Filter.
async fn run_ipa(p: &Pod, on: &Node, nodes: &[Node], pods: &[Pod]) -> Status {
    let mut state = CycleState::new();
    let (_, s) = InterPodAffinity
        .pre_filter(&mut state, p, nodes, pods, &EmptyListers)
        .await;
    if !s.is_success() {
        return if s.is_skip() { Status::success() } else { s };
    }
    InterPodAffinity
        .filter(&state, p, &NodeInfo::new(on, pods).with_all_nodes(nodes))
        .await
}

/// PreFilter then Filter, as the framework runs them: a `Skip` skips Filter.
async fn run_pts(p: &Pod, on: &Node, nodes: &[Node], pods: &[Pod]) -> Status {
    let plugin = PodTopologySpreadFilter::default();
    let mut state = CycleState::new();
    let (_, s) = plugin
        .pre_filter(&mut state, p, nodes, pods, &EmptyListers)
        .await;
    if !s.is_success() {
        return if s.is_skip() { Status::success() } else { s };
    }
    plugin
        .filter(&state, p, &NodeInfo::new(on, pods).with_all_nodes(nodes))
        .await
}

fn term() -> Value {
    json!([{"labelSelector": {"matchLabels": {"app": "web"}}, "topologyKey": "zone"}])
}

#[tokio::test]
async fn required_affinity_unmet_is_unresolvable() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = pod("e", "a", json!({}));
    let p = pod(
        "p",
        "",
        json!({"affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term()}}}),
    );
    let s = run_ipa(&p, &nodes[1], &nodes, std::slice::from_ref(&existing)).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_AFFINITY_RULES_NOT_MATCH]);
    let s = run_ipa(&p, &nodes[0], &nodes, std::slice::from_ref(&existing)).await;
    assert!(s.is_success());
}

#[tokio::test]
async fn required_anti_affinity_violated_is_unschedulable() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = pod("e", "a", json!({}));
    let p = pod(
        "p",
        "",
        json!({"affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term()}}}),
    );
    let s = run_ipa(&p, &nodes[0], &nodes, std::slice::from_ref(&existing)).await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec![ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH]);
    let s = run_ipa(&p, &nodes[1], &nodes, std::slice::from_ref(&existing)).await;
    assert!(s.is_success());
}

#[tokio::test]
async fn pod_without_affinity_passes() {
    let nodes = vec![node("a", "z1")];
    let p = pod("p", "", json!({}));
    assert!(run_ipa(&p, &nodes[0], &nodes, &[]).await.is_success());
    assert!(run_pts(&p, &nodes[0], &nodes, &[]).await.is_success());
}

#[tokio::test]
async fn topology_spread_skew_exceeded_is_unschedulable() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![pod("e1", "a", json!({})), pod("e2", "a", json!({}))];
    let p = pod(
        "p",
        "",
        json!({"topologySpreadConstraints": [{
            "maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
            "labelSelector": {"matchLabels": {"app": "web"}}
        }]}),
    );
    let s = run_pts(&p, &nodes[0], &nodes, &existing).await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec![ERR_REASON_CONSTRAINTS_NOT_MATCH]);
    let s = run_pts(&p, &nodes[1], &nodes, &existing).await;
    assert!(s.is_success());
}

/// `satisfyExistingPodsAntiAffinity`: an existing pod's required
/// anti-affinity that matches the incoming pod keeps it out of the topology
/// (TestSatisfiesExistingPodsAntiAffinity).
#[tokio::test]
async fn existing_pods_anti_affinity_is_unschedulable() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = pod(
        "e",
        "a",
        json!({"affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term()}}}),
    );
    let p = pod("p", "", json!({}));
    let s = run_ipa(&p, &nodes[0], &nodes, std::slice::from_ref(&existing)).await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(
        s.reasons(),
        vec![ERR_REASON_EXISTING_ANTI_AFFINITY_RULES_NOT_MATCH]
    );
    let s = run_ipa(&p, &nodes[1], &nodes, std::slice::from_ref(&existing)).await;
    assert!(s.is_success());
}

/// The first pod of a series with affinity to itself is allowed
/// (`satisfyPodAffinity` :408-417), but only on a node with the topology key.
#[tokio::test]
async fn self_affinity_lets_the_first_pod_in() {
    let mut keyless = node("c", "x");
    keyless.metadata.labels = Some(Default::default());
    let nodes = vec![node("a", "z1"), keyless];
    let p = pod(
        "p",
        "",
        json!({"affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term()}}}),
    );
    assert!(run_ipa(&p, &nodes[0], &nodes, &[]).await.is_success());
    let s = run_ipa(&p, &nodes[1], &nodes, &[]).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

/// A pod whose affinity selector cannot be parsed is
/// UnschedulableAndUnresolvable at PreFilter ("parsing pod: ...").
#[tokio::test]
async fn unparsable_selector_fails_pre_filter() {
    let nodes = vec![node("a", "z1")];
    let p = pod(
        "p",
        "",
        json!({"affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
            "labelSelector": {"matchExpressions": [{"key": "app", "operator": "Bogus"}]},
            "topologyKey": "zone"
        }]}}}),
    );
    let (_, s) = InterPodAffinity
        .pre_filter(&mut CycleState::new(), &p, &nodes, &[], &EmptyListers)
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert!(s.message().starts_with("parsing pod:"), "{}", s.message());
}

fn hard(max_skew: i32, extra: Value) -> Value {
    let mut c = json!({
        "maxSkew": max_skew, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
        "labelSelector": {"matchLabels": {"app": "web"}}
    });
    for (k, v) in extra.as_object().into_iter().flatten() {
        c[k] = v.clone();
    }
    json!({"topologySpreadConstraints": [c]})
}

/// `Filter`: a node without the constraint's topology label is
/// UnschedulableAndUnresolvable (ErrReasonNodeLabelNotMatch).
#[tokio::test]
async fn node_without_the_topology_label_is_unresolvable() {
    let mut keyless = node("c", "x");
    keyless.metadata.labels = Some(Default::default());
    let nodes = vec![node("a", "z1"), keyless];
    let p = pod("p", "", hard(1, json!({})));
    let s = run_pts(&p, &nodes[1], &nodes, &[]).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_NODE_LABEL_NOT_MATCH]);
}

/// `minMatchNum`: with fewer eligible domains than `minDomains` the global
/// minimum is 0 (TestMinDomains).
#[tokio::test]
async fn min_domains_treats_the_global_minimum_as_zero() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![pod("e1", "a", json!({})), pod("e2", "b", json!({}))];
    // Two domains with one pod each: skew 1 + 1 - 1 = 1 passes.
    let p = pod("p", "", hard(1, json!({})));
    assert!(run_pts(&p, &nodes[0], &nodes, &existing).await.is_success());
    // minDomains 3 > 2 domains: minimum is 0, skew 1 + 1 - 0 = 2 > 1.
    let p = pod("p", "", hard(1, json!({"minDomains": 3})));
    let s = run_pts(&p, &nodes[0], &nodes, &existing).await;
    assert_eq!(s.code(), Code::Unschedulable);
}

/// `calPreFilterState`: nodes the pod's required node affinity excludes are
/// not domains (NodeAffinityPolicy Honor, the default).
#[tokio::test]
async fn nodes_excluded_by_node_affinity_do_not_count_as_domains() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![pod("e1", "a", json!({})), pod("e2", "a", json!({}))];
    let mut spec = hard(1, json!({}));
    spec["nodeSelector"] = json!({"zone": "z1"});
    let p = pod("p", "", spec);
    // Only z1 is an eligible domain, so the minimum is z1's own count and
    // z1 passes: 2 + 1 - 2 = 1.
    assert!(run_pts(&p, &nodes[0], &nodes, &existing).await.is_success());
}

/// A pod without constraints gets the hard default constraints, none by
/// default: PreFilter is Skip.
#[tokio::test]
async fn pods_without_hard_constraints_are_skipped() {
    let nodes = vec![node("a", "z1")];
    let (_, s) = PodTopologySpreadFilter::default()
        .pre_filter(
            &mut CycleState::new(),
            &pod("p", "", json!({})),
            &nodes,
            &[],
            &EmptyListers,
        )
        .await;
    assert!(s.is_skip());
}
