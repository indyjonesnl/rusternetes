//! InterPodAffinity and PodTopologySpread Filter plugin objects: the codes
//! and reasons of `interpodaffinity/filtering.go:410-432` and
//! `podtopologyspread/filtering.go:309-358`.
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo, Status};
use rusternetes_scheduler::in_tree::{
    InterPodAffinity, PodTopologySpread, ERR_REASON_AFFINITY_RULES_NOT_MATCH,
    ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH, ERR_REASON_CONSTRAINTS_NOT_MATCH,
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

async fn run(
    plugin: &dyn FilterPlugin,
    p: &Pod,
    on: &Node,
    nodes: &[Node],
    pods: &[Pod],
) -> Status {
    plugin
        .filter(
            &CycleState::new(),
            p,
            &NodeInfo::new(on, pods).with_all_nodes(nodes),
        )
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
    let s = run(
        &InterPodAffinity,
        &p,
        &nodes[1],
        &nodes,
        std::slice::from_ref(&existing),
    )
    .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_AFFINITY_RULES_NOT_MATCH]);
    let s = run(
        &InterPodAffinity,
        &p,
        &nodes[0],
        &nodes,
        std::slice::from_ref(&existing),
    )
    .await;
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
    let s = run(
        &InterPodAffinity,
        &p,
        &nodes[0],
        &nodes,
        std::slice::from_ref(&existing),
    )
    .await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec![ERR_REASON_ANTI_AFFINITY_RULES_NOT_MATCH]);
    let s = run(
        &InterPodAffinity,
        &p,
        &nodes[1],
        &nodes,
        std::slice::from_ref(&existing),
    )
    .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn pod_without_affinity_passes() {
    let nodes = vec![node("a", "z1")];
    let p = pod("p", "", json!({}));
    assert!(run(&InterPodAffinity, &p, &nodes[0], &nodes, &[])
        .await
        .is_success());
    assert!(run(&PodTopologySpread, &p, &nodes[0], &nodes, &[])
        .await
        .is_success());
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
    let s = run(&PodTopologySpread, &p, &nodes[0], &nodes, &existing).await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec![ERR_REASON_CONSTRAINTS_NOT_MATCH]);
    let s = run(&PodTopologySpread, &p, &nodes[1], &nodes, &existing).await;
    assert!(s.is_success());
}
