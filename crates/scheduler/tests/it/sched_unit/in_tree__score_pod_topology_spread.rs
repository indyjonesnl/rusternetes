//! PodTopologySpread PreScore/Score/NormalizeScore through the plugin object
//! (`podtopologyspread/scoring_test.go` TestPreScoreSkip, TestPreScoreStateEmptyNodes,
//! TestPodTopologySpreadScore).
use super::listers::EmptyListers;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::CycleState;
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn node(name: &str, zone: &str) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name, "labels": {"zone": zone}},
        "status": {"allocatable": {"cpu": "4", "memory": "4Gi", "pods": "110"}}
    }))
    .unwrap()
}

fn pod(name: &str, node: &str, constraints: Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default", "labels": {"foo": ""}},
        "spec": {"nodeName": node, "containers": [{"name": "c"}],
                 "topologySpreadConstraints": constraints}
    }))
    .unwrap()
}

fn soft_zone_constraint() -> Value {
    json!([{
        "maxSkew": 1,
        "topologyKey": "zone",
        "whenUnsatisfiable": "ScheduleAnyway",
        "labelSelector": {"matchLabels": {"foo": ""}}
    }])
}

async fn scores(p: &Pod, nodes: &[Node], existing: &[Pod]) -> Option<Vec<i64>> {
    let fw = Profile::named("default-scheduler").framework;
    let refs: Vec<&Node> = nodes.iter().collect();
    let mut state = CycleState::new();
    // The default `System` constraints need Service/controller listers only
    // for pods without constraints of their own, so `NoHandle` suffices here.
    let s = fw
        .run_pre_score_plugins(&mut state, p, &refs, existing, nodes, &EmptyListers)
        .await;
    assert!(s.is_success(), "{s:?}");
    fw.run_score_plugins(&state, p, &refs, existing, nodes)
        .unwrap()
        .into_iter()
        .find(|s| s.name == "PodTopologySpread")
        .map(|s| s.scores)
}

#[tokio::test]
async fn zone_with_fewer_matching_pods_scores_higher() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![
        pod("e1", "a", json!([])),
        pod("e2", "a", json!([])),
        pod("e3", "b", json!([])),
    ];
    let p = pod("p", "", soft_zone_constraint());
    // counts z1=2, z2=1, weight ln(2+2): raw round(2.77)=3 and round(1.39)=1,
    // inverted 100*(3+1-s)/3 => 33, 100.
    assert_eq!(scores(&p, &nodes, &existing).await, Some(vec![33, 100]));
}

#[tokio::test]
async fn nodes_missing_the_topology_key_are_ignored() {
    let mut no_zone = node("c", "x");
    no_zone.metadata.labels = Some(Default::default());
    let nodes = vec![node("a", "z1"), no_zone];
    let existing = vec![pod("e1", "a", json!([]))];
    let p = pod("p", "", soft_zone_constraint());
    assert_eq!(scores(&p, &nodes, &existing).await, Some(vec![100, 0]));
}
