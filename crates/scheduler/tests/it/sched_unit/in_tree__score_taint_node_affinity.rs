//! TaintToleration and NodeAffinity PreScore/Score/NormalizeScore through the
//! plugin objects and `Framework::run_score_plugins`
//! (`tainttoleration/taint_toleration_test.go` TestTaintTolerationScore,
//! `nodeaffinity/node_affinity_test.go` TestNodeAffinityPriority).
use rusternetes_common::resources::pod::NodeAffinity;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{CycleState, PreScorePlugin};
use rusternetes_scheduler::in_tree::NodeAffinityPlugin;
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn pod(spec: Value) -> Pod {
    serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
}

fn node(name: &str, labels: Value, taints: Value) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name, "labels": labels}, "spec": {"taints": taints}
    }))
    .unwrap()
}

fn prefer(key: &str) -> Value {
    json!({"key": key, "value": "v", "effect": "PreferNoSchedule"})
}

async fn scores(
    fw: &rusternetes_scheduler::framework_runtime::Framework,
    p: &Pod,
    nodes: &[Node],
    name: &str,
) -> Option<Vec<i64>> {
    let refs: Vec<&Node> = nodes.iter().collect();
    let mut state = CycleState::new();
    let s = fw.run_pre_score_plugins(&mut state, p, &refs).await;
    assert!(s.is_success(), "{s:?}");
    let out = fw.run_score_plugins(&state, p, &refs, &[], nodes).unwrap();
    out.into_iter().find(|s| s.name == name).map(|s| s.scores)
}

#[tokio::test]
async fn taint_toleration_prefers_nodes_with_fewer_intolerable_taints() {
    let fw = Profile::named("default-scheduler").framework;
    let nodes = vec![
        node("a", json!({}), json!([])),
        node("b", json!({}), json!([prefer("k1")])),
        node("c", json!({}), json!([prefer("k1"), prefer("k2")])),
    ];
    let p = pod(json!({"containers": []}));
    assert_eq!(
        scores(&fw, &p, &nodes, "TaintToleration").await,
        Some(vec![100, 50, 0])
    );
    // A toleration for k1 makes b as good as a.
    let tolerant = pod(json!({"containers": [], "tolerations": [
        {"key": "k1", "operator": "Equal", "value": "v", "effect": "PreferNoSchedule"}]}));
    assert_eq!(
        scores(&fw, &tolerant, &nodes, "TaintToleration").await,
        Some(vec![100, 100, 0])
    );
}

#[tokio::test]
async fn node_affinity_scores_preferred_terms_and_normalizes() {
    let fw = Profile::named("default-scheduler").framework;
    let nodes = vec![
        node("a", json!({"foo": "x"}), json!([])),
        node("b", json!({"foo": "x", "bar": "y"}), json!([])),
        node("c", json!({}), json!([])),
    ];
    let p = pod(json!({"containers": [], "affinity": {"nodeAffinity": {
    "preferredDuringSchedulingIgnoredDuringExecution": [
        {"weight": 2, "preference": {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["x"]}]}},
        {"weight": 4, "preference": {"matchExpressions": [{"key": "bar", "operator": "In", "values": ["y"]}]}}
    ]}}}));
    // Raw 2, 6, 0 normalised against the max (6).
    assert_eq!(
        scores(&fw, &p, &nodes, "NodeAffinity").await,
        Some(vec![33, 100, 0])
    );
}

#[tokio::test]
async fn node_affinity_skips_pods_that_prefer_nothing() {
    let fw = Profile::named("default-scheduler").framework;
    let nodes = vec![node("a", json!({}), json!([]))];
    let p = pod(json!({"containers": []}));
    // PreScore Skip leaves the plugin out of the Score results.
    assert_eq!(scores(&fw, &p, &nodes, "NodeAffinity").await, None);
    let mut state = CycleState::new();
    let state = NodeAffinityPlugin::new(None)
        .pre_score(&mut state, &p, &[&nodes[0]])
        .await;
    assert!(state.is_skip());
}

#[tokio::test]
async fn added_affinity_scores_even_without_pod_preferences() {
    let added: NodeAffinity = serde_json::from_value(json!({
        "preferredDuringSchedulingIgnoredDuringExecution": [
            {"weight": 5, "preference": {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["x"]}]}}]
    }))
    .unwrap();
    let plugin = NodeAffinityPlugin::new(Some(&added));
    let mut state = CycleState::new();
    let n = node("a", json!({"foo": "x"}), json!([]));
    let s = plugin
        .pre_score(&mut state, &pod(json!({"containers": []})), &[&n])
        .await;
    assert!(s.is_success());
    use rusternetes_scheduler::fwk::{NodeInfo, ScorePlugin};
    let pods: Vec<Pod> = vec![];
    let (score, st) = plugin.score(
        &state,
        &pod(json!({"containers": []})),
        &NodeInfo::new(&n, &pods),
    );
    assert!(st.is_success());
    assert_eq!(score, 5);
}
