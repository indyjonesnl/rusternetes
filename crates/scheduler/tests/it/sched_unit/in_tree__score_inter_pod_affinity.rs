//! InterPodAffinity PreScore/Score/NormalizeScore through the plugin object
//! (`interpodaffinity/scoring_test.go` TestPreferredAffinity).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, NoHandle};
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn node(name: &str, zone: &str) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name, "labels": {"zone": zone}},
        "status": {"allocatable": {"cpu": "4", "memory": "4Gi", "pods": "110"}}
    }))
    .unwrap()
}

fn pod(name: &str, node: &str, labels: Value, affinity: Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default", "labels": labels},
        "spec": {"nodeName": node, "containers": [{"name": "c"}], "affinity": affinity}
    }))
    .unwrap()
}

fn preferred(kind: &str, weight: i32) -> Value {
    json!({kind: {"preferredDuringSchedulingIgnoredDuringExecution": [{
        "weight": weight,
        "podAffinityTerm": {
            "labelSelector": {"matchLabels": {"app": "db"}},
            "topologyKey": "zone"
        }
    }]}})
}

async fn run(p: &Pod, nodes: &[Node], existing: &[Pod]) -> (Code, Option<Vec<i64>>) {
    let fw = Profile::named("default-scheduler").framework;
    let refs: Vec<&Node> = nodes.iter().collect();
    let mut state = CycleState::new();
    let s = fw
        .run_pre_score_plugins(&mut state, p, &refs, existing, nodes, &NoHandle)
        .await;
    let scores = fw
        .run_score_plugins(&state, p, &refs, existing, nodes)
        .unwrap()
        .into_iter()
        .find(|s| s.name == "InterPodAffinity")
        .map(|s| s.scores);
    (s.code(), scores)
}

#[tokio::test]
async fn preferred_affinity_favours_the_zone_with_matching_pods() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![pod("db", "a", json!({"app": "db"}), Value::Null)];
    let p = pod("p", "", json!({}), preferred("podAffinity", 5));
    let (code, scores) = run(&p, &nodes, &existing).await;
    assert_eq!(code, Code::Success);
    assert_eq!(scores, Some(vec![100, 0]));
}

#[tokio::test]
async fn preferred_anti_affinity_avoids_the_zone_with_matching_pods() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let existing = vec![pod("db", "a", json!({"app": "db"}), Value::Null)];
    let p = pod("p", "", json!({}), preferred("podAntiAffinity", 5));
    let (_, scores) = run(&p, &nodes, &existing).await;
    assert_eq!(scores, Some(vec![0, 100]));
}

#[tokio::test]
async fn nothing_to_score_is_skipped() {
    let nodes = vec![node("a", "z1"), node("b", "z2")];
    let p = pod("p", "", json!({}), Value::Null);
    let (code, scores) = run(&p, &nodes, &[]).await;
    // RunPreScorePlugins records the Skip; the plugin's Score is not run.
    assert_eq!(code, Code::Success);
    assert_eq!(scores, None);
}
