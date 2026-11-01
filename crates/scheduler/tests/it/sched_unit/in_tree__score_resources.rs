//! NodeResourcesFit and NodeResourcesBalancedAllocation scoring through the
//! plugin objects and `Framework::run_score_plugins`
//! (`noderesources/least_allocated_test.go`, `balanced_allocation_test.go`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::CycleState;
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn pod(name: &str, node: &str, requests: Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"nodeName": node, "containers": [{"name": "c", "resources": {"requests": requests}}]}
    }))
    .unwrap()
}

fn node(name: &str) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name},
        "status": {"allocatable": {"cpu": "4", "memory": "4Gi", "pods": "110"}}
    }))
    .unwrap()
}

async fn scores(p: &Pod, nodes: &[Node], existing: &[Pod], plugin: &str) -> Option<Vec<i64>> {
    let fw = Profile::named("default-scheduler").framework;
    let refs: Vec<&Node> = nodes.iter().collect();
    let mut state = CycleState::new();
    let s = fw
        .run_pre_score_plugins(&mut state, p, &refs, existing)
        .await;
    assert!(s.is_success(), "{s:?}");
    fw.run_score_plugins(&state, p, &refs, existing, nodes)
        .unwrap()
        .into_iter()
        .find(|s| s.name == plugin)
        .map(|s| s.scores)
}

#[tokio::test]
async fn least_allocated_and_balanced_use_node_usage() {
    let nodes = vec![node("a"), node("b")];
    // b already runs 2 cpu.
    let existing = vec![pod("e", "b", json!({"cpu": "2", "memory": "1Gi"}))];
    let p = pod("p", "", json!({"cpu": "1", "memory": "1Gi"}));
    // a: cpu 75, mem 75 => 75. b: cpu (4-3)/4 = 25, mem (4-2)/4 = 50 => 37.
    assert_eq!(
        scores(&p, &nodes, &existing, "NodeResourcesFit").await,
        Some(vec![75, 37])
    );
    // Balanced: a fractions 0.25/0.25 => 100; b 0.75/0.5 => std 0.125 => 87.
    assert_eq!(
        scores(&p, &nodes, &existing, "NodeResourcesBalancedAllocation").await,
        Some(vec![100, 87])
    );
}

#[tokio::test]
async fn balanced_allocation_skips_best_effort_pods() {
    let nodes = vec![node("a")];
    let p = pod("p", "", json!({}));
    assert_eq!(
        scores(&p, &nodes, &[], "NodeResourcesBalancedAllocation").await,
        None
    );
    // Fit still scores (non-zero default requests).
    assert!(scores(&p, &nodes, &[], "NodeResourcesFit").await.is_some());
}

#[tokio::test]
async fn terminal_pods_do_not_count_against_usage() {
    let nodes = vec![node("a")];
    let mut done = pod("e", "a", json!({"cpu": "3", "memory": "3Gi"}));
    done.status = Some(serde_json::from_value(json!({"phase": "Succeeded"})).unwrap());
    let p = pod("p", "", json!({"cpu": "1", "memory": "1Gi"}));
    assert_eq!(
        scores(&p, &nodes, &[done], "NodeResourcesFit").await,
        Some(vec![75])
    );
}
