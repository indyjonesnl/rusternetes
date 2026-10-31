//! `TestTaintTolerationFilter` cases (`plugins/tainttoleration/taint_toleration_test.go`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo};
use rusternetes_scheduler::in_tree::TaintToleration;
use serde_json::json;

async fn passes(tolerations: serde_json::Value, taints: serde_json::Value) -> Option<String> {
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {"name": "p"}, "spec": {"tolerations": tolerations}
    }))
    .unwrap();
    let node: Node = serde_json::from_value(json!({
        "metadata": {"name": "n"}, "spec": {"taints": taints}
    }))
    .unwrap();
    let pods: Vec<Pod> = vec![];
    let s = TaintToleration
        .filter(&CycleState::new(), &pod, &NodeInfo::new(&node, &pods))
        .await;
    if s.is_success() {
        None
    } else {
        assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
        Some(s.message())
    }
}

const WANT: &str = "node(s) had untolerated taint(s)";

#[tokio::test]
async fn pod_without_tolerations_cant_go_on_tainted_node() {
    let got = passes(
        json!([]),
        json!([{"key": "dedicated", "value": "user1", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got.as_deref(), Some(WANT));
}

#[tokio::test]
async fn pod_tolerating_dedicated_node_schedules() {
    let got = passes(
        json!([{"key": "dedicated", "value": "user1", "effect": "NoSchedule"}]),
        json!([{"key": "dedicated", "value": "user1", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got, None);
}

#[tokio::test]
async fn toleration_for_another_user_does_not_match() {
    let got = passes(
        json!([{"key": "dedicated", "value": "user1", "effect": "NoSchedule"}]),
        json!([{"key": "dedicated", "value": "user2", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got.as_deref(), Some(WANT));
}

#[tokio::test]
async fn exists_operator_tolerates_the_taint() {
    let got = passes(
        json!([{"key": "foo", "operator": "Exists", "effect": "NoSchedule"}]),
        json!([{"key": "foo", "value": "bar", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got, None);
}

#[tokio::test]
async fn effect_mismatch_is_not_tolerated() {
    let got = passes(
        json!([{"key": "foo", "value": "bar", "effect": "PreferNoSchedule"}]),
        json!([{"key": "foo", "value": "bar", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got.as_deref(), Some(WANT));
}

#[tokio::test]
async fn empty_effect_toleration_tolerates_any_effect() {
    let got = passes(
        json!([{"key": "foo", "value": "bar"}]),
        json!([{"key": "foo", "value": "bar", "effect": "NoSchedule"}]),
    )
    .await;
    assert_eq!(got, None);
}

#[tokio::test]
async fn prefer_no_schedule_taint_never_blocks() {
    let got = passes(
        json!([]),
        json!([{"key": "foo", "value": "bar", "effect": "PreferNoSchedule"}]),
    )
    .await;
    assert_eq!(got, None);
}

#[tokio::test]
async fn no_execute_taint_blocks() {
    let got = passes(
        json!([]),
        json!([{"key": "foo", "value": "bar", "effect": "NoExecute"}]),
    )
    .await;
    assert_eq!(got.as_deref(), Some(WANT));
}
