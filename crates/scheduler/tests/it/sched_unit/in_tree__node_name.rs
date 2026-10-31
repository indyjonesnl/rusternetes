//! `TestNodeName` (`plugins/nodename/node_name_test.go`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo};
use rusternetes_scheduler::in_tree::{NodeName, ERR_REASON_NODE_NAME};

async fn run(pod_node: Option<&str>, node_name: &str) -> rusternetes_scheduler::fwk::Status {
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "p"},
        "spec": pod_node.map(|n| serde_json::json!({"nodeName": n})).unwrap_or(serde_json::json!({}))
    }))
    .unwrap();
    let node: Node =
        serde_json::from_value(serde_json::json!({"metadata": {"name": node_name}})).unwrap();
    let pods: Vec<Pod> = vec![];
    NodeName
        .filter(&CycleState::new(), &pod, &NodeInfo::new(&node, &pods))
        .await
}

#[tokio::test]
async fn no_host_specified() {
    assert!(run(None, "foo").await.is_success());
}

#[tokio::test]
async fn host_matches() {
    assert!(run(Some("foo"), "foo").await.is_success());
}

#[tokio::test]
async fn host_doesnt_match() {
    let s = run(Some("bar"), "foo").await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_NODE_NAME.to_string()]);
}
