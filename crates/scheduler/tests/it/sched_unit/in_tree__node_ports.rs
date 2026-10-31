//! `TestNodePorts` (`plugins/nodeports/node_ports_test.go`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo, PreFilterPlugin};
use rusternetes_scheduler::in_tree::{NodePorts, ERR_REASON_NODE_PORTS};
use serde_json::json;

/// `newPod(host, "PROTO/ip/port"...)`.
fn new_pod(host: &str, ports: &[&str]) -> Pod {
    let ports: Vec<_> = ports
        .iter()
        .map(|p| {
            let mut it = p.split('/');
            let (proto, ip, port) = (it.next().unwrap(), it.next().unwrap(), it.next().unwrap());
            json!({"hostIP": ip, "hostPort": port.parse::<u16>().unwrap(), "containerPort": 80, "protocol": proto})
        })
        .collect();
    serde_json::from_value(json!({
        "metadata": {"name": "p"},
        "spec": {"nodeName": host, "containers": [{"name": "c", "ports": ports}]}
    }))
    .unwrap()
}

async fn run(pod: Pod, existing: Vec<Pod>) -> rusternetes_scheduler::fwk::Status {
    let node: Node = serde_json::from_value(json!({"metadata": {"name": "m1"}})).unwrap();
    let mut state = CycleState::new();
    let (result, status) = NodePorts.pre_filter(&mut state, &pod, &[]).await;
    assert!(result.is_none(), "NodePorts never narrows the nodes");
    if status.is_skip() {
        return rusternetes_scheduler::fwk::Status::success();
    }
    assert!(status.is_success());
    NodePorts
        .filter(&state, &pod, &NodeInfo::new(&node, &existing))
        .await
}

fn conflict(s: rusternetes_scheduler::fwk::Status) {
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec![ERR_REASON_NODE_PORTS.to_string()]);
}

#[tokio::test]
async fn skip_filter_no_ports() {
    assert!(run(new_pod("m1", &[]), vec![]).await.is_success());
    // PreFilter says Skip, so the framework does not run Filter.
    let (result, status) = NodePorts
        .pre_filter(&mut CycleState::new(), &new_pod("m1", &[]), &[])
        .await;
    assert!(result.is_none());
    assert!(status.is_skip());
}

#[tokio::test]
async fn filter_without_pre_filter_state_is_an_error() {
    // `getPreFilterState`: PreFilter was not invoked.
    let node: Node = serde_json::from_value(json!({"metadata": {"name": "m1"}})).unwrap();
    let pods: Vec<Pod> = vec![];
    let s = NodePorts
        .filter(
            &CycleState::new(),
            &new_pod("m1", &["TCP/127.0.0.1/80"]),
            &NodeInfo::new(&node, &pods),
        )
        .await;
    assert_eq!(s.code(), Code::Error);
    assert!(s.message().contains("PreFilterNodePorts"));
}

#[tokio::test]
async fn other_port() {
    let s = run(
        new_pod("m1", &["UDP/127.0.0.1/8080"]),
        vec![new_pod("m1", &["UDP/127.0.0.1/9090"])],
    )
    .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn same_udp_port() {
    conflict(
        run(
            new_pod("m1", &["UDP/127.0.0.1/8080"]),
            vec![new_pod("m1", &["UDP/127.0.0.1/8080"])],
        )
        .await,
    );
}

#[tokio::test]
async fn same_tcp_port() {
    conflict(
        run(
            new_pod("m1", &["TCP/127.0.0.1/8080"]),
            vec![new_pod("m1", &["TCP/127.0.0.1/8080"])],
        )
        .await,
    );
}

#[tokio::test]
async fn different_host_ip() {
    let s = run(
        new_pod("m1", &["TCP/127.0.0.1/8080"]),
        vec![new_pod("m1", &["TCP/127.0.0.2/8080"])],
    )
    .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn different_protocol() {
    let s = run(
        new_pod("m1", &["UDP/127.0.0.1/8080"]),
        vec![new_pod("m1", &["TCP/127.0.0.1/8080"])],
    )
    .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn second_udp_port_conflict() {
    conflict(
        run(
            new_pod("m1", &["UDP/127.0.0.1/8000", "UDP/127.0.0.1/8080"]),
            vec![new_pod("m1", &["UDP/127.0.0.1/8080"])],
        )
        .await,
    );
}

#[tokio::test]
async fn first_tcp_port_conflict() {
    conflict(
        run(
            new_pod("m1", &["TCP/127.0.0.1/8001", "UDP/127.0.0.1/8080"]),
            vec![new_pod("m1", &["TCP/127.0.0.1/8001", "UDP/127.0.0.1/8081"])],
        )
        .await,
    );
}

#[tokio::test]
async fn wildcard_host_ip_conflicts_with_a_specific_one() {
    conflict(
        run(
            new_pod("m1", &["TCP/0.0.0.0/8080"]),
            vec![new_pod("m1", &["TCP/127.0.0.1/8080"])],
        )
        .await,
    );
}

#[tokio::test]
async fn pods_on_other_nodes_do_not_count() {
    let s = run(
        new_pod("m1", &["TCP/127.0.0.1/8080"]),
        vec![new_pod("m2", &["TCP/127.0.0.1/8080"])],
    )
    .await;
    assert!(s.is_success());
}
