//! `TestNodeUnschedulable`
//! (`pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable_test.go`)
//! through the plugin object, and through `Framework::run_filter_plugins`
//! (`runtime/framework.go:1033-1066`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::framework_runtime::ExtensionPoint;
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo};
use rusternetes_scheduler::in_tree::{NodeUnschedulable, ERR_REASON_UNSCHEDULABLE};
use rusternetes_scheduler::profile::Profile;

fn pod(v: serde_json::Value) -> Pod {
    serde_json::from_value(v).unwrap()
}

fn bare_pod() -> Pod {
    pod(serde_json::json!({"metadata": {"name": "p"}}))
}

fn node(unschedulable: bool) -> Node {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": "n"},
        "spec": {"unschedulable": unschedulable}
    }))
    .unwrap()
}

fn tolerating_pod() -> Pod {
    pod(serde_json::json!({
        "metadata": {"name": "p"},
        "spec": {"tolerations": [
            {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}
        ]}
    }))
}

#[tokio::test]
async fn does_not_schedule_pod_to_unschedulable_node() {
    let n = node(true);
    let pods: Vec<Pod> = vec![];
    let s = NodeUnschedulable
        .filter(&CycleState::new(), &bare_pod(), &NodeInfo::new(&n, &pods))
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_UNSCHEDULABLE.to_string()]);
}

#[tokio::test]
async fn schedules_pod_to_normal_node() {
    let n = node(false);
    let pods: Vec<Pod> = vec![];
    let s = NodeUnschedulable
        .filter(&CycleState::new(), &bare_pod(), &NodeInfo::new(&n, &pods))
        .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn schedules_pod_with_toleration_to_unschedulable_node() {
    let n = node(true);
    let pods: Vec<Pod> = vec![];
    let s = NodeUnschedulable
        .filter(
            &CycleState::new(),
            &tolerating_pod(),
            &NodeInfo::new(&n, &pods),
        )
        .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn framework_dispatches_the_plugin_object_and_names_it() {
    let fw = Profile::named("default-scheduler").framework;
    assert!(fw.has(ExtensionPoint::Filter, "NodeUnschedulable"));
    let n = node(true);
    let pods: Vec<Pod> = vec![];
    let mut state = CycleState::new();
    let (_, pre, _) = fw
        .run_pre_filter_plugins(&mut state, &bare_pod(), &[])
        .await;
    assert!(pre.is_success());
    let s = fw
        .run_filter_plugins(&state, &bare_pod(), &NodeInfo::new(&n, &pods))
        .await;
    assert!(s.is_rejected());
    assert_eq!(s.plugin(), "NodeUnschedulable");
    let s = fw
        .run_filter_plugins(&state, &tolerating_pod(), &NodeInfo::new(&n, &pods))
        .await;
    assert!(s.is_success());
}
