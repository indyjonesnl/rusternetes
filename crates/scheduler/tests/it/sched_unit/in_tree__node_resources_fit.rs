//! Cases of `TestEnoughRequests` (`plugins/noderesources/fit_test.go`) and of
//! `RunFilterPluginsWithNominatedPods` (`runtime/framework.go:1145-1190`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, NoHandle, NodeInfo, PreFilterPlugin, Status,
};
use rusternetes_scheduler::in_tree::NodeResourcesFit;
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn pod(name: &str, node: &str, requests: Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"nodeName": node, "containers": [{"name": "c", "resources": {"requests": requests}}]}
    }))
    .unwrap()
}

fn node(allocatable: Value) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": "n"},
        "status": {"allocatable": allocatable}
    }))
    .unwrap()
}

fn alloc() -> Value {
    json!({"cpu": "10", "memory": "20", "pods": "32", "ephemeral-storage": "20", "example.com/foo": "5"})
}

async fn run(p: &Pod, n: &Node, existing: &[Pod]) -> Status {
    let mut state = CycleState::new();
    let (result, status) = NodeResourcesFit
        .pre_filter(&mut state, p, &[], &[], &NoHandle)
        .await;
    assert!(result.is_none() && status.is_success());
    NodeResourcesFit
        .filter(&state, p, &NodeInfo::new(n, existing))
        .await
}

#[tokio::test]
async fn filter_without_pre_filter_state_is_an_error() {
    let n = node(alloc());
    let s = NodeResourcesFit
        .filter(
            &CycleState::new(),
            &pod("p", "", json!({})),
            &NodeInfo::new(&n, &[]),
        )
        .await;
    assert_eq!(s.code(), Code::Error);
    assert!(s.message().contains("PreFilterNodeResourcesFit"));
}

#[tokio::test]
async fn no_resources_requested_always_fits() {
    let n = node(alloc());
    let existing = vec![pod("e", "n", json!({"cpu": "10", "memory": "20"}))];
    assert!(run(&pod("p", "", json!({})), &n, &existing)
        .await
        .is_success());
}

#[tokio::test]
async fn too_many_resources_fails() {
    let n = node(alloc());
    let existing = vec![pod("e", "n", json!({"cpu": "10", "memory": "20"}))];
    let s = run(
        &pod("p", "", json!({"cpu": "1", "memory": "1"})),
        &n,
        &existing,
    )
    .await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec!["Insufficient cpu", "Insufficient memory"]);
}

#[tokio::test]
async fn request_over_capacity_is_unresolvable() {
    let n = node(alloc());
    let s = run(&pod("p", "", json!({"cpu": "11"})), &n, &[]).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec!["Insufficient cpu"]);
}

#[tokio::test]
async fn equal_edge_case_fits() {
    let n = node(alloc());
    let existing = vec![pod("e", "n", json!({"cpu": "9", "memory": "19"}))];
    assert!(run(
        &pod("p", "", json!({"cpu": "1", "memory": "1"})),
        &n,
        &existing
    )
    .await
    .is_success());
}

#[tokio::test]
async fn ephemeral_storage_is_checked() {
    let n = node(alloc());
    let s = run(&pod("p", "", json!({"ephemeral-storage": "25"})), &n, &[]).await;
    assert_eq!(s.reasons(), vec!["Insufficient ephemeral-storage"]);
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn extended_resource_fits_and_overflows() {
    let n = node(alloc());
    assert!(run(&pod("p", "", json!({"example.com/foo": "5"})), &n, &[])
        .await
        .is_success());
    let existing = vec![pod("e", "n", json!({"example.com/foo": "3"}))];
    let s = run(
        &pod("p", "", json!({"example.com/foo": "3"})),
        &n,
        &existing,
    )
    .await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec!["Insufficient example.com/foo"]);
    let s = run(&pod("p", "", json!({"example.com/bar": "1"})), &n, &[]).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn too_many_pods() {
    let n = node(json!({"cpu": "10", "memory": "20", "pods": "1"}));
    let existing = vec![pod("e", "n", json!({}))];
    let s = run(&pod("p", "", json!({})), &n, &existing).await;
    assert_eq!(s.code(), Code::Unschedulable);
    assert_eq!(s.reasons(), vec!["Too many pods"]);
}

#[tokio::test]
async fn terminal_pods_do_not_count() {
    let n = node(json!({"cpu": "1", "memory": "20", "pods": "1"}));
    let mut done = pod("e", "n", json!({"cpu": "1"}));
    done.status = Some(serde_json::from_value(json!({"phase": "Succeeded"})).unwrap());
    assert!(run(&pod("p", "", json!({"cpu": "1"})), &n, &[done])
        .await
        .is_success());
}

#[tokio::test]
async fn init_container_max_and_overhead_are_in_the_request() {
    let n = node(json!({"cpu": "4", "memory": "20", "pods": "10"}));
    let p: Pod = serde_json::from_value(json!({
        "metadata": {"name": "p"},
        "spec": {
            "overhead": {"cpu": "1"},
            "initContainers": [{"name": "i", "resources": {"requests": {"cpu": "3"}}}],
            "containers": [{"name": "c", "resources": {"requests": {"cpu": "1"}}}]
        }
    }))
    .unwrap();
    // max(init 3, containers 1) + overhead 1 = 4 fits exactly; one more does not.
    assert!(run(&p, &n, &[]).await.is_success());
    let existing = vec![pod("e", "n", json!({"cpu": "1"}))];
    assert_eq!(
        run(&p, &n, &existing).await.reasons(),
        vec!["Insufficient cpu"]
    );
}

#[tokio::test]
async fn nominated_pods_reserve_space_in_the_first_pass_only() {
    let fw = Profile::named("default-scheduler").framework;
    let n = node(json!({"cpu": "2", "memory": "20", "pods": "10"}));
    let candidate = pod("p", "", json!({"cpu": "2"}));
    // A pod nominated to n by preemption, not yet bound.
    let mut nominee = pod("nom", "", json!({"cpu": "2"}));
    nominee.status = Some(serde_json::from_value(json!({"nominatedNodeName": "n"})).unwrap());
    let mut state = CycleState::new();
    let (_, pre, _) = fw
        .run_pre_filter_plugins(
            &mut state,
            &candidate,
            std::slice::from_ref(&n),
            &[],
            &NoHandle,
        )
        .await;
    assert!(pre.is_success());
    let all = vec![nominee.clone()];

    let with = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &n,
            &all,
            std::slice::from_ref(&n),
            vec![&nominee],
        )
        .await;
    assert!(with.is_rejected(), "the nominee's space is reserved");
    assert_eq!(with.plugin(), "NodeResourcesFit");

    let without = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &n,
            &all,
            std::slice::from_ref(&n),
            vec![],
        )
        .await;
    assert!(without.is_success());
}

#[tokio::test]
async fn node_without_allocatable_is_not_filtered() {
    let n: Node = serde_json::from_value(json!({"metadata": {"name": "n"}})).unwrap();
    assert!(run(&pod("p", "", json!({"cpu": "100"})), &n, &[])
        .await
        .is_success());
}
