//! `batch_test.go` (`TestBatchBasic`) and the `OpportunisticBatching` wiring
//! in `schedulePod`/`findNodesThatFitPod` (schedule_one.go:443-471, :509-530).
//!
//! The upstream test injects the signature and a Filter plugin that lets a
//! node hold one "blocking" pod. Here the signature is the real
//! `Framework::sign_pod` and the one-pod-per-node Filter is NodePorts: a pod
//! with `hostPort: 80` is blocking, a pod without host ports is not.
use super::listers::EmptyListers;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::diagnosis::Diagnosis;
use rusternetes_scheduler::framework_runtime::Framework;
use rusternetes_scheduler::fwk::CycleState;
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_scheduler::scheduler_config::profiles_from_config;
use rusternetes_scheduler::score::SortedNodeScores;
use rusternetes_storage::memory::MemoryStorage;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

const PROFILES: &str = r#"
profiles:
- schedulerName: batching
  plugins:
    multiPoint:
      disabled:
      - name: '*'
    queueSort:
      enabled:
      - name: PrioritySort
    bind:
      enabled:
      - name: DefaultBinder
    preFilter:
      enabled:
      - name: NodePorts
    filter:
      enabled:
      - name: NodePorts
    preScore:
      enabled:
      - name: TaintToleration
    score:
      enabled:
      - name: TaintToleration
"#;

fn framework() -> Framework {
    profiles_from_config(PROFILES, false)
        .unwrap()
        .get("batching")
        .unwrap()
        .framework
        .clone()
}

fn node(name: &str) -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "uid": format!("{name}-uid")},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    }))
    .unwrap()
}

/// `ports` are host ports; 80 is the "blocking" port.
fn pod(name: &str, ports: &[i32], node_name: Option<&str>) -> Pod {
    let ports: Vec<_> = ports
        .iter()
        .map(|p| json!({"containerPort": *p, "hostPort": *p}))
        .collect();
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
        "spec": {
            "schedulerName": "batching",
            "nodeName": node_name,
            "containers": [{"name": "c", "image": "busybox", "ports": ports}]
        },
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

fn sorted(names: &[&str]) -> SortedNodeScores {
    // Strictly decreasing scores keep the given order.
    let totals = (0..names.len()).map(|i| 100 - i as i64).collect();
    SortedNodeScores::new(names.iter().map(|n| n.to_string()).collect(), totals)
}

struct Case {
    name: &'static str,
    first_ports: &'static [i32],
    first_scheduled: bool,
    first_chosen: &'static str,
    first_others: Option<&'static [&'static str]>,
    skip_pod: bool,
    second_ports: &'static [i32],
    second_chosen: &'static str,
    expected_hint: Option<&'static str>,
    /// The remaining nodes of the state after the second pod.
    expected_state: Option<&'static [&'static str]>,
}

/// `TestBatchBasic`: the first pod goes through the batch, then the second.
#[tokio::test]
async fn batch_basic() {
    let cases = [
        Case {
            name: "a second pod with the same signature gets a hint",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&["n1"]),
            skip_pod: false,
            second_ports: &[80],
            second_chosen: "n1",
            expected_hint: Some("n1"),
            expected_state: None,
        },
        Case {
            name: "a second pod with a different signature doesn't get a hint",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&["n1"]),
            skip_pod: false,
            // Still blocked by the first pod on n3, but signs differently.
            second_ports: &[80, 81],
            second_chosen: "n1",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "a second pod doesn't get a hint if it's not 1-pod-per-node",
            first_ports: &[],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&["n1"]),
            skip_pod: false,
            second_ports: &[],
            second_chosen: "n1",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "pod doesn't get hint if previous pod didn't scheduled",
            first_ports: &[80],
            first_scheduled: false,
            first_chosen: "n3",
            first_others: Some(&["n1"]),
            skip_pod: false,
            second_ports: &[80],
            second_chosen: "n1",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "empty list",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&[]),
            skip_pod: false,
            second_ports: &[80],
            second_chosen: "n4",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "nil list",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: None,
            skip_pod: false,
            second_ports: &[80],
            second_chosen: "n4",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "pod doesn't get hint because the previous pod is to a different profile",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&["n1"]),
            skip_pod: true,
            second_ports: &[80],
            second_chosen: "n1",
            expected_hint: None,
            expected_state: None,
        },
        Case {
            name: "pod uses batch from preceding pod and leaves remaining batch to next pod",
            first_ports: &[80],
            first_scheduled: true,
            first_chosen: "n3",
            first_others: Some(&["n1", "n2"]),
            skip_pod: false,
            second_ports: &[80],
            second_chosen: "n1",
            expected_hint: Some("n1"),
            expected_state: Some(&["n2"]),
        },
    ];
    for c in cases {
        let fw = framework();
        let nodes = vec![node("n3")];
        let first = pod("first", c.first_ports, Some("n3"));
        let handle = EmptyListers;

        // The first pod finds no hint.
        let mut state = CycleState::new();
        let (_, status, _) = fw
            .run_pre_filter_plugins(&mut state, &first, &nodes, &[], &handle)
            .await;
        assert!(status.is_success(), "{}", c.name);
        let (hint, signature) = fw.get_node_hint(&first, &state, 1, &nodes, &[]).await;
        assert_eq!(hint, None, "{}", c.name);
        assert!(signature.is_some(), "{}", c.name);
        if c.first_scheduled {
            fw.store_schedule_results(signature, "", c.first_chosen, c.first_others.map(sorted), 1);
        }

        // The second pod; the first one is now on n3.
        let cycle = if c.skip_pod { 3 } else { 2 };
        let second = pod("second", c.second_ports, None);
        let all_pods = vec![first];
        let mut state = CycleState::new();
        let (_, status, _) = fw
            .run_pre_filter_plugins(&mut state, &second, &nodes, &all_pods, &handle)
            .await;
        assert!(status.is_success(), "{}", c.name);
        let (hint, signature) = fw
            .get_node_hint(&second, &state, cycle, &nodes, &all_pods)
            .await;
        assert_eq!(hint.as_deref(), c.expected_hint, "{}", c.name);
        fw.store_schedule_results(
            signature.clone(),
            hint.as_deref().unwrap_or(""),
            c.second_chosen,
            None,
            cycle,
        );
        let got = fw.batch_state();
        match (c.expected_state, got) {
            (None, None) => {}
            (None, Some((_, nodes))) => assert!(nodes.is_empty(), "{}: {nodes:?}", c.name),
            (Some(want), Some((sig, nodes))) => {
                assert_eq!(nodes, want, "{}", c.name);
                assert_eq!(sig, signature.unwrap(), "{}", c.name);
            }
            (Some(_), None) => panic!("{}: expected a cached state", c.name),
        }
    }
}

/// `maxBatchAge`: a state older than 500ms is thrown away.
#[tokio::test]
async fn expired_state_gives_no_hint() {
    use rusternetes_scheduler::batch::{OpportunisticBatch, Precheck, MAX_BATCH_AGE};
    assert_eq!(MAX_BATCH_AGE, std::time::Duration::from_millis(500));
    let batch = OpportunisticBatch::default();
    batch.store_schedule_results(Some(b"sig".to_vec()), "", "n3", Some(sorted(&["n1"])), 1);
    assert_eq!(
        batch.precheck(Some(&b"sig".to_vec()), 2),
        Precheck::CheckLastChosenNode("n3".to_string())
    );
    tokio::time::sleep(MAX_BATCH_AGE + std::time::Duration::from_millis(50)).await;
    assert_eq!(
        batch.precheck(Some(&b"sig".to_vec()), 2),
        Precheck::Unusable
    );
}

/// Three identical blocking pods on three nodes through `schedulePod`: the
/// first one filters and scores every node, the next two take the batch's
/// hints and evaluate only the hinted node.
#[tokio::test]
async fn pods_after_the_first_take_the_batch_hint() {
    let profiles = profiles_from_config(PROFILES, false).unwrap();
    let scheduler = Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
    let nodes = vec![node("n1"), node("n2"), node("n3")];
    let fw = &scheduler
        .profile_for(&pod("x", &[], None))
        .unwrap()
        .framework;
    let mut all_pods: Vec<Pod> = Vec::new();
    let mut placed = Vec::new();
    let mut cursor_after_search = None;
    for (cycle, name) in ["p1", "p2", "p3"].iter().enumerate() {
        let p = pod(name, &[80], None);
        let mut diag = Diagnosis::default();
        let (chosen, _) = scheduler
            .select_node_cycle(
                &p,
                &nodes,
                &all_pods,
                &HashMap::new(),
                &mut diag,
                Some(cycle as i64 + 1),
            )
            .await
            .expect("a node fits");
        let cursor = scheduler
            .next_start_node_index
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(fw.batched_pods(), cycle as i64, "cycle {cycle}");
        if cycle == 0 {
            // The first pod searched every node and cached the other two.
            cursor_after_search = Some(cursor);
            assert_eq!(fw.batch_state().unwrap().1.len(), 2);
        } else {
            // The hinted node is evaluated alone, like a nominated node: the
            // node search and its cursor update are skipped.
            assert_eq!(Some(cursor), cursor_after_search, "cycle {cycle}");
        }
        placed.push(chosen.metadata.name.clone());
        all_pods.push(pod(name, &[80], Some(&chosen.metadata.name)));
    }
    placed.sort();
    placed.dedup();
    assert_eq!(placed.len(), 3, "one pod per node: {placed:?}");
}

/// Without a cycle count (or with the gate off) nothing is hinted or stored.
#[tokio::test]
async fn no_cycle_count_means_no_batching() {
    let profiles = profiles_from_config(PROFILES, false).unwrap();
    let scheduler = Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
    let nodes = vec![node("n1"), node("n2")];
    let p = pod("p1", &[80], None);
    let mut diag = Diagnosis::default();
    scheduler
        .select_node_inner(&p, &nodes, &[], &HashMap::new(), &mut diag)
        .await
        .expect("a node fits");
    let fw = &scheduler.profile_for(&p).unwrap().framework;
    assert!(fw.batch_state().is_none());
}
