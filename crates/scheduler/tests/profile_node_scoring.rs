//! #3402: per-profile `percentageOfNodesToScore` and `NodeAffinity`
//! `addedAffinity` plugin args.
//!
//! Ported from:
//! - `pkg/scheduler/schedule_one.go` `numFeasibleNodesToFind` :710-738 and
//!   `findNodesThatPassFilters` :624-708 (stop once enough feasible nodes are
//!   found; profile percentage overrides the global one), with
//!   `nextStartNodeIndex` rotation (:544-548);
//! - `pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go`
//!   `Filter` :217-236 (`addedNodeSelector`, reason
//!   `node(s) didn't match scheduler-enforced node affinity`) and `Score`
//!   :264-296 (`addedPrefSchedTerms`).

use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_scheduler::scheduler_config::profiles_from_config;
use rusternetes_storage::{memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

fn node(name: &str, labels: serde_json::Value) -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "uid": format!("{name}-uid"), "labels": labels},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    }))
    .unwrap()
}

fn pod(scheduler: &str, affinity: serde_json::Value) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default", "uid": "p-uid"},
        "spec": {
            "schedulerName": scheduler,
            "containers": [{"name": "c", "image": "busybox"}],
            "affinity": affinity
        },
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

/// Build the scheduler from `doc`, create `nodes` and one pod `p` handled by
/// profile `scheduler`, run one scheduling pass and return `p`'s node.
async fn schedule(
    doc: &str,
    scheduler: &str,
    nodes: Vec<Node>,
    affinity: serde_json::Value,
) -> Option<String> {
    let profiles = profiles_from_config(doc, false).unwrap();
    let storage = Arc::new(MemoryStorage::new());
    for n in nodes {
        storage
            .create(&format!("/registry/nodes/{}", n.metadata.name), &n)
            .await
            .unwrap();
    }
    storage
        .create("/registry/pods/default/p", &pod(scheduler, affinity))
        .await
        .unwrap();
    let s = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
    s.schedule_pending_pods().await.unwrap();
    storage
        .get::<Pod>("/registry/pods/default/p")
        .await
        .unwrap()
        .spec
        .and_then(|s| s.node_name)
}

/// 300 nodes, only the last one carries the label the pod prefers.
fn many_nodes() -> Vec<Node> {
    (0..300)
        .map(|i| {
            let labels = if i == 299 {
                json!({"pick": "me"})
            } else {
                json!({})
            };
            node(&format!("n-{i:03}"), labels)
        })
        .collect()
}

fn prefers_pick_me() -> serde_json::Value {
    json!({"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
        "weight": 100,
        "preference": {"matchExpressions": [
            {"key": "pick", "operator": "In", "values": ["me"]}
        ]}
    }]}})
}

/// `numFeasibleNodesToFind(nil, 300)` with the global default (0) is
/// `300 * (50 - 300/125) / 100` = 144 nodes: the scheduler stops looking after
/// 144 feasible nodes, so the preferred node 299 is never scored. A profile
/// `percentageOfNodesToScore: 100` overrides the global value and scores all.
#[tokio::test]
async fn profile_percentage_of_nodes_to_score_overrides_global() {
    let doc = r#"
profiles:
- schedulerName: default-scheduler
- schedulerName: all-nodes
  percentageOfNodesToScore: 100
"#;
    let default = schedule(doc, "default-scheduler", many_nodes(), prefers_pick_me())
        .await
        .unwrap();
    assert_ne!(
        default, "n-299",
        "default profile must stop after 144 nodes"
    );
    assert!(default.as_str() < "n-144", "{default}");
    let all = schedule(doc, "all-nodes", many_nodes(), prefers_pick_me()).await;
    assert_eq!(all.as_deref(), Some("n-299"));
}

/// The global `percentageOfNodesToScore` applies to profiles that set none.
#[tokio::test]
async fn global_percentage_of_nodes_to_score_applies() {
    let doc = r#"
percentageOfNodesToScore: 100
profiles:
- schedulerName: default-scheduler
"#;
    let got = schedule(doc, "default-scheduler", many_nodes(), prefers_pick_me()).await;
    assert_eq!(got.as_deref(), Some("n-299"));
}

fn two_nodes() -> Vec<Node> {
    vec![node("n1", json!({})), node("n2", json!({"disk": "ssd"}))]
}

const ADDED_AFFINITY_DOC: &str = r#"
profiles:
- schedulerName: default-scheduler
- schedulerName: ssd-only
  pluginConfig:
  - name: NodeAffinity
    args:
      addedAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: disk
              operator: In
              values: [ssd]
- schedulerName: prefers-ssd
  pluginConfig:
  - name: NodeAffinity
    args:
      addedAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          preference:
            matchExpressions:
            - key: disk
              operator: In
              values: [ssd]
"#;

/// `addedAffinity.requiredDuringSchedulingIgnoredDuringExecution` is ANDed
/// with every pod's own affinity by the NodeAffinity Filter.
#[tokio::test]
async fn added_affinity_required_filters_nodes() {
    let default = schedule(
        ADDED_AFFINITY_DOC,
        "default-scheduler",
        two_nodes(),
        json!({}),
    )
    .await;
    assert_eq!(default.as_deref(), Some("n1"));
    let ssd = schedule(ADDED_AFFINITY_DOC, "ssd-only", two_nodes(), json!({})).await;
    assert_eq!(ssd.as_deref(), Some("n2"));
}

/// `addedAffinity.preferredDuringSchedulingIgnoredDuringExecution` is added to
/// the NodeAffinity Score of every pod.
#[tokio::test]
async fn added_affinity_preferred_scores_nodes() {
    let got = schedule(ADDED_AFFINITY_DOC, "prefers-ssd", two_nodes(), json!({})).await;
    assert_eq!(got.as_deref(), Some("n2"));
}
