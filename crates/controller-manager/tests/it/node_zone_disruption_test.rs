//! Zone-aware rate-limited NoExecute tainting (#2627).
//!
//! Upstream: `pkg/controller/nodelifecycle/node_lifecycle_controller.go`
//! `doNoExecuteTaintingPass` (:595) drains one `RateLimitedTimedQueue` per zone
//! (`zoneNoExecuteTainter`); `handleDisruption` (:996) and `setLimiterInZone`
//! (:1161) retune each zone's limiter from `ComputeZoneState` (:1281).
//! Defaults: evictionLimiterQPS 0.1, secondaryEvictionLimiterQPS 0.01,
//! largeClusterThreshold 50, unhealthyZoneThreshold 0.55
//! (`pkg/controller/apis/config/v1alpha1/defaults.go`).

use chrono::{Duration, Utc};
use rusternetes_common::resources::{Node, NodeCondition, NodeStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::node::NodeController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn zoned_node(name: &str, zone: &str, ready_fresh: bool) -> Node {
    let heartbeat = if ready_fresh {
        Utc::now()
    } else {
        Utc::now() - Duration::seconds(120)
    };
    let mut labels = HashMap::new();
    labels.insert(
        "topology.kubernetes.io/region".to_string(),
        "r1".to_string(),
    );
    labels.insert("topology.kubernetes.io/zone".to_string(), zone.to_string());
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: Some(labels),
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(heartbeat),
                last_transition_time: Some(heartbeat),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is posting ready status".to_string()),
            }]),
            addresses: None,
            capacity: None,
            allocatable: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        }),
    }
}

async fn count_not_ready_tainted(storage: &MemoryStorage, names: &[&str]) -> usize {
    let mut n = 0;
    for name in names {
        let node: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        let tainted = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| {
                ts.iter()
                    .any(|t| t.key == "node.kubernetes.io/unreachable" && t.effect == "NoExecute")
            })
            .unwrap_or(false);
        if tainted {
            n += 1;
        }
    }
    n
}

async fn seed(storage: &Arc<MemoryStorage>, c: &NodeController<MemoryStorage>, nodes: &[Node]) {
    for node in nodes {
        storage
            .create(&build_key("nodes", None, &node.metadata.name), node)
            .await
            .unwrap();
        c.seed_first_seen_for_test(&node.metadata.name);
    }
}

/// Normal zone (3 of 6 not ready = 0.5 < 0.55): the zone's limiter admits
/// `EvictionRateLimiterBurst = 1` taint per pass, not all three at once.
#[tokio::test]
async fn test_zone_normal_state_rate_limits_noexecute_taints() {
    let storage = Arc::new(MemoryStorage::new());
    let c = NodeController::new(storage.clone());
    let nodes = [
        zoned_node("bad-1", "z1", false),
        zoned_node("bad-2", "z1", false),
        zoned_node("bad-3", "z1", false),
        zoned_node("ok-1", "z1", true),
        zoned_node("ok-2", "z1", true),
        zoned_node("ok-3", "z1", true),
    ];
    seed(&storage, &c, &nodes).await;

    c.reconcile_all().await.unwrap();

    let tainted = count_not_ready_tainted(&storage, &["bad-1", "bad-2", "bad-3"]).await;
    assert_eq!(
        tainted, 1,
        "one pass at 0.1 QPS / burst 1 may taint exactly one node; a partition must throttle"
    );
    assert_eq!(
        count_not_ready_tainted(&storage, &["ok-1", "ok-2", "ok-3"]).await,
        0
    );
}

/// FullDisruption of every zone: "Controller detected that all Nodes are
/// not-Ready. Entering master disruption mode" (:1039) stops all evictions.
#[tokio::test]
async fn test_all_nodes_not_ready_enters_master_disruption_no_taints() {
    let storage = Arc::new(MemoryStorage::new());
    let c = NodeController::new(storage.clone());
    let nodes = [
        zoned_node("bad-1", "z1", false),
        zoned_node("bad-2", "z1", false),
    ];
    seed(&storage, &c, &nodes).await;

    c.reconcile_all().await.unwrap();

    assert_eq!(
        count_not_ready_tainted(&storage, &["bad-1", "bad-2"]).await,
        0,
        "no node may be tainted when every node is NotReady"
    );
}

/// PartialDisruption in a small cluster (<= largeClusterThreshold):
/// `ReducedQPSFunc` (:1216) returns 0, evictions stop.
#[tokio::test]
async fn test_partial_disruption_small_cluster_stops_taints() {
    let storage = Arc::new(MemoryStorage::new());
    let c = NodeController::new(storage.clone());
    let nodes = [
        zoned_node("bad-1", "z1", false),
        zoned_node("bad-2", "z1", false),
        zoned_node("bad-3", "z1", false),
        zoned_node("bad-4", "z1", false),
        zoned_node("ok-1", "z1", true),
    ];
    seed(&storage, &c, &nodes).await;

    c.reconcile_all().await.unwrap();

    assert_eq!(
        count_not_ready_tainted(&storage, &["bad-1", "bad-2", "bad-3", "bad-4"]).await,
        0,
        "4/5 unhealthy in a small zone is PartialDisruption: QPS 0, no taints"
    );
}
