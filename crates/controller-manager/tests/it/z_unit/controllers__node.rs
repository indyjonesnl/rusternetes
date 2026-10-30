//! Unit tests moved from `src/controllers/node.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Duration, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::node::Taint;
    pub use rusternetes_common::resources::{
        Lease, Node, NodeCondition, NodeStatus, Pod, PodStatus,
    };
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_controller_manager::controllers::node_lifecycle_metrics;
    pub use rusternetes_controller_manager::controllers::node_lifecycle_queue::{
        RateLimitedTimedQueue, RateLimiter, TimedValue, EVICTION_RATE_LIMITER_BURST,
        NODE_EVICTION_PERIOD,
    };
    pub use rusternetes_controller_manager::controllers::node_lifecycle_zone::{
        get_zone_key, EvictionConfig, ZoneState,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::node::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_node_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = NodeController::new(storage);
    }

    fn ready_node(name: &str, status: &str) -> Node {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name},
            "spec": {},
            "status": {"conditions": [{"type": "Ready", "status": status}]}
        }))
        .unwrap()
    }

    fn pod_on(node: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"nodeName": node, "containers": [{"name": "c", "image": "i"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        }))
        .unwrap()
    }

    /// processPod (node_lifecycle_controller.go:1153-1158): a pod on a node
    /// whose Ready != True gets its Ready condition set False.
    #[tokio::test]
    async fn process_pod_marks_pod_not_ready_on_unready_node() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(
                &build_key("nodes", None, "n1"),
                &ready_node("n1", "Unknown"),
            )
            .await
            .unwrap();
        let pk = build_key("pods", Some("default"), "p");
        storage.create(&pk, &pod_on("n1")).await.unwrap();
        c.process_pod("pods/default/p").await.unwrap();
        let got: Pod = storage.get(&pk).await.unwrap();
        let ready = got
            .status
            .unwrap()
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap();
        assert_eq!(ready.status, "False");
    }

    /// Ready node: the pod is left alone (:1153 guard).
    #[tokio::test]
    async fn process_pod_leaves_pod_on_ready_node() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(&build_key("nodes", None, "n1"), &ready_node("n1", "True"))
            .await
            .unwrap();
        let pk = build_key("pods", Some("default"), "p");
        storage.create(&pk, &pod_on("n1")).await.unwrap();
        c.process_pod("pods/default/p").await.unwrap();
        let got: Pod = storage.get(&pk).await.unwrap();
        assert_eq!(got.status.unwrap().conditions.unwrap()[0].status, "True");
    }

    /// The health monitor is its own pass over all nodes (:506-512), not a
    /// per-node queue step.
    #[tokio::test]
    async fn monitor_node_health_flips_stale_node_unready() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(&build_key("nodes", None, "n1"), &ready_node("n1", "True"))
            .await
            .unwrap();
        c.seed_first_seen_for_test("n1");
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS + 1));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        let ready = got.status.unwrap().conditions.unwrap();
        assert_eq!(
            ready
                .iter()
                .find(|c| c.condition_type == "Ready")
                .unwrap()
                .status,
            "Unknown"
        );
    }

    // ------------------------------------------------------------------
    // tryUpdateNodeHealth stale-heartbeat write (#2836). Ported from
    // pkg/controller/nodelifecycle/node_lifecycle_controller_test.go
    // TestMonitorNodeHealthUpdateStatus (:1010): the case "Node created long
    // time ago, with status updated by kubelet exceeds grace period" (:1097-1190)
    // and "without status" (:1019). Impl: node_lifecycle_controller.go:937-985.
    // ------------------------------------------------------------------

    async fn monitored(
        node: &Node,
    ) -> (
        Arc<MemoryStorage>,
        Vec<NodeCondition>,
        chrono::DateTime<Utc>,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, node).await;
        let before = Utc::now();
        c.monitor_node_health().await.unwrap();
        let got: Node = storage
            .get(&build_key("nodes", None, &node.metadata.name))
            .await
            .unwrap();
        (storage, got.status.unwrap().conditions.unwrap(), before)
    }

    fn cond<'a>(cs: &'a [NodeCondition], t: &str) -> &'a NodeCondition {
        cs.iter().find(|c| c.condition_type == t).unwrap()
    }

    #[tokio::test]
    async fn stale_heartbeat_posts_ready_unknown_not_false() {
        let node = znode("n1", "z1", "True", SECS_STALE);
        let hb = node.status.as_ref().unwrap().conditions.as_ref().unwrap()[0].last_heartbeat_time;
        let (_s, cs, before) = monitored(&node).await;
        let r = cond(&cs, "Ready");
        assert_eq!(r.status, "Unknown");
        assert_eq!(r.reason.as_deref(), Some("NodeStatusUnknown"));
        assert_eq!(
            r.message.as_deref(),
            Some("Kubelet stopped posting node status.")
        );
        // The controller must not forge a heartbeat the kubelet never sent.
        assert_eq!(
            r.last_heartbeat_time.map(|t| t.timestamp()),
            hb.map(|t| t.timestamp())
        );
        assert!(r.last_transition_time.unwrap().timestamp() >= before.timestamp());
    }

    #[tokio::test]
    async fn stale_heartbeat_marks_pressure_conditions_never_updated() {
        let mut node = znode("n1", "z1", "True", SECS_STALE);
        node.metadata.creation_timestamp = Some(Utc::now() - Duration::days(1));
        let created = node.metadata.creation_timestamp;
        let (_s, cs, before) = monitored(&node).await;
        for t in ["MemoryPressure", "DiskPressure", "PIDPressure"] {
            let c = cond(&cs, t);
            assert_eq!(c.status, "Unknown", "{t}");
            assert_eq!(c.reason.as_deref(), Some("NodeStatusNeverUpdated"), "{t}");
            assert_eq!(
                c.message.as_deref(),
                Some("Kubelet never posted node status."),
                "{t}"
            );
            assert_eq!(
                c.last_heartbeat_time.map(|t| t.timestamp()),
                created.map(|t| t.timestamp()),
                "{t}"
            );
            assert!(
                c.last_transition_time.unwrap().timestamp() >= before.timestamp(),
                "{t}"
            );
        }
    }

    #[tokio::test]
    async fn stale_heartbeat_on_not_ready_node_also_goes_unknown() {
        // currentCondition.Status != Unknown => Unknown, whatever it was.
        let node = znode("n1", "z1", "False", SECS_STALE);
        let (_s, cs, _) = monitored(&node).await;
        assert_eq!(cond(&cs, "Ready").status, "Unknown");
    }

    #[tokio::test]
    async fn fresh_not_ready_condition_is_left_to_the_kubelet() {
        let node = znode("n1", "z1", "False", 1);
        let (_s, cs, _) = monitored(&node).await;
        assert_eq!(cond(&cs, "Ready").status, "False");
    }

    #[tokio::test]
    async fn node_without_status_gets_never_updated_unknown() {
        let node: Node = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "n1", "creationTimestamp": "2012-01-01T00:00:00Z"},
            "spec": {}
        }))
        .unwrap();
        let (_s, cs, _) = monitored(&node).await;
        let r = cond(&cs, "Ready");
        assert_eq!(r.status, "Unknown");
        assert_eq!(r.reason.as_deref(), Some("NodeStatusNeverUpdated"));
        assert_eq!(
            r.message.as_deref(),
            Some("Kubelet never posted node status.")
        );
    }

    // ------------------------------------------------------------------
    // nodeHealthMap (#2973). Ported from
    // pkg/controller/nodelifecycle/node_lifecycle_controller_test.go:
    // TestMonitorNodeHealthUpdateStatus (:1010),
    // TestMonitorNodeHealthUpdateNodeAndPodStatusWithLease (:1283) and
    // TestTryUpdateNodeHealth (:3532). Impl: tryUpdateNodeHealth
    // (node_lifecycle_controller.go:830-935): staleness is measured on the
    // CONTROLLER's clock from when it last saw the heartbeat/lease change
    // (`probeTimestamp`), never from the kubelet's own timestamps.
    // ------------------------------------------------------------------

    /// Create a node WITHOUT `seed_first_seen_for_test`: the controller has
    /// never seen it, like a real first observation.
    async fn put_raw(storage: &Arc<MemoryStorage>, n: &Node) {
        let key = build_key("nodes", None, &n.metadata.name);
        storage.create(&key, n).await.unwrap();
    }

    /// A Ready=True node whose kubelet clock is an hour behind ours.
    fn skewed_node(name: &str, heartbeat: chrono::DateTime<Utc>) -> Node {
        let hb = heartbeat.to_rfc3339();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "uid": format!("uid-{name}"),
                "creationTimestamp": (Utc::now() - Duration::days(1)).to_rfc3339()},
            "spec": {},
            "status": {"conditions": [{
                "type": "Ready", "status": "True",
                "lastHeartbeatTime": hb, "lastTransitionTime": hb,
            }]}
        }))
        .unwrap()
    }

    async fn ready_status(storage: &Arc<MemoryStorage>, name: &str) -> String {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        cond(&n.status.unwrap().conditions.unwrap(), "Ready")
            .status
            .clone()
    }

    async fn set_heartbeat(
        storage: &Arc<MemoryStorage>,
        name: &str,
        heartbeat: chrono::DateTime<Utc>,
    ) {
        let key = build_key("nodes", None, name);
        let mut n: Node = storage.get(&key).await.unwrap();
        let c = n
            .status
            .as_mut()
            .unwrap()
            .conditions
            .as_mut()
            .unwrap()
            .iter_mut()
            .find(|c| c.condition_type == "Ready")
            .unwrap();
        c.last_heartbeat_time = Some(heartbeat);
        storage.update_status(&key, &n).await.unwrap();
    }

    /// TestTryUpdateNodeHealth (node_lifecycle_controller_test.go:3532-3745),
    /// ported table row for row: seed `nodeHealthMap` with the node's own
    /// status and `creationTimestamp` as probe time, run `tryUpdateNodeHealth`,
    /// and require the returned current Ready status to equal the Ready status
    /// saved in the map. Upstream's `fakeNow`/`fakeOld` are `now` and a year
    /// before it, as the controller clock here is `Utc::now()` + offset.
    #[tokio::test]
    async fn try_update_node_health_table() {
        let fake_now = Utc::now();
        let fake_old = fake_now - Duration::days(365);
        // (name, creation/heartbeat time, Ready status or None for "Status nil")
        let table: [(&str, chrono::DateTime<Utc>, Option<&str>); 8] = [
            ("Status true", fake_now, Some("True")),
            ("Status false", fake_now, Some("False")),
            ("Status unknown", fake_now, Some("Unknown")),
            ("Status nil", fake_now, None),
            ("Status true - after grace period", fake_old, Some("True")),
            ("Status false - after grace period", fake_old, Some("False")),
            (
                "Status unknown - after grace period",
                fake_old,
                Some("Unknown"),
            ),
            ("Status nil - after grace period", fake_old, None),
        ];
        for (name, at, status) in table {
            let storage = Arc::new(MemoryStorage::new());
            let c = NodeController::new(storage.clone());
            let t = at.to_rfc3339();
            let conditions = match status {
                Some(s) => serde_json::json!([{
                    "type": "Ready", "status": s,
                    "lastHeartbeatTime": t, "lastTransitionTime": t,
                }]),
                None => serde_json::json!([]),
            };
            let node: Node = serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Node",
                "metadata": {"name": "node0", "creationTimestamp": t},
                "spec": {},
                "status": {"conditions": conditions}
            }))
            .unwrap();
            put_raw(&storage, &node).await;
            c.node_health.lock().unwrap().insert(
                "node0".to_string(),
                NodeHealthData {
                    probe_timestamp: at,
                    ready_transition_timestamp: at,
                    status_ready: ready_condition_of(&node).cloned(),
                    lease: None,
                },
            );
            let (_grace, _observed, current) = c
                .try_update_node_health(&node)
                .await
                .unwrap_or_else(|e| panic!("{name}: unexpected error: {e}"));
            let saved = c
                .node_health
                .lock()
                .unwrap()
                .get("node0")
                .and_then(|h| h.status_ready.clone());
            assert_eq!(
                current.as_ref().map(|c| c.status.clone()),
                saved.as_ref().map(|c| c.status.clone()),
                "{name}"
            );
        }
    }

    /// The switch of updateNodeFunc (:752-763), every (current, observed,
    /// needsRetry) combination.
    #[test]
    fn should_mark_pods_not_ready_follows_update_node_func_switch() {
        // (current, observed, needs_retry, want)
        let table = [
            ("True", "True", false, false),
            ("True", "True", true, false),
            ("False", "True", false, true),
            ("Unknown", "True", false, true),
            ("Unknown", "False", false, false),
            // a failed earlier attempt is retried while observed stays not Ready
            ("Unknown", "False", true, true),
            ("Unknown", "Unknown", true, true),
            // recovered node: nothing to mark
            ("True", "Unknown", true, true),
            ("True", "False", false, false),
        ];
        for (current, observed, retry, want) in table {
            assert_eq!(
                should_mark_pods_not_ready(current, observed, retry),
                want,
                "current={current} observed={observed} retry={retry}"
            );
        }
    }

    fn pod_ready(status: &str) -> Pod {
        let mut p = pod_on("n1");
        p.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].status = status.to_string();
        p
    }

    async fn pod_ready_status(storage: &Arc<MemoryStorage>) -> String {
        let p: Pod = storage
            .get(&build_key("pods", Some("default"), "p"))
            .await
            .unwrap();
        p.status.unwrap().conditions.unwrap()[0].status.clone()
    }

    /// updateNodeFunc (:752-763): the pass that observes Ready True -> not True
    /// marks the node's pods NotReady itself (`MarkPodsNotReady`), without
    /// waiting for the pod worker.
    #[tokio::test]
    async fn monitor_marks_pods_not_ready_on_ready_transition() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(&build_key("nodes", None, "n1"), &ready_node("n1", "True"))
            .await
            .unwrap();
        storage
            .create(&build_key("pods", Some("default"), "p"), &pod_ready("True"))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        assert_eq!(pod_ready_status(&storage).await, "True");
        c.advance_clock_for_test(Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS + 1));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "Unknown");
        assert_eq!(pod_ready_status(&storage).await, "False");
    }

    /// "Missing timestamp for Node. Assuming now as a timestamp" (:896): a
    /// heartbeat that is old by the kubelet's clock is not held against the node
    /// the first time the controller sees it.
    #[tokio::test]
    async fn first_observation_assumes_now_so_clock_skew_is_irrelevant() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let node = skewed_node("n1", Utc::now() - Duration::hours(1));
        put_raw(&storage, &node).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "True");
        // ...but it is held against it once nodeMonitorGracePeriod passes with
        // no new heartbeat observed.
        c.advance_clock_for_test(Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS + 1));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "Unknown");
    }

    /// A heartbeat VALUE that changed since last observed refreshes
    /// probeTimestamp (:906-919), however skewed its absolute time is.
    #[tokio::test]
    async fn changed_heartbeat_refreshes_probe_timestamp() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let hb = Utc::now() - Duration::hours(1);
        put_raw(&storage, &skewed_node("n1", hb)).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        set_heartbeat(&storage, "n1", hb + Duration::seconds(10)).await;
        c.monitor_node_health().await.unwrap();
        // 60s after the first look, but only 30s after the heartbeat moved.
        c.advance_clock_for_test(Duration::seconds(30));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "True");
    }

    fn lease_at(name: &str, renew: chrono::DateTime<Utc>) -> Lease {
        use rusternetes_common::resources::LeaseSpec;
        Lease {
            type_meta: TypeMeta {
                kind: "Lease".to_string(),
                api_version: "coordination.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("kube-node-lease"),
            spec: Some(LeaseSpec {
                holder_identity: Some(name.to_string()),
                lease_duration_seconds: Some(40),
                acquire_time: Some(renew),
                renew_time: Some(renew),
                lease_transitions: Some(0),
                preferred_holder: None,
                strategy: None,
            }),
        }
    }

    /// `savedLease.Spec.RenewTime.Before(observedLease.Spec.RenewTime)` (:925):
    /// a lease renewal that moved refreshes the probe; the saved lease is what
    /// is compared, not the lease's own clock.
    #[tokio::test]
    async fn lease_renewal_observed_later_refreshes_probe_timestamp() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let t0 = Utc::now() - Duration::hours(1);
        put_raw(&storage, &skewed_node("n1", t0)).await;
        let lkey = build_key("leases", Some("kube-node-lease"), "n1");
        storage.create(&lkey, &lease_at("n1", t0)).await.unwrap();
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        storage
            .update(&lkey, &lease_at("n1", t0 + Duration::seconds(10)))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "True");
    }

    /// Same lease renewTime, stale heartbeat: nothing moved, so after the grace
    /// period the node goes Unknown (:1675 "status ... exceeds grace period.
    /// Node lease is also expired").
    #[tokio::test]
    async fn unmoved_lease_does_not_refresh_probe_timestamp() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let t0 = Utc::now() - Duration::hours(1);
        put_raw(&storage, &skewed_node("n1", t0)).await;
        let lkey = build_key("leases", Some("kube-node-lease"), "n1");
        storage.create(&lkey, &lease_at("n1", t0)).await.unwrap();
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS + 1));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "Unknown");
    }

    /// `gracePeriod = nc.nodeStartupGracePeriod` (:842) for a node that never
    /// posted Ready, measured from `node.CreationTimestamp` (:843-849):
    /// "Node created recently, without status" (:1300) is left alone.
    #[tokio::test]
    async fn node_without_status_gets_startup_grace_from_creation_timestamp() {
        let created = (Utc::now() - Duration::seconds(50)).to_rfc3339();
        let node: Node = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "n1", "creationTimestamp": created},
            "spec": {}
        }))
        .unwrap();
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put_raw(&storage, &node).await;
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert!(
            got.status.and_then(|s| s.conditions).is_none(),
            "inside nodeStartupGracePeriod nothing is posted"
        );
        c.advance_clock_for_test(Duration::seconds(NODE_STARTUP_GRACE_PERIOD_SECS as i64));
        c.monitor_node_health().await.unwrap();
        assert_eq!(ready_status(&storage, "n1").await, "Unknown");
    }

    /// `updateNodeHealthDuration` / `updateAllNodesHealthDuration`
    /// (metrics.go:75-92) are observed once per node / once per pass.
    #[tokio::test]
    async fn health_duration_histograms_are_observed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put_raw(&storage, &skewed_node("n1", Utc::now())).await;
        let (one, all) = (
            node_lifecycle_metrics::update_node_health_count(),
            node_lifecycle_metrics::update_all_nodes_health_count(),
        );
        c.monitor_node_health().await.unwrap();
        assert!(node_lifecycle_metrics::update_node_health_count() > one);
        assert!(node_lifecycle_metrics::update_all_nodes_health_count() > all);
        assert!(node_lifecycle_metrics::gather()
            .contains("node_collector_update_node_health_duration_seconds_bucket"));
    }

    /// #2974: the kubelet renews its own Lease (pkg/kubelet/nodelease). The
    /// controller must not, or a dead kubelet looks alive forever.
    #[tokio::test]
    async fn controller_does_not_renew_the_node_lease() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let t0 = (Utc::now() - Duration::seconds(300)).timestamp();
        let t0 = chrono::DateTime::<Utc>::from_timestamp(t0, 0).unwrap();
        put_raw(&storage, &skewed_node("n1", t0)).await;
        let lkey = build_key("leases", Some("kube-node-lease"), "n1");
        storage.create(&lkey, &lease_at("n1", t0)).await.unwrap();
        c.seed_first_seen_for_test("n1");
        c.reconcile_all().await.unwrap();
        let l: Lease = storage.get(&lkey).await.unwrap();
        assert_eq!(l.spec.unwrap().renew_time, Some(t0));
    }

    // ------------------------------------------------------------------
    // Zone-aware rate-limited NoExecute tainting (#2627). Ported from
    // pkg/controller/nodelifecycle/node_lifecycle_controller_test.go:
    // TestApplyNoExecuteTaints (:2289), TestApplyNoExecuteTaintsToNodesEnqueueTwice
    // (:2712), TestSwapUnreachableNotReadyTaints (:2948). Upstream's
    // newNodeLifecycleControllerFromClient passes `testRateLimiterQPS =
    // 100000` for both limiters (:58); `fast_config` does the same.
    // ------------------------------------------------------------------

    const SECS_STALE: i64 = 3600;

    fn fast_config() -> EvictionConfig {
        EvictionConfig {
            eviction_limiter_qps: 100_000.0,
            secondary_eviction_limiter_qps: 100_000.0,
            large_cluster_threshold: 50,
            unhealthy_zone_threshold: 0.55,
        }
    }

    /// A node in `zone1` with the given Ready status and heartbeat age.
    fn znode(name: &str, zone: &str, status: &str, heartbeat_age_secs: i64) -> Node {
        let hb = (Utc::now() - Duration::seconds(heartbeat_age_secs)).to_rfc3339();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "uid": format!("uid-{name}"), "labels": {
                "topology.kubernetes.io/region": "region1",
                "topology.kubernetes.io/zone": zone,
                "failure-domain.beta.kubernetes.io/region": "region1",
                "failure-domain.beta.kubernetes.io/zone": zone,
            }},
            "spec": {},
            "status": {"conditions": [{
                "type": "Ready", "status": status,
                "lastHeartbeatTime": hb, "lastTransitionTime": hb,
            }]}
        }))
        .unwrap()
    }

    async fn put(storage: &Arc<MemoryStorage>, c: &NodeController<MemoryStorage>, n: &Node) {
        let key = build_key("nodes", None, &n.metadata.name);
        if storage.get::<Node>(&key).await.is_ok() {
            storage.update(&key, n).await.unwrap();
        } else {
            storage.create(&key, n).await.unwrap();
        }
        c.seed_first_seen_for_test(&n.metadata.name);
    }

    async fn taint_keys(storage: &Arc<MemoryStorage>, name: &str) -> Vec<(String, String)> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec
            .and_then(|s| s.taints)
            .unwrap_or_default()
            .into_iter()
            .map(|t| (t.key, t.effect))
            .collect()
    }

    fn noexec(key: &str) -> (String, String) {
        (key.to_string(), "NoExecute".to_string())
    }

    /// TestApplyNoExecuteTaints: Unknown -> unreachable, False -> not-ready,
    /// Ready -> no taint, all NoExecute with `timeAdded` stamped.
    #[tokio::test]
    async fn apply_no_execute_taints() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        // "we need second healthy node in tests" (all-NotReady stops evictions).
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("node2", "zone1", "False", 0)).await;

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;

        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/unreachable")]
        );
        assert!(taint_keys(&storage, "node1").await.is_empty());
        assert_eq!(
            taint_keys(&storage, "node2").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
        let n0: Node = storage
            .get(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        assert!(n0.spec.unwrap().taints.unwrap()[0].time_added.is_some());
    }

    /// TestApplyNoExecuteTaintsToNodesEnqueueTwice: a node queued twice, then
    /// healthy again, must not wedge the queue ("the taint job never stuck").
    #[tokio::test]
    async fn apply_no_execute_taints_to_nodes_enqueue_twice() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;

        // 1. monitor node health twice, add untainted node once
        c.monitor_node_health().await.unwrap();
        c.monitor_node_health().await.unwrap();

        // 2. mark node0 healthy
        put(&storage, &c, &znode("node0", "zone1", "True", 0)).await;
        // add other notReady nodes
        put(
            &storage,
            &c,
            &znode("node3", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node4", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("node5", "zone1", "False", 0)).await;

        // 3. monitor again
        c.monitor_node_health().await.unwrap();
        // 4. do NoExecute taint pass
        c.do_no_execute_tainting_pass().await;

        assert!(taint_keys(&storage, "node0").await.is_empty());
        assert_eq!(
            taint_keys(&storage, "node3").await,
            [noexec("node.kubernetes.io/unreachable")]
        );
        assert_eq!(
            taint_keys(&storage, "node5").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
    }

    /// TestSwapUnreachableNotReadyTaints: Unknown -> unreachable; when the
    /// condition turns False the unreachable taint is swapped for not-ready
    /// "straight away" (processTaintBaseEviction :783-786).
    #[tokio::test]
    async fn swap_unreachable_not_ready_taints() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/unreachable")]
        );

        // node0 now reports NotReady (False); keep its taint, swap the status.
        let mut n0: Node = storage
            .get(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        n0.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].status = "False".into();
        n0.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_heartbeat_time =
            Some(Utc::now());
        storage
            .update(&build_key("nodes", None, "node0"), &n0)
            .await
            .unwrap();

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
    }

    async fn zone_qps(c: &NodeController<MemoryStorage>, zone: &str) -> f32 {
        let q = c
            .evictor
            .lock()
            .await
            .zone_no_execute_tainter
            .get(&format!("region1:\0:{zone}"))
            .cloned()
            .expect("zone queue");
        q.limiter_qps().await
    }

    /// handleDisruption (:996-1085): PartialDisruption in a large cluster uses
    /// the secondary rate; recovery returns to the primary rate.
    #[tokio::test]
    async fn partial_disruption_in_large_cluster_uses_secondary_qps_then_recovers() {
        let storage = Arc::new(MemoryStorage::new());
        let cfg = EvictionConfig {
            large_cluster_threshold: 3,
            ..EvictionConfig::default()
        };
        let c = NodeController::with_eviction_config(storage.clone(), cfg);
        put(&storage, &c, &znode("ok", "zone1", "True", 0)).await;
        for n in ["b1", "b2", "b3", "b4"] {
            put(&storage, &c, &znode(n, "zone1", "False", 0)).await;
        }
        c.monitor_node_health().await.unwrap();
        // 5 nodes > threshold 3, 4/5 unhealthy: ReducedQPSFunc -> secondary.
        assert_eq!(zone_qps(&c, "zone1").await, 0.01);

        for n in ["b1", "b2", "b3", "b4"] {
            put(&storage, &c, &znode(n, "zone1", "True", 0)).await;
        }
        c.monitor_node_health().await.unwrap();
        assert_eq!(zone_qps(&c, "zone1").await, 0.1);
    }

    /// Entering master disruption removes taints and stops evictions
    /// (:1039-1056); exiting it restores per-zone limiters and restarts the
    /// nodes' grace clocks (:1058-1074).
    #[tokio::test]
    async fn master_disruption_clears_taints_and_stops_then_resumes() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(
            storage.clone(),
            EvictionConfig {
                eviction_limiter_qps: 100_000.0,
                ..EvictionConfig::default()
            },
        );
        put(&storage, &c, &znode("a", "zone1", "False", 0)).await;
        put(&storage, &c, &znode("b", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "a").await,
            [noexec("node.kubernetes.io/not-ready")]
        );

        // b stops posting too: its heartbeat does not move for longer than
        // nodeMonitorGracePeriod on the controller's clock, so every node is
        // NotReady -> master disruption.
        c.advance_clock_for_test(Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS + 1));
        // (a keeps posting its Ready=False, so it stays NotReady, not Unknown.)
        set_heartbeat(&storage, "a", Utc::now() + Duration::seconds(10)).await;
        c.monitor_node_health().await.unwrap();
        assert!(
            taint_keys(&storage, "a").await.is_empty(),
            "entering master disruption removes the taints"
        );
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::FullDisruption]
        );
        c.do_no_execute_tainting_pass().await;
        assert!(taint_keys(&storage, "a").await.is_empty());
        assert_ne!(zone_qps(&c, "zone1").await, 100_000.0);

        // b recovers: its kubelet posts a new heartbeat (a distinct value, as the
        // stored one has one-second resolution); exiting full disruption resets
        // the limiter for the zone.
        put(&storage, &c, &znode("b", "zone1", "True", -20)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::Normal]
        );
        assert_eq!(zone_qps(&c, "zone1").await, 100_000.0);
    }

    /// A zone in FullDisruption while another zone still responds keeps the
    /// normal rate (`enterFullDisruptionFunc = HealthyQPSFunc`, :358); the
    /// healthy zone is unaffected.
    #[tokio::test]
    async fn single_zone_full_disruption_keeps_normal_rate() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("z1a", "z1", "False", 0)).await;
        put(&storage, &c, &znode("z1b", "z1", "False", 0)).await;
        put(&storage, &c, &znode("z2a", "z2", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(zone_qps(&c, "z1").await, 0.1);
        assert_eq!(zone_qps(&c, "z2").await, 0.1);
        let states = c.evictor.lock().await.zone_states.clone();
        assert_eq!(states["region1:\0:z1"], ZoneState::FullDisruption);
        assert_eq!(states["region1:\0:z2"], ZoneState::Normal);
    }

    /// Nodes labelled `node.kubernetes.io/exclude-disruption` are excluded
    /// from disruption checks (isNodeExcludedFromDisruptionChecks, :817-825).
    #[tokio::test]
    async fn excluded_nodes_do_not_count_towards_disruption() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        let mut ex = znode("ex", "zone1", "False", 0);
        ex.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_NODE_DISRUPTION_EXCLUSION.to_string(), String::new());
        put(&storage, &c, &ex).await;
        put(&storage, &c, &znode("ok", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::Normal]
        );
    }

    fn metric_zone(zone: &str) -> String {
        get_zone_key(&znode("x", zone, "True", 0))
    }

    /// `handleDisruption` (:1001-1004) sets zoneSize / zoneHealth /
    /// unhealthyNodes per zone; `addPodEvictorForNewZone` (:1235) seeds
    /// `evictions_total` at 0. Upstream has no test for these series; the
    /// expected values follow its formulas verbatim.
    #[tokio::test]
    async fn monitor_publishes_zone_metrics() {
        use rusternetes_controller_manager::controllers::node_lifecycle_metrics as m;
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(&storage, &c, &znode("m0", "metrics-zone-a", "True", 0)).await;
        put(&storage, &c, &znode("m1", "metrics-zone-a", "True", 0)).await;
        put(&storage, &c, &znode("m2", "metrics-zone-a", "True", 0)).await;
        put(
            &storage,
            &c,
            &znode("m3", "metrics-zone-a", "False", SECS_STALE),
        )
        .await;
        c.monitor_node_health().await.unwrap();

        let z = metric_zone("metrics-zone-a");
        assert_eq!(m::zone_size(&z), Some(4.0));
        assert_eq!(m::unhealthy_nodes(&z), Some(1.0));
        assert_eq!(m::zone_health(&z), Some(75.0));
        assert_eq!(m::evictions_total(&z), 0);
        assert!(m::gather().contains("node_collector_evictions_total"));
    }

    /// `doNoExecuteTaintingPass` (:655-658): a successful taint swap counts
    /// one eviction for the node's zone.
    #[tokio::test]
    async fn tainting_pass_counts_evictions() {
        use rusternetes_controller_manager::controllers::node_lifecycle_metrics as m;
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("e0", "metrics-zone-b", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("e1", "metrics-zone-b", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(m::evictions_total(&metric_zone("metrics-zone-b")), 1);
    }

    /// `handleDisruption` (:1016-1021): a zone with no nodes left reads
    /// size 0, health 100, unhealthy 0.
    #[tokio::test]
    async fn emptied_zone_metrics_reset() {
        use rusternetes_controller_manager::controllers::node_lifecycle_metrics as m;
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("r0", "metrics-zone-c", "False", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("r1", "metrics-zone-c", "True", 0)).await;
        put(&storage, &c, &znode("r2", "metrics-zone-d", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        let z = metric_zone("metrics-zone-c");
        assert_eq!(m::zone_size(&z), Some(2.0));

        storage
            .delete(&build_key("nodes", None, "r0"))
            .await
            .unwrap();
        storage
            .delete(&build_key("nodes", None, "r1"))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        assert_eq!(m::zone_size(&z), Some(0.0));
        assert_eq!(m::zone_health(&z), Some(100.0));
        assert_eq!(m::unhealthy_nodes(&z), Some(0.0));
    }

    fn noschedule(key: &str) -> (String, String) {
        (key.to_string(), "NoSchedule".to_string())
    }

    /// TestNoScheduleTaintingPass shape (node_lifecycle_controller_test.go
    /// TestTaintNodeByCondition): a real api-server's NodeTaint admission
    /// stamps `not-ready:NoSchedule` at registration; once the node reports
    /// Ready=True the per-node pass must remove it (#3003).
    #[tokio::test]
    async fn no_schedule_not_ready_taint_cleared_when_ready() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        let mut n = znode("node0", "zone1", "True", 0);
        n.spec = serde_json::from_value(serde_json::json!({
            "taints": [
                {"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"},
                {"key": "example.com/user", "effect": "NoSchedule"}
            ]
        }))
        .unwrap();
        put(&storage, &c, &n).await;
        c.reconcile_all().await.unwrap();
        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noschedule("example.com/user")]
        );
    }

    /// Ready=False -> not-ready:NoSchedule; Ready=Unknown -> unreachable;
    /// spec.unschedulable -> unschedulable; pressure condition True -> its taint.
    #[tokio::test]
    async fn no_schedule_taints_follow_conditions() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(&storage, &c, &znode("ok", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("nr", "zone1", "False", 0)).await;
        let mut cordoned = znode("cordon", "zone1", "True", 0);
        cordoned.spec = serde_json::from_value(serde_json::json!({"unschedulable": true})).unwrap();
        put(&storage, &c, &cordoned).await;
        for n in ["nr", "cordon"] {
            let node: Node = storage.get(&build_key("nodes", None, n)).await.unwrap();
            c.process_node(&node).await.unwrap();
        }
        assert_eq!(
            taint_keys(&storage, "nr").await,
            [noschedule("node.kubernetes.io/not-ready")]
        );
        assert_eq!(
            taint_keys(&storage, "cordon").await,
            [noschedule("node.kubernetes.io/unschedulable")]
        );
        assert!(taint_keys(&storage, "ok").await.is_empty());
    }

    /// #3009: upstream's `doNodeProcessingPassWorker`
    /// (node_lifecycle_controller.go:516-540) has no startup-grace gate of its
    /// own after `doNoScheduleTaintingPass`; the only grace is the nodeHealthMap
    /// one inside `monitorNodeHealth`. A freshly observed node reporting
    /// `Ready=False reason=NodeShutdown` must therefore get its shutdown taint
    /// on the first pass, without `seed_first_seen_for_test`.
    #[tokio::test]
    async fn process_node_has_no_second_startup_grace_gate() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        let mut n = znode("fresh", "zone1", "False", 0);
        n.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].reason =
            Some("NodeShutdown".to_string());
        // Not `put`: that backdates the node's health-map entry.
        put_raw(&storage, &n).await;
        c.process_node(&n).await.unwrap();
        assert!(
            taint_keys(&storage, "fresh")
                .await
                .contains(&noschedule("node.kubernetes.io/shutdown")),
            "shutdown taint withheld by a startup grace upstream does not have"
        );
    }
}
