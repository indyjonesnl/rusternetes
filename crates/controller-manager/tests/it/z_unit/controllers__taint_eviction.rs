//! Unit tests moved from `src/controllers/taint_eviction.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::{
        EventSource, EventType, Node, Pod, PodCondition, Taint, Toleration,
    };
    pub use rusternetes_controller_manager::controllers::taint_eviction_timed_workers::{
        TimedWorkerQueue, WorkArgs,
    };
    pub use rusternetes_storage::{
        build_key, build_prefix, EventRecorder, Storage, WatchEvent, WorkQueue,
    };
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tokio::sync::mpsc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    //! Cases ported from `taint_eviction_test.go` (kubernetes release-1.35).
    //! The fake clientset's "patch"/"delete" actions become: the pod gaining a
    //! `DisruptionTarget` condition (status patch) and being deleted.
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::taint_eviction::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    fn pod(name: &str, node: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default", "uid": format!("uid-{name}")},
            "spec": {"nodeName": node, "containers": [{"name": "c", "image": "i"}]},
            "status": {"phase": "Running"}
        }))
        .unwrap()
    }

    /// `createNoExecuteTaint`.
    fn taint(i: usize) -> Taint {
        Taint {
            key: format!("testTaint{i}"),
            value: Some(format!("test{i}")),
            effect: "NoExecute".to_string(),
            time_added: Some(Utc::now()),
        }
    }

    fn toleration(i: usize, seconds: Option<i64>) -> Toleration {
        Toleration {
            key: Some(format!("testTaint{i}")),
            operator: None,
            value: Some(format!("test{i}")),
            effect: Some("NoExecute".to_string()),
            toleration_seconds: seconds,
        }
    }

    /// `addToleration`: duration < 0 means no tolerationSeconds.
    fn add_toleration(mut p: Pod, i: usize, duration: i64) -> Pod {
        p.spec.as_mut().unwrap().tolerations =
            Some(vec![toleration(i, (duration >= 0).then_some(duration))]);
        p
    }

    fn node(name: &str, taints: &[usize]) -> Node {
        let mut n: Node = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name}, "spec": {}
        }))
        .unwrap();
        n.spec.as_mut().unwrap().taints =
            Some(taints.iter().map(|i| taint(*i)).collect::<Vec<_>>()).filter(|t| !t.is_empty());
        n
    }

    struct Fixture {
        storage: Arc<MemoryStorage>,
        ctl: Arc<TaintEvictionController<MemoryStorage>>,
    }

    impl Fixture {
        async fn new(pods: Vec<Pod>) -> Self {
            let storage = Arc::new(MemoryStorage::new());
            for p in &pods {
                let key = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
                storage.create(&key, p).await.unwrap();
            }
            let ctl = Arc::new(TaintEvictionController::new(Arc::clone(&storage)));
            ctl.start_workers();
            Self { storage, ctl }
        }

        async fn put_node(&self, n: &Node) {
            let key = build_key("nodes", None, &n.metadata.name);
            if self.storage.update(&key, n).await.is_err() {
                self.storage.create(&key, n).await.unwrap();
            }
        }

        /// (patched, deleted) for a pod, `verifyPodActions`' two booleans.
        async fn actions(&self, name: &str) -> (bool, bool) {
            let key = build_key("pods", Some("default"), name);
            match self.storage.get::<Pod>(&key).await {
                Err(_) => (true, true),
                Ok(p) => (
                    p.status
                        .and_then(|s| s.conditions)
                        .is_some_and(|c| c.iter().any(|c| c.condition_type == "DisruptionTarget")),
                    p.metadata.deletion_timestamp.is_some(),
                ),
            }
        }

        /// `verifyPodActions`: poll until the expected outcome (or 5s).
        async fn verify(&self, name: &str, patch: bool, delete: bool) {
            for _ in 0..500 {
                if self.actions(name).await == (patch, delete) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(self.actions(name).await, (patch, delete), "pod {name}");
        }

        /// Give the controller time to (not) act, then assert.
        async fn verify_stays(&self, name: &str, patch: bool, delete: bool) {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(self.actions(name).await, (patch, delete), "pod {name}");
        }

        fn set_tainted(&self, node: &str, taints: Vec<Taint>) {
            self.ctl
                .tainted_nodes
                .lock()
                .unwrap()
                .insert(node.to_string(), taints);
        }

        fn queued(&self, name: &str) -> bool {
            self.ctl
                .taint_eviction_queue
                .get_worker_unsafe(&format!("default/{name}"))
                .is_some()
        }
    }

    // ---- helpers ---------------------------------------------------------

    #[test]
    fn hash_is_fnv1a_32_mod_max() {
        // FNV-1a 32 of "" is the offset basis 0x811c9dc5 = 2166136261.
        assert_eq!(hash("", 8), (2166136261u32 % 8) as usize);
        // Known FNV-1a 32 vector: "a" -> 0xe40c292c.
        assert_eq!(hash("a", 1 << 16), (0xe40c292cu32 % (1 << 16)) as usize);
        for n in ["node1", "node-2", "x"] {
            assert!(hash(n, UPDATE_WORKER_SIZE) < UPDATE_WORKER_SIZE);
            assert_eq!(hash(n, 8), hash(n, 8), "stable");
        }
    }

    /// `TestFilterNoExecuteTaints`.
    #[test]
    fn filter_no_execute_taints() {
        let taints = vec![
            Taint {
                key: "one".into(),
                value: Some("one".into()),
                effect: "NoExecute".into(),
                time_added: None,
            },
            Taint {
                key: "two".into(),
                value: Some("two".into()),
                effect: "NoSchedule".into(),
                time_added: None,
            },
        ];
        let got = get_no_execute_taints(&taints);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].effect, "NoExecute");
    }

    /// `TestGetMinTolerationTime`.
    #[test]
    fn get_min_toleration_time_cases() {
        let t = |s: Option<i64>| Toleration {
            key: None,
            operator: None,
            value: None,
            effect: None,
            toleration_seconds: s,
        };
        let one = Duration::from_secs(1);
        assert_eq!(
            get_min_toleration_time(&[]),
            MinTolerationTime::After(Duration::ZERO)
        );
        assert_eq!(
            get_min_toleration_time(&[t(None)]),
            MinTolerationTime::Forever
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(1)), t(Some(2))]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(1)), t(None)]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(None), t(Some(1))]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(0))]),
            MinTolerationTime::After(Duration::ZERO)
        );
    }

    // ---- pods ------------------------------------------------------------

    /// `TestCreatePod`.
    #[tokio::test]
    async fn create_pod() {
        struct Case {
            description: &'static str,
            pod: Pod,
            tainted: Vec<(&'static str, Vec<Taint>)>,
            patch: bool,
            delete: bool,
        }
        let cases = vec![
            Case {
                description: "not scheduled - ignore",
                pod: pod("pod1", ""),
                tainted: vec![],
                patch: false,
                delete: false,
            },
            Case {
                description: "scheduled on untainted Node",
                pod: pod("pod1", "node1"),
                tainted: vec![],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node",
                pod: pod("pod1", "node1"),
                tainted: vec![("node1", vec![taint(1)])],
                patch: true,
                delete: true,
            },
            Case {
                description: "schedule on tainted Node with finite toleration",
                pod: add_toleration(pod("pod1", "node1"), 1, 100),
                tainted: vec![("node1", vec![taint(1)])],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node with infinite toleration",
                pod: add_toleration(pod("pod1", "node1"), 1, -1),
                tainted: vec![("node1", vec![taint(1)])],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node with infinite invalid toleration",
                pod: add_toleration(pod("pod1", "node1"), 2, -1),
                tainted: vec![("node1", vec![taint(1)])],
                patch: true,
                delete: true,
            },
        ];
        for c in cases {
            let f = Fixture::new(vec![c.pod.clone()]).await;
            for (n, t) in c.tainted {
                f.set_tainted(n, t);
            }
            f.ctl.pod_updated(None, Some(&c.pod)).await;
            if c.delete {
                f.verify("pod1", c.patch, c.delete).await;
            } else {
                f.verify_stays("pod1", c.patch, c.delete).await;
            }
            eprintln!("ok: {}", c.description);
            f.ctl.shutdown().await;
        }
    }

    /// `TestDeletePod`: a deleted pod must not panic.
    #[tokio::test]
    async fn delete_pod() {
        let f = Fixture::new(vec![]).await;
        f.set_tainted("node1", vec![taint(1)]);
        f.ctl.pod_updated(Some(&pod("pod1", "node1")), None).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        f.ctl.shutdown().await;
    }

    /// `TestUpdatePod`.
    #[tokio::test]
    async fn update_pod() {
        struct Case {
            description: &'static str,
            prev: Pod,
            await_scheduled: bool,
            new: Pod,
            patch: bool,
            delete: bool,
        }
        let cases = vec![
            Case {
                description: "scheduling onto tainted Node",
                prev: pod("pod1", ""),
                await_scheduled: false,
                new: pod("pod1", "node1"),
                patch: true,
                delete: true,
            },
            Case {
                description: "scheduling onto tainted Node with toleration",
                prev: add_toleration(pod("pod1", ""), 1, -1),
                await_scheduled: false,
                new: add_toleration(pod("pod1", "node1"), 1, -1),
                patch: false,
                delete: false,
            },
            Case {
                description: "removing toleration",
                prev: add_toleration(pod("pod1", "node1"), 1, 100),
                await_scheduled: true,
                new: pod("pod1", "node1"),
                patch: true,
                delete: true,
            },
            Case {
                description: "lengthening toleration shouldn't work",
                prev: add_toleration(pod("pod1", "node1"), 1, 1),
                await_scheduled: true,
                new: add_toleration(pod("pod1", "node1"), 1, 100),
                patch: true,
                delete: true,
            },
        ];
        for c in cases {
            let f = Fixture::new(vec![c.prev.clone()]).await;
            f.set_tainted("node1", vec![taint(1)]);
            f.ctl.pod_updated(None, Some(&c.prev)).await;
            if c.await_scheduled {
                for _ in 0..100 {
                    if f.queued("pod1") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(
                    f.queued("pod1"),
                    "{}: eviction not scheduled",
                    c.description
                );
            }
            // The informer's podIndexer.Update(newPod).
            let key = build_key("pods", Some("default"), "pod1");
            f.storage.update(&key, &c.new).await.unwrap();
            f.ctl.pod_updated(Some(&c.prev), Some(&c.new)).await;
            if c.delete {
                f.verify("pod1", c.patch, c.delete).await;
            } else {
                f.verify_stays("pod1", c.patch, c.delete).await;
            }
            eprintln!("ok: {}", c.description);
            f.ctl.shutdown().await;
        }
    }

    // ---- nodes -----------------------------------------------------------

    /// `TestCreateNode`.
    #[tokio::test]
    async fn create_node() {
        let cases: Vec<(&str, Pod, Node, bool)> = vec![
            (
                "Creating Node matching already assigned Pod",
                pod("pod1", "node1"),
                node("node1", &[]),
                false,
            ),
            (
                "Creating tainted Node matching already assigned Pod",
                pod("pod1", "node1"),
                node("node1", &[1]),
                true,
            ),
            (
                "Creating tainted Node matching already assigned tolerating Pod",
                add_toleration(pod("pod1", "node1"), 1, -1),
                node("node1", &[1]),
                false,
            ),
        ];
        for (description, p, n, evicted) in cases {
            let f = Fixture::new(vec![p]).await;
            f.put_node(&n).await;
            f.ctl.node_updated(None, Some(&n)).await;
            if evicted {
                f.verify("pod1", true, true).await;
            } else {
                f.verify_stays("pod1", false, false).await;
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// #3261: terminal pods get no special treatment. Upstream
    /// `taint_eviction.go` never reads `pod.Status.Phase`
    /// (`grep -n "Phase\|IsPodTerminal" pkg/controller/tainteviction/taint_eviction.go`
    /// has no match), so an untolerated Succeeded/Failed pod on a NoExecute
    /// tainted node is evicted like any other (the kubelet used to skip them).
    #[tokio::test]
    async fn terminal_pods_are_evicted_like_any_other() {
        for phase in ["Succeeded", "Failed"] {
            let mut p = pod("pod1", "node1");
            p.status.as_mut().unwrap().phase = Some(match phase {
                "Succeeded" => rusternetes_common::types::Phase::Succeeded,
                _ => rusternetes_common::types::Phase::Failed,
            });
            let f = Fixture::new(vec![p]).await;
            let n = node("node1", &[1]);
            f.put_node(&n).await;
            f.ctl.node_updated(None, Some(&n)).await;
            f.verify("pod1", true, true).await;
            eprintln!("ok: {phase}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestDeleteNode`.
    #[tokio::test]
    async fn delete_node() {
        let f = Fixture::new(vec![]).await;
        f.set_tainted("node1", vec![taint(1)]);
        f.ctl.node_updated(Some(&node("node1", &[])), None).await;
        for _ in 0..100 {
            if !f.ctl.tainted_nodes.lock().unwrap().contains_key("node1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !f.ctl.tainted_nodes.lock().unwrap().contains_key("node1"),
            "Failed to await for processing node deleted"
        );
        f.ctl.shutdown().await;
    }

    /// `TestUpdateNode`.
    #[tokio::test]
    async fn update_node() {
        let multi = {
            let mut p = pod("pod1", "node1");
            p.spec.as_mut().unwrap().tolerations =
                Some(vec![toleration(1, Some(1)), toleration(2, Some(100))]);
            p
        };
        // (description, pod, old, new, patch, delete, extra sleep ms)
        let cases: Vec<(&str, Pod, Node, Node, bool, bool, u64)> = vec![
            (
                "Added taint, expect node patched and deleted",
                pod("pod1", "node1"),
                node("node1", &[]),
                node("node1", &[1]),
                true,
                true,
                0,
            ),
            (
                "Added tolerated taint",
                add_toleration(pod("pod1", "node1"), 1, 100),
                node("node1", &[]),
                node("node1", &[1]),
                false,
                false,
                0,
            ),
            (
                "Only one added taint tolerated",
                add_toleration(pod("pod1", "node1"), 1, 100),
                node("node1", &[]),
                node("node1", &[1, 2]),
                true,
                true,
                0,
            ),
            (
                "Taint removed",
                add_toleration(pod("pod1", "node1"), 1, 1),
                node("node1", &[1]),
                node("node1", &[]),
                false,
                false,
                1500,
            ),
            (
                "Pod with multiple tolerations are evicted when first one runs out",
                multi,
                node("node1", &[]),
                node("node1", &[1, 2]),
                true,
                true,
                0,
            ),
        ];
        for (description, p, old, new, patch, delete, extra) in cases {
            let f = Fixture::new(vec![p]).await;
            f.put_node(&new).await;
            f.ctl.node_updated(Some(&old), Some(&new)).await;
            if extra > 0 {
                tokio::time::sleep(Duration::from_millis(extra)).await;
            }
            if delete {
                f.verify("pod1", patch, delete).await;
            } else {
                f.verify_stays("pod1", patch, delete).await;
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestUpdateNodeWithMultipleTaints`.
    #[tokio::test]
    async fn update_node_with_multiple_taints() {
        let mut p = pod("pod1", "node1");
        let exists = |i: usize, secs: Option<i64>| Toleration {
            key: Some(format!("testTaint{i}")),
            operator: Some("Exists".into()),
            value: None,
            effect: Some("NoExecute".into()),
            toleration_seconds: secs,
        };
        p.spec.as_mut().unwrap().tolerations = Some(vec![exists(1, None), exists(2, Some(60))]);
        let f = Fixture::new(vec![p]).await;

        // no taint
        f.put_node(&node("node1", &[])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(!f.queued("pod1"), "pod queued for deletion with no taints");

        // no taint -> infinitely tolerated taint
        f.put_node(&node("node1", &[1])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            !f.queued("pod1"),
            "pod queued for deletion with permanently tolerated taint"
        );

        // infinitely tolerated taint -> temporarily tolerated taint
        f.put_node(&node("node1", &[1, 2])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            f.queued("pod1"),
            "pod not queued for deletion after addition of temporarily tolerated taint"
        );

        // temporarily tolerated taint -> infinitely tolerated taint
        f.put_node(&node("node1", &[1])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            !f.queued("pod1"),
            "pod queued for deletion after removal of temporarily tolerated taint"
        );

        assert_eq!(
            f.actions("pod1").await,
            (false, false),
            "Unexpected deletion"
        );
        f.ctl.shutdown().await;
    }

    /// `TestUpdateNodeWithMultiplePods`.
    #[tokio::test]
    async fn update_node_with_multiple_pods() {
        // Taint 1 only: pod1 (no toleration) goes at once, pod2 (1s) after
        // ~1s, pod3 (forever) never.
        let f = Fixture::new(vec![
            pod("pod1", "node1"),
            add_toleration(pod("pod2", "node1"), 1, 1),
            add_toleration(pod("pod3", "node1"), 1, -1),
        ])
        .await;
        let new = node("node1", &[1]);
        f.put_node(&new).await;
        f.ctl
            .node_updated(Some(&node("node1", &[])), Some(&new))
            .await;
        f.verify("pod1", true, true).await;
        assert_eq!(f.actions("pod2").await, (false, false), "pod2 too early");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(f.actions("pod2").await, (true, true), "pod2 after 1s");
        assert_eq!(
            f.actions("pod3").await,
            (false, false),
            "pod3 tolerates forever"
        );
        f.ctl.shutdown().await;

        // Taints 1 and 2: every pod fails to tolerate both (pod3/pod2 only
        // tolerate taint 1), so all go at once.
        let f = Fixture::new(vec![
            pod("pod1", "node1"),
            add_toleration(pod("pod2", "node1"), 1, 1),
            add_toleration(pod("pod3", "node1"), 1, -1),
        ])
        .await;
        let new = node("node1", &[1, 2]);
        f.put_node(&new).await;
        f.ctl
            .node_updated(Some(&node("node1", &[])), Some(&new))
            .await;
        for p in ["pod1", "pod2", "pod3"] {
            f.verify(p, true, true).await;
        }
        f.ctl.shutdown().await;
    }

    /// `TestEventualConsistency`: a pod the node update did not know about is
    /// handled when its own update arrives.
    #[tokio::test]
    async fn eventual_consistency() {
        let cases: Vec<(&str, Option<Pod>, Pod)> = vec![
            (
                "existing pod2 scheduled onto tainted Node",
                Some(pod("pod2", "")),
                pod("pod2", "node1"),
            ),
            (
                "existing pod2 with taint toleration scheduled onto tainted Node",
                Some(add_toleration(pod("pod2", ""), 1, 100)),
                add_toleration(pod("pod2", "node1"), 1, 100),
            ),
            (
                "new pod2 created on tainted Node",
                None,
                pod("pod2", "node1"),
            ),
            (
                "new pod2 with tait toleration created on tainted Node",
                None,
                add_toleration(pod("pod2", "node1"), 1, 100),
            ),
        ];
        for (description, prev, new_pod) in cases {
            let f = Fixture::new(vec![pod("pod1", "node1")]).await;
            let new_node = node("node1", &[1]);
            f.put_node(&new_node).await;
            if let Some(prev) = &prev {
                let key = build_key("pods", Some("default"), "pod2");
                f.storage.create(&key, prev).await.unwrap();
                f.ctl.pod_updated(None, Some(prev)).await;
            }
            // First a NodeUpdate that deletes pod1; it does not know pod2 yet.
            f.ctl
                .node_updated(Some(&node("node1", &[])), Some(&new_node))
                .await;
            f.verify("pod1", true, true).await;

            // Then the delayed update of pod2 reaches the manager: delete it too.
            let key = build_key("pods", Some("default"), "pod2");
            if prev.is_some() {
                f.storage.update(&key, &new_pod).await.unwrap();
            } else {
                f.storage.create(&key, &new_pod).await.unwrap();
            }
            f.ctl.pod_updated(prev.as_ref(), Some(&new_pod)).await;
            // pod2's toleration (100s) is shorter than nothing, but taint 1
            // only: with toleration it is scheduled, without it deleted. The
            // upstream case expects a patch+delete for all four; a tolerated
            // pod is not deleted until 100s, so only assert the intolerant ones.
            if new_pod
                .spec
                .as_ref()
                .and_then(|s| s.tolerations.as_ref())
                .is_none()
            {
                f.verify("pod2", true, true).await;
            } else {
                for _ in 0..100 {
                    if f.queued("pod2") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(f.queued("pod2"), "{description}: eviction scheduled");
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestPodDeletionEvent`: the two TaintManagerEviction events.
    #[tokio::test]
    async fn pod_deletion_events() {
        let storage = Arc::new(MemoryStorage::new());
        let ctl = TaintEvictionController::new(Arc::clone(&storage));
        emit_pod_event(
            &ctl.recorder,
            "test",
            "test",
            "Marking for deletion Pod test/test",
        )
        .await;
        ctl.emit_event("test", "other", "Cancelling deletion of Pod test/other")
            .await;
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/test/").await.unwrap();
        let msgs: Vec<String> = events.iter().map(|e| e.message.clone()).collect();
        assert!(
            msgs.contains(&"Marking for deletion Pod test/test".to_string()),
            "{msgs:?}"
        );
        assert!(
            msgs.contains(&"Cancelling deletion of Pod test/other".to_string()),
            "{msgs:?}"
        );
        for e in &events {
            assert_eq!(e.reason, "TaintManagerEviction");
            assert_eq!(e.event_type, EventType::Normal);
            assert_eq!(e.involved_object.kind.as_deref(), Some("Pod"));
            assert_eq!(e.involved_object.api_version.as_deref(), Some("v1"));
        }
    }

    // ---- sharding --------------------------------------------------------

    /// The pod dispatcher shards by the node a pod sits on, the same hash the
    /// node dispatcher uses (taint_eviction.go:334-338), so a node's pods and
    /// the node itself are always handled by one worker.
    #[test]
    fn pod_item_round_trips_and_shards_by_node() {
        let item = PodUpdateItem {
            pod_name: "p".into(),
            pod_namespace: "ns".into(),
            node_name: "node-7".into(),
        };
        let key = item.encode();
        assert_eq!(PodUpdateItem::decode(&key), Some(item));
        assert_eq!(
            hash(key.split('/').next().unwrap(), UPDATE_WORKER_SIZE),
            hash("node-7", UPDATE_WORKER_SIZE)
        );
    }

    /// Many nodes spread over more than one worker shard.
    #[test]
    fn nodes_spread_across_shards() {
        let shards: std::collections::HashSet<usize> = (0..64)
            .map(|i| hash(&format!("node-{i}"), UPDATE_WORKER_SIZE))
            .collect();
        assert!(shards.len() > 1);
    }

    /// Updates for distinct nodes are processed concurrently across workers: a
    /// storage that stalls node reads must not block a node on another shard.
    #[tokio::test]
    async fn node_updates_run_on_more_than_one_worker() {
        use rusternetes_controller_manager::controllers::worker_pool::test_support::StallStorage;
        // Pick two node names that land on different shards.
        let a = "node-a".to_string();
        let b = (0..64)
            .map(|i| format!("node-{i}"))
            .find(|n| hash(n, UPDATE_WORKER_SIZE) != hash(&a, UPDATE_WORKER_SIZE))
            .unwrap();
        let inner = Arc::new(MemoryStorage::new());
        let storage = StallStorage::new(Arc::clone(&inner), "/registry/nodes/");
        for n in [&a, &b] {
            inner
                .create(&build_key("nodes", None, n), &node(n, &[1]))
                .await
                .unwrap();
        }
        let ctl = Arc::new(TaintEvictionController::new(Arc::clone(&storage)));
        ctl.start_workers();
        ctl.node_update_queue.add(a.clone()).await;
        ctl.node_update_queue.add(b.clone()).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(storage.peak() > 1, "node updates ran on one worker only");
        ctl.shutdown().await;
    }

    /// #3275: upstream `addConditionAndDeletePod` (taint_eviction.go:129-153)
    /// has no kube-system skip: the DisruptionTarget condition is patched and
    /// the pod deleted like any other.
    #[tokio::test]
    async fn add_condition_and_delete_pod_does_not_skip_kube_system() {
        let storage = MemoryStorage::new();
        let mut p = pod("sys", "node-1");
        p.metadata.namespace = Some("kube-system".to_string());
        let key = build_key("pods", Some("kube-system"), "sys");
        storage.create(&key, &p).await.unwrap();

        add_condition_and_delete_pod(&storage, "sys", "kube-system")
            .await
            .unwrap();

        let got = storage.get::<Pod>(&key).await;
        let patched = got.as_ref().map_or(true, |p| {
            p.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|c| c.iter().any(|c| c.condition_type == "DisruptionTarget"))
        });
        let deleted = got.map_or(true, |p| p.metadata.deletion_timestamp.is_some());
        assert!(patched && deleted, "patched={patched} deleted={deleted}");
    }

    /// #3275: upstream has no deletionTimestamp skip either; the condition is
    /// still patched onto an already-terminating pod.
    #[tokio::test]
    async fn add_condition_and_delete_pod_does_not_skip_terminating() {
        let storage = MemoryStorage::new();
        let mut p = pod("term", "node-1");
        p.metadata.deletion_timestamp = Some(Utc::now());
        let key = build_key("pods", Some("default"), "term");
        storage.create(&key, &p).await.unwrap();

        add_condition_and_delete_pod(&storage, "term", "default")
            .await
            .unwrap();

        let got = storage.get::<Pod>(&key).await.unwrap();
        assert!(got
            .status
            .and_then(|s| s.conditions)
            .is_some_and(|c| c.iter().any(|c| c.condition_type == "DisruptionTarget")));
    }
}
