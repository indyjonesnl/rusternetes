//! Unit tests moved from `src/controllers/podgc.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
    pub use rusternetes_common::resources::{Node, Pod, PodCondition};
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_controller_manager::controllers::podgc_metrics as metrics;
    pub use rusternetes_controller_manager::controllers::taint_eviction::update_pod_condition;
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{debug, error, info};
}

mod tests {
    //! Cases ported from `gc_controller_test.go` (release-1.35): TestGCTerminated,
    //! TestGCOrphaned, TestGCUnscheduledTerminating, TestGCTerminating,
    //! TestGCInspectingPatchedPodBeforeDeletion. The fake clientset's recorded
    //! `patch`/`delete` actions are the controller's test seam; the metric
    //! assertions use one namespace per test because the counters are
    //! process-global and tests run in parallel (upstream resets them).
    use super::__parent::*;
    use chrono::TimeZone;
    use rusternetes_controller_manager::controllers::podgc::*;
    use rusternetes_storage::MemoryStorage;
    use serde_json::json;

    fn phase_str(p: Option<&str>) -> serde_json::Value {
        match p {
            Some(p) => json!({ "phase": p }),
            None => json!({}),
        }
    }

    fn pod_json(
        ns: &str,
        name: &str,
        node: &str,
        phase: Option<&str>,
        created_hours: i64,
        deleting: bool,
    ) -> Pod {
        let created = Utc.timestamp_opt(created_hours * 3600, 0).unwrap();
        let mut v = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": name, "namespace": ns, "uid": format!("uid-{ns}-{name}"),
                "creationTimestamp": created.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            },
            "spec": { "containers": [], "nodeName": node },
            "status": phase_str(phase),
        });
        if deleting {
            v["metadata"]["deletionTimestamp"] = json!("1970-01-01T00:00:00Z");
        }
        serde_json::from_value(v).unwrap()
    }

    fn node_json(name: &str, ready: Option<&str>, taints: Vec<(&str, &str)>) -> Node {
        let mut v = json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": { "name": name, "uid": format!("uid-{name}") },
            "spec": { "taints": taints.iter().map(|(k, e)| json!({"key": k, "effect": e})).collect::<Vec<_>>() },
        });
        if let Some(r) = ready {
            v["status"] = json!({ "conditions": [{ "type": "Ready", "status": r }] });
        }
        serde_json::from_value(v).unwrap()
    }

    async fn seed(storage: &MemoryStorage, pods: &[Pod], nodes: &[Node]) {
        for p in pods {
            let key = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
            storage.create(&key, p).await.unwrap();
        }
        for n in nodes {
            storage
                .create(&build_key("nodes", None, &n.metadata.name), n)
                .await
                .unwrap();
        }
    }

    fn names(v: &Mutex<Vec<String>>) -> Vec<String> {
        let mut n = v.lock().unwrap().clone();
        n.sort();
        n.dedup();
        n
    }

    fn patched_names<S: Storage>(gcc: &PodGcController<S>) -> Vec<String> {
        let mut n: Vec<String> = gcc
            .patched
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        n.sort();
        n
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn check_metrics(ns: &str, deleted: usize, reason: &str) {
        assert_eq!(metrics::deleting_pods(ns, reason), deleted as u64, "total");
        assert_eq!(metrics::deleting_pod_errors(ns, reason), 0, "errors");
    }

    fn controller(storage: &Arc<MemoryStorage>, threshold: i32) -> PodGcController<MemoryStorage> {
        PodGcController::new(storage.clone(), threshold)
    }

    #[tokio::test]
    async fn gc_terminated() {
        // (name, phase, reason) per pod; created one hour apart in order.
        type Case = (
            &'static str,
            Vec<(&'static str, &'static str, &'static str)>,
            i32,
            Vec<&'static str>,
        );
        let cases: Vec<Case> = vec![
            (
                "delete pod a which is PodFailed and pod b which is PodSucceeded",
                vec![
                    ("a", "Failed", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", ""),
                ],
                1,
                vec!["a", "b"],
            ),
            (
                "threshold = 0, disables terminated pod deletion",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                0,
                vec![],
            ),
            (
                "threshold = 1, delete pod b which is PodSucceeded",
                vec![
                    ("a", "Running", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", ""),
                ],
                1,
                vec!["b"],
            ),
            (
                "threshold = 1, delete pod a which is PodFailed",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                1,
                vec!["a"],
            ),
            (
                "threshold = 5, don't delete pod",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                5,
                vec![],
            ),
            (
                "evicted pod goes first",
                vec![
                    ("a", "Failed", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", "Evicted"),
                ],
                1,
                vec!["a", "c"],
            ),
            (
                "evicted pod goes first, running pod kept",
                vec![
                    ("a", "Running", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", "Evicted"),
                ],
                1,
                vec!["c"],
            ),
        ];
        for (i, (name, pods, threshold, want_deleted)) in cases.into_iter().enumerate() {
            let ns = format!("terminated-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let mut objs = Vec::new();
            for (h, (n, phase, reason)) in pods.iter().enumerate() {
                let mut p = pod_json(&ns, n, "node", Some(phase), h as i64 + 1, false);
                if !reason.is_empty() {
                    p.status.as_mut().unwrap().reason = Some(reason.to_string());
                }
                objs.push(p);
            }
            seed(&storage, &objs, &[node_json("node", Some("True"), vec![])]).await;
            let gcc = controller(&storage, threshold);
            gcc.gc(Utc::now()).await;
            assert_eq!(names(&gcc.deleted), strs(&want_deleted), "{name}");
            assert!(patched_names(&gcc).is_empty(), "{name}: patched");
            check_metrics(&ns, want_deleted.len(), metrics::REASON_TERMINATED);
            // The deleted pods are really gone from storage.
            let left: Vec<Pod> = storage
                .list(&build_prefix("pods", Some(&ns)))
                .await
                .unwrap();
            assert_eq!(left.len(), pods.len() - want_deleted.len(), "{name}");
        }
    }

    struct OrphanCase {
        name: &'static str,
        // Nodes in the (lagging) lister snapshot / live in storage at the 1st pass.
        lister_nodes: Vec<&'static str>,
        client_nodes: Vec<&'static str>,
        delay: ChronoDuration,
        added_client: Vec<&'static str>,
        deleted_client: Vec<&'static str>,
        added_lister: Vec<&'static str>,
        deleted_lister: Vec<&'static str>,
        pods: Vec<(&'static str, &'static str, &'static str)>,
        items_in_queue: usize,
        deleted: Vec<&'static str>,
        patched: Vec<&'static str>,
    }

    impl Default for OrphanCase {
        fn default() -> Self {
            Self {
                name: "",
                lister_nodes: vec![],
                client_nodes: vec![],
                delay: ChronoDuration::seconds(80),
                added_client: vec![],
                deleted_client: vec![],
                added_lister: vec![],
                deleted_lister: vec![],
                pods: vec![],
                items_in_queue: 0,
                deleted: vec![],
                patched: vec![],
            }
        }
    }

    #[tokio::test]
    async fn gc_orphaned() {
        let two_q = ChronoDuration::seconds(80);
        let cases = vec![
            OrphanCase {
                name: "nodes present in lister",
                lister_nodes: vec!["existing1", "existing2"],
                delay: two_q,
                pods: vec![
                    ("a", "existing1", "Running"),
                    ("b", "existing2", "Failed"),
                    ("c", "existing2", "Succeeded"),
                ],
                items_in_queue: 0,
                ..Default::default()
            },
            OrphanCase {
                name: "nodes present in client",
                client_nodes: vec!["existing1", "existing2"],
                delay: two_q,
                pods: vec![
                    ("a", "existing1", "Running"),
                    ("b", "existing2", "Failed"),
                    ("c", "existing2", "Succeeded"),
                ],
                items_in_queue: 2,
                ..Default::default()
            },
            OrphanCase {
                name: "no nodes",
                delay: two_q,
                pods: vec![("a", "deleted", "Failed"), ("b", "deleted", "Succeeded")],
                items_in_queue: 1,
                deleted: vec!["a", "b"],
                ..Default::default()
            },
            OrphanCase {
                name: "no nodes, one running pod",
                delay: two_q,
                pods: vec![
                    ("a", "deleted", "Failed"),
                    ("b", "deleted", "Succeeded"),
                    ("c", "deleted", "Running"),
                ],
                items_in_queue: 1,
                deleted: vec!["a", "b", "c"],
                patched: vec!["c"],
                ..Default::default()
            },
            OrphanCase {
                name: "quarantine not finished",
                delay: ChronoDuration::seconds(20),
                pods: vec![("a", "deleted", "Failed")],
                items_in_queue: 0,
                ..Default::default()
            },
            OrphanCase {
                name: "wrong nodes",
                lister_nodes: vec!["existing"],
                delay: two_q,
                pods: vec![("a", "deleted", "Running")],
                items_in_queue: 1,
                deleted: vec!["a"],
                patched: vec!["a"],
                ..Default::default()
            },
            OrphanCase {
                name: "some nodes missing",
                lister_nodes: vec!["existing"],
                delay: two_q,
                pods: vec![
                    ("a", "deleted", "Failed"),
                    ("b", "existing", "Failed"),
                    ("c", "deleted", "Succeeded"),
                    ("d", "deleted", "Running"),
                ],
                items_in_queue: 1,
                deleted: vec!["a", "c", "d"],
                patched: vec!["d"],
                ..Default::default()
            },
            OrphanCase {
                name: "node added to client after quarantine",
                delay: two_q,
                added_client: vec!["node"],
                pods: vec![("a", "node", "Running")],
                items_in_queue: 1,
                ..Default::default()
            },
            OrphanCase {
                name: "node added to informer after quarantine",
                delay: two_q,
                added_lister: vec!["node"],
                pods: vec![("a", "node", "Failed")],
                items_in_queue: 1,
                ..Default::default()
            },
            OrphanCase {
                // It shouldn't happen that the client lags behind the informer;
                // upstream keeps this as a sanity check.
                name: "node deleted from client after quarantine",
                client_nodes: vec!["node"],
                delay: two_q,
                deleted_client: vec!["node"],
                pods: vec![("a", "node", "Failed")],
                items_in_queue: 1,
                deleted: vec!["a"],
                ..Default::default()
            },
            OrphanCase {
                name: "node deleted from informer after quarantine",
                lister_nodes: vec!["node"],
                delay: two_q,
                deleted_lister: vec!["node"],
                pods: vec![("a", "node", "Succeeded")],
                items_in_queue: 0,
                ..Default::default()
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("orphaned-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .map(|(n, node, ph)| pod_json(&ns, n, node, Some(ph), 0, false))
                .collect();
            let client_nodes: Vec<Node> = c
                .client_nodes
                .iter()
                .map(|n| node_json(n, Some("True"), vec![]))
                .collect();
            seed(&storage, &pods, &client_nodes).await;
            let mut lister: Vec<Node> = c
                .lister_nodes
                .iter()
                .map(|n| node_json(n, Some("True"), vec![]))
                .collect();
            let gcc = controller(&storage, -1);
            let t0 = Utc.timestamp_opt(1_000_000, 0).unwrap();

            // First GC of orphaned pods.
            gcc.gc_snapshot(&pods, &lister, t0).await;
            assert!(
                gcc.deleted.lock().unwrap().is_empty(),
                "{}: no pods should be deleted at this point",
                c.name
            );

            // Move the clock forward; the quarantined items become ready.
            let t1 = t0 + c.delay;
            let ready = gcc
                .node_queue
                .lock()
                .unwrap()
                .values()
                .filter(|at| **at <= t1)
                .count();
            assert_eq!(ready, c.items_in_queue, "{}: items in node queue", c.name);

            // Execute planned node changes.
            for n in &c.added_client {
                let node = node_json(n, Some("True"), vec![]);
                storage
                    .create(&build_key("nodes", None, n), &node)
                    .await
                    .unwrap();
            }
            for n in &c.deleted_client {
                storage.delete(&build_key("nodes", None, n)).await.unwrap();
            }
            for n in &c.added_lister {
                lister.push(node_json(n, Some("True"), vec![]));
            }
            lister.retain(|n| !c.deleted_lister.contains(&n.metadata.name.as_str()));

            // Actual pod deletion.
            gcc.gc_snapshot(&pods, &lister, t1).await;
            assert_eq!(names(&gcc.deleted), strs(&c.deleted), "{}: deleted", c.name);
            assert_eq!(patched_names(&gcc), strs(&c.patched), "{}: patched", c.name);
            check_metrics(&ns, c.deleted.len(), metrics::REASON_ORPHANED);
        }
    }

    #[tokio::test]
    async fn gc_unscheduled_terminating() {
        // (name, phase, deleting, nodeName, expected)
        struct Case {
            name: &'static str,
            pods: Vec<(&'static str, &'static str, bool, &'static str)>,
            deleted: Vec<&'static str>,
            patched: Vec<&'static str>,
        }
        let cases = vec![
            Case {
                name: "Unscheduled pod in any phase must be deleted",
                pods: vec![
                    ("a", "Failed", true, ""),
                    ("b", "Succeeded", true, ""),
                    ("c", "Running", true, ""),
                ],
                deleted: vec!["a", "b", "c"],
                patched: vec!["c"],
            },
            Case {
                name: "Scheduled pod in any phase must not be deleted",
                pods: vec![
                    ("a", "Failed", false, ""),
                    ("b", "Succeeded", false, "node"),
                    ("c", "Running", true, "node"),
                ],
                deleted: vec![],
                patched: vec![],
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("unscheduled-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .enumerate()
                .map(|(h, (n, ph, del, node))| pod_json(&ns, n, node, Some(ph), h as i64 + 1, *del))
                .collect();
            seed(&storage, &pods, &[]).await;
            let gcc = controller(&storage, -1);
            gcc.gc_unscheduled_terminating(&pods).await;
            assert_eq!(names(&gcc.deleted), strs(&c.deleted), "{}", c.name);
            assert_eq!(patched_names(&gcc), strs(&c.patched), "{}", c.name);
            check_metrics(
                &ns,
                c.deleted.len(),
                metrics::REASON_TERMINATING_UNSCHEDULED,
            );
        }
    }

    #[tokio::test]
    async fn gc_terminating() {
        const NOEXEC: &str = "NoExecute";
        let oos = TAINT_NODE_OUT_OF_SERVICE;
        // Upstream's second case, verbatim (pod `a5` sits on worker-3).
        let nodes = vec![
            node_json("worker-0", Some("False"), vec![(oos, NOEXEC)]),
            node_json("worker-1", Some("False"), vec![]),
            node_json("worker-2", Some("True"), vec![]),
            node_json("worker-3", Some("False"), vec![(oos, "NoSchedule")]),
            node_json("worker-4", Some("False"), vec![(oos, "PreferNoSchedule")]),
            node_json("worker-5", Some("False"), vec![(oos, NOEXEC)]),
        ];
        struct Case {
            name: &'static str,
            nodes: Vec<Node>,
            // (name, phase, deleting, nodeName)
            pods: Vec<(String, Option<&'static str>, bool, String)>,
            deleted: Vec<String>,
            patched: Vec<String>,
        }
        let mut mixed_pods = Vec::new();
        for w in 0..6 {
            let node = format!("worker-{w}");
            let s = w + 1;
            mixed_pods.push((format!("a{s}"), None, false, node.clone()));
            mixed_pods.push((format!("b{s}"), None, true, node.clone()));
            mixed_pods.push((format!("c{s}"), Some("Pending"), false, node.clone()));
            mixed_pods.push((format!("d{s}"), Some("Running"), false, node.clone()));
            mixed_pods.push((format!("e{s}"), Some("Unknown"), false, node));
        }
        // `{name: "a5", nodeName: "worker-3"}`.
        mixed_pods[20].3 = "worker-3".to_string();
        let cases = vec![
            Case {
                name: "pods have deletion timestamp set and the corresponding nodes are not ready",
                nodes: vec![
                    node_json("worker-0", Some("False"), vec![]),
                    node_json("worker-1", Some("False"), vec![]),
                ],
                pods: vec![
                    ("a".into(), None, true, "worker-0".into()),
                    ("b".into(), None, true, "worker-1".into()),
                ],
                deleted: vec![],
                patched: vec![],
            },
            Case {
                name: "some pods have deletion timestamp and/or phase set and some of the corresponding nodes have an outofservice taint that are not ready",
                nodes,
                pods: mixed_pods,
                deleted: strs(&["b1", "b4", "b5", "b6"]),
                patched: strs(&["b1", "b4", "b5", "b6"]),
            },
            Case {
                name: "pods deleted from node tainted out-of-service",
                nodes: vec![node_json("worker", Some("False"), vec![(oos, NOEXEC)])],
                pods: vec![
                    ("a".into(), Some("Running"), true, "worker".into()),
                    ("b".into(), Some("Failed"), true, "worker".into()),
                    ("c".into(), Some("Succeeded"), true, "worker".into()),
                ],
                deleted: strs(&["a", "b", "c"]),
                patched: strs(&["a"]),
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("terminating-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .enumerate()
                .map(|(h, (n, ph, del, node))| pod_json(&ns, n, node, *ph, h as i64 + 1, *del))
                .collect();
            seed(&storage, &pods, &c.nodes).await;
            let gcc = controller(&storage, -1);
            gcc.gc(Utc::now()).await;
            assert_eq!(names(&gcc.deleted), c.deleted, "{}", c.name);
            assert_eq!(patched_names(&gcc), c.patched, "{}", c.name);
            check_metrics(
                &ns,
                c.deleted.len(),
                metrics::REASON_TERMINATING_OUT_OF_SERVICE,
            );
        }
    }

    #[tokio::test]
    async fn gc_inspecting_patched_pod_before_deletion() {
        // "orphaned pod should have DisruptionTarget condition added before deletion"
        let storage = Arc::new(MemoryStorage::new());
        let mut pod = pod_json(
            "default",
            "testPod",
            "deletedNode",
            Some("Running"),
            0,
            false,
        );
        pod.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
            condition_type: "Ready".into(),
            status: "True".into(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]);
        seed(&storage, std::slice::from_ref(&pod), &[]).await;
        // `gcc.quarantineTime = time.Duration(-1)`.
        let gcc = PodGcController::new_internal(
            storage.clone(),
            -1,
            GC_CHECK_PERIOD,
            Duration::from_secs(0),
        );
        let now = Utc::now();
        // A zero quarantine is ready within the same pass (AddAfter(-1) upstream).
        gcc.gc_snapshot(std::slice::from_ref(&pod), &[], now).await;

        let patched = gcc.patched.lock().unwrap().clone();
        assert_eq!(patched.len(), 1, "exactly one status patch");
        let (name, status) = &patched[0];
        assert_eq!(name, "testPod");
        assert_eq!(status.phase, Some(Phase::Failed));
        let conds = status.conditions.as_ref().unwrap();
        assert_eq!(conds.len(), 2);
        assert_eq!(conds[0].condition_type, "Ready");
        assert_eq!(conds[0].status, "True");
        assert_eq!(conds[1].condition_type, "DisruptionTarget");
        assert_eq!(conds[1].status, "True");
        assert_eq!(conds[1].reason.as_deref(), Some("DeletionByPodGC"));
        assert_eq!(
            conds[1].message.as_deref(),
            Some("PodGC: node no longer exists")
        );
        assert_eq!(names(&gcc.deleted), strs(&["testPod"]));
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "testPod"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn patch_precondition_uid_mismatch_is_not_deleted() {
        // utilpod.PatchPodStatus carries the UID as a precondition: a pod that was
        // recreated under the same name must survive.
        let storage = Arc::new(MemoryStorage::new());
        let old = pod_json("uid-ns", "p", "gone", Some("Running"), 1, false);
        let mut new = old.clone();
        new.metadata.uid = "recreated".into();
        seed(&storage, &[new], &[]).await;
        let gcc = controller(&storage, -1);
        let err = gcc.mark_failed_and_delete_pod(&old).await.unwrap_err();
        assert!(err.to_string().contains("Precondition failed"), "{err}");
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("uid-ns"), "p"))
            .await
            .is_ok());
    }
}
