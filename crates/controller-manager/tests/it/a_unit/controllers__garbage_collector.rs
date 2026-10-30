//! Unit tests moved from `src/controllers/garbage_collector.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{DeletionPropagation, ObjectMeta};
    pub use rusternetes_storage::Storage;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::Value;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use tokio::time::{sleep, Duration};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::types::OwnerReference;
    use rusternetes_controller_manager::controllers::garbage_collector::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn gc_scan_interval_backs_off_when_idle_and_resets_on_work() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(60);

        // Idle scans back off exponentially, clamped at max.
        let mut i = base;
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(10));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(20));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(40));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, max, "must clamp at max (80s -> 60s)");
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, max, "stays at max while idle");

        // A scan that did work resets to the base cadence immediately.
        i = next_scan_interval(i, true, base, max);
        assert_eq!(i, base, "work resets to base");

        // Never drops below base; a single idle scan from base doubles it.
        assert_eq!(
            next_scan_interval(base, false, base, max),
            Duration::from_secs(10)
        );
        assert_eq!(next_scan_interval(base, true, base, max), base);
    }

    #[tokio::test]
    async fn test_deletion_propagation_policy() {
        let gc = GarbageCollector::new(Arc::new(MemoryStorage::new()));

        // Test foreground deletion
        let mut metadata = ObjectMeta::new("test");
        metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Foreground
        );

        // Test orphan deletion
        metadata.finalizers = Some(vec!["orphan".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Orphan
        );

        // Test default (background)
        metadata.finalizers = None;
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Background
        );
    }

    #[test]
    fn test_owner_reference_creation() {
        let owner_ref = OwnerReference::new("v1", "Pod", "my-pod", "abc-123")
            .with_controller(true)
            .with_block_owner_deletion(true);

        assert_eq!(owner_ref.kind, "Pod");
        assert_eq!(owner_ref.controller, Some(true));
        assert_eq!(owner_ref.block_owner_deletion, Some(true));
    }

    #[test]
    fn test_metadata_finalizer_helpers() {
        let mut metadata = ObjectMeta::new("test");

        // Test add_finalizer
        metadata.add_finalizer("my-finalizer".to_string());
        assert!(metadata.has_finalizers());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test idempotent add
        metadata.add_finalizer("my-finalizer".to_string());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test remove_finalizer
        metadata.remove_finalizer("my-finalizer");
        assert!(!metadata.has_finalizers());
    }

    /// A foreground deletion inside an ownership CYCLE must still complete.
    ///
    /// Three pods each own the next and each carries `blockOwnerDeletion: true`,
    /// so every member blocks its owner. Deleting one with foreground
    /// propagation deadlocks a naive implementation: pod1 waits for pod2, pod2
    /// waits for pod3, pod3 waits for pod1.
    ///
    /// Upstream breaks the ring in `attemptToDeleteItem`
    /// (`pkg/controller/garbagecollector/garbagecollector.go:595-620`): when an
    /// item has a dependent that is ITSELF deleting dependents, it patches its
    /// own ownerReferences to be non-blocking and proceeds with the foreground
    /// delete. The comment there calls the check a deliberate false-positive-
    /// prone circle detection.
    ///
    /// Pins `[sig-api-machinery] Garbage collector should not be blocked by
    /// dependency circle [Conformance]`
    /// (`test/e2e/apimachinery/garbage_collector.go:826-880`), which builds this
    /// exact three-pod ring and requires ALL of them to disappear.
    #[tokio::test]
    async fn foreground_deletion_breaks_an_ownership_cycle() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        // pod1 <- pod2 <- pod3 <- pod1: each owned by the previous one.
        let uids = ["pod1-uid", "pod2-uid", "pod3-uid"];
        let names = ["pod1", "pod2", "pod3"];

        for i in 0..3 {
            // owner is the PREVIOUS pod in the ring
            let owner_idx = (i + 2) % 3;
            let mut meta = ObjectMeta::new(names[i]);
            meta.namespace = Some("default".to_string());
            meta.uid = uids[i].to_string();
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: names[owner_idx].to_string(),
                uid: uids[owner_idx].to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            // The one being deleted enters foreground deletion.
            if i == 0 {
                meta.deletion_timestamp = Some(chrono::Utc::now());
                meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
            }
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            let key = format!("/registry/pods/default/{}", names[i]);
            storage.create(&key, &pod).await.unwrap();
        }

        // A bounded number of scans must drain the whole ring. Upstream needs
        // several passes too (each pass unblocks/deletes one more member), but
        // it must CONVERGE rather than spin forever.
        for _ in 0..10 {
            gc.scan_and_collect().await.unwrap();
            let remaining: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
            if remaining.is_empty() {
                return;
            }
        }

        let remaining: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let names: Vec<&str> = remaining.iter().map(|p| p.metadata.name.as_str()).collect();
        panic!("ownership cycle deadlocked foreground deletion; still present: {names:?}");
    }

    /// Foreground deletion must NOT remove the `foregroundDeletion` finalizer
    /// (and thus must not delete the owner) while any blocking dependent still
    /// exists in storage.
    ///
    /// Reproduces `[sig-api-machinery] Garbage collector should keep the rc
    /// around until all its pods are deleted if the deleteOptions says so`
    /// (garbage_collector.go:711), which deletes the rc with
    /// PropagationPolicy=Foreground and then asserts there are zero pods once
    /// the rc is gone. The previous code removed the finalizer in the same pass
    /// it issued the dependent deletes — gated only on the delete calls
    /// returning, not on the dependents actually being gone — so a stale
    /// snapshot (pods still draining or created after the snapshot) let the rc
    /// be deleted while pods remained. `still_has_dependents` is the gate.
    #[tokio::test]
    async fn still_has_dependents_blocks_until_dependent_gone() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-foreground-uid";

        let mut pod_meta = ObjectMeta::new("dependent-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        let pod_key = "/registry/pods/default/dependent-pod";
        storage.create(pod_key, &pod).await.unwrap();

        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "owner with an owned pod still in storage must report remaining dependents"
        );

        // Pod drained — gate must clear so the owner can be finalized/deleted.
        storage.delete(pod_key).await.unwrap();
        assert!(
            !gc.still_has_dependents(owner_uid).await.unwrap(),
            "once the owned pod is gone, the owner must report no remaining dependents"
        );
    }

    /// Once all dependents drain, the foreground gate must clear: the owner's
    /// `foregroundDeletion` finalizer is removed and the owner deleted. Guards
    /// the gate against deadlocking the happy path.
    #[tokio::test]
    async fn foreground_owner_deleted_after_dependents_drain() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-fg-complete-uid";

        // Owner rc: being foreground-deleted (deletionTimestamp + finalizer).
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        storage.create(rc_key, &rc).await.unwrap();

        // One pod owned solely by the rc.
        let mut pod_meta = ObjectMeta::new("rc-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/rc-pod", &pod)
            .await
            .unwrap();

        // First scan: deletes the pod, gate clears (0 dependents), finalizer
        // removed, rc deleted — all in one scan since the snapshot is complete.
        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "dependent pod must be deleted, {} left",
            pods.len()
        );

        let rc_after: rusternetes_common::Result<Value> = storage.get(rc_key).await;
        assert!(
            rc_after.is_err(),
            "rc must be deleted once its dependents are gone (foreground complete)"
        );
    }

    /// The exact Bug-B race: an in-flight pod the RC controller created just
    /// before observing the owner's deletion lands AFTER the foreground delete
    /// pass took its snapshot. The owner's `foregroundDeletion` finalizer must
    /// NOT be removed (owner must NOT be deleted) while that pod still exists —
    /// only once it drains. This is what
    /// `[sig-api-machinery] Garbage collector should keep the rc around until
    /// all its pods are deleted` (garbage_collector.go:711) asserts; the old
    /// code removed the finalizer in the same pass and deleted the rc with pods
    /// still present.
    #[tokio::test]
    async fn foreground_gate_holds_for_inflight_pod_after_delete_pass() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());
        let owner_uid = "rc-fg-inflight-uid";

        // rc being foreground-deleted.
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        let rc_val = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        storage.create(rc_key, &rc_val).await.unwrap();

        // Helper: a pod owned solely by the rc.
        let mk_pod = |name: &str| Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("default".to_string());
                m.owner_references = Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "ReplicationController".to_string(),
                    name: "simpletest.rc".to_string(),
                    uid: owner_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);
                m
            },
            spec: None,
            status: None,
        };

        // pod_a is present when the foreground delete pass takes its snapshot.
        let pod_a_key = "/registry/pods/default/rc-pod-a";
        storage
            .create(pod_a_key, &mk_pod("rc-pod-a"))
            .await
            .unwrap();

        let rc_info = ResourceInfo {
            key: rc_key.to_string(),
            metadata: rc_meta.clone(),
            resource_type: "replicationcontrollers".to_string(),
        };
        let empty_dep_map: HashMap<String, Vec<String>> = HashMap::new();

        // 1) Foreground delete pass: deletes the dependents it saw (pod_a).
        gc.delete_dependents_foreground(&rc_info, &empty_dep_map)
            .await
            .unwrap();
        assert!(
            storage.get::<Value>(pod_a_key).await.is_err(),
            "pod_a should have been deleted by the foreground pass"
        );

        // 2) An in-flight pod the RC controller created before noticing the
        //    deletion lands AFTER the delete pass's snapshot.
        let pod_b_key = "/registry/pods/default/rc-pod-b";
        storage
            .create(pod_b_key, &mk_pod("rc-pod-b"))
            .await
            .unwrap();

        // 3) GATE (the protection this pass provides): a dependent still exists,
        //    so the foreground finalizer must NOT be removed and the rc must be
        //    retained. The old code removed it here and deleted the rc.
        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "gate must report remaining dependents while the in-flight pod exists"
        );
        assert!(
            storage.get::<Value>(rc_key).await.is_ok(),
            "rc must be retained while the in-flight pod_b still exists"
        );

        // 4) pod_b drains; a subsequent scan deletes any remaining dependent and,
        //    finding the gate clear, finally collects the rc.
        storage.delete(pod_b_key).await.unwrap();
        gc.scan_and_collect().await.unwrap();
        assert!(
            storage.get::<Value>(rc_key).await.is_err(),
            "rc must be collected once every dependent has drained"
        );
    }

    /// A single GC scan must remove an orphan whose owner is already gone.
    /// Conformance tests for orphan pod cleanup observe per-cycle latency,
    /// so the previous "2-scan grace" gating added a full reconcile cycle
    /// of wait before any orphan was reaped. `delete_orphan_verified` already re-reads
    /// the owner from storage as a race guard, so the second scan was
    /// unnecessary and observably slow.
    #[tokio::test]
    async fn test_orphan_deleted_on_first_scan() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut m = ObjectMeta::new("orphan-pod");
        m.namespace = Some("default".to_string());
        m.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "missing-rs".to_string(),
            uid: "missing-rs-uid-never-existed".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: m,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/orphan-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "orphan must be deleted on the first GC scan, got {} remaining",
            pods.len()
        );
    }

    /// One GC scan must drain a whole ownership CHAIN, not one level of it.
    ///
    /// `[sig-api-machinery] Garbage collector should not be blocked by
    /// dependency circle` (test/e2e/apimachinery/garbage_collector.go:826)
    /// builds pod1 -> pod3 -> pod2 -> pod1, deletes pod1, and gives the cluster
    /// 150s for ALL THREE to disappear. The pods carry
    /// terminationGracePeriodSeconds=0, so the api-server removes pod1 at once
    /// and the GC inherits a 2-long dangling chain: pod2 (owner pod1, gone) and
    /// pod3 (owner pod2, still present).
    ///
    /// Deleting only the orphans present in this scan's snapshot peels one
    /// level per scan, so the chain needs as many scans as it is long. Each
    /// scan lists every resource type across the cluster and the interval backs
    /// off to 60s while idle — under conformance load that ran past the spec's
    /// 150s budget and left pod3 behind (run 33155140545, 2026-08-28).
    ///
    /// Upstream does not wait for its next sweep: deleting a dependent feeds
    /// the graph builder, which enqueues everything that pointed at it, so a
    /// chain collapses transitively in one go
    /// (pkg/controller/garbagecollector/graph_builder.go `processGraphChanges`
    /// → `attemptToDelete` enqueue of `n.dependents`). A scan-based GC gets the
    /// same property by re-running the orphan pass until it reaches a fixpoint.
    #[tokio::test]
    async fn one_scan_drains_a_whole_orphan_chain() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        // pod2 -> pod1 (already deleted by the api-server), pod3 -> pod2.
        let chain = [("pod2", "pod1", "pod1-uid"), ("pod3", "pod2", "pod2-uid")];
        for (name, owner_name, owner_uid) in chain {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("gc-ns".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: owner_name.to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/gc-ns/{name}"), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        let remaining: Vec<Pod> = storage.list("/registry/pods/gc-ns/").await.unwrap();
        let names: Vec<&str> = remaining.iter().map(|p| p.metadata.name.as_str()).collect();
        assert!(
            remaining.is_empty(),
            "a single scan must collect the whole chain, not one level per scan; still present: {names:?}"
        );
    }

    /// Background orphan-collection must overlap its per-orphan work.
    ///
    /// `delete_orphan_verified` costs 1 + N reads (the dependent, then each owner) plus
    /// a delete, so a serial round pays that per orphan, one after another.
    /// Upstream drains orphans through `ConcurrentGCSyncs` = 20
    /// `attemptToDeleteItem` workers
    /// (`pkg/controller/garbagecollector/garbagecollector.go:172`, workers
    /// from `cmd/kube-controller-manager/app/options/garbagecollectorcontroller.go`),
    /// each item verified independently (`:521` re-reads the owner). Guards
    /// issue #1860.
    #[tokio::test]
    async fn background_orphan_rounds_verify_and_delete_concurrently() {
        struct SlowGet {
            inner: Arc<MemoryStorage>,
        }

        #[async_trait::async_trait]
        impl Storage for SlowGet {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                // A real backend's read is a round trip. MemoryStorage never
                // yields, so without this a spawned task would run to
                // completion before the next starts and overlap would be
                // unobservable.
                tokio::time::sleep(Duration::from_millis(5)).await;
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        const ORPHANS: usize = 20;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(SlowGet {
            inner: inner.clone(),
        });
        let gc = GarbageCollector::new(storage.clone());

        for i in 0..ORPHANS {
            let mut meta = ObjectMeta::new(format!("orphan-{i}"));
            meta.namespace = Some("gc-ns".to_string());
            meta.uid = format!("orphan-{i}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "gone-rc".to_string(),
                uid: "gone-rc-uid".to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/gc-ns/orphan-{i}"), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        let remaining: Vec<Pod> = storage.list("/registry/pods/gc-ns/").await.unwrap();
        assert!(
            remaining.is_empty(),
            "every orphan must still be collected, {} left",
            remaining.len()
        );
        let peak = gc.peak_concurrent_dependent_ops();
        assert!(
            peak >= ORPHANS / 2,
            "collecting {ORPHANS} orphans peaked at {peak} concurrent operations; \
             upstream runs 20 workers over the same queue"
        );
    }

    /// The object whose foreground deletion started the walk must never be
    /// removed from storage BY the walk. In an ownership cycle the walk comes
    /// back around to it (pod3's dependent is pod1, the object being deleted);
    /// deleting it there takes it out from behind its own finalizers.
    ///
    /// Upstream never does this: `attemptToDeleteItem` returns immediately for
    /// an item that is already being deleted, and an item's own
    /// `foregroundDeletion` finalizer is only dropped by
    /// `processDeletingDependentsItem` once its blocking dependents are gone
    /// (pkg/controller/garbagecollector/garbagecollector.go). A third-party
    /// finalizer makes the difference observable: the pod must survive the scan.
    #[tokio::test]
    async fn foreground_cycle_does_not_delete_the_walk_root_behind_its_finalizers() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let uids = ["root-uid", "mid-uid", "tail-uid"];
        let names = ["root", "mid", "tail"];

        for i in 0..3 {
            let owner_idx = (i + 2) % 3;
            let mut meta = ObjectMeta::new(names[i]);
            meta.namespace = Some("default".to_string());
            meta.uid = uids[i].to_string();
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: names[owner_idx].to_string(),
                uid: uids[owner_idx].to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            if i == 0 {
                meta.deletion_timestamp = Some(chrono::Utc::now());
                // A third-party finalizer alongside the GC one: nothing in the
                // GC is allowed to remove the object while this is present.
                meta.finalizers = Some(vec![
                    "foregroundDeletion".to_string(),
                    "example.com/blocker".to_string(),
                ]);
            }
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/default/{}", names[i]), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        let root: rusternetes_common::Result<Value> =
            storage.get("/registry/pods/default/root").await;
        let root = root.expect("root must survive: its third-party finalizer is still set");
        let finalizers: Vec<String> = root
            .pointer("/metadata/finalizers")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        assert!(
            finalizers.contains(&"example.com/blocker".to_string()),
            "the third-party finalizer must remain, got {finalizers:?}"
        );
    }

    /// A pod in graceful termination — `deletionTimestamp` and a grace period
    /// set, no finalizers — belongs to the kubelet, which removes it once the
    /// container actually stops. The GC must leave it alone until then.
    ///
    /// Upstream returns immediately for exactly this shape, in
    /// `attemptToDeleteItem`
    /// (pkg/controller/garbagecollector/garbagecollector.go:511):
    ///
    /// ```text
    /// // "being deleted" is an one-way trip to the final deletion. We'll just
    /// // wait for the final deletion, and then process the object's dependents.
    /// if item.isBeingDeleted() && !item.isDeletingDependents() {
    ///     return nil
    /// }
    /// ```
    ///
    /// `isDeletingDependents` is the `foregroundDeletion` finalizer, so an
    /// object carrying no GC finalizer is never the GC's to remove. Ours
    /// deleted it on the next scan, racing the kubelet's shutdown and taking
    /// the pod out of the API before its grace period had run.
    ///
    /// The sweep is deferred, not dropped: it is still the only thing that
    /// finishes an object whose last finalizer is removed by a PUT, since
    /// `ShouldDeleteDuringUpdate` is implemented on the PATCH path alone.
    /// #1828 tracks retiring it.
    #[tokio::test]
    async fn gc_leaves_a_gracefully_terminating_pod_alone() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut meta = ObjectMeta::new("terminating-pod");
        meta.namespace = Some("default".to_string());
        meta.uid = "terminating-pod-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now());
        meta.deletion_grace_period_seconds = Some(30);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: None,
            status: None,
        };
        let key = "/registry/pods/default/terminating-pod";
        storage.create(key, &pod).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        let after: rusternetes_common::Result<Value> = storage.get(key).await;
        assert!(
            after.is_ok(),
            "a terminating pod inside its grace period must survive the GC scan — \
             the kubelet removes it when the container stops"
        );
    }

    /// The same sweep must still fire once the grace period has run out: it is
    /// the only thing that finishes an object whose last finalizer was removed
    /// by a PUT (see `deletion_grace_period_elapsed`).
    #[tokio::test]
    async fn gc_still_sweeps_a_pod_past_its_grace_period() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut meta = ObjectMeta::new("expired-pod");
        meta.namespace = Some("default".to_string());
        meta.uid = "expired-pod-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(120));
        meta.deletion_grace_period_seconds = Some(30);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: None,
            status: None,
        };
        let key = "/registry/pods/default/expired-pod";
        storage.create(key, &pod).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        let after: rusternetes_common::Result<Value> = storage.get(key).await;
        assert!(
            after.is_err(),
            "a pod whose grace period expired long ago must still be swept"
        );
    }

    /// The foreground gate counts *blocking* dependents only — those whose
    /// ownerReference to the owner sets `blockOwnerDeletion: true`.
    ///
    /// Upstream gates on exactly that set. `node.blockingDependents()`
    /// (pkg/controller/garbagecollector/graph.go:178):
    ///
    /// ```text
    /// for _, owner := range dep.owners {
    ///     if owner.UID == n.identity.UID && owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
    ///         ret = append(ret, dep)
    ///     }
    /// }
    /// ```
    ///
    /// and `processDeletingDependentsItem` (garbagecollector.go:654) drops the
    /// `foregroundDeletion` finalizer as soon as that set is empty. A dependent
    /// that does not ask to block its owner must not hold the owner open.
    #[tokio::test]
    async fn foreground_gate_counts_blocking_dependents_only() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());
        let owner_uid = "gate-owner-uid";

        let make_pod = |name: &str, block: Option<bool>| {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("default".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "gate-owner".to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: block,
            }]);
            Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            }
        };

        // blockOwnerDeletion explicitly false: not a blocking dependent.
        storage
            .create(
                "/registry/pods/default/non-blocking",
                &make_pod("non-blocking", Some(false)),
            )
            .await
            .unwrap();
        // Field absent: upstream treats a nil BlockOwnerDeletion as not blocking.
        storage
            .create("/registry/pods/default/unset", &make_pod("unset", None))
            .await
            .unwrap();

        assert!(
            !gc.still_has_dependents(owner_uid).await.unwrap(),
            "dependents that do not set blockOwnerDeletion: true must not hold \
             the owner's foregroundDeletion finalizer open"
        );

        // One blocking dependent is enough to hold the gate.
        storage
            .create(
                "/registry/pods/default/blocking",
                &make_pod("blocking", Some(true)),
            )
            .await
            .unwrap();
        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "a dependent with blockOwnerDeletion: true must hold the gate"
        );
    }

    /// The foreground cascade must not re-LIST the whole cluster once per
    /// dependent.
    ///
    /// `delete_dependents_foreground` recursed into every sole-owned dependent
    /// and each level called `get_all_resources()`, which LISTs all 28 resource
    /// types. Deleting an rc with N pods therefore cost N + 3 full passes —
    /// ~1484 LIST operations for the 50 sole-owned pods of
    /// `[sig-api-machinery] Garbage collector should not delete dependents that
    /// have both valid owner and owner that's waiting for dependents to be
    /// deleted [Serial]`, per scan, while that same spec's 90s budget was
    /// waiting on the cascade to finish (#1836).
    ///
    /// Upstream never pays this: its GC walks an in-memory dependency graph
    /// maintained by informers (pkg/controller/garbagecollector/graph_builder.go)
    /// and does no listing during a cascade at all. One snapshot per cascade is
    /// the closest equivalent that keeps our scan-based design.
    #[tokio::test]
    async fn foreground_cascade_takes_one_list_pass_not_one_per_dependent() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-many-pods-uid";
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 20},
        });
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest.rc",
                &rc,
            )
            .await
            .unwrap();

        const PODS: usize = 20;
        for i in 0..PODS {
            let name = format!("rc-pod-{i}");
            let mut meta = ObjectMeta::new(name.clone());
            meta.namespace = Some("default".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "simpletest.rc".to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/default/{name}"), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        // The cascade still has to work.
        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "all {PODS} pods must be collected, {} left",
            pods.len()
        );

        // One pass to open the scan, one for the cascade, one for the
        // foreground gate's fresh re-read. Emphatically not one per pod.
        let passes = gc.list_pass_count();
        assert!(
            passes <= 4,
            "foreground cascade of {PODS} dependents took {passes} full-cluster \
             list passes; it must not scale with the number of dependents"
        );
    }

    /// Only deletion signals may kick a scan.
    ///
    /// Upstream's graph builder reacts to every event, but it keeps a cached
    /// graph so a status write is cheap
    /// (`pkg/controller/garbagecollector/graph_builder.go`
    /// `processGraphChanges`). Our scan re-LISTs 28 resource types, so kicking
    /// on an Added or a plain Modified would turn every pod status write into
    /// a full-cluster pass and undo the idle-quiet property (#1040).
    ///
    /// A delete orphans dependents; a Modified that stamps `deletionTimestamp`
    /// starts a graceful/foreground deletion the scan's `process_deletion` arm
    /// owns. Everything else waits for the poll.
    #[test]
    fn watch_event_warrants_scan_only_for_deletion_signals() {
        use rusternetes_storage::WatchEvent;

        let live = r#"{"metadata":{"name":"p","uid":"u"}}"#.to_string();
        let terminating =
            r#"{"metadata":{"name":"p","uid":"u","deletionTimestamp":"2026-09-02T23:23:14Z"}}"#
                .to_string();

        assert!(event_warrants_scan(&WatchEvent::Deleted(
            "/registry/services/default/svc".to_string(),
            live.clone()
        )));
        assert!(event_warrants_scan(&WatchEvent::Modified(
            "/registry/namespaces/ns-1842".to_string(),
            terminating.clone()
        )));

        assert!(!event_warrants_scan(&WatchEvent::Added(
            "/registry/pods/default/p".to_string(),
            live.clone()
        )));
        assert!(!event_warrants_scan(&WatchEvent::Modified(
            "/registry/pods/default/p".to_string(),
            live
        )));
        // An Added of an already-terminating object is still not a new signal:
        // the object was created being-deleted only in a relist, and the poll
        // arm picks it up.
        assert!(!event_warrants_scan(&WatchEvent::Added(
            "/registry/pods/default/p".to_string(),
            terminating
        )));
        // Unparseable payloads must not kick — a malformed value is not a
        // deletion signal.
        assert!(!event_warrants_scan(&WatchEvent::Modified(
            "/registry/pods/default/p".to_string(),
            "not json, has deletionTimestamp in it though".to_string()
        )));
    }

    /// The deletion-latency regression.
    ///
    /// The scan interval backs off while idle (5 -> 10 -> 20 -> 40 -> 60s), so
    /// an owner deleted just after an idle scan is not even *noticed* for up to
    /// `max_scan_interval`. Measured 2026-09-02 on the kine leg: the
    /// `[sig-api-machinery] Namespaces [Serial]` spec gave up at 23:23:14 and
    /// the GC's first delete attempt landed at 23:23:42 — 28s late. Budgets
    /// that lose: 30s (`wait.ForeverTestTimeout`, issue #1839) and 90s
    /// (the Namespaces [Serial] specs).
    ///
    /// Upstream never has this window: it is informer-driven, and a delete
    /// event enqueues the removed node's dependents directly
    /// (pkg/controller/garbagecollector/graph_builder.go `processGraphChanges`
    /// -> `attemptToDelete`). Port the reaction: a registry watch kicks the
    /// scan instead of waiting out the backoff.
    #[tokio::test]
    async fn watch_kick_collects_orphan_without_waiting_out_backoff() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        // Base 30s / max 60s: any purely poll-driven GC needs >= 30s to look
        // again, so collecting inside the 5s assertion below can only happen
        // because the delete event woke it.
        let gc = Arc::new(GarbageCollector::with_scan_intervals(
            storage.clone(),
            Duration::from_secs(30),
            Duration::from_secs(60),
        ));

        let owner_uid = "rc-watch-kick-uid";
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        storage.create(rc_key, &rc).await.unwrap();

        let pod_key = "/registry/pods/default/rc-pod";
        let mut pod_meta = ObjectMeta::new("rc-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.uid = "rc-watch-kick-pod-uid".to_string();
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        storage.create(pod_key, &pod).await.unwrap();

        let runner = Arc::clone(&gc);
        let handle = tokio::spawn(async move { runner.run().await });

        // Let the opening scan finish and settle into its (backed-off) sleep.
        // Nothing is collectable yet: the owner still exists.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            storage.get::<Value>(pod_key).await.is_ok(),
            "pod must survive while its owner lives"
        );

        storage.delete(rc_key).await.unwrap();

        let collected = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage.get::<Value>(pod_key).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        handle.abort();

        assert!(
            collected.is_ok(),
            "orphan must be collected off the owner's delete event, not after \
             the {:?} idle backoff",
            Duration::from_secs(60)
        );
    }

    /// A Terminating namespace still holding `spec.finalizers` must NOT be
    /// swept by the GC's no-finalizers backstop.
    ///
    /// `ObjectMeta::has_finalizers()` reads `metadata.finalizers`. A namespace
    /// keeps its finalizer in **`spec.finalizers`**, so the backstop concluded
    /// "no finalizers remaining" for every Terminating namespace and fired a
    /// DELETE on each scan. Against an api-server those are correctly refused
    /// (upstream `Delete` returns the object untouched while finalizers remain,
    /// pkg/registry/core/namespace/storage/storage.go:250-252), so the sweep
    /// just re-logged
    /// "Deleting resource (no finalizers remaining): /registry/namespaces/..."
    /// on a loop — observed 8 times in 93s against kine on a namespace that
    /// took 118s to finalize (#1846).
    ///
    /// Upstream states the rule as a namespace-specific override of exactly the
    /// hook this sweep stands in for (#1828):
    ///
    ///     func ShouldDeleteNamespaceDuringUpdate(...) bool {
    ///         return len(ns.Spec.Finalizers) == 0 &&
    ///             genericregistry.ShouldDeleteDuringUpdate(ctx, key, obj, existing)
    ///     }
    ///
    /// (same file, :257-265)
    #[tokio::test]
    async fn terminating_namespace_with_spec_finalizers_is_not_swept() {
        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let key = "/registry/namespaces/ns-1846";
        let mut meta = ObjectMeta::new("ns-1846");
        meta.uid = "ns-1846-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now());
        // Empty, exactly as a real namespace has it: the finalizer lives in
        // spec, not metadata.
        meta.finalizers = None;

        let namespace = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": serde_json::to_value(&meta).unwrap(),
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Terminating"},
        });
        storage.create(key, &namespace).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        assert!(
            storage.get::<Value>(key).await.is_ok(),
            "namespace must survive the sweep while spec.finalizers is non-empty"
        );

        // Once the namespace controller has cleared spec.finalizers the
        // backstop must still finish the job, or clearing them would leak the
        // object. This half guards against "fixing" the bug by disabling the
        // sweep for namespaces outright.
        let mut finalized: Value = storage.get(key).await.unwrap();
        finalized["spec"]["finalizers"] = serde_json::json!([]);
        storage.update(key, &finalized).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        assert!(
            storage.get::<Value>(key).await.is_err(),
            "namespace must be removed once spec.finalizers is empty"
        );
    }

    /// Every dependent operation blocks for a beat, so a serial loop cannot
    /// hide behind a fast in-memory backend.
    struct SlowStorage {
        inner: Arc<MemoryStorage>,
    }

    impl SlowStorage {
        fn new(inner: Arc<MemoryStorage>) -> Self {
            Self { inner }
        }

        async fn stall() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[async_trait::async_trait]
    impl Storage for SlowStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }

        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }

        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            Self::stall().await;
            self.inner.update_raw(key, value).await
        }

        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            Self::stall().await;
            self.inner.delete(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// Seed an owner marked for deletion under `policy`, plus `count`
    /// dependents pointing at it. Returns the storage the GC should run on.
    async fn seed_owner_with_dependents(
        policy: &str,
        count: usize,
        block_owner_deletion: bool,
    ) -> Arc<SlowStorage> {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(SlowStorage::new(Arc::new(MemoryStorage::new())));

        let owner_uid = "fanout-rc-uid";
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec![policy.to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": count},
        });
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest.rc",
                &rc,
            )
            .await
            .unwrap();

        for i in 0..count {
            let name = format!("fanout-pod-{i}");
            let mut meta = ObjectMeta::new(name.clone());
            meta.namespace = Some("default".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "simpletest.rc".to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(block_owner_deletion),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/default/{name}"), &pod)
                .await
                .unwrap();
        }

        storage
    }

    /// Orphaning an owner's dependents must fan out, not walk them one at a
    /// time.
    ///
    /// Upstream starts **one goroutine per dependent** and waits for the lot
    /// (`pkg/controller/garbagecollector/garbagecollector.go:673-696`):
    ///
    /// ```text
    /// errCh := make(chan error, len(dependents))
    /// wg := sync.WaitGroup{}
    /// wg.Add(len(dependents))
    /// for i := range dependents {
    ///     go func(dependent *node) {
    ///         defer wg.Done()
    ///         ...
    ///         _, err = gc.patch(dependent, p, ...)
    /// ```
    ///
    /// This is load-bearing for the deadline, and the e2e test says so in as
    /// many words (`test/e2e/apimachinery/garbage_collector.go:410-416`):
    ///
    /// ```text
    /// // Orphaning the 100 pods takes 100 PATCH operations. The default qps of
    /// // a client is 5. If the qps is saturated, it will take 20s to orphan
    /// // the pods.
    /// ```
    ///
    /// 100 PATCHes concurrently, paced by the client limiter, is ~20s against
    /// a 180s budget. 100 PATCHes serially is 100 round-trip latencies, and
    /// that is what timed out `should orphan pods created by rc if delete
    /// options say so [Serial] [Conformance]` on the kine leg.
    #[tokio::test]
    async fn orphan_dependents_fan_out_instead_of_walking_serially() {
        const DEPENDENTS: usize = 20;
        let storage = seed_owner_with_dependents("orphan", DEPENDENTS, false).await;
        let gc = GarbageCollector::new(storage.clone());

        gc.scan_and_collect().await.unwrap();

        // The work still has to be correct: every pod keeps living, with the
        // owner reference stripped.
        let pods: Vec<serde_json::Value> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), DEPENDENTS, "orphaned pods must survive");
        for pod in &pods {
            let refs = pod.get("metadata").and_then(|m| m.get("ownerReferences"));
            assert!(
                refs.is_none() || refs.unwrap().as_array().is_some_and(|a| a.is_empty()),
                "owner reference must be stripped, got {refs:?}"
            );
        }

        let peak = gc.peak_concurrent_dependent_ops();
        assert!(
            peak >= DEPENDENTS / 2,
            "orphaning {DEPENDENTS} dependents peaked at {peak} concurrent operations; \
             upstream issues one per dependent at once, and a serial walk (peak 1) is \
             what blows the conformance deadline"
        );
    }

    /// The foreground cascade has the same problem and the same fix: upstream
    /// runs `ConcurrentGCSyncs` = 20 `attemptToDelete` workers
    /// (`pkg/controller/garbagecollector/config/v1alpha1/defaults.go:38`), so
    /// dependent deletes overlap rather than queueing behind each other.
    ///
    /// Guards `should keep the rc around until all its pods are deleted if the
    /// deleteOptions says so [Serial] [Conformance]`, which fails the same way
    /// when the cascade is serial.
    #[tokio::test]
    async fn foreground_cascade_deletes_dependents_concurrently() {
        const DEPENDENTS: usize = 20;
        let storage = seed_owner_with_dependents("foregroundDeletion", DEPENDENTS, true).await;
        let gc = GarbageCollector::new(storage.clone());

        gc.scan_and_collect().await.unwrap();

        let pods: Vec<serde_json::Value> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "the cascade must still delete every dependent, {} left",
            pods.len()
        );

        let peak = gc.peak_concurrent_dependent_ops();
        assert!(
            peak >= DEPENDENTS / 2,
            "foreground cascade of {DEPENDENTS} dependents peaked at {peak} concurrent \
             deletes; upstream runs 20 workers over the same queue"
        );
    }

    /// A dependent operation that panics must count as a failure, not as
    /// success.
    ///
    /// `run_dependent_ops` joins spawned tasks, and a `JoinError` (panic or
    /// cancellation) carries no per-op outcome. Treating it as "nothing went
    /// wrong" removes the `orphan` finalizer while the dependent still
    /// references the owner — precisely the race the aggregate-error path
    /// exists to prevent, and the reason upstream reports every dependent's
    /// error rather than the first (garbagecollector.go:697-706).
    #[tokio::test]
    async fn a_panicking_dependent_op_is_reported_not_swallowed() {
        struct PanicOnUpdate {
            inner: Arc<MemoryStorage>,
        }

        #[async_trait::async_trait]
        impl Storage for PanicOnUpdate {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                _key: &str,
                _value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                panic!("dependent write blew up");
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        // The object must exist: the executor reads before it writes, so a
        // missing key would short-circuit before reaching the panicking write.
        let inner = Arc::new(MemoryStorage::new());
        inner
            .create(
                "/registry/pods/default/boom",
                &serde_json::json!({
                    "metadata": {
                        "name": "boom", "uid": "boom-uid",
                        "ownerReferences": [{"uid": "gone-uid", "name": "gone"}]
                    }
                }),
            )
            .await
            .unwrap();
        let storage = Arc::new(PanicOnUpdate { inner });
        let gc = GarbageCollector::new(storage);

        let failures = gc
            .run_dependent_ops(vec![DependentOp::StripOwnerRef {
                key: "/registry/pods/default/boom".to_string(),
                owner_uid: "gone-uid".to_string(),
            }])
            .await;

        assert_eq!(
            failures.len(),
            1,
            "a panicking dependent op must be reported as a failure so the \
             orphan finalizer stays on; got {failures:?}"
        );
    }

    /// Stripping an ownerReference must survive a concurrent write to the
    /// dependent.
    ///
    /// The GC used to build the new object from the scan snapshot and write it
    /// whole. On a live cluster the kubelet is writing pod status the entire
    /// time, so by the time the write lands the stored resourceVersion has
    /// moved and the api-server rejects it:
    ///
    /// ```text
    /// Conflict: the object has been modified; please apply your changes to the
    /// latest version of simpletest-rc-to-be-deleted-gnk9l
    /// (stored resourceVersion: 7906, provided: 7830)
    /// ```
    ///
    /// That is not cosmetic. A failed strip leaves the dependent pointing at
    /// the owner, so `still_has_dependents` stays true and the owner keeps its
    /// `foregroundDeletion` finalizer until some later scan retries. Measured
    /// on run 33842493018: the owner was finally deleted at 06:36:39.9, and
    /// `should not delete dependents that have both valid owner and owner
    /// that's waiting for dependents to be deleted [Serial] [Conformance]`
    /// gave up at 06:36:43.5 — missed by less than one poll interval.
    ///
    /// Upstream never hits this because it PATCHes away the single
    /// ownerReference rather than rewriting the object
    /// (`GenerateDeleteOwnerRefStrategicMergeBytes`,
    /// pkg/controller/controller_ref_manager.go:572-589), so a concurrent
    /// status write is not a conflict at all. Our `Storage` trait has no patch
    /// verb, so this ports upstream's other sanctioned answer for
    /// read-modify-write, `retry.RetryOnConflict` with `DefaultRetry`
    /// (staging/src/k8s.io/client-go/util/retry/util.go:28-33, 103-105).
    #[tokio::test]
    async fn stripping_an_owner_ref_retries_on_conflict() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ConflictOnce {
            inner: Arc<MemoryStorage>,
            conflicts_left: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl Storage for ConflictOnce {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                if self
                    .conflicts_left
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
                    return Err(rusternetes_common::Error::Conflict(
                        "the object has been modified; please apply your changes to the \
                         latest version (stored resourceVersion: 7906, provided: 7830)"
                            .to_string(),
                    ));
                }
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        let inner = Arc::new(MemoryStorage::new());
        let key = "/registry/pods/default/p1";
        inner
            .create(
                key,
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {
                        "name": "p1", "namespace": "default", "uid": "p1-uid",
                        "ownerReferences": [
                            {"apiVersion": "v1", "kind": "ReplicationController",
                             "name": "rc1", "uid": "rc1-uid", "blockOwnerDeletion": true},
                            {"apiVersion": "v1", "kind": "ReplicationController",
                             "name": "rc2", "uid": "rc2-uid"}
                        ]
                    }
                }),
            )
            .await
            .unwrap();

        // Two conflicts in a row, inside upstream's budget of 5 attempts.
        let storage = Arc::new(ConflictOnce {
            inner: inner.clone(),
            conflicts_left: AtomicUsize::new(2),
        });
        let gc = GarbageCollector::new(storage);

        let failures = gc
            .run_dependent_ops(vec![DependentOp::StripOwnerRef {
                key: key.to_string(),
                owner_uid: "rc1-uid".to_string(),
            }])
            .await;

        assert!(
            failures.is_empty(),
            "a conflict must be retried, not reported as a failure to orphan: {failures:?}"
        );

        // And the strip must actually have happened, keeping the other owner.
        let stored: serde_json::Value = inner.get(key).await.unwrap();
        let refs = stored["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(refs.len(), 1, "only the target owner should be removed");
        assert_eq!(refs[0]["uid"], "rc2-uid", "the surviving owner must be rc2");
    }
}
