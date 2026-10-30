//! Unit tests moved from `src/controllers/pod_informer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_controller_manager::controllers::controller_utils::is_pod_ready;
    pub use rusternetes_controller_manager::controllers::job_tracking::has_job_tracking_finalizer;
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent, WorkQueue};
    pub use std::collections::hash_map::DefaultHasher;
    pub use std::collections::HashMap;
    pub use std::future::Future;
    pub use std::hash::{Hash, Hasher};
    pub use std::pin::Pin;
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{error, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::pod_informer::*;
    use serde_json::json;

    fn pod_json_with(
        name: &str,
        owner: Option<(&str, bool)>,
        labels: &[(&str, &str)],
        terminating: bool,
        rv: Option<&str>,
    ) -> String {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = json!({
            "metadata": {"name": name, "namespace": "default", "labels": labels},
            "spec": {"containers": []},
        });
        if let Some((o, controller)) = owner {
            v["metadata"]["ownerReferences"] = json!([{
                "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": o, "uid": "u",
                "controller": controller
            }]);
        }
        if terminating {
            v["metadata"]["deletionTimestamp"] = json!("2026-10-15T00:00:00Z");
        }
        if let Some(rv) = rv {
            v["metadata"]["resourceVersion"] = json!(rv);
        }
        v.to_string()
    }

    fn pod_json(name: &str, owner: Option<&str>, terminating: bool) -> String {
        pod_json_with(name, owner.map(|o| (o, true)), &[], terminating, None)
    }

    fn key(name: &str) -> String {
        format!("/registry/pods/default/{name}")
    }

    fn pod(name: &str, owner: &str, rv: Option<&str>) -> Pod {
        serde_json::from_str(&pod_json_with(name, Some((owner, true)), &[], false, rv)).unwrap()
    }

    fn rs_ref(name: &str) -> ControllerRef {
        ControllerRef {
            kind: "ReplicaSet".into(),
            name: name.into(),
            uid: "u".into(),
        }
    }

    fn effects(observation: Option<PodObservation>) -> Vec<PodEffect> {
        observation.expect("attributable to a pod").effects
    }

    /// `processDeltas` (controller.go:581-607): the first sight of an object is `OnAdd`, every later
    /// event for it `OnUpdate` (shared_informer.go); only `addPod` observes a
    /// creation (replica_set.go:436).
    #[test]
    fn only_the_first_sight_of_a_pod_is_a_creation() {
        let cache = PodInformerCache::new();
        let first = cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            first,
            Some(PodObservation {
                pod_key: "default/p".into(),
                effects: vec![PodEffect::ObserveCreation(rs_ref("rs"))],
            })
        );
        // A duplicate Added (the re-list racing the watch) and an ordinary
        // Modified are updates: they wake the owner, observe nothing.
        for event in [
            WatchEvent::Added(key("p"), pod_json("p", Some("rs"), false)),
            WatchEvent::Modified(key("p"), pod_json("p", Some("rs"), false)),
        ] {
            assert_eq!(
                effects(cache.on_event(&event)),
                vec![PodEffect::Enqueue(rs_ref("rs"))]
            );
        }
    }

    /// A Modified for a pod the informer never saw is an add (`processDeltas`
    /// treats Updated of an unknown object as `OnAdd`).
    #[test]
    fn a_modified_pod_never_seen_is_an_add() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("rs"), false)
            ))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
    }

    /// `addPod`: "is already pending deletion. Prevent the pod from being a
    /// creation observation" (replica_set.go:418-422); `updatePod` observes a
    /// deletion as soon as the deletionTimestamp is set (:468-480).
    #[test]
    fn a_terminating_pod_is_a_deletion_not_a_creation() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(
                key("a"),
                pod_json("a", Some("rs"), true)
            ))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
        cache.on_event(&WatchEvent::Added(
            key("b"),
            pod_json("b", Some("rs"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("b"),
                pod_json("b", Some("rs"), true)
            ))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
    }

    /// `updatePod` of a terminating pod whose labels also changed runs
    /// `deletePod` for the old pod too (:468-480), so a controller the old
    /// labels pointed at is not left waiting.
    #[test]
    fn a_terminating_relabelled_pod_observes_the_old_controller_too() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json_with("p", Some(("old", true)), &[("a", "1")], false, None),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json_with("p", Some(("new", true)), &[("a", "2")], true, None),
            ))),
            vec![
                PodEffect::ObserveDeletion(rs_ref("new")),
                PodEffect::ObserveDeletion(rs_ref("old")),
            ]
        );
    }

    /// `deletePod` reads the controller from the tombstone's last known state;
    /// the pod is forgotten so a recreation of the name is an add again.
    #[test]
    fn a_deleted_pod_is_forgotten() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        let deleted = cache.on_event(&WatchEvent::Deleted(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            deleted,
            Some(PodObservation {
                pod_key: "default/p".into(),
                effects: vec![PodEffect::ObserveDeletion(rs_ref("rs"))],
            })
        );
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(
                key("p"),
                pod_json("p", Some("rs"), false)
            ))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
    }

    /// An undecodable DELETED value falls back to the cached controller
    /// (`DeletedFinalStateUnknown`).
    #[test]
    fn a_delete_with_an_unreadable_value_uses_the_cached_controller() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Deleted(key("p"), "{".into()))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
    }

    /// `deletePod`: "No controller should care about orphans being deleted"
    /// (:553-557); and a reference that is not `controller: true` is no
    /// controller (`GetControllerOf`).
    #[test]
    fn orphans_and_non_controller_refs_are_not_controlled() {
        let cache = PodInformerCache::new();
        let plain = pod_json_with("p", Some(("rs", false)), &[("a", "1")], false, None);
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(key("p"), plain.clone()))),
            vec![PodEffect::EnqueueMatching(
                [("a".to_string(), "1".to_string())].into_iter().collect()
            )]
        );
        assert!(effects(cache.on_event(&WatchEvent::Deleted(key("p"), plain))).is_empty());
    }

    /// `updatePod` (:458-529) for an unchanged pod: the same resourceVersion
    /// is a periodic resync and does nothing; an orphan wakes matching
    /// controllers only when its labels or controller reference changed.
    #[test]
    fn update_pod_resync_and_orphan_change_rules() {
        let cache = PodInformerCache::new();
        let labelled = |l: &str, rv: &str| pod_json_with("p", None, &[("a", l)], false, Some(rv));
        cache.on_event(&WatchEvent::Added(key("p"), labelled("1", "1")));
        // Same RV: ignored, even though the labels differ.
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("2", "1")))).is_empty()
        );
        // New RV, same labels: the orphan is not worth waking anyone for.
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("2", "2")))).is_empty()
        );
        // New RV, changed labels: wake whoever matches now.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("3", "3")))),
            vec![PodEffect::EnqueueMatching(
                [("a".to_string(), "3".to_string())].into_iter().collect()
            )]
        );
    }

    /// `updatePod`: when the controller reference changes, the old controller
    /// is synced first, then the current one (:484-514).
    #[test]
    fn a_changed_controller_ref_wakes_old_then_current() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("old"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("new"), false)
            ))),
            vec![
                PodEffect::Enqueue(rs_ref("old")),
                PodEffect::Enqueue(rs_ref("new"))
            ]
        );
    }

    /// `DeltaFIFO.Replace`: after a watch gap the re-list yields adds for new
    /// pods and deletes for vanished ones, and nothing for pods already known
    /// at the same resourceVersion.
    #[test]
    fn a_relist_reports_what_the_watch_gap_hid() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("kept"),
            pod_json_with("kept", Some(("rs", true)), &[], false, Some("1")),
        ));
        cache.on_event(&WatchEvent::Added(
            key("gone"),
            pod_json("gone", Some("rs"), false),
        ));

        let observed = cache.replace(&[pod("kept", "rs", Some("1")), pod("new", "rs", Some("1"))]);

        assert_eq!(observed.len(), 2);
        assert!(observed.contains(&PodObservation {
            pod_key: "default/new".into(),
            effects: vec![PodEffect::ObserveCreation(rs_ref("rs"))],
        }));
        assert!(observed.contains(&PodObservation {
            pod_key: "default/gone".into(),
            effects: vec![PodEffect::ObserveDeletion(rs_ref("rs"))],
        }));
        // A second identical re-list is silent.
        assert!(cache
            .replace(&[pod("kept", "rs", Some("1")), pod("new", "rs", Some("1"))])
            .is_empty());
    }

    fn with_ready(json: String, ready: bool) -> String {
        let mut v: serde_json::Value = serde_json::from_str(&json).unwrap();
        v["status"] = json!({"conditions": [{
            "type": "Ready", "status": if ready { "True" } else { "False" }
        }]});
        v.to_string()
    }

    fn owned(rv: &str, ready: bool) -> String {
        with_ready(
            pod_json_with("p", Some(("rs", true)), &[], false, Some(rv)),
            ready,
        )
    }

    /// `updatePod` (replica_set.go:509): `!IsPodReady(old) && IsPodReady(cur)`
    /// follows `enqueueRS` with `enqueueRSAfter`.
    #[test]
    fn a_ready_transition_schedules_the_availability_recheck() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(key("p"), owned("1", false)));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("2", true)))),
            vec![
                PodEffect::Enqueue(rs_ref("rs")),
                PodEffect::EnqueueAfterMinReady(rs_ref("rs"))
            ]
        );
        // Already Ready on both sides: no recheck.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("3", true)))),
            vec![PodEffect::Enqueue(rs_ref("rs"))]
        );
        // Ready to not Ready is not a transition to Ready.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("4", false)))),
            vec![PodEffect::Enqueue(rs_ref("rs"))]
        );
    }

    /// `addPod` never schedules the recheck, and an orphan has no controller
    /// whose `MinReadySeconds` could apply (:493-515 sits inside
    /// `if curControllerRef != nil`).
    #[test]
    fn only_a_controlled_update_schedules_the_recheck() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(key("p"), owned("1", true)))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
        let orphan = |rv: &str, ready: bool| {
            with_ready(
                pod_json_with("o", None, &[("a", "1")], false, Some(rv)),
                ready,
            )
        };
        cache.on_event(&WatchEvent::Added(key("o"), orphan("1", false)));
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("o"), orphan("2", true)))).is_empty()
        );
    }

    struct Target {
        min_ready_seconds: i32,
    }

    impl PodEventTarget for Target {
        fn queue_prefix(&self) -> &'static str {
            "replicasets"
        }
        async fn resolve_controller_ref(
            &self,
            _namespace: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            Some(ResolvedController {
                name: controller_ref.name.clone(),
                min_ready_seconds: self.min_ready_seconds,
            })
        }
        async fn pod_controllers(&self, _: &str, _: &HashMap<String, String>) -> Vec<String> {
            Vec::new()
        }
        fn creation_observed(&self, _: &str) {}
        fn deletion_observed(&self, _: &str, _: &str) {}
    }

    fn recheck() -> PodObservation {
        PodObservation {
            pod_key: "default/p".into(),
            effects: vec![PodEffect::EnqueueAfterMinReady(rs_ref("rs"))],
        }
    }

    /// `rs.Spec.MinReadySeconds > 0` gates `enqueueRSAfter`: the key is not
    /// queued now, and is queued once the duration has passed.
    #[tokio::test]
    async fn the_recheck_enqueues_after_min_ready_seconds() {
        let queue = WorkQueue::new();
        apply_pod_observation(
            &Target {
                min_ready_seconds: 1,
            },
            &queue,
            &recheck(),
        )
        .await;
        assert_eq!(queue.len().await, 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), queue.get())
                .await
                .is_err(),
            "must not be available before minReadySeconds"
        );
        let got = tokio::time::timeout(Duration::from_secs(3), queue.get())
            .await
            .expect("available after minReadySeconds");
        assert_eq!(got.as_deref(), Some("replicasets/default/rs"));
    }

    #[tokio::test]
    async fn no_min_ready_seconds_means_no_recheck() {
        let queue = WorkQueue::new();
        apply_pod_observation(
            &Target {
                min_ready_seconds: 0,
            },
            &queue,
            &recheck(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), queue.get())
                .await
                .is_err()
        );
    }

    /// A target that records what the informer's handlers lowered.
    struct Recording {
        prefix: &'static str,
        log: Mutex<Vec<String>>,
    }

    impl Recording {
        fn new(prefix: &'static str) -> Arc<Self> {
            Arc::new(Self {
                prefix,
                log: Mutex::new(Vec::new()),
            })
        }
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    impl PodEventTarget for Recording {
        fn queue_prefix(&self) -> &'static str {
            self.prefix
        }
        async fn resolve_controller_ref(
            &self,
            _: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            Some(ResolvedController {
                name: controller_ref.name.clone(),
                min_ready_seconds: 0,
            })
        }
        async fn pod_controllers(&self, _: &str, _: &HashMap<String, String>) -> Vec<String> {
            Vec::new()
        }
        fn creation_observed(&self, key: &str) {
            self.log.lock().unwrap().push(format!("created {key}"));
        }
        fn deletion_observed(&self, key: &str, pod_key: &str) {
            self.log
                .lock()
                .unwrap()
                .push(format!("deleted {key} {pod_key}"));
        }
    }

    /// `processDeltas` classifies once and `sharedProcessor.distribute` tells
    /// every listener (controller.go:581-607, shared_informer.go:863): two
    /// targets on one informer both see ONE add (so both lower their
    /// expectations), the next event as an update for both, and a pod that
    /// vanished across a re-list as a delete for both.
    #[tokio::test]
    async fn every_target_sees_the_same_add_update_and_relist_delete() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_millis(10),
        ));
        let rs = Recording::new("replicasets");
        let rc = Recording::new("replicationcontrollers");
        let (rs_queue, rc_queue) = (WorkQueue::new(), WorkQueue::new());
        informer
            .add_event_handler(rs.clone(), rs_queue.clone())
            .await;
        informer
            .add_event_handler(rc.clone(), rc_queue.clone())
            .await;

        informer
            .on_event(&WatchEvent::Added(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("1")),
            ))
            .await;
        informer
            .on_event(&WatchEvent::Modified(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("2")),
            ))
            .await;
        for t in [&rs, &rc] {
            assert_eq!(
                t.log(),
                vec!["created default/c"],
                "one add, then an update"
            );
        }
        assert_eq!(rs_queue.len().await, 1);
        assert_eq!(rc_queue.len().await, 1);

        // The pod is gone from storage; the re-list finds it vanished.
        informer.relist().await;
        for t in [&rs, &rc] {
            assert_eq!(
                t.log(),
                vec!["created default/c", "deleted default/c default/p"]
            );
        }
    }

    /// A handler added after the informer holds pods is replayed them as adds
    /// (`AddEventHandlerWithOptions`), so it does not miss a creation.
    #[tokio::test]
    async fn a_late_handler_is_replayed_the_held_pods() {
        use rusternetes_storage::memory::MemoryStorage;
        let informer = Arc::new(SharedPodInformer::new(
            Arc::new(MemoryStorage::new()),
            Duration::from_millis(10),
        ));
        informer
            .on_event(&WatchEvent::Added(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("1")),
            ))
            .await;
        let late = Recording::new("replicasets");
        informer
            .add_event_handler(late.clone(), WorkQueue::new())
            .await;
        assert_eq!(late.log(), vec!["created default/c"]);
    }

    /// A target whose controller-ref lookup parks until released, like the
    /// storage read `resolve_controller_ref` does in API mode.
    struct Gated {
        inner: Arc<Recording>,
        entered: tokio::sync::Notify,
        gate: tokio::sync::Semaphore,
    }

    impl PodEventTarget for Gated {
        fn queue_prefix(&self) -> &'static str {
            self.inner.queue_prefix()
        }
        async fn resolve_controller_ref(
            &self,
            ns: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
            self.inner.resolve_controller_ref(ns, controller_ref).await
        }
        async fn pod_controllers(&self, ns: &str, l: &HashMap<String, String>) -> Vec<String> {
            self.inner.pod_controllers(ns, l).await
        }
        fn creation_observed(&self, key: &str) {
            self.inner.creation_observed(key)
        }
        fn deletion_observed(&self, key: &str, pod_key: &str) {
            self.inner.deletion_observed(key, pod_key)
        }
    }

    /// `AddEventHandlerWithOptions` holds `s.blockDeltas` across
    /// `addListener` and the replay (shared_informer.go:698-699), and
    /// `HandleDeltas` takes the same lock around `processDeltas`
    /// (shared_informer.go:717-718), so an event arriving while the replay is
    /// in flight is delivered to the new handler afterwards, never lost.
    #[tokio::test]
    async fn an_event_during_a_late_handlers_replay_is_not_lost() {
        use rusternetes_storage::memory::MemoryStorage;
        let informer = Arc::new(SharedPodInformer::new(
            Arc::new(MemoryStorage::new()),
            Duration::from_millis(10),
        ));
        let added = |name: &str, rv: &str| {
            WatchEvent::Added(
                key(name),
                pod_json_with(name, Some(("c", true)), &[], false, Some(rv)),
            )
        };
        informer.on_event(&added("a", "1")).await;

        let late = Arc::new(Gated {
            inner: Recording::new("replicasets"),
            entered: tokio::sync::Notify::new(),
            gate: tokio::sync::Semaphore::new(0),
        });
        let register = {
            let (informer, late) = (informer.clone(), late.clone());
            tokio::spawn(async move { informer.add_event_handler(late, WorkQueue::new()).await })
        };
        late.entered.notified().await; // replay of "a" is in flight
        let event = {
            let informer = informer.clone();
            tokio::spawn(async move { informer.on_event(&added("b", "2")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        late.gate.add_permits(100);
        register.await.unwrap();
        event.await.unwrap();
        assert_eq!(
            late.inner.log(),
            vec!["created default/c", "created default/c"],
            "the replayed add of a and the live add of b"
        );
    }
}
