//! Tests for `src/async_preemption.rs` and `Scheduler::prepare_candidate_async`.
//!
//! Ports of upstream `preemption_test.go`: `TestPrepareCandidateAsyncSetsPreemptingSets`
//! (call order and set contents) and `TestIsPodRunningPreemption`, plus the
//! DefaultPreemption `PreEnqueue` gate through the scheduling queue.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use k3rs_common::resources::{Pod, PodSpec};
    pub use k3rs_scheduler::async_preemption::{
        AsyncPreemption, DefaultPreemptionPreEnqueue, PendingVictim,
    };
    pub use k3rs_scheduler::queue::scheduling_queue::PreEnqueuePlugin;
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use k3rs_common::resources::PodStatus;
    use k3rs_scheduler::profile::Profile;
    use k3rs_scheduler::queue::nominator::NominatingInfo;
    use k3rs_scheduler::queue::scheduling_gates::build_pre_enqueue_plugin_map_with_preemption;
    use k3rs_scheduler::queue::scheduling_queue::{
        priority_sort_less, PriorityQueue, PriorityQueueOptions,
    };
    use k3rs_scheduler::scheduler::Scheduler;
    use k3rs_storage::{MemoryStorage, Storage};
    use std::time::Duration;

    fn pod(name: &str, uid: &str, node: &str, priority: i32) -> Pod {
        let mut p = Pod::new(
            name,
            PodSpec {
                node_name: (!node.is_empty()).then(|| node.to_string()),
                priority: Some(priority),
                ..Default::default()
            },
        );
        p.metadata.namespace = Some("default".to_string());
        p.metadata.uid = uid.to_string();
        p
    }

    async fn stored(storage: &MemoryStorage, p: &Pod) {
        storage
            .create(&format!("/registry/pods/default/{}", p.metadata.name), p)
            .await
            .unwrap();
    }

    fn queue_with_preemption(state: &Arc<AsyncPreemption>) -> Arc<PriorityQueue> {
        let profile = Profile::named("default-scheduler");
        PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                queueing_hint_map:
                    k3rs_scheduler::queue::hints::build_queueing_hint_map_per_profile(
                        [&profile],
                        &Default::default(),
                    ),
                pre_enqueue_plugin_map: build_pre_enqueue_plugin_map_with_preemption(
                    [&profile],
                    Some(state),
                ),
                ..Default::default()
            },
        )
    }

    async fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if done() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// `TestIsPodRunningPreemption` (preemption_test.go:1467-1589). The pod
    /// lister is replaced by [`AsyncPreemption::observe_pod`] (module docs).
    #[test]
    fn is_pod_running_preemption() {
        let victim = pod("victim1", "victim1", "node", 5);
        let mut terminating = pod("victim-deleted", "victim-deleted", "node", 5);
        terminating.metadata.deletion_timestamp = Some(chrono::Utc::now());

        // preemptor not in preempting set
        let s = AsyncPreemption::new();
        assert!(!s.is_pod_running_preemption("preemptor"));

        // not in preempting set, last-victim map not empty
        s.set_last_victim("preemptor", &victim);
        assert!(!s.is_pod_running_preemption("preemptor"));
        s.finish("preemptor");

        // in preempting set, no last victim registered for it
        s.start("preemptor");
        s.set_last_victim("otherPod", &victim);
        assert!(s.is_pod_running_preemption("preemptor"));

        // in preempting set, last victim registered, still running
        s.set_last_victim("preemptor", &victim);
        assert!(s.is_pod_running_preemption("preemptor"));
        // an unrelated terminating pod does not end it
        s.observe_pod(&terminating, false);
        s.observe_pod(&pod("victim2", "victim2", "node", 5), false);
        assert!(s.is_pod_running_preemption("preemptor"));

        // victim has a deletion timestamp: preemption is done
        let s = AsyncPreemption::new();
        s.start("preemptor");
        s.set_last_victim("preemptor", &terminating);
        s.observe_pod(&terminating, false);
        assert!(!s.is_pod_running_preemption("preemptor"));

        // victim deleted: preemption is done
        let s = AsyncPreemption::new();
        s.start("preemptor");
        s.set_last_victim("preemptor", &victim);
        s.observe_pod(&victim, true);
        assert!(!s.is_pod_running_preemption("preemptor"));
    }

    /// `DefaultPreemption.PreEnqueue` (default_preemption.go:134-144).
    #[test]
    fn pre_enqueue_gates_a_preempting_pod() {
        let state = AsyncPreemption::new();
        let pl = DefaultPreemptionPreEnqueue::new(Arc::clone(&state));
        let preemptor = pod("p", "p-uid", "", 100);
        assert!(pl.pre_enqueue(&preemptor).is_success());
        state.start("p-uid");
        assert_eq!(
            pl.pre_enqueue(&preemptor),
            k3rs_scheduler::fwk::Status::new(
                k3rs_scheduler::fwk::Code::UnschedulableAndUnresolvable,
                vec!["waiting for the preemption for this pod to be finished".to_string()]
            )
        );
        state.finish("p-uid");
        assert!(pl.pre_enqueue(&preemptor).is_success());
    }

    /// The default profile enables DefaultPreemption at PreEnqueue after
    /// SchedulingGates (`profile.PreEnqueuePlugins()`).
    #[test]
    fn default_profile_instantiates_default_preemption_pre_enqueue() {
        let profile = Profile::named("default-scheduler");
        let state = AsyncPreemption::new();
        let map = build_pre_enqueue_plugin_map_with_preemption([&profile], Some(&state));
        let names: Vec<&str> = map["default-scheduler"].iter().map(|p| p.name()).collect();
        assert_eq!(names, ["SchedulingGates", "DefaultPreemption"]);
    }

    /// End to end through the queue: a pod running its preemption never
    /// reaches activeQ (`runPreEnqueuePlugins` in `moveToActiveQ`), and the
    /// `Activate` that follows a failed preemption releases it.
    #[tokio::test]
    async fn preempting_pod_stays_out_of_active_q_until_finished() {
        let state = AsyncPreemption::new();
        let queue = queue_with_preemption(&state);
        let preemptor = pod("p", "p-uid", "", 100);

        state.start("p-uid");
        queue.add(preemptor.clone());
        assert!(queue.pods_in_active_q().is_empty());
        assert_eq!(queue.unschedulable_pods().len(), 1);

        state.finish("p-uid");
        queue.activate(std::slice::from_ref(&preemptor));
        assert_eq!(queue.pods_in_active_q().len(), 1);
        assert!(queue.unschedulable_pods().is_empty());
    }

    /// `TestPrepareCandidateAsyncSetsPreemptingSets`
    /// (preemption_test.go:1242-1439): the preemptor is in `preempting` before
    /// every `PreemptPod`, the last victim is evicted last and only then is in
    /// `lastVictimsPendingPreemption`, and both sets are empty afterwards.
    async fn prepare_candidate_async_sets_preempting_sets(victim_names: &[&str]) {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = Arc::new(Scheduler::new_with_name(
            storage.clone(),
            1,
            "default-scheduler".to_string(),
        ));
        let queue = queue_with_preemption(&scheduler.preemption);
        let mut victims = Vec::new();
        for name in victim_names {
            let v = pod(name, name, "node1", 50);
            stored(&storage, &v).await;
            victims.push(v);
        }
        let preemptor = pod("preemptor", "preemptor", "", 1000);

        let errors: Arc<Mutex<Vec<String>>> = Arc::default();
        let calls = Arc::new(Mutex::new(0usize));
        let state = Arc::clone(&scheduler.preemption);
        let total = victims.len();
        let last_name = victims.last().map(|v| v.metadata.name.clone());
        let (e, c) = (Arc::clone(&errors), Arc::clone(&calls));
        *scheduler.before_preempt_pod.lock().unwrap() = Some(Arc::new(move |victim: &Pod| {
            let mut n = c.lock().unwrap();
            *n += 1;
            let mut e = e.lock().unwrap();
            if !state.preempting().contains("preemptor") {
                e.push(format!(
                    "preempting set lacks the preemptor before {}",
                    victim.metadata.name
                ));
            }
            let pending = state.last_victims_pending_preemption();
            if Some(&victim.metadata.name) == last_name.as_ref() {
                if *n != total {
                    e.push(format!("last victim called as no. {n}, want {total}"));
                }
                if pending.get("preemptor").map(|v| v.name.as_str())
                    != Some(victim.metadata.name.as_str())
                {
                    e.push(format!("last victim not registered: {pending:?}"));
                }
            } else {
                if *n >= total {
                    e.push(format!("{} called as last (no. {n})", victim.metadata.name));
                }
                if pending.contains_key("preemptor") {
                    e.push(format!(
                        "last victim registered before {}: {pending:?}",
                        victim.metadata.name
                    ));
                }
            }
        }));

        assert!(scheduler.preemption.preempting().is_empty());
        assert!(scheduler
            .preemption
            .last_victims_pending_preemption()
            .is_empty());

        scheduler.prepare_candidate_async(&queue, &preemptor, "node1", &victims);

        // Marked synchronously, before the task runs (`ev.preempting.Insert`
        // precedes `go func()`).
        if total > 0 {
            assert!(scheduler.preemption.preempting().contains("preemptor"));
        }
        let state = Arc::clone(&scheduler.preemption);
        assert!(
            wait_until(|| state.preempting().is_empty()).await,
            "timed out waiting for the preempting set to empty"
        );
        assert!(scheduler
            .preemption
            .last_victims_pending_preemption()
            .is_empty());
        assert_eq!(*calls.lock().unwrap(), total);
        assert_eq!(*errors.lock().unwrap(), Vec::<String>::new());
        for v in &victims {
            let got: Pod = storage
                .get(&format!("/registry/pods/default/{}", v.metadata.name))
                .await
                .unwrap();
            assert!(got.metadata.deletion_timestamp.is_some(), "evicted");
        }
    }

    #[tokio::test]
    async fn prepare_candidate_async_no_victims() {
        prepare_candidate_async_sets_preempting_sets(&[]).await;
    }

    #[tokio::test]
    async fn prepare_candidate_async_one_victim() {
        prepare_candidate_async_sets_preempting_sets(&["victim1"]).await;
    }

    #[tokio::test]
    async fn prepare_candidate_async_two_victims() {
        prepare_candidate_async_sets_preempting_sets(&["victim1", "victim2"]).await;
    }

    #[tokio::test]
    async fn prepare_candidate_async_many_victims() {
        let names: Vec<String> = (0..40).map(|i| format!("v{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        prepare_candidate_async_sets_preempting_sets(&refs).await;
    }

    /// Victims whose deletion already started are skipped, so with none left
    /// nothing is marked and no task runs.
    #[tokio::test]
    async fn prepare_candidate_async_skips_terminating_victims() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = Arc::new(Scheduler::new_with_name(
            storage.clone(),
            1,
            "default-scheduler".to_string(),
        ));
        let queue = queue_with_preemption(&scheduler.preemption);
        let mut dying = pod("dying", "dying", "node1", 50);
        dying.metadata.deletion_timestamp = Some(chrono::Utc::now());
        stored(&storage, &dying).await;
        let preemptor = pod("preemptor", "preemptor", "", 1000);

        scheduler.prepare_candidate_async(&queue, &preemptor, "node1", &[dying]);

        assert!(scheduler.preemption.preempting().is_empty());
    }

    /// The task clears the nomination of the lower-priority pods nominated to
    /// the node (`getLowerPriorityNominatedPods` + `clearNominatedNodeName`).
    #[tokio::test]
    async fn prepare_candidate_async_clears_lower_priority_nominations() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler = Arc::new(Scheduler::new_with_name(
            storage.clone(),
            1,
            "default-scheduler".to_string(),
        ));
        let queue = queue_with_preemption(&scheduler.preemption);
        let victim = pod("victim", "victim", "node1", 1);
        stored(&storage, &victim).await;
        let mut low = pod("low", "low", "", 5);
        low.status = Some(PodStatus {
            nominated_node_name: Some("node1".to_string()),
            ..Default::default()
        });
        stored(&storage, &low).await;
        queue.add_nominated_pod(&low, Some(&NominatingInfo::noop()));
        let preemptor = pod("preemptor", "preemptor", "", 1000);

        scheduler.prepare_candidate_async(&queue, &preemptor, "node1", &[victim]);

        let state = Arc::clone(&scheduler.preemption);
        assert!(wait_until(|| state.preempting().is_empty()).await);
        let got: Pod = storage.get("/registry/pods/default/low").await.unwrap();
        assert_eq!(got.status.and_then(|s| s.nominated_node_name), None);
    }
}
