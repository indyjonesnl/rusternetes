//! Unit tests moved from `src/queue/scheduling_queue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_scheduler::profile::pod_scheduler_name;
    pub use rusternetes_scheduler::queue::events::*;
    pub use rusternetes_scheduler::queue::heap::Heap;
    pub use rusternetes_scheduler::queue::nominator::{
        NominatingInfo, Nominator, PodLister, PodRef,
    };
    pub use std::collections::{HashMap, HashSet, VecDeque};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, SystemTime, UNIX_EPOCH};
    pub use tokio::sync::Notify;
    pub use tracing::{debug, error, info, trace};
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::scheduling_queue::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn mk_pod(name: &str, ns: &str, uid: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {}
        }))
        .unwrap()
    }

    fn mk_pod_prio(name: &str, ns: &str, uid: &str, prio: i32) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"priority": prio}
        }))
        .unwrap()
    }

    fn mk_node() -> rusternetes_common::resources::Node {
        serde_json::from_value(serde_json::json!({"metadata": {"name": "node"}})).unwrap()
    }

    const NODE_ADD: ClusterEvent = ClusterEvent::new(EventResource::Node, ActionType::ADD);
    const PV_ADD: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolume, ActionType::ADD);
    const PV_UPDATE: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolume, ActionType::UPDATE);
    const PVC_ADD: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolumeClaim, ActionType::ADD);
    const CSI_NODE_UPDATE: ClusterEvent =
        ClusterEvent::new(EventResource::CsiNode, ActionType::UPDATE);

    fn hint_queue(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(QueueingHint::Queue)
        })
    }
    fn hint_skip(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(QueueingHint::QueueSkip)
        })
    }
    fn hint_err(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Err("unexpected error".to_string())
        })
    }

    fn hints(
        entries: Vec<(ClusterEvent, Vec<(&str, QueueingHintFn)>)>,
    ) -> QueueingHintMapPerProfile {
        let mut m: QueueingHintMap = HashMap::new();
        for (ev, fns) in entries {
            m.entry(ev)
                .or_default()
                .extend(fns.into_iter().map(|(n, f)| QueueingHintFunction {
                    plugin_name: n.to_string(),
                    queueing_hint_fn: f,
                }));
        }
        HashMap::from([(
            rusternetes_scheduler::profile::DEFAULT_SCHEDULER_NAME.to_string(),
            m,
        )])
    }

    /// Upstream tests hand the popped `podInfo` back to
    /// `AddUnschedulableIfNotPresent` with `UnschedulablePlugins` set, so
    /// `Attempts` carries over.
    fn rejected(mut p_info: QueuedPodInfo, plugins: &[&str]) -> QueuedPodInfo {
        p_info.unschedulable_plugins = plugins.iter().map(|s| s.to_string()).collect();
        p_info
    }

    fn new_test_queue(
        hint_map: QueueingHintMapPerProfile,
        clock: Arc<dyn Clock>,
    ) -> Arc<PriorityQueue> {
        PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                clock,
                queueing_hint_map: hint_map,
                ..Default::default()
            },
        )
    }

    /// `Test_isPodWorthRequeuing` (`scheduling_queue_test.go:4039-4347`).
    #[test]
    fn is_pod_worth_requeuing_table() {
        let count = Arc::new(AtomicUsize::new(0));
        let pod1 = mk_pod("pod1", "ns1", "1");
        let node = mk_node();
        struct Case {
            name: &'static str,
            unschedulable: &'static [&'static str],
            pending: &'static [&'static str],
            event: ClusterEvent,
            new_obj: Option<Arc<EventObject>>,
            expected: QueueingStrategy,
            expected_execution_count: usize,
            map: QueueingHintMapPerProfile,
        }
        let node_obj = || Some(Arc::new(mk_node()) as Arc<EventObject>);
        let cases = vec![
            Case {
                name: "return Queue when no queueing hint function is registered for the event",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 0,
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
            },
            Case {
                name: "Treat the event as Queue when QueueHintFn returns error",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(NODE_ADD, vec![("fooPlugin1", hint_err(&count))])]),
            },
            Case {
                name: "return Queue when the event is wildcard",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_UNSCHEDULABLE_TIMEOUT,
                new_obj: None,
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "return Queue when the event is wildcard and the wildcard targets the pod to be requeued right now",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_FORCE_ACTIVATE,
                new_obj: Some(Arc::new(mk_pod("pod1", "ns1", "1"))),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "return Skip when the event is wildcard, but the wildcard targets a different pod",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_FORCE_ACTIVATE,
                new_obj: Some(Arc::new(mk_pod("pod-different", "ns2", "2"))),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "interprets Queue from the Pending plugin as queueImmediately",
                unschedulable: &["fooPlugin1", "fooPlugin3"],
                pending: &["fooPlugin2"],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueImmediately,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![
                        // Queue → queueAfterBackoff, but continues because the
                        // Pod has PendingPlugins which can yield queueImmediately.
                        ("fooPlugin1", hint_queue(&count)),
                        // queueImmediately: the highest priority, returns at once.
                        ("fooPlugin2", hint_queue(&count)),
                        ("fooPlugin3", hint_queue(&count)),
                        ("fooPlugin4", hint_err(&count)),
                    ],
                )]),
            },
            Case {
                name: "interprets Queue from the Unschedulable plugin as queueAfterBackoff",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![("fooPlugin1", hint_skip(&count)), ("fooPlugin2", hint_queue(&count))],
                )]),
            },
            Case {
                name: "Queueing hint function that isn't from the plugin in UnschedulablePlugins/PendingPlugins is ignored",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![
                        ("fooPlugin1", hint_skip(&count)),
                        ("fooPlugin2", hint_skip(&count)),
                        ("fooPlugin3", hint_queue(&count)), // ignored
                    ],
                )]),
            },
            Case {
                name: "If event is specific Node update event, queueing hint function for NodeUpdate/UpdateNodeLabel is also executed",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_LABEL),
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![
                    (
                        ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_LABEL),
                        vec![("fooPlugin1", hint_queue(&count)), ("fooPlugin2", hint_queue(&count))],
                    ),
                    (
                        ClusterEvent::new(EventResource::Node, ActionType::UPDATE),
                        vec![("fooPlugin1", hint_queue(&count))],
                    ),
                    // not executed because NodeAdd is unrelated.
                    (NODE_ADD, vec![("fooPlugin1", hint_queue(&count))]),
                ]),
            },
            Case {
                name: "If event with '*' Resource, queueing hint function for specified Resource is also executed",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(
                    ClusterEvent::new(EventResource::WildCard, ActionType::ADD),
                    vec![("fooPlugin1", hint_queue(&count))],
                )]),
            },
            Case {
                name: "If event is a wildcard one, queueing hint function for all kinds of events is executed",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_LABEL | ActionType::UPDATE_NODE_TAINT,
                ),
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(
                    ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                    vec![("fooPlugin1", hint_queue(&count))],
                )]),
            },
        ];
        let _ = node;
        for c in cases {
            count.store(0, Ordering::SeqCst);
            let q = new_test_queue(c.map, Arc::new(RealClock));
            let mut p_info = QueuedPodInfo::for_lookup(pod1.clone(), c.unschedulable);
            p_info.pending_plugins = c.pending.iter().map(|s| s.to_string()).collect();
            let actual = q.is_pod_worth_requeuing(&p_info, &c.event, None, c.new_obj.as_deref());
            assert_eq!(actual, c.expected, "{}: isPodWorthRequeuing()", c.name);
            assert_eq!(
                count.load(Ordering::SeqCst),
                c.expected_execution_count,
                "{}: executed queueing hint functions",
                c.name
            );
        }
    }

    /// The API server defaults an unset `spec.schedulerName` to
    /// `default-scheduler` (`SetDefaults_PodSpec`); objects read straight from
    /// storage may still carry none. The hint map is keyed by profile name,
    /// so the lookup must use the same defaulting `responsibleForPod` does —
    /// otherwise every event degrades to "no map registered" → backoff.
    #[test]
    fn is_pod_worth_requeuing_defaults_unset_scheduler_name() {
        let count = Arc::new(AtomicUsize::new(0));
        let q = new_test_queue(
            hints(vec![(NODE_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
            Arc::new(RealClock),
        );
        let pod = mk_pod("pod1", "ns1", "1");
        assert!(pod.spec.as_ref().unwrap().scheduler_name.is_none());
        let p_info = QueuedPodInfo::for_lookup(pod, &["fooPlugin1"]);
        let node = mk_node();
        assert_eq!(
            q.is_pod_worth_requeuing(&p_info, &NODE_ADD, None, Some(&node)),
            QueueingStrategy::QueueSkip,
            "the default profile's hint must decide, not the missing-map fallback"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// `TestPriorityQueue_AddUnschedulableIfNotPresent` (`:945-973`), minus
    /// the nominator assertion.
    #[tokio::test]
    async fn add_unschedulable_if_not_present() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let unschedulable = mk_pod_prio("up", "ns1", "upns1", 0);
        let high_pri = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        // insert unschedulablePod and pop right after that because the
        // scheduling queue records it as in-flight.
        q.add(unschedulable.clone());
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "up");

        q.add(high_pri.clone());
        q.add_unschedulable_if_not_present(
            QueuedPodInfo::for_lookup(unschedulable.clone(), &["plugin"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "hpp");
        // unschedulablePod is inserted to the unschedulable pool because no
        // events happened during scheduling.
        assert_eq!(
            q.get_unschedulable_pod(&unschedulable)
                .map(|p| p.metadata.name),
            Some("up".to_string())
        );
    }

    /// `TestPriorityQueue_AddUnschedulableIfNotPresent_Backoff` (`:979-1037`).
    #[tokio::test]
    async fn add_unschedulable_if_not_present_backoff() {
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let total = 10;
        let mut expected = Vec::new();
        for i in 0..total {
            let p = mk_pod_prio(
                &format!("pod{i}"),
                &format!("ns{i}"),
                &format!("upns{i}"),
                i as i32,
            );
            expected.push(p.clone());
            q.add(p);
        }
        // Pop all pods except for the first one
        for i in (1..total).rev() {
            let p = q.pop().await.unwrap();
            assert_eq!(p.pod.metadata.name, expected[i].metadata.name);
        }
        // move all pods to active queue when we were trying to schedule them
        q.move_all_to_active_or_backoff_queue(&EVENT_UNSCHEDULABLE_TIMEOUT, None, None, None);
        let old_cycle = q.scheduling_cycle();
        let first = q.pop().await.unwrap();
        assert_eq!(first.pod.metadata.name, expected[0].metadata.name);
        // mark pods[1] ~ pods[totalNum-1] as unschedulable and add them back
        for p in expected.iter().skip(1) {
            q.add_unschedulable_if_not_present(
                QueuedPodInfo::for_lookup(p.clone(), &["plugin"]),
                old_cycle,
            )
            .unwrap();
        }
        // Since there was a move request at the same cycle as "oldCycle",
        // these pods should be in the backoff queue.
        for p in expected.iter().skip(1) {
            assert!(
                q.backoff_q_has(p),
                "Expected {} to be added to backoffQ.",
                p.metadata.name
            );
        }
    }

    /// `TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint`
    /// (`:1910-2062`), the non-gated rows (PreEnqueue plugins are covered by
    /// `gated_pods_follow_their_gating_plugin_events` below).
    #[tokio::test]
    async fn move_all_to_active_or_backoff_queue_with_queueing_hint() {
        let now = UNIX_EPOCH + Duration::from_secs(1_735_689_600); // 2025-01-01
        let count = Arc::new(AtomicUsize::new(0));
        let p = mk_pod("pod1", "ns1", "1");
        struct Case {
            name: &'static str,
            unschedulable: &'static [&'static str],
            pending: &'static [&'static str],
            hint: QueueingHintFn,
            duration: Duration,
            trigger_event: Option<ClusterEvent>,
            expected_q: &'static str,
        }
        let cases = vec![
            Case {
                // This pod has PendingPlugins and hence will be pushed directly to activeQ
                name: "Queue queues pod to activeQ",
                unschedulable: &[],
                pending: &["foo"],
                hint: hint_queue(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: ACTIVE_Q,
            },
            Case {
                // needs UnschedulableCount to make it backing off.
                name: "Queue queues pod to backoffQ if Pod is backing off",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_queue(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: BACKOFF_Q,
            },
            Case {
                // The pod failed once, which gets DefaultPodInitialBackoffDuration
                // as the penalty; +1s because the pod is still backing off if
                // `{backoff expiration time} == truncate({current time})`.
                name: "Queue queues pod to activeQ if Pod is not backing off",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_queue(&count),
                duration: DEFAULT_POD_INITIAL_BACKOFF_DURATION + Duration::from_secs(1),
                trigger_event: None,
                expected_q: ACTIVE_Q,
            },
            Case {
                name: "Skip queues pod to unschedulablePods",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_skip(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: UNSCHEDULABLE_Q,
            },
            Case {
                name: "Queue queues pod to backoffQ if Pod is not gated and the event is wildcard",
                unschedulable: &["foo"],
                pending: &[],
                hint: Arc::new(|_, _, _| {
                    Err(
                        "QueueingHintFn should not be called as trigger event is wildcard"
                            .to_string(),
                    )
                }),
                duration: Duration::ZERO,
                trigger_event: Some(EVENT_UNSCHEDULABLE_TIMEOUT),
                expected_q: BACKOFF_Q,
            },
        ];
        for c in cases {
            let clock = Arc::new(FakeClock::new(now));
            let q = new_test_queue(
                hints(vec![(NODE_ADD, vec![("foo", c.hint)])]),
                clock.clone(),
            );
            q.add(p.clone());
            let popped = q.pop().await.unwrap();
            assert_eq!(popped.pod.metadata.name, "pod1", "{}", c.name);
            let mut p_info = QueuedPodInfo::for_lookup(p.clone(), c.unschedulable);
            p_info.pending_plugins = c.pending.iter().map(|s| s.to_string()).collect();
            // add to unsched pod pool
            q.add_unschedulable_if_not_present(p_info, q.scheduling_cycle())
                .unwrap();
            clock.step(c.duration);
            let event = c.trigger_event.unwrap_or(NODE_ADD);
            q.move_all_to_active_or_backoff_queue(&event, None, None, None);
            match c.expected_q {
                BACKOFF_Q => assert_ne!(
                    q.backoff_q_len(),
                    0,
                    "{}: expected pod to be queued to backoffQ",
                    c.name
                ),
                ACTIVE_Q => assert_ne!(
                    q.active_q_len(),
                    0,
                    "{}: expected pod to be queued to activeQ",
                    c.name
                ),
                _ => assert!(
                    q.get_unschedulable_pod(&p).is_some(),
                    "{}: expected pod in unschedulablePods",
                    c.name
                ),
            }
        }
    }

    /// The gated rows of
    /// `TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint`
    /// (`:1964-2000`) with a `preEnqueuePlugin{allowlists}`-style gate.
    struct AllowlistGate {
        name: &'static str,
        allow: &'static [&'static str],
    }
    impl PreEnqueuePlugin for AllowlistGate {
        fn name(&self) -> &str {
            self.name
        }
        fn pre_enqueue(&self, pod: &Pod) -> Result<(), String> {
            let labels = pod.metadata.labels.clone().unwrap_or_default();
            if self.allow.iter().any(|a| labels.contains_key(*a)) {
                Ok(())
            } else {
                Err(format!("pod is gated by {}", self.name))
            }
        }
    }

    #[tokio::test]
    async fn gated_pods_follow_their_gating_plugin_events() {
        let now = UNIX_EPOCH + Duration::from_secs(1_735_689_600);
        let gated_pod = mk_pod("pod1", "ns1", "1");
        let count = Arc::new(AtomicUsize::new(0));
        // (gating plugin events, trigger event, hint, expected queue)
        let cases: Vec<(&str, &'static str, ClusterEvent, QueueingHintFn, &str)> = vec![
            // The hint must not be called: the gating plugin isn't interested
            // in NodeAdd, so the queue keeps the Pod in unschedQ.
            (
                "QueueHintFunction is not called when Pod is gated by the plugin that isn't interested in the event",
                "gate-pod-update",
                NODE_ADD,
                Arc::new(|_, _, _| Err("QueueingHintFn should not be called as pod is gated".to_string())),
                UNSCHEDULABLE_Q,
            ),
            // The gating plugin ("foo") is interested in NodeAdd, so the hint
            // runs; the Pod stays gated and therefore stays in unschedQ.
            (
                "QueueHintFunction is called when Pod is gated by the plugin that is interested in the event",
                "foo",
                NODE_ADD,
                hint_queue(&count),
                UNSCHEDULABLE_Q,
            ),
            // Wildcard events always re-evaluate; the Pod is still gated.
            (
                "Queue queues pod to unschedulableQ when Pod is gated and the event is wildcard",
                "foo",
                EVENT_UNSCHEDULABLE_TIMEOUT,
                Arc::new(|_, _, _| Err("should not be called".to_string())),
                UNSCHEDULABLE_Q,
            ),
        ];
        for (name, gate, event, hint, expected_q) in cases {
            count.store(0, Ordering::SeqCst);
            let hint_map = hints(vec![
                (NODE_ADD, vec![("foo", hint)]),
                (
                    EVENT_UNSCHEDULED_POD_UPDATE,
                    vec![("gate-pod-update", hint_queue(&count))],
                ),
            ]);
            let pre_enqueue: PreEnqueuePluginMap = HashMap::from([(
                rusternetes_scheduler::profile::DEFAULT_SCHEDULER_NAME.to_string(),
                vec![Arc::new(AllowlistGate {
                    name: gate,
                    allow: &["allow"],
                }) as Arc<dyn PreEnqueuePlugin>],
            )]);
            let q = PriorityQueue::new(
                priority_sort_less(),
                PriorityQueueOptions {
                    clock: Arc::new(FakeClock::new(now)),
                    queueing_hint_map: hint_map,
                    pre_enqueue_plugin_map: pre_enqueue,
                    ..Default::default()
                },
            );
            q.add(gated_pod.clone());
            // gated at Add → straight to unschedulablePods, never popped
            assert_eq!(q.active_q_len(), 0, "{name}");
            let info = q.get_pod("pod1", "ns1").unwrap();
            assert_eq!(info.gating_plugin, gate, "{name}");
            assert!(info.unschedulable_plugins.contains(gate), "{name}");
            q.move_all_to_active_or_backoff_queue(&event, None, None, None);
            match expected_q {
                UNSCHEDULABLE_Q => assert!(q.get_unschedulable_pod(&gated_pod).is_some(), "{name}"),
                other => panic!("{name}: unexpected queue {other}"),
            }
            if gate == "foo" && event == NODE_ADD {
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    1,
                    "{name}: hint should run once"
                );
            }
        }
        // Ungating: the gate's Pod/Update event with a now-allowed pod moves it on.
        let hint_map = hints(vec![(
            EVENT_UNSCHEDULED_POD_UPDATE,
            vec![(
                "gate",
                rusternetes_scheduler::queue::hints::default_queueing_hint_fn(),
            )],
        )]);
        let pre_enqueue: PreEnqueuePluginMap = HashMap::from([(
            rusternetes_scheduler::profile::DEFAULT_SCHEDULER_NAME.to_string(),
            vec![Arc::new(AllowlistGate {
                name: "gate",
                allow: &["allow"],
            }) as Arc<dyn PreEnqueuePlugin>],
        )]);
        let q = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                clock: Arc::new(FakeClock::new(now)),
                queueing_hint_map: hint_map,
                pre_enqueue_plugin_map: pre_enqueue,
                ..Default::default()
            },
        );
        q.add(gated_pod.clone());
        assert!(q.get_unschedulable_pod(&gated_pod).is_some());
        let mut ungated = gated_pod.clone();
        ungated.metadata.labels = Some(HashMap::from([("allow".to_string(), "".to_string())]));
        q.update(Some(&gated_pod), &ungated);
        assert_eq!(
            q.active_q_len(),
            1,
            "ungated pod moves to activeQ (not backing off: never attempted)"
        );
        assert!(!q.get_pod("pod1", "ns1").unwrap().gated());
    }

    /// `Test_InFlightPods` (`:190-885`), the QHint-enabled rows.
    #[tokio::test]
    async fn in_flight_pods() {
        let pod1 = mk_pod("targetpod", "", "pod1");
        let pod2 = mk_pod("targetpod2", "", "pod2");
        let pod3 = mk_pod("targetpod3", "", "pod3");
        let count = Arc::new(AtomicUsize::new(0));

        #[allow(clippy::large_enum_variant)]
        enum Action {
            EventHappens(ClusterEvent),
            PodPopped(Pod),
            PodEnqueued(QueuedPodInfo),
        }
        use Action::*;
        #[allow(clippy::large_enum_variant)]
        enum Want {
            Pod(Pod),
            Event(ClusterEvent),
        }
        struct Case {
            name: &'static str,
            map: QueueingHintMapPerProfile,
            initial_pods: Vec<Pod>,
            actions: Vec<Action>,
            want_in_flight_pods: Vec<Pod>,
            want_in_flight_events: Vec<Want>,
            want_backoff_q: Option<Vec<&'static str>>,
            want_unsched: Vec<&'static str>,
        }
        let q_all = |count: &Arc<AtomicUsize>| {
            hints(vec![
                (PV_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (NODE_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (PVC_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (CSI_NODE_UPDATE, vec![("fooPlugin1", hint_queue(count))]),
                (
                    EVENT_ASSIGNED_POD_ADD,
                    vec![("fooPlugin1", hint_queue(count))],
                ),
            ])
        };
        let cases = vec![
            Case {
                name: "Pod and interested events are registered in inFlightPods/inFlightEvents",
                map: hints(vec![(PV_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    // not added to inFlightEvents: no inFlightPods yet.
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    // gets added for the pod.
                    EventHappens(PV_ADD),
                    // not added: no plugin is interested in PvUpdate.
                    EventHappens(PV_UPDATE),
                ],
                want_in_flight_pods: vec![pod1.clone()],
                want_in_flight_events: vec![Want::Pod(pod1.clone()), Want::Event(PV_ADD)],
                want_backoff_q: None,
                want_unsched: vec![],
            },
            Case {
                name: "Pod, registered in inFlightPods, is enqueued back to backoffQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    // requeued to backoffQ immediately: no unschedulable plugin
                    // registered → an unexpected error (e.g. network).
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                ],
                want_in_flight_pods: vec![pod2.clone()],
                want_in_flight_events: vec![Want::Pod(pod2.clone()), Want::Event(NODE_ADD)],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
            Case {
                name: "All Pods registered in inFlightPods are enqueued back to activeQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                    EventHappens(CSI_NODE_UPDATE),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod2.clone(), &[])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod", "targetpod2"]),
                want_unsched: vec![],
            },
            Case {
                name: "One intermediate Pod registered in inFlightPods is enqueued back to activeQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone(), pod3.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    // This Pod won't be requeued again.
                    PodPopped(pod3.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod2.clone(), &[])),
                ],
                want_in_flight_pods: vec![pod1.clone(), pod3.clone()],
                want_in_flight_events: vec![
                    Want::Pod(pod1.clone()),
                    Want::Event(PV_ADD),
                    Want::Event(NODE_ADD),
                    Want::Pod(pod3.clone()),
                    Want::Event(EVENT_ASSIGNED_POD_ADD),
                ],
                want_backoff_q: Some(vec!["targetpod2"]),
                want_unsched: vec![],
            },
            Case {
                name: "events before popping Pod are ignored when Pod is enqueued back to queue",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    EventHappens(EVENT_UNSCHEDULABLE_TIMEOUT),
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    // not requeued to activeQ/backoffQ because fooPlugin1 returns QueueSkip.
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to backoff if no failed plugin",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
            Case {
                name: "pod is enqueued to unschedulable pod pool if no events that can make the pod schedulable",
                // fooPlugin1 has no queueing hint function for NodeAdd.
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(NODE_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to unschedulable pod pool because the failed plugin has a hint fn but it returns Skip",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to activeQ because the failed plugin has a hint fn and it returns Queue",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
        ];
        for c in cases {
            let clock = Arc::new(FakeClock::new(SystemTime::now()));
            let q = new_test_queue(c.map, clock.clone());
            for p in &c.initial_pods {
                q.add(p.clone());
                clock.step(Duration::from_secs(1));
            }
            for a in c.actions {
                match a {
                    PodPopped(p) => {
                        let popped = q.pop().await.unwrap();
                        assert_eq!(
                            popped.pod.metadata.uid, p.metadata.uid,
                            "{}: unexpected popped pod",
                            c.name
                        );
                    }
                    EventHappens(ev) => {
                        q.move_all_to_active_or_backoff_queue(&ev, None, None, None)
                    }
                    PodEnqueued(p) => q
                        .add_unschedulable_if_not_present(p, q.scheduling_cycle())
                        .unwrap(),
                }
            }
            let mut got: Vec<String> = q
                .in_flight_pods()
                .iter()
                .map(|p| p.metadata.uid.clone())
                .collect();
            got.sort();
            let mut want: Vec<String> = c
                .want_in_flight_pods
                .iter()
                .map(|p| p.metadata.uid.clone())
                .collect();
            want.sort();
            assert_eq!(got, want, "{}: inFlightPods", c.name);

            let got_events: Vec<String> = q
                .in_flight_events()
                .iter()
                .map(|e| match e {
                    InFlightEntry::Pod { pod, .. } => format!("pod:{}", pod.metadata.uid),
                    InFlightEntry::Event { event, .. } => format!("event:{}", event.event.label()),
                })
                .collect();
            let want_events: Vec<String> = c
                .want_in_flight_events
                .iter()
                .map(|w| match w {
                    Want::Pod(p) => format!("pod:{}", p.metadata.uid),
                    Want::Event(e) => format!("event:{}", e.label()),
                })
                .collect();
            assert_eq!(got_events, want_events, "{}: inFlightEvents", c.name);

            if let Some(want) = c.want_backoff_q {
                let mut names: Vec<String> = q
                    .pods_in_backoff_q()
                    .iter()
                    .map(|p| p.metadata.name.clone())
                    .collect();
                names.sort();
                let mut want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
                want.sort();
                assert_eq!(names, want, "{}: backoffQ pod names", c.name);
            }
            for name in c.want_unsched {
                assert!(
                    q.get_unschedulable_pod(&mk_pod(name, "", "")).is_some(),
                    "{}: Pod {name} was not found in the unschedulablePods.",
                    c.name
                );
            }
        }
    }

    /// `TestPriorityQueue_Update` (`:1123-1316`), condensed to the rows that
    /// don't need the nominator: activeQ update in place, unschedulable pod
    /// update with hints, unknown pod → activeQ.
    #[tokio::test]
    async fn update_moves_between_queues() {
        let count = Arc::new(AtomicUsize::new(0));
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let map = hints(vec![(
            ClusterEvent::new(EventResource::Pod, ActionType::UPDATE_POD_LABEL),
            vec![("fooPlugin1", hint_queue(&count))],
        )]);
        let q = new_test_queue(map, clock.clone());
        let pod = mk_pod("p", "ns", "u1");
        // unknown pod: added to activeQ
        q.update(None, &pod);
        assert_eq!(q.active_q_len(), 1);
        // in activeQ: updated in place, still one entry
        let mut labelled = pod.clone();
        labelled.metadata.labels = Some(HashMap::from([("a".to_string(), "b".to_string())]));
        q.update(Some(&pod), &labelled);
        assert_eq!(q.active_q_len(), 1);
        assert_eq!(
            q.get_pod("p", "ns").unwrap().pod.metadata.labels,
            labelled.metadata.labels
        );
        // pop, fail with fooPlugin1 → unschedulable pool
        let popped = q.pop().await.unwrap();
        q.add_unschedulable_if_not_present(
            rejected(popped.clone(), &["fooPlugin1"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        assert!(q.get_unschedulable_pod(&pod).is_some());
        // an unrelated update (general Pod/Update) → stays unschedulable
        let mut annotated = labelled.clone();
        annotated.metadata.annotations = Some(HashMap::from([("x".to_string(), "y".to_string())]));
        q.update(Some(&labelled), &annotated);
        assert!(q.get_unschedulable_pod(&pod).is_some());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        // a label update → fooPlugin1's hint says Queue → backing off → backoffQ
        let mut relabelled = annotated.clone();
        relabelled.metadata.labels = Some(HashMap::from([("a".to_string(), "c".to_string())]));
        q.update(Some(&annotated), &relabelled);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(q.backoff_q_has(&pod));
        assert!(q.get_unschedulable_pod(&pod).is_none());
        // in-flight pod: update only records the event
        clock.step(Duration::from_secs(2));
        q.flush_backoff_q_completed();
        let popped = q.pop().await.unwrap();
        assert_eq!(popped.attempts, 2);
        let mut again = relabelled.clone();
        again.metadata.labels = Some(HashMap::from([("a".to_string(), "d".to_string())]));
        q.update(Some(&relabelled), &again);
        assert_eq!(q.active_q_len(), 0);
        assert_eq!(q.in_flight_events().len(), 2);
        q.add_unschedulable_if_not_present(
            rejected(popped.clone(), &["fooPlugin1"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        // the recorded in-flight label event requeues it (after backoff)
        assert!(q.backoff_q_has(&pod));
    }

    /// `TestPriorityQueue_Delete` (`:1368-1390`).
    #[tokio::test]
    async fn delete_from_any_queue() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let a = mk_pod("a", "ns", "a");
        let b = mk_pod("b", "ns", "b");
        q.add(a.clone());
        q.add(b.clone());
        q.delete(&a);
        assert_eq!(q.active_q_len(), 1);
        let popped = q.pop().await.unwrap();
        assert_eq!(popped.pod.metadata.name, "b");
        q.add_unschedulable_if_not_present(
            QueuedPodInfo::for_lookup(b.clone(), &["x"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        assert!(q.get_unschedulable_pod(&b).is_some());
        q.delete(&b);
        assert!(q.get_unschedulable_pod(&b).is_none());
        assert_eq!(
            q.pending_pods().1,
            "activeQ:0; backoffQ:0; unschedulablePods:0"
        );
    }

    /// `TestBackOffFlow` (`:3838-3917`): exponential backoff per attempt,
    /// capped at the max.
    #[tokio::test]
    async fn backoff_flow() {
        // Fixed start with a half-second fraction. The backoff expiry and
        // `now` are both aligned to the 1s ordering window, and each step
        // below advances the clock by `wanted - 1ms + 1s`, so a start time
        // taken from the wall clock lands the "window equality still
        // backing off" boundary on a whole second about 1 run in 1000 and
        // fails "pod should have left backoffQ".
        let clock = Arc::new(FakeClock::new(
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_500),
        ));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let pod = mk_pod("test-pod", "test-ns", "test-uid");
        q.add(pod.clone());
        let steps: [(&str, Duration); 5] = [
            ("1s", Duration::from_secs(1)),
            ("2s", Duration::from_secs(2)),
            ("4s", Duration::from_secs(4)),
            ("8s", Duration::from_secs(8)),
            ("10s", Duration::from_secs(10)),
        ];
        for (i, (name, wanted)) in steps.iter().enumerate() {
            let p = q.pop().await.unwrap();
            assert_eq!(p.attempts, i + 1, "{name}");
            q.add_unschedulable_if_not_present(
                rejected(p.clone(), &["plugin"]),
                q.scheduling_cycle(),
            )
            .unwrap();
            // An event moves it to backoffQ.
            q.move_all_to_active_or_backoff_queue(&EVENT_UNSCHEDULABLE_TIMEOUT, None, None, None);
            assert!(q.backoff_q_has(&pod), "{name}: pod should be in backoffQ");
            // Before backoff completes nothing moves (the window boundary
            // counts as still backing off).
            clock.step(*wanted - Duration::from_millis(1));
            q.flush_backoff_q_completed();
            assert!(
                q.backoff_q_has(&pod),
                "{name}: pod should still be backing off"
            );
            // After the window passes, it is flushed to activeQ.
            clock.step(Duration::from_secs(1));
            q.flush_backoff_q_completed();
            assert!(
                !q.backoff_q_has(&pod),
                "{name}: pod should have left backoffQ"
            );
            assert_eq!(q.active_q_len(), 1, "{name}");
        }
    }

    /// `TestPriorityQueue_Activate` (`:1392-1545`), the in-flight row: an
    /// Activate for a pod being scheduled registers `EventForceActivate`.
    #[tokio::test]
    async fn activate_in_flight_pod_registers_force_activate() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let pod = mk_pod("p", "ns", "u");
        q.add(pod.clone());
        let p = q.pop().await.unwrap();
        q.activate(std::slice::from_ref(&pod));
        assert_eq!(q.in_flight_events().len(), 2);
        q.add_unschedulable_if_not_present(rejected(p.clone(), &["plugin"]), q.scheduling_cycle())
            .unwrap();
        // ForceActivate targets this pod → queueAfterBackoff → backoffQ
        assert!(q.backoff_q_has(&pod));
        // Activate from backoffQ moves straight to activeQ.
        q.activate(std::slice::from_ref(&pod));
        assert_eq!(q.active_q_len(), 1);
        assert!(!q.backoff_q_has(&pod));
        // Unknown pod is ignored.
        q.activate(&[mk_pod("unknown", "ns", "x")]);
        assert_eq!(q.active_q_len(), 1);
    }

    /// `TestHighPriorityFlushUnschedulablePodsLeftover` (`:3006-3064`).
    #[tokio::test]
    async fn flush_unschedulable_pods_leftover_moves_old_pods() {
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let high = mk_pod_prio("hp", "ns", "hp", 100);
        let med = mk_pod_prio("mp", "ns", "mp", 50);
        for p in [&high, &med] {
            q.add(p.clone());
            let popped = q.pop().await.unwrap();
            q.add_unschedulable_if_not_present(
                QueuedPodInfo::for_lookup(popped.pod, &["plugin"]),
                q.scheduling_cycle(),
            )
            .unwrap();
        }
        assert_eq!(q.unschedulable_pods().len(), 2);
        clock.step(DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION + Duration::from_secs(1));
        q.flush_unschedulable_pods_leftover();
        assert_eq!(q.unschedulable_pods().len(), 0);
        // Backoff has long expired → straight to activeQ, highest priority first.
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "hp");
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "mp");
    }

    #[tokio::test]
    async fn pop_blocks_until_add_and_returns_none_on_close() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.pop().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        q.add(mk_pod("p", "ns", "u"));
        let popped = h.await.unwrap().unwrap();
        assert_eq!(popped.pod.metadata.name, "p");
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.pop().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        q.close();
        assert!(h.await.unwrap().is_none());
    }

    #[test]
    fn calculate_backoff_duration_table() {
        let init = Duration::from_secs(1);
        let max = Duration::from_secs(10);
        assert_eq!(calculate_backoff_duration(init, max, 0), Duration::ZERO);
        assert_eq!(
            calculate_backoff_duration(init, max, 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 2),
            Duration::from_secs(2)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 4),
            Duration::from_secs(8)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 5),
            Duration::from_secs(10)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 100),
            Duration::from_secs(10)
        );
    }

    // -- nominator tests (scheduling_queue_test.go:2565-2780) ---------------

    fn nominated_pod(name: &str, ns: &str, uid: &str, prio: i32, node: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"priority": prio},
            "status": {"nominatedNodeName": node}
        }))
        .unwrap()
    }

    fn lister_of(pods: Vec<Pod>) -> PodLister {
        Arc::new(move |ns, name| {
            pods.iter()
                .find(|p| {
                    p.metadata.namespace.as_deref().unwrap_or("") == ns && p.metadata.name == name
                })
                .cloned()
        })
    }

    type NominatorState = (HashMap<String, Vec<PodRef>>, HashMap<String, String>);

    fn nominator_state(q: &PriorityQueue) -> NominatorState {
        let inner = q.inner.lock().unwrap();
        (
            inner.nominator.nominated_pods.clone(),
            inner.nominator.nominated_pod_to_node.clone(),
        )
    }

    fn expected_state(pairs: &[(&str, &Pod)]) -> NominatorState {
        let mut pods: HashMap<String, Vec<PodRef>> = HashMap::new();
        let mut to_node = HashMap::new();
        for (node, pod) in pairs {
            pods.entry(node.to_string())
                .or_default()
                .push(PodRef::from_pod(pod));
            to_node.insert(pod.metadata.uid.clone(), node.to_string());
        }
        (pods, to_node)
    }

    fn names(pods: &[Pod]) -> Vec<String> {
        pods.iter().map(|p| p.metadata.name.clone()).collect()
    }

    /// `TestPriorityQueue_NominatedPodsForNode` (`:2565-2589`).
    #[tokio::test]
    async fn nominated_pods_for_node() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let unsched = nominated_pod("up", "ns1", "upns1", 0, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        q.add(med);
        q.add(unsched);
        q.add(high);
        assert_eq!(q.pop().await.unwrap().pod.metadata.name, "hpp");
        let mut got = q.nominated_pods_for_node("node1");
        assert_eq!(names(&got), vec!["mpp", "up"]);
        // The returned pods are copies.
        got[0].metadata.name = "not mpp".to_string();
        assert_eq!(
            names(&q.nominated_pods_for_node("node1")),
            vec!["mpp", "up"]
        );
        assert!(q.nominated_pods_for_node("node2").is_empty());
    }

    /// `TestPriorityQueue_NominatedPodDeleted` (`:2591-2650`).
    #[tokio::test]
    async fn nominated_pod_deleted() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let high_nominated = nominated_pod("hpp", "ns1", "hppns1", 100000, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        for (name, pod, deleted, want_len) in [
            ("alive pod gets added into PodNominator", med, false, 1),
            (
                "deleted pod shouldn't be added into PodNominator",
                high_nominated,
                true,
                0,
            ),
            (
                "pod without .status.nominatedPodName shouldn't be added into PodNominator",
                high,
                false,
                0,
            ),
        ] {
            let lister = lister_of(if deleted { vec![] } else { vec![pod.clone()] });
            let q = PriorityQueue::new(
                priority_sort_less(),
                PriorityQueueOptions {
                    pod_lister: Some(lister),
                    ..Default::default()
                },
            );
            q.add_nominated_pod(&pod, None);
            let node = pod
                .status
                .as_ref()
                .and_then(|s| s.nominated_node_name.clone())
                .unwrap_or_default();
            assert_eq!(q.nominated_pods_for_node(&node).len(), want_len, "{name}");
        }
    }

    /// `TestPriorityQueue_UpdateNominatedPodForNode` (`:2695-2780`).
    #[tokio::test]
    async fn update_nominated_pod_for_node() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let unsched = nominated_pod("up", "ns1", "upns1", 0, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        let mut scheduled = mk_pod_prio("sp", "ns1", "spns1", 0);
        scheduled.spec.as_mut().unwrap().node_name = Some("node1".to_string());
        let non_existent = nominated_pod("nonexistent", "ns1", "nens1", 0, "node1");
        let q = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                pod_lister: Some(lister_of(vec![
                    med.clone(),
                    unsched.clone(),
                    high.clone(),
                    scheduled.clone(),
                ])),
                ..Default::default()
            },
        );
        q.add(med.clone());
        // Update `unsched` on a different node than specified in the pod.
        q.add_nominated_pod(&unsched, Some(&NominatingInfo::override_node("node5")));
        // Nominate a pod on a node that is not specified in the pod object.
        q.add_nominated_pod(&high, Some(&NominatingInfo::override_node("node2")));
        let expected = expected_state(&[("node1", &med), ("node2", &high), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after adding pods");
        assert_eq!(q.pop().await.unwrap().pod.metadata.name, "mpp");
        // List of nominated pods shouldn't change after popping them from the queue.
        assert_eq!(nominator_state(&q), expected, "after popping pods");
        // Update one of the nominated pods that doesn't have nominatedNodeName
        // in the pod object.
        q.add_nominated_pod(&high, Some(&NominatingInfo::override_node("node4")));
        let expected = expected_state(&[("node1", &med), ("node4", &high), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after updating pods");
        // Nominate a pod that was deleted from the informer cache: nothing changes.
        q.add_nominated_pod(&non_existent, Some(&NominatingInfo::override_node("node1")));
        assert_eq!(
            nominator_state(&q),
            expected,
            "after nominating a deleted pod"
        );
        // Nominate a pod that was already scheduled in the informer cache:
        // nothing changes.
        let mut scheduled_unbound = scheduled.clone();
        scheduled_unbound.spec.as_mut().unwrap().node_name = None;
        q.add_nominated_pod(
            &scheduled_unbound,
            Some(&NominatingInfo::override_node("node1")),
        );
        assert_eq!(
            nominator_state(&q),
            expected,
            "after nominating a scheduled pod"
        );
        // Delete a nominated pod that doesn't have nominatedNodeName in the pod object.
        q.delete_nominated_pod_if_exists(&high);
        let expected = expected_state(&[("node1", &med), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after deleting pods");
    }

    /// `Delete` clears the nominator (scheduling_queue.go:1118) and an `Update`
    /// of a queued pod without `nominatedNodeName` keeps reserving the node
    /// recorded in memory (`UpdateNominatedPod`, nominator.go:110-136).
    #[tokio::test]
    async fn queue_delete_and_update_maintain_nominator() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let pod = mk_pod_prio("p", "ns1", "p-uid", 0);
        q.add(pod.clone());
        q.add_nominated_pod(&pod, Some(&NominatingInfo::override_node("node1")));
        let mut updated = pod.clone();
        updated.metadata.resource_version = Some("2".to_string());
        q.update(Some(&pod), &updated);
        assert_eq!(q.nominated_pods_for_node("node1").len(), 1);
        q.delete(&updated);
        assert!(q.nominated_pods_for_node("node1").is_empty());
        assert!(q.nominated_nodes().is_empty());
    }
}
