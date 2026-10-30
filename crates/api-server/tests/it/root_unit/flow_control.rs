//! Unit tests moved from `src/flow_control.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::flow_control_conc_alloc::{
        compute_concurrency_allocation, AllocProblemItem,
    };
    pub use rusternetes_api_server::flow_control_integrator::{Integrator, IntegratorResults};
    pub use rusternetes_api_server::flow_control_metrics as fcmetrics;
    pub use rusternetes_api_server::flow_control_queueset::{
        Clock, DispatchingConfig, Execution, QueueSet, QueuingConfig, RealClock, RejectReason,
        WorkEstimate,
    };
    pub use rusternetes_common::resources::flowcontrol::{
        FlowDistinguisherMethodType, FlowSchema, FlowSchemaCondition, FlowSchemaSubject,
        LimitResponseType, NonResourcePolicyRule, PolicyRulesWithSubjects,
        PriorityLevelConfiguration, PriorityLevelType, ResourcePolicyRule, SubjectKind,
    };
    pub use rusternetes_common::validation::flowcontrol_bootstrap::{
        mandatory_flow_schema, mandatory_priority_level_configuration,
    };
    pub use rusternetes_storage::{build_key, Storage};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex, RwLock};
    pub use std::time::{Duration, Instant};
    pub use tracing::{error, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::flow_control::*;
    use rusternetes_storage::memory::MemoryStorage;

    fn cls(fs: &str, pl: &str, dist: &str) -> Classification {
        Classification {
            flow_schema: fs.into(),
            priority_level: pl.into(),
            flow_distinguisher: dist.into(),
            ..Default::default()
        }
    }

    fn engine() -> FlowControlEngine<MemoryStorage> {
        FlowControlEngine::new(Arc::new(MemoryStorage::new()))
    }

    fn digest_for(user: &str, groups: &[&str], verb: &str, path: &str) -> RequestDigest {
        RequestDigest {
            user_name: user.into(),
            groups: groups.iter().map(|s| s.to_string()).collect(),
            verb: verb.into(),
            path: path.into(),
            ..Default::default()
        }
    }

    #[test]
    fn mandatory_exempt_matches_masters_group() {
        let e = engine();
        let mut d = digest_for("admin", &["system:masters"], "get", "/healthz");
        d.is_resource_request = true;
        d.resource = "pods".into();
        let c = e.classify(&d);
        assert_eq!(c.flow_schema, "exempt");
        assert_eq!(c.priority_level, "exempt");
    }

    #[test]
    fn everything_else_falls_to_catch_all() {
        let e = engine();
        let mut d = digest_for("bob", &["system:authenticated"], "list", "");
        d.is_resource_request = true;
        d.resource = "pods".into();
        d.namespace = "default".into();
        let c = e.classify(&d);
        assert_eq!(c.flow_schema, "catch-all");
        assert_eq!(c.priority_level, "catch-all");
    }

    #[test]
    fn catch_all_nominal_cl_is_whole_server_limit() {
        // exempt shares 0 (plSpecCommons), catch-all shares 5 => ceil(600*5/5).
        let e = engine();
        assert_eq!(e.concurrency_limit("catch-all"), Some(600));
        assert_eq!(e.concurrency_limit("exempt"), Some(0));
    }

    #[test]
    fn non_resource_url_prefix_matching() {
        let urls = vec!["/healthz/*".to_string()];
        assert!(matches_non_resource_url(&urls, "/healthz/ping"));
        assert!(!matches_non_resource_url(&urls, "/healthzz"));
        assert!(matches_non_resource_url(&["*".to_string()], "/anything"));
    }

    #[test]
    fn service_account_namespace_wildcard() {
        assert!(sa_matches_namespace(
            "kube-system",
            "system:serviceaccount:kube-system:x"
        ));
        assert!(!sa_matches_namespace(
            "kube",
            "system:serviceaccount:kube-system:x"
        ));
    }

    #[tokio::test]
    async fn exempt_never_waits() {
        let e = engine();
        assert!(e
            .execute(&cls("exempt", "exempt", ""), 1, Duration::from_millis(1))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limited_level_rejects_when_seats_exhausted() {
        // catch-all is `Reject` upstream: a request beyond the limit is 429.
        let e = FlowControlEngine::with_limits(Arc::new(MemoryStorage::new()), 1, 0);
        assert_eq!(e.concurrency_limit("catch-all"), Some(1));
        let held = e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5),
            )
            .await
            .unwrap();
        let second = e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5),
            )
            .await;
        assert_eq!(second.err(), Some(FlowControlError::TooManyRequests));
        drop(held);
        assert!(e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5)
            )
            .await
            .is_ok());
    }

    // ---- borrowing (#2743; exempt_borrowing_test.go) ----

    fn borrowing_pl(
        name: &str,
        shares: i32,
        lendable: Option<i32>,
        borrowing: Option<i32>,
    ) -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(128, 6, 50);
        pl.metadata = ObjectMeta::new(name);
        let l = pl.spec.limited.as_mut().unwrap();
        l.nominal_concurrency_shares = Some(shares);
        l.lendable_percent = lendable;
        l.borrowing_limit_percent = borrowing;
        pl
    }

    /// `TestUpdateBorrowing` (exempt_borrowing_test.go:33). The levels mirror
    /// `SuggestedPriorityLevelConfigurationWorkloadHigh` (shares 40, lendable
    /// 50%), `...WorkloadLow` (100, 90%) and the mandatory catch-all (5, 0%).
    #[test]
    fn update_borrowing_matches_upstream_exempt_borrowing_test() {
        use rusternetes_api_server::flow_control_integrator::test_clock::ManualClock;
        let period = BORROWING_ADJUSTMENT_PERIOD;
        let clk = ManualClock::new();
        let server_cl = (40 + 100 + 5) * 6;
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            server_cl,
            0,
            clk.clone(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("high", 40, Some(50), None),
                mandatory_priority_level_configuration("exempt").unwrap(),
                borrowing_pl("mid", 100, Some(90), None),
                mandatory_priority_level_configuration("catch-all").unwrap(),
            ],
            vec![],
        );
        let st = |n: &str| e.borrowing_state(n).unwrap();
        let demand = |n: &str, x: f64| e.seat_demand(n).unwrap().set(x);
        let (high, mid, low) = (st("high"), st("mid"), st("catch-all"));
        assert_eq!(
            high.nominal_cl + mid.nominal_cl + low.nominal_cl,
            server_cl as usize
        );

        // Scenario 1: everybody wants more than ServerConcurrencyLimit.
        // Exempt borrows so much that less than minCL is left for each
        // non-exempt level.
        for n in ["exempt", "high", "mid", "catch-all"] {
            demand(n, (server_cl + 100) as f64);
        }
        clk.set(period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, server_cl as usize + 100);
        assert_eq!(st("high").current_cl, high.min_cl);
        assert_eq!(st("mid").current_cl, mid.min_cl);
        assert_eq!(st("catch-all").current_cl, low.min_cl);

        // Scenario 2: non-exempt want more than serverCL but get halfway
        // between minCL and minCurrentCL.
        let exp_high = (high.nominal_cl + high.min_cl) / 2;
        let exp_mid = (mid.nominal_cl + mid.min_cl) / 2;
        let exp_low = (low.nominal_cl + low.min_cl) / 2;
        let exp_exempt = server_cl as usize - (exp_high + exp_mid + exp_low);
        demand("exempt", exp_exempt as f64);
        clk.set(2 * period);
        e.update_borrowing();
        clk.set(3 * period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, exp_exempt);
        assert_eq!(st("high").current_cl, exp_high);
        assert_eq!(st("mid").current_cl, exp_mid);
        assert_eq!(st("catch-all").current_cl, exp_low);

        // Scenario 3: only mid is willing to lend, and exempt borrows all of
        // that (regular borrowing).
        let exp_high = high.nominal_cl;
        let exp_mid = mid.min_cl;
        let exp_low = low.nominal_cl;
        let exp_exempt = server_cl as usize - (exp_high + exp_mid + exp_low);
        demand("exempt", exp_exempt as f64);
        demand("mid", 1.0);
        clk.set(4 * period);
        e.update_borrowing();
        clk.set(5 * period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, exp_exempt);
        assert_eq!(st("high").current_cl, exp_high);
        assert_eq!(st("mid").current_cl, exp_mid);
        assert_eq!(st("catch-all").current_cl, exp_low);
    }

    /// `finishQueueSetReconfigsLocked` (apf_controller.go:859-900): min/max
    /// from lendable/borrowing percents, initial currentCL = nominal -
    /// lendable/2, and a level with no demand lends down to its minCL.
    #[test]
    fn digest_derives_min_max_and_initial_current_cl() {
        use rusternetes_api_server::flow_control_integrator::test_clock::ManualClock;
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            100,
            0,
            ManualClock::new(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("a", 50, Some(40), Some(30)),
                borrowing_pl("b", 50, None, Some(0)),
            ],
            vec![],
        );
        // catch-all (5 shares) joins: shareSum 105.
        let a = e.borrowing_state("a").unwrap();
        assert_eq!(a.nominal_cl, 48); // ceil(100*50/105)
        assert_eq!(a.min_cl, 48 - 19); // lendable round(48*0.4)=19
        assert_eq!(a.max_cl, 48 + 14); // borrowing round(48*0.3)=14
        let b = e.borrowing_state("b").unwrap();
        assert_eq!((b.min_cl, b.max_cl), (48, 48));
        assert_eq!(b.current_cl, 48);
        // No demand has been observed, so the first adjustment lends all it can.
        e.update_borrowing();
        assert!(e.borrowing_state("a").unwrap().current_cl >= a.min_cl);
    }

    /// `TestBorrowing` (flowcontrol/borrowing_test.go:60). Two levels with
    /// nominal limit 12 each (server limit 24); 24 closed-loop clients on
    /// level 0 and 6 on level 1, each request holding a seat for 250 ms.
    /// The first 10 s are ignored, the next 15 s are measured: level 0 must
    /// deliver about 16 (its 12 plus what it borrows) and level 1 about 6.
    /// Upstream runs this on `eventclock.Real`; we run the same scenario on
    /// tokio's paused clock. Requests are classified directly to their level
    /// (the upstream FlowSchemas only route `test-userN` to `test-plN`).
    async fn run_borrowing_scenario(lendable: [i32; 2], borrowing: [i32; 2]) {
        use rusternetes_api_server::flow_control_integrator::test_clock::TokioClock;
        let clk = TokioClock::new();
        let st = Arc::new(MemoryStorage::new());
        let e = Arc::new(FlowControlEngine::with_limits_and_clock(
            st,
            24,
            0,
            clk.clone(),
        ));
        let mut pls = vec![];
        for flow in 0..2 {
            let mut pl = borrowing_pl(
                &format!("test-pl{flow}"),
                100,
                Some(lendable[flow]),
                Some(borrowing[flow]),
            );
            pl.spec.limited.as_mut().unwrap().limit_response = Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 10,
                    hand_size: 2,
                    queue_length_limit: 10,
                }),
            });
            pls.push(pl);
        }
        e.digest_config_objects(pls, vec![]);
        // `wait.Until(cfgCtlr.updateBorrowing, borrowingAdjustmentPeriod)`.
        let updater =
            rusternetes_api_server::flow_control_filter::spawn_borrowing_updater(e.clone());
        let integrators: Vec<Arc<Integrator>> = (0..2)
            .map(|_| Arc::new(Integrator::new(clk.clone())))
            .collect();
        let mut clients = vec![];
        for (flow, n) in [24usize, 6].into_iter().enumerate() {
            for _ in 0..n {
                let (e, igr) = (e.clone(), integrators[flow].clone());
                let c = cls(
                    &format!("test-fs{flow}"),
                    &format!("test-pl{flow}"),
                    &format!("test-user{flow}"),
                );
                clients.push(tokio::spawn(async move {
                    loop {
                        // `controller.Handle(...)`; a rejection just retries.
                        if let Ok(p) = e.execute(&c, 1, Duration::from_secs(15)).await {
                            igr.add(1.0);
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            igr.add(-1.0);
                            drop(p);
                        } else {
                            tokio::task::yield_now().await;
                        }
                    }
                }));
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        for i in &integrators {
            i.reset();
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
        let r0 = integrators[0].reset();
        let r1 = integrators[1].reset();
        updater.abort();
        for c in clients {
            c.abort();
        }
        assert!(
            (15.5..=16.1).contains(&r0.average),
            "flow 0 average concurrency {} but expected about 16",
            r0.average
        );
        assert!(
            (5.5..=6.1).contains(&r1.average),
            "flow 1 average concurrency {} but expected about 6",
            r1.average
        );
    }

    #[tokio::test(start_paused = true)]
    async fn borrowing_lendable_limited_matches_upstream_test_borrowing() {
        run_borrowing_scenario([50, 33], [67, 50]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn borrowing_borrowing_limited_matches_upstream_test_borrowing() {
        run_borrowing_scenario([50, 67], [33, 50]).await;
    }

    /// A busy level borrows what an idle lendable level lends, and the
    /// queueset enforces the new `currentCL` (the point of
    /// `updateBorrowingLocked` completing the queueset with it).
    #[test]
    fn busy_level_borrows_from_idle_lendable_level() {
        use rusternetes_api_server::flow_control_integrator::test_clock::ManualClock;
        let clk = ManualClock::new();
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            100,
            0,
            clk.clone(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("busy", 50, Some(50), None),
                borrowing_pl("idle", 50, Some(50), None),
            ],
            vec![],
        );
        let busy = e.borrowing_state("busy").unwrap();
        assert!(busy.current_cl < busy.nominal_cl + 1);
        e.seat_demand("busy").unwrap().set(1000.0);
        clk.set(BORROWING_ADJUSTMENT_PERIOD);
        e.update_borrowing();
        let after = e.borrowing_state("busy").unwrap();
        assert!(
            after.current_cl > after.nominal_cl,
            "expected to borrow above nominal {}, got {}",
            after.nominal_cl,
            after.current_cl
        );
        assert!(after.current_cl <= after.max_cl);
        assert_eq!(
            e.queueset_for("busy").unwrap().concurrency_limit(),
            after.current_cl
        );
        let idle = e.borrowing_state("idle").unwrap();
        assert!(idle.current_cl >= idle.min_cl && idle.current_cl < idle.nominal_cl);
    }

    // ---- QueueSet wiring (#2741) ----

    use rusternetes_common::resources::flowcontrol::{
        LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration, QueuingConfiguration,
    };
    use rusternetes_common::types::ObjectMeta;

    /// A `Limited`/`Queue` level named `test` (CL 1 when the server limit is 1).
    fn queueing_pl(queues: i32, hand: i32, len: i32) -> PriorityLevelConfiguration {
        let mut pl = mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("test");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(1000),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues,
                    hand_size: hand,
                    queue_length_limit: len,
                }),
            }),
        });
        pl
    }

    fn reject_pl() -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(0, 0, 0);
        if let Some(l) = pl.spec.limited.as_mut() {
            l.limit_response = Some(LimitResponse {
                type_: LimitResponseType::Reject,
                queuing: None,
            });
        }
        pl
    }

    async fn engine_with(pl: PriorityLevelConfiguration) -> FlowControlEngine<MemoryStorage> {
        let st = Arc::new(MemoryStorage::new());
        st.create(&build_key("prioritylevelconfigurations", None, "test"), &pl)
            .await
            .unwrap();
        let e = FlowControlEngine::with_limits(st, 1, 0);
        e.initialize().await.unwrap();
        e
    }

    #[test]
    fn hash_flow_id_matches_upstream_vectors() {
        // sha256(fsName || 0x00 || distinguisher)[:8] little-endian
        // (apf_controller.go:1131-1140); vectors from python hashlib.
        assert_eq!(hash_flow_id("fs", "a"), 3672294129925421940);
        assert_eq!(hash_flow_id("fs", "b"), 11054805014913603293);
        assert_eq!(hash_flow_id("", ""), 10987292151339758702);
        assert_eq!(hash_flow_id("catch-all", "user"), 2294043605982591715);
    }

    #[tokio::test]
    async fn queued_requests_are_dispatched_fairly_across_flows() {
        // One seat; flow a queues 3 requests, then flow b queues 1. Fair
        // queueing serves b before a's second request; a FIFO semaphore does not.
        let e = Arc::new(engine_with(queueing_pl(64, 1, 10)).await);
        let held = e
            .execute(&cls("fs", "test", "h"), 1, Duration::from_secs(5))
            .await
            .unwrap();
        let order = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let mut tasks = vec![];
        for (name, flow) in [("a1", "a"), ("a2", "a"), ("a3", "a"), ("b1", "b")] {
            let (e, order) = (e.clone(), order.clone());
            tasks.push(tokio::spawn(async move {
                let p = e
                    .execute(&cls("fs", "test", flow), 1, Duration::from_secs(5))
                    .await
                    .unwrap();
                order.lock().unwrap().push(name.to_string());
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(p);
            }));
            tokio::task::yield_now().await;
        }
        drop(held);
        for t in tasks {
            t.await.unwrap();
        }
        let order = order.lock().unwrap().clone();
        let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
        assert!(pos("b1") < pos("a2"), "unfair order: {order:?}");
    }

    #[tokio::test]
    async fn reload_keeps_in_flight_seats_accounted() {
        // Reconfigure via set_configuration (not a fresh queueset): a seat held
        // across `initialize()` must still count against the level.
        let e = engine_with(queueing_pl(4, 1, 1)).await;
        let held = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        e.initialize().await.unwrap();
        let second = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(20))
            .await;
        assert_eq!(second.err(), Some(FlowControlError::TooManyRequests));
        drop(held);
        assert!(e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(20))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn queue_length_limit_rejects_immediately() {
        let e = Arc::new(engine_with(queueing_pl(1, 1, 1)).await);
        let _held = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let queued = tokio::spawn(async move {
            e2.execute(&cls("fs", "test", "a"), 1, Duration::from_millis(300))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let start = std::time::Instant::now();
        let third = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_secs(5))
            .await;
        assert_eq!(third.err(), Some(FlowControlError::TooManyRequests));
        assert!(start.elapsed() < Duration::from_millis(200));
        let _ = queued.await;
    }

    #[tokio::test]
    async fn exempt_is_not_limited_by_concurrency() {
        // Exempt: DesiredNumQueues < 0 dispatches immediately whatever the CL.
        let e = engine();
        let mut held = vec![];
        for _ in 0..1000 {
            held.push(
                e.execute(&cls("exempt", "exempt", ""), 5, Duration::from_millis(1))
                    .await
                    .unwrap(),
            );
        }
    }

    // ---- quiescing / reaping (#2769) ----
    //
    // Port of the invariant `TestConfigConsumer` checks after each digest
    // (controller_test.go:253-285): a queueset exists for every desired level,
    // every mandatory level, and every undesired level that is still busy; no
    // other queueset is non-idle.

    async fn put_pl(st: &Arc<MemoryStorage>, pl: &PriorityLevelConfiguration) {
        let key = build_key("prioritylevelconfigurations", None, &pl.metadata.name);
        if st.create(&key, pl).await.is_err() {
            st.update(&key, pl).await.unwrap();
        }
    }

    async fn del_pl(st: &Arc<MemoryStorage>, name: &str) {
        let _ = st
            .delete(&build_key("prioritylevelconfigurations", None, name))
            .await;
    }

    fn named_pl(name: &str, queues: i32, hand: i32) -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(queues, hand, 10);
        pl.metadata = ObjectMeta::new(name);
        pl
    }

    async fn level_a_engine() -> (Arc<MemoryStorage>, FlowControlEngine<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl("a", 4, 2)).await;
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.initialize().await.unwrap();
        (st, e)
    }

    async fn hold_seat(e: &FlowControlEngine<MemoryStorage>, pl: &str) -> FlowControlPermit {
        e.execute(&cls("x", pl, "d"), 1, Duration::from_millis(50))
            .await
            .unwrap()
    }

    // presyncFlowSchemaStatus / digestFlowSchemasLocked
    // (apf_controller.go:956-985, :773): the Dangling condition of every
    // FlowSchema reflects whether its priority-level reference exists, and
    // syncOne writes it (:613).
    async fn dangling_of(
        st: &Arc<MemoryStorage>,
        name: &str,
    ) -> Option<rusternetes_common::resources::flowcontrol::FlowSchemaCondition> {
        let fs: FlowSchema = st.get(&build_key("flowschemas", None, name)).await.unwrap();
        fs.status
            .and_then(|s| s.conditions)
            .and_then(|c| c.into_iter().find(|c| c.type_ == "Dangling"))
    }

    fn fs_ref(name: &str, pl: &str) -> FlowSchema {
        let mut fs = mandatory_flow_schema("catch-all").unwrap();
        fs.metadata = ObjectMeta::new(name);
        fs.spec.priority_level_configuration.name = pl.to_string();
        fs.spec.matching_precedence = 500;
        fs
    }

    #[tokio::test]
    async fn sync_one_writes_dangling_conditions() {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl("a", 4, 2)).await;
        for fs in [fs_ref("good", "a"), fs_ref("bad", "nope")] {
            st.create(&build_key("flowschemas", None, &fs.metadata.name), &fs)
                .await
                .unwrap();
        }
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.sync_one().await.unwrap();
        let good = dangling_of(&st, "good")
            .await
            .expect("good has a condition");
        assert_eq!(good.status, "False");
        assert_eq!(good.reason.as_deref(), Some("Found"));
        assert_eq!(
            good.message.as_deref(),
            Some("This FlowSchema references the PriorityLevelConfiguration object named \"a\" and it exists")
        );
        let bad = dangling_of(&st, "bad").await.expect("bad has a condition");
        assert_eq!(bad.status, "True");
        assert_eq!(bad.reason.as_deref(), Some("NotFound"));
        assert_eq!(
            bad.message.as_deref(),
            Some("This FlowSchema references the PriorityLevelConfiguration object named \"nope\" but there is no such object")
        );
    }

    /// `apply` (apf_controller.go:628-635): `ApplyStatus` with
    /// `ApplyOptions{FieldManager: asFieldManager, Force: true}`, where
    /// `asFieldManager` is `ConfigConsumerAsFieldManager`
    /// (apf_filter.go:42, `"api-priority-and-fairness-config-consumer-v1"`).
    #[tokio::test]
    async fn dangling_status_is_applied_by_the_config_consumer_field_manager() {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl("a", 4, 2)).await;
        let fs = fs_ref("good", "a");
        st.create(&build_key("flowschemas", None, "good"), &fs)
            .await
            .unwrap();
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.sync_one().await.unwrap();
        let v: serde_json::Value = st
            .get(&build_key("flowschemas", None, "good"))
            .await
            .unwrap();
        let mf = v["metadata"]["managedFields"]
            .as_array()
            .expect("managedFields");
        let entry = mf
            .iter()
            .find(|m| m["manager"] == "api-priority-and-fairness-config-consumer-v1")
            .expect("entry for the config consumer");
        assert_eq!(entry["operation"], "Apply");
    }

    #[tokio::test]
    async fn dangling_condition_already_current_is_not_rewritten() {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl("a", 4, 2)).await;
        let fs = fs_ref("good", "a");
        st.create(&build_key("flowschemas", None, "good"), &fs)
            .await
            .unwrap();
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.sync_one().await.unwrap();
        let first = dangling_of(&st, "good").await.unwrap();
        // presyncFlowSchemaStatus returns early when status/reason/message
        // already match, so the transition time is untouched.
        let ups = e.digest_config_objects(
            vec![named_pl("a", 4, 2)],
            vec![st
                .get(&build_key("flowschemas", None, "good"))
                .await
                .unwrap()],
        );
        assert!(ups.is_empty(), "{ups:?}");
        assert_eq!(
            serde_json::to_value(dangling_of(&st, "good").await.unwrap()).unwrap(),
            serde_json::to_value(first).unwrap()
        );
    }

    // shouldDelayUpdate (apf_controller.go:651-670): more than 5 updates of
    // one FlowSchema in the past minute delays the next.
    #[test]
    fn should_delay_update_after_five_updates_in_a_minute() {
        let e = engine();
        {
            let mut r = e.most_recent_updates.lock().unwrap();
            for _ in 0..5 {
                r.push(UpdateAttempt {
                    time_updated: e.clock.now(),
                    updated_items: ["x".to_string()].into_iter().collect(),
                });
            }
        }
        assert!(!e.should_delay_update("x"), "5 is allowed");
        e.most_recent_updates.lock().unwrap().push(UpdateAttempt {
            time_updated: e.clock.now(),
            updated_items: ["x".to_string()].into_iter().collect(),
        });
        assert!(e.should_delay_update("x"), "6th is delayed");
        assert!(!e.should_delay_update("y"));
    }

    #[tokio::test]
    async fn removed_idle_level_is_dropped() {
        let (st, e) = level_a_engine().await;
        assert!(e.queueset_for("a").is_some());
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn removed_busy_level_quiesces_then_is_reaped() {
        // processOldPLsLocked (apf_controller.go:810-836) + maybeReap.
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.is_quiescing("a"));
        assert!(Arc::ptr_eq(&qs, &e.queueset_for("a").unwrap()));
        // Still busy: reaping does nothing.
        e.maybe_reap("a");
        assert!(e.queueset_for("a").is_some());
        drop(permit);
        e.maybe_reap("a");
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn quiescing_level_that_becomes_desired_again_keeps_its_queueset() {
        // digestNewPLsLocked (apf_controller.go:737-740).
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        let pl = named_pl("a", 4, 2);
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.is_quiescing("a"));
        put_pl(&st, &pl).await;
        e.initialize().await.unwrap();
        assert!(!e.is_quiescing("a"));
        assert!(Arc::ptr_eq(&qs, &e.queueset_for("a").unwrap()));
        drop(permit);
    }

    #[tokio::test]
    async fn broken_update_to_busy_level_keeps_old_queueset_quiescing() {
        // digestNewPLsLocked ignores a broken spec (:765-769); the old state
        // is then processed as an undesired level (:810-836).
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        // hand size larger than the deck: invalid shuffle sharding.
        put_pl(&st, &named_pl("a", 2, 5)).await;
        e.initialize().await.unwrap();
        let kept = e.queueset_for("a").expect("busy level must be retained");
        assert!(Arc::ptr_eq(&qs, &kept));
        assert!(e.is_quiescing("a"));
        drop(permit);
    }

    #[tokio::test]
    async fn broken_update_to_idle_level_drops_it() {
        let (st, e) = level_a_engine().await;
        put_pl(&st, &named_pl("a", 2, 5)).await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn broken_new_level_is_ignored() {
        let (st, e) = level_a_engine().await;
        put_pl(&st, &named_pl("b", 2, 5)).await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("b").is_none());
        assert!(e.queueset_for("a").is_some());
    }

    #[tokio::test]
    async fn mandatory_levels_are_never_dropped() {
        let (_st, e) = level_a_engine().await;
        for _ in 0..2 {
            e.initialize().await.unwrap();
            assert!(e.queueset_for("exempt").is_some());
            assert!(e.queueset_for("catch-all").is_some());
        }
    }

    #[tokio::test]
    async fn quiescing_level_still_gets_a_share_while_draining() {
        // processOldPLsLocked :852-857: lingering levels stay in shareSum.
        let (st, e) = level_a_engine().await;
        let before = e.concurrency_limit("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert_eq!(e.concurrency_limit("a"), Some(before));
        drop(permit);
    }

    #[tokio::test]
    async fn digest_invariant_over_a_sequence_of_configs() {
        // Deterministic stand-in for TestConfigConsumer's random walk.
        let st = Arc::new(MemoryStorage::new());
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        let names = ["p0", "p1", "p2", "p3"];
        let mut held: Vec<(String, FlowControlPermit)> = vec![];
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut persisting: std::collections::HashSet<String> = Default::default();
        for _step in 0..40 {
            let mut desired = std::collections::HashSet::new();
            for n in names {
                match next() % 3 {
                    0 => del_pl(&st, n).await,
                    1 => {
                        put_pl(&st, &named_pl(n, 4, 2)).await;
                        desired.insert(n.to_string());
                    }
                    // broken: ignored, like an absent object
                    _ => put_pl(&st, &named_pl(n, 2, 5)).await,
                }
            }
            e.initialize().await.unwrap();
            let mut next_persist = std::collections::HashSet::new();
            for n in &persisting {
                if held.iter().any(|(h, _)| h == n) {
                    next_persist.insert(n.clone());
                }
            }
            persisting = next_persist.union(&desired).cloned().collect();
            for n in &persisting {
                assert!(e.queueset_for(n).is_some(), "missing queueset {n}");
            }
            for n in names {
                if !persisting.contains(n) {
                    assert!(
                        e.queueset_for(n).is_none_or(|q| q.is_idle()),
                        "unexpected busy queueset {n}"
                    );
                }
            }
            for n in &desired {
                if next() % 2 == 0 {
                    held.push((n.clone(), hold_seat(&e, n).await));
                }
            }
            if next() % 3 == 0 {
                held.clear();
            }
        }
    }

    // ---- apiserver_flowcontrol_* metrics (#2809) ----
    //
    // Ports of the observations `queueset.go` (AddReject :321/:340/:433,
    // AddRequestsInQueues :646/:705, AddRequestsExecuting :678/:724/:866,
    // AddSeatConcurrencyInUse) and `apf_filter.go` `Handle` (:164-201:
    // AddDispatch, ObserveExecutionDuration, observeQueueWaitTime) make.

    use rusternetes_api_server::flow_control::metric_test_util::sample;

    #[tokio::test]
    async fn dispatch_counts_and_executing_gauges_follow_the_permit() {
        let e = engine_with(queueing_pl(4, 1, 10)).await;
        let l = [("priority_level", "test"), ("flow_schema", "m-dispatch")];
        let before = sample("apiserver_flowcontrol_dispatched_requests_total", &l);
        let permit = e
            .execute(
                &cls("m-dispatch", "test", "a"),
                1,
                Duration::from_millis(50),
            )
            .await
            .unwrap();
        assert_eq!(
            sample("apiserver_flowcontrol_dispatched_requests_total", &l) - before,
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_requests", &l),
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_seats", &l),
            1.0
        );
        let exec_before = sample("apiserver_flowcontrol_request_execution_seconds", &l);
        drop(permit);
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_requests", &l),
            0.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_seats", &l),
            0.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_request_execution_seconds", &l) - exec_before,
            1.0
        );
    }

    #[tokio::test]
    async fn queue_full_is_counted_as_a_rejection() {
        let e = Arc::new(engine_with(queueing_pl(1, 1, 1)).await);
        let _held = e
            .execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let queued = tokio::spawn(async move {
            e2.execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(300))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-full"),
            ("reason", "queue-full"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        let inq = [("priority_level", "test"), ("flow_schema", "m-full")];
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_requests", &inq),
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_seats", &inq),
            1.0
        );
        assert!(e
            .execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(5))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
        queued.abort();
    }

    #[tokio::test]
    async fn a_wait_that_times_out_is_a_time_out_rejection_and_leaves_the_queue() {
        let e = engine_with(queueing_pl(4, 1, 10)).await;
        let _held = e
            .execute(&cls("m-to", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-to"),
            ("reason", "time-out"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        assert!(e
            .execute(&cls("m-to", "test", "a"), 1, Duration::from_millis(20))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
        let q = [("priority_level", "test"), ("flow_schema", "m-to")];
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_requests", &q),
            0.0
        );
        // The waited request is observed with execute="true" (`req != nil`,
        // apf_filter.go:201 call sites).
        assert_eq!(
            sample("apiserver_flowcontrol_request_wait_duration_seconds", &q),
            1.0
        );
    }

    #[tokio::test]
    async fn a_queued_request_that_dispatches_observes_its_wait() {
        let e = Arc::new(engine_with(queueing_pl(4, 1, 10)).await);
        let held = e
            .execute(&cls("m-wait", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let t = tokio::spawn(async move {
            e2.execute(&cls("m-wait", "test", "a"), 1, Duration::from_secs(5))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(held);
        let _p = t.await.unwrap().unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-wait"),
            ("execute", "true"),
        ];
        // Only the queued request observes a wait (`if queued`).
        assert_eq!(
            sample("apiserver_flowcontrol_request_wait_duration_seconds", &l),
            1.0
        );
    }

    #[tokio::test]
    async fn concurrency_limit_rejection_without_queues() {
        // desired_num_queues < 1 (Reject-type level): queueset.go:321.
        let e = engine_with(reject_pl()).await;
        let _held = e
            .execute(&cls("m-cl", "test", ""), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-cl"),
            ("reason", "concurrency-limit"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        assert!(e
            .execute(&cls("m-cl", "test", ""), 1, Duration::from_millis(5))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
    }

    // ---- #2960: limit gauges, utilization and read-vs-write histograms ----

    use rusternetes_api_server::flow_control::metric_test_util::{has_series, sample_sum};

    const NS: &str = "apiserver_flowcontrol_";

    async fn engine_with_level(
        name: &str,
    ) -> (Arc<MemoryStorage>, FlowControlEngine<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl(name, 4, 2)).await;
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.initialize().await.unwrap();
        (st, e)
    }

    async fn hold(
        e: &FlowControlEngine<MemoryStorage>,
        pl: &str,
        mutating: bool,
    ) -> FlowControlPermit {
        let mut c = cls("x", pl, "d");
        c.is_mutating = mutating;
        e.execute(&c, 1, Duration::from_millis(50)).await.unwrap()
    }

    // SetPriorityLevelConfiguration (apf_controller.go:874),
    // NotePriorityLevelConcurrencyAdjustment (:481), SetFairFrac (:440-454).
    #[tokio::test]
    async fn digest_publishes_the_limit_gauges() {
        let (_st, e) = engine_with_level("m-limits").await;
        let (nominal, min, max, current) = {
            let cfg = e.config.read().unwrap();
            let l = &cfg.levels["m-limits"];
            let current_cl = l.borrow.lock().unwrap().current_cl;
            (l.nominal_cl, l.min_cl, l.max_cl, current_cl)
        };
        let l = [("priority_level", "m-limits")];
        assert!(nominal > 0);
        for name in ["nominal_limit_seats", "request_concurrency_limit"] {
            assert_eq!(sample(&format!("{NS}{name}"), &l), nominal as f64, "{name}");
        }
        assert_eq!(sample(&format!("{NS}lower_limit_seats"), &l), min as f64);
        assert_eq!(sample(&format!("{NS}upper_limit_seats"), &l), max as f64);
        assert_eq!(
            sample(&format!("{NS}current_limit_seats"), &l),
            current as f64
        );
        for name in [
            "demand_seats_high_watermark",
            "demand_seats_average",
            "demand_seats_stdev",
            "demand_seats_smoothed",
            "target_seats",
        ] {
            assert!(has_series(&format!("{NS}{name}"), &l), "{name} missing");
        }
        assert!(has_series(&format!("{NS}seat_fair_frac"), &[]));
    }

    // execSeatsGauge / reqsGaugePair (queueset.go:281-284, :680-681, :867-879):
    // the level's utilization histograms integrate over time.
    #[tokio::test]
    async fn utilization_histograms_integrate_a_held_seat() {
        let (_st, e) = engine_with_level("m-util").await;
        let exec = [("priority_level", "m-util"), ("phase", "executing")];
        let seats = format!("{NS}priority_level_seat_utilization");
        let reqs = format!("{NS}priority_level_request_utilization");
        let permit = hold(&e, "m-util", false).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(sample_sum(&seats, &exec) > 0.0, "seat utilization held");
        assert!(sample_sum(&reqs, &exec) > 0.0, "request utilization held");
        drop(permit);
        let after = sample_sum(&seats, &exec);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let later = sample_sum(&seats, &exec);
        assert!(
            (later - after).abs() < 1e-6,
            "idle adds nothing: {after} vs {later}"
        );
        assert!(has_series(
            &reqs,
            &[("priority_level", "m-util"), ("phase", "waiting")]
        ));
    }

    // seatDemandRatioedGauge (apf_controller.go:705-729, queueset.go:650):
    // the demand_seats histogram integrates (seats in use + waiting) over
    // the level's concurrency denominator.
    #[tokio::test]
    async fn demand_seats_histogram_integrates_a_held_seat() {
        let (_st, e) = engine_with_level("m-demand").await;
        let name = format!("{NS}demand_seats");
        let permit = hold(&e, "m-demand", false).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            sample_sum(&name, &[("priority_level", "m-demand")]) > 0.0,
            "demand integrates while a seat is held"
        );
        drop(permit);
    }

    // noteExecutingDelta (priority-and-fairness.go:150-156): the read-vs-write
    // histograms split on the verb.
    #[tokio::test]
    async fn read_vs_write_splits_on_the_verb() {
        let (_st, e) = engine_with_level("m-rw").await;
        let name = format!("{NS}read_vs_write_current_requests");
        let mutating = [("phase", "executing"), ("request_kind", "mutating")];
        let before = sample_sum(&name, &mutating);
        let permit = hold(&e, "m-rw", true).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(sample_sum(&name, &mutating) > before);
        drop(permit);
        assert!(has_series(
            &name,
            &[("phase", "waiting"), ("request_kind", "readOnly")]
        ));
    }
}
