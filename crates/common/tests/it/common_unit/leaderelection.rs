//! Unit tests moved from `src/leaderelection.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::signal::ShutdownContext;
    pub use rusternetes_common::{Error, Result};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::leaderelection::*;

    /// Scripted in-memory lock recording every API interaction (the fake
    /// clientset of leaderelection_test.go).
    struct FakeLock {
        identity: String,
        stored: Mutex<Option<LeaderElectionRecord>>,
        ops: Mutex<Vec<&'static str>>,
        events: Mutex<Vec<String>>,
    }

    impl FakeLock {
        fn new(identity: &str, stored: Option<LeaderElectionRecord>) -> Arc<Self> {
            Arc::new(Self {
                identity: identity.to_string(),
                stored: Mutex::new(stored),
                ops: Mutex::default(),
                events: Mutex::default(),
            })
        }
        fn ops(&self) -> Vec<&'static str> {
            self.ops.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ResourceLock for FakeLock {
        async fn get(&self) -> Result<LeaderElectionRecord> {
            self.ops.lock().unwrap().push("get");
            self.stored
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| Error::NotFound("lease".into()))
        }
        async fn create(&self, ler: &LeaderElectionRecord) -> Result<()> {
            self.ops.lock().unwrap().push("create");
            *self.stored.lock().unwrap() = Some(ler.clone());
            Ok(())
        }
        async fn update(&self, ler: &LeaderElectionRecord) -> Result<()> {
            self.ops.lock().unwrap().push("update");
            *self.stored.lock().unwrap() = Some(ler.clone());
            Ok(())
        }
        async fn record_event(&self, s: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("{} {}", self.identity, s));
        }
        fn describe(&self) -> String {
            "foo/bar".into()
        }
        fn identity(&self) -> String {
            self.identity.clone()
        }
    }

    fn callbacks() -> LeaderCallbacks {
        LeaderCallbacks {
            on_started_leading: Arc::new(|_| {}),
            on_stopped_leading: Arc::new(|| {}),
            on_new_leader: None,
        }
    }

    fn config(lock: Arc<dyn ResourceLock>) -> ElectionConfig {
        ElectionConfig {
            lock,
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(2),
            retry_period: Duration::from_secs(1),
            release_on_cancel: false,
            name: "test".into(),
            callbacks: callbacks(),
        }
    }

    /// TestLeaderElectionConfigValidation + NewLeaderElector's checks.
    #[test]
    fn config_validation() {
        let lock = FakeLock::new("baz", None);
        assert!(LeaderElector::new(config(lock.clone())).is_ok());

        let err = |c: ElectionConfig| LeaderElector::new(c).err().unwrap().to_string();
        assert!(err(ElectionConfig {
            lease_duration: Duration::from_secs(2),
            ..config(lock.clone())
        })
        .contains("leaseDuration must be greater than renewDeadline"));
        assert!(err(ElectionConfig {
            renew_deadline: Duration::from_millis(1100),
            retry_period: Duration::from_secs(1),
            ..config(lock.clone())
        })
        .contains("renewDeadline must be greater than retryPeriod*JitterFactor"));
        assert!(err(config(FakeLock::new("", None))).contains("Lock identity is empty"));
    }

    fn elector_at(
        lock: Arc<FakeLock>,
        observed: LeaderElectionRecord,
        observed_time: DateTime<Utc>,
    ) -> LeaderElector {
        let mut cfg = config(lock);
        cfg.lease_duration = Duration::from_secs(10);
        let e = LeaderElector::new(cfg).unwrap();
        {
            let mut o = e.observed.lock().unwrap();
            o.record = observed;
            o.time = observed_time;
        }
        e
    }

    fn rec(holder: &str, dur: i32) -> LeaderElectionRecord {
        LeaderElectionRecord {
            holder_identity: holder.into(),
            lease_duration_seconds: dur,
            ..Default::default()
        }
    }

    /// TestTryAcquireOrRenewLeases (leaderelection_test.go:78-): each case's
    /// API interactions, outcome, holder and transition count.
    #[tokio::test]
    async fn try_acquire_or_renew_cases() {
        let future = Utc::now() + chrono::Duration::hours(1000);
        let past = Utc::now() - chrono::Duration::hours(1000);
        struct Case {
            name: &'static str,
            stored: Option<LeaderElectionRecord>,
            observed: LeaderElectionRecord,
            observed_time: DateTime<Utc>,
            ops: Vec<&'static str>,
            success: bool,
            transition: bool,
            holder: &'static str,
        }
        let epoch = DateTime::<Utc>::UNIX_EPOCH;
        let cases = vec![
            Case {
                name: "acquire from no object",
                stored: None,
                observed: rec("", 0),
                observed_time: epoch,
                ops: vec!["get", "create"],
                success: true,
                transition: false,
                holder: "baz",
            },
            Case {
                name: "acquire from object without annotations",
                stored: Some(rec("", 0)),
                observed: rec("", 0),
                observed_time: epoch,
                ops: vec!["get", "update"],
                success: true,
                transition: true,
                holder: "baz",
            },
            Case {
                name: "acquire from unled object",
                stored: Some(rec("", 0)),
                observed: rec("", 0),
                observed_time: epoch,
                ops: vec!["get", "update"],
                success: true,
                transition: true,
                holder: "baz",
            },
            Case {
                name: "acquire from led, unacked object",
                stored: Some(rec("bing", 0)),
                observed: rec("bing", 0),
                observed_time: past,
                ops: vec!["get", "update"],
                success: true,
                transition: true,
                holder: "baz",
            },
            Case {
                name: "acquire from empty led, acked object",
                stored: Some(rec("", 0)),
                observed: rec("", 0),
                observed_time: future,
                ops: vec!["get", "update"],
                success: true,
                transition: true,
                holder: "baz",
            },
            Case {
                name: "don't acquire from led, acked object",
                stored: Some(rec("bing", 10)),
                observed: rec("bing", 10),
                observed_time: future,
                ops: vec!["get"],
                success: false,
                transition: false,
                holder: "bing",
            },
            // Upstream sees get+update only because LeaseLock.Update errors
            // before a get ("lease not initialized"), sending the fast path
            // to the slow path; FakeLock has no such state, so it is the
            // optimistic update alone.
            Case {
                name: "renew already acquired object",
                stored: Some(rec("baz", 0)),
                observed: rec("baz", 0),
                observed_time: future,
                ops: vec!["update"],
                success: true,
                transition: false,
                holder: "baz",
            },
        ];
        for c in cases {
            let lock = FakeLock::new("baz", c.stored);
            let e = elector_at(lock.clone(), c.observed, c.observed_time);
            assert_eq!(e.try_acquire_or_renew().await, c.success, "{}", c.name);
            assert_eq!(lock.ops(), c.ops, "{}: api interactions", c.name);
            let o = e.observed.lock().unwrap();
            assert_eq!(o.record.holder_identity, c.holder, "{}", c.name);
            assert_eq!(
                o.record.leader_transitions,
                i32::from(c.transition),
                "{}",
                c.name
            );
        }
    }

    /// "acquire from led object with the lease duration seconds": the holder's
    /// record is observed first, then the lease expires after its duration.
    #[tokio::test]
    async fn acquire_after_observed_lease_expires() {
        let lock = FakeLock::new("baz", Some(rec("bing", 1)));
        let clock_now = Arc::new(Mutex::new(Utc::now()));
        let mut e = elector_at(lock.clone(), rec("", 0), DateTime::<Utc>::UNIX_EPOCH);
        let c = clock_now.clone();
        e.clock = Arc::new(move || *c.lock().unwrap());
        assert!(!e.try_acquire_or_renew().await, "held and unexpired");
        *clock_now.lock().unwrap() += chrono::Duration::seconds(2);
        assert!(e.try_acquire_or_renew().await, "expired after duration");
        assert_eq!(e.observed.lock().unwrap().record.leader_transitions, 1);
    }

    /// TestFastPathLeaderElection: a leader with a valid lease updates without
    /// a preceding get.
    #[tokio::test]
    async fn leader_fast_path_skips_get() {
        let lock = FakeLock::new("baz", Some(rec("baz", 10)));
        let e = elector_at(lock.clone(), rec("baz", 10), Utc::now());
        assert!(e.try_acquire_or_renew().await);
        assert_eq!(lock.ops(), vec!["update"]);
    }

    /// TestReleaseLeaseLeases / TestReleaseMethodCallsGet: release gets, then
    /// updates with an empty holder and LeaseDurationSeconds 1.
    #[tokio::test]
    async fn release_gives_up_lease() {
        let lock = FakeLock::new("baz", Some(rec("baz", 10)));
        let e = elector_at(lock.clone(), rec("baz", 10), Utc::now());
        assert!(e.release().await);
        assert_eq!(lock.ops(), vec!["get", "update"]);
        let s = lock.stored.lock().unwrap().clone().unwrap();
        assert_eq!(s.holder_identity, "");
        assert_eq!(s.lease_duration_seconds, 1);
        assert_eq!(e.observed.lock().unwrap().record, s);
    }

    /// release() of a lease someone else holds only gets (leaderelection.go:
    /// `if !le.IsLeader() { return true }`).
    #[tokio::test]
    async fn release_when_not_leader_only_gets() {
        let lock = FakeLock::new("baz", Some(rec("bing", 10)));
        let e = elector_at(lock.clone(), rec("bing", 10), Utc::now());
        assert!(e.release().await);
        assert_eq!(lock.ops(), vec!["get"]);
    }

    /// OnNewLeader reports the observed holder once per transition
    /// (maybeReportTransition).
    #[tokio::test]
    async fn reports_new_leader_once() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let s = seen.clone();
        let lock = FakeLock::new("baz", None);
        let mut cfg = config(lock);
        cfg.callbacks.on_new_leader = Some(Arc::new(move |l| s.lock().unwrap().push(l)));
        let e = LeaderElector::new(cfg).unwrap();
        assert!(e.try_acquire_or_renew().await);
        e.maybe_report_transition();
        e.maybe_report_transition();
        assert_eq!(*seen.lock().unwrap(), vec!["baz".to_string()]);
    }

    fn fast_config(lock: Arc<FakeLock>) -> ElectionConfig {
        ElectionConfig {
            lease_duration: Duration::from_millis(900),
            renew_deadline: Duration::from_millis(300),
            retry_period: Duration::from_millis(50),
            ..config(lock)
        }
    }

    /// TestReleaseOnCancellation_Leases: cancelling ctx while leading releases
    /// the lock, fires OnStoppedLeading, and records the events.
    #[tokio::test]
    async fn release_on_cancel_releases_and_stops() {
        let lock = FakeLock::new("baz", None);
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut cfg = fast_config(lock.clone());
        cfg.release_on_cancel = true;
        let n = started.clone();
        cfg.callbacks.on_started_leading = Arc::new(move |_| n.notify_one());
        let st = stopped.clone();
        cfg.callbacks.on_stopped_leading =
            Arc::new(move || st.store(true, std::sync::atomic::Ordering::SeqCst));
        let e = Arc::new(LeaderElector::new(cfg).unwrap());
        let ctx = ShutdownContext::new();
        let run = tokio::spawn({
            let e = e.clone();
            let ctx = ctx.clone();
            async move { e.run(ctx).await }
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("became leader");
        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run returns after cancel")
            .unwrap();
        let s = lock.stored.lock().unwrap().clone().unwrap();
        assert_eq!(s.holder_identity, "", "lock released");
        assert_eq!(s.lease_duration_seconds, 1);
        assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            *lock.events.lock().unwrap(),
            vec!["baz became leader", "baz stopped leading"]
        );
    }

    /// Run returns (firing OnStoppedLeading) as soon as a renew cannot succeed
    /// within RenewDeadline: it never resumes as a follower
    /// (leaderelection.go:208-230). Another holder steals the lease.
    #[tokio::test]
    async fn run_returns_when_lease_lost() {
        let lock = FakeLock::new("baz", None);
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut cfg = fast_config(lock.clone());
        let n = started.clone();
        cfg.callbacks.on_started_leading = Arc::new(move |_| n.notify_one());
        let st = stopped.clone();
        cfg.callbacks.on_stopped_leading =
            Arc::new(move || st.store(true, std::sync::atomic::Ordering::SeqCst));
        let e = Arc::new(LeaderElector::new(cfg).unwrap());
        let run = tokio::spawn({
            let e = e.clone();
            async move { e.run(ShutdownContext::new()).await }
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("became leader");
        *lock.stored.lock().unwrap() = Some(LeaderElectionRecord {
            holder_identity: "other".into(),
            lease_duration_seconds: 3600,
            renew_time: Utc::now(),
            ..Default::default()
        });
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run returns once the lease is lost")
            .unwrap();
        assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!e.is_leader());
    }

    /// Run with a cancelled ctx and no lease acquired still fires
    /// OnStoppedLeading (the deferred call) and never OnStartedLeading.
    #[tokio::test]
    async fn cancel_before_acquire_stops_without_leading() {
        let lock = FakeLock::new("baz", Some(rec("bing", 3600)));
        {
            *lock.stored.lock().unwrap() = Some(LeaderElectionRecord {
                renew_time: Utc::now(),
                ..rec("bing", 3600)
            });
        }
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut cfg = fast_config(lock);
        let s = started.clone();
        cfg.callbacks.on_started_leading =
            Arc::new(move |_| s.store(true, std::sync::atomic::Ordering::SeqCst));
        let st = stopped.clone();
        cfg.callbacks.on_stopped_leading =
            Arc::new(move || st.store(true, std::sync::atomic::Ordering::SeqCst));
        let e = LeaderElector::new(cfg).unwrap();
        let ctx = ShutdownContext::new();
        let c = ctx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            c.cancel();
        });
        tokio::time::timeout(Duration::from_secs(5), e.run(ctx))
            .await
            .expect("run returns on cancel");
        assert!(!started.load(std::sync::atomic::Ordering::SeqCst));
        assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
    }
}
