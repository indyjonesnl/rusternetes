//! Unit tests moved from `src/lease_lock.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::leaderelection::{LeaderElectionRecord, ResourceLock};
    pub use rusternetes_common::resources::coordination::{Lease, LeaseSpec};
    pub use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{EventRecorder, Storage};
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use chrono::Utc;
    use rusternetes_common::leaderelection::{ElectionConfig, LeaderCallbacks, LeaderElector};
    use rusternetes_common::signal::ShutdownContext;
    use rusternetes_storage::lease_lock::*;
    use rusternetes_storage::MemoryStorage;
    use std::time::Duration;

    fn record(holder: &str) -> LeaderElectionRecord {
        LeaderElectionRecord {
            holder_identity: holder.into(),
            lease_duration_seconds: 15,
            acquire_time: Utc::now(),
            renew_time: Utc::now(),
            leader_transitions: 2,
            ..Default::default()
        }
    }

    /// Get of a missing Lease is NotFound (what `tryAcquireOrRenew` keys on).
    #[tokio::test]
    async fn get_missing_is_not_found() {
        let lock = LeaseLock::new(Arc::new(MemoryStorage::new()), "kube-system", "kcm", "a");
        assert!(matches!(lock.get().await, Err(Error::NotFound(_))));
    }

    /// TestLeaseSpecToLeaderElectionRecordRoundTrip: create then get returns
    /// the record; the stored object is a real coordination.k8s.io/v1 Lease.
    #[tokio::test]
    async fn create_get_round_trip_stores_a_lease() {
        let storage = Arc::new(MemoryStorage::new());
        let lock = LeaseLock::new(storage.clone(), "kube-system", "kcm", "a");
        let rec = record("a");
        lock.create(&rec).await.unwrap();
        let stored: Lease = storage
            .get("/registry/leases/kube-system/kcm")
            .await
            .unwrap();
        assert_eq!(stored.type_meta.kind, "Lease");
        assert_eq!(stored.metadata.namespace.as_deref(), Some("kube-system"));
        let spec = stored.spec.unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("a"));
        assert_eq!(spec.lease_transitions, Some(2));
        let got = lock.get().await.unwrap();
        assert_eq!(got.holder_identity, "a");
        assert_eq!(got.leader_transitions, 2);
        assert_eq!(got.lease_duration_seconds, 15);
    }

    /// TestReleaseOnCancellation_Leases' event expectations
    /// (leaderelection_test.go): RecordEvent writes a Normal `LeaderElection`
    /// event "<identity> <s>" about the Lease; without a recorder it is a no-op.
    #[tokio::test]
    async fn record_event_emits_leader_election_event_on_the_lease() {
        let storage = Arc::new(MemoryStorage::new());
        let lock = LeaseLock::new(storage.clone(), "kube-system", "kcm", "a").with_event_recorder(
            EventRecorder::new(storage.clone()),
            "kube-controller-manager",
        );
        lock.create(&record("a")).await.unwrap();
        lock.record_event("became leader").await;
        lock.record_event("stopped leading").await;
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/kube-system/").await.unwrap();
        assert_eq!(events.len(), 1, "same (object, reason) bumps one Event");
        let e = &events[0];
        assert_eq!(e.reason, "LeaderElection");
        assert_eq!(e.event_type, EventType::Normal);
        assert_eq!(e.message, "a stopped leading");
        assert_eq!(e.count, 2);
        assert_eq!(e.source.component, "kube-controller-manager");
        assert_eq!(e.involved_object.kind.as_deref(), Some("Lease"));
        assert_eq!(
            e.involved_object.api_version.as_deref(),
            Some("coordination.k8s.io/v1")
        );
        assert_eq!(e.involved_object.name.as_deref(), Some("kcm"));
        assert!(!e.involved_object.uid.as_deref().unwrap_or("").is_empty());

        let bare = LeaseLock::new(storage.clone(), "other", "kcm", "a");
        bare.create(&record("a")).await.unwrap();
        bare.record_event("became leader").await;
        let none: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/other/").await.unwrap();
        assert!(none.is_empty());
    }

    /// `Update` before get/create is an error (leaselock.go).
    #[tokio::test]
    async fn update_before_get_errors() {
        let lock = LeaseLock::new(Arc::new(MemoryStorage::new()), "kube-system", "kcm", "a");
        let err = lock.update(&record("a")).await.unwrap_err();
        assert!(err.to_string().contains("lease not initialized"));
    }

    /// The cached resourceVersion makes `update` optimistic: a stale writer
    /// loses (the property that makes two electors safe).
    #[tokio::test]
    async fn stale_update_conflicts() {
        let storage = Arc::new(MemoryStorage::new());
        let a = LeaseLock::new(storage.clone(), "kube-system", "kcm", "a");
        let b = LeaseLock::new(storage.clone(), "kube-system", "kcm", "b");
        a.create(&record("a")).await.unwrap();
        b.get().await.unwrap();
        a.update(&record("a")).await.unwrap(); // bumps the resourceVersion
        assert!(
            b.update(&record("b")).await.is_err(),
            "stale write must fail"
        );
        assert_eq!(a.get().await.unwrap().holder_identity, "a");
    }

    /// Two electors on one Lease: exactly one leads; on release the other
    /// takes over without waiting out the full lease duration.
    #[tokio::test]
    async fn two_electors_one_leader_and_handover_on_release() {
        let storage = Arc::new(MemoryStorage::new());
        let mk = |id: &str, release: bool| {
            let lock = Arc::new(LeaseLock::new(storage.clone(), "kube-system", "kcm", id));
            let cfg = ElectionConfig {
                lock,
                lease_duration: Duration::from_secs(30),
                renew_deadline: Duration::from_secs(1),
                retry_period: Duration::from_millis(50),
                release_on_cancel: release,
                name: "kcm".into(),
                callbacks: LeaderCallbacks {
                    on_started_leading: Arc::new(|_| {}),
                    on_stopped_leading: Arc::new(|| {}),
                    on_new_leader: None,
                },
            };
            Arc::new(LeaderElector::new(cfg).unwrap())
        };
        let (a, b) = (mk("a", true), mk("b", true));
        let (ca, cb) = (ShutdownContext::new(), ShutdownContext::new());
        let ra = tokio::spawn({
            let (a, c) = (a.clone(), ca.clone());
            async move { a.run(c).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let rb = tokio::spawn({
            let (b, c) = (b.clone(), cb.clone());
            async move { b.run(c).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(a.is_leader() && !b.is_leader());
        ca.cancel();
        tokio::time::timeout(Duration::from_secs(3), ra)
            .await
            .unwrap()
            .unwrap();
        // 30s lease, so b can only lead this soon if a released it.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !b.is_leader() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("b takes over after a released the lease");
        cb.cancel();
        tokio::time::timeout(Duration::from_secs(3), rb)
            .await
            .unwrap()
            .unwrap();
    }
}
