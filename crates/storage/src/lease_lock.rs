//! `resourcelock.LeaseLock` (client-go
//! `tools/leaderelection/resourcelock/leaselock.go`, release-1.35) over a
//! [`Storage`] backend, so the controller-manager and scheduler elect through a
//! `coordination.k8s.io/v1` Lease on every backend (etcd, SQLite, Redis).

use crate::Storage;
use async_trait::async_trait;
use rusternetes_common::leaderelection::{LeaderElectionRecord, ResourceLock};
use rusternetes_common::resources::coordination::{Lease, LeaseSpec};
use rusternetes_common::{Error, Result};
use std::sync::{Arc, Mutex};

pub struct LeaseLock<S: Storage> {
    storage: Arc<S>,
    namespace: String,
    name: String,
    identity: String,
    /// `ll.lease`: the object last returned by get/create/update, whose
    /// resourceVersion guards the next update.
    lease: Mutex<Option<Lease>>,
}

impl<S: Storage> LeaseLock<S> {
    pub fn new(
        storage: Arc<S>,
        namespace: impl Into<String>,
        name: impl Into<String>,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            storage,
            namespace: namespace.into(),
            name: name.into(),
            identity: identity.into(),
            lease: Mutex::new(None),
        }
    }

    fn key(&self) -> String {
        format!("/registry/leases/{}/{}", self.namespace, self.name)
    }
}

/// `LeaseSpecToLeaderElectionRecord` (leaselock.go).
pub fn lease_spec_to_record(spec: &LeaseSpec) -> LeaderElectionRecord {
    let zero = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
    LeaderElectionRecord {
        holder_identity: spec.holder_identity.clone().unwrap_or_default(),
        lease_duration_seconds: spec.lease_duration_seconds.unwrap_or(0),
        acquire_time: spec.acquire_time.unwrap_or(zero),
        renew_time: spec.renew_time.unwrap_or(zero),
        leader_transitions: spec.lease_transitions.unwrap_or(0),
        preferred_holder: spec.preferred_holder.clone().unwrap_or_default(),
        strategy: spec.strategy.clone().unwrap_or_default(),
    }
}

/// `LeaderElectionRecordToLeaseSpec` (leaselock.go): holder, duration, times
/// and transitions always set; preferredHolder / strategy only when non-empty.
pub fn record_to_lease_spec(r: &LeaderElectionRecord) -> LeaseSpec {
    LeaseSpec {
        holder_identity: Some(r.holder_identity.clone()),
        lease_duration_seconds: Some(r.lease_duration_seconds),
        acquire_time: Some(r.acquire_time),
        renew_time: Some(r.renew_time),
        lease_transitions: Some(r.leader_transitions),
        preferred_holder: (!r.preferred_holder.is_empty()).then(|| r.preferred_holder.clone()),
        strategy: (!r.strategy.is_empty()).then(|| r.strategy.clone()),
    }
}

#[async_trait]
impl<S: Storage> ResourceLock for LeaseLock<S> {
    /// `Get`: reads the Lease and caches it for the next `update`.
    async fn get(&self) -> Result<LeaderElectionRecord> {
        let lease: Lease = self.storage.get(&self.key()).await?;
        let record = lease_spec_to_record(&lease.spec.clone().unwrap_or_default());
        *self.lease.lock().unwrap() = Some(lease);
        Ok(record)
    }

    /// `Create`.
    async fn create(&self, ler: &LeaderElectionRecord) -> Result<()> {
        let lease = Lease::new(&self.name, &self.namespace).with_spec(record_to_lease_spec(ler));
        let created = self.storage.create(&self.key(), &lease).await?;
        *self.lease.lock().unwrap() = Some(created);
        Ok(())
    }

    /// `Update`: errors before a get/create, as upstream's
    /// "lease not initialized, call get or create first"; the cached
    /// resourceVersion makes the write an optimistic update.
    async fn update(&self, ler: &LeaderElectionRecord) -> Result<()> {
        let mut lease = self.lease.lock().unwrap().clone().ok_or_else(|| {
            Error::Internal("lease not initialized, call get or create first".into())
        })?;
        lease.spec = Some(record_to_lease_spec(ler));
        let updated = self.storage.update(&self.key(), &lease).await?;
        *self.lease.lock().unwrap() = Some(updated);
        Ok(())
    }

    fn describe(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorage;
    use chrono::Utc;
    use rusternetes_common::leaderelection::{ElectionConfig, LeaderCallbacks, LeaderElector};
    use rusternetes_common::signal::ShutdownContext;
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
