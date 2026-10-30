//! `resourcelock.LeaseLock` (client-go
//! `tools/leaderelection/resourcelock/leaselock.go`, release-1.35) over a
//! [`Storage`] backend, so the controller-manager and scheduler elect through a
//! `coordination.k8s.io/v1` Lease on every backend (etcd, SQLite, Redis).

use crate::{EventRecorder, Storage};
use async_trait::async_trait;
use rusternetes_common::leaderelection::{LeaderElectionRecord, ResourceLock};
use rusternetes_common::resources::coordination::{Lease, LeaseSpec};
use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
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
    /// `LockConfig.EventRecorder` (resourcelock/interface.go:72-73), optional:
    /// the recorder plus the `EventSource` it stamps on every event.
    recorder: Option<(EventRecorder<S>, EventSource)>,
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
            recorder: None,
        }
    }

    /// Sets `ResourceLockConfig.EventRecorder`; `component` is the
    /// `EventSource.Component` of the recorder (kube-controller-manager's
    /// user agent, the scheduler profile name).
    pub fn with_event_recorder(mut self, recorder: EventRecorder<S>, component: &str) -> Self {
        self.recorder = Some((
            recorder,
            EventSource {
                component: component.to_string(),
                host: None,
            },
        ));
        self
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

    /// `RecordEvent` (leaselock.go:98-109): a Normal `LeaderElection` event
    /// "<identity> <s>" on the cached Lease (`ll.lease.ObjectMeta`, so an
    /// empty reference before any get/create), typed as coordination.k8s.io/v1
    /// Lease. A no-op without a recorder.
    async fn record_event(&self, s: &str) {
        let Some((recorder, source)) = &self.recorder else {
            return;
        };
        let meta = self
            .lease
            .lock()
            .unwrap()
            .as_ref()
            .map(|l| l.metadata.clone())
            .unwrap_or_default();
        let involved = ObjectReference {
            api_version: Some("coordination.k8s.io/v1".into()),
            kind: Some("Lease".into()),
            name: Some(meta.name.clone()),
            namespace: meta.namespace.clone(),
            uid: Some(meta.uid.clone()),
            resource_version: meta.resource_version.clone(),
            field_path: None,
        };
        let message = format!("{} {}", self.identity, s);
        if let Err(e) = recorder
            .event(
                &involved,
                source,
                EventType::Normal,
                "LeaderElection",
                &message,
            )
            .await
        {
            tracing::warn!(error = %e, "failed to record leader election event");
        }
    }

    fn describe(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}
