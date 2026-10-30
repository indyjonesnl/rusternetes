//! Lease-lock leader election, a port of client-go
//! `staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go` and
//! `resourcelock/{interface.go,leaselock.go}` (release-1.35).
//!
//! The lock is a `coordination.k8s.io/v1` Lease carrying a
//! [`LeaderElectionRecord`]; the elector is `LeaderElector` with
//! `tryAcquireOrRenew`, `acquire` / `renew` (`RetryPeriod`, `RenewDeadline`,
//! `JitterFactor`), `release` (`ReleaseOnCancel`) and the
//! `OnStartedLeading` / `OnStoppedLeading` / `OnNewLeader` callbacks.
//!
//! Not ported: coordinated leader election (`tryCoordinatedRenew`,
//! `PreferredHolder` / `Strategy` handling beyond round-tripping the fields),
//! the watchdog health check, and metrics.

use crate::signal::ShutdownContext;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info};

/// `JitterFactor` (leaderelection.go:68).
pub const JITTER_FACTOR: f64 = 1.2;

/// `resourcelock.LeaderElectionRecord` (interface.go). Zero times are the Unix
/// epoch, the analogue of Go's zero `metav1.Time`.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderElectionRecord {
    pub holder_identity: String,
    pub lease_duration_seconds: i32,
    pub acquire_time: DateTime<Utc>,
    pub renew_time: DateTime<Utc>,
    pub leader_transitions: i32,
    pub preferred_holder: String,
    pub strategy: String,
}

impl Default for LeaderElectionRecord {
    fn default() -> Self {
        Self {
            holder_identity: String::new(),
            lease_duration_seconds: 0,
            acquire_time: DateTime::<Utc>::UNIX_EPOCH,
            renew_time: DateTime::<Utc>::UNIX_EPOCH,
            leader_transitions: 0,
            preferred_holder: String::new(),
            strategy: String::new(),
        }
    }
}

/// `resourcelock.Interface`. `get` returns `Error::NotFound` when the lock
/// object does not exist (`errors.IsNotFound`).
#[async_trait]
pub trait ResourceLock: Send + Sync {
    async fn get(&self) -> Result<LeaderElectionRecord>;
    async fn create(&self, ler: &LeaderElectionRecord) -> Result<()>;
    async fn update(&self, ler: &LeaderElectionRecord) -> Result<()>;
    /// `RecordEvent`: emits a `LeaderElection` event; best effort.
    async fn record_event(&self, _s: &str) {}
    fn describe(&self) -> String;
    fn identity(&self) -> String;
}

/// `leaderelection.LeaderCallbacks`.
#[derive(Clone)]
pub struct LeaderCallbacks {
    /// Run when a candidate acquires the lease; the context is cancelled when
    /// leadership ends.
    pub on_started_leading: Arc<dyn Fn(ShutdownContext) + Send + Sync>,
    /// Run when the elector stops leading (also when it never led and `ctx`
    /// was cancelled), exactly as the deferred call in `Run`.
    pub on_stopped_leading: Arc<dyn Fn() + Send + Sync>,
    /// Run when the observed holder changes.
    pub on_new_leader: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

/// `leaderelection.LeaderElectionConfig`.
#[derive(Clone)]
pub struct ElectionConfig {
    pub lock: Arc<dyn ResourceLock>,
    pub lease_duration: Duration,
    pub renew_deadline: Duration,
    pub retry_period: Duration,
    pub release_on_cancel: bool,
    pub name: String,
    pub callbacks: LeaderCallbacks,
}

pub struct Observed {
    pub record: LeaderElectionRecord,
    pub time: DateTime<Utc>,
    pub reported_leader: String,
}

/// `leaderelection.LeaderElector`.
pub struct LeaderElector {
    pub config: ElectionConfig,
    pub clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    pub observed: Mutex<Observed>,
}

impl LeaderElector {
    /// `NewLeaderElector` (leaderelection.go:73-110); same checks, same order,
    /// same messages.
    pub fn new(config: ElectionConfig) -> Result<Self> {
        let invalid = |m: &str| Err(Error::InvalidResource(m.to_string()));
        if config.lease_duration <= config.renew_deadline {
            return invalid("leaseDuration must be greater than renewDeadline");
        }
        if config.renew_deadline <= config.retry_period.mul_f64(JITTER_FACTOR) {
            return invalid("renewDeadline must be greater than retryPeriod*JitterFactor");
        }
        if config.lease_duration.is_zero() {
            return invalid("leaseDuration must be greater than zero");
        }
        if config.renew_deadline.is_zero() {
            return invalid("renewDeadline must be greater than zero");
        }
        if config.retry_period.is_zero() {
            return invalid("retryPeriod must be greater than zero");
        }
        if config.lock.identity().is_empty() {
            return invalid("Lock identity is empty");
        }
        Ok(Self {
            config,
            clock: Arc::new(Utc::now),
            observed: Mutex::new(Observed {
                record: LeaderElectionRecord::default(),
                time: DateTime::<Utc>::UNIX_EPOCH,
                reported_leader: String::new(),
            }),
        })
    }

    /// `Run` (leaderelection.go:196-206): returns when `ctx` is cancelled or
    /// the lease is no longer held; `OnStoppedLeading` always fires on the way
    /// out (the deferred call), after any `release`.
    pub async fn run(&self, ctx: ShutdownContext) {
        if self.acquire(&ctx).await {
            let leading = ShutdownContext::new();
            // `context.WithCancel(ctx)`: a parent cancel reaches the child.
            let forward = {
                let (parent, child) = (ctx.clone(), leading.clone());
                tokio::spawn(async move {
                    tokio::select! {
                        _ = parent.cancelled() => child.cancel(),
                        _ = child.cancelled() => {}
                    }
                })
            };
            let started = self.config.callbacks.on_started_leading.clone();
            let lead_ctx = leading.clone();
            tokio::spawn(async move { started(lead_ctx) });
            self.renew(&leading).await;
            leading.cancel();
            let _ = forward.await;
        }
        (self.config.callbacks.on_stopped_leading)();
    }

    /// `IsLeader`: the last observed holder is this client.
    pub fn is_leader(&self) -> bool {
        self.observed.lock().unwrap().record.holder_identity == self.config.lock.identity()
    }

    /// `GetLeader`: the last observed holder, empty when none was observed.
    pub fn get_leader(&self) -> String {
        self.observed.lock().unwrap().record.holder_identity.clone()
    }

    /// `JitterUntil(f, RetryPeriod, JitterFactor, sliding=true, ctx.Done())`
    /// sleep: `d + rand(0..1)*JitterFactor*d`, interrupted by `ctx`. Returns
    /// true when `ctx` was cancelled.
    async fn sleep_or_cancel(ctx: &ShutdownContext, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => ctx.is_cancelled(),
            _ = ctx.cancelled() => true,
        }
    }

    fn jittered(d: Duration) -> Duration {
        let r = (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1_000_000.0;
        d + d.mul_f64(r * JITTER_FACTOR)
    }

    /// `acquire` (leaderelection.go:233-262): true once the lease is held,
    /// false if `ctx` is cancelled first.
    async fn acquire(&self, ctx: &ShutdownContext) -> bool {
        let desc = self.config.lock.describe();
        info!(lock = %desc, "Attempting to acquire leader lease...");
        loop {
            if ctx.is_cancelled() {
                return false;
            }
            let succeeded = self.try_acquire_or_renew().await;
            self.maybe_report_transition();
            if succeeded {
                self.config.lock.record_event("became leader").await;
                info!(lock = %desc, "Successfully acquired lease");
                return true;
            }
            debug!(lock = %desc, "Failed to acquire lease");
            if Self::sleep_or_cancel(ctx, Self::jittered(self.config.retry_period)).await {
                return false;
            }
        }
    }

    /// `renew` (leaderelection.go:265-297): renews every `RetryPeriod`, each
    /// attempt retried within `RenewDeadline`; returns when a renewal cannot
    /// succeed in time or `ctx` is cancelled, then releases if
    /// `ReleaseOnCancel`.
    async fn renew(&self, ctx: &ShutdownContext) {
        let desc = self.config.lock.describe();
        loop {
            if ctx.is_cancelled() {
                break;
            }
            // wait.PollUntilContextTimeout(ctx, RetryPeriod, RenewDeadline, immediate=true, ..)
            let poll = async {
                loop {
                    if self.try_acquire_or_renew().await {
                        return true;
                    }
                    tokio::time::sleep(self.config.retry_period).await;
                }
            };
            let renewed = tokio::select! {
                r = tokio::time::timeout(self.config.renew_deadline, poll) => r.unwrap_or(false),
                _ = ctx.cancelled() => false,
            };
            self.maybe_report_transition();
            if !renewed {
                info!(lock = %desc, "Failed to renew lease");
                break;
            }
            debug!(lock = %desc, "Successfully renewed lease");
            if Self::sleep_or_cancel(ctx, self.config.retry_period).await {
                break;
            }
        }
        self.config.lock.record_event("stopped leading").await;
        // if we hold the lease, give it up
        if self.config.release_on_cancel {
            self.release().await;
        }
    }

    /// `release` (leaderelection.go:300-336): gets the lock, and if still the
    /// holder updates it with an empty holder and `LeaseDurationSeconds: 1`.
    /// Bounded by `RenewDeadline`.
    pub async fn release(&self) -> bool {
        let lock = &self.config.lock;
        let work = async {
            // update the resourceVersion of lease
            let old = match lock.get().await {
                Ok(r) => r,
                Err(Error::NotFound(_)) => {
                    info!(lock = %lock.describe(), "lease lock not found");
                    return false;
                }
                Err(e) => {
                    error!(lock = %lock.describe(), error = %e, "error retrieving resource lock");
                    return false;
                }
            };
            if !self.is_leader() {
                return true;
            }
            let now = (self.clock)();
            let record = LeaderElectionRecord {
                leader_transitions: old.leader_transitions,
                lease_duration_seconds: 1,
                renew_time: now,
                acquire_time: now,
                ..Default::default()
            };
            if let Err(e) = lock.update(&record).await {
                error!(lock = %lock.describe(), error = %e, "Failed to release lease");
                return false;
            }
            self.set_observed(&record);
            true
        };
        tokio::time::timeout(self.config.renew_deadline, work)
            .await
            .unwrap_or(false)
    }

    /// `tryAcquireOrRenew` (leaderelection.go:~390-470), non-coordinated path.
    pub async fn try_acquire_or_renew(&self) -> bool {
        let lock = &self.config.lock;
        let now = (self.clock)();
        let mut record = LeaderElectionRecord {
            holder_identity: lock.identity(),
            lease_duration_seconds: self.config.lease_duration.as_secs() as i32,
            renew_time: now,
            acquire_time: now,
            ..Default::default()
        };

        // 1. fast path for the leader to update optimistically assuming that
        // the record observed last time is the current version.
        if self.is_leader() && self.is_lease_valid(now) {
            let old = self.observed.lock().unwrap().record.clone();
            record.acquire_time = old.acquire_time;
            record.leader_transitions = old.leader_transitions;
            match lock.update(&record).await {
                Ok(()) => {
                    self.set_observed(&record);
                    return true;
                }
                Err(e) => error!(
                    lock = %lock.describe(), error = %e,
                    "Failed to update lease optimistically, falling back to slow path"
                ),
            }
        }

        // 2. obtain or create the ElectionRecord
        let old = match lock.get().await {
            Ok(r) => r,
            Err(Error::NotFound(_)) => {
                if let Err(e) = lock.create(&record).await {
                    error!(lock = %lock.describe(), error = %e, "Error initially creating lease lock");
                    return false;
                }
                self.set_observed(&record);
                return true;
            }
            Err(e) => {
                error!(lock = %lock.describe(), error = %e, "Error retrieving lease lock");
                return false;
            }
        };

        // 3. Record obtained, check the Identity & Time
        {
            let mut o = self.observed.lock().unwrap();
            if o.record != old {
                o.record = old.clone();
                o.time = (self.clock)();
            }
        }
        if !old.holder_identity.is_empty() && self.is_lease_valid(now) && !self.is_leader() {
            debug!(
                lock = %lock.describe(), holder = %old.holder_identity,
                "Lease is held by and has not yet expired"
            );
            return false;
        }

        // 4. We're going to try to update. The record is set to its default
        // here. Let's correct it before updating.
        if self.is_leader() {
            record.acquire_time = old.acquire_time;
            record.leader_transitions = old.leader_transitions;
        } else {
            record.leader_transitions = old.leader_transitions + 1;
        }
        if let Err(e) = lock.update(&record).await {
            error!(lock = %lock.describe(), error = %e, "Failed to update lease");
            return false;
        }
        self.set_observed(&record);
        true
    }

    /// `maybeReportTransition`.
    pub fn maybe_report_transition(&self) {
        let holder = {
            let mut o = self.observed.lock().unwrap();
            if o.record.holder_identity == o.reported_leader {
                return;
            }
            o.reported_leader = o.record.holder_identity.clone();
            o.reported_leader.clone()
        };
        if let Some(cb) = &self.config.callbacks.on_new_leader {
            cb(holder);
        }
    }

    /// `isLeaseValid`.
    fn is_lease_valid(&self, now: DateTime<Utc>) -> bool {
        let o = self.observed.lock().unwrap();
        o.time + chrono::Duration::seconds(o.record.lease_duration_seconds as i64) > now
    }

    /// `setObservedRecord`: stores the record and stamps `observedTime`.
    fn set_observed(&self, record: &LeaderElectionRecord) {
        let mut o = self.observed.lock().unwrap();
        o.record = record.clone();
        o.time = (self.clock)();
    }
}
