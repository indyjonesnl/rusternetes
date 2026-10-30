//! API Priority and Fairness: the storage object-count tracker.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/util/flowcontrol/request/
//! object_count_tracker.go` (release-1.35): `StorageObjectCountTracker`,
//! `objectCountTracker`, `timestampedStats`, `ObjectCountNotFoundErr` /
//! `ObjectCountStaleErr`, `pruneInterval` and `staleTolerationThreshold`.
//!
//! It feeds the list work estimator's `statsGetterFn`
//! (`flow_control_work_estimator`): [`ObjectCountTracker::stats_getter`] is the
//! Rust spelling of `objectCountTracker.Get` used as that closure.
//!
//! DELIBERATE DEVIATIONS (Rust expression, not mechanism):
//!
//! - `RunUntil(stopCh)` becomes [`ObjectCountTracker::run_until`] taking a
//!   `tokio::sync::watch::Receiver<bool>` (true = stop); the prune interval and
//!   body are unchanged.
//! - Go's `Get` returns `(Stats, error)` with the stale entry's stats alongside
//!   `ObjectCountStaleErr`; [`ObjectCountTracker::get`] returns the same pair as
//!   `(Stats, Option<StatsError>)`.
//! - The producer (upstream `storage/etcd3/stats.go` `statsCache` poller calling
//!   `Set`) is not ported here; see the follow-up issue.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::flow_control_work_estimator::{Stats, StatsError};

/// `pruneInterval` (object_count_tracker.go:35).
pub const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

/// `staleTolerationThreshold` (object_count_tracker.go:39).
pub const STALE_TOLERATION_THRESHOLD: Duration = Duration::from_secs(180);

/// `clock.PassiveClock`.
pub trait PassiveClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// `clock.RealClock`.
pub struct RealClock;

impl PassiveClock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// `timestampedStats` (object_count_tracker.go:85).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimestampedStats {
    pub stats: Stats,
    pub last_updated_at: Instant,
}

/// `objectCountTracker` / `StorageObjectCountTracker`.
pub struct ObjectCountTracker {
    pub clock: Arc<dyn PassiveClock>,
    pub counts: Mutex<HashMap<String, TimestampedStats>>,
}

impl ObjectCountTracker {
    /// `NewStorageObjectCountTracker`.
    pub fn new() -> Self {
        Self::with_clock(Arc::new(RealClock))
    }

    pub fn with_clock(clock: Arc<dyn PassiveClock>) -> Self {
        Self {
            clock,
            counts: Mutex::new(HashMap::new()),
        }
    }

    /// `Set`.
    pub fn set(&self, group_resource: &str, stats: Stats) {
        let now = self.clock.now();
        let mut counts = self.counts.lock().unwrap();
        counts.insert(
            group_resource.to_string(),
            TimestampedStats {
                stats,
                last_updated_at: now,
            },
        );
    }

    /// `Get`.
    pub fn get(&self, group_resource: &str) -> (Stats, Option<StatsError>) {
        let now = self.clock.now();
        let counts = self.counts.lock().unwrap();
        match counts.get(group_resource) {
            Some(item) => {
                // `lastUpdatedAt.Before(now - staleTolerationThreshold)`
                if now.duration_since(item.last_updated_at) > STALE_TOLERATION_THRESHOLD {
                    (item.stats, Some(StatsError::Stale))
                } else {
                    (item.stats, None)
                }
            }
            None => (Stats::default(), Some(StatsError::NotFound)),
        }
    }

    /// `prune`.
    pub fn prune(&self, threshold: Duration) {
        let now = self.clock.now();
        let mut counts = self.counts.lock().unwrap();
        // Keep entries with `lastUpdatedAt.After(now - threshold)`.
        counts.retain(|_, c| now.saturating_duration_since(c.last_updated_at) < threshold);
    }

    /// `RunUntil`.
    pub async fn run_until(&self, mut stop: tokio::sync::watch::Receiver<bool>) {
        // `wait.PollUntil(pruneInterval, ...)`: first prune after one interval.
        loop {
            tokio::select! {
                _ = tokio::time::sleep(PRUNE_INTERVAL) => self.prune(PRUNE_INTERVAL),
                r = stop.changed() => {
                    if r.is_err() || *stop.borrow() {
                        break;
                    }
                }
            }
        }
        tracing::info!("StorageObjectCountTracker pruner is exiting");
    }

    /// `Get` as the estimator's `statsGetterFn`.
    pub fn stats_getter(self: &Arc<Self>) -> crate::flow_control_work_estimator::StatsGetter {
        let t = self.clone();
        Box::new(move |gr| match t.get(gr) {
            (s, None) => Ok(s),
            (_, Some(e)) => Err(e),
        })
    }
}

impl Default for ObjectCountTracker {
    fn default() -> Self {
        Self::new()
    }
}
