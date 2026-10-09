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
struct TimestampedStats {
    stats: Stats,
    last_updated_at: Instant,
}

/// `objectCountTracker` / `StorageObjectCountTracker`.
pub struct ObjectCountTracker {
    clock: Arc<dyn PassiveClock>,
    counts: Mutex<HashMap<String, TimestampedStats>>,
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
        let _ = (group_resource, stats);
        todo!()
    }

    /// `Get`.
    pub fn get(&self, group_resource: &str) -> (Stats, Option<StatsError>) {
        let _ = group_resource;
        todo!()
    }

    /// `prune`.
    fn prune(&self, threshold: Duration) {
        let _ = threshold;
        todo!()
    }

    /// `RunUntil`.
    pub async fn run_until(&self, stop: tokio::sync::watch::Receiver<bool>) {
        let _ = stop;
        todo!()
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

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeClock(Mutex<Instant>);
    impl FakeClock {
        fn new(t: Instant) -> Arc<Self> {
            Arc::new(Self(Mutex::new(t)))
        }
        fn set(&self, t: Instant) {
            *self.0.lock().unwrap() = t;
        }
    }
    impl PassiveClock for FakeClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    fn st(n: i64) -> Stats {
        Stats {
            object_count: n,
            estimated_average_object_size_bytes: 0,
        }
    }

    /// `TestStorageObjectCountTracker` (object_count_tracker_test.go:31).
    #[test]
    fn storage_object_count_tracker() {
        struct Case {
            name: &'static str,
            last_updated: Duration,
            skip_setting: bool,
            count: i64,
            err_expected: Option<StatsError>,
            count_expected: i64,
        }
        let cases = [
            Case {
                name: "object count not tracked for given resource",
                last_updated: Duration::ZERO,
                skip_setting: true,
                count: 0,
                err_expected: Some(StatsError::NotFound),
                count_expected: 0,
            },
            Case {
                name: "object count is zero",
                last_updated: Duration::ZERO,
                skip_setting: false,
                count: 0,
                err_expected: None,
                count_expected: 0,
            },
            Case {
                name: "object count is more than zero",
                last_updated: Duration::ZERO,
                skip_setting: false,
                count: 799,
                err_expected: None,
                count_expected: 799,
            },
            Case {
                name: "object count stale",
                last_updated: STALE_TOLERATION_THRESHOLD + Duration::from_millis(1),
                skip_setting: false,
                count: 799,
                err_expected: Some(StatsError::Stale),
                count_expected: 799,
            },
        ];
        for c in cases {
            // Go sets the clock to `now - lastUpdated`; Instant cannot always
            // go backwards, so run `last_updated` later instead (same delta).
            let base = Instant::now();
            let clock = FakeClock::new(base);
            let tracker = ObjectCountTracker::with_clock(clock.clone());
            let key = "foo.bar.resource";
            if !c.skip_setting {
                tracker.set(key, st(c.count));
            }
            clock.set(base + c.last_updated);
            let (stats, err) = tracker.get(key);
            assert_eq!(c.err_expected, err, "{}", c.name);
            assert_eq!(c.count_expected, stats.object_count, "{}", c.name);
        }
    }

    /// `TestStorageObjectCountTrackerWithPrune` (object_count_tracker_test.go:94).
    #[test]
    fn storage_object_count_tracker_with_prune() {
        let base = Instant::now();
        let min = |m: u64| Duration::from_secs(60 * m);
        let clock = FakeClock::new(base);
        let tracker = ObjectCountTracker::with_clock(clock.clone());
        // Go: now-61m, now-60m, now-59m, then now. Shift by +61m.
        tracker.set("k1", st(61));
        clock.set(base + min(1));
        tracker.set("k2", st(60));
        clock.set(base + min(2));
        tracker.set("k3", st(59));
        clock.set(base + min(61));
        tracker.prune(Duration::from_secs(3600));

        let counts = tracker.counts.lock().unwrap();
        let mut expected = HashMap::new();
        expected.insert(
            "k3".to_string(),
            TimestampedStats {
                stats: st(59),
                last_updated_at: base + min(2),
            },
        );
        assert_eq!(expected, *counts);
    }

    /// `statsGetterFn` adapter maps the pair onto the estimator's `Result`.
    #[test]
    fn stats_getter_maps_errors() {
        let base = Instant::now();
        let clock = FakeClock::new(base);
        let tracker = Arc::new(ObjectCountTracker::with_clock(clock.clone()));
        let g = tracker.stats_getter();
        assert_eq!(g("a.b"), Err(StatsError::NotFound));
        tracker.set("a.b", st(5));
        assert_eq!(g("a.b"), Ok(st(5)));
        clock.set(base + STALE_TOLERATION_THRESHOLD + Duration::from_millis(1));
        assert_eq!(g("a.b"), Err(StatsError::Stale));
    }
}
