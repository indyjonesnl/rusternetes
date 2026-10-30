//! Unit tests moved from `src/flow_control_object_count.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::flow_control_work_estimator::{Stats, StatsError};
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::flow_control_object_count::*;

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
