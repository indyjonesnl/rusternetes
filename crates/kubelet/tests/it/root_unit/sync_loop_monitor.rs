//! `SyncLoopHealthCheck` (`pkg/kubelet/kubelet.go:3096-3107`) and the
//! `syncLoopMonitor` it reads.
use rusternetes_kubelet::sync_loop_monitor::{SyncLoopMonitor, MIN_SYNC_LOOP_STALENESS};
use std::time::{Duration, Instant};

const MIN: Duration = Duration::from_secs(60);

#[test]
fn passes_before_the_loop_has_ever_run() {
    // `enterLoopTime.IsZero()`: no failure until the loop starts.
    let m = SyncLoopMonitor::default();
    assert!(m.latest_loop_entry_time().is_none());
    assert!(m
        .health_check(Instant::now() + 10 * MIN, Duration::from_secs(10))
        .is_ok());
}

#[test]
fn fails_only_after_the_minimum_five_minutes() {
    let m = SyncLoopMonitor::default();
    let t = Instant::now();
    m.store(t);
    let resync = Duration::from_secs(60); // 2*resync = 2m < 5m floor
    assert!(
        m.health_check(t + MIN_SYNC_LOOP_STALENESS, resync).is_ok(),
        "not After()"
    );
    assert_eq!(
        m.health_check(
            t + MIN_SYNC_LOOP_STALENESS + Duration::from_millis(1),
            resync
        ),
        Err("sync Loop took longer than expected".to_string())
    );
}

#[test]
fn threshold_scales_with_a_long_resync_interval() {
    let m = SyncLoopMonitor::default();
    let t = Instant::now();
    m.store(t);
    let resync = Duration::from_secs(10 * 60); // 2*resync = 20m
    assert!(m
        .health_check(t + Duration::from_secs(19 * 60), resync)
        .is_ok());
    assert!(m
        .health_check(t + Duration::from_secs(21 * 60), resync)
        .is_err());
}

#[test]
fn a_fresh_store_recovers_the_check() {
    let m = SyncLoopMonitor::default();
    let t = Instant::now();
    m.store(t);
    let late = t + Duration::from_secs(10 * 60);
    assert!(m.health_check(late, Duration::from_secs(3)).is_err());
    m.store(late);
    assert!(m.health_check(late, Duration::from_secs(3)).is_ok());
}
