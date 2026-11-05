//! Port of the kubelet's `syncLoopMonitor` and `SyncLoopHealthCheck`
//! (`pkg/kubelet/kubelet.go:2539,2543,3086-3107`).
//!
//! The sync loop stores the time on entry to and exit from every
//! `syncLoopIteration`. The health check fails only when that time is non-zero
//! (the loop has started) and older than `max(2 * resyncInterval, 5m)`; before
//! the loop first runs the check passes.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// `minDuration := time.Minute * 5` (`kubelet.go:3098`).
pub const MIN_SYNC_LOOP_STALENESS: Duration = Duration::from_secs(5 * 60);

/// `kl.syncLoopMonitor` (an `atomic.Value` holding a `time.Time`).
#[derive(Debug, Default)]
pub struct SyncLoopMonitor {
    latest: Mutex<Option<Instant>>,
}

impl SyncLoopMonitor {
    /// `kl.syncLoopMonitor.Store(kl.clock.Now())`.
    pub fn store(&self, now: Instant) {
        *self.latest.lock().unwrap_or_else(|e| e.into_inner()) = Some(now);
    }

    /// `LatestLoopEntryTime`: `None` is Go's zero `time.Time`.
    pub fn latest_loop_entry_time(&self) -> Option<Instant> {
        *self.latest.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `SyncLoopHealthCheck`.
    pub fn health_check(&self, now: Instant, resync_interval: Duration) -> Result<(), String> {
        let duration = (resync_interval * 2).max(MIN_SYNC_LOOP_STALENESS);
        match self.latest_loop_entry_time() {
            Some(enter) if now > enter + duration => {
                Err("sync Loop took longer than expected".to_string())
            }
            _ => Ok(()),
        }
    }
}
