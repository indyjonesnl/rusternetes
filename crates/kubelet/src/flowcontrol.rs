//! Port of `flowcontrol.Backoff`
//! (`staging/src/k8s.io/client-go/util/flowcontrol/backoff.go`): a per-key
//! exponential back-off with expiry. Entries start at the initial delay, double
//! on every `Next` up to the maximum, and are treated as stable (reset, and
//! collectable by `GC`) once `2 * max` has passed without an update.
//!
//! Deviations: no jitter (`NewBackOff` uses none, `maxJitterFactor == 0`) and no
//! pluggable `HasExpiredFunc`; the clock is explicit: every method that reads
//! the time takes `now`, which callers pass where upstream reads
//! `Clock.Now()` (for `Next` upstream uses the caller's `eventTime` only for the
//! expiry test and `Clock.Now()` for `lastUpdate`; callers always pass the same
//! instant, as `image_manager.go` does).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
struct Entry {
    backoff: Duration,
    last_update: Instant,
}

/// `flowcontrol.Backoff`.
#[derive(Debug)]
pub struct Backoff {
    default_duration: Duration,
    max_duration: Duration,
    per_item: Mutex<HashMap<String, Entry>>,
}

impl Backoff {
    /// `NewBackOff(initial, max)`.
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            default_duration: initial,
            max_duration: max,
            per_item: Mutex::new(HashMap::new()),
        }
    }

    fn items(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.per_item.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `hasExpired`: stable for twice the maximum.
    fn has_expired(&self, event_time: Instant, last_update: Instant) -> bool {
        event_time.saturating_duration_since(last_update) > self.max_duration * 2
    }

    /// `Get`: the current back-off duration.
    pub fn get(&self, id: &str) -> Duration {
        self.items().get(id).map_or(Duration::ZERO, |e| e.backoff)
    }

    /// `Next`: move the back-off to the next mark, capping at the maximum.
    pub fn next(&self, id: &str, now: Instant) {
        let mut items = self.items();
        let entry = match items.get(id) {
            Some(e) if !self.has_expired(now, e.last_update) => Entry {
                backoff: (e.backoff * 2).min(self.max_duration),
                last_update: now,
            },
            _ => Entry {
                backoff: self.default_duration,
                last_update: now,
            },
        };
        items.insert(id.to_string(), entry);
    }

    /// `Reset` / `DeleteEntry`: forget the key.
    pub fn reset(&self, id: &str) {
        self.items().remove(id);
    }

    /// `IsInBackOffSince`: the time since `event_time` is smaller than the
    /// current window.
    pub fn is_in_back_off_since(&self, id: &str, event_time: Instant, now: Instant) -> bool {
        match self.items().get(id) {
            Some(e) if !self.has_expired(event_time, e.last_update) => {
                now.saturating_duration_since(event_time) < e.backoff
            }
            _ => false,
        }
    }

    /// `IsInBackOffSinceUpdate`: the time since the last update is smaller than
    /// the current window.
    pub fn is_in_back_off_since_update(&self, id: &str, event_time: Instant) -> bool {
        match self.items().get(id) {
            Some(e) if !self.has_expired(event_time, e.last_update) => {
                event_time.saturating_duration_since(e.last_update) < e.backoff
            }
            _ => false,
        }
    }

    /// `GC`: drop records past their expiration (`2 * max`).
    pub fn gc(&self, now: Instant) {
        self.items()
            .retain(|_, e| !self.has_expired(now, e.last_update));
    }

    /// Number of tracked keys (tests).
    pub fn len(&self) -> usize {
        self.items().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
