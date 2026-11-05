//! Per-watcher input queue and event dispatch of the apiserver watch cache.
//!
//! Ports, from `staging/src/k8s.io/apiserver/pkg/storage/cacher` (release-1.35):
//!
//! * `suggestedWatchChannelSize` (`watch_cache.go:831-863`): each watcher's own
//!   input channel is sized `ceil(capacity / eventFreshDuration.Seconds())`,
//!   clamped to `[10, max]`.
//! * `cacheWatcher.input` / `nonblockingAdd` / `add` (`cache_watcher.go`): a
//!   bounded per-watcher queue. A full queue does not block the others.
//! * `Cacher.dispatchEvent` (`cacher.go:952-1019`): offer the event to every
//!   watcher without blocking first; the watchers whose queue was full then
//!   share one timer whose length comes from `dispatchTimeoutBudget`
//!   (`time_budget.go`). Once it fires, a watcher that still cannot take the
//!   event is TERMINATED (`forget(false)`): its stream ends and the client
//!   re-watches from the last resourceVersion it received.
//!
//! Deviations (Rusternetes-specific):
//! * Watchers are not indexed by namespace/name/trigger: our cache is per
//!   storage prefix and every watcher of the prefix is offered every event;
//!   selector filtering happens in the watch handler. `suggestedWatchChannelSize`
//!   is therefore always called with `indexExists = false` (cap 100).
//! * The cacher's bookmark time buckets (`watcherBookmarkTimeBuckets`) are not
//!   ported here; watch bookmarks are produced by the watch handler.
//! * `cacheWatcher.process`'s `result` channel is [`process`]'s output queue,
//!   also `chanSize` deep.

use crate::watch_cache::{CachedWatchEvent, WatchEventData, EVENT_FRESH_DURATION};
use rusternetes_storage::WatchEvent;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// `minWatchChanSize` (`watch_cache.go`).
pub const MIN_WATCH_CHAN_SIZE: usize = 10;
/// `maxWatchChanSizeWithIndexAndTrigger`.
pub const MAX_WATCH_CHAN_SIZE_WITH_INDEX_AND_TRIGGER: usize = 10;
/// `maxWatchChanSizeWithIndexWithoutTrigger`.
pub const MAX_WATCH_CHAN_SIZE_WITH_INDEX_WITHOUT_TRIGGER: usize = 1000;
/// `maxWatchChanSizeWithoutIndex`.
pub const MAX_WATCH_CHAN_SIZE_WITHOUT_INDEX: usize = 100;

/// `watchCache.suggestedWatchChannelSize`: roughly one second of history.
pub fn suggested_watch_channel_size(
    current_capacity: usize,
    event_fresh_duration: Duration,
    index_exists: bool,
    trigger_used: bool,
) -> usize {
    let size = (current_capacity as f64 / event_fresh_duration.as_secs_f64()).ceil() as usize;
    let max = match (index_exists, trigger_used) {
        (true, true) => MAX_WATCH_CHAN_SIZE_WITH_INDEX_AND_TRIGGER,
        (true, false) => MAX_WATCH_CHAN_SIZE_WITH_INDEX_WITHOUT_TRIGGER,
        (false, _) => MAX_WATCH_CHAN_SIZE_WITHOUT_INDEX,
    };
    size.max(MIN_WATCH_CHAN_SIZE).min(max)
}

/// The size [`Dispatcher::add_watcher`] uses for a prefix whose ring currently
/// holds `capacity` events (`cacher.go:569`, no index).
pub fn watch_channel_size(capacity: usize) -> usize {
    suggested_watch_channel_size(capacity, EVENT_FRESH_DURATION, false, false)
}

/// `refreshPerSecond` (`time_budget.go`).
const REFRESH_PER_SECOND: Duration = Duration::from_millis(50);
/// `maxBudget`.
const MAX_BUDGET: Duration = Duration::from_millis(100);

/// `timeBudgetImpl`: a budget of blocking time that is refreshed over time.
#[derive(Debug)]
pub struct TimeBudget {
    budget: Duration,
    max_budget: Duration,
    refresh: Duration,
    last: Instant,
}

impl Default for TimeBudget {
    fn default() -> Self {
        Self::new(Instant::now())
    }
}

impl TimeBudget {
    /// `newTimeBudget`.
    pub fn new(now: Instant) -> Self {
        Self::with_limits(now, MAX_BUDGET, REFRESH_PER_SECOND)
    }

    /// A budget with explicit `maxBudget` / `refresh` (the struct literal of
    /// `TestTimeBudget`).
    pub fn with_limits(now: Instant, max_budget: Duration, refresh: Duration) -> Self {
        Self {
            budget: Duration::ZERO,
            max_budget,
            refresh,
            last: now,
        }
    }

    /// `takeAvailable`: the budget accumulated since the last access (capped
    /// at the maximum). The whole budget is taken.
    pub fn take_available(&mut self, now: Instant) -> Duration {
        let acc =
            now.saturating_duration_since(self.last).as_secs_f64() * self.refresh.as_secs_f64();
        self.budget = (self.budget + Duration::from_secs_f64(acc)).min(self.max_budget);
        self.last = now;
        std::mem::take(&mut self.budget)
    }

    /// `returnUnused`: give back what was not spent.
    pub fn return_unused(&mut self, unused: Duration) {
        self.budget = (self.budget + unused).min(self.max_budget);
    }
}

/// The dispatcher-side half of a `cacheWatcher`.
pub struct CacheWatcher {
    input: mpsc::Sender<CachedWatchEvent>,
    stopped: Arc<AtomicBool>,
}

impl CacheWatcher {
    /// `cacheWatcher.nonblockingAdd`.
    fn nonblocking_add(&self, event: &CachedWatchEvent) -> bool {
        self.input.try_send(event.clone()).is_ok()
    }

    /// The watcher can no longer receive: its consumer went away or it was
    /// terminated.
    fn is_gone(&self) -> bool {
        self.input.is_closed() || self.stopped.load(Ordering::Acquire)
    }

    /// `cacheWatcher.forget(false)` from `add`'s `closeFunc`: stop without
    /// draining the input buffer.
    fn terminate(&self, prefix: &str) {
        tracing::info!(
            "Forcing watcher of {prefix} to close due to unresponsiveness (queue {} of {} events)",
            self.input.max_capacity() - self.input.capacity(),
            self.input.max_capacity()
        );
        self.stopped.store(true, Ordering::Release);
    }
}

/// The consumer-side half of a `cacheWatcher`: its `input` channel.
pub struct WatchReceiver {
    rx: mpsc::Receiver<CachedWatchEvent>,
    stopped: Arc<AtomicBool>,
    chan_size: usize,
}

impl std::fmt::Debug for WatchReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchReceiver")
            .field("chan_size", &self.chan_size)
            .field("stopped", &self.is_terminated())
            .finish()
    }
}

impl WatchReceiver {
    /// Next event, or `None` once the watcher was stopped (terminated as too
    /// slow, or the cache shut down). Events still queued at that point are
    /// discarded, as with `drainInputBuffer == false`.
    pub async fn recv(&mut self) -> Option<CachedWatchEvent> {
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }
        let ev = self.rx.recv().await?;
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }
        Some(ev)
    }

    /// Whether the dispatcher terminated this watcher.
    pub fn is_terminated(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// `chanSize` this watcher was created with.
    pub fn chan_size(&self) -> usize {
        self.chan_size
    }
}

/// A prefix's watcher set and `dispatchEvent` (`cacher.go:952`).
pub struct Dispatcher {
    watchers: Mutex<Vec<Arc<CacheWatcher>>>,
    budget: Mutex<TimeBudget>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(Vec::new()),
            budget: Mutex::new(TimeBudget::default()),
        }
    }

    fn list(&self) -> std::sync::MutexGuard<'_, Vec<Arc<CacheWatcher>>> {
        self.watchers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `newCacheWatcher` + registration (`cacher.go:569-663`), with the input
    /// queue sized for a ring of `capacity` events.
    pub fn add_watcher(&self, capacity: usize) -> WatchReceiver {
        let chan_size = watch_channel_size(capacity);
        let (tx, rx) = mpsc::channel(chan_size);
        let stopped = Arc::new(AtomicBool::new(false));
        self.list().push(Arc::new(CacheWatcher {
            input: tx,
            stopped: stopped.clone(),
        }));
        WatchReceiver {
            rx,
            stopped,
            chan_size,
        }
    }

    /// Live watchers (the receiver count of the old shared channel).
    pub fn receiver_count(&self) -> usize {
        self.list().iter().filter(|w| !w.is_gone()).count()
    }

    /// Stop every watcher (`terminateAllWatchers`).
    pub fn terminate_all(&self) {
        for w in self.list().drain(..) {
            w.stopped.store(true, Ordering::Release);
        }
    }

    /// `Cacher.dispatchEvent` for a non-bookmark event.
    pub async fn dispatch(&self, prefix: &str, event: &CachedWatchEvent) {
        // startDispatching: the watchers interested in this event; forget the
        // ones that are already gone.
        let targets: Vec<Arc<CacheWatcher>> = {
            let mut list = self.list();
            list.retain(|w| !w.is_gone());
            list.clone()
        };
        // Non-blocking first, so faster watchers are not held up by slower ones.
        let blocked: Vec<&Arc<CacheWatcher>> = targets
            .iter()
            .filter(|w| !w.nonblocking_add(event))
            .collect();
        if blocked.is_empty() {
            return;
        }
        let start = Instant::now();
        let timeout = self
            .budget
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_available(start);
        let deadline = tokio::time::Instant::now() + timeout;
        // One timer for all blocked watchers: each gets the chance to unblock
        // while it runs; after it fires a still-full watcher is terminated.
        let mut fired = false;
        for w in blocked {
            // `add`: retry without blocking first.
            if w.nonblocking_add(event) {
                continue;
            }
            if !fired {
                match tokio::time::timeout_at(deadline, w.input.send(event.clone())).await {
                    Ok(_) => continue, // sent (or the consumer is gone)
                    Err(_) => fired = true,
                }
            }
            w.terminate(prefix);
        }
        self.budget
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .return_unused(timeout.saturating_sub(start.elapsed()));
        self.list().retain(|w| !w.is_gone());
    }
}

fn to_watch_event(cached: CachedWatchEvent) -> WatchEvent {
    match cached.event {
        WatchEventData::Added(key, value) => WatchEvent::Added(key, (*value).clone()),
        WatchEventData::Modified(key, value) => WatchEvent::Modified(key, (*value).clone()),
        WatchEventData::Deleted(key, prev) => WatchEvent::Deleted(key, (*prev).clone()),
    }
}

/// `cacheWatcher.process` (`cache_watcher.go`): replay `history`, then forward
/// live events newer than `since_revision` (raised past everything replayed)
/// into a `chan_size`-deep result queue that the HTTP stream drains. Ends when
/// the watcher is terminated or the consumer goes away.
///
/// A live event is skipped only when it carries a real revision (> 0): the
/// in-memory backend stamps none, which would otherwise read as already seen.
pub fn process(
    history: Vec<CachedWatchEvent>,
    mut rx: WatchReceiver,
    since_revision: i64,
) -> mpsc::Receiver<WatchEvent> {
    let max_seen = history
        .iter()
        .map(|e| e.revision)
        .max()
        .unwrap_or(0)
        .max(since_revision);
    let (tx, result) = mpsc::channel(rx.chan_size());
    tokio::spawn(async move {
        for cached in history {
            if tx.send(to_watch_event(cached)).await.is_err() {
                return;
            }
        }
        loop {
            let cached = tokio::select! {
                _ = tx.closed() => return,
                ev = rx.recv() => match ev {
                    Some(ev) => ev,
                    None => return,
                },
            };
            if cached.revision > 0 && cached.revision <= max_seen {
                continue;
            }
            if tx.send(to_watch_event(cached)).await.is_err() {
                return;
            }
        }
    });
    result
}
