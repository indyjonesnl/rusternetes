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
//! * `watchersBuffer` selection (`cacher.go:1039-1112`, `startDispatching`): a
//!   watcher scoped to one name (`metadata.name=<n>` field selector,
//!   `namespacedName{name: n}`) is offered only events for that name, the
//!   others are offered every event. The terminated-watchers counter
//!   (`metrics.TerminatedWatchersCounter`) is bumped on every termination.
//!
//! Deviations (Rusternetes-specific):
//! * The namespace dimension of `namespacedName` is the storage prefix itself
//!   (a namespaced watch has its own prefix). Trigger-value indexing
//!   (`valueWatchers`, `TriggerFunc`) is NOT ported: it needs the previous
//!   object of an update (`triggerValuesThreadUnsafe` reads both `Object` and
//!   `PrevObject`) and our cached events carry only the new one, so a
//!   watcher whose object moved out of its trigger value would miss the
//!   DELETE. There is no trigger, so `suggestedWatchChannelSize` is called with
//!   `indexExists = false` (cap 100).
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
    fn terminate(&self, group: &str, resource: &str) {
        crate::cache_metrics::observe_terminated_watcher(group, resource);
        let prefix = resource;
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

/// The watchers of a prefix, indexed as upstream's `watchersSet`
/// (`allWatchers` keyed by `namespacedName`): unscoped watchers, and watchers
/// scoped to one object name.
#[derive(Default)]
struct WatcherSet {
    unscoped: Vec<Arc<CacheWatcher>>,
    by_name: std::collections::HashMap<String, Vec<Arc<CacheWatcher>>>,
}

impl WatcherSet {
    fn retain_live(&mut self) {
        self.unscoped.retain(|w| !w.is_gone());
        self.by_name.retain(|_, ws| {
            ws.retain(|w| !w.is_gone());
            !ws.is_empty()
        });
    }

    fn count(&self) -> usize {
        self.unscoped.iter().filter(|w| !w.is_gone()).count()
            + self
                .by_name
                .values()
                .flatten()
                .filter(|w| !w.is_gone())
                .count()
    }
}

/// The object name of a storage key (`/registry/<resource>[/<ns>]/<name>`).
fn key_name(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or_default()
}

/// A prefix's watcher set and `dispatchEvent` (`cacher.go:952`).
pub struct Dispatcher {
    watchers: Mutex<WatcherSet>,
    budget: Mutex<TimeBudget>,
    /// `(group, resource)` metric labels, set by the first scoped subscriber.
    group_resource: std::sync::OnceLock<(String, String)>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(WatcherSet::default()),
            budget: Mutex::new(TimeBudget::default()),
            group_resource: std::sync::OnceLock::new(),
        }
    }

    fn list(&self) -> std::sync::MutexGuard<'_, WatcherSet> {
        self.watchers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Label the terminated-watchers metric (first call wins).
    pub fn set_group_resource(&self, group: &str, resource: &str) {
        let _ = self
            .group_resource
            .set((group.to_string(), resource.to_string()));
    }

    /// `newCacheWatcher` + registration (`cacher.go:569-663`), with the input
    /// queue sized for a ring of `capacity` events.
    pub fn add_watcher(&self, capacity: usize) -> WatchReceiver {
        self.add_scoped_watcher(capacity, None)
    }

    /// Register a watcher for `scope` (labels the metric, indexes by name).
    pub fn add_scoped(
        &self,
        capacity: usize,
        scope: &crate::watch_cache::WatchScope,
    ) -> WatchReceiver {
        if !scope.resource.is_empty() {
            self.set_group_resource(&scope.group, &scope.resource);
        }
        self.add_scoped_watcher(capacity, scope.name.clone())
    }

    /// [`Self::add_watcher`] for a watcher scoped to one object `name`.
    pub fn add_scoped_watcher(&self, capacity: usize, name: Option<String>) -> WatchReceiver {
        let chan_size = watch_channel_size(capacity);
        let (tx, rx) = mpsc::channel(chan_size);
        let stopped = Arc::new(AtomicBool::new(false));
        let watcher = Arc::new(CacheWatcher {
            input: tx,
            stopped: stopped.clone(),
        });
        {
            let mut set = self.list();
            match name {
                Some(n) => set.by_name.entry(n).or_default().push(watcher),
                None => set.unscoped.push(watcher),
            }
        }
        WatchReceiver {
            rx,
            stopped,
            chan_size,
        }
    }

    /// Live watchers (the receiver count of the old shared channel).
    pub fn receiver_count(&self) -> usize {
        self.list().count()
    }

    /// Stop every watcher (`terminateAllWatchers`).
    pub fn terminate_all(&self) {
        let (unscoped, by_name) = {
            let mut set = self.list();
            (
                std::mem::take(&mut set.unscoped),
                std::mem::take(&mut set.by_name),
            )
        };
        for w in unscoped.into_iter().chain(by_name.into_values().flatten()) {
            w.stopped.store(true, Ordering::Release);
        }
    }

    /// `Cacher.dispatchEvent` for a non-bookmark event.
    pub async fn dispatch(&self, prefix: &str, event: &CachedWatchEvent) {
        // startDispatching: the watchers interested in this event; forget the
        // ones that are already gone.
        let targets: Vec<Arc<CacheWatcher>> = {
            let mut set = self.list();
            set.retain_live();
            let name = match &event.event {
                WatchEventData::Added(k, _)
                | WatchEventData::Modified(k, _)
                | WatchEventData::Deleted(k, _) => key_name(k),
            };
            let mut t = set.unscoped.clone();
            if let Some(ws) = set.by_name.get(name) {
                t.extend(ws.iter().cloned());
            }
            t
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
            let (group, resource) = match self.group_resource.get() {
                Some((g, r)) => (g.as_str(), r.as_str()),
                None => (
                    "",
                    prefix
                        .trim_start_matches("/registry/")
                        .split('/')
                        .next()
                        .unwrap_or_default(),
                ),
            };
            w.terminate(group, resource);
        }
        self.budget
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .return_unused(timeout.saturating_sub(start.elapsed()));
        self.list().retain_live();
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
