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
//! * Trigger-value indexing (`cacher.go:835-850` `triggerValuesThreadUnsafe`,
//!   `watchersSet.valueWatchers`): for a resource with a `TriggerFunc`
//!   ([`trigger_field`]: pods' `spec.nodeName`,
//!   `pkg/registry/core/pod/storage/storage.go:96`), a watcher whose field
//!   selector pins the trigger (`pred.MatcherIndex`) is registered under that
//!   value only and is offered an event when the object's current OR previous
//!   trigger value matches, so an object leaving the value still reaches the
//!   watcher as the transition it must turn into a DELETED.
//!   `suggestedWatchChannelSize` gets `indexExists`/`triggerUsed` accordingly.
//!
//! Deviations (Rusternetes-specific):
//! * The namespace dimension of `namespacedName` is the storage prefix itself
//!   (a namespaced watch has its own prefix).
//! * Upstream's `watchCache` keeps every object (`w.store`), and
//!   `processEvent` reads the previous one from it (`PrevObject`). That is the
//!   whole resource in memory, which this project avoids: while a prefix has
//!   trigger watchers we keep only `key -> last trigger value`. An update for
//!   a key not yet seen has an UNKNOWN previous value and, as upstream does
//!   when the trigger is unsupported (`startDispatching`'s paranoid branch),
//!   is offered to every trigger watcher. The map is dropped when the last
//!   trigger watcher goes away, so it can never go stale.
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

/// `TriggerFunc` registrations of the registries
/// (`pkg/registry/core/pod/storage/storage.go:96`): the field whose value is
/// the dispatch index of a resource. Only pods have one that matters here.
pub fn trigger_field(group: &str, resource: &str) -> Option<&'static str> {
    (group.is_empty() && resource == "pods").then_some("spec.nodeName")
}

/// `NodeNameTriggerFunc` generalised: the string at a dotted field path of a
/// stored JSON object (`""` when absent, as for an unscheduled pod).
fn trigger_value_of(value: &str, field: &str) -> String {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(value) else {
        return String::new();
    };
    let mut cur = &v;
    for part in field.split('.') {
        match cur.get(part) {
            Some(next) => cur = next,
            None => return String::new(),
        }
    }
    cur.as_str().unwrap_or_default().to_string()
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
/// (`allWatchers` keyed by `namespacedName`, `valueWatchers` keyed by trigger
/// value): unscoped watchers, watchers scoped to one object name, and watchers
/// pinned to one trigger value.
#[derive(Default)]
struct WatcherSet {
    unscoped: Vec<Arc<CacheWatcher>>,
    by_name: std::collections::HashMap<String, Vec<Arc<CacheWatcher>>>,
    by_value: std::collections::HashMap<String, Vec<Arc<CacheWatcher>>>,
}

impl WatcherSet {
    fn retain_live(&mut self) {
        self.unscoped.retain(|w| !w.is_gone());
        for map in [&mut self.by_name, &mut self.by_value] {
            map.retain(|_, ws| {
                ws.retain(|w| !w.is_gone());
                !ws.is_empty()
            });
        }
    }

    fn count(&self) -> usize {
        self.unscoped.iter().filter(|w| !w.is_gone()).count()
            + self
                .by_name
                .values()
                .chain(self.by_value.values())
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
    /// `key -> last trigger value` while the prefix has trigger watchers (see
    /// the module docs: the previous object upstream reads from `w.store`).
    trigger_state: Mutex<std::collections::HashMap<String, String>>,
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
            trigger_state: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// `c.indexedTrigger`: the trigger field of this prefix's resource.
    fn trigger(&self) -> Option<&'static str> {
        let (g, r) = self.group_resource.get()?;
        trigger_field(g, r)
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
        // `cacher.go:553-569`: `indexExists` is whether the resource has a
        // trigger, `triggerUsed` whether this watch pins it.
        let index_exists = self.trigger().is_some();
        let trigger_value = scope.trigger_value.clone().filter(|_| index_exists);
        self.register(capacity, scope.name.clone(), trigger_value, index_exists)
    }

    /// [`Self::add_watcher`] for a watcher scoped to one object `name`.
    pub fn add_scoped_watcher(&self, capacity: usize, name: Option<String>) -> WatchReceiver {
        self.register(capacity, name, None, false)
    }

    fn register(
        &self,
        capacity: usize,
        name: Option<String>,
        trigger_value: Option<String>,
        index_exists: bool,
    ) -> WatchReceiver {
        let chan_size = suggested_watch_channel_size(
            capacity,
            EVENT_FRESH_DURATION,
            index_exists,
            trigger_value.is_some(),
        );
        let (tx, rx) = mpsc::channel(chan_size);
        let stopped = Arc::new(AtomicBool::new(false));
        let watcher = Arc::new(CacheWatcher {
            input: tx,
            stopped: stopped.clone(),
        });
        {
            let mut set = self.list();
            // `watchersSet.addWatcher`: a trigger watcher lives under its value
            // only, any other under its name scope.
            match (trigger_value, name) {
                (Some(v), _) => set.by_value.entry(v).or_default().push(watcher),
                (None, Some(n)) => set.by_name.entry(n).or_default().push(watcher),
                (None, None) => set.unscoped.push(watcher),
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
        let (unscoped, by_name, by_value) = {
            let mut set = self.list();
            (
                std::mem::take(&mut set.unscoped),
                std::mem::take(&mut set.by_name),
                std::mem::take(&mut set.by_value),
            )
        };
        for w in unscoped
            .into_iter()
            .chain(by_name.into_values().flatten())
            .chain(by_value.into_values().flatten())
        {
            w.stopped.store(true, Ordering::Release);
        }
    }

    /// `Cacher.dispatchEvent` for a non-bookmark event.
    pub async fn dispatch(&self, prefix: &str, event: &CachedWatchEvent) {
        // startDispatching: the watchers interested in this event; forget the
        // ones that are already gone.
        let (is_delete, key, value) = match &event.event {
            WatchEventData::Added(k, v) => (false, k, v),
            WatchEventData::Modified(k, v) => (false, k, v),
            WatchEventData::Deleted(k, v) => (true, k, v),
        };
        let was_added = matches!(event.event, WatchEventData::Added(..));
        // `triggerValuesThreadUnsafe`, only when some watcher is indexed by value.
        let trigger = self.trigger().filter(|_| !self.list().by_value.is_empty());
        let values = trigger.map(|field| {
            let cur = trigger_value_of(value, field);
            let mut state = self.trigger_state.lock().unwrap_or_else(|e| e.into_inner());
            let prev = if is_delete {
                state.remove(key)
            } else {
                state.insert(key.clone(), cur.clone())
            };
            match prev {
                // No previous object (a create), or an unchanged value.
                _ if was_added => Some(vec![cur]),
                _ if is_delete => Some(vec![cur]),
                Some(p) if p == cur => Some(vec![cur]),
                Some(p) => Some(vec![cur, p]),
                // Previous value unknown: offer to every value watcher.
                None => None,
            }
        });
        if trigger.is_none() {
            self.trigger_state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
        }
        let targets: Vec<Arc<CacheWatcher>> = {
            let mut set = self.list();
            set.retain_live();
            let name = key_name(key);
            let mut t = set.unscoped.clone();
            if let Some(ws) = set.by_name.get(name) {
                t.extend(ws.iter().cloned());
            }
            match values {
                Some(Some(vals)) => {
                    for v in vals {
                        if let Some(ws) = set.by_value.get(&v) {
                            t.extend(ws.iter().cloned());
                        }
                    }
                }
                // Unsupported trigger or unknown previous value.
                _ => t.extend(set.by_value.values().flatten().cloned()),
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
