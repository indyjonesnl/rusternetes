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
//! * Bookmarks (`cacher.go` `dispatchEvents` bookmark timer,
//!   `startDispatchingBookmarkEventsLocked`, `cache_watcher.go`
//!   `nextBookmarkTime` / `bookmarkAfterResourceVersion` state): a watcher that
//!   allows bookmarks is queued in [`TimeBucketWatchers`] and is offered a
//!   bookmark when its bucket expires, roughly every minute and shortly before
//!   its deadline.
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
//! * Bookmark resource versions come from the last event the shared watch
//!   delivered (`lastProcessedResourceVersion`); there are no storage progress
//!   notifications to advance it on a quiet prefix.
//! * `cacheWatcher.process`'s `result` channel is [`process`]'s output queue,
//!   also `chanSize` deep.

use crate::watch_cache::{CachedWatchEvent, WatchEventData, EVENT_FRESH_DURATION};
use crate::watcher_bookmark::{BookmarkSchedule, BookmarkState, NextBookmark, TimeBucketWatchers};
use rusternetes_storage::WatchEvent;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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

/// An item on a watcher's input queue (`watchCacheEvent`, whose `Type` may be
/// `Bookmark`).
#[derive(Debug, Clone)]
pub enum CacheItem {
    Event(CachedWatchEvent),
    Bookmark { revision: i64 },
}

/// What the consumer of a watch receives (`watch.Event`).
#[derive(Debug, Clone)]
pub enum WatchItem {
    Event(WatchEvent),
    /// `initial_events_end` is the `k8s.io/initial-events-end` annotation
    /// (`storage.AnnotateInitialEventsEndBookmark`): set on the first bookmark
    /// after the initial events of a watch-list request.
    Bookmark {
        revision: i64,
        initial_events_end: bool,
    },
}

/// How a watcher is created (`newCacheWatcher` arguments that matter here).
#[derive(Debug, Clone, Default)]
pub struct WatcherOptions {
    /// `pred.AllowWatchBookmarks`.
    pub allow_watch_bookmarks: bool,
    /// The watch's deadline, so a bookmark can be sent just before it.
    pub deadline: Option<Instant>,
    /// `bookmarkAfterResourceVersion`: 0 means no special preference.
    pub bookmark_after_resource_version: i64,
}

/// State shared by the dispatcher-side and the consumer-side halves of a
/// `cacheWatcher`.
pub struct WatcherState {
    stopped: AtomicBool,
    deadline: Option<Instant>,
    bookmark_after_rv: AtomicI64,
    /// `cacheWatcher.state` under `stateMutex`.
    state: Mutex<BookmarkState>,
}

impl WatcherState {
    fn new(opts: &WatcherOptions) -> Self {
        let s = Self {
            stopped: AtomicBool::new(false),
            deadline: opts.deadline,
            bookmark_after_rv: AtomicI64::new(0),
            state: Mutex::new(BookmarkState::BookmarkSent),
        };
        s.set_bookmark_after_resource_version(opts.bookmark_after_resource_version);
        s
    }

    fn state(&self) -> std::sync::MutexGuard<'_, BookmarkState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `setBookmarkAfterResourceVersion`.
    pub fn set_bookmark_after_resource_version(&self, rv: i64) {
        *self.state() = if rv == 0 {
            BookmarkState::BookmarkSent // no specific RV requested: a no-op
        } else {
            BookmarkState::WaitingForBookmark
        };
        self.bookmark_after_rv.store(rv, Ordering::Release);
    }

    /// `wasBookmarkAfterRvReceivedLocked`.
    pub fn was_bookmark_after_rv_received(&self) -> bool {
        *self.state() != BookmarkState::WaitingForBookmark
    }

    /// `wasBookmarkAfterRvSent`.
    pub fn was_bookmark_after_rv_sent(&self) -> bool {
        *self.state() == BookmarkState::BookmarkSent
    }

    /// `markBookmarkAfterRvAsReceived`: a bookmark reached the input queue.
    pub fn mark_bookmark_after_rv_as_received(&self) {
        let mut st = self.state();
        if *st == BookmarkState::WaitingForBookmark {
            *st = BookmarkState::BookmarkReceived;
        }
    }

    /// `markBookmarkAfterRvSent`: a bookmark was sent to the client.
    pub fn mark_bookmark_after_rv_sent(&self) {
        *self.state() = BookmarkState::BookmarkSent;
    }

    /// `cacheWatcher.nextBookmarkTime` (`cache_watcher.go:244-285`).
    pub fn next_bookmark_time(&self, now: Instant, bookmark_frequency: Duration) -> NextBookmark {
        // (c) the bookmarkAfterResourceVersion was not confirmed yet.
        if !self.was_bookmark_after_rv_received() {
            return NextBookmark::Immediately;
        }
        let heartbeat = now + bookmark_frequency;
        let Some(deadline) = self.deadline else {
            return NextBookmark::At(heartbeat);
        };
        // (a) right before the deadline, (b) roughly every minute.
        let pretimeout = deadline.checked_sub(Duration::from_secs(2));
        let at = match pretimeout {
            Some(p) if p < heartbeat => p,
            _ => heartbeat,
        };
        if at < now {
            return NextBookmark::Never;
        }
        NextBookmark::At(at)
    }

    pub fn is_terminated(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

/// The dispatcher-side half of a `cacheWatcher`.
pub struct CacheWatcher {
    input: mpsc::Sender<CacheItem>,
    shared: Arc<WatcherState>,
}

impl BookmarkSchedule for CacheWatcher {
    fn next_bookmark_time(&self, now: Instant, frequency: Duration) -> NextBookmark {
        self.shared.next_bookmark_time(now, frequency)
    }
}

impl CacheWatcher {
    /// `newCacheWatcher`: the dispatcher half and the consumer half.
    pub fn new(chan_size: usize, opts: &WatcherOptions) -> (Arc<Self>, WatchReceiver) {
        let (tx, rx) = mpsc::channel(chan_size);
        let shared = Arc::new(WatcherState::new(opts));
        (
            Arc::new(Self {
                input: tx,
                shared: shared.clone(),
            }),
            WatchReceiver {
                rx,
                shared,
                chan_size,
            },
        )
    }

    /// The shared bookmark state.
    pub fn state(&self) -> &Arc<WatcherState> {
        &self.shared
    }

    /// `cacheWatcher.nonblockingAdd`.
    fn nonblocking_add(&self, item: &CacheItem) -> bool {
        if let CacheItem::Bookmark { revision } = item {
            // Discard a bookmark older than bookmarkAfterResourceVersion so the
            // input queue is not polluted.
            if *revision < self.shared.bookmark_after_rv.load(Ordering::Acquire) {
                return false;
            }
        }
        if self.input.try_send(item.clone()).is_ok() {
            if matches!(item, CacheItem::Bookmark { .. }) {
                self.shared.mark_bookmark_after_rv_as_received();
            }
            true
        } else {
            false
        }
    }

    /// The watcher can no longer receive: its consumer went away or it was
    /// terminated.
    fn is_gone(&self) -> bool {
        self.input.is_closed() || self.shared.is_terminated()
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
        self.shared.stopped.store(true, Ordering::Release);
    }
}

/// The consumer-side half of a `cacheWatcher`: its `input` channel.
pub struct WatchReceiver {
    rx: mpsc::Receiver<CacheItem>,
    shared: Arc<WatcherState>,
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
    /// Next item, or `None` once the watcher was stopped (terminated as too
    /// slow, or the cache shut down). Items still queued at that point are
    /// discarded, as with `drainInputBuffer == false`.
    pub async fn recv_item(&mut self) -> Option<CacheItem> {
        if self.shared.is_terminated() {
            return None;
        }
        let item = self.rx.recv().await?;
        if self.shared.is_terminated() {
            return None;
        }
        Some(item)
    }

    /// Next event, skipping bookmarks.
    pub async fn recv(&mut self) -> Option<CachedWatchEvent> {
        loop {
            match self.recv_item().await? {
                CacheItem::Event(e) => return Some(e),
                CacheItem::Bookmark { .. } => continue,
            }
        }
    }

    /// Whether the dispatcher terminated this watcher.
    pub fn is_terminated(&self) -> bool {
        self.shared.is_terminated()
    }

    /// `chanSize` this watcher was created with.
    pub fn chan_size(&self) -> usize {
        self.chan_size
    }

    /// The shared watcher state (bookmark bookkeeping).
    pub fn state(&self) -> &Arc<WatcherState> {
        &self.shared
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
    /// `c.bookmarkWatchers`.
    bookmark_watchers: Mutex<TimeBucketWatchers<CacheWatcher>>,
    /// `lastProcessedResourceVersion` (`cacher.go:dispatchEvents`): the
    /// highest revision this prefix's event stream has delivered, the
    /// revision a bookmark carries.
    last_processed: AtomicI64,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            watchers: Mutex::new(WatcherSet::default()),
            budget: Mutex::new(TimeBudget::new(now)),
            group_resource: std::sync::OnceLock::new(),
            trigger_state: Mutex::new(std::collections::HashMap::new()),
            bookmark_watchers: Mutex::new(TimeBucketWatchers::new(
                now,
                crate::watcher_bookmark::DEFAULT_BOOKMARK_FREQUENCY,
            )),
            last_processed: AtomicI64::new(0),
        }
    }

    /// Seed `lastProcessedResourceVersion`: the ring is complete up to here,
    /// so a bookmark at this revision is safe.
    pub fn set_last_processed(&self, revision: i64) {
        self.last_processed.fetch_max(revision, Ordering::AcqRel);
    }

    /// `lastProcessedResourceVersion`.
    pub fn last_processed(&self) -> i64 {
        self.last_processed.load(Ordering::Acquire)
    }

    /// The bookmark timer of `Cacher.dispatchEvents` (`cacher.go:858-899`):
    /// when the clock reaches a bucket, offer a bookmark at the last processed
    /// revision to its watchers. Nothing is sent before any revision is known.
    pub fn bookmark_tick(&self, now: Instant) {
        let revision = self.last_processed();
        if revision > 0 {
            self.dispatch_bookmark(now, revision);
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

    fn buckets(&self) -> std::sync::MutexGuard<'_, TimeBucketWatchers<CacheWatcher>> {
        self.bookmark_watchers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
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
        self.register(
            capacity,
            scope.name.clone(),
            trigger_value,
            index_exists,
            scope.options.clone(),
        )
    }

    /// [`Self::add_watcher`] for a watcher scoped to one object `name`.
    pub fn add_scoped_watcher(&self, capacity: usize, name: Option<String>) -> WatchReceiver {
        self.register(capacity, name, None, false, WatcherOptions::default())
    }

    fn register(
        &self,
        capacity: usize,
        name: Option<String>,
        trigger_value: Option<String>,
        index_exists: bool,
        opts: WatcherOptions,
    ) -> WatchReceiver {
        let chan_size = suggested_watch_channel_size(
            capacity,
            EVENT_FRESH_DURATION,
            index_exists,
            trigger_value.is_some(),
        );
        let (watcher, rx) = CacheWatcher::new(chan_size, &opts);
        {
            let mut set = self.list();
            // `watchersSet.addWatcher`: a trigger watcher lives under its value
            // only, any other under its name scope.
            match (trigger_value, name) {
                (Some(v), _) => set.by_value.entry(v).or_default().push(watcher.clone()),
                (None, Some(n)) => set.by_name.entry(n).or_default().push(watcher.clone()),
                (None, None) => set.unscoped.push(watcher.clone()),
            }
        }
        // "Add it to the queue only when the client supports watch bookmarks."
        if opts.allow_watch_bookmarks {
            self.buckets().add_watcher(watcher, Instant::now());
        }
        rx
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
            w.shared.stopped.store(true, Ordering::Release);
        }
    }

    /// `Cacher.dispatchEvent` for a bookmark (`cacher.go:952-964`,
    /// `startDispatchingBookmarkEventsLocked`, `finishDispatching`): offer a
    /// bookmark at `revision` to the watchers whose bucket expired, without
    /// blocking, and queue them again for the next one.
    pub fn dispatch_bookmark(&self, now: Instant, revision: i64) {
        // Pop already expired watchers; stopped ones are ignored (they are not
        // removed from the buckets when they stop).
        let expired: Vec<Arc<CacheWatcher>> = self
            .buckets()
            .pop_expired_watchers(now)
            .into_iter()
            .flatten()
            .filter(|w| !w.is_gone())
            .collect();
        let item = CacheItem::Bookmark { revision };
        for w in &expired {
            w.nonblocking_add(&item);
        }
        // finishDispatching: requeue the watchers that are still running.
        let mut buckets = self.buckets();
        for w in expired {
            if !w.is_gone() {
                buckets.add_watcher(w, now);
            }
        }
    }

    /// `Cacher.dispatchEvent` for a non-bookmark event.
    pub async fn dispatch(&self, prefix: &str, event: &CachedWatchEvent) {
        if event.revision > 0 {
            self.set_last_processed(event.revision);
        }
        let item = CacheItem::Event(event.clone());
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
            .filter(|w| !w.nonblocking_add(&item))
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
            if w.nonblocking_add(&item) {
                continue;
            }
            if !fired {
                match tokio::time::timeout_at(deadline, w.input.send(item.clone())).await {
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

/// `cacheWatcher.processInterval` + `process` (`cache_watcher.go`): replay
/// `history`, send the initial-events-end bookmark when one is due
/// (`cacheInterval.initialEventsEndBookmark`), then forward live items newer
/// than `since_revision` (raised past everything replayed) into a
/// `chan_size`-deep result queue that the HTTP stream drains. Ends when the
/// watcher is terminated or the consumer goes away.
///
/// A live event is skipped only when it carries a real revision (> 0): the
/// in-memory backend stamps none, which would otherwise read as already seen.
pub fn process(
    history: Vec<CachedWatchEvent>,
    initial_events_end: Option<i64>,
    rx: WatchReceiver,
    since_revision: i64,
) -> mpsc::Receiver<WatchItem> {
    process_with(
        history,
        initial_events_end.map(|revision| FirstBookmark {
            revision,
            initial_events_end: true,
        }),
        rx,
        since_revision,
    )
}

/// A bookmark sent right after the replayed events.
#[derive(Debug, Clone, Copy)]
pub struct FirstBookmark {
    pub revision: i64,
    /// Carries the `k8s.io/initial-events-end` annotation (the watch-list
    /// terminator). Without it the bookmark is a plain first frame.
    pub initial_events_end: bool,
}

/// [`process`] with the first bookmark spelled out.
pub fn process_with(
    history: Vec<CachedWatchEvent>,
    first_bookmark: Option<FirstBookmark>,
    mut rx: WatchReceiver,
    since_revision: i64,
) -> mpsc::Receiver<WatchItem> {
    let max_seen = history
        .iter()
        .map(|e| e.revision)
        .max()
        .unwrap_or(0)
        .max(since_revision);
    let shared = rx.shared.clone();
    let (tx, result) = mpsc::channel(rx.chan_size());
    tokio::spawn(async move {
        for cached in history {
            if tx
                .send(WatchItem::Event(to_watch_event(cached)))
                .await
                .is_err()
            {
                return;
            }
        }
        if let Some(first) = first_bookmark {
            // `convertToWatchEvent` annotates a bookmark sent while
            // `!wasBookmarkAfterRvSent`; this one always is the first.
            if tx
                .send(WatchItem::Bookmark {
                    revision: first.revision,
                    initial_events_end: first.initial_events_end,
                })
                .await
                .is_err()
            {
                return;
            }
            shared.mark_bookmark_after_rv_sent();
        }
        loop {
            let item = tokio::select! {
                _ = tx.closed() => return,
                item = rx.recv_item() => match item {
                    Some(item) => item,
                    None => return,
                },
            };
            match item {
                CacheItem::Event(cached) => {
                    if cached.revision > 0 && cached.revision <= max_seen {
                        continue;
                    }
                    if tx
                        .send(WatchItem::Event(to_watch_event(cached)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                CacheItem::Bookmark { revision } => {
                    // Only a bookmark newer than what was sent, or the one
                    // at exactly that version when none was sent yet.
                    if revision > max_seen
                        || (revision == max_seen && !shared.was_bookmark_after_rv_sent())
                    {
                        let initial_events_end = !shared.was_bookmark_after_rv_sent();
                        if tx
                            .send(WatchItem::Bookmark {
                                revision,
                                initial_events_end,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                        shared.mark_bookmark_after_rv_sent();
                    }
                }
            }
        }
    });
    result
}
