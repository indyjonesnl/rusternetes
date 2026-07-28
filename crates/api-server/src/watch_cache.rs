//! Watch Cache / Multiplexer
//!
//! Maintains a single etcd watch per resource prefix and fans out events
//! to all subscribed client watches. This avoids creating N etcd watches
//! for N clients, which overwhelms etcd and exhausts HTTP/2 stream limits.

use async_trait::async_trait;
use rusternetes_storage::StorageBackend;
use rusternetes_storage::{Storage, WatchEvent, WatchStream};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// The minimal storage surface the shared per-prefix watch loop needs.
///
/// Abstracted behind a trait for two reasons:
/// 1. It documents that the loop only ever *watches* — it never reads/writes.
/// 2. It lets the reconnect/replay behaviour be unit-tested with a mock
///    backend that can deterministically end a stream (simulating a dropped
///    connection to the storage server) and assert the gap is replayed.
///
/// Production always uses `StorageBackend` (the impl below).
#[async_trait]
pub(crate) trait WatchSource: Send + Sync + 'static {
    /// Start a watch from the *current* revision — tails future events only.
    /// Used for the very first connection, where the handler's initial LIST
    /// already covers pre-existing state.
    async fn watch_from_now(&self, prefix: &str) -> rusternetes_common::Result<WatchStream>;

    /// Start a watch that first *replays* every committed event since
    /// `revision`, then tails. Used on reconnect so events committed while the
    /// stream was down are not silently dropped (#cert-manager-cainjector).
    async fn watch_since(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream>;

    /// Current storage revision. Recorded as the ring "floor" when a shared
    /// prefix watch starts: the ring is complete for every revision > floor,
    /// so a client watch from an older resourceVersion must 410-relist rather
    /// than risk silently missing events.
    async fn head_revision(&self) -> i64;

    /// Whether `revision` is no longer available in the backend's history.
    /// A compacted resume point can never become valid again (compaction only
    /// moves forward), so the shared loop must re-base instead of retrying it
    /// (#1687). Defaults to "available" for test sources that don't compact.
    async fn is_revision_compacted(&self, _revision: i64) -> bool {
        false
    }
}

#[async_trait]
impl WatchSource for StorageBackend {
    async fn watch_from_now(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.watch_backend(prefix).await
    }

    async fn watch_since(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        Storage::watch_from_revision(self, prefix, revision).await
    }

    async fn head_revision(&self) -> i64 {
        self.current_revision().await.unwrap_or(0)
    }

    async fn is_revision_compacted(&self, revision: i64) -> bool {
        // A probe failure must not be read as "compacted" — that would throw
        // away replay history the backend still has.
        Storage::is_revision_compacted(self, revision)
            .await
            .unwrap_or(false)
    }
}

/// Maximum number of events to retain in the history ring buffer per prefix
/// while the prefix has at least one live external watcher.
/// K8s default watch cache capacity is 1000 events.
/// 5000 × 26 prefixes × ~3KB = ~390MB of memory.
const HISTORY_CAPACITY: usize = 500;

/// Replay-ring size retained for a prefix with **zero** live external watchers
/// (#1089). The full ring is only needed to replay history to reconnecting
/// watchers; at idle there are none, so we keep just a small recent tail and
/// free the rest. The tail still covers the short replay sequences the
/// remaining ring consumers need — notably the CRD watch path, whose
/// Established delivery replays just the ADDED+MODIFIED pair. The primary
/// resourceVersion replay path is storage-backed (`watch_from_revision` +
/// `is_revision_compacted` → 410), independent of this ring, so shrinking it
/// cannot regress replay correctness.
const HISTORY_IDLE_CAPACITY: usize = 16;

/// How often the idle-GC sweep reclaims replay rings for prefixes that have
/// dropped to zero watchers.
const IDLE_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Liveness bound for a shared prefix stream: if the backend delivered nothing
/// for this long WHILE the storage head revision advanced past our resume
/// point, the stream is presumed silently stalled (rhino/SQLite watches can
/// stall open under write bursts — no events, no error, no end). We tear it
/// down and reconnect via `watch_since(resume+1)`, whose replay recovers every
/// missed event. Bounded staleness instead of a silent forever-wedge (#1165).
const WATCH_LIVENESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// A cached watch event with metadata
#[derive(Debug, Clone)]
pub struct CachedWatchEvent {
    pub event: WatchEventData,
    pub revision: i64,
}

/// The event data (simplified from WatchEvent).
/// Uses Arc<String> for value JSON to avoid cloning large JSON strings
/// across multiple broadcast subscribers and the history buffer.
#[derive(Debug, Clone)]
pub enum WatchEventData {
    Added(String, Arc<String>),    // key, value JSON
    Modified(String, Arc<String>), // key, value JSON
    Deleted(String, Arc<String>),  // key, previous value JSON
}

/// WatchCache manages shared watch streams for resource prefixes.
/// Instead of one etcd watch per client, we have one per prefix.
pub struct WatchCache {
    /// Map of resource prefix → broadcast sender
    /// Each prefix has one etcd watch that broadcasts to all subscribers
    watchers: RwLock<HashMap<String, broadcast::Sender<CachedWatchEvent>>>,
    storage: Arc<dyn WatchSource>,
    /// Current revision counter (approximation based on timestamp)
    #[allow(dead_code)]
    revision: RwLock<i64>,
    /// Ring buffer of recent events per prefix for history replay
    history: Arc<RwLock<HashMap<String, VecDeque<CachedWatchEvent>>>>,
    /// Per-prefix replay floor: the ring is complete for revisions > floor
    /// (floor = storage head at shared-watch start, advanced whenever the ring
    /// trims). RV-watches below the floor must 410 so the client relists.
    floors: Arc<RwLock<HashMap<String, i64>>>,
}

impl WatchCache {
    pub fn new(storage: Arc<StorageBackend>) -> Self {
        // Coerce the concrete backend to the watch-only trait object.
        Self::from_source(storage)
    }

    /// Build a cache over any `WatchSource`. Lets unit tests inject a mock
    /// backend; production goes through `new` with a real `StorageBackend`.
    pub(crate) fn from_source(storage: Arc<dyn WatchSource>) -> Self {
        Self {
            watchers: RwLock::new(HashMap::new()),
            storage,
            revision: RwLock::new(0), // Will be populated from etcd events
            history: Arc::new(RwLock::new(HashMap::new())),
            floors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Subscribe to watch events for a resource prefix.
    /// Returns a broadcast receiver that will receive all events for this prefix.
    /// If no etcd watch exists for this prefix, one is started.
    pub async fn subscribe(&self, prefix: &str) -> broadcast::Receiver<CachedWatchEvent> {
        self.subscribe_with_floor(prefix, None).await
    }

    /// Like [`subscribe`], but when THIS call creates the shared watcher, start
    /// its replay from `desired_floor` instead of the current storage head.
    /// The first watcher of an on-demand prefix (e.g. a fresh namespace's
    /// resourcequotas) arrives with a resourceVersion from its own LIST — a
    /// GLOBAL revision that is typically older than the global head even
    /// though nothing for this prefix happened in between. Flooring at head
    /// would 410 that perfectly valid first watch; flooring at the client's RV
    /// (and replaying `watch_since(rv+1)` into the ring) serves it exactly.
    async fn subscribe_with_floor(
        &self,
        prefix: &str,
        desired_floor: Option<i64>,
    ) -> broadcast::Receiver<CachedWatchEvent> {
        // Check if we already have a watcher for this prefix
        {
            let watchers = self.watchers.read().await;
            if let Some(tx) = watchers.get(prefix) {
                return tx.subscribe();
            }
        }

        // Create a new watcher
        // Buffer size: K8s default watch cache is 1000 events. 16384 used ~1.2GB
        // of memory with 26 prefixes × 16K events × ~3KB each. 4096 keeps the
        // lag-termination path (see broadcast_to_stream) rare under conformance
        // churn while staying well under that ceiling.
        let (tx, rx) = broadcast::channel(4096);
        {
            let mut watchers = self.watchers.write().await;
            // Double-check after acquiring write lock
            if let Some(existing_tx) = watchers.get(prefix) {
                return existing_tx.subscribe();
            }
            watchers.insert(prefix.to_string(), tx.clone());
        }

        // Record the replay floor BEFORE the shared watch starts: the ring is
        // complete for revisions > floor. Starting the loop with
        // `watch_since(floor + 1)` (not "from now") closes the boot race where
        // an event committed between head_revision() and stream establishment
        // would be invisible to the ring forever.
        let start_floor = match desired_floor {
            Some(f) => f,
            None => self.storage.head_revision().await,
        };
        self.floors
            .write()
            .await
            .insert(prefix.to_string(), start_floor);

        // Highest revision we have delivered so far. `None` on the very first
        // connection (tail from now — the handler's initial LIST covers
        // pre-existing state). On every reconnect we resume from `last + 1` so
        // events committed while the stream was down are replayed, not
        // silently dropped.
        let mut resume_rev: Option<i64> = if start_floor > 0 {
            Some(start_floor)
        } else {
            None
        };

        // Perform the FIRST connection synchronously, before this function
        // returns `rx` to the caller. Without this, there is a window between
        // "the caller believes it is now watching" (rx handed back) and "the
        // storage subscription is actually live" (established inside the
        // spawned task, below) during which a write is invisible to this
        // watcher forever. For a real backend (etcd/rhino) that window is
        // harmless: `watch_since` REPLAYS every committed event from the given
        // revision, so a delayed connect still catches up. But the in-memory
        // bus backend's `watch_from_revision` has no true replay — it is a
        // plain "future events only" subscription — so a write landing in that
        // async gap is lost permanently, not just delayed. Established
        // live: reflector_lists_then_streams_live_mutations flaked exactly
        // this way once RV-watches started going through the shared cache.
        let first_connect = match resume_rev {
            Some(rev) => self.storage.watch_since(prefix, rev + 1).await,
            None => self.storage.watch_from_now(prefix).await,
        };

        // Continue the watch (reconnects included) in a background task.
        let storage = self.storage.clone();
        let prefix_owned = prefix.to_string();
        let tx_clone = tx.clone();
        let history_ref = self.history.clone();
        let floors_ref = self.floors.clone();

        tokio::spawn(async move {
            info!(
                "WatchCache: starting shared watch for prefix {}",
                prefix_owned
            );
            let mut pending_connect = Some(first_connect);
            // Watchdog cadence with backoff: a genuinely quiet prefix whose
            // reconnect replays nothing gets a doubling interval (cap 60s) so
            // hundreds of idle namespaced prefixes don't hammer the backend
            // every 10s; any real event resets to the 10s base.
            let mut liveness_interval = WATCH_LIVENESS_INTERVAL;
            loop {
                let connect = match pending_connect.take() {
                    Some(c) => c,
                    None => match resume_rev {
                        // A resume point the backend has compacted away can
                        // never be served: rhino/etcd cancels the watch, the
                        // stream ends with no events, and retrying the same
                        // revision reconnects into the same cancellation
                        // forever — the prefix goes permanently deaf (#1687).
                        // Idle prefixes (CSRs, namespaces, resourcequotas) are
                        // the ones that fall below the compaction floor.
                        // Re-base onto the current tail instead; the history in
                        // the gap is genuinely gone, and subscribers past the
                        // ring floor already 410-relist
                        // (`subscribe_from_checked`), which is upstream's
                        // "too old resource version" contract.
                        Some(rev) if storage.is_revision_compacted(rev + 1).await => {
                            warn!(
                                "WatchCache: {} resume revision {} is compacted — re-basing to the current tail; history in the gap is unrecoverable",
                                prefix_owned,
                                rev + 1
                            );
                            resume_rev = None;
                            storage.watch_from_now(&prefix_owned).await
                        }
                        Some(rev) => storage.watch_since(&prefix_owned, rev + 1).await,
                        None => storage.watch_from_now(&prefix_owned).await,
                    },
                };
                match connect {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        loop {
                            // Liveness watchdog: a stalled-open backend stream
                            // is indistinguishable from a quiet prefix except
                            // by the storage head advancing without us seeing
                            // events. Reconnect-with-replay heals it for every
                            // subscriber at once.
                            let event_result = match tokio::time::timeout(
                                liveness_interval,
                                stream.next(),
                            )
                            .await
                            {
                                Ok(Some(ev)) => {
                                    // Live stream — restore the fast watchdog.
                                    liveness_interval = WATCH_LIVENESS_INTERVAL;
                                    ev
                                }
                                Ok(None) => break, // stream ended → reconnect
                                Err(_idle) => {
                                    if let Some(resume) = resume_rev {
                                        let head = storage.head_revision().await;
                                        if head > resume {
                                            tracing::debug!(
                                                "WatchCache: {} stream silent for {:?} while storage advanced ({} > {}) — reconnecting with replay",
                                                prefix_owned,
                                                liveness_interval,
                                                head,
                                                resume
                                            );
                                            // Back off for the next round: if this
                                            // reconnect replays nothing the prefix is
                                            // just quiet, not stalled.
                                            liveness_interval = (liveness_interval * 2)
                                                .min(std::time::Duration::from_secs(30));
                                            break; // watch_since(resume+1) replays the gap
                                        }
                                    }
                                    continue;
                                }
                            };
                            {
                                // Extract the resourceVersion from the event value's metadata.
                                // Uses string search instead of full JSON parse since the format
                                // is controlled by our inject_resource_version() and is always
                                // "resourceVersion":"<digits>".
                                fn extract_rv(value: &str) -> i64 {
                                    const NEEDLE: &str = "\"resourceVersion\":\"";
                                    if let Some(start) = value.find(NEEDLE) {
                                        let num_start = start + NEEDLE.len();
                                        if let Some(end) = value[num_start..].find('"') {
                                            return value[num_start..num_start + end]
                                                .parse::<i64>()
                                                .unwrap_or(0);
                                        }
                                    }
                                    0
                                }

                                let cached = match event_result {
                                    Ok(WatchEvent::Added(key, value)) => {
                                        let rev = extract_rv(&value);
                                        CachedWatchEvent {
                                            event: WatchEventData::Added(key, Arc::new(value)),
                                            revision: rev,
                                        }
                                    }
                                    Ok(WatchEvent::Modified(key, value)) => {
                                        let rev = extract_rv(&value);
                                        CachedWatchEvent {
                                            event: WatchEventData::Modified(key, Arc::new(value)),
                                            revision: rev,
                                        }
                                    }
                                    Ok(WatchEvent::Deleted(key, prev_value)) => {
                                        let rev = extract_rv(&prev_value);
                                        CachedWatchEvent {
                                            event: WatchEventData::Deleted(
                                                key,
                                                Arc::new(prev_value),
                                            ),
                                            revision: rev,
                                        }
                                    }
                                    Err(_) => {
                                        // Transient error, continue
                                        continue;
                                    }
                                };

                                // Append to history ring buffer
                                {
                                    let mut hist: tokio::sync::RwLockWriteGuard<
                                        '_,
                                        HashMap<String, VecDeque<CachedWatchEvent>>,
                                    > = history_ref.write().await;
                                    let buf = hist.entry(prefix_owned.clone()).or_default();
                                    buf.push_back(cached.clone());
                                    // Retain the full ring only while someone is
                                    // watching; once the last watcher leaves, trim to
                                    // the idle tail so a busy-then-idle prefix doesn't
                                    // pin ~500 events forever (#1089).
                                    let cap = if tx_clone.receiver_count() > 0 {
                                        HISTORY_CAPACITY
                                    } else {
                                        HISTORY_IDLE_CAPACITY
                                    };
                                    let mut trimmed_to: Option<i64> = None;
                                    while buf.len() > cap {
                                        if let Some(popped) = buf.pop_front() {
                                            trimmed_to = Some(popped.revision);
                                        }
                                    }
                                    if let Some(rev) = trimmed_to {
                                        let mut floors = floors_ref.write().await;
                                        let f = floors.entry(prefix_owned.clone()).or_insert(0);
                                        if rev > *f {
                                            *f = rev;
                                        }
                                    }
                                }

                                // Advance the resume point so a reconnect replays
                                // strictly-later events (no gap, no duplicates).
                                // Guard on > 0: memory-backend events carry no RV.
                                if cached.revision > 0 {
                                    resume_rev = Some(cached.revision);
                                }

                                // Broadcast to live subscribers (Err is OK if no receivers)
                                let _ = tx_clone.send(cached);
                            }
                        }
                        // Stream ended, reconnect after brief pause
                        // Don't check subscriber count here — new subscribers may arrive
                        debug!(
                            "WatchCache: stream ended for {}, reconnecting",
                            prefix_owned
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        error!(
                            "WatchCache: failed to create watch for {}: {}",
                            prefix_owned, e
                        );
                        // A replay-from-revision connect can fail permanently if
                        // that revision has been compacted (long disconnect).
                        // Drop back to a from-now watch so we recover instead of
                        // spinning on the same compacted revision; the small
                        // gap this leaves is the same window a client watching
                        // that RV would get a 410 for.
                        resume_rev = None;
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        rx
    }

    /// Spawn the background sweep that reclaims replay-ring memory for prefixes
    /// whose external watcher count has dropped to zero (#1089). The append path
    /// already trims to the idle tail when a new event arrives; this sweep covers
    /// the steady-idle case where a busy prefix goes quiet with no further events
    /// to trigger that trim, and `shrink_to_fit`s the freed capacity back.
    pub fn spawn_idle_gc(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(IDLE_GC_INTERVAL);
            tick.tick().await; // first tick fires immediately; skip it
            loop {
                tick.tick().await;
                this.gc_idle_history().await;
            }
        });
    }

    /// Shrink the replay ring of every prefix that currently has no live
    /// receivers down to [`HISTORY_IDLE_CAPACITY`], releasing the backing
    /// capacity. Idempotent and cheap when nothing is idle.
    async fn gc_idle_history(&self) {
        let idle_prefixes: Vec<String> = {
            let watchers = self.watchers.read().await;
            watchers
                .iter()
                .filter(|(_, tx)| tx.receiver_count() == 0)
                .map(|(prefix, _)| prefix.clone())
                .collect()
        };
        if idle_prefixes.is_empty() {
            return;
        }
        let mut hist = self.history.write().await;
        let mut floors = self.floors.write().await;
        for prefix in idle_prefixes {
            if let Some(buf) = hist.get_mut(&prefix) {
                if buf.len() > HISTORY_IDLE_CAPACITY {
                    let drop_n = buf.len() - HISTORY_IDLE_CAPACITY;
                    if let Some(last_dropped) = buf.get(drop_n - 1) {
                        let f = floors.entry(prefix.clone()).or_insert(0);
                        if last_dropped.revision > *f {
                            *f = last_dropped.revision;
                        }
                    }
                    buf.drain(..drop_n);
                    buf.shrink_to_fit();
                    debug!(
                        "WatchCache: idle-GC shrank replay ring for {} to {} events",
                        prefix,
                        buf.len()
                    );
                }
            }
        }
    }

    /// Get the current approximate revision
    #[allow(dead_code)]
    pub async fn current_revision(&self) -> i64 {
        *self.revision.read().await
    }

    /// Get all cached events for a prefix with revision > the given revision.
    pub async fn get_events_since(&self, prefix: &str, revision: i64) -> Vec<CachedWatchEvent> {
        let hist = self.history.read().await;
        match hist.get(prefix) {
            Some(buf) => buf
                .iter()
                .filter(|e| e.revision > revision)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Subscribe to watch events and replay any historical events since the
    /// given resourceVersion. Returns (historical_events, live_receiver).
    /// The caller should send historical events first, then consume the receiver.
    pub async fn subscribe_from(
        &self,
        prefix: &str,
        since_revision: i64,
    ) -> (Vec<CachedWatchEvent>, broadcast::Receiver<CachedWatchEvent>) {
        // Subscribe first to avoid missing events between history query and subscribe
        let rx = self.subscribe(prefix).await;
        // Then get historical events
        let history = self.get_events_since(prefix, since_revision).await;
        (history, rx)
    }

    /// Like [`subscribe_from`], but verifies the ring actually COVERS
    /// `since_revision`. Returns `Err(floor)` when it does not — the ring only
    /// holds events with revision > floor, so replaying from an older RV would
    /// silently skip whatever was trimmed (or predates the shared watch).
    /// Callers must answer 410 Expired so the client relists — upstream
    /// cacher "too old resource version" semantics.
    pub async fn subscribe_from_checked(
        &self,
        prefix: &str,
        since_revision: i64,
    ) -> Result<(Vec<CachedWatchEvent>, broadcast::Receiver<CachedWatchEvent>), i64> {
        // Subscribe first: creates the shared watcher if absent, flooring it at
        // the client's RV so the very first watch of an on-demand prefix is
        // served (backend replay from rv+1 fills the ring for it).
        let rx = self
            .subscribe_with_floor(prefix, Some(since_revision))
            .await;
        let floor = self.floors.read().await.get(prefix).copied().unwrap_or(0);
        if since_revision < floor {
            return Err(floor);
        }
        let history = self.get_events_since(prefix, since_revision).await;
        Ok((history, rx))
    }
}

/// Convert a broadcast receiver into a WatchStream compatible with existing handlers.
pub fn broadcast_to_stream(mut rx: broadcast::Receiver<CachedWatchEvent>) -> WatchStream {
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(cached) => {
                    let event = match cached.event {
                        WatchEventData::Added(key, value) => WatchEvent::Added(key, (*value).clone()),
                        WatchEventData::Modified(key, value) => WatchEvent::Modified(key, (*value).clone()),
                        WatchEventData::Deleted(key, prev) => WatchEvent::Deleted(key, (*prev).clone()),
                    };
                    yield Ok(event);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // The subscriber fell behind the broadcast ring and `n`
                    // events are irretrievably gone for THIS stream. Silently
                    // continuing would leave the client permanently blind to
                    // those objects (client-go reflectors never re-list on
                    // their own) — the exact wedge behind stuck informers
                    // (#1165: deployment ReadyReplicas, endpointslice
                    // readiness, quota usage). Upstream's watch cache
                    // TERMINATES a too-slow watcher so the client re-lists
                    // (apiserver/pkg/storage/cacher). Mirror that: surface a
                    // 410-style error and end the stream.
                    tracing::warn!(
                        "watch subscriber lagged by {} events — terminating watch so the client relists",
                        n
                    );
                    yield Err(rusternetes_common::Error::Gone(format!(
                        "too old resource version: watch lagged behind by {} events",
                        n
                    )));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };
    Box::pin(stream)
}

/// Convert historical events + a broadcast receiver into a WatchStream.
/// Historical events are replayed first (in order), then live events follow.
pub fn broadcast_to_stream_with_history(
    history: Vec<CachedWatchEvent>,
    mut rx: broadcast::Receiver<CachedWatchEvent>,
) -> WatchStream {
    // Track the highest revision we replayed so we can deduplicate
    let max_history_rev = history.iter().map(|e| e.revision).max().unwrap_or(0);

    let stream = async_stream::stream! {
        // Replay historical events first
        for cached in history {
            let event = match cached.event {
                WatchEventData::Added(key, value) => WatchEvent::Added(key, (*value).clone()),
                WatchEventData::Modified(key, value) => WatchEvent::Modified(key, (*value).clone()),
                WatchEventData::Deleted(key, prev) => WatchEvent::Deleted(key, (*prev).clone()),
            };
            yield Ok(event);
        }

        // Then stream live events, skipping any that overlap with history
        loop {
            match rx.recv().await {
                Ok(cached) => {
                    // Skip events we already replayed from history. Only
                    // applies when the event carries a REAL revision (> 0):
                    // the in-memory backend never stamps a resourceVersion
                    // onto the raw published object, so `extract_rv` always
                    // returns 0 for it — indistinguishable from
                    // `max_history_rev`'s empty-history default of 0. Without
                    // this guard, `0 <= 0` treated every live MemoryStorage
                    // event as an already-seen duplicate and silently dropped
                    // it (reflector_lists_then_streams_live_mutations).
                    if cached.revision > 0 && cached.revision <= max_history_rev {
                        continue;
                    }
                    let event = match cached.event {
                        WatchEventData::Added(key, value) => WatchEvent::Added(key, (*value).clone()),
                        WatchEventData::Modified(key, value) => WatchEvent::Modified(key, (*value).clone()),
                        WatchEventData::Deleted(key, prev) => WatchEvent::Deleted(key, (*prev).clone()),
                    };
                    yield Ok(event);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // See broadcast_to_stream: a lagged subscriber lost events
                    // permanently — terminate so the client relists (upstream
                    // cacher parity), never silently continue.
                    tracing::warn!(
                        "watch subscriber (with history) lagged by {} events — terminating watch so the client relists",
                        n
                    );
                    yield Err(rusternetes_common::Error::Gone(format!(
                        "too old resource version: watch lagged behind by {} events",
                        n
                    )));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use rusternetes_storage::StorageBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mock source with a non-zero head revision: the shared loop must start
    /// with `watch_since(head+1)` (never from-now) and the floor must gate
    /// old-RV subscriptions with Err(floor) → 410 → client relists.
    struct FloorSource {
        since_revs: std::sync::Mutex<Vec<i64>>,
    }

    #[async_trait]
    impl WatchSource for FloorSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            panic!("with a non-zero head revision the loop must use watch_since, not from-now");
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_revs.lock().unwrap().push(revision);
            let s = futures::stream::iter(Vec::new()).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }

        async fn head_revision(&self) -> i64 {
            100
        }
    }

    // The FIRST checked subscriber of a prefix defines the ring floor (its
    // list RV) and the backend replay starts from rv+1 — an on-demand prefix
    // (fresh namespace) must serve its very first watch instead of 410ing it
    // just because the GLOBAL head is newer. Later subscribers below the floor
    // get Err(floor) → 410 → relist.
    #[tokio::test]
    async fn subscribe_from_checked_gates_on_ring_floor() {
        let source = Arc::new(FloorSource {
            since_revs: std::sync::Mutex::new(Vec::new()),
        });
        let cache = WatchCache::from_source(source.clone());

        // First subscriber at RV 50 (global head is 100): must be SERVED —
        // the watcher is created with floor 50.
        assert!(
            cache
                .subscribe_from_checked("/registry/pods/", 50)
                .await
                .is_ok(),
            "first watch of an on-demand prefix must be served from its own RV"
        );

        // The shared loop connected via watch_since(first_rv + 1), filling the
        // ring from exactly where the first client needs it.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if source.since_revs.lock().unwrap().first() == Some(&51) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("shared loop must start from watch_since(first_rv+1)");

        // A LATER subscriber below the established floor must 410-relist.
        let err = cache
            .subscribe_from_checked("/registry/pods/", 30)
            .await
            .expect_err("RV below the ring floor must 410, not silently under-replay");
        assert_eq!(err, 50);

        // At/above the floor → served.
        assert!(cache
            .subscribe_from_checked("/registry/pods/", 50)
            .await
            .is_ok());
    }

    // Regression (#1165 wedge): a broadcast subscriber that lags must be
    // TERMINATED with a Gone error (→ handler sends a 410 ERROR event, client
    // relists), never silently skipped past — a silently dropped MODIFIED
    // leaves long-lived informers (KCM deployment ReadyReplicas, endpointslice
    // readiness, quota usage) permanently stale.
    #[tokio::test]
    async fn lagged_subscriber_terminates_with_gone() {
        let (tx, rx) = broadcast::channel::<CachedWatchEvent>(2);
        // Overflow the 2-slot ring before the stream consumes anything.
        for rev in 1..=5 {
            let val = Arc::new(format!(
                "{{\"metadata\":{{\"name\":\"k{rev}\",\"resourceVersion\":\"{rev}\"}}}}"
            ));
            tx.send(CachedWatchEvent {
                revision: rev,
                event: WatchEventData::Added(format!("/registry/secrets/ns/k{rev}"), val),
            })
            .unwrap();
        }

        let mut stream = broadcast_to_stream(rx);
        let mut yielded = Vec::new();
        let mut got_gone = false;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("stream must not hang after lag")
        {
            match item {
                Ok(ev) => yielded.push(ev),
                Err(rusternetes_common::Error::Gone(msg)) => {
                    assert!(
                        msg.contains("lagged"),
                        "Gone error should describe the lag, got: {msg}"
                    );
                    got_gone = true;
                }
                Err(other) => panic!("unexpected error type: {other}"),
            }
        }
        assert!(
            got_gone,
            "lagged subscriber MUST receive Error::Gone before the stream ends (got {} events, then clean end)",
            yielded.len()
        );
    }

    fn added(rev: i64) -> Result<WatchEvent, rusternetes_common::Error> {
        // The loop parses the RV out of the JSON via extract_rv, so the value
        // MUST carry a `resourceVersion` matching `rev`.
        let json =
            format!("{{\"metadata\":{{\"name\":\"k{rev}\",\"resourceVersion\":\"{rev}\"}}}}");
        Ok(WatchEvent::Added(
            format!("/registry/secrets/ns/k{rev}"),
            json,
        ))
    }

    /// A watch source that models what rhino/etcd does once the resume
    /// revision has been COMPACTED: the watch is cancelled server-side, so the
    /// stream ends immediately having delivered nothing. `watch_from_now` still
    /// works and tails live events.
    ///
    /// Retrying the compacted revision can never succeed — compaction only
    /// moves forward — so a loop that keeps resuming from it leaves the prefix
    /// permanently deaf (#1687).
    struct CompactedResumeSource {
        since_calls: AtomicUsize,
        from_now_calls: AtomicUsize,
    }

    #[async_trait]
    impl WatchSource for CompactedResumeSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            self.from_now_calls.fetch_add(1, Ordering::SeqCst);
            // Re-based stream: a live event arrives and the stream stays open.
            let s = futures::stream::iter(vec![added(101)]).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            _revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_calls.fetch_add(1, Ordering::SeqCst);
            // Cancelled by the backend: ends at once, no events, no error.
            let s = futures::stream::iter(Vec::new());
            Ok(Box::pin(s))
        }

        async fn head_revision(&self) -> i64 {
            100
        }

        async fn is_revision_compacted(&self, _revision: i64) -> bool {
            true
        }
    }

    /// Regression for #1687: when the resume revision has been compacted, the
    /// shared prefix loop must re-base onto `watch_from_now` instead of
    /// retrying the dead revision forever. Idle prefixes (CSRs, namespaces,
    /// resourcequotas) fall below rhino's compaction floor and went
    /// permanently deaf — no live event reached any subscriber, which is what
    /// timed out the CSR API-operations conformance watch step.
    #[tokio::test]
    async fn compacted_resume_revision_rebases_instead_of_wedging() {
        let source = Arc::new(CompactedResumeSource {
            since_calls: AtomicUsize::new(0),
            from_now_calls: AtomicUsize::new(0),
        });
        let cache = WatchCache::from_source(source.clone());

        let mut rx = cache
            .subscribe("/registry/certificatesigningrequests/")
            .await;

        // The live event that only a re-based stream can deliver.
        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("compacted resume must re-base, not wedge retrying the dead revision")
            .expect("subscriber must receive the live event");
        assert_eq!(got.revision, 101);
        assert!(
            source.from_now_calls.load(Ordering::SeqCst) >= 1,
            "loop must reconnect via watch_from_now once the resume revision is compacted"
        );
    }

    /// A watch source that models a storage connection that drops after the
    /// first event, while a new object is committed during the gap.
    ///
    /// - `watch_from_now`: first call yields the event at rev 5 then the stream
    ///   ENDS (a dropped connection). Any later `watch_from_now` call yields
    ///   nothing and stays open — this is what the buggy reconnect hits, so it
    ///   never observes the rev-6 create.
    /// - `watch_since(rev)`: yields the gap event at rev 6 then stays open.
    ///   Only the fixed reconnect path calls this.
    struct DroppingSource {
        from_now_calls: AtomicUsize,
        since_revs: std::sync::Mutex<Vec<i64>>,
    }

    #[async_trait]
    impl WatchSource for DroppingSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            let n = self.from_now_calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First connection: deliver rev 5, then the stream ends.
                let s = futures::stream::iter(vec![added(5)]);
                Ok(Box::pin(s))
            } else {
                // Reconnect via from-now (the BUG): the rev-6 create committed
                // during the gap is invisible here. Stay open so the loop
                // doesn't spin.
                let s = futures::stream::iter(Vec::new()).chain(futures::stream::pending::<
                    Result<WatchEvent, rusternetes_common::Error>,
                >());
                Ok(Box::pin(s))
            }
        }

        async fn head_revision(&self) -> i64 {
            0 // from-now first connect, like the memory backend
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_revs.lock().unwrap().push(revision);
            // Replay the gap event committed while the stream was down, then
            // stay open.
            let s = futures::stream::iter(vec![added(6)]).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }
    }

    /// When the shared backend watch drops and reconnects, an event committed
    /// during the gap MUST still reach subscribers. Regression test for the
    /// cert-manager cainjector flake: the CA-secret create landed in a
    /// reconnect gap, the caBundle never got injected, and every admission
    /// webhook call failed with `UnknownIssuer`.
    #[tokio::test]
    async fn reconnect_replays_events_committed_during_the_gap() {
        let source = Arc::new(DroppingSource {
            from_now_calls: AtomicUsize::new(0),
            since_revs: std::sync::Mutex::new(Vec::new()),
        });
        let cache = WatchCache::from_source(source.clone());

        let mut rx = cache.subscribe("/registry/secrets/").await;

        // Collect revisions until we see rev 6 (the gap event) or time out.
        let mut seen = Vec::new();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        seen.push(ev.revision);
                        if ev.revision == 6 {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "timed out waiting for the gap event (rev 6); got {seen:?}. \
             The reconnect must replay events committed while the stream was down."
        );
        assert!(
            seen.contains(&5),
            "the pre-drop event (rev 5) should arrive"
        );
        assert!(
            seen.contains(&6),
            "the event committed during the reconnect gap (rev 6) must be replayed"
        );
        assert_eq!(
            *source.since_revs.lock().unwrap(),
            vec![6],
            "reconnect must resume from last_delivered_rev + 1 (5 + 1 = 6)"
        );
    }

    fn make_event(rev: i64) -> CachedWatchEvent {
        CachedWatchEvent {
            event: WatchEventData::Added(format!("k{rev}"), Arc::new("{}".to_string())),
            revision: rev,
        }
    }

    #[tokio::test]
    async fn idle_gc_shrinks_only_prefixes_without_receivers() {
        let storage = Arc::new(StorageBackend::new_memory());
        let cache = WatchCache::new(storage);

        // Prefix "a": a watcher with a LIVE receiver — must not be shrunk.
        let (tx_a, rx_a) = broadcast::channel(1000);
        // Prefix "b": a watcher whose receiver was dropped — count 0, eligible.
        let (tx_b, rx_b) = broadcast::channel(1000);
        drop(rx_b);
        {
            let mut watchers = cache.watchers.write().await;
            watchers.insert("a".to_string(), tx_a);
            watchers.insert("b".to_string(), tx_b);
        }
        {
            let mut hist = cache.history.write().await;
            hist.insert("a".to_string(), (0..100).map(make_event).collect());
            hist.insert("b".to_string(), (0..100).map(make_event).collect());
        }

        cache.gc_idle_history().await;

        let hist = cache.history.read().await;
        assert_eq!(
            hist["a"].len(),
            100,
            "a prefix with a live receiver must not be shrunk"
        );
        assert_eq!(
            hist["b"].len(),
            HISTORY_IDLE_CAPACITY,
            "an idle prefix is shrunk to the idle tail"
        );
        // The MOST RECENT events are retained, so recent replay (e.g. the CRD
        // Established MODIFIED) still survives the shrink.
        assert_eq!(hist["b"].back().unwrap().revision, 99);
        assert_eq!(
            hist["b"].front().unwrap().revision,
            100 - HISTORY_IDLE_CAPACITY as i64
        );

        drop(rx_a); // keep the receiver alive across the GC sweep above
    }
}
