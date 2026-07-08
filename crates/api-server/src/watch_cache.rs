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
use tracing::{debug, error, info};

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
        }
    }

    /// Subscribe to watch events for a resource prefix.
    /// Returns a broadcast receiver that will receive all events for this prefix.
    /// If no etcd watch exists for this prefix, one is started.
    pub async fn subscribe(&self, prefix: &str) -> broadcast::Receiver<CachedWatchEvent> {
        // Check if we already have a watcher for this prefix
        {
            let watchers = self.watchers.read().await;
            if let Some(tx) = watchers.get(prefix) {
                return tx.subscribe();
            }
        }

        // Create a new watcher
        // Buffer size: K8s default watch cache is 1000 events. 16384 used ~1.2GB
        // of memory with 26 prefixes × 16K events × ~3KB each.
        let (tx, rx) = broadcast::channel(1000);
        {
            let mut watchers = self.watchers.write().await;
            // Double-check after acquiring write lock
            if let Some(existing_tx) = watchers.get(prefix) {
                return existing_tx.subscribe();
            }
            watchers.insert(prefix.to_string(), tx.clone());
        }

        // Start the etcd watch in a background task
        let storage = self.storage.clone();
        let prefix_owned = prefix.to_string();
        let tx_clone = tx.clone();
        let history_ref = self.history.clone();

        tokio::spawn(async move {
            info!(
                "WatchCache: starting shared watch for prefix {}",
                prefix_owned
            );
            // Highest revision we have delivered so far. `None` on the very
            // first connection (tail from now — the handler's initial LIST
            // covers pre-existing state). On every reconnect we resume from
            // `last + 1` so events committed while the stream was down are
            // replayed, not silently dropped. Missing this replay let the
            // cert-manager cainjector miss the webhook-CA secret create that
            // landed in a reconnect gap, leaving the caBundle empty and every
            // admission call failing with `UnknownIssuer`.
            let mut resume_rev: Option<i64> = None;
            loop {
                let connect = match resume_rev {
                    Some(rev) => storage.watch_since(&prefix_owned, rev + 1).await,
                    None => storage.watch_from_now(&prefix_owned).await,
                };
                match connect {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        while let Some(event_result) = stream.next().await {
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
                                        event: WatchEventData::Deleted(key, Arc::new(prev_value)),
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
                                while buf.len() > cap {
                                    buf.pop_front();
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
        for prefix in idle_prefixes {
            if let Some(buf) = hist.get_mut(&prefix) {
                if buf.len() > HISTORY_IDLE_CAPACITY {
                    let drop_n = buf.len() - HISTORY_IDLE_CAPACITY;
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
                    debug!("Watch stream lagged by {} events, continuing", n);
                    continue;
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
                    // Skip events we already replayed from history
                    if cached.revision <= max_history_rev {
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
                    debug!("Watch stream lagged by {} events, continuing", n);
                    continue;
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
