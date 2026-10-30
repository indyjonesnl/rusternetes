//! Kubernetes-style rate-limited work queue with deduplication.
//!
//! Modeled after client-go's `workqueue.RateLimitingInterface`. Controllers
//! enqueue resource keys from watch events; worker tasks pull keys and
//! reconcile one resource at a time. The queue provides:
//!
//! - **Deduplication**: a key already queued or being processed is not re-added
//! - **Rate limiting**: exponential backoff on failures (1s → 2s → 4s → ... → 5min cap)
//! - **Fair FIFO**: keys processed in arrival order
//! - **Shutdown**: graceful drain via `shutdown()`

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};

use crate::WatchEvent;

/// Sentinel key that triggers a full `reconcile_all()` instead of
/// per-resource reconciliation. Used by controllers that don't
/// have a `reconcile_one()` method.
pub const RECONCILE_ALL_SENTINEL: &str = "__reconcile_all__";

/// Extract the resource identity key from a WatchEvent's storage key.
///
/// Storage keys follow `/registry/{resource_type}/{namespace}/{name}` for
/// namespaced resources or `/registry/{resource_type}/{name}` for
/// cluster-scoped resources.
///
/// Returns the key with the `/registry/` prefix stripped, e.g.
/// `deployments/default/my-deploy` or `namespaces/kube-system`.
pub fn extract_key(event: &WatchEvent) -> String {
    let storage_key = match event {
        WatchEvent::Added(key, _) => key,
        WatchEvent::Modified(key, _) => key,
        WatchEvent::Deleted(key, _) => key,
    };
    storage_key
        .strip_prefix("/registry/")
        .unwrap_or(storage_key)
        .to_string()
}

/// Configuration for work queue rate limiting.
pub struct WorkQueueConfig {
    /// Initial backoff duration after first failure. Default: 1 second.
    pub base_delay: Duration,
    /// Maximum backoff duration. Default: 5 minutes.
    pub max_delay: Duration,
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            // Match K8s client-go defaults: 5ms base, 1000s max
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_secs(1000),
        }
    }
}

/// A rate-limited work queue with deduplication.
#[derive(Clone)]
pub struct WorkQueue {
    pub inner: Arc<Mutex<WorkQueueInner>>,
    pub notify: Arc<Notify>,
    pub config: Arc<WorkQueueConfig>,
}

pub struct WorkQueueInner {
    /// Keys waiting to be processed (FIFO).
    pub queue: VecDeque<String>,
    /// Set of keys currently in `queue` (O(1) dedup check).
    pub dirty: HashSet<String>,
    /// Keys currently being processed by a worker.
    pub processing: HashSet<String>,
    /// Number of consecutive failures per key.
    pub failures: HashMap<String, u32>,
    /// Keys delayed until a deadline (waiting for backoff).
    pub delayed: HashMap<String, Instant>,
    /// When each key was last dequeued for processing.
    /// Used to prevent self-write feedback loops: if a key is re-dirtied
    /// within `min_reprocess_interval` of being dequeued, the re-queue
    /// is delayed to break the cycle.
    pub last_dequeued: HashMap<String, Instant>,
    /// Whether the queue has been shut down.
    pub shutdown: bool,
}

/// Minimum interval between processing the same key. Prevents tight loops
/// when a controller writes back to its own watched resource (e.g. status
/// updates). Set to 200ms — fast enough for test convergence, long enough
/// to coalesce self-triggered events (status write -> watch -> re-enqueue).
const MIN_REPROCESS_INTERVAL: Duration = Duration::from_millis(200);

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkQueue {
    /// Create a new work queue with default rate limiting.
    pub fn new() -> Self {
        Self::with_config(WorkQueueConfig::default())
    }

    /// Create a new work queue with custom rate limiting configuration.
    pub fn with_config(config: WorkQueueConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WorkQueueInner {
                queue: VecDeque::new(),
                dirty: HashSet::new(),
                processing: HashSet::new(),
                failures: HashMap::new(),
                delayed: HashMap::new(),
                last_dequeued: HashMap::new(),
                shutdown: false,
            })),
            notify: Arc::new(Notify::new()),
            config: Arc::new(config),
        }
    }

    /// Add a key to the queue. If the key is already queued or being
    /// processed, this is a no-op (deduplication).
    pub async fn add(&self, key: String) {
        let mut inner = self.inner.lock().await;
        if inner.shutdown {
            return;
        }
        // If already queued, nothing to do
        if inner.dirty.contains(&key) {
            return;
        }
        // Mark as dirty. If the key is currently being processed,
        // done() will see the dirty flag and re-queue it.
        inner.dirty.insert(key.clone());
        if inner.processing.contains(&key) {
            // Will be re-queued when done() is called
            return;
        }
        // Remove from delayed if present (immediate add takes priority)
        inner.delayed.remove(&key);
        inner.queue.push_back(key);
        drop(inner);
        self.notify.notify_one();
    }

    /// Add a key that will become available after `delay`. Used internally
    /// for rate-limited re-queues.
    pub async fn add_after(&self, key: String, delay: Duration) {
        let mut inner = self.inner.lock().await;
        if inner.shutdown {
            return;
        }
        // Don't delay if already queued
        if inner.dirty.contains(&key) {
            return;
        }
        // If processing, mark dirty so done() re-queues with delay
        // (but for rate-limited requeue we use delayed map directly)
        if inner.processing.contains(&key) {
            // Don't mark dirty — requeue_rate_limited should use delayed map
        }
        let deadline = Instant::now() + delay;
        // delaying_queue.go `insert`: "if the entry already exists, update
        // the time only if it would cause the item to be queued sooner".
        let entry = inner.delayed.entry(key).or_insert(deadline);
        if deadline < *entry {
            *entry = deadline;
        }
        drop(inner);
        // Wake up get() so it can recalculate its sleep deadline
        self.notify.notify_one();
    }

    /// Get the next key to process. Blocks until a key is available
    /// or the queue is shut down. Returns `None` on shutdown.
    ///
    /// Safe for any number of concurrent callers, which is what lets a
    /// controller run a worker pool over one queue. Two properties carry that:
    ///
    /// 1. A key moves into `processing` under the same lock that pops it, so no
    ///    two workers ever hold the same key, and `add()` for a key already in
    ///    `processing` only marks it dirty — `done()` re-queues it afterwards.
    ///    That is client-go's dirty/processing split: `Add` returns early on
    ///    `processing.Has(item)`
    ///    (staging/src/k8s.io/client-go/util/workqueue/queue.go:227, early
    ///    return at 245-247) and `Done` re-pushes only then (queue.go:289,
    ///    push at 297).
    ///
    /// 2. No wake-up is lost. `add()` and `done()` each notify for the single
    ///    key they enqueue, matching upstream's `cond.Signal()` (queue.go:250
    ///    and 298). The delayed-item promotion below is the one path that
    ///    enqueues MANY keys while notifying none — and it does not need to:
    ///    a worker parks on `tokio::select!` over `notified()` AND a sleep to
    ///    the earliest `delayed` deadline, so every parked worker wakes on that
    ///    deadline by itself and re-checks the queue. Upstream reaches the same
    ///    place differently: its delaying queue promotes ready items from a
    ///    separate `waitingLoop` goroutine
    ///    (util/workqueue/delaying_queue.go:276) by calling `q.Add(entry.data)`
    ///    (drain loop 298-307, `Add` at 305), so each promoted item signals
    ///    once. Both designs guarantee a promoted batch gets drained; ours does
    ///    it with per-worker timers instead of a promoter goroutine.
    ///
    /// The tests `worker_pool_drains_every_promoted_delayed_key` and
    /// `a_key_re_added_while_processing_is_not_handed_to_a_second_worker` pin
    /// both properties, since a pool over this queue depends on them.
    pub async fn get(&self) -> Option<String> {
        loop {
            {
                let mut inner = self.inner.lock().await;
                if inner.shutdown && inner.queue.is_empty() {
                    return None;
                }

                // Promote delayed items that are now ready
                let now = Instant::now();
                let ready: Vec<String> = inner
                    .delayed
                    .iter()
                    .filter(|(_, &deadline)| now >= deadline)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in ready {
                    inner.delayed.remove(&key);
                    // `waitingLoop` hands the matured entry to `Add`
                    // (delaying_queue.go:305): a key being processed is only
                    // marked dirty, and `done()` re-queues it.
                    if inner.dirty.insert(key.clone()) && !inner.processing.contains(&key) {
                        inner.queue.push_back(key);
                    }
                }

                // Try to dequeue
                if let Some(key) = inner.queue.pop_front() {
                    inner.dirty.remove(&key);
                    inner.processing.insert(key.clone());
                    inner.last_dequeued.insert(key.clone(), Instant::now());
                    return Some(key);
                }
            }

            // Nothing ready — wait for notification or next delayed deadline
            let next_deadline = {
                let inner = self.inner.lock().await;
                if inner.shutdown {
                    return None;
                }
                inner.delayed.values().copied().min()
            };

            match next_deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if deadline > now {
                        tokio::select! {
                            _ = self.notify.notified() => {}
                            _ = tokio::time::sleep(deadline - now) => {}
                        }
                    }
                    // Loop back to check/promote
                }
                None => {
                    self.notify.notified().await;
                }
            }
        }
    }

    /// Mark a key as done processing. Must be called after `get()`.
    /// If the key was re-added via `add()` while being processed,
    /// it will be re-queued — either immediately (if enough time has passed)
    /// or after a cooldown delay (to prevent self-write feedback loops
    /// where status updates trigger immediate re-reconciliation).
    pub async fn done(&self, key: &str) {
        let mut inner = self.inner.lock().await;
        inner.processing.remove(key);
        // If add() was called while we were processing, re-queue
        if inner.dirty.contains(key) {
            // Check if this key was dequeued recently — if so, delay to
            // prevent self-write feedback loops (controller writes status,
            // which triggers watch event, which re-enqueues same key).
            let now = Instant::now();
            if let Some(&dequeued_at) = inner.last_dequeued.get(key) {
                let elapsed = now - dequeued_at;
                if elapsed < MIN_REPROCESS_INTERVAL {
                    // Too soon — delay the re-queue
                    let remaining = MIN_REPROCESS_INTERVAL - elapsed;
                    inner.dirty.remove(key);
                    inner.delayed.insert(key.to_string(), now + remaining);
                    drop(inner);
                    self.notify.notify_one();
                    return;
                }
            }
            // Enough time has passed — immediate re-queue
            inner.queue.push_back(key.to_string());
            drop(inner);
            self.notify.notify_one();
        }
    }

    /// Re-queue a key with exponential backoff. Called when reconciliation
    /// fails. Increments the failure counter and delays the re-queue.
    pub async fn requeue_rate_limited(&self, key: String) {
        let delay = {
            let mut inner = self.inner.lock().await;
            let failures = inner.failures.entry(key.clone()).or_insert(0);
            *failures += 1;
            backoff_duration(*failures, self.config.base_delay, self.config.max_delay)
        };
        self.add_after(key, delay).await;
    }

    /// `NumRequeues`: how many times the key has been rate-limit requeued
    /// since the last `forget`.
    pub async fn num_requeues(&self, key: &str) -> u32 {
        let inner = self.inner.lock().await;
        inner.failures.get(key).copied().unwrap_or(0)
    }

    /// Clear the failure counter for a key. Called after successful
    /// reconciliation.
    pub async fn forget(&self, key: &str) {
        let mut inner = self.inner.lock().await;
        inner.failures.remove(key);
    }

    /// Signal shutdown. After this, `get()` returns `None` once the
    /// queue is drained.
    pub async fn shutdown(&self) {
        let mut inner = self.inner.lock().await;
        inner.shutdown = true;
        drop(inner);
        self.notify.notify_waiters();
    }

    /// Returns the number of keys currently waiting in the queue.
    pub async fn len(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.queue.len()
    }

    /// Returns true if the queue is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

pub fn backoff_duration(failures: u32, base: Duration, max: Duration) -> Duration {
    if failures == 0 {
        return Duration::ZERO;
    }
    let multiplier = 1u64
        .checked_shl(failures.saturating_sub(1))
        .unwrap_or(u64::MAX);
    let delay_ms = (base.as_millis() as u64).saturating_mul(multiplier);
    let delay = Duration::from_millis(delay_ms.min(max.as_millis() as u64));
    delay.min(max)
}
