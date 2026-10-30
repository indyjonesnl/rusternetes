//! A typed, synchronous-producer rate-limiting delaying work queue.
//!
//! Port of `k8s.io/client-go/util/workqueue` (release-1.35) as used by the
//! device taint eviction controller:
//!
//! - `Typed` queue (`queue.go`): `dirty` / `processing` sets so an item added
//!   while it is being processed is handed out again only after `Done`, and an
//!   item queued twice is delivered once.
//! - `delayingType` (`delaying_queue.go`): `AddAfter` keeps only the EARLIER
//!   ready time of an item that is already waiting (`insert`, :340-349).
//! - `DefaultTypedControllerRateLimiter` (`default_rate_limiters.go:50-55`):
//!   `max(ItemExponentialFailureRateLimiter(5ms, 1000s),
//!   BucketRateLimiter(10 qps, 100 burst))`.
//!
//! Unlike the string-keyed, async-producer [`rusternetes_storage::WorkQueue`],
//! producers here are plain (non-async) methods. Upstream's informer event
//! handlers call `Add`/`AddAfter` while holding the controller mutex and must
//! never block; an async producer could not be called from there.
//!
//! [`WorkItemQueue`] is the `workqueue.TypedRateLimitingInterface` seam: the
//! controller depends on the trait, tests substitute a recording mock (the
//! upstream `Mock`, `mockqueue_test.go`) and the eviction simulation a no-op
//! queue (`nopqueue.go`).

use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

/// The subset of `workqueue.TypedRateLimitingInterface` the controller uses.
pub trait WorkItemQueue<T>: Send + Sync {
    /// `Add`.
    fn add(&self, item: T);
    /// `AddAfter`.
    fn add_after(&self, item: T, delay: Duration);
    /// `AddRateLimited`.
    fn add_rate_limited(&self, item: T);
    /// `Forget`.
    fn forget(&self, item: &T);
    /// `NumRequeues`.
    fn num_requeues(&self, item: &T) -> usize;
    /// `Get`: the next ready item, or `None` once the queue is shut down.
    fn get(&self) -> BoxFuture<'_, Option<T>>;
    /// `Done`.
    fn done(&self, item: &T);
    /// `ShutDown`.
    fn shut_down(&self);
}

/// `NOPQueue` (`devicetainteviction/nopqueue.go`): drops everything and
/// reports "shut down" from `Get`.
pub struct NopQueue;

impl<T: Send + 'static> WorkItemQueue<T> for NopQueue {
    fn add(&self, _item: T) {}
    fn add_after(&self, _item: T, _delay: Duration) {}
    fn add_rate_limited(&self, _item: T) {}
    fn forget(&self, _item: &T) {}
    fn num_requeues(&self, _item: &T) -> usize {
        0
    }
    fn get(&self) -> BoxFuture<'_, Option<T>> {
        Box::pin(std::future::ready(None))
    }
    fn done(&self, _item: &T) {}
    fn shut_down(&self) {}
}

/// `ItemExponentialFailureRateLimiter` base delay in
/// `DefaultTypedControllerRateLimiter`.
const BASE_DELAY: Duration = Duration::from_millis(5);
/// `ItemExponentialFailureRateLimiter` max delay.
const MAX_DELAY: Duration = Duration::from_secs(1000);
/// `BucketRateLimiter`: `rate.NewLimiter(rate.Limit(10), 100)`.
const BUCKET_QPS: f64 = 10.0;
const BUCKET_BURST: f64 = 100.0;

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    /// `rate.Limiter.Reserve().Delay()`: take one token, report how long the
    /// caller has to wait for it.
    fn reserve(&mut self, now: Instant) -> Duration {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * BUCKET_QPS).min(BUCKET_BURST);
        self.last = now;
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-self.tokens / BUCKET_QPS)
        }
    }
}

struct Inner<T> {
    queue: VecDeque<T>,
    dirty: HashSet<T>,
    processing: HashSet<T>,
    waiting: HashMap<T, Instant>,
    failures: HashMap<T, u32>,
    bucket: Bucket,
    shutting_down: bool,
}

impl<T: Clone + Eq + Hash> Inner<T> {
    /// `Typed.Add`.
    fn add(&mut self, item: T) -> bool {
        if self.shutting_down || self.dirty.contains(&item) {
            return false;
        }
        self.dirty.insert(item.clone());
        if self.processing.contains(&item) {
            return false;
        }
        self.queue.push_back(item);
        true
    }

    /// `MaxOfRateLimiter.When`: the larger of the per-item exponential
    /// back-off and the shared bucket's delay. Both limiters always advance.
    fn when(&mut self, item: &T) -> Duration {
        let exp = self.failures.entry(item.clone()).or_insert(0);
        let backoff = BASE_DELAY
            .checked_mul(2u32.saturating_pow(*exp))
            .map_or(MAX_DELAY, |d| d.min(MAX_DELAY));
        *exp += 1;
        let bucket = self.bucket.reserve(Instant::now());
        backoff.max(bucket)
    }
}

/// The real queue: `NewTypedRateLimitingQueueWithConfig(
/// DefaultTypedControllerRateLimiter, ...)`.
pub struct RateLimitingQueue<T> {
    inner: Mutex<Inner<T>>,
    notify: Notify,
}

impl<T: Clone + Eq + Hash + Send + 'static> RateLimitingQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                queue: VecDeque::new(),
                dirty: HashSet::new(),
                processing: HashSet::new(),
                waiting: HashMap::new(),
                failures: HashMap::new(),
                bucket: Bucket {
                    tokens: BUCKET_BURST,
                    last: Instant::now(),
                },
                shutting_down: false,
            }),
            notify: Notify::new(),
        }
    }

    /// Number of items ready for `get` (`Len`).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }

    /// `Len() == 0`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Clone + Eq + Hash + Send + 'static> Default for RateLimitingQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Eq + Hash + Send + Sync + 'static> WorkItemQueue<T> for RateLimitingQueue<T> {
    fn add(&self, item: T) {
        let added = self.inner.lock().unwrap().add(item);
        if added {
            self.notify.notify_one();
        }
    }

    fn add_after(&self, item: T, delay: Duration) {
        if delay.is_zero() {
            self.add(item);
            return;
        }
        let ready = Instant::now() + delay;
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.shutting_down {
                return;
            }
            // delaying_queue.go:340-349: only ever shorten an existing delay.
            match inner.waiting.get_mut(&item) {
                Some(existing) if *existing <= ready => return,
                Some(existing) => *existing = ready,
                None => {
                    inner.waiting.insert(item, ready);
                }
            }
        }
        // Wake a waiter so it re-computes its sleep deadline.
        self.notify.notify_one();
    }

    fn add_rate_limited(&self, item: T) {
        let delay = self.inner.lock().unwrap().when(&item);
        self.add_after(item, delay);
    }

    fn forget(&self, item: &T) {
        self.inner.lock().unwrap().failures.remove(item);
    }

    fn num_requeues(&self, item: &T) -> usize {
        self.inner
            .lock()
            .unwrap()
            .failures
            .get(item)
            .copied()
            .unwrap_or(0) as usize
    }

    fn get(&self) -> BoxFuture<'_, Option<T>> {
        Box::pin(async move {
            loop {
                let next_wake = {
                    let mut inner = self.inner.lock().unwrap();
                    let now = Instant::now();
                    let ready: Vec<T> = inner
                        .waiting
                        .iter()
                        .filter(|(_, at)| **at <= now)
                        .map(|(item, _)| item.clone())
                        .collect();
                    for item in ready {
                        inner.waiting.remove(&item);
                        inner.add(item);
                    }
                    if let Some(item) = inner.queue.pop_front() {
                        inner.processing.insert(item.clone());
                        inner.dirty.remove(&item);
                        // More may be ready for other workers.
                        if !inner.queue.is_empty() {
                            self.notify.notify_one();
                        }
                        return Some(item);
                    }
                    if inner.shutting_down {
                        return None;
                    }
                    inner.waiting.values().min().copied()
                };
                match next_wake {
                    Some(at) => {
                        let _ = tokio::time::timeout_at(at, self.notify.notified()).await;
                    }
                    None => self.notify.notified().await,
                }
            }
        })
    }

    fn done(&self, item: &T) {
        let requeued = {
            let mut inner = self.inner.lock().unwrap();
            inner.processing.remove(item);
            if inner.dirty.contains(item) {
                inner.queue.push_back(item.clone());
                true
            } else {
                false
            }
        };
        if requeued {
            self.notify.notify_one();
        }
    }

    fn shut_down(&self) {
        self.inner.lock().unwrap().shutting_down = true;
        // Every blocked worker has to observe the shutdown.
        for _ in 0..64 {
            self.notify.notify_one();
        }
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn deduplicates_and_requeues_dirty_item_after_done() {
        let q = RateLimitingQueue::<u32>::new();
        q.add(1);
        q.add(1);
        assert_eq!(q.len(), 1);
        assert_eq!(q.get().await, Some(1));
        // Added while processing: not handed out until Done.
        q.add(1);
        assert_eq!(q.len(), 0);
        q.done(&1);
        assert_eq!(q.len(), 1);
        assert_eq!(q.get().await, Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn add_after_keeps_the_earlier_deadline() {
        let q = Arc::new(RateLimitingQueue::<u32>::new());
        q.add_after(7, Duration::from_secs(60));
        q.add_after(7, Duration::from_secs(10));
        q.add_after(7, Duration::from_secs(30));
        let start = Instant::now();
        assert_eq!(q.get().await, Some(7));
        assert_eq!(start.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limited_requeue_backs_off_exponentially_and_forget_resets() {
        let q = RateLimitingQueue::<u32>::new();
        let start = Instant::now();
        q.add_rate_limited(1);
        assert_eq!(q.get().await, Some(1));
        q.done(&1);
        assert_eq!(start.elapsed(), Duration::from_millis(5));
        assert_eq!(q.num_requeues(&1), 1);
        q.add_rate_limited(1);
        assert_eq!(q.get().await, Some(1));
        assert_eq!(start.elapsed(), Duration::from_millis(15));
        q.forget(&1);
        assert_eq!(q.num_requeues(&1), 0);
    }

    #[tokio::test]
    async fn shut_down_releases_blocked_getters() {
        let q = Arc::new(RateLimitingQueue::<u32>::new());
        let q2 = q.clone();
        let waiter = tokio::spawn(async move { q2.get().await });
        tokio::task::yield_now().await;
        q.shut_down();
        assert_eq!(waiter.await.unwrap(), None);
    }
}
