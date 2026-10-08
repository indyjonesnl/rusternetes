//! Rate-limited timed queue used by the node lifecycle controller to throttle
//! NoExecute tainting per zone.
//!
//! Port of `pkg/controller/nodelifecycle/scheduler/rate_limited_queue.go`
//! (release-1.35): `TimedValue`, `TimedQueue`, `UniqueQueue` and
//! `RateLimitedTimedQueue`, plus the three `flowcontrol.RateLimiter`
//! implementations the controller uses (`NewTokenBucketRateLimiter`,
//! `NewFakeAlwaysRateLimiter`, `NewFakeNeverRateLimiter`,
//! `staging/src/k8s.io/client-go/util/flowcontrol/throttle.go`).
//!
//! Deliberate Rust deviations (expression, not mechanism): the Go `TimedQueue`
//! is a `container/heap`; here it is a `Vec` kept ordered by `ProcessAt` with
//! FIFO order among equal times, which yields the same `Head()`/`Get()`
//! sequence and additionally makes ties deterministic. `now` is an injectable
//! clock because Go swaps the package-level `now` var in tests.

// The controller-manager bin compiles its own copy of `controllers`; the ported
// queue API (`Get`, `Clear`, `Always`, inspection helpers) is only exercised by
// the lib and tests.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// `NodeEvictionPeriod`: how often the controller drains the taint queues
/// (rate_limited_queue.go:36).
pub const NODE_EVICTION_PERIOD: Duration = Duration::from_millis(100);

/// `EvictionRateLimiterBurst` (rate_limited_queue.go:39): burst of every
/// eviction rate limiter.
pub const EVICTION_RATE_LIMITER_BURST: u32 = 1;

/// Injectable `time.Now`.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

/// `TimedValue`: a value that should be processed at a designated time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedValue {
    pub value: String,
    /// UID could be anything that helps identify the value.
    pub uid: String,
    pub added_at: DateTime<Utc>,
    pub process_at: DateTime<Utc>,
}

/// The limiters `flowcontrol` hands the queue.
#[derive(Debug)]
pub enum RateLimiter {
    /// `NewFakeAlwaysRateLimiter`.
    Always,
    /// `NewFakeNeverRateLimiter`.
    Never,
    /// `NewTokenBucketRateLimiter(qps, burst)` (a `golang.org/x/time/rate`
    /// bucket: starts full, refills at `qps` tokens/second up to `burst`).
    TokenBucket {
        qps: f32,
        burst: u32,
        tokens: f64,
        last: Instant,
    },
}

impl RateLimiter {
    pub fn token_bucket(qps: f32, burst: u32) -> Self {
        RateLimiter::TokenBucket {
            qps,
            burst,
            tokens: burst as f64,
            last: Instant::now(),
        }
    }

    /// `QPS()`. throttle.go:148-150 and :178-180: both fakes report 1.
    pub fn qps(&self) -> f32 {
        match self {
            RateLimiter::Always | RateLimiter::Never => 1.0,
            RateLimiter::TokenBucket { qps, .. } => *qps,
        }
    }

    /// `TryAccept()`.
    pub fn try_accept(&mut self) -> bool {
        match self {
            RateLimiter::Always => true,
            RateLimiter::Never => false,
            RateLimiter::TokenBucket {
                qps,
                burst,
                tokens,
                last,
            } => {
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(*last).as_secs_f64();
                *last = now;
                *tokens = (*tokens + elapsed * f64::from(*qps)).min(f64::from(*burst));
                if *tokens >= 1.0 {
                    *tokens -= 1.0;
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Default)]
struct UniqueQueueInner {
    /// Ordered by `process_at`, FIFO among equals; `queue[0]` is the head.
    queue: Vec<TimedValue>,
    set: HashSet<String>,
}

impl UniqueQueueInner {
    fn push(&mut self, value: TimedValue) {
        let idx = self
            .queue
            .partition_point(|v| v.process_at <= value.process_at);
        self.queue.insert(idx, value);
    }

    fn position(&self, value: &str) -> Option<usize> {
        self.queue.iter().position(|v| v.value == value)
    }
}

/// `UniqueQueue`: a FIFO queue which additionally guarantees that any element
/// can be added only once until it is removed.
#[derive(Default)]
pub struct UniqueQueue {
    inner: Mutex<UniqueQueueInner>,
}

impl UniqueQueue {
    /// `Add`: add a new value if it wasn't added before, or was explicitly
    /// removed by `remove`. Returns true if the value was added.
    pub fn add(&self, value: TimedValue) -> bool {
        let mut q = self.inner.lock().unwrap();
        if q.set.contains(&value.value) {
            return false;
        }
        q.set.insert(value.value.clone());
        q.push(value);
        true
    }

    /// `Replace`: replace an existing value in the queue; does nothing if it
    /// is not queued. Returns true if the item was found.
    pub fn replace(&self, value: TimedValue) -> bool {
        let mut q = self.inner.lock().unwrap();
        match q.position(&value.value) {
            Some(i) => {
                q.queue.remove(i);
                q.push(value);
                true
            }
            None => false,
        }
    }

    /// `RemoveFromQueue`: remove from the queue but keep in the set, so it
    /// won't be added a second time. Returns true if something was removed.
    pub fn remove_from_queue(&self, value: &str) -> bool {
        let mut q = self.inner.lock().unwrap();
        if !q.set.contains(value) {
            return false;
        }
        match q.position(value) {
            Some(i) => {
                q.queue.remove(i);
                true
            }
            None => false,
        }
    }

    /// `Remove`: remove from the queue and the set, allowing a subsequent
    /// `add`. Returns false only if the value was not present at all.
    pub fn remove(&self, value: &str) -> bool {
        let mut q = self.inner.lock().unwrap();
        if !q.set.remove(value) {
            return false;
        }
        if let Some(i) = q.position(value) {
            q.queue.remove(i);
        }
        true
    }

    /// `Get`: the oldest added value that wasn't returned yet.
    pub fn get(&self) -> Option<TimedValue> {
        let mut q = self.inner.lock().unwrap();
        if q.queue.is_empty() {
            return None;
        }
        let result = q.queue.remove(0);
        q.set.remove(&result.value);
        Some(result)
    }

    /// `Head`: like `get` without removing.
    pub fn head(&self) -> Option<TimedValue> {
        self.inner.lock().unwrap().queue.first().cloned()
    }

    /// `Clear`: remove all items and the duplication-preventing set.
    pub fn clear(&self) {
        let mut q = self.inner.lock().unwrap();
        q.queue.clear();
        q.set.clear();
    }

    /// Queued values in order (test/inspection helper).
    pub fn values(&self) -> Vec<String> {
        let q = self.inner.lock().unwrap();
        q.queue.iter().map(|v| v.value.clone()).collect()
    }

    /// Values in the duplication-preventing set (test/inspection helper).
    pub fn set_values(&self) -> HashSet<String> {
        self.inner.lock().unwrap().set.clone()
    }
}

/// `RateLimitedTimedQueue`: a unique item priority queue ordered by the
/// expected next time of execution. It is also rate limited.
pub struct RateLimitedTimedQueue {
    queue: UniqueQueue,
    /// `limiterLock`: held for the whole of `try_process`, like upstream's
    /// `Try`, so a concurrent `swap_limiter` waits for the pass to finish.
    limiter: tokio::sync::Mutex<RateLimiter>,
    now: Clock,
}

impl RateLimitedTimedQueue {
    /// `NewRateLimitedTimedQueue`.
    pub fn new(limiter: RateLimiter) -> Self {
        Self::with_clock(limiter, system_clock())
    }

    pub fn with_clock(limiter: RateLimiter, now: Clock) -> Self {
        Self {
            queue: UniqueQueue::default(),
            limiter: tokio::sync::Mutex::new(limiter),
            now,
        }
    }

    /// `Try`: processes the queue. Ends prematurely if the RateLimiter forbids
    /// an action. Each value is processed once if `f` returns `(true, _)`,
    /// otherwise it is added back to the queue `wait` (+1ns, as Go's
    /// `wait + 1`) in the future. The same value is processed only once unless
    /// `remove` is explicitly called on it.
    pub async fn try_process<F, Fut>(&self, mut f: F)
    where
        F: FnMut(TimedValue) -> Fut,
        Fut: Future<Output = (bool, Duration)>,
    {
        let mut head = self.queue.head();
        let mut limiter = self.limiter.lock().await;
        while let Some(mut val) = head {
            // rate limit the queue checking
            if !limiter.try_accept() {
                // Try again later
                break;
            }

            let now = (self.now)();
            if now < val.process_at {
                break;
            }

            let (done, wait) = f(val.clone()).await;
            if !done {
                val.process_at = now
                    + chrono::Duration::from_std(wait).unwrap_or_default()
                    + chrono::Duration::nanoseconds(1);
                self.queue.replace(val);
            } else {
                self.queue.remove_from_queue(&val.value);
            }
            head = self.queue.head();
        }
    }

    /// `Add`: won't add the same value a second time if it was already added
    /// and not removed.
    pub fn add(&self, value: &str, uid: &str) -> bool {
        let now = (self.now)();
        self.queue.add(TimedValue {
            value: value.to_string(),
            uid: uid.to_string(),
            added_at: now,
            process_at: now,
        })
    }

    /// `Remove`: the value won't be processed until added again.
    pub fn remove(&self, value: &str) -> bool {
        self.queue.remove(value)
    }

    /// `Clear`.
    pub fn clear(&self) {
        self.queue.clear();
    }

    /// `SwapLimiter`: swap the limiter for a new QPS if the QPS differs.
    pub async fn swap_limiter(&self, new_qps: f32) {
        let mut limiter = self.limiter.lock().await;
        if limiter.qps() == new_qps {
            return;
        }
        let new_limiter = if new_qps <= 0.0 {
            RateLimiter::Never
        } else {
            let mut l = RateLimiter::token_bucket(new_qps, EVICTION_RATE_LIMITER_BURST);
            // If we're currently waiting on limiter, we drain the new one -
            // this is a good approach when Burst value is 1
            if !limiter.try_accept() {
                l.try_accept();
            }
            l
        };
        *limiter = new_limiter;
    }

    /// Current limiter QPS (`limiter.QPS()`).
    pub async fn limiter_qps(&self) -> f32 {
        self.limiter.lock().await.qps()
    }

    pub fn queued_values(&self) -> Vec<String> {
        self.queue.values()
    }

    pub fn set_values(&self) -> HashSet<String> {
        self.queue.set_values()
    }
}

#[cfg(test)]
mod tests {
    //! Ported from
    //! `pkg/controller/nodelifecycle/scheduler/rate_limited_queue_test.go`.
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn unix(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn always_queue() -> RateLimitedTimedQueue {
        RateLimitedTimedQueue::new(RateLimiter::Always)
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// TestUniqueQueueGet (:42): `now` ticks one second per call.
    #[test]
    fn unique_queue_get() {
        let tick = AtomicI64::new(0);
        let now = || unix(tick.fetch_add(1, Ordering::SeqCst));
        let queue = UniqueQueue::default();
        for (v, uid) in [("first", "11111"), ("second", "22222"), ("third", "33333")] {
            queue.add(TimedValue {
                value: v.into(),
                uid: uid.into(),
                added_at: now(),
                process_at: now(),
            });
        }
        assert_eq!(queue.values(), ["first", "second", "third"]);
        assert_eq!(queue.set_values(), set(&["first", "second", "third"]));

        queue.get();
        assert_eq!(queue.values(), ["second", "third"]);
        assert_eq!(queue.set_values(), set(&["second", "third"]));

        queue.get();
        assert_eq!(queue.values(), ["third"]);
        assert_eq!(queue.set_values(), set(&["third"]));

        queue.get();
        assert!(queue.values().is_empty());
        assert!(queue.set_values().is_empty());
    }

    /// TestAddNode (:92).
    #[test]
    fn add_node() {
        let evictor = always_queue();
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");
        assert_eq!(evictor.queued_values(), ["first", "second", "third"]);
        assert_eq!(evictor.set_values(), set(&["first", "second", "third"]));
    }

    /// TestDelNode (:115): removed values leave queue and set; re-adding the
    /// same value is accepted again.
    #[test]
    fn del_node() {
        let tick = Arc::new(AtomicI64::new(0));
        let t = tick.clone();
        let clock: Clock = Arc::new(move || unix(t.fetch_add(1, Ordering::SeqCst)));
        let evictor = RateLimitedTimedQueue::with_clock(RateLimiter::Always, clock);
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");
        assert!(evictor.remove("first"));
        assert_eq!(evictor.queued_values(), ["second", "third"]);
        assert_eq!(evictor.set_values(), set(&["second", "third"]));

        assert!(evictor.remove("second"));
        assert_eq!(evictor.queued_values(), ["third"]);
        assert!(evictor.remove("third"));
        assert!(evictor.queued_values().is_empty());
        assert!(evictor.set_values().is_empty());
        // `Remove` of an absent value is a no-op returning false.
        assert!(!evictor.remove("third"));
        assert!(evictor.add("first", "11111"));
    }

    /// TestTry (:190).
    #[tokio::test]
    async fn try_processes_each_remaining_value() {
        let evictor = always_queue();
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");
        evictor.remove("second");

        let seen = Mutex::new(HashSet::new());
        evictor
            .try_process(|v| {
                seen.lock().unwrap().insert(v.value);
                async { (true, Duration::ZERO) }
            })
            .await;
        assert_eq!(*seen.lock().unwrap(), set(&["first", "third"]));
    }

    /// TestTryOrdering (:213): the clock advances 1ms on every call unless
    /// `delay` holds it; a `false` result requeues at now + wait + 1ns.
    #[tokio::test]
    async fn try_ordering() {
        let current = Arc::new(Mutex::new(unix(0)));
        let delay = Arc::new(Mutex::new(0u32));
        let (c, d) = (current.clone(), delay.clone());
        let clock: Clock = Arc::new(move || {
            let mut delay = d.lock().unwrap();
            let mut cur = c.lock().unwrap();
            if *delay > 0 {
                *delay -= 1;
            } else {
                *cur += chrono::Duration::milliseconds(1);
            }
            *cur
        });
        let ms = |n: i64| unix(0) + chrono::Duration::milliseconds(n);
        let evictor = RateLimitedTimedQueue::with_clock(RateLimiter::Always, clock);
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");

        let order = Mutex::new(Vec::<String>::new());
        let count = Mutex::new(0);
        let has_queued = Mutex::new(false);
        evictor
            .try_process(|value| {
                *count.lock().unwrap() += 1;
                let mut result = (true, Duration::ZERO);
                match value.value.as_str() {
                    "first" => assert_eq!(value.added_at, ms(1)),
                    "second" => {
                        assert_eq!(value.added_at, ms(2));
                        let mut q = has_queued.lock().unwrap();
                        if *q {
                            assert_eq!(value.process_at, ms(6));
                        } else {
                            *q = true;
                            *delay.lock().unwrap() = 1;
                            result = (false, Duration::from_millis(2));
                        }
                    }
                    "third" => assert_eq!(value.added_at, ms(3)),
                    _ => unreachable!(),
                }
                if result.0 {
                    order.lock().unwrap().push(value.value);
                }
                async move { result }
            })
            .await;
        assert_eq!(*order.lock().unwrap(), ["first", "third"]);
        assert_eq!(*count.lock().unwrap(), 3);
    }

    /// TestTryRemovingWhileTry (:279): removing a value while it is being
    /// processed means it is not re-queued when the action asks for a retry.
    #[tokio::test]
    async fn try_removing_while_try() {
        let evictor = Arc::new(always_queue());
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");

        let order = Mutex::new(Vec::<String>::new());
        let count = Mutex::new(0);
        let queued = Mutex::new(false);
        let e = evictor.clone();
        evictor
            .try_process(|value| {
                *count.lock().unwrap() += 1;
                let retry = {
                    let mut q = queued.lock().unwrap();
                    if !*q && value.value == "second" {
                        *q = true;
                        true
                    } else {
                        false
                    }
                };
                let result = if retry {
                    // the concurrent `Remove("second")` of the Go test
                    e.remove("second");
                    (false, Duration::from_millis(1))
                } else {
                    order.lock().unwrap().push(value.value);
                    (true, Duration::ZERO)
                };
                async move { result }
            })
            .await;
        assert_eq!(*order.lock().unwrap(), ["first", "third"]);
        assert_eq!(*count.lock().unwrap(), 3);
    }

    /// TestClear (:325).
    #[test]
    fn clear() {
        let evictor = always_queue();
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");
        evictor.clear();
        assert!(evictor.queued_values().is_empty());
    }

    /// TestSwapLimiter (:338).
    #[tokio::test]
    async fn swap_limiter() {
        let evictor = always_queue();
        assert_eq!(evictor.limiter_qps().await, RateLimiter::Always.qps());

        evictor.swap_limiter(0.0).await;
        assert_eq!(evictor.limiter_qps().await, RateLimiter::Never.qps());

        evictor.swap_limiter(5.5).await;
        assert_eq!(evictor.limiter_qps().await, 5.5);

        // Same QPS: the limiter is not swapped. A swap would build a fresh
        // bucket; observable here as the (drained) bucket staying drained.
        {
            let mut l = evictor.limiter.lock().await;
            assert!(!matches!(&*l, RateLimiter::Never));
            l.try_accept(); // consume the single burst token
        }
        evictor.swap_limiter(5.5).await;
        let mut l = evictor.limiter.lock().await;
        assert!(
            !l.try_accept(),
            "same-QPS swap must keep the existing bucket"
        );
    }

    /// TestAddAfterTry (:367): a processed value stays in the set until
    /// removed, so it cannot be re-added.
    #[tokio::test]
    async fn add_after_try() {
        let evictor = always_queue();
        evictor.add("first", "11111");
        evictor.add("second", "22222");
        evictor.add("third", "33333");
        evictor.remove("second");

        let seen = Mutex::new(HashSet::new());
        evictor
            .try_process(|v| {
                seen.lock().unwrap().insert(v.value);
                async { (true, Duration::ZERO) }
            })
            .await;
        assert_eq!(*seen.lock().unwrap(), set(&["first", "third"]));

        assert!(!evictor.add("first", "11111"), "first is still in the set");
        evictor.remove("first");
        assert!(evictor.add("first", "11111"));
    }

    /// Token bucket with burst 1 admits exactly one action, then refuses until
    /// the bucket refills (`EvictionRateLimiterBurst`).
    #[tokio::test]
    async fn token_bucket_burst_one_admits_a_single_item() {
        let evictor =
            RateLimitedTimedQueue::new(RateLimiter::token_bucket(0.1, EVICTION_RATE_LIMITER_BURST));
        for n in ["a", "b", "c"] {
            evictor.add(n, n);
        }
        let seen = Mutex::new(Vec::<String>::new());
        evictor
            .try_process(|v| {
                seen.lock().unwrap().push(v.value);
                async { (true, Duration::ZERO) }
            })
            .await;
        assert_eq!(*seen.lock().unwrap(), ["a"]);
    }

    /// A Never limiter admits nothing (full disruption).
    #[tokio::test]
    async fn never_limiter_admits_nothing() {
        let evictor = RateLimitedTimedQueue::new(RateLimiter::Never);
        evictor.add("a", "a");
        let seen = Mutex::new(0);
        evictor
            .try_process(|_| {
                *seen.lock().unwrap() += 1;
                async { (true, Duration::ZERO) }
            })
            .await;
        assert_eq!(*seen.lock().unwrap(), 0);
    }
}
