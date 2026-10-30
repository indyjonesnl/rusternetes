//! Unit tests moved from `src/controllers/node_lifecycle_queue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use std::collections::HashSet;
    pub use std::future::Future;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
}

mod tests {
    //! Ported from
    //! `pkg/controller/nodelifecycle/scheduler/rate_limited_queue_test.go`.
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::node_lifecycle_queue::*;
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
