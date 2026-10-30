//! Unit tests moved from `src/controllers/typed_workqueue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::future::BoxFuture;
    pub use std::collections::{HashMap, HashSet, VecDeque};
    pub use std::hash::Hash;
    pub use std::sync::Mutex;
    pub use std::time::Duration;
    pub use tokio::sync::Notify;
    pub use tokio::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::typed_workqueue::*;
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
