//! Unit tests moved from `src/workqueue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_storage::WatchEvent;
    pub use std::collections::{HashMap, HashSet, VecDeque};
    pub use std::sync::Arc;
    pub use std::time::{Duration, Instant};
    pub use tokio::sync::{Mutex, Notify};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::workqueue::*;

    #[tokio::test]
    async fn test_add_and_get() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        q.add("key2".into()).await;
        assert_eq!(q.get().await, Some("key1".into()));
        assert_eq!(q.get().await, Some("key2".into()));
    }

    #[tokio::test]
    async fn test_deduplication_while_queued() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        q.add("key1".into()).await; // duplicate — should be no-op
        q.add("key2".into()).await;
        assert_eq!(q.get().await, Some("key1".into()));
        assert_eq!(q.get().await, Some("key2".into()));
    }

    #[tokio::test]
    async fn test_requeue_on_done_when_dirty() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        let key = q.get().await.unwrap();
        assert_eq!(key, "key1");

        // Adding same key while processing marks it dirty
        q.add("key1".into()).await;
        // Not in queue yet (still processing)
        assert_eq!(q.len().await, 0);

        // done() delays re-queue by MIN_REPROCESS_INTERVAL to prevent
        // self-write feedback loops. The key goes into the delayed map
        // rather than immediately into the queue.
        q.done(&key).await;

        // Wait for MIN_REPROCESS_INTERVAL to elapse so the key becomes available
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Should be able to get it after the cooldown
        let key2 = q.get().await.unwrap();
        assert_eq!(key2, "key1");
        q.done(&key2).await;
    }

    #[tokio::test]
    async fn test_done_removes_from_processing() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        let key = q.get().await.unwrap();
        q.done(&key).await;

        // Now we can add it again
        q.add("key1".into()).await;
        assert_eq!(q.len().await, 1);
    }

    #[tokio::test]
    async fn test_forget_clears_failures() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        let key = q.get().await.unwrap();

        // Simulate failure
        q.requeue_rate_limited(key.clone()).await;
        q.done(&key).await;

        // Forget should clear the failure count
        q.forget(&key).await;

        // Verify by checking that a new requeue uses base delay (not 2x)
        let inner = q.inner.lock().await;
        assert!(!inner.failures.contains_key(&key));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let q = WorkQueue::new();
        q.shutdown().await;
        assert_eq!(q.get().await, None);
    }

    #[tokio::test]
    async fn test_shutdown_drains_existing() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        q.shutdown().await;
        // Should still be able to get items already in queue
        assert_eq!(q.get().await, Some("key1".into()));
        q.done("key1").await;
        // Then returns None
        assert_eq!(q.get().await, None);
    }

    #[tokio::test]
    async fn test_backoff_duration() {
        let base = Duration::from_millis(5);
        let max = Duration::from_secs(1000);
        assert_eq!(backoff_duration(0, base, max), Duration::ZERO);
        assert_eq!(backoff_duration(1, base, max), Duration::from_millis(5));
        assert_eq!(backoff_duration(2, base, max), Duration::from_millis(10));
        assert_eq!(backoff_duration(3, base, max), Duration::from_millis(20));
        assert_eq!(backoff_duration(4, base, max), Duration::from_millis(40));
        assert_eq!(backoff_duration(10, base, max), Duration::from_millis(2560));
        assert_eq!(backoff_duration(20, base, max), Duration::from_secs(1000)); // capped
    }

    /// delaying_queue.go `insert`: "if the entry already exists, update the
    /// time only if it would cause the item to be queued sooner".
    #[tokio::test]
    async fn add_after_keeps_the_earlier_deadline() {
        let q = WorkQueue::new();
        q.add_after("key1".into(), Duration::from_millis(100)).await;
        q.add_after("key1".into(), Duration::from_secs(60)).await;
        let got = tokio::time::timeout(Duration::from_secs(3), q.get())
            .await
            .expect("the earlier deadline must stand");
        assert_eq!(got.as_deref(), Some("key1"));
    }

    /// delaying_queue.go `waitingLoop` hands a ready entry to `Add`, which
    /// marks a key being processed dirty so `Done` re-queues it; the wake-up
    /// must not be dropped.
    #[tokio::test]
    async fn a_delayed_key_that_matures_while_processing_is_not_lost() {
        let q = WorkQueue::new();
        q.add("key1".into()).await;
        let key = q.get().await.unwrap();
        q.add_after(key.clone(), Duration::from_millis(50)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Promotion happens inside get(); nothing else is queued, so poll once.
        assert!(tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .is_err());
        q.done(&key).await;
        let got = tokio::time::timeout(Duration::from_secs(3), q.get())
            .await
            .expect("re-queued by done()");
        assert_eq!(got.as_deref(), Some("key1"));
    }

    #[tokio::test]
    async fn test_add_after_delay() {
        let q = WorkQueue::new();
        q.add_after("key1".into(), Duration::from_millis(50)).await;

        // Should not be available immediately
        assert_eq!(q.len().await, 0);

        // Wait for it to become available
        tokio::time::sleep(Duration::from_millis(100)).await;

        // get() should promote it
        let key = q.get().await.unwrap();
        assert_eq!(key, "key1");
        q.done(&key).await;
    }

    #[tokio::test]
    async fn test_extract_key_namespaced() {
        let event = WatchEvent::Modified(
            "/registry/deployments/default/my-deploy".into(),
            "{}".into(),
        );
        assert_eq!(extract_key(&event), "deployments/default/my-deploy");
    }

    #[tokio::test]
    async fn test_extract_key_cluster_scoped() {
        let event = WatchEvent::Added("/registry/namespaces/kube-system".into(), "{}".into());
        assert_eq!(extract_key(&event), "namespaces/kube-system");
    }

    #[tokio::test]
    async fn test_extract_key_deleted() {
        let event = WatchEvent::Deleted("/registry/pods/test-ns/test-pod".into(), "{}".into());
        assert_eq!(extract_key(&event), "pods/test-ns/test-pod");
    }

    #[tokio::test]
    async fn test_concurrent_get_blocks() {
        let q = WorkQueue::new();
        let q2 = q.clone();

        // Spawn a task that waits for an item
        let handle = tokio::spawn(async move { q2.get().await });

        // Give the task time to block
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Add an item — should unblock the waiting get()
        q.add("key1".into()).await;

        let result = handle.await.unwrap();
        assert_eq!(result, Some("key1".into()));
    }

    #[tokio::test]
    async fn test_requeue_rate_limited_escalates() {
        let q = WorkQueue::with_config(WorkQueueConfig {
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
        });

        q.add("key1".into()).await;
        let key = q.get().await.unwrap();

        // First failure: 10ms delay
        q.requeue_rate_limited(key.clone()).await;
        q.done(&key).await;

        // Should be available after delay
        tokio::time::sleep(Duration::from_millis(20)).await;
        let key = q.get().await.unwrap();
        assert_eq!(key, "key1");

        // Second failure: 20ms delay
        q.requeue_rate_limited(key.clone()).await;
        q.done(&key).await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        let key = q.get().await.unwrap();
        assert_eq!(key, "key1");
        q.done(&key).await;
    }

    /// A pool of workers must drain every key promoted out of `delayed` in one
    /// pass. Before the promotion path woke the pool, the promoting worker took
    /// one key and the remaining N-1 sat unclaimed while every other worker
    /// stayed parked in `get()`'s unbounded `notified().await` — no further
    /// `add()` was coming, so the queue simply stalled with work in it.
    #[tokio::test]
    async fn worker_pool_drains_every_promoted_delayed_key() {
        const KEYS: usize = 8;
        const WORKERS: usize = 5;

        let q = WorkQueue::new();
        // Land all keys in `delayed` with a deadline that has passed by the
        // time the workers look, so ONE `get()` promotes the whole batch.
        for i in 0..KEYS {
            q.add_after(format!("key{i}"), Duration::from_millis(50))
                .await;
        }

        let processed = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handles = Vec::new();
        for _ in 0..WORKERS {
            let q = q.clone();
            let processed = Arc::clone(&processed);
            handles.push(tokio::spawn(async move {
                while let Some(key) = q.get().await {
                    processed.lock().await.push(key.clone());
                    q.forget(&key).await;
                    q.done(&key).await;
                }
            }));
        }

        // Generous relative to the 50ms delay: the point is that the keys
        // arrive at all, not how fast. A stall here never completes.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if processed.lock().await.len() >= KEYS {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "only {} of {KEYS} promoted keys were processed — the pool stalled",
                processed.lock().await.len()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        q.shutdown().await;
        for h in handles {
            let _ = h.await;
        }

        let mut seen = processed.lock().await.clone();
        seen.sort();
        seen.dedup();
        assert_eq!(
            seen.len(),
            KEYS,
            "every promoted key should be processed exactly once, got {seen:?}"
        );
    }

    /// The `processing` set is what makes a pool safe: two workers must never
    /// hold the same key at once. Ports client-go's dirty/processing split
    /// (queue.go:227 `Add` returns early when `processing.Has(item)`, and
    /// queue.go:289 `Done` re-pushes only then).
    #[tokio::test]
    async fn a_key_re_added_while_processing_is_not_handed_to_a_second_worker() {
        let q = WorkQueue::new();
        q.add("key1".to_string()).await;

        let held = q.get().await.unwrap();
        assert_eq!(held, "key1");

        // Re-add while the first worker still holds it.
        q.add("key1".to_string()).await;

        // A second worker must not receive the same key.
        let second = tokio::time::timeout(Duration::from_millis(200), q.get()).await;
        assert!(
            second.is_err(),
            "a key in `processing` was handed to a second worker: {second:?}"
        );

        // Once released, the re-add is honoured.
        q.done(&held).await;
        let again = tokio::time::timeout(Duration::from_secs(2), q.get())
            .await
            .expect("the re-added key should be requeued by done()")
            .expect("queue should not be shut down");
        assert_eq!(again, "key1");
    }
}
