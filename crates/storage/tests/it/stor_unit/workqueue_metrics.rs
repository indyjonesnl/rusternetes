//! `workqueue_*` metrics of a named `WorkQueue`, against the behaviour of
//! client-go `util/workqueue/metrics.go` (release-1.35).
#![allow(unused_imports, dead_code)]

mod tests {
    use rusternetes_storage::WorkQueue;
    use std::time::Duration;

    /// One series from the default registry's text exposition, e.g.
    /// `series("workqueue_depth", "q")`. A series that does not exist yet is 0.
    fn series(metric: &str, queue: &str) -> f64 {
        use prometheus::Encoder;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&prometheus::default_registry().gather(), &mut buf)
            .unwrap();
        let prefix = format!("{metric}{{name=\"{queue}\"}} ");
        String::from_utf8(buf)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix(&prefix).map(|v| v.parse().unwrap()))
            .unwrap_or(0.0)
    }

    fn depth(q: &str) -> f64 {
        series("workqueue_depth", q)
    }
    fn adds(q: &str) -> f64 {
        series("workqueue_adds_total", q)
    }
    fn latency_count(q: &str) -> f64 {
        series("workqueue_queue_duration_seconds_count", q)
    }
    fn work_count(q: &str) -> f64 {
        series("workqueue_work_duration_seconds_count", q)
    }
    fn retries(q: &str) -> f64 {
        series("workqueue_retries_total", q)
    }

    /// `add` counts once per distinct queued key (a duplicate returns before
    /// `metrics.add`, queue.go:230-235); `get` takes it off the depth and
    /// records its time in the queue; `done` records the work duration.
    #[tokio::test]
    async fn add_get_done_update_depth_adds_and_durations() {
        let n = "wqm_basic";
        let q = WorkQueue::named(n);
        q.add("a".into()).await;
        q.add("a".into()).await;
        q.add("b".into()).await;
        assert_eq!((adds(n), depth(n)), (2.0, 2.0));

        let a = q.get().await.unwrap();
        assert_eq!((depth(n), latency_count(n)), (1.0, 1.0));
        assert_eq!(work_count(n), 0.0);
        q.done(&a).await;
        assert_eq!(work_count(n), 1.0);

        let b = q.get().await.unwrap();
        q.done(&b).await;
        assert_eq!((depth(n), latency_count(n), work_count(n)), (0.0, 2.0, 2.0));
    }

    /// A key added while it is being processed counts as an add and a unit of
    /// depth right away; `done` re-queues it without counting it again.
    #[tokio::test]
    async fn add_while_processing_counts_once() {
        let n = "wqm_dirty";
        let q = WorkQueue::named(n);
        q.add("a".into()).await;
        let a = q.get().await.unwrap();
        // Outlast the re-process cooldown so `done` re-queues directly.
        tokio::time::sleep(Duration::from_millis(250)).await;
        q.add("a".into()).await;
        assert_eq!((adds(n), depth(n)), (2.0, 1.0));
        q.done(&a).await;
        assert_eq!((adds(n), depth(n)), (2.0, 1.0));
        let a = q.get().await.unwrap();
        q.done(&a).await;
        assert_eq!(depth(n), 0.0);
    }

    /// Our re-process cooldown parks a re-dirtied key in `delayed` instead of
    /// the queue. Depth must still balance and the key must not be counted as
    /// a second add when it comes back.
    #[tokio::test]
    async fn cooldown_requeue_keeps_depth_balanced() {
        let n = "wqm_cooldown";
        let q = WorkQueue::named(n);
        q.add("a".into()).await;
        let a = q.get().await.unwrap();
        q.add("a".into()).await;
        assert_eq!((adds(n), depth(n)), (2.0, 1.0));
        // Immediately done: inside the cooldown, so the key is held back.
        q.done(&a).await;
        assert_eq!(depth(n), 0.0, "held-back key is not in the queue");
        let a = tokio::time::timeout(Duration::from_secs(5), q.get())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((adds(n), depth(n)), (2.0, 0.0));
        assert_eq!(latency_count(n), 2.0);
        q.done(&a).await;
    }

    /// Every `AddAfter` is a retry (delaying_queue.go:255), including the
    /// rate-limited re-queue.
    #[tokio::test]
    async fn add_after_and_rate_limited_requeue_count_retries() {
        let n = "wqm_retries";
        let q = WorkQueue::named(n);
        q.add_after("a".into(), Duration::from_millis(1)).await;
        assert_eq!(retries(n), 1.0);
        q.requeue_rate_limited("b".into()).await;
        assert_eq!(retries(n), 2.0);
    }

    /// A delayed key becomes an add when it is promoted into the queue.
    #[tokio::test]
    async fn delayed_key_counts_as_an_add_when_it_matures() {
        let n = "wqm_delayed";
        let q = WorkQueue::named(n);
        q.add_after("a".into(), Duration::from_millis(20)).await;
        assert_eq!((adds(n), depth(n)), (0.0, 0.0));
        let a = q.get().await.unwrap();
        assert_eq!((adds(n), depth(n)), (1.0, 0.0));
        q.done(&a).await;
    }

    /// `newQueueMetrics`: a queue without a name records nothing.
    #[tokio::test]
    async fn unnamed_queue_records_nothing() {
        let q = WorkQueue::named("");
        q.add("a".into()).await;
        let a = q.get().await.unwrap();
        q.done(&a).await;
        assert_eq!(series("workqueue_adds_total", ""), 0.0);
        let q = WorkQueue::new();
        q.add("b".into()).await;
        assert_eq!(series("workqueue_adds_total", ""), 0.0);
    }

    /// `updateUnfinishedWorkLoop`: the gauges follow the items in processing
    /// and drop back to zero once they are done.
    #[tokio::test]
    async fn unfinished_work_gauges_follow_processing_items() {
        let n = "wqm_unfinished";
        let q = WorkQueue::named(n);
        q.add("a".into()).await;
        let a = q.get().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let unfinished = series("workqueue_unfinished_work_seconds", n);
        let longest = series("workqueue_longest_running_processor_seconds", n);
        assert!((0.5..=3.0).contains(&unfinished), "{unfinished}");
        assert!((0.5..=3.0).contains(&longest), "{longest}");
        q.done(&a).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(series("workqueue_unfinished_work_seconds", n), 0.0);
        assert_eq!(
            series("workqueue_longest_running_processor_seconds", n),
            0.0
        );
    }
}
