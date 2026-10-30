//! Unit tests moved from `src/controllers/taint_eviction_timed_workers.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use futures::future::BoxFuture;
    pub use std::collections::HashMap;
    pub use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use tokio::sync::Notify;
    pub use tracing::{debug, error};
}

mod tests {
    //! Cases ported from `timed_workers_test.go`: TestExecute (:30),
    //! TestExecuteDelayed (:53), TestCancel (:85), TestCancelAndRead (:119).
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::taint_eviction_timed_workers::*;
    use std::time::Duration;

    fn counting_queue() -> (TimedWorkerQueue, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let queue = TimedWorkerQueue::new(Arc::new(move |_fire_at, _args| {
            let c = Arc::clone(&c);
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }));
        (queue, count)
    }

    #[tokio::test]
    async fn execute_runs_every_item_with_no_delay() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        for i in 0..5 {
            queue.add_work(WorkArgs::new(&format!("p{i}"), "ns"), now, now);
        }
        queue.wait_idle().await;
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn execute_delayed_runs_after_the_delay() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        for i in 0..5 {
            queue.add_work(
                WorkArgs::new(&format!("p{i}"), "ns"),
                now,
                now + chrono::Duration::milliseconds(200),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0, "fired before its delay");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn cancel_work_stops_a_scheduled_item() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        for i in 0..5 {
            queue.add_work(
                WorkArgs::new(&format!("p{i}"), "ns"),
                now,
                now + chrono::Duration::milliseconds(200),
            );
        }
        assert!(queue.cancel_work("ns/p0"));
        assert!(queue.cancel_work("ns/p3"));
        assert!(!queue.cancel_work("ns/p3"), "second cancel finds nothing");
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancel_and_read_get_worker_unsafe() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        let fire = now + chrono::Duration::milliseconds(200);
        queue.add_work(WorkArgs::new("p0", "ns"), now, fire);
        queue.add_work(WorkArgs::new("p1", "ns"), now, fire);
        let w = queue.get_worker_unsafe("ns/p0").expect("scheduled");
        assert_eq!(w.fire_at, fire);
        assert!(queue.cancel_work("ns/p0"));
        assert!(queue.get_worker_unsafe("ns/p0").is_none());
        // Adding again after cancel works.
        queue.add_work(WorkArgs::new("p0", "ns"), now, fire);
        assert!(queue.get_worker_unsafe("ns/p0").is_some());
        queue.cancel_and_wait().await;
        assert!(queue.get_worker_unsafe("ns/p1").is_none());
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0, "cancel_and_wait stops all");
    }

    #[tokio::test]
    async fn add_work_keeps_the_existing_item() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        queue.add_work(
            WorkArgs::new("p", "ns"),
            now,
            now + chrono::Duration::milliseconds(100),
        );
        // Second add for the same key is skipped (timed_workers.go:153-156).
        queue.add_work(
            WorkArgs::new("p", "ns"),
            now,
            now + chrono::Duration::milliseconds(5000),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn update_work_replaces_unless_same_fire_time() {
        let (queue, count) = counting_queue();
        let now = Utc::now();
        let late = now + chrono::Duration::milliseconds(5000);
        queue.add_work(WorkArgs::new("p", "ns"), now, late);
        // Same fireAt: kept (timed_workers.go:175).
        queue.update_work(WorkArgs::new("p", "ns"), now, late);
        assert_eq!(queue.get_worker_unsafe("ns/p").unwrap().fire_at, late);
        // Different fireAt: replaced (:179-181).
        queue.update_work(
            WorkArgs::new("p", "ns"),
            now,
            now + chrono::Duration::milliseconds(100),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
