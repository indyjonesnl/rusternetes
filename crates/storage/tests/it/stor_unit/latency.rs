//! Unit tests moved from `src/latency.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::future::Future;
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};
    use rusternetes_storage::latency::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn round_trips_sum_into_storage_tracker() {
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            tracked(tokio::time::sleep(Duration::from_millis(20))).await;
            tracked(tokio::time::sleep(Duration::from_millis(20))).await;
        })
        .await;
        assert!(t.storage.get_latency() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn list_item_decode_sums_into_decode_tracker() {
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            tracked_decode(|| std::thread::sleep(Duration::from_millis(10)));
            tracked_decode(|| std::thread::sleep(Duration::from_millis(10)));
        })
        .await;
        assert!(t.decode.get_latency() >= Duration::from_millis(20));
        assert_eq!(t.storage.get_latency(), Duration::ZERO);
    }

    #[tokio::test]
    async fn without_trackers_it_just_runs() {
        assert_eq!(tracked(async { 7 }).await, 7);
    }

    #[tokio::test]
    async fn retry_busy_tracks_each_attempt_but_not_backoff() {
        use rusternetes_storage::busy::{retry_busy, RetryPolicy};
        let t = Arc::new(LatencyTrackers::new());
        let mut n = 0;
        let r: Result<(), String> = with_latency_trackers(t.clone(), async {
            retry_busy(
                RetryPolicy {
                    attempts: 3,
                    step: Duration::from_millis(100),
                },
                || {
                    n += 1;
                    let busy = n < 3;
                    async move {
                        if busy {
                            Err("database is locked".to_string())
                        } else {
                            Ok(())
                        }
                    }
                },
            )
            .await
        })
        .await;
        assert!(r.is_ok());
        // Backoff slept 100ms + 200ms; the attempts themselves took ~0.
        assert!(t.storage.get_latency() < Duration::from_millis(50));
    }
}
