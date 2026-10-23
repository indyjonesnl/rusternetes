//! Storage round-trip latency tracking.
//!
//! Port of `storage/etcd3/latency_tracker.go` (`clientV3KVLatencyTracker`):
//! every KV round trip (`Put`/`Get`/`Delete`/`Do`/`Txn.Commit`) is timed and
//! fed to `endpointsrequest.TrackStorageLatency`, which sums into the
//! request's `LatencyTrackers.StorageLatency`. A request making N round trips
//! accumulates the sum. `Compact` and watches are not request-bound and are
//! not tracked, and lease grants are not KV calls either.
//!
//! Outside a request (controllers, background tasks) no trackers are
//! installed and [`tracked`] only runs the future, as upstream's
//! `TrackStorageLatency` does when `LatencyTrackersFrom` returns `!ok`.

use std::future::Future;
use std::time::Instant;

/// Time one storage round trip and add it to the request's storage latency.
pub(crate) async fn tracked<F: Future>(f: F) -> F::Output {
    let started = Instant::now();
    let out = f.await;
    rusternetes_common::latency_trackers::track_storage_latency(started.elapsed());
    out
}

/// Time one list-item decode and add it to the request's decode latency
/// (`DecodeListItem`, `storage/etcd3/decoder.go:78-84`).
pub(crate) fn tracked_decode<R>(f: impl FnOnce() -> R) -> R {
    let started = Instant::now();
    let out = f();
    rusternetes_common::latency_trackers::track_decode_latency(started.elapsed());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};
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
        use crate::busy::{retry_busy, RetryPolicy};
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
