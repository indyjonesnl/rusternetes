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
pub async fn tracked<F: Future>(f: F) -> F::Output {
    let started = Instant::now();
    let out = f.await;
    rusternetes_common::latency_trackers::track_storage_latency(started.elapsed());
    out
}

/// Time one list-item decode and add it to the request's decode latency
/// (`DecodeListItem`, `storage/etcd3/decoder.go:78-84`).
pub fn tracked_decode<R>(f: impl FnOnce() -> R) -> R {
    let started = Instant::now();
    let out = f();
    rusternetes_common::latency_trackers::track_decode_latency(started.elapsed());
    out
}
