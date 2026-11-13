//! Port of the cacher's bookmark time buckets:
//! `watcherBookmarkTimeBuckets` (`cacher.go:194-250`:
//! `newTimeBucketWatchers`, `addWatcherThreadUnsafe`,
//! `popExpiredWatchersThreadUnsafe`) and `defaultBookmarkFrequency`
//! (`cacher.go:75`). The `cacheWatcher` bookmark state machine they schedule
//! (`nextBookmarkTime` etc., `cache_watcher.go:36-50`, `:215-327`) is on
//! [`CacheWatcher`].
//!
//! This is the scheduling half only (issue #3826): nothing dispatches
//! bookmarks from it yet. Bookmarks are still produced by the watch handlers;
//! moving production into the cacher is the follow-up that consumes this.
//!
//! As upstream, watchers whose next bookmark falls within the same second share
//! a bucket; a bookmark scheduled for X is sent within (X-1, X+1).

use crate::cache_watcher::{CacheWatcher, NextBookmark};
use crate::flow_control_object_count::PassiveClock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `defaultBookmarkFrequency` (`cacher.go:75`).
pub const DEFAULT_BOOKMARK_FREQUENCY: Duration = Duration::from_secs(60);

/// `watcherBookmarkTimeBuckets`. `clock` is `clock.Clock`; the repo's
/// [`PassiveClock`] (`Now`; `Since` is `now - t`) is what both reads need.
pub struct TimeBucketWatchers {
    /// The key is the number of seconds since `create_time`.
    watchers_buckets: HashMap<i64, Vec<Arc<CacheWatcher>>>,
    create_time: Instant,
    start_bucket_id: i64,
    clock: Arc<dyn PassiveClock>,
    bookmark_frequency: Duration,
}

impl TimeBucketWatchers {
    /// `newTimeBucketWatchers`.
    pub fn new(clock: Arc<dyn PassiveClock>, bookmark_frequency: Duration) -> Self {
        Self {
            watchers_buckets: HashMap::new(),
            create_time: clock.now(),
            start_bucket_id: 0,
            clock,
            bookmark_frequency,
        }
    }

    /// `len(t.watchersBuckets)`.
    pub fn bucket_count(&self) -> usize {
        self.watchers_buckets.len()
    }

    /// `addWatcherThreadUnsafe`: add to the bucket of its next bookmark time; a
    /// time before the start goes to the first bucket.
    pub fn add_watcher_thread_unsafe(&mut self, w: Arc<CacheWatcher>) -> bool {
        // The returned time can be before `create_time`, especially when
        // `next_bookmark_time` gives the zero value of type Time, so the
        // bucket ID can be negative upstream; clamped to `start_bucket_id`.
        let bucket_id = match w.next_bookmark_time(self.clock.now(), self.bookmark_frequency) {
            NextBookmark::Never => return false,
            NextBookmark::Immediately => self.start_bucket_id,
            NextBookmark::At(t) => (t.saturating_duration_since(self.create_time).as_secs() as i64)
                .max(self.start_bucket_id),
        };
        self.watchers_buckets.entry(bucket_id).or_default().push(w);
        true
    }

    /// `popExpiredWatchersThreadUnsafe`: the buckets up to the current second.
    pub fn pop_expired_watchers_thread_unsafe(&mut self) -> Vec<Vec<Arc<CacheWatcher>>> {
        let current_bucket_id = self
            .clock
            .now()
            .saturating_duration_since(self.create_time)
            .as_secs() as i64;
        // There should be one or two elements in almost all cases.
        let mut expired = Vec::with_capacity(2);
        while self.start_bucket_id <= current_bucket_id {
            if let Some(ws) = self.watchers_buckets.remove(&self.start_bucket_id) {
                expired.push(ws);
            }
            self.start_bucket_id += 1;
        }
        expired
    }
}
