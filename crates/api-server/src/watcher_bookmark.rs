//! Port of the cacher's bookmark scheduling:
//! `watcherBookmarkTimeBuckets` (`cacher.go:194-250`) and the
//! `cacheWatcher` bookmark states (`cache_watcher.go:36-50`,
//! `nextBookmarkTime` `:240-262`, `setBookmarkAfterResourceVersion` and the
//! `*BookmarkAfterRv*` helpers).
//!
//! This is the scheduling half only (issue #3826): nothing dispatches
//! bookmarks from it yet. Bookmarks are still produced by the watch handlers;
//! moving production into the cacher is the follow-up that consumes this.
//!
//! As upstream, watchers whose next bookmark falls within the same second share
//! a bucket; a bookmark scheduled for X is sent within (X-1, X+1). The clock is
//! explicit: methods take `now` where upstream reads `clock.Now()`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `defaultBookmarkFrequency` (`cacher.go:75`).
pub const DEFAULT_BOOKMARK_FREQUENCY: Duration = Duration::from_secs(60);

/// `cacheWatcherWaitingForBookmark` / `cacheWatcherBookmarkReceived` /
/// `cacheWatcherBookmarkSent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookmarkState {
    /// Waiting for a bookmark event with a specific RV.
    WaitingForBookmark,
    /// A bookmark with the required RV was received.
    BookmarkReceived,
    /// A bookmark was sent to the client.
    BookmarkSent,
}

/// The result of `nextBookmarkTime(now, frequency)` `(time.Time, bool)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextBookmark {
    /// The zero `time.Time`: due immediately (bucketed at the start).
    Immediately,
    At(Instant),
    /// `ok == false`.
    Never,
}

/// What a bucketed watcher must provide.
pub trait BookmarkSchedule {
    fn next_bookmark_time(&self, now: Instant, frequency: Duration) -> NextBookmark;
}

/// `watcherBookmarkTimeBuckets`.
pub struct TimeBucketWatchers<W: BookmarkSchedule> {
    /// The key is the number of seconds since `create_time`.
    buckets: HashMap<i64, Vec<Arc<W>>>,
    create_time: Instant,
    start_bucket_id: i64,
    bookmark_frequency: Duration,
}

impl<W: BookmarkSchedule> TimeBucketWatchers<W> {
    /// `newTimeBucketWatchers`.
    pub fn new(create_time: Instant, bookmark_frequency: Duration) -> Self {
        Self {
            buckets: HashMap::new(),
            create_time,
            start_bucket_id: 0,
            bookmark_frequency,
        }
    }

    /// Number of non-empty buckets (`len(watchersBuckets)`).
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// `addWatcherThreadUnsafe`: add to the bucket of its next bookmark time; a
    /// time before the start goes to the first bucket.
    pub fn add_watcher(&mut self, w: Arc<W>, now: Instant) -> bool {
        let bucket_id = match w.next_bookmark_time(now, self.bookmark_frequency) {
            NextBookmark::Never => return false,
            // The zero time is before `create_time`: the first bucket.
            NextBookmark::Immediately => self.start_bucket_id,
            NextBookmark::At(t) => (t.saturating_duration_since(self.create_time).as_secs() as i64)
                .max(self.start_bucket_id),
        };
        self.buckets.entry(bucket_id).or_default().push(w);
        true
    }

    /// `popExpiredWatchersThreadUnsafe`: the buckets up to the current second.
    pub fn pop_expired_watchers(&mut self, now: Instant) -> Vec<Vec<Arc<W>>> {
        let current = now.saturating_duration_since(self.create_time).as_secs() as i64;
        let mut expired = Vec::with_capacity(2);
        while self.start_bucket_id <= current {
            if let Some(ws) = self.buckets.remove(&self.start_bucket_id) {
                expired.push(ws);
            }
            self.start_bucket_id += 1;
        }
        expired
    }
}

/// The bookmark bookkeeping of a `cacheWatcher` (`state` under `stateMutex`,
/// `deadline`, `bookmarkAfterResourceVersion`).
#[derive(Debug)]
pub struct WatcherBookmark {
    deadline: Option<Instant>,
    state: std::sync::Mutex<BookmarkState>,
}

impl WatcherBookmark {
    /// `newCacheWatcher`'s initial `state` is `cacheWatcherBookmarkSent`
    /// until `setBookmarkAfterResourceVersion` says otherwise; `deadline` is
    /// `None` for the zero `time.Time`.
    pub fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            state: std::sync::Mutex::new(BookmarkState::BookmarkSent),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, BookmarkState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `setBookmarkAfterResourceVersion`: 0 means no specific RV is awaited.
    pub fn set_bookmark_after_resource_version(&self, rv: i64) {
        *self.state() = if rv == 0 {
            BookmarkState::BookmarkSent
        } else {
            BookmarkState::WaitingForBookmark
        };
    }

    /// `wasBookmarkAfterRvReceived`.
    pub fn was_bookmark_after_rv_received(&self) -> bool {
        *self.state() != BookmarkState::WaitingForBookmark
    }

    /// `wasBookmarkAfterRvSent`.
    pub fn was_bookmark_after_rv_sent(&self) -> bool {
        *self.state() == BookmarkState::BookmarkSent
    }

    /// `markBookmarkAfterRvAsReceived` for a `watch.Bookmark` event; other
    /// event types are a no-op upstream.
    pub fn mark_bookmark_after_rv_as_received(&self) {
        let mut st = self.state();
        if *st == BookmarkState::WaitingForBookmark {
            *st = BookmarkState::BookmarkReceived;
        }
    }

    /// `markBookmarkAfterRvAsSent`.
    pub fn mark_bookmark_after_rv_as_sent(&self) {
        *self.state() = BookmarkState::BookmarkSent;
    }

    /// `cacheWatcher.nextBookmarkTime`.
    pub fn next_bookmark_time(&self, now: Instant, bookmark_frequency: Duration) -> NextBookmark {
        // (c) the bookmarkAfterResourceVersion was not confirmed yet.
        if !self.was_bookmark_after_rv_received() {
            return NextBookmark::Immediately;
        }
        let mut heartbeat = now + bookmark_frequency;
        let Some(deadline) = self.deadline else {
            return NextBookmark::At(heartbeat);
        };
        // `deadline.Add(-2s)`; before the Instant epoch it is certainly
        // before the heartbeat.
        if let Some(pretimeout) = deadline.checked_sub(Duration::from_secs(2)) {
            if pretimeout < heartbeat {
                heartbeat = pretimeout;
            }
        }
        if heartbeat < now {
            return NextBookmark::Never;
        }
        NextBookmark::At(heartbeat)
    }
}

impl BookmarkSchedule for WatcherBookmark {
    fn next_bookmark_time(&self, now: Instant, frequency: Duration) -> NextBookmark {
        WatcherBookmark::next_bookmark_time(self, now, frequency)
    }
}
