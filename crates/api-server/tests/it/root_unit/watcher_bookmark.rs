//! Ports of `cacher/cache_watcher_test.go::TestTimeBucketWatchersBasic` and
//! `TestBookmarkAfterResourceVersionWatchers`. The fake clock is an explicit
//! `Instant` (`clock.Step(d)` is `now += d`).
use rusternetes_api_server::watcher_bookmark::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn time_bucket_watchers_basic() {
    let newer = |deadline: Instant| {
        let w = WatcherBookmark::new(Some(deadline));
        w.set_bookmark_after_resource_version(0);
        Arc::new(w)
    };
    let mut now = Instant::now();
    let mut watchers = TimeBucketWatchers::new(now, DEFAULT_BOOKMARK_FREQUENCY);
    let start = now;
    watchers.add_watcher(newer(start + secs(10)), now);
    watchers.add_watcher(newer(start + secs(20)), now);
    watchers.add_watcher(newer(start + secs(20)), now);

    assert_eq!(watchers.bucket_count(), 2);
    assert_eq!(watchers.pop_expired_watchers(now).len(), 0);

    now += secs(10);
    let w1 = watchers.pop_expired_watchers(now);
    assert!(w1.len() == 1 && w1[0].len() == 1, "{}", w1.len());
    assert_eq!(watchers.pop_expired_watchers(now).len(), 0);

    now += secs(12);
    let w2 = watchers.pop_expired_watchers(now);
    assert!(w2.len() == 1 && w2[0].len() == 2);
}

#[test]
fn bookmark_after_resource_version_watchers() {
    let newer = |deadline: Instant| {
        let w = WatcherBookmark::new(Some(deadline));
        w.set_bookmark_after_resource_version(10);
        Arc::new(w)
    };
    let mut now = Instant::now();
    let mut target = TimeBucketWatchers::new(now, DEFAULT_BOOKMARK_FREQUENCY);
    assert!(target.add_watcher(newer(now + secs(120)), now));

    // Waiting for the bookmark, so it is scheduled immediately.
    let mut ret = target.pop_expired_watchers(now);
    assert!(ret.len() == 1 && ret[0].len() == 1);
    assert!(target.add_watcher(ret[0][0].clone(), now));

    now += secs(1);
    ret = target.pop_expired_watchers(now);
    assert!(ret.len() == 1 && ret[0].len() == 1);
    assert!(target.add_watcher(ret[0][0].clone(), now));

    now += secs(29);
    ret = target.pop_expired_watchers(now);
    assert!(ret.len() == 1 && ret[0].len() == 1);

    // After confirming, the watcher is no longer expired immediately.
    ret[0][0].mark_bookmark_after_rv_as_received();
    assert!(target.add_watcher(ret[0][0].clone(), now));
    now += secs(30);
    assert_eq!(target.pop_expired_watchers(now).len(), 0);

    now += secs(30);
    ret = target.pop_expired_watchers(now);
    assert!(ret.len() == 1 && ret[0].len() == 1);
}

#[test]
fn next_bookmark_time_cases() {
    let now = Instant::now();
    let f = DEFAULT_BOOKMARK_FREQUENCY;
    // No deadline: heartbeat.
    let w = WatcherBookmark::new(None);
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::At(now + f));
    // Deadline within the heartbeat: 2s before it.
    let w = WatcherBookmark::new(Some(now + secs(10)));
    assert_eq!(
        w.next_bookmark_time(now, f),
        NextBookmark::At(now + secs(8))
    );
    // Already past the pre-timeout time: not scheduled.
    let w = WatcherBookmark::new(Some(now + secs(1)));
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::Never);
    // State machine.
    let w = WatcherBookmark::new(None);
    assert!(w.was_bookmark_after_rv_sent());
    w.set_bookmark_after_resource_version(5);
    assert!(!w.was_bookmark_after_rv_received());
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::Immediately);
    w.mark_bookmark_after_rv_as_received();
    assert!(w.was_bookmark_after_rv_received() && !w.was_bookmark_after_rv_sent());
    w.mark_bookmark_after_rv_as_sent();
    assert!(w.was_bookmark_after_rv_sent());
}
