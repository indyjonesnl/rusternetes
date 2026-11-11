//! Ports of `cacher/cache_watcher_test.go::TestTimeBucketWatchersBasic` and
//! `TestBookmarkAfterResourceVersionWatchers`, plus `nextBookmarkTime` cases
//! (`cache_watcher.go:215-262`). `FakeClock` is `testingclock.FakeClock` over
//! the repo's `PassiveClock` (`clock.Step(d)` is `step(d)`).
use rusternetes_api_server::cache_watcher::*;
use rusternetes_api_server::flow_control_object_count::PassiveClock;
use rusternetes_api_server::watcher_bookmark::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct FakeClock(Mutex<Instant>);
impl FakeClock {
    fn new(t: Instant) -> Arc<Self> {
        Arc::new(Self(Mutex::new(t)))
    }
    fn step(&self, d: Duration) {
        *self.0.lock().unwrap() += d;
    }
}
impl PassiveClock for FakeClock {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// `newCacheWatcher(0, ..., deadline, true, pods, "")` (chan size 1: a tokio
/// bounded channel cannot be unbuffered) followed by
/// `setBookmarkAfterResourceVersion(rv)`. The receiver is dropped: only the
/// bookmark state is exercised.
fn new_watcher(deadline: Instant, rv: u64) -> Arc<CacheWatcher> {
    let (w, _rx) = new_cache_watcher(1, Some(deadline));
    w.set_bookmark_after_resource_version(rv);
    Arc::new(w)
}

#[test]
fn time_bucket_watchers_basic() {
    let clock = FakeClock::new(Instant::now());
    let mut watchers = TimeBucketWatchers::new(clock.clone(), DEFAULT_BOOKMARK_FREQUENCY);
    let now = clock.now();
    watchers.add_watcher_thread_unsafe(new_watcher(now + secs(10), 0));
    watchers.add_watcher_thread_unsafe(new_watcher(now + secs(20), 0));
    watchers.add_watcher_thread_unsafe(new_watcher(now + secs(20), 0));

    assert_eq!(watchers.bucket_count(), 2);
    assert_eq!(watchers.pop_expired_watchers_thread_unsafe().len(), 0);

    clock.step(secs(10));
    let w1 = watchers.pop_expired_watchers_thread_unsafe();
    assert!(w1.len() == 1 && w1[0].len() == 1, "{}", w1.len());
    assert_eq!(watchers.pop_expired_watchers_thread_unsafe().len(), 0);

    clock.step(secs(12));
    let w2 = watchers.pop_expired_watchers_thread_unsafe();
    assert!(w2.len() == 1 && w2[0].len() == 2);
}

#[test]
fn bookmark_after_resource_version_watchers() {
    let clock = FakeClock::new(Instant::now());
    let mut target = TimeBucketWatchers::new(clock.clone(), DEFAULT_BOOKMARK_FREQUENCY);
    assert!(target.add_watcher_thread_unsafe(new_watcher(clock.now() + secs(120), 10)));

    // the watcher is immediately expired (it's waiting for bookmark, so it is
    // scheduled immediately)
    let mut ret = target.pop_expired_watchers_thread_unsafe();
    assert!(ret.len() == 1 && ret[0].len() == 1);
    assert!(target.add_watcher_thread_unsafe(ret[0][0].clone()));

    // after one second time the watcher is still expired
    clock.step(secs(1));
    ret = target.pop_expired_watchers_thread_unsafe();
    assert!(ret.len() == 1 && ret[0].len() == 1);
    assert!(target.add_watcher_thread_unsafe(ret[0][0].clone()));

    // after 29 seconds the watcher is still expired
    clock.step(secs(29));
    ret = target.pop_expired_watchers_thread_unsafe();
    assert!(ret.len() == 1 && ret[0].len() == 1);

    // after confirming the watcher is not expired immediately
    ret[0][0].mark_bookmark_after_rv_as_received(true);
    assert!(target.add_watcher_thread_unsafe(ret[0][0].clone()));
    clock.step(secs(30));
    assert_eq!(target.pop_expired_watchers_thread_unsafe().len(), 0);

    clock.step(secs(30));
    ret = target.pop_expired_watchers_thread_unsafe();
    assert!(ret.len() == 1 && ret[0].len() == 1);
}

#[test]
fn next_bookmark_time_cases() {
    let now = Instant::now();
    let f = DEFAULT_BOOKMARK_FREQUENCY;
    // Zero deadline: heartbeat.
    let (w, _rx) = new_cache_watcher(1, None);
    w.set_bookmark_after_resource_version(0);
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::At(now + f));
    // Deadline within the heartbeat: 2s before it.
    let w = new_watcher(now + secs(10), 0);
    assert_eq!(
        w.next_bookmark_time(now, f),
        NextBookmark::At(now + secs(8))
    );
    // Already past the pre-timeout time: not scheduled.
    let w = new_watcher(now + secs(1), 0);
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::Never);
}

#[test]
fn bookmark_state_machine() {
    let now = Instant::now();
    let f = DEFAULT_BOOKMARK_FREQUENCY;
    // newCacheWatcher leaves `state` at its zero value, WaitingForBookmark.
    let (w, _rx) = new_cache_watcher(1, None);
    assert!(!w.was_bookmark_after_rv_received());
    w.set_bookmark_after_resource_version(0);
    assert!(w.was_bookmark_after_rv_sent());

    w.set_bookmark_after_resource_version(5);
    assert_eq!(w.bookmark_after_resource_version(), 5);
    assert!(!w.was_bookmark_after_rv_received());
    assert_eq!(w.next_bookmark_time(now, f), NextBookmark::Immediately);
    // Only watch.Bookmark events change the state.
    w.mark_bookmark_after_rv_as_received(false);
    assert!(!w.was_bookmark_after_rv_received());
    w.mark_bookmark_after_rv_as_received(true);
    assert!(w.was_bookmark_after_rv_received() && !w.was_bookmark_after_rv_sent());
    w.mark_bookmark_after_rv_sent(false);
    assert!(!w.was_bookmark_after_rv_sent());
    w.mark_bookmark_after_rv_sent(true);
    assert!(w.was_bookmark_after_rv_sent());
}
