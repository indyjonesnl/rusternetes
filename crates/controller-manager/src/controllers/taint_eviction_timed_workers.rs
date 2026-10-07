//! `TimedWorkerQueue`: a keyed set of deferred work items that can be
//! cancelled before they fire.
//!
//! Ported from `pkg/controller/tainteviction/timed_workers.go`
//! (kubernetes release-1.35): `WorkArgs` (:31), `TimedWorker` (:53),
//! `createWorker` (:63), `TimedWorker.Cancel` (:101), `TimedWorkerQueue`
//! (:111), `CreateWorkerQueue` (:123), `getWrappedWorkerFunc` (:131),
//! `AddWork` (:147), `UpdateWork` (:164), `CancelWork` (:190),
//! `GetWorkerUnsafe` (:208) and `CancelAndWait` (:215).
//!
//! Go keeps a `sync.WaitGroup` of running work goroutines and a
//! `clock.AfterFunc` timer per worker. Here each worker is a tokio task that
//! sleeps until `fire_at` and is woken early by a `Notify` when cancelled; the
//! `running` counter plays the part of the `WaitGroup`.

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::{debug, error};

/// `WorkArgs` (timed_workers.go:31). The Go `NamespacedObject` also carries an
/// optional UID that `KeyFromWorkArgs` ignores; the taint manager never sets
/// it, so it is not modelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkArgs {
    pub namespace: String,
    pub name: String,
}

impl WorkArgs {
    /// `NewWorkArgs` (timed_workers.go:46).
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    }

    /// `KeyFromWorkArgs` (timed_workers.go:41): the `NamespacedName.String()`.
    pub fn key(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// The function a worker runs: `func(ctx, fireAt, args) error`.
pub type WorkFn =
    Arc<dyn Fn(DateTime<Utc>, WorkArgs) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

/// A scheduled (not yet started) worker. Entries whose work needed no timer
/// are stored as `None` in the queue map, exactly like Go's nil `*TimedWorker`
/// ("Entries may be nil if the work didn't need a timer and is already
/// running", timed_workers.go:114).
#[derive(Debug, Clone)]
pub struct TimedWorker {
    pub created_at: DateTime<Utc>,
    pub fire_at: DateTime<Utc>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

impl TimedWorker {
    /// `TimedWorker.Cancel` (timed_workers.go:101).
    fn cancel(&self) {
        // Mark first: this ensures the worker is either already running or
        // never starts once Cancel returns (timed_workers.go:73-81).
        self.cancelled.store(true, Ordering::SeqCst);
        self.wake.notify_one();
    }
}

struct Inner {
    /// Workers keyed by `WorkArgs::key`. `None` = already running.
    workers: Mutex<HashMap<String, Option<TimedWorker>>>,
    /// Number of work functions currently executing (Go `workerWG`).
    running: AtomicUsize,
    idle: Notify,
    work_fn: WorkFn,
}

/// `TimedWorkerQueue` (timed_workers.go:111).
#[derive(Clone)]
pub struct TimedWorkerQueue {
    inner: Arc<Inner>,
}

impl TimedWorkerQueue {
    /// `CreateWorkerQueue` (timed_workers.go:123).
    pub fn new(work_fn: WorkFn) -> Self {
        Self {
            inner: Arc::new(Inner {
                workers: Mutex::new(HashMap::new()),
                running: AtomicUsize::new(0),
                idle: Notify::new(),
                work_fn,
            }),
        }
    }

    /// `createWorker` (timed_workers.go:63) plus `getWrappedWorkerFunc`
    /// (:131). Returns `None` when the work was started immediately and needs
    /// no timer. Must be called with the `workers` lock held so the removal
    /// performed after the work finishes cannot race the insertion.
    fn create_worker(
        &self,
        key: String,
        args: WorkArgs,
        created_at: DateTime<Utc>,
        fire_at: DateTime<Utc>,
    ) -> Option<TimedWorker> {
        let delay = fire_at - created_at;
        let worker = TimedWorker {
            created_at,
            fire_at,
            cancelled: Arc::new(AtomicBool::new(false)),
            wake: Arc::new(Notify::new()),
        };
        let inner = Arc::clone(&self.inner);
        let cancelled = Arc::clone(&worker.cancelled);
        let wake = Arc::clone(&worker.wake);
        let immediate = delay <= chrono::Duration::zero();
        if immediate {
            // Registered before the task is spawned so `wait_idle` cannot
            // miss it.
            inner.running.fetch_add(1, Ordering::SeqCst);
        }
        tokio::spawn(async move {
            if !immediate {
                let sleep = delay.to_std().unwrap_or_default();
                tokio::select! {
                    _ = tokio::time::sleep(sleep) => {}
                    _ = wake.notified() => {}
                }
                // Register BEFORE checking the flag: `cancel_and_wait` sets
                // the flag then waits for `running == 0`, so a worker is
                // either waited for or sees the flag (timed_workers.go:73-81).
                inner.running.fetch_add(1, Ordering::SeqCst);
            }
            let _guard = RunningGuard(Arc::clone(&inner));
            if cancelled.load(Ordering::SeqCst) {
                return;
            }
            debug!(item = %key, fired_at = %fire_at, "Firing worker");
            let result = (inner.work_fn)(fire_at, args).await;
            if let Err(e) = &result {
                // timed_workers.go:88
                error!("TaintEvictionController: timed worker failed: {e}");
            }
            // getWrappedWorkerFunc (:136-139): the worker removes itself.
            inner.workers.lock().unwrap().remove(&key);
        });
        if immediate {
            None
        } else {
            Some(worker)
        }
    }

    /// `AddWork` (timed_workers.go:147): adds work to be executed not earlier
    /// than `fire_at`; an existing item for the same key is left alone.
    pub fn add_work(&self, args: WorkArgs, created_at: DateTime<Utc>, fire_at: DateTime<Utc>) {
        let key = args.key();
        let mut workers = self.inner.workers.lock().unwrap();
        if workers.contains_key(&key) {
            debug!(item = %key, "Trying to add already existing work, skipping");
            return;
        }
        let worker = self.create_worker(key.clone(), args, created_at, fire_at);
        workers.insert(key, worker);
    }

    /// `UpdateWork` (timed_workers.go:164): adds or replaces a work item. A
    /// no-op when the old and new `fire_at` are the same.
    pub fn update_work(&self, args: WorkArgs, created_at: DateTime<Utc>, fire_at: DateTime<Utc>) {
        let key = args.key();
        let mut workers = self.inner.workers.lock().unwrap();
        if let Some(existing) = workers.get(&key) {
            match existing {
                // "Keeping existing work, already in progress"
                None => return,
                Some(w) if w.fire_at == fire_at => return,
                Some(w) => w.cancel(),
            }
        }
        let worker = self.create_worker(key.clone(), args, created_at, fire_at);
        workers.insert(key, worker);
    }

    /// `CancelWork` (timed_workers.go:190): removes scheduled execution.
    /// Returns true if a scheduled (not yet running) work item was cancelled.
    pub fn cancel_work(&self, key: &str) -> bool {
        let mut workers = self.inner.workers.lock().unwrap();
        match workers.remove(key) {
            Some(Some(worker)) => {
                worker.cancel();
                true
            }
            _ => false,
        }
    }

    /// `GetWorkerUnsafe` (timed_workers.go:208). `None` for an absent key and
    /// for an entry that is already running (Go's nil worker).
    pub fn get_worker_unsafe(&self, key: &str) -> Option<TimedWorker> {
        self.inner
            .workers
            .lock()
            .unwrap()
            .get(key)
            .and_then(|w| w.clone())
    }

    /// `CancelAndWait` (timed_workers.go:215): cancels every worker and waits
    /// for running work to terminate.
    pub async fn cancel_and_wait(&self) {
        {
            let mut workers = self.inner.workers.lock().unwrap();
            for worker in workers.values().flatten() {
                worker.cancel();
            }
            workers.clear();
        }
        self.wait_idle().await;
    }

    /// Wait until no work function is executing. Scheduled-but-sleeping
    /// workers do not count. (Rust-only helper: Go tests use `wg.Wait`.)
    pub async fn wait_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            // Enable before the check so a wake between check and await is
            // not lost.
            notified.as_mut().enable();
            if self.inner.running.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct RunningGuard(Arc<Inner>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if self.0.running.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    //! Cases ported from `timed_workers_test.go`: TestExecute (:30),
    //! TestExecuteDelayed (:53), TestCancel (:85), TestCancelAndRead (:119).
    use super::*;
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
