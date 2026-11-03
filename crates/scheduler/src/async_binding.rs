//! Bookkeeping for binding cycles that run as their own task.
//!
//! Upstream starts the binding cycle in a goroutine (`ScheduleOne`,
//! schedule_one.go:123-134) and never waits for it: the assumed pod in the
//! scheduler cache is the only record that a binding is in flight, and on
//! shutdown the goroutine just sees its context cancelled
//! (`Scheduler.Run`, scheduler.go:545-556). That stays the model here: a
//! task cancelled by runtime shutdown leaves the pod unbound in storage and
//! the assumed state dies with the process.
//!
//! This counter exists for observability only: tests (and a graceful
//! shutdown, should one want to drain) can wait until no binding task is
//! running. Nothing in the scheduling path reads it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct InFlightBindings {
    running: AtomicUsize,
    idle: Notify,
}

/// Held by a binding task; dropping it (also on panic) ends the count.
#[derive(Debug)]
pub struct BindingGuard(Arc<InFlightBindings>);

impl InFlightBindings {
    pub fn start(self: &Arc<Self>) -> BindingGuard {
        self.running.fetch_add(1, Ordering::SeqCst);
        BindingGuard(Arc::clone(self))
    }

    pub fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    /// Resolves once no binding task is running.
    pub async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            if self.running() == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for BindingGuard {
    fn drop(&mut self) {
        if self.0.running.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
