//! The workqueue and worker behind the CRD naming, establishing,
//! nonstructuralschema and apiapproval controllers.
//!
//! Upstream: each controller owns a rate-limited workqueue fed by informer
//! events (`AddFunc`/`UpdateFunc`/`DeleteFunc` -> `queue.Add(key)`,
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/controller/status/
//! naming_controller.go:343-363`, `establish/establishing_controller.go:71-73`)
//! and drained by a worker that `Forget`s a key on success and
//! `AddRateLimited`s it on error (`establishing_controller.go:98-117`,
//! `naming_controller.go` `processNextWorkItem`). A key queued twice before it
//! is picked up is processed once (`workqueue` dedups).
//!
//! Here a CRD write (`CrdRest::create`/`update`) is the event: it adds the
//! CRD's name. One worker per [`CrdControllers`] runs the naming controller's
//! `sync`, then the establishing controller's and the condition controllers',
//! for that name, as upstream's single-worker controllers do. The worker is
//! woken by the add (no polling), so a CRD is established within one
//! scheduling round of its create. The 5 minute resync
//! (`customresourcedefinition::start_controllers`) remains the backstop.
//!
//! Deviation: upstream's `masterCount > 1` 5 second `QueueCRD` delay
//! (`customresource_handler.go:486-494`) is not modelled; this api-server is
//! the only writer, which is upstream's `masterCount == 1` branch (delay 0).

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rusternetes_storage::StorageBackend;
use tokio::sync::Notify;
use tracing::warn;

use super::condition_controllers::ConditionControllers;
use super::customresourcedefinition::{ControllerRateLimiter, CrdRest};

#[derive(Default)]
struct Queue {
    order: VecDeque<String>,
    queued: HashSet<String>,
}

/// The controllers' shared state: the queue, the rate limiter that spaces
/// retries, and the condition controllers' memos.
pub struct CrdControllers {
    queue: Mutex<Queue>,
    notify: Arc<Notify>,
    started: AtomicBool,
    limiter: Mutex<ControllerRateLimiter>,
    pub(super) conditions: ConditionControllers,
}

impl Default for CrdControllers {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            notify: Arc::new(Notify::new()),
            started: AtomicBool::new(false),
            limiter: Mutex::new(ControllerRateLimiter::new()),
            conditions: ConditionControllers::default(),
        }
    }
}

impl Drop for CrdControllers {
    /// Wake the worker so it sees the controllers are gone and exits.
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl CrdControllers {
    /// `queue.Add(key)`: a name already waiting is not queued again.
    pub fn add(&self, name: &str) {
        {
            let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            if !q.queued.insert(name.to_string()) {
                return;
            }
            q.order.push_back(name.to_string());
        }
        self.notify.notify_one();
    }

    /// `queue.AddRateLimited(key)`: `add` after the per-item backoff.
    fn add_rate_limited(self: &Arc<Self>, name: &str) {
        let delay = self
            .limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .when_at(name, tokio::time::Instant::now());
        let weak = Arc::downgrade(self);
        let name = name.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(c) = weak.upgrade() {
                c.add(&name);
            }
        });
    }

    fn forget(&self, name: &str) {
        self.limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .forget(name);
    }

    fn pop(&self) -> Option<String> {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        let name = q.order.pop_front()?;
        q.queued.remove(&name);
        Some(name)
    }

    /// The worker, started once on the first add through a [`CrdRest`]
    /// (`go wait.UntilWithContext(ctx, c.runWorker, time.Second)`). It holds
    /// the controllers only weakly: when the last handle goes, it exits.
    pub(super) fn ensure_started(self: &Arc<Self>, storage: Arc<StorageBackend>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let weak = Arc::downgrade(self);
        let notify = self.notify.clone();
        tokio::spawn(async move {
            loop {
                let Some(controllers) = weak.upgrade() else {
                    return;
                };
                let Some(name) = controllers.pop() else {
                    drop(controllers);
                    notify.notified().await;
                    continue;
                };
                let rest = CrdRest::with_controllers(storage.clone(), controllers.clone());
                match rest.process_one(&name).await {
                    Ok(()) => controllers.forget(&name),
                    Err(e) => {
                        warn!("customresourcedefinition {name}: controllers failed: {e}");
                        controllers.add_rate_limited(&name);
                    }
                }
            }
        });
    }
}
