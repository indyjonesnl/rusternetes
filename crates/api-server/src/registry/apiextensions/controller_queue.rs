//! The workqueue and worker behind the CRD naming and establishing
//! controllers, and the workers of the nonstructuralschema and apiapproval
//! controllers (each on its own queue, five workers, as apiserver.go:247-248).
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
use super::customresourcedefinition::{ControllerRateLimiter, CrdRest, CRD_FINALIZER_WORKERS};
use super::finalizer_queue::FinalizerQueue;

#[derive(Clone, Copy, Debug)]
enum ConditionKind {
    NonStructural,
    ApiApproval,
}

#[derive(Default)]
pub struct Queue {
    order: VecDeque<String>,
    queued: HashSet<String>,
}

/// The controllers' shared state: the queue, the rate limiter that spaces
/// retries, and the condition controllers' memos.
pub struct CrdControllers {
    pub queue: Mutex<Queue>,
    pub notify: Arc<Notify>,
    pub started: AtomicBool,
    pub limiter: Mutex<ControllerRateLimiter>,
    /// The CRD finalizer's own workqueue (crd_finalizer.go:73-76), drained by
    /// [`CRD_FINALIZER_WORKERS`] workers.
    pub finalizer: Arc<FinalizerQueue>,
    /// The nonstructuralschema controller's own workqueue
    /// (`non_structural_schema_condition_controller`), drained by
    /// [`CRD_FINALIZER_WORKERS`] workers (`go nonStructuralSchemaController.
    /// RunWithContext(5, ...)`, apiserver.go:247).
    pub non_structural: Arc<FinalizerQueue>,
    /// The apiapproval controller's own workqueue
    /// (`kubernetes_api_approval_conformant_condition_controller`), five
    /// workers (apiserver.go:248).
    pub api_approval: Arc<FinalizerQueue>,
    pub conditions: ConditionControllers,
}

impl Default for CrdControllers {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            notify: Arc::new(Notify::new()),
            started: AtomicBool::new(false),
            limiter: Mutex::new(ControllerRateLimiter::new()),
            finalizer: Arc::default(),
            non_structural: Arc::default(),
            api_approval: Arc::default(),
            conditions: ConditionControllers::default(),
        }
    }
}

impl Drop for CrdControllers {
    /// Wake the worker so it sees the controllers are gone and exits.
    fn drop(&mut self) {
        self.notify.notify_one();
        self.finalizer.wake_all();
        self.non_structural.wake_all();
        self.api_approval.wake_all();
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

    /// `c.queue.Add(key)` of the nonstructuralschema and apiapproval
    /// controllers' `enqueue` (nonstructuralschema_controller.go:239-248,
    /// apiapproval_controller.go:233-242), called from their `AddFunc` and
    /// `UpdateFunc`. A `DeleteFunc` does not enqueue (they only forget).
    pub fn add_conditions(&self, name: &str) {
        self.non_structural.add(name);
        self.api_approval.add(name);
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

    pub fn pop(&self) -> Option<String> {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        let name = q.order.pop_front()?;
        q.queued.remove(&name);
        Some(name)
    }

    /// `runWorker` / `processNextWorkItem` (crd_finalizer.go:292-318): take a
    /// key off the finalizer queue, run `sync`, `Forget` the key on success or
    /// `AddRateLimited` it on error, then `Done`. The worker holds the
    /// controllers only weakly: when the last handle goes, it exits.
    fn spawn_finalizer_worker(self: &Arc<Self>, storage: Arc<StorageBackend>) {
        let weak = Arc::downgrade(self);
        let queue = self.finalizer.clone();
        tokio::spawn(async move {
            loop {
                let Some(controllers) = weak.upgrade() else {
                    return;
                };
                let Some(key) = queue.get() else {
                    drop(controllers);
                    queue.notified().await;
                    continue;
                };
                let rest = CrdRest::with_controllers(storage.clone(), controllers.clone());
                match rest.process_finalizer_key(&key).await {
                    Ok(()) => queue.forget(&key),
                    Err(e) => {
                        warn!("customresourcedefinition {key}: finalizer failed: {e}");
                        queue.add_rate_limited(&key);
                    }
                }
                queue.done(&key);
            }
        });
    }

    /// `go wait.UntilWithContext(ctx, c.runWorker, time.Second)` of the
    /// nonstructuralschema / apiapproval controllers (`processNextWorkItem`,
    /// nonstructuralschema_controller.go:219-237): `Forget` on success,
    /// `AddRateLimited` on error, then `Done`.
    fn spawn_condition_worker(self: &Arc<Self>, kind: ConditionKind, storage: Arc<StorageBackend>) {
        let weak = Arc::downgrade(self);
        let queue = match kind {
            ConditionKind::NonStructural => self.non_structural.clone(),
            ConditionKind::ApiApproval => self.api_approval.clone(),
        };
        tokio::spawn(async move {
            loop {
                let Some(controllers) = weak.upgrade() else {
                    return;
                };
                let Some(key) = queue.get() else {
                    drop(controllers);
                    queue.notified().await;
                    continue;
                };
                let rest = CrdRest::with_controllers(storage.clone(), controllers.clone());
                let result = match kind {
                    ConditionKind::NonStructural => rest.sync_non_structural(&key).await,
                    ConditionKind::ApiApproval => rest.sync_api_approval(&key).await,
                };
                match result {
                    Ok(()) => queue.forget(&key),
                    Err(e) => {
                        warn!("customresourcedefinition {key}: {kind:?} controller failed: {e}");
                        queue.add_rate_limited(&key);
                    }
                }
                queue.done(&key);
            }
        });
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
        // `for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx,
        // c.runWorker, time.Second) }` (crd_finalizer.go:283-285).
        for _ in 0..CRD_FINALIZER_WORKERS {
            self.spawn_finalizer_worker(storage.clone());
        }
        // `go nonStructuralSchemaController.RunWithContext(5, ..)`,
        // `go apiApprovalController.RunWithContext(5, ..)` (apiserver.go:247-248).
        for _ in 0..CRD_FINALIZER_WORKERS {
            self.spawn_condition_worker(ConditionKind::NonStructural, storage.clone());
            self.spawn_condition_worker(ConditionKind::ApiApproval, storage.clone());
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
