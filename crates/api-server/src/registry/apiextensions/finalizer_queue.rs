//! The workqueue, event filters and workers of the CRD finalizer, a port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/controller/finalizer/crd_finalizer.go`
//! (the `sync` itself is `CrdRest::finalize` in `customresourcedefinition.rs`).
//!
//! Upstream: `NewCRDFinalizer` builds a rate-limited workqueue
//! (`workqueue.NewTypedRateLimitingQueueWithConfig(
//! workqueue.DefaultTypedControllerRateLimiter[string](), ...)`, :73-76) and
//! registers only `AddFunc` and `UpdateFunc` on the CRD informer (:81-84);
//! `RunWithContext(5, ctx)` starts five workers on it (:276-290, the `5` of
//! `go finalizingController.Run(5, ...)`, apiserver.go:249); a worker
//! `Forget`s a key whose `sync` succeeded and `AddRateLimited`s one that
//! failed (`processNextWorkItem`, :301-318).
//!
//! The workqueue itself (`client-go/util/workqueue/queue.go`): a key added
//! while queued is not queued twice (`dirty`), a key is never handed to two
//! workers at once (`processing`), and a key added while it is being
//! processed is queued again once its worker calls `Done`.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use rusternetes_common::resources::CustomResourceDefinition;
use rusternetes_common::validation::crd::CUSTOM_RESOURCE_CLEANUP_FINALIZER;
use tokio::sync::Notify;

use super::condition_controllers::remove_crd_condition;
use super::controllers::{crd_has_finalizer, TERMINATING};
use super::customresourcedefinition::ControllerRateLimiter;

#[derive(Default)]
struct State {
    queue: VecDeque<String>,
    dirty: HashSet<String>,
    processing: HashSet<String>,
}

/// `workqueue.TypedRateLimitingInterface[string]` named `crd_finalizer`.
pub struct FinalizerQueue {
    state: Mutex<State>,
    notify: Notify,
    limiter: Mutex<ControllerRateLimiter>,
}

impl Default for FinalizerQueue {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            notify: Notify::new(),
            limiter: Mutex::new(ControllerRateLimiter::new()),
        }
    }
}

impl FinalizerQueue {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `Add` (queue.go): a key already waiting is not queued again, and one
    /// being processed is queued when its worker is done with it.
    pub fn add(&self, key: &str) {
        {
            let mut s = self.state();
            if !s.dirty.insert(key.to_string()) {
                return;
            }
            if s.processing.contains(key) {
                return;
            }
            s.queue.push_back(key.to_string());
        }
        self.notify.notify_one();
    }

    /// `Get` (queue.go) without the blocking: the next key, now processing.
    pub fn get(&self) -> Option<String> {
        let mut s = self.state();
        let key = s.queue.pop_front()?;
        s.processing.insert(key.clone());
        s.dirty.remove(&key);
        Some(key)
    }

    /// `Done` (queue.go): a key re-added meanwhile is queued again.
    pub fn done(&self, key: &str) {
        let requeued = {
            let mut s = self.state();
            s.processing.remove(key);
            if s.dirty.contains(key) {
                s.queue.push_back(key.to_string());
                true
            } else {
                false
            }
        };
        if requeued {
            self.notify.notify_one();
        }
    }

    /// `Forget` (default_rate_limiters.go).
    pub fn forget(&self, key: &str) {
        self.limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .forget(key);
    }

    /// `AddRateLimited` (rate_limiting_queue.go): `Add` after the limiter's
    /// delay for the key.
    pub fn add_rate_limited(self: &Arc<Self>, key: &str) {
        let delay = self
            .limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .when_at(key, tokio::time::Instant::now());
        let weak = Arc::downgrade(self);
        let key = key.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(q) = weak.upgrade() {
                q.add(&key);
            }
        });
    }

    /// Wait for an `add`.
    pub(super) async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Wake every idle worker, so they see the queue is gone and exit.
    pub(super) fn wake_all(&self) {
        self.notify.notify_waiters();
    }
}

/// `OverlappingBuiltInResources` (crd_finalizer.go:50-56): the built-in
/// resources whose storage a CRD of the same group and plural would share, so
/// the finalizer must not delete the "instances" it finds there.
pub fn overlapping_built_in_resource(group: &str, plural: &str) -> bool {
    matches!(
        (group, plural),
        ("apiregistration.k8s.io", "apiservices")
            | ("apiextensions.k8s.io", "customresourcedefinitions")
    )
}

/// `addCustomResourceDefinition` (crd_finalizer.go:332-339): "only queue
/// deleted things".
pub fn add_event_enqueues(crd: &CustomResourceDefinition) -> bool {
    crd.metadata.deletion_timestamp.is_some()
        && crd_has_finalizer(crd, CUSTOM_RESOURCE_CLEANUP_FINALIZER)
}

/// `updateCustomResourceDefinition` (crd_finalizer.go:341-366).
pub fn update_event_enqueues(
    old: &CustomResourceDefinition,
    new: &CustomResourceDefinition,
) -> bool {
    // "only queue deleted things that haven't been finalized by us"
    if new.metadata.deletion_timestamp.is_none()
        || !crd_has_finalizer(new, CUSTOM_RESOURCE_CLEANUP_FINALIZER)
    {
        return false;
    }
    // "always requeue resyncs just in case"
    if old.metadata.resource_version == new.metadata.resource_version {
        return true;
    }
    // "If the only difference is in the terminating condition, then there's
    // no reason to requeue here. This controller is likely to be the
    // originator, so requeuing would hot-loop us."
    let mut old_copy = old.clone();
    let mut new_copy = new.clone();
    old_copy.metadata.resource_version = None;
    new_copy.metadata.resource_version = None;
    // `RemoveCRDCondition` (helpers.go) leaves an empty, non-nil condition
    // list on both copies whatever they held; `status` is a Go value, here an
    // `Option`.
    for copy in [&mut old_copy, &mut new_copy] {
        remove_crd_condition(copy, TERMINATING);
        copy.status
            .get_or_insert_with(Default::default)
            .conditions
            .get_or_insert_with(Vec::new);
    }
    old_copy != new_copy
}
