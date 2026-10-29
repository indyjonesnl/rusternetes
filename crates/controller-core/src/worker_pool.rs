//! Shared worker-pool launcher for queue-driven controllers.
//!
//! Ported from the launch loop every upstream controller's `Run` uses:
//!
//! ```go
//! for i := 0; i < workers; i++ {
//!     go wait.UntilWithContext(ctx, c.worker, time.Second)
//! }
//! ```
//!
//! (e.g. pkg/controller/deployment/deployment_controller.go `Run`;
//! pkg/controller/replicaset/replica_set.go `Run`). N workers drain ONE shared
//! workqueue — not N queues. That is safe because `WorkQueue::get` moves a key
//! into its `processing` set before handing it out, so a key in flight is never
//! given to a second worker (client-go `Typed[T]` dirty/processing split,
//! staging/src/k8s.io/client-go/util/workqueue/queue.go `Get`/`Add`/`Done`).
//!
//! The per-controller counts are the upstream `Concurrent*Syncs` defaults,
//! cited at each constant.

use rusternetes_storage::WorkQueue;
use std::future::Future;

/// Spawn `workers` tasks that each run `make_worker(queue.clone())`.
pub fn spawn_workers<F, Fut>(workers: usize, queue: &WorkQueue, make_worker: F)
where
    F: Fn(WorkQueue) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    for _ in 0..workers {
        tokio::spawn(make_worker(queue.clone()));
    }
}
