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

#[cfg(test)]
pub mod test_support {
    //! Storage wrapper that makes overlap between workers observable, so a
    //! test can drive a controller's real `run()` and assert the pool is wider
    //! than one (see namespace.rs `run_processes_namespaces_with_a_worker_pool`
    //! for why `reconcile_all` is the wrong thing to test).
    use rusternetes_storage::{memory::MemoryStorage, Storage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Seed `count` objects of `plural` (built by `make(i)`), spawn the
    /// controller's real `run()` over a [`StallStorage`], let the pool work for
    /// a moment, and return the peak number of overlapping storage calls.
    ///
    /// Only a worker `get`s the primary object it just dequeued, and one
    /// worker does so one key at a time, so a peak > 1 means the live path
    /// runs more than one worker.
    pub async fn peak_concurrency<F, Fut>(
        plural: &str,
        count: usize,
        make: impl Fn(usize) -> serde_json::Value,
        run: F,
    ) -> usize
    where
        F: FnOnce(Arc<StallStorage>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let inner = Arc::new(MemoryStorage::new());
        let storage = StallStorage::new(Arc::clone(&inner), &format!("/registry/{plural}/"));
        for i in 0..count {
            let obj = make(i);
            let key = format!("/registry/{plural}/default/obj-{i}");
            inner.create(&key, &obj).await.unwrap();
        }
        let handle = tokio::spawn(run(Arc::clone(&storage)));
        tokio::time::sleep(Duration::from_millis(800)).await;
        handle.abort();
        storage.peak()
    }

    /// Minimal pod template shared by the workload fixtures.
    pub fn template() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"labels": {"a": "b"}},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        })
    }

    pub fn meta(i: usize) -> serde_json::Value {
        serde_json::json!({"name": format!("obj-{i}"), "namespace": "default", "uid": format!("uid-{i}")})
    }

    pub struct StallStorage {
        pub inner: Arc<MemoryStorage>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
        /// Only `get`s of the controller's primary resource are stalled and
        /// counted: that is the worker loading the key it just dequeued.
        enqueue_prefix: String,
    }

    impl StallStorage {
        pub fn new(inner: Arc<MemoryStorage>, enqueue_prefix: &str) -> Arc<Self> {
            Arc::new(Self {
                inner,
                inflight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                enqueue_prefix: enqueue_prefix.to_string(),
            })
        }

        async fn stall(&self) {
            let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(40)).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
        }

        pub fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Storage for StallStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            if key.starts_with(&self.enqueue_prefix) {
                self.stall().await;
            }
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }
        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }
        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }
        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }
}
