//! TTL-after-finished controller.
//!
//! Ported from `pkg/controller/ttlafterfinished/ttlafterfinished_controller.go`
//! and `metrics/metrics.go` (release-1.35), launched like
//! `newTTLAfterFinishedController`
//! (`cmd/kube-controller-manager/app/core.go:907-930`,
//! `ConcurrentTTLSyncs` workers, default 5:
//! `pkg/controller/ttlafterfinished/config/v1alpha1/defaults.go:33-34`).
//!
//! Ported 1:1:
//! - the `ttl_jobs_to_delete` rate-limited queue of `namespace/name` keys;
//! - the Job event handlers: `addJob` / `updateJob` enqueue a Job only when it
//!   has no `deletionTimestamp` and `needsCleanup` (a non-nil
//!   `spec.ttlSecondsAfterFinished` on a finished Job); the informer has no
//!   `DeleteFunc`;
//! - `processNextWorkItem` / `handleErr` (error -> `AddRateLimited`, success ->
//!   `Forget`);
//! - `processJob`: read the Job from the lister (the informer store), run
//!   `processTTL`, then **re-read the Job from the API** and re-run
//!   `processTTL` on the fresh copy before deleting ("the Job TTL might be
//!   stale"), then delete with `PropagationPolicy: Foreground` and
//!   `Preconditions{UID: fresh.UID}`, and observe
//!   `job_deletion_duration_seconds` since the expiry time;
//! - `processTTL` / `needsCleanup` / `getFinishAndExpireTime` / `timeLeft` /
//!   `jobFinishTime` (the finish time is the `lastTransitionTime` of the first
//!   `Complete` or `Failed` condition that is `True`; a zero time is an error),
//!   including `enqueueAfter` for a Job whose TTL has not yet expired;
//! - the `ttl_after_finished_controller_job_deletion_duration_seconds`
//!   histogram (`ExponentialBuckets(0.1, 2, 14)`).
//!
//! The Job informer is a `k3rs_client::reflector::Reflector` over the
//! [`Storage`] trait (via `storage_list_watch::StorageListWatch`), so
//! `processJob`'s first read costs no storage round trip, as upstream's lister
//! read does. The fresh read is a real `Storage::get`, as upstream's
//! `client.BatchV1().Jobs(ns).Get`.
//!
//! Deviations:
//! - Upstream builds an `EventRecorder` (component
//!   `ttl-after-finished-controller`) that it never uses; none is created.
//! - The Job `Delete` goes through
//!   [`Storage::delete_foreground_with_preconditions`]: a real `DELETE` with
//!   `propagationPolicy=Foreground` and the UID precondition over the API; on a
//!   direct backend it emulates the registry's `updateForGCFinalizers` (adds the
//!   `foregroundDeletion` finalizer and `deletionTimestamp`, which the garbage
//!   controller then completes).
//! - A lagged informer subscription re-enqueues every Job that needs cleanup (an
//!   informer resync) instead of losing events.
//! - The previous implementation also honoured a `ttlSecondsAfterFinished`
//!   *annotation*. Upstream has no such thing (only the typed field), so it is
//!   gone.

use crate::controllers::storage_list_watch::StorageListWatch;
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use k3rs_client::reflector::{Reflector, StoreEvent};
use k3rs_common::resources::workloads::Job;
use k3rs_common::Error;
use k3rs_storage::{build_key, build_prefix, Preconditions, Storage, WorkQueue};
use prometheus::Histogram;
use std::sync::{Arc, LazyLock};
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, error, info, warn};

/// Workers draining the queue: `ConcurrentTTLSyncs (ttl-after-finished)`
/// default (`pkg/controller/ttlafterfinished/config/v1alpha1/defaults.go:33-34`).
pub const CONCURRENT_TTL_SYNCS: usize = 5;

/// Queue name (`TypedRateLimitingQueueConfig{Name: "ttl_jobs_to_delete"}`).
pub const QUEUE_NAME: &str = "ttl_jobs_to_delete";

/// `TTLAfterFinishedSubsystem` (metrics.go:28).
pub const SUBSYSTEM: &str = "ttl_after_finished_controller";

/// `JobDeletionDurationSeconds` (metrics.go:32-41): the time it took to delete
/// the job since it became eligible for deletion.
static JOB_DELETION_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    let h = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            format!("{SUBSYSTEM}_job_deletion_duration_seconds"),
            "The time it took to delete the job since it became eligible for deletion",
        )
        // Start with 100ms with the last bucket being [~27m, Inf).
        .buckets(prometheus::exponential_buckets(0.1, 2.0, 14).expect("valid buckets")),
    )
    .expect("valid opts");
    let _ = prometheus::default_registry().register(Box::new(h.clone()));
    h
});

/// `metrics.Register()`: make the series appear on `/metrics`.
pub fn register_metrics() {
    LazyLock::force(&JOB_DELETION_DURATION_SECONDS);
}

/// Number of observations of `job_deletion_duration_seconds` (tests).
pub fn job_deletion_observations() -> u64 {
    JOB_DELETION_DURATION_SECONDS.get_sample_count()
}

/// `jobutil.FinishedCondition` (pkg/controller/job/util/utils.go:26-33): the
/// first `Complete` or `Failed` condition whose status is `True`.
fn finished_condition(j: &Job) -> Option<&k3rs_common::resources::workloads::JobCondition> {
    j.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| matches!(c.condition_type.as_str(), "Complete" | "Failed") && c.status == "True")
}

/// `jobutil.IsJobFinished` (utils.go:37-40).
pub fn is_job_finished(j: &Job) -> bool {
    finished_condition(j).is_some()
}

fn job_ref(j: &Job) -> String {
    format!(
        "{}/{}",
        j.metadata.namespace.as_deref().unwrap_or(""),
        j.metadata.name
    )
}

/// `needsCleanup`: whether a Job has finished and has a TTL set.
pub fn needs_cleanup(j: &Job) -> bool {
    j.spec.ttl_seconds_after_finished.is_some() && is_job_finished(j)
}

/// `jobFinishTime`: the time an already finished Job finished.
fn job_finish_time(finished_job: &Job) -> Result<DateTime<Utc>, Error> {
    match finished_condition(finished_job) {
        Some(c) => c.last_transition_time.ok_or_else(|| {
            Error::Internal(format!(
                "unable to find the time when the Job {} finished",
                job_ref(finished_job)
            ))
        }),
        // This should never happen if the Jobs has finished.
        None => Err(Error::Internal(format!(
            "unable to find the status of the finished Job {}",
            job_ref(finished_job)
        ))),
    }
}

/// `getFinishAndExpireTime`: `(finishAt, expireAt)`.
pub fn get_finish_and_expire_time(j: &Job) -> Result<(DateTime<Utc>, DateTime<Utc>), Error> {
    if !needs_cleanup(j) {
        return Err(Error::Internal(format!(
            "job {} should not be cleaned up",
            job_ref(j)
        )));
    }
    let finish_at = job_finish_time(j)?;
    // `needs_cleanup` guarantees the TTL is set.
    let ttl = j.spec.ttl_seconds_after_finished.unwrap_or_default();
    let expire_at = finish_at + Duration::seconds(i64::from(ttl));
    Ok((finish_at, expire_at))
}

/// `timeLeft`: the remaining TTL (negative once expired) and the expiry time.
pub fn time_left(j: &Job, since: DateTime<Utc>) -> Result<(Duration, DateTime<Utc>), Error> {
    let (finish_at, expire_at) = get_finish_and_expire_time(j)?;
    if finish_at > since {
        warn!(
            "Warning: Found Job finished in the future. This is likely due to time skew in the cluster. Job cleanup will be deferred. job={}",
            job_ref(j)
        );
    }
    let remaining = expire_at - since;
    debug!(
        "Found Job finished job={} finishTime={finish_at} remainingTTL={remaining} startTime={since} deadlineTTL={expire_at}",
        job_ref(j)
    );
    Ok((remaining, expire_at))
}

/// `cache.MetaNamespaceKeyFunc`.
fn job_key(j: &Job) -> String {
    match j.metadata.namespace.as_deref() {
        Some(ns) if !ns.is_empty() => format!("{ns}/{}", j.metadata.name),
        _ => j.metadata.name.clone(),
    }
}

/// `cache.SplitMetaNamespaceKey`.
fn split_meta_namespace_key(key: &str) -> Result<(&str, &str), Error> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        [name] => Ok(("", name)),
        [ns, name] => Ok((ns, name)),
        _ => Err(Error::Internal(format!("unexpected key format: {key:?}"))),
    }
}

type JobReflector = Reflector<Job, fn(&Job) -> String>;

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// `Controller`.
pub struct TTLAfterFinishedController<S: Storage> {
    storage: Arc<S>,
    /// `jobInformer` (`jLister` reads its store).
    jobs: Arc<JobReflector>,
    /// `queue`: Jobs whose TTL the controller will check and attempt to delete
    /// when it expires.
    queue: WorkQueue,
    /// `clock`.
    clock: Clock,
}

impl<S: Storage + 'static> TTLAfterFinishedController<S> {
    /// `New`.
    pub fn new(storage: Arc<S>) -> Self {
        register_metrics();
        let lw = StorageListWatch::<S, Job>::new(storage.clone(), build_prefix("jobs", None));
        Self {
            storage,
            jobs: Arc::new(Reflector::new(Arc::new(lw), job_key as fn(&Job) -> String)),
            queue: WorkQueue::named(QUEUE_NAME),
            clock: Arc::new(Utc::now),
        }
    }

    /// Replace the clock (`tc.clock`); tests only.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The controller's work queue (shared with tests).
    pub fn queue(&self) -> &WorkQueue {
        &self.queue
    }

    /// `addJob` / `updateJob`: enqueue a Job with no `deletionTimestamp` that
    /// `needsCleanup`. The informer has no `DeleteFunc`.
    pub async fn on_job_event(&self, event: &StoreEvent<Job>) {
        match event {
            StoreEvent::Added(job) | StoreEvent::Modified(job) => {
                debug!("Adding/updating job {}", job_ref(job));
                if job.metadata.deletion_timestamp.is_none() && needs_cleanup(job) {
                    self.enqueue(job).await;
                }
            }
            StoreEvent::Deleted(_) => {}
        }
    }

    async fn enqueue(&self, job: &Job) {
        debug!("Add job to cleanup {}", job_ref(job));
        self.queue.add(job_key(job)).await;
    }

    /// `enqueueAfter`.
    async fn enqueue_after(&self, job: &Job, after: std::time::Duration) {
        self.queue.add_after(job_key(job), after).await;
    }

    async fn enqueue_all(&self) {
        for job in self.jobs.store().items() {
            self.on_job_event(&StoreEvent::Added(job)).await;
        }
    }

    /// Start the informer and its event handler. The subscription is taken
    /// before the reflector runs so no event is missed.
    pub fn start_informer(self: &Arc<Self>) {
        let mut rx = self.jobs.subscribe();
        let r = self.jobs.clone();
        tokio::spawn(async move { r.run().await });
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => this.on_job_event(&ev).await,
                    Err(RecvError::Lagged(n)) => {
                        warn!("ttl-after-finished Job events lagged {n}; resyncing");
                        this.enqueue_all().await;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    /// `Run`: starts the informer, waits for its cache to sync, then runs
    /// `workers` workers over the one queue until it is shut down.
    pub async fn run_with_workers(self: Arc<Self>, workers: usize) -> Result<()> {
        info!("Starting TTL after finished controller");
        self.start_informer();
        // `WaitForNamedCacheSync(jListerSynced)`.
        self.jobs.wait_for_cache_sync().await;
        // The informer's initial list is delivered as Add events.
        self.enqueue_all().await;

        spawn_workers(workers, &self.queue, {
            let this = Arc::clone(&self);
            move |_queue| {
                let this = Arc::clone(&this);
                async move { while this.process_next_work_item().await {} }
            }
        });
        // `<-ctx.Done()`: the workers run until the queue is shut down.
        std::future::pending::<()>().await;
        info!("Shutting down TTL after finished controller");
        Ok(())
    }

    /// `Run` with `ConcurrentTTLSyncs` workers.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.run_with_workers(CONCURRENT_TTL_SYNCS).await
    }

    /// `processNextWorkItem`: `false` when the queue is shut down.
    pub async fn process_next_work_item(&self) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        let result = self.process_job(&key).await;
        self.handle_err(result, &key).await;
        self.queue.done(&key).await;
        true
    }

    /// `handleErr`.
    async fn handle_err(&self, err: Result<(), Error>, key: &str) {
        match err {
            Ok(()) => self.queue.forget(key).await,
            Err(e) => {
                error!("error cleaning up Job {key}, will retry: {e}");
                self.queue.requeue_rate_limited(key.to_string()).await;
            }
        }
    }

    /// `processJob`: check the Job's state and TTL and delete the Job when it
    /// finishes and its TTL after finished has expired. If the Job hasn't
    /// finished or its TTL hasn't expired, it is added to the queue after the
    /// TTL is expected to expire. Not meant to be invoked concurrently with the
    /// same key (the queue guarantees it).
    pub async fn process_job(&self, key: &str) -> Result<(), Error> {
        let (namespace, name) = split_meta_namespace_key(key)?;

        // Ignore the Jobs that are already deleted or being deleted, or the
        // ones that don't need clean up.
        let job = self.jobs.store().get(key);
        debug!("Checking if Job is ready for cleanup {key}");
        // `errors.IsNotFound(err)`.
        let Some(job) = job else {
            return Ok(());
        };

        let Some(expired_at) = self.process_ttl(&job).await? else {
            return Ok(());
        };
        let _ = expired_at;

        // The Job's TTL is assumed to have expired, but the Job TTL might be
        // stale. Before deleting the Job, do a final sanity check. If TTL is
        // modified before we do this check, we cannot be sure if the TTL truly
        // expires. The latest Job may have a different UID, but it's fine
        // because the checks will be run again.
        let storage_key = build_key("jobs", (!namespace.is_empty()).then_some(namespace), name);
        let fresh: Job = match self.storage.get(&storage_key).await {
            Ok(j) => j,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        // Use the latest Job TTL to see if the TTL truly expires.
        let Some(expired_at) = self.process_ttl(&fresh).await? else {
            return Ok(());
        };
        // Cascade deletes the Jobs if TTL truly expires.
        let preconditions = Preconditions::new_uid(fresh.metadata.uid.clone());
        debug!("Cleaning up Job {}", job_ref(&fresh));
        self.storage
            .delete_foreground_with_preconditions(&storage_key, &preconditions)
            .await?;
        // `time.Since(*expiredAt).Seconds()`.
        let since = (self.clock)() - expired_at;
        JOB_DELETION_DURATION_SECONDS.observe(since.num_milliseconds() as f64 / 1000.0);
        Ok(())
    }

    /// `processTTL`: whether a given Job's TTL has expired (returns the expiry
    /// time), adding it to the queue after the TTL is expected to expire if the
    /// TTL will expire later.
    pub async fn process_ttl(&self, job: &Job) -> Result<Option<DateTime<Utc>>, Error> {
        // We don't care about the Jobs that are going to be deleted, or the
        // ones that don't need clean up.
        if job.metadata.deletion_timestamp.is_some() || !needs_cleanup(job) {
            return Ok(None);
        }

        let now = (self.clock)();
        let (t, e) = time_left(job, now)?;

        // TTL has expired.
        if t <= Duration::zero() {
            return Ok(Some(e));
        }

        // `t` is positive here.
        self.enqueue_after(job, t.to_std().unwrap_or_default())
            .await;
        Ok(None)
    }
}
