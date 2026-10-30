// TTL Controller - Automatic cleanup of completed Jobs based on TTL
//
// Implements:
// - TTL after finished for Jobs
// - Automatic deletion of finished Jobs after specified time
// - Cleanup of associated Pods

use crate::controllers::worker_pool::spawn_workers;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use rusternetes_common::resources::workloads::Job;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tokio::time::{sleep, Duration as TokioDuration};
use tracing::{debug, error, info, warn};

/// Workers draining this controller's queue: upstream `ConcurrentTTLSyncs (ttl-after-finished)` default,
/// pkg/controller/ttlafterfinished/config/v1alpha1/defaults.go:33-34, launched at cmd/kube-controller-manager/app/core.go:927.
const CONCURRENT_TTL_SYNCS: usize = 5;

/// TTL Controller for automatic cleanup of finished Jobs
pub struct TTLController<S: Storage> {
    storage: Arc<S>,
    /// How often to check for expired Jobs
    check_interval: TokioDuration,
}

impl<S: Storage + 'static> TTLController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            check_interval: TokioDuration::from_secs(60), // Check every 60 seconds
        }
    }

    /// Watch-based run loop. Watches jobs as primary resource.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) {
        info!("Starting TTL Controller");

        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentTTLSyncs (ttl-after-finished)` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_TTL_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("jobs", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    sleep(self.check_interval).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Check all Jobs and cleanup expired ones
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (Some(parts[1]), parts[2]),
                2 => (None, parts[1]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("jobs", ns, name);
            match self.storage.get::<Job>(&storage_key).await {
                Ok(job) => match self.check_job_ttl(&job).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("TTL check failed for {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Job>("/registry/jobs/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("jobs/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list jobs for enqueue: {}", e);
            }
        }
    }

    /// Check a single Job's TTL and clean up if expired.
    async fn check_job_ttl(&self, job: &Job) -> rusternetes_common::Result<()> {
        let now = Utc::now();
        if let Some(ttl_seconds) = self.get_ttl_seconds_after_finished(job) {
            if self.should_cleanup(job, ttl_seconds, now).await {
                self.cleanup_job(job).await?;
                info!(
                    "TTL cleanup: deleted job {}/{}",
                    job.metadata.namespace.as_deref().unwrap_or("default"),
                    job.metadata.name
                );
            }
        }
        Ok(())
    }

    pub async fn check_and_cleanup(&self) -> rusternetes_common::Result<()> {
        debug!("Checking for expired Jobs");

        // List all Jobs across all namespaces
        let prefix = build_prefix("jobs", None);
        let jobs: Vec<Job> = self.storage.list(&prefix).await?;

        let now = Utc::now();
        let mut cleaned_count = 0;

        for job in jobs {
            if let Some(ttl_seconds) = self.get_ttl_seconds_after_finished(&job) {
                if self.should_cleanup(&job, ttl_seconds, now).await {
                    if let Err(e) = self.cleanup_job(&job).await {
                        error!(
                            "Failed to cleanup job {}/{}: {}",
                            job.metadata.namespace.as_deref().unwrap_or("default"),
                            job.metadata.name,
                            e
                        );
                    } else {
                        cleaned_count += 1;
                    }
                }
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} expired Jobs", cleaned_count);
        }

        Ok(())
    }

    /// Get the effective `ttlSecondsAfterFinished` for a Job.
    ///
    /// Upstream's ttl-after-finished controller reads the typed
    /// `JobSpec.TTLSecondsAfterFinished` field
    /// (`pkg/controller/ttlafterfinished`: `needsCleanup` /
    /// `getFinishAndExpireTime`). We prefer that field, falling back to the
    /// legacy `ttlSecondsAfterFinished` annotation for objects that only carry
    /// it there.
    pub fn get_ttl_seconds_after_finished(&self, job: &Job) -> Option<i32> {
        if let Some(ttl) = job.spec.ttl_seconds_after_finished {
            return Some(ttl);
        }
        job.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("ttlSecondsAfterFinished"))
            .and_then(|v| v.parse().ok())
    }

    /// Check if a Job should be cleaned up
    pub async fn should_cleanup(&self, job: &Job, ttl_seconds: i32, now: DateTime<Utc>) -> bool {
        // Job must be finished (Complete or Failed)
        if !self.is_job_finished(job) {
            return false;
        }

        // Get the finish time
        if let Some(finish_time) = self.get_job_finish_time(job) {
            let ttl_duration = Duration::seconds(ttl_seconds as i64);
            let expiry_time = finish_time + ttl_duration;

            if now >= expiry_time {
                info!(
                    "Job {}/{} has exceeded TTL ({} seconds after finish at {})",
                    job.metadata.namespace.as_deref().unwrap_or("default"),
                    job.metadata.name,
                    ttl_seconds,
                    finish_time
                );
                return true;
            }
        }

        false
    }

    /// Check if a Job is finished
    pub fn is_job_finished(&self, job: &Job) -> bool {
        if let Some(status) = &job.status {
            if let Some(conditions) = &status.conditions {
                return conditions.iter().any(|c| {
                    matches!(c.condition_type.as_str(), "Complete" | "Failed") && c.status == "True"
                });
            }
        }
        false
    }

    /// Get the finish time of a Job
    pub fn get_job_finish_time(&self, job: &Job) -> Option<DateTime<Utc>> {
        if let Some(status) = &job.status {
            if let Some(conditions) = &status.conditions {
                // Find the Complete or Failed condition
                for condition in conditions {
                    if matches!(condition.condition_type.as_str(), "Complete" | "Failed")
                        && condition.status == "True"
                    {
                        return condition.last_transition_time;
                    }
                }
            }
        }
        None
    }

    /// Cleanup a Job and its associated Pods
    async fn cleanup_job(&self, job: &Job) -> rusternetes_common::Result<()> {
        let namespace = job.metadata.namespace.as_deref().unwrap_or("default");
        let name = &job.metadata.name;

        info!("Cleaning up Job: {}/{}", namespace, name);

        // Delete associated Pods first (cascade deletion)
        if let Err(e) = self.delete_job_pods(namespace, &job.metadata.uid).await {
            warn!(
                "Failed to delete pods for job {}/{}: {}",
                namespace, name, e
            );
        }

        // Delete the Job itself
        let key = build_key("jobs", Some(namespace), name);
        self.storage.delete(&key).await?;

        info!("Successfully cleaned up Job: {}/{}", namespace, name);
        Ok(())
    }

    /// Delete all Pods owned by a Job
    async fn delete_job_pods(
        &self,
        namespace: &str,
        job_uid: &str,
    ) -> rusternetes_common::Result<()> {
        use rusternetes_common::resources::Pod;

        let prefix = build_prefix("pods", Some(namespace));
        let pods: Vec<Pod> = self.storage.list(&prefix).await?;

        for pod in pods {
            // Check if this Pod is owned by the Job
            if let Some(owner_refs) = &pod.metadata.owner_references {
                if owner_refs.iter().any(|o| o.uid == job_uid) {
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    if let Err(e) = self.storage.delete(&pod_key).await {
                        warn!("Failed to delete pod {}: {}", pod.metadata.name, e);
                    } else {
                        debug!("Deleted pod {}", pod.metadata.name);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Job spec extension for TTL
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpecWithTTL {
    /// Duration in seconds the job can be active before the system tries to terminate it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,

    /// Clean up finished Jobs (complete or failed) after this time (in seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds_after_finished: Option<i32>,
}
