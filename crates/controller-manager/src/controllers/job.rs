use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::workloads::{
    Job, JobCondition, JobStatus, UncountedTerminatedPods,
};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{OwnerReference, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};

use super::job_tracking::{
    clean_uncounted_pods_without_finalizers, has_job_tracking_finalizer, push_uncounted_failed,
    push_uncounted_succeeded, remove_tracking_finalizer, uncounted_has_failed,
    uncounted_has_succeeded, FinalizerExpectations, JOB_TRACKING_FINALIZER,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct JobController<S: Storage> {
    storage: Arc<S>,
    /// Pod UIDs whose tracking-finalizer removal has been issued but not yet
    /// observed. Upstream's `uidTrackingExpectations`
    /// (`pkg/controller/job/tracking_utils.go:48`) — the brake that stops a
    /// stale pod list from claiming the same termination twice.
    finalizer_expectations: FinalizerExpectations,
}

/// Cap on how many UIDs one pass may park in `.status.uncountedTerminatedPods`.
///
/// Upstream `MaxUncountedPods = 500` (`pkg/controller/job/job_controller.go:76`),
/// which stops at the cap with the reasoning: "1. Ensure that the UIDs
/// representation are under 20 KB. 2. Cap the number of finalizer removals so
/// that syncing of big Jobs doesn't starve smaller ones." The remaining pods
/// are picked up on the next pass — the status write and the pod updates
/// re-enqueue the Job anyway.
const MAX_UNCOUNTED_PODS: usize = 500;

/// What one pass of the tracking protocol concluded.
///
/// The two pairs of counters are deliberately distinct, and conflating them
/// double-counts every pod. `.status.succeeded` / `.status.failed` hold only
/// what has been *counted* — the API contract says so outright
/// (`staging/src/k8s.io/api/batch/v1/types.go:588`: "UncountedTerminatedPods
/// holds UIDs of Pods that have terminated but haven't been accounted in Job
/// status counters"). The decision values add the parked UIDs on top, mirroring
/// upstream's `jobCtx.succeeded` / `jobCtx.failed` (`job_controller.go:925-926`).
struct TrackedPods {
    /// What to persist in `.status.succeeded`: counted pods only.
    status_succeeded: Option<i32>,
    /// What to persist in `.status.failed`: counted pods only.
    status_failed: Option<i32>,
    /// Successes for completion decisions: counted plus parked.
    succeeded: i32,
    /// Failures for backoff decisions: counted plus parked.
    failed: i32,
    /// The list to persist in `.status.uncountedTerminatedPods`.
    uncounted: UncountedTerminatedPods,
    /// Pods whose tracking finalizer must come off — but only AFTER the status
    /// above has been written, never before.
    to_release: Vec<Pod>,
}

/// Terminal-failure conditions for a Job, in the order the api-server demands.
///
/// Upstream stages a failing Job in two conditions: the interim
/// `FailureTarget` is appended first, and the final `Failed` carries the SAME
/// reason and message (`job_controller.go:1307-1316` ->
/// `newFailedConditionForFailureTarget`, `job_controller.go:1562`). Validation
/// enforces the pair — `pkg/apis/batch/validation/validation.go:520-522`:
///
/// ```text
/// status.conditions: Invalid value: cannot set Failed=True condition
///   without the FailureTarget=true condition
/// ```
///
/// so a lone `Failed=True` makes the api-server reject the whole status write,
/// the Job stays `active: 1`, and every spec that waits for it to fail hangs.
/// Terminal-success conditions for a Job, in the order the api-server demands.
///
/// The mirror of [`failed_job_conditions`]: the interim `SuccessCriteriaMet`
/// is appended first and `Complete` inherits its reason and message
/// (`job_controller.go:1317-1327`). Validation enforces the pair —
/// `pkg/apis/batch/validation/validation.go:525-527`:
///
/// ```text
/// status.conditions: Invalid value: cannot set Complete=True condition
///   without the SuccessCriteriaMet=true condition
/// ```
///
/// A lone `Complete=True` is rejected, so the Job never leaves `active`.
/// Does this pod count as a failure for its Job?
///
/// Port of upstream `isPodFailed` (`pkg/controller/job/job_controller.go:2011`).
/// The second clause is the one that is easy to miss and expensive to omit:
///
/// ```text
/// // Count deleted Pods as failures to account for orphan Pods that
/// // never have a chance to reach the Failed phase.
/// return p.DeletionTimestamp != nil && p.Status.Phase != v1.PodSucceeded
/// ```
///
/// A pod that is deleted while still Running never reports a terminal phase, so
/// without this it would hold its tracking finalizer forever and sit in
/// `Terminating` — the Job controller waiting for a phase the kubelet will
/// never write. `podReplacementPolicy: Failed` opts out, because there the Job
/// deliberately waits for the real terminal phase before replacing the pod.
fn is_pod_failed(pod: &Pod, only_replace_failed_pods: bool) -> bool {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
    if matches!(phase, Some(Phase::Failed)) {
        return true;
    }
    if only_replace_failed_pods {
        return false;
    }
    pod.metadata.deletion_timestamp.is_some() && !matches!(phase, Some(Phase::Succeeded))
}

/// Has this Job reached a terminal condition?
fn job_is_finished(job: &Job) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                (c.condition_type == "Complete" || c.condition_type == "Failed")
                    && c.status == "True"
            })
        })
}

fn complete_job_conditions(reason: String, message: String) -> Vec<JobCondition> {
    let now = chrono::Utc::now();
    vec![
        JobCondition {
            condition_type: "SuccessCriteriaMet".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(reason.clone()),
            message: Some(message.clone()),
        },
        JobCondition {
            condition_type: "Complete".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(reason),
            message: Some(message),
        },
    ]
}

fn failed_job_conditions(reason: String, message: String) -> Vec<JobCondition> {
    let now = chrono::Utc::now();
    vec![
        JobCondition {
            condition_type: "FailureTarget".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(reason.clone()),
            message: Some(message.clone()),
        },
        JobCondition {
            condition_type: "Failed".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(reason),
            message: Some(message),
        },
    ]
}

/// Raise outgoing Job status counters to the persisted values so they never
/// decrease.
///
/// Upstream's api-server refuses a status update that lowers `failed` or
/// `succeeded` — `RejectDecreasingFailedCounter` /
/// `RejectDecreasingSucceededCounter` in
/// `pkg/apis/batch/validation/validation.go:722-730` — with
/// `status.failed: Invalid value: 0: cannot decrease the failed counter`.
///
/// This controller recomputes both counters from the live pod list on every
/// reconcile and additionally subtracts pods matched by an `Ignore`
/// podFailurePolicy rule, so a count that was already written can drop back
/// down: the pods carrying those failures get deleted, or the Ignore rule only
/// matches once the `DisruptionTarget` condition is observed. The first such
/// write is rejected and so is every write after it, so the terminal
/// `Complete=True` condition never lands and the Job hangs until the e2e
/// timeout (#1955).
///
/// Upstream never needs this clamp because it accumulates the counters in the
/// Job status and gates each pod's accounting on the
/// `batch.kubernetes.io/job-tracking` finalizer, so a failure is counted
/// exactly once and an ignored one is never counted at all. Porting that
/// accounting is tracked separately; until then, keeping the counters
/// monotonic is what the API contract requires.
fn clamp_counters_monotonic(next: &mut JobStatus, persisted: Option<&JobStatus>) {
    let Some(old) = persisted else {
        return;
    };
    if next.failed.unwrap_or(0) < old.failed.unwrap_or(0) {
        next.failed = old.failed;
    }
    if next.succeeded.unwrap_or(0) < old.succeeded.unwrap_or(0) {
        next.succeeded = old.succeeded;
    }
}

impl<S: Storage + 'static> JobController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            finalizer_expectations: FinalizerExpectations::new(),
        }
    }

    /// Phase 1 of the exactly-once protocol: claim every terminal pod that is
    /// still held by the tracking finalizer.
    ///
    /// Port of the accounting half of upstream's
    /// `trackJobStatusAndRemoveFinalizers` (`pkg/controller/job/job_controller.go`),
    /// which builds `uidsWithFinalizer`, folds already-released UIDs into the
    /// real counters via `cleanUncountedPodsWithoutFinalizers`, and appends
    /// newly-finished pods to `.status.uncountedTerminatedPods`.
    ///
    /// The counters it returns are cumulative — upstream's
    ///
    /// ```text
    /// jobCtx.succeeded = job.Status.Succeeded + int32(len(newSucceededPods)) +
    ///     int32(len(jobCtx.uncounted.succeeded))
    /// ```
    ///
    /// — so they cannot fall when a pod is deleted, and a pod that terminates
    /// and is collected between two passes is still counted, because it was
    /// held in place until its UID was written down.
    fn track_terminated_pods(
        &self,
        job_key: &str,
        persisted: Option<&JobStatus>,
        job_pods: &[Pod],
        never_count_failed: &HashSet<String>,
        is_indexed: bool,
        only_replace_failed_pods: bool,
    ) -> TrackedPods {
        // Upstream satisfies an expectation when its informer delivers the pod
        // without the finalizer (`finalizerRemovalObserved`). Our equivalent
        // signal is this list: a pod that no longer carries the finalizer — or
        // that is gone entirely — has had its removal observed.
        let still_held: HashSet<&str> = job_pods
            .iter()
            .filter(|p| has_job_tracking_finalizer(p))
            .map(|p| p.metadata.uid.as_str())
            .collect();
        for uid in self.finalizer_expectations.expected(job_key) {
            if !still_held.contains(uid.as_str()) {
                self.finalizer_expectations.removal_observed(job_key, &uid);
            }
        }
        let expected_removed = self.finalizer_expectations.expected(job_key);

        // A pod with a removal in flight is NOT counted as holding the
        // finalizer, exactly as upstream excludes `expectedRmFinalizers` when
        // building `uidsWithFinalizer`.
        let uids_with_finalizer: HashSet<String> = job_pods
            .iter()
            .filter(|p| {
                has_job_tracking_finalizer(p) && !expected_removed.contains(&p.metadata.uid)
            })
            .map(|p| p.metadata.uid.clone())
            .collect();

        let mut base_succeeded = persisted.and_then(|s| s.succeeded);
        let mut base_failed = persisted.and_then(|s| s.failed);
        let mut uncounted = persisted
            .and_then(|s| s.uncounted_terminated_pods.clone())
            .unwrap_or(UncountedTerminatedPods {
                succeeded: None,
                failed: None,
            });

        // Phase 3 of the previous pass: UIDs whose finalizer removal has landed
        // become real counter increments.
        clean_uncounted_pods_without_finalizers(
            &mut base_succeeded,
            &mut base_failed,
            &mut uncounted,
            &uids_with_finalizer,
        );

        // Phase 1 of this pass: claim newly-finished pods.
        let mut to_release: Vec<Pod> = Vec::new();
        for pod in job_pods.iter() {
            if !has_job_tracking_finalizer(pod) || expected_removed.contains(&pod.metadata.uid) {
                continue;
            }
            let uid = pod.metadata.uid.as_str();
            let phase = if matches!(
                pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Succeeded)
            ) {
                Some(Phase::Succeeded)
            } else if is_pod_failed(pod, only_replace_failed_pods) {
                Some(Phase::Failed)
            } else {
                None
            };
            match phase {
                Some(Phase::Succeeded) => {
                    // An Indexed Job tracks successes by completion index in
                    // `.status.completedIndexes`, which is durable on its own,
                    // so upstream never parks their UIDs: "The completion index
                    // is enough to avoid recounting succeeded pods. No need to
                    // track UIDs."
                    if !is_indexed && !uncounted_has_succeeded(&uncounted, uid) {
                        push_uncounted_succeeded(&mut uncounted, uid);
                    }
                    to_release.push(pod.clone());
                }
                Some(Phase::Failed) => {
                    // An excluded failure is still released — it just never
                    // reaches a counter. Upstream's `Ignore` action does the
                    // same: the pod goes into `podsToRemoveFinalizer` without
                    // ever being appended to the uncounted list.
                    if !never_count_failed.contains(&pod.metadata.name)
                        && !uncounted_has_failed(&uncounted, uid)
                    {
                        push_uncounted_failed(&mut uncounted, uid);
                    }
                    to_release.push(pod.clone());
                }
                _ => {}
            }

            if uncounted.succeeded.as_ref().map_or(0, |v| v.len())
                + uncounted.failed.as_ref().map_or(0, |v| v.len())
                >= MAX_UNCOUNTED_PODS
            {
                break;
            }
        }

        let succeeded = base_succeeded.unwrap_or(0)
            + uncounted.succeeded.as_ref().map_or(0, |v| v.len() as i32);
        let failed =
            base_failed.unwrap_or(0) + uncounted.failed.as_ref().map_or(0, |v| v.len() as i32);

        TrackedPods {
            status_succeeded: Some(base_succeeded.unwrap_or(0)),
            status_failed: Some(base_failed.unwrap_or(0)),
            succeeded,
            failed,
            uncounted,
            to_release,
        }
    }

    /// Phase 2 of the exactly-once protocol: release the pods whose outcome the
    /// status write just recorded.
    ///
    /// Ordering is the whole point — upstream's
    /// `flushUncountedAndRemoveFinalizers` writes the status FIRST and only
    /// then removes finalizers, so a crash in between leaves a pod that is
    /// still held and still claimable rather than one that is gone and lost.
    /// Call this only after the status write has succeeded.
    /// Returns the UIDs whose finalizer is confirmed gone, so the caller can
    /// fold exactly those into the counters — upstream deletes the same UIDs
    /// from `uidsWithFinalizer` before re-running
    /// `cleanUncountedPodsWithoutFinalizers`.
    async fn release_tracked_pods(
        &self,
        job_key: &str,
        namespace: &str,
        pods: &[Pod],
    ) -> HashSet<String> {
        let mut released = HashSet::new();
        for pod in pods {
            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
            let Ok(mut fresh) = self.storage.get::<Pod>(&pod_key).await else {
                // Already gone: nothing holds it, so the expectation is moot.
                self.finalizer_expectations
                    .removal_observed(job_key, &pod.metadata.uid);
                released.insert(pod.metadata.uid.clone());
                continue;
            };
            if !remove_tracking_finalizer(&mut fresh) {
                released.insert(pod.metadata.uid.clone());
                continue;
            }
            // Record the expectation BEFORE the write, as upstream does: if the
            // write lands but our next list is stale, the expectation is what
            // stops the pod being counted a second time.
            self.finalizer_expectations
                .expect_removed(job_key, [pod.metadata.uid.clone()]);
            if let Err(e) = self.storage.update(&pod_key, &fresh).await {
                // The removal did not happen, so it must not stay expected —
                // otherwise the pod is skipped forever and its outcome is lost.
                self.finalizer_expectations
                    .removal_observed(job_key, &pod.metadata.uid);
                warn!(
                    "Failed to remove the job-tracking finalizer from pod {}: {}",
                    pod.metadata.name, e
                );
            } else {
                released.insert(pod.metadata.uid.clone());
            }
        }
        released
    }

    /// Write `.status`, release the pods it accounted for, then fold those
    /// releases into the counters and write again.
    ///
    /// These three steps and their order are upstream's
    /// `flushUncountedAndRemoveFinalizers` (`job_controller.go:1388`): flush
    /// first so a crash leaves a pod still held and still claimable rather than
    /// gone and forgotten, remove the finalizers, then re-run
    /// `cleanUncountedPodsWithoutFinalizers` over the UIDs that were actually
    /// released — in the SAME pass, so the visible counters converge here
    /// instead of waiting for a sync that a finished Job never gets.
    async fn flush_status_and_release(
        &self,
        key: &str,
        job_tracking_key: &str,
        namespace: &str,
        job: &mut Job,
        pods_to_release: &[Pod],
        job_pods: &[Pod],
    ) -> Result<()> {
        self.write_status(key, job).await?;

        // Upstream's `canRemoveFinalizer` (`job_controller.go:1359`) short-
        // circuits to true the moment the Job is being deleted or has reached a
        // terminal condition: nothing more will ever be counted, so holding the
        // pods back only wedges them in `Terminating`.
        let mut to_release: Vec<Pod> = pods_to_release.to_vec();
        if job.metadata.is_being_deleted() || job_is_finished(job) {
            let already: HashSet<&str> = pods_to_release
                .iter()
                .map(|p| p.metadata.uid.as_str())
                .collect();
            for pod in job_pods {
                if has_job_tracking_finalizer(pod) && !already.contains(pod.metadata.uid.as_str()) {
                    to_release.push(pod.clone());
                }
            }
        }

        let released = self
            .release_tracked_pods(job_tracking_key, namespace, &to_release)
            .await;
        if released.is_empty() {
            return Ok(());
        }

        let mut folded = false;
        if let Some(status) = job.status.as_mut() {
            if let Some(mut uncounted) = status.uncounted_terminated_pods.take() {
                // Everything still parked that was NOT released stays parked.
                let still_held: HashSet<String> = uncounted
                    .succeeded
                    .iter()
                    .chain(uncounted.failed.iter())
                    .flatten()
                    .filter(|uid| !released.contains(*uid))
                    .cloned()
                    .collect();
                folded = clean_uncounted_pods_without_finalizers(
                    &mut status.succeeded,
                    &mut status.failed,
                    &mut uncounted,
                    &still_held,
                );
                let empty = uncounted.succeeded.as_ref().map_or(0, |v| v.len())
                    + uncounted.failed.as_ref().map_or(0, |v| v.len())
                    == 0;
                status.uncounted_terminated_pods = if empty { None } else { Some(uncounted) };
            }
        }
        if folded {
            self.write_status(key, job).await?;
        }
        Ok(())
    }

    /// Persist `job.status`, retrying a CAS conflict, and skipping the write
    /// entirely when nothing changed (a redundant write wakes every watcher).
    async fn write_status(&self, key: &str, job: &mut Job) -> Result<()> {
        let status_to_save = job.status.clone();
        for attempt in 0..3 {
            match self.storage.get::<Job>(key).await {
                Ok(mut fresh_job) => {
                    // Counters must not go backwards against what is already
                    // persisted, or the api-server refuses this and every later
                    // write (#1955).
                    let mut next_status = status_to_save.clone();
                    if let Some(next) = next_status.as_mut() {
                        clamp_counters_monotonic(next, fresh_job.status.as_ref());
                    }
                    if fresh_job.status == next_status {
                        job.status = next_status;
                        return Ok(());
                    }
                    fresh_job.status = next_status.clone();
                    // Status subresource write (#1723).
                    match self.storage.update_status(key, &fresh_job).await {
                        Ok(_) => {
                            job.status = next_status;
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                "Job status update CAS conflict on {} (attempt {}): {}",
                                key,
                                attempt + 1,
                                e
                            );
                            if attempt == 2 {
                                return Err(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    // The Job was deleted between the list and this write.
                    debug!("Job {} no longer exists: {}", key, e);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting JobController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to Jobs AND Pods
            let prefix = "/registry/jobs/";
            let watch_result = self.storage.watch(prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(Duration::from_secs(5));
            resync.tick().await; // consume first immediate tick

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
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_job(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Pod watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
            // Watch broke — loop back to re-establish
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("jobs", Some(ns), name);
            match self.storage.get::<Job>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
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
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("jobs/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list jobs for enqueue: {}", e);
            }
        }
    }

    /// When a pod changes, check its ownerReferences for a Job owner
    /// and enqueue that Job for reconciliation.
    async fn enqueue_owner_job(&self, queue: &WorkQueue, event: &rusternetes_storage::WatchEvent) {
        let pod_key = extract_key(event);
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        let storage_key = format!("/registry/{}", pod_key);
        match self.storage.get::<Pod>(&storage_key).await {
            Ok(pod) => {
                if let Some(refs) = &pod.metadata.owner_references {
                    for owner_ref in refs {
                        if owner_ref.kind == "Job" {
                            queue.add(format!("jobs/{}/{}", ns, owner_ref.name)).await;
                        }
                    }
                }
            }
            Err(_) => {
                // Pod deleted — enqueue all Jobs in this namespace
                if let Ok(items) = self
                    .storage
                    .list::<Job>(&build_prefix("jobs", Some(ns)))
                    .await
                {
                    for job in &items {
                        queue
                            .add(format!("jobs/{}/{}", ns, job.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        let jobs: Vec<Job> = self.storage.list("/registry/jobs/").await?;

        for mut job in jobs {
            if let Err(e) = self.reconcile(&mut job).await {
                error!("Failed to reconcile Job {}: {}", job.metadata.name, e);
            }
        }

        self.reconcile_orphan_pods().await;

        Ok(())
    }

    /// Strip the tracking finalizer from pods whose Job can no longer account
    /// for them.
    ///
    /// Port of upstream's orphan-pod reconciler — `enqueueOrphanPod` /
    /// `syncOrphanPod` / `handleSingleOrphanPod`
    /// (`pkg/controller/job/job_controller.go:688-766`), which upstream reaches
    /// from its pod event handlers whenever a pod carrying the finalizer has no
    /// controller, or one whose Job is gone or finished: "syncJob will not
    /// remove this finalizer."
    ///
    /// Shipping the finalizer without this is what turns a Job deletion into a
    /// namespace stuck in `Terminating`: the garbage collector deletes the Job,
    /// `reconcile` never runs for it again, and its pods hold a finalizer that
    /// nobody is left to remove.
    async fn reconcile_orphan_pods(&self) {
        let Ok(pods) = self.storage.list::<Pod>("/registry/pods/").await else {
            return;
        };
        for pod in pods.iter().filter(|p| has_job_tracking_finalizer(p)) {
            let Some(namespace) = pod.metadata.namespace.as_deref() else {
                continue;
            };
            let owner = pod
                .metadata
                .owner_references
                .as_ref()
                .and_then(|refs| refs.iter().find(|r| r.controller == Some(true)));

            if let Some(owner) = owner {
                // A pod controlled by something that is not a batch/v1 Job is
                // not ours to strip.
                if owner.kind != "Job" || owner.api_version != "batch/v1" {
                    continue;
                }
                let job_key = build_key("jobs", Some(namespace), &owner.name);
                if let Ok(job) = self.storage.get::<Job>(&job_key).await {
                    if job.metadata.uid == owner.uid {
                        // Managed by an external controller: not ours either.
                        if job
                            .spec
                            .managed_by
                            .as_deref()
                            .is_some_and(|m| m != "kubernetes.io/job-controller")
                        {
                            continue;
                        }
                        // The Job is alive and still counting. Leave it alone.
                        if !job_is_finished(&job) {
                            continue;
                        }
                    }
                }
            }

            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
            let Ok(mut fresh) = self.storage.get::<Pod>(&pod_key).await else {
                continue;
            };
            if !remove_tracking_finalizer(&mut fresh) {
                continue;
            }
            if let Err(e) = self.storage.update(&pod_key, &fresh).await {
                warn!(
                    "Failed to release orphan pod {}/{} from the job-tracking finalizer: {}",
                    namespace, fresh.metadata.name, e
                );
            } else {
                debug!(
                    "Released orphan pod {}/{} from the job-tracking finalizer",
                    namespace, fresh.metadata.name
                );
            }
        }
    }

    async fn reconcile(&self, job: &mut Job) -> Result<()> {
        // Owned, not borrowed from `job`: the status flush needs `&mut job`
        // while these are still in use.
        let name = job.metadata.name.clone();
        let namespace = job.metadata.namespace.clone().unwrap();
        let name = name.as_str();
        let namespace = namespace.as_str();

        // Skip reconciliation for Jobs being deleted — GC handles pod cleanup.
        if job.metadata.is_being_deleted() {
            // But first let go of every pod this Job still holds. Nothing more
            // will ever be counted, and a pod left holding the tracking
            // finalizer can never be deleted — the Job's own teardown would
            // block forever. Upstream's `canRemoveFinalizer` returns true
            // outright when `jobCtx.job.DeletionTimestamp != nil`.
            let job_tracking_key = format!("{}/{}", namespace, name);
            let pod_prefix = format!("/registry/pods/{}/", namespace);
            if let Ok(all_pods) = self.storage.list::<Pod>(&pod_prefix).await {
                let held: Vec<Pod> = all_pods
                    .into_iter()
                    .filter(|p| {
                        has_job_tracking_finalizer(p)
                            && p.metadata
                                .owner_references
                                .as_ref()
                                .is_some_and(|refs| refs.iter().any(|r| r.uid == job.metadata.uid))
                    })
                    .collect();
                if !held.is_empty() {
                    self.release_tracked_pods(&job_tracking_key, namespace, &held)
                        .await;
                }
            }
            // Upstream drops the job's entry from `uidTrackingExpectations`
            // when the Job goes away (`deleteExpectations`); keeping it would
            // leak a growing set of UIDs for an object that no longer exists.
            self.finalizer_expectations.forget(&job_tracking_key);
            return Ok(());
        }

        // Honour spec.managedBy: when a Job is managed by an external controller
        // (anything other than the in-tree "kubernetes.io/job-controller"), the
        // in-tree controller must not act on it — no pod creation, no status
        // mutation. K8s ref: pkg/controller/job/job_controller.go syncJob
        // early-returns when controllerName != JobControllerName.
        const JOB_CONTROLLER_NAME: &str = "kubernetes.io/job-controller";
        if let Some(managed_by) = job.spec.managed_by.as_deref() {
            if managed_by != JOB_CONTROLLER_NAME {
                debug!(
                    "Skipping Job {}/{}: managedBy={} is not the in-tree controller",
                    namespace, name, managed_by
                );
                return Ok(());
            }
        }

        debug!("Reconciling Job {}/{}", namespace, name);

        // For completed/failed jobs, still update terminating count
        // (pods may still be shutting down after job completion).
        // K8s ref: pkg/controller/job/job_controller.go — syncJob continues
        // to update status.terminating for completed jobs.
        if let Some(ref status) = job.status {
            if let Some(ref conditions) = status.conditions {
                let is_finished = conditions.iter().any(|c| {
                    (c.condition_type == "Complete" || c.condition_type == "Failed")
                        && c.status == "True"
                });
                if is_finished {
                    // Honour spec.ttlSecondsAfterFinished: once the TTL has
                    // elapsed relative to the job's completionTime, mark the job
                    // for deletion (set deletionTimestamp). K8s ref:
                    // pkg/controller/ttlafterfinished — the TTL-after-finished
                    // controller deletes finished jobs whose TTL has expired.
                    if let Some(ttl) = job.spec.ttl_seconds_after_finished {
                        if let Some(completion_time) = status.completion_time {
                            let elapsed = chrono::Utc::now()
                                .signed_duration_since(completion_time)
                                .num_seconds();
                            if elapsed >= ttl as i64 && !job.metadata.is_being_deleted() {
                                info!(
                                    "Job {}/{} TTL ({}s) elapsed {}s after completion — marking for deletion",
                                    namespace, name, ttl, elapsed
                                );
                                let key = build_key("jobs", Some(namespace), name);
                                if let Ok(fresh_job) = self.storage.get::<Job>(&key).await {
                                    if fresh_job.metadata.deletion_timestamp.is_none() {
                                        // DELETE, never stamp deletionTimestamp:
                                        // the field is immutable on update, so an
                                        // api-server that enforces that rejects the
                                        // write and the finished Job is never
                                        // collected. Upstream's TTL controller
                                        // deletes through the API
                                        // (pkg/controller/ttlafterfinished/
                                        // ttlafterfinished_controller.go:256 ->
                                        // Jobs(ns).Delete). Same defect class
                                        // as #1812.
                                        let _ = self.storage.delete_gracefully(&key).await;
                                    }
                                }
                                return Ok(());
                            }
                        }
                    }
                    // Still update terminating count for finished jobs
                    let pod_prefix = format!("/registry/pods/{}/", namespace);
                    let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
                    let job_uid = &job.metadata.uid;
                    let terminating = all_pods
                        .iter()
                        .filter(|p| {
                            let owned = p.metadata.owner_references.as_ref().is_some_and(|refs| {
                                refs.iter().any(|r| r.uid == *job_uid && r.kind == "Job")
                            });
                            let is_terminating = p.metadata.deletion_timestamp.is_some()
                                && !matches!(
                                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                                    Some(Phase::Succeeded) | Some(Phase::Failed)
                                );
                            owned && is_terminating
                        })
                        .count() as i32;
                    // Update terminating count if it changed
                    if status.terminating != Some(terminating) {
                        let key = build_key("jobs", Some(namespace), name);
                        // Re-read for fresh resourceVersion to avoid CAS conflict
                        if let Ok(mut fresh_job) = self.storage.get::<Job>(&key).await {
                            if fresh_job.status.as_ref().and_then(|s| s.terminating)
                                != Some(terminating)
                            {
                                if let Some(ref mut s) = fresh_job.status {
                                    s.terminating = Some(terminating);
                                }
                                // Status subresource write (#1723).
                                let _ = self.storage.update_status(&key, &fresh_job).await;
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }

        let completions = job.spec.completions.unwrap_or(1);
        let parallelism = job.spec.parallelism.unwrap_or(1);
        let backoff_limit = job.spec.backoff_limit.unwrap_or(6);

        // Get current pods for this Job
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Find pods owned by this Job via ownerReferences (authoritative),
        // or matching selector labels (for orphan adoption).
        // Also fall back to job-name label matching for backwards compatibility.
        let job_uid = &job.metadata.uid;
        let selector_labels = job
            .spec
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.as_ref());
        let mut job_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                    .unwrap_or(false);
                if owned_by_ref {
                    return true;
                }
                // Check if the pod is an orphan (no controller ownerRef) that matches our selector
                let has_any_controller = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.controller.unwrap_or(false)))
                    .unwrap_or(false);
                if has_any_controller {
                    return false; // Pod is owned by another controller, skip
                }
                // Match by selector labels (primary) or job-name label (fallback)
                let pod_labels = pod.metadata.labels.as_ref();
                let matches_selector = selector_labels
                    .map(|sel| {
                        pod_labels
                            .map(|pl| sel.iter().all(|(k, v)| pl.get(k) == Some(v)))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                let matches_job_name = pod_labels
                    .and_then(|labels| labels.get("job-name"))
                    .map(|j| j == name)
                    .unwrap_or(false);
                matches_selector || matches_job_name
            })
            .collect();

        // Release pods whose labels no longer match the Job selector
        let selector = job
            .spec
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.as_ref());
        if selector.is_none() && !job_pods.is_empty() {
            debug!(
                "Job {}/{} has no matchLabels selector, skipping release check for {} pods",
                namespace,
                name,
                job_pods.len()
            );
        }
        for pod in &job_pods {
            if let Some(sel) = selector {
                let labels = pod.metadata.labels.as_ref();
                let matches = labels.is_some_and(|l| sel.iter().all(|(k, v)| l.get(k) == Some(v)));
                if !matches {
                    // Pod no longer matches the selector — RELEASE it, which
                    // upstream defines as removing the controller ownerRef and
                    // nothing else: `ReleasePod` issues one patch generated by
                    // `GenerateDeleteOwnerRefStrategicMergeBytes` and never
                    // touches the pod's lifecycle
                    // (pkg/controller/controller_ref_manager.go:238-264).
                    //
                    // We used to also stamp a deletionTimestamp here, citing
                    // upstream for it — that citation was wrong. Deleting a pod
                    // the user has deliberately detached is destructive, and it
                    // fails the Conformance spec "Job should adopt matching
                    // orphans and release non-matching pods", which strips the
                    // labels and then polls the pod until its controllerRef is
                    // nil (test/e2e/apps/job.go:960-973).
                    //
                    // CAS retry handles concurrent updates.
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    for _ in 0..3 {
                        match self.storage.get::<Pod>(&pod_key).await {
                            Ok(mut fresh_pod) => {
                                if let Some(ref mut refs) = fresh_pod.metadata.owner_references {
                                    refs.retain(|r| &r.uid != job_uid);
                                }
                                match self.storage.update(&pod_key, &fresh_pod).await {
                                    Ok(_) => {
                                        info!(
                                            "Released pod {} from job {}/{} (labels no longer match)",
                                            pod.metadata.name, namespace, name
                                        );
                                        break;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "CAS retry releasing pod {}: {}",
                                            pod.metadata.name, e
                                        );
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    continue;
                }
            }
        }

        // Adopt orphaned pods — re-add ownerReference if pod matches by label but not by ownerRef
        for slot in job_pods.iter_mut() {
            let has_owner_ref = slot
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                .unwrap_or(false);
            if has_owner_ref {
                continue;
            }
            let mut adopted_pod = slot.clone();
            let owner_ref = rusternetes_common::types::OwnerReference {
                api_version: "batch/v1".to_string(),
                kind: "Job".to_string(),
                name: name.to_string(),
                uid: job.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            };
            adopted_pod
                .metadata
                .owner_references
                .get_or_insert_with(Vec::new)
                .push(owner_ref);
            // Adoption also takes on the tracking finalizer, or the adopted pod
            // is invisible to the accounting protocol for the rest of its life.
            // Upstream does exactly this, with the comment "When adopting Pods,
            // this operation adds an ownerRef and finalizers"
            // (`job_controller.go:795-814`).
            if !has_job_tracking_finalizer(&adopted_pod) {
                adopted_pod
                    .metadata
                    .finalizers
                    .get_or_insert_with(Vec::new)
                    .push(JOB_TRACKING_FINALIZER.to_string());
            }
            let pod_key = build_key("pods", Some(namespace), &adopted_pod.metadata.name);
            if let Err(e) = self.storage.update(&pod_key, &adopted_pod).await {
                tracing::warn!("Failed to adopt pod {}: {}", adopted_pod.metadata.name, e);
            } else {
                info!(
                    "Adopted orphaned pod {} for job {}/{}",
                    adopted_pod.metadata.name, namespace, name
                );
                // Keep the local view in step, as upstream does, so this pass
                // already accounts for the pod it just adopted.
                *slot = adopted_pod;
            }
        }
        let job_pods = job_pods;

        let is_indexed = job.spec.completion_mode.as_deref() == Some("Indexed");

        // podFailurePolicy is evaluated BEFORE any counting, because an
        // `Ignore` match must stop a failure from ever being counted rather
        // than subtract it afterwards. Upstream reaches the same ordering via
        // `matchPodFailurePolicy` (`pkg/controller/job/pod_failure_policy.go`),
        // which returns `(nil, false, &ignore)` so the caller never appends the
        // UID to `.status.uncountedTerminatedPods` at all
        // (`job_controller.go` -> `trackJobStatusAndRemoveFinalizers`).
        // Build a set of indexes that failed due to FailIndex podFailurePolicy
        let mut fail_index_set: HashSet<i32> = HashSet::new();

        // Check podFailurePolicy — if a failed pod matches a FailJob rule, fail immediately
        // Also check for FailIndex rules
        let mut pod_failure_policy_triggered = false;
        let mut pod_failure_message = String::new();
        let mut ignored_pods: HashSet<String> = HashSet::new();
        if let Some(ref policy) = job.spec.pod_failure_policy {
            if !policy.rules.is_empty() {
                for pod in job_pods.iter() {
                    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    // Check ALL terminated pods (Failed AND Succeeded with non-zero exit).
                    // K8s podFailurePolicy evaluates against any pod with terminated containers,
                    // not just Failed phase pods. A pod can be Succeeded but have containers
                    // that exited with non-zero codes (if other containers succeeded).
                    if !matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
                        continue;
                    }
                    // Get container exit codes
                    let exit_codes: Vec<i32> = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .map(|cs| {
                            cs.iter()
                                .filter_map(|c| match &c.state {
                                    Some(
                                        rusternetes_common::resources::ContainerState::Terminated {
                                            exit_code,
                                            ..
                                        },
                                    ) => Some(*exit_code),
                                    _ => None,
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    for rule in policy.rules.iter() {
                        let action = rule.action.as_str();

                        let mut rule_matched = false;

                        // Check onExitCodes
                        if let Some(ref on_exit) = rule.on_exit_codes {
                            let operator = on_exit.operator.as_str();
                            let values = &on_exit.values;
                            rule_matched = exit_codes.iter().any(|code| match operator {
                                "In" => values.contains(code),
                                "NotIn" => !values.contains(code),
                                _ => false,
                            });
                        }

                        // Check onPodConditions
                        if !rule_matched && !rule.on_pod_conditions.is_empty() {
                            let pod_conditions =
                                pod.status.as_ref().and_then(|s| s.conditions.as_ref());
                            rule_matched = rule.on_pod_conditions.iter().any(|cond| {
                                let ctype = cond.condition_type.as_str();
                                let cstatus = cond.status.as_deref().unwrap_or("True");
                                pod_conditions
                                    .map(|pcs| {
                                        pcs.iter().any(|pc| {
                                            pc.condition_type == ctype && pc.status == cstatus
                                        })
                                    })
                                    .unwrap_or(false)
                            });
                        }

                        if rule_matched {
                            match action {
                                "FailJob" => {
                                    pod_failure_policy_triggered = true;
                                    pod_failure_message =
                                        "Pod failed with exit code matching FailJob rule"
                                            .to_string();
                                    break;
                                }
                                "FailIndex" => {
                                    if is_indexed {
                                        if let Some(idx) = get_pod_index(pod) {
                                            fail_index_set.insert(idx);
                                        }
                                    }
                                    break;
                                }
                                "Ignore" => {
                                    ignored_pods.insert(pod.metadata.name.clone());
                                    break;
                                }
                                _ => {
                                    // "Count" and other actions: count normally against backoff
                                    break;
                                }
                            }
                        }
                    }
                    if pod_failure_policy_triggered {
                        break;
                    }
                }
            }
        }

        // Indexes that have EVER succeeded. Upstream keeps this in
        // `.status.completedIndexes` precisely so it outlives the pods:
        // `calculateSucceededIndexes` (`pkg/controller/job/indexed_job_utils.go`)
        // unions the persisted string with the succeeded pods it can still see.
        // Recomputing it from the live list alone loses an index the moment its
        // pod is collected.
        let succeeded_index_set: HashSet<i32> = if is_indexed {
            let mut set = parse_index_ranges(
                job.status
                    .as_ref()
                    .and_then(|s| s.completed_indexes.as_deref())
                    .unwrap_or(""),
            );
            set.extend(collect_indexes_in_phase(job_pods.iter(), Phase::Succeeded));
            set
        } else {
            HashSet::new()
        };

        // Failures that must never reach `.status.failed`: the ones an `Ignore`
        // rule matched, and — for Indexed Jobs — a failure on an index that has
        // already succeeded.
        let mut never_count_failed: HashSet<String> = ignored_pods.clone();
        if is_indexed {
            for pod in job_pods.iter() {
                if get_pod_index(pod).is_some_and(|i| succeeded_index_set.contains(&i)) {
                    never_count_failed.insert(pod.metadata.name.clone());
                }
            }
        }

        // Exactly-once accounting. `succeeded` / `failed` are no longer a
        // recount of whichever pods happen to still exist — they are the
        // persisted counters plus the terminal pods this pass is claiming
        // (#1959). See `job_tracking` for the protocol.
        let only_replace_failed_pods = job.spec.pod_replacement_policy.as_deref() == Some("Failed");
        let tracked = self.track_terminated_pods(
            &format!("{}/{}", namespace, name),
            job.status.as_ref(),
            &job_pods,
            &never_count_failed,
            is_indexed,
            only_replace_failed_pods,
        );

        // Decision values (counted + parked) drive completion and backoff.
        let succeeded = if is_indexed {
            succeeded_index_set.len() as i32
        } else {
            tracked.succeeded
        };
        let failed = tracked.failed;
        // Persisted values (counted only) go into `.status`. For an Indexed Job
        // the succeeded-index set IS the durable record, so the two coincide.
        let status_succeeded = if is_indexed {
            Some(succeeded_index_set.len() as i32)
        } else {
            tracked.status_succeeded
        };
        let status_failed = tracked.status_failed;
        let uncounted_terminated = tracked.uncounted;
        let pods_to_release = tracked.to_release;
        let job_tracking_key = format!("{}/{}", namespace, name);
        // Upstream drops the field once both lists drain, rather than
        // persisting an empty object.
        let uncounted_status = match (
            uncounted_terminated
                .succeeded
                .as_ref()
                .map_or(0, |v| v.len()),
            uncounted_terminated.failed.as_ref().map_or(0, |v| v.len()),
        ) {
            (0, 0) => None,
            _ => Some(uncounted_terminated),
        };

        let mut active = 0;
        let mut ready = 0i32;
        for pod in job_pods.iter() {
            if let Some(status) = &pod.status {
                if matches!(&status.phase, Some(Phase::Running) | Some(Phase::Pending)) {
                    active += 1;
                }
                // Count pods with Ready condition = True
                if let Some(conditions) = &status.conditions {
                    if conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                    {
                        ready += 1;
                    }
                }
            }
        }

        // Handle suspended jobs: delete all active pods and set active to 0
        if job.spec.suspend.unwrap_or(false) {
            if active > 0 {
                for pod in job_pods.iter() {
                    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                        let _ = self.storage.delete(&pod_key).await;
                        info!(
                            "Suspended job {}/{}: deleted active pod {}",
                            namespace, name, pod.metadata.name
                        );
                    }
                }
            }
            // Preserve existing start_time
            let existing_start_time = job.status.as_ref().and_then(|s| s.start_time);
            let existing_conditions = job.status.as_ref().and_then(|s| s.conditions.clone());
            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: existing_conditions,
                start_time: existing_start_time,
                completion_time: None,
                ready: Some(ready),
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: uncounted_status.clone(),
                observed_generation: job.metadata.generation,
            });
            let key = format!("/registry/jobs/{}/{}", namespace, name);
            self.flush_status_and_release(
                &key,
                &job_tracking_key,
                namespace,
                job,
                &pods_to_release,
                &job_pods,
            )
            .await?;
            return Ok(());
        }

        // Handle activeDeadlineSeconds — fail the job if it has been active too long
        if let Some(deadline) = job.spec.active_deadline_seconds {
            if let Some(start) = job.status.as_ref().and_then(|s| s.start_time) {
                let elapsed = chrono::Utc::now()
                    .signed_duration_since(start)
                    .num_seconds();
                if elapsed > deadline {
                    warn!(
                        "Job {}/{} exceeded activeDeadlineSeconds ({} > {})",
                        namespace, name, elapsed, deadline
                    );
                    // Delete all active pods
                    for pod in job_pods.iter() {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                            let _ = self.storage.delete(&pod_key).await;
                        }
                    }
                    job.status = Some(JobStatus {
                        active: Some(0),
                        succeeded: status_succeeded,
                        failed: status_failed,
                        conditions: Some(failed_job_conditions(
                            "DeadlineExceeded".to_string(),
                            format!(
                                "Job was active longer than specified deadline of {} seconds",
                                deadline
                            ),
                        )),
                        start_time: job.status.as_ref().and_then(|s| s.start_time),
                        // completionTime is valid ONLY on a Complete job
                        // (validation.go:505-513: "cannot set completionTime
                        // when there is no Complete=True condition").
                        completion_time: None,
                        ready: Some(ready),
                        terminating: None,
                        completed_indexes: None,
                        failed_indexes: None,
                        uncounted_terminated_pods: uncounted_status.clone(),
                        observed_generation: job.metadata.generation,
                    });
                    let key = format!("/registry/jobs/{}/{}", namespace, name);
                    self.flush_status_and_release(
                        &key,
                        &job_tracking_key,
                        namespace,
                        job,
                        &pods_to_release,
                        &job_pods,
                    )
                    .await?;
                    return Ok(());
                }
            }
        }

        // For Indexed completion mode, report the durable succeeded-index set.
        let completed_indexes: Option<String> = if is_indexed && !succeeded_index_set.is_empty() {
            let mut indexes: Vec<i32> = succeeded_index_set.iter().copied().collect();
            indexes.sort();
            Some(format_index_ranges(&indexes))
        } else {
            None
        };

        // Track failed indexes for backoffLimitPerIndex
        let backoff_limit_per_index = job.spec.backoff_limit_per_index;
        let mut backoff_failed_index_set: HashSet<i32> = HashSet::new();

        if is_indexed && backoff_limit_per_index.is_some() {
            let per_index_limit = backoff_limit_per_index.unwrap_or(0);
            backoff_failed_index_set = indexes_over_backoff_limit(job_pods.iter(), per_index_limit);
        }

        // Merge FailIndex and backoff-per-index failed sets
        let all_failed_index_set: HashSet<i32> = fail_index_set
            .union(&backoff_failed_index_set)
            .copied()
            .collect();

        let failed_indexes: Option<String> = if !all_failed_index_set.is_empty() {
            let mut sorted: Vec<i32> = all_failed_index_set.iter().copied().collect();
            sorted.sort();
            Some(format_index_ranges(&sorted))
        } else {
            None
        };

        // For Indexed mode, K8s sets status.succeeded to the count of unique
        // succeeded indexes (NOT the raw succeeded pod count). K8s ref:
        //   pkg/controller/job/job_controller.go — status.Succeeded =
        //   succeededIndexes.total().
        let succeeded_index_count = if is_indexed {
            succeeded_index_set.len() as i32
        } else {
            succeeded
        };

        info!(
            "Job {}/{}: active={}, succeeded={}, failed={}, target={}",
            namespace, name, active, succeeded, failed, completions
        );

        // Check if Job is complete
        // For indexed jobs, check number of distinct succeeded indexes
        let is_complete = if is_indexed {
            succeeded_index_count >= completions
        } else {
            succeeded >= completions
        };

        // Check maxFailedIndexes — if the number of failed indexes exceeds this limit, fail the job
        let max_failed_indexes_exceeded = if is_indexed {
            if let Some(max_failed) = job.spec.max_failed_indexes {
                let failed_index_count = all_failed_index_set.len() as i32;
                // Also count unique indexes with only failed pods (no succeeded) when no backoffLimitPerIndex
                if backoff_limit_per_index.is_none() && fail_index_set.is_empty() {
                    let mut failed_idx_set: HashSet<i32> = HashSet::new();
                    for pod in job_pods.iter() {
                        if matches!(
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            Some(Phase::Failed)
                        ) {
                            if let Some(index) = get_pod_index(pod) {
                                failed_idx_set.insert(index);
                            }
                        }
                    }
                    failed_idx_set.len() as i32 > max_failed
                } else {
                    failed_index_count > max_failed
                }
            } else {
                false
            }
        } else {
            false
        };

        // For backoffLimitPerIndex, job fails when all indexes are either succeeded or failed
        let is_failed = pod_failure_policy_triggered
            || max_failed_indexes_exceeded
            || if backoff_limit_per_index.is_some() && is_indexed {
                let completed_count = succeeded_index_count;
                let failed_count = all_failed_index_set.len() as i32;
                (completed_count + failed_count) >= completions
            } else {
                failed > backoff_limit
            };

        // Preserve the existing start_time if the job was already started
        let existing_start_time = job.status.as_ref().and_then(|s| s.start_time);

        // Set start_time when the job first has any pods (active, succeeded, or failed)
        let start_time = if active > 0 || succeeded > 0 || failed > 0 {
            Some(existing_start_time.unwrap_or_else(chrono::Utc::now))
        } else {
            existing_start_time
        };

        // Check successPolicy — if defined and criteria met, mark job complete
        let success_policy_met = if let Some(ref policy) = job.spec.success_policy {
            policy.rules.iter().any(|rule| {
                let indexes_ok = if let Some(ref succeeded_indexes_str) = rule.succeeded_indexes {
                    // Parse required indexes and check they are all in completed set
                    let completed = completed_indexes.as_deref().unwrap_or("");
                    let completed_set: HashSet<i32> = parse_index_ranges(completed);
                    let required_set: HashSet<i32> = parse_index_ranges(succeeded_indexes_str);
                    required_set.is_subset(&completed_set)
                } else {
                    true // No index constraint
                };

                let count_ok = if let Some(count) = rule.succeeded_count {
                    succeeded_index_count >= count
                } else {
                    true // No count constraint
                };

                // If rule has neither succeededIndexes nor succeededCount, match on all completions
                let has_criteria =
                    rule.succeeded_indexes.is_some() || rule.succeeded_count.is_some();
                if has_criteria {
                    indexes_ok && count_ok
                } else {
                    succeeded_index_count >= completions
                }
            })
        } else {
            false
        };

        if success_policy_met {
            info!("Job {}/{} met success policy criteria", namespace, name);

            // Delete remaining active pods. K8s's job controller issues a real
            // pod delete on completion (DeletePod + finalizer removal), so the
            // pods are gone — not left lingering with a deletionTimestamp. We
            // delete them outright so status.terminating settles at 0 on the
            // next sync (the conformance test asserts Terminating == 0); the
            // kubelet GCs the container once the pod disappears from the API.
            for pod in job_pods.iter() {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    let _ = self.storage.delete(&pod_key).await;
                }
            }

            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: Some(complete_job_conditions(
                    "SuccessPolicy".to_string(),
                    "Matched rules in the SuccessPolicy".to_string(),
                )),
                start_time,
                completion_time: Some(chrono::Utc::now()),
                ready: Some(0), // Job is complete, no ready pods
                // K8s sets terminating to 0 when the job completes, even if pods
                // are still being cleaned up. The job status should reflect the
                // final state, not the transitional state.
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: uncounted_status.clone(),
                observed_generation: job.metadata.generation,
            });
            let key = format!("/registry/jobs/{}/{}", namespace, name);
            self.flush_status_and_release(
                &key,
                &job_tracking_key,
                namespace,
                job,
                &pods_to_release,
                &job_pods,
            )
            .await?;
            return Ok(());
        }

        if is_complete {
            info!("Job {}/{} completed successfully", namespace, name);
            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: Some(complete_job_conditions(
                    "CompletionsReached".to_string(),
                    "Reached expected number of succeeded pods".to_string(),
                )),
                start_time,
                completion_time: Some(chrono::Utc::now()),
                // K8s sets ready and terminating to 0 when a job completes.
                // The test expects non-nil pointer to 0, not nil (omitted).
                ready: Some(0),
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: uncounted_status.clone(),
                observed_generation: job.metadata.generation,
            });
        } else if is_failed {
            warn!(
                "Job {}/{} failed after {} failures",
                namespace, name, failed
            );

            // Determine failure reason
            let (reason, message) = if pod_failure_policy_triggered {
                ("PodFailurePolicy".to_string(), pod_failure_message.clone())
            } else if max_failed_indexes_exceeded {
                (
                    "MaxFailedIndexesExceeded".to_string(),
                    "Job has exceeded the maximum number of failed indexes".to_string(),
                )
            } else if backoff_limit_per_index.is_some() && is_indexed {
                (
                    "FailedIndexes".to_string(),
                    format!(
                        "Job has failed indexes: {}",
                        failed_indexes.as_deref().unwrap_or("")
                    ),
                )
            } else {
                (
                    "BackoffLimitExceeded".to_string(),
                    format!("Job has reached backoff limit of {}", backoff_limit),
                )
            };

            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: Some(failed_job_conditions(reason, message)),
                start_time,
                // completionTime is valid ONLY on a Complete job
                // (validation.go:505-513).
                completion_time: None,
                // K8s sets ready and terminating to 0 when a job is terminal.
                ready: Some(0),
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: uncounted_status.clone(),
                observed_generation: job.metadata.generation,
            });
        } else {
            // Re-list pods right before creating to minimize race window where
            // two parallel reconciliations both see "need 1 more pod" and both create one.
            let fresh_all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let fresh_job_pods: Vec<&Pod> = fresh_all_pods
                .iter()
                .filter(|pod| {
                    pod.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                        .unwrap_or(false)
                        || pod
                            .metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("job-name"))
                            .map(|j| j == name)
                            .unwrap_or(false)
                })
                .collect();

            let mut fresh_active = 0i32;
            let mut fresh_succeeded = 0i32;
            for pod in fresh_job_pods.iter() {
                if let Some(status) = &pod.status {
                    match &status.phase {
                        Some(Phase::Running) | Some(Phase::Pending) => fresh_active += 1,
                        Some(Phase::Succeeded) => fresh_succeeded += 1,
                        _ => {}
                    }
                }
            }

            // Calculate how many new pods to create using fresh counts
            let pods_needed = std::cmp::min(
                parallelism - fresh_active,
                completions - fresh_succeeded - fresh_active,
            );

            if pods_needed > 0 {
                // For Indexed mode, find which indexes still need pods
                let indexes_to_create: Vec<i32> = if is_indexed {
                    // Track indexes that already have active or succeeded pods
                    let mut active_or_succeeded_indexes: HashSet<i32> = HashSet::new();
                    for pod in fresh_job_pods.iter() {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if matches!(
                            phase,
                            Some(Phase::Running) | Some(Phase::Pending) | Some(Phase::Succeeded)
                        ) {
                            if let Some(idx) = get_pod_index(pod) {
                                active_or_succeeded_indexes.insert(idx);
                            }
                        }
                    }
                    // The exhausted-index set must be recomputed from the SAME
                    // fresh list the active/succeeded counts came from.
                    // `all_failed_index_set` was derived from the snapshot taken at
                    // the top of this reconcile; against a vanilla api-server that
                    // snapshot lags, so an index that has already burned its
                    // `backoffLimitPerIndex` retries can still look retryable and
                    // get one pod too many. With `backoffLimitPerIndex: 1` and two
                    // failing indexes that made `status.failed` 5 instead of 4, and
                    // `[sig-apps] Job should execute all indexes despite some
                    // failing when using backoffLimitPerIndex` failed on
                    // `Expected <int32>: 5 to equal <int32>: 4` (#1821). Union, never
                    // replace: a fresher view can only ADD exhausted indexes, and the
                    // FailIndex half of the set has no fresh equivalent here.
                    let mut exhausted_indexes = all_failed_index_set.clone();
                    if let Some(per_index_limit) = backoff_limit_per_index {
                        exhausted_indexes.extend(indexes_over_backoff_limit(
                            fresh_job_pods.iter().copied(),
                            per_index_limit,
                        ));
                    }
                    (0..completions)
                        .filter(|i| {
                            // Skip indexes that already have active or succeeded pods
                            if active_or_succeeded_indexes.contains(i) {
                                return false;
                            }
                            // Skip indexes that are permanently failed (backoffLimitPerIndex or FailIndex)
                            if exhausted_indexes.contains(i) {
                                return false;
                            }
                            true
                        })
                        .take(pods_needed as usize)
                        .collect()
                } else {
                    (0..pods_needed).collect()
                };

                for (i, idx) in indexes_to_create.iter().enumerate() {
                    match self.create_pod(job, namespace, *idx, is_indexed).await {
                        Ok(_) => {
                            info!(
                                "Created pod for Job {}/{} ({}/{})",
                                namespace,
                                name,
                                fresh_job_pods.len() + i + 1,
                                completions
                            );
                        }
                        Err(e) => {
                            let err_str = format!("{}", e);
                            if err_str.contains("already exists")
                                || err_str.contains("AlreadyExists")
                            {
                                debug!(
                                    "Pod already exists for Job {}/{}, skipping",
                                    namespace, name
                                );
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }

                // Re-count pods after creation to get accurate status
                let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;
                let job_pods_after: Vec<Pod> = all_pods_after
                    .into_iter()
                    .filter(|pod| {
                        pod.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("job-name"))
                            .map(|j| j == name)
                            .unwrap_or(false)
                    })
                    .collect();

                // Only `active` is re-derived here. `succeeded` and `failed`
                // are cumulative counters owned by the tracking protocol — a
                // pod contributes to them exactly once, when its UID is claimed
                // — so recounting them from this list would count every
                // already-counted pod a second time. The pods just created are
                // Pending, which is precisely what `active` measures.
                active = job_pods_after
                    .iter()
                    .filter(|pod| {
                        matches!(
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            Some(Phase::Running) | Some(Phase::Pending)
                        )
                    })
                    .count() as i32;
            }

            // Update status — but preserve conditions and completion_time if job
            // was already completed (e.g. by SuccessPolicy). Otherwise the regular
            // update path overwrites the completion status.
            let existing_conditions = job.status.as_ref().and_then(|s| s.conditions.clone());
            let existing_completion = job.status.as_ref().and_then(|s| s.completion_time);
            let already_complete = existing_conditions
                .as_ref()
                .map(|c| {
                    c.iter().any(|cond| {
                        (cond.condition_type == "Complete"
                            || cond.condition_type == "SuccessCriteriaMet")
                            && cond.status == "True"
                    })
                })
                .unwrap_or(false);

            if !already_complete {
                job.status = Some(JobStatus {
                    active: Some(active),
                    succeeded: status_succeeded,
                    failed: status_failed,
                    conditions: existing_conditions,
                    start_time,
                    completion_time: existing_completion,
                    ready: Some(ready),
                    terminating: None,
                    completed_indexes: completed_indexes.clone(),
                    failed_indexes: failed_indexes.clone(),
                    uncounted_terminated_pods: uncounted_status.clone(),
                    observed_generation: job.metadata.generation,
                });
            }
        }

        // Save updated status
        let key = format!("/registry/jobs/{}/{}", namespace, name);
        let has_complete = job
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Complete" && cond.status == "True")
            })
            .unwrap_or(false);
        let has_failed = job
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Failed" && cond.status == "True")
            })
            .unwrap_or(false);
        if has_complete || has_failed {
            info!(
                "Job {}/{} status update: complete={}, failed={}, conditions={:?}",
                namespace,
                name,
                has_complete,
                has_failed,
                job.status.as_ref().and_then(|s| s.conditions.as_ref())
            );
        }
        self.flush_status_and_release(
            &key,
            &job_tracking_key,
            namespace,
            job,
            &pods_to_release,
            &job_pods,
        )
        .await?;

        Ok(())
    }

    async fn create_pod(
        &self,
        job: &Job,
        namespace: &str,
        index: i32,
        is_indexed: bool,
    ) -> Result<()> {
        let job_name = &job.metadata.name;
        let pod_name = format!(
            "{}-{}",
            job_name,
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        );

        // Create pod from template
        let template = &job.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("job-name".to_string(), job_name.clone());
        labels.insert("controller-uid".to_string(), job.metadata.uid.clone());
        if is_indexed {
            labels.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }

        let mut annotations = template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.clone())
            .unwrap_or_default();
        if is_indexed {
            annotations.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }

        let mut spec = template.spec.clone();

        // Respect the template's restart policy.
        // For restartPolicy: OnFailure, the kubelet will restart failed containers in-place,
        // allowing the pod to eventually succeed without the Job controller creating new pods.
        // For restartPolicy: Never (or if not set), the Job controller handles retries.
        if spec.restart_policy.is_none() {
            spec.restart_policy = Some("Never".to_string());
        }

        // For Indexed mode, set hostname to {job-name}-{index} (K8s convention)
        if is_indexed {
            spec.hostname = Some(format!("{}-{}", job_name, index));
        }

        // For Indexed mode, inject JOB_COMPLETION_INDEX env var into all containers
        if is_indexed {
            for container in &mut spec.containers {
                let env = container.env.get_or_insert_with(Vec::new);
                if !env.iter().any(|e| e.name == "JOB_COMPLETION_INDEX") {
                    env.push(rusternetes_common::resources::EnvVar {
                        name: "JOB_COMPLETION_INDEX".to_string(),
                        value: Some(index.to_string()),
                        value_from: None,
                    });
                }
            }
        }

        // Propagate the SA's imagePullSecrets (#1084) — controllers bypass the
        // api-server admission path that normally does this.
        super::propagate_sa_image_pull_secrets(&*self.storage, namespace, &mut spec).await;

        // DefaultTolerationSeconds admission (#442): controllers bypass the
        // api-server admission path that adds these NoExecute tolerations.
        rusternetes_common::tolerations::add_default_tolerations(&mut spec);

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: pod_name.clone(),
                generate_name: None,
                generation: None,
                managed_fields: None,
                namespace: Some(namespace.to_string()),
                labels: Some(labels),
                annotations: Some(annotations),
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                // Hold the pod until the Job status has accounted for it.
                // Upstream sets the same finalizer on every pod it creates
                // (`batch.JobTrackingFinalizer`,
                // staging/src/k8s.io/api/batch/v1/types.go:44): "It prevents
                // them from being deleted before being accounted in the Job
                // status."
                finalizers: Some(vec![JOB_TRACKING_FINALIZER.to_string()]),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: job_name.clone(),
                    uid: job.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
            },
            spec: Some(spec),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                host_ip: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        // Check ResourceQuota before creating pod
        super::check_resource_quota(&*self.storage, namespace).await?;

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        self.storage.create(&key, &pod).await?;

        Ok(())
    }
}

/// Extract the completion index from a pod owned by an Indexed Job.
/// Looks for `batch.kubernetes.io/job-completion-index` on annotations,
/// then labels, then the `JOB_COMPLETION_INDEX` env var on the first container.
fn get_pod_index(pod: &Pod) -> Option<i32> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
        .and_then(|v| v.parse::<i32>().ok())
        .or_else(|| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("batch.kubernetes.io/job-completion-index"))
                .and_then(|v| v.parse::<i32>().ok())
        })
        .or_else(|| {
            pod.spec.as_ref().and_then(|s| {
                s.containers.first().and_then(|c| {
                    c.env.as_ref().and_then(|envs| {
                        envs.iter()
                            .find(|e| e.name == "JOB_COMPLETION_INDEX")
                            .and_then(|e| e.value.as_ref())
                            .and_then(|v| v.parse::<i32>().ok())
                    })
                })
            })
        })
}

/// Collect the set of unique completion indexes whose pods are in the given
/// phase. Used for K8s-compatible Indexed Job status accounting.
/// Indexes whose failed-pod count **exceeds** `backoffLimitPerIndex`, i.e. the
/// indexes that are permanently failed and must never get another pod.
///
/// Upstream counts one pod per index per attempt and marks the index failed
/// once the count is greater than the limit
/// (`pkg/controller/job/indexed_job_utils.go`, `calculateFailedIndexes`), so
/// `backoffLimitPerIndex: 1` allows exactly two pods per failing index.
///
/// Shared by the status computation and the pod-creation gate on purpose: those
/// two used to derive it from different pod lists. See
/// `indexes_over_backoff_limit` at the creation site for what that cost.
fn indexes_over_backoff_limit<'a, I>(pods: I, per_index_limit: i32) -> HashSet<i32>
where
    I: IntoIterator<Item = &'a Pod>,
{
    let mut failures_per_index: HashMap<i32, i32> = HashMap::new();
    for pod in pods {
        if matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Failed)
        ) {
            if let Some(index) = get_pod_index(pod) {
                *failures_per_index.entry(index).or_insert(0) += 1;
            }
        }
    }
    failures_per_index
        .into_iter()
        .filter(|(_, count)| *count > per_index_limit)
        .map(|(idx, _)| idx)
        .collect()
}

fn collect_indexes_in_phase<'a, I>(pods: I, phase: Phase) -> HashSet<i32>
where
    I: IntoIterator<Item = &'a Pod>,
{
    let mut set = HashSet::new();
    for pod in pods {
        if pod.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&phase) {
            if let Some(idx) = get_pod_index(pod) {
                set.insert(idx);
            }
        }
    }
    set
}

/// Parse index ranges like "0,1,3-5" into a set of integers {0, 1, 3, 4, 5}
fn parse_index_ranges(s: &str) -> HashSet<i32> {
    let mut set = HashSet::new();
    if s.is_empty() {
        return set;
    }
    for part in s.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() == 2 {
                if let (Ok(start), Ok(end)) = (
                    bounds[0].trim().parse::<i32>(),
                    bounds[1].trim().parse::<i32>(),
                ) {
                    for i in start..=end {
                        set.insert(i);
                    }
                }
            }
        } else if let Ok(idx) = part.parse::<i32>() {
            set.insert(idx);
        }
    }
    set
}

/// Format a sorted, deduped list of indexes into compressed ranges: [0, 1, 2, 5] -> "0-2,5"
fn format_index_ranges(indexes: &[i32]) -> String {
    if indexes.is_empty() {
        return String::new();
    }
    let mut ranges: Vec<String> = Vec::new();
    let mut start = indexes[0];
    let mut end = indexes[0];
    for &idx in &indexes[1..] {
        if idx == end + 1 {
            end = idx;
        } else {
            if start == end {
                ranges.push(start.to_string());
            } else {
                ranges.push(format!("{}-{}", start, end));
            }
            start = idx;
            end = idx;
        }
    }
    if start == end {
        ranges.push(start.to_string());
    } else {
        ranges.push(format!("{}-{}", start, end));
    }
    ranges.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::workloads::{Job, JobSpec, PodTemplateSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStatus, Pod, PodCondition, PodSpec, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn test_container() -> Container {
        Container {
            name: "test".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            image_pull_policy: None,
            security_context: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            resize_policy: None,
            restart_policy: None,
            ..Default::default()
        }
    }

    fn make_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
        Job {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: "job-uid-1".to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: None,
                    spec: PodSpec {
                        containers: vec![test_container()],
                        ..Default::default()
                    },
                },
                completions: Some(completions),
                parallelism: Some(parallelism),
                backoff_limit: Some(6),
                active_deadline_seconds: None,
                selector: None,
                manual_selector: None,
                suspend: None,
                ttl_seconds_after_finished: None,
                completion_mode: None,
                backoff_limit_per_index: None,
                max_failed_indexes: None,
                pod_failure_policy: None,
                pod_replacement_policy: None,
                success_policy: None,
                managed_by: None,
            },
            status: None,
        }
    }

    fn make_pod(name: &str, namespace: &str, phase: Phase, job_name: &str, job_uid: &str) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: format!("pod-uid-{}", name),
                // Model a controller-created pod: the Job controller stamps
                // every pod it creates with the tracking finalizer, and a pod
                // without it is invisible to the accounting protocol by design
                // (upstream `getValidPodsWithFilter`: "Pods that don't have a
                // completion finalizer ... have already been accounted for").
                finalizers: Some(vec![JOB_TRACKING_FINALIZER.to_string()]),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), job_name.to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: job_name.to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase),
                ..Default::default()
            }),
        }
    }

    fn make_indexed_pod(
        name: &str,
        namespace: &str,
        phase: Phase,
        job_name: &str,
        job_uid: &str,
        index: i32,
    ) -> Pod {
        let mut pod = make_pod(name, namespace, phase, job_name, job_uid);
        pod.metadata.annotations = Some({
            let mut m = HashMap::new();
            m.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
            m
        });
        if let Some(ref mut labels) = pod.metadata.labels {
            labels.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }
        pod
    }

    fn make_failed_pod_with_exit_code(
        name: &str,
        namespace: &str,
        job_name: &str,
        job_uid: &str,
        index: i32,
        exit_code: i32,
    ) -> Pod {
        let mut pod = make_indexed_pod(name, namespace, Phase::Failed, job_name, job_uid, index);
        if let Some(ref mut status) = pod.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState::Terminated {
                    exit_code,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        pod
    }

    #[test]
    fn test_pods_needed_calculation() {
        let parallelism = 3;
        let completions = 10;
        let active = 2;
        let succeeded = 5;

        let pods_needed = std::cmp::min(
            parallelism - active,             // 1 (can run 1 more in parallel)
            completions - succeeded - active, // 3 (need 3 more to complete)
        );

        assert_eq!(pods_needed, 1);
    }

    #[test]
    fn test_job_completion() {
        let completions = 5;
        let succeeded = 5;
        assert!(succeeded >= completions);
    }

    #[test]
    fn test_backoff_limit_exceeded() {
        let backoff_limit = 6;
        let failed = 7;
        assert!(failed > backoff_limit);
    }

    #[test]
    fn test_parse_index_ranges() {
        assert_eq!(parse_index_ranges(""), HashSet::new());
        assert_eq!(
            parse_index_ranges("0"),
            [0].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,1,2"),
            [0, 1, 2].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0-3"),
            [0, 1, 2, 3].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,2-4,7"),
            [0, 2, 3, 4, 7].into_iter().collect::<HashSet<i32>>()
        );
    }

    #[test]
    fn test_format_index_ranges() {
        assert_eq!(format_index_ranges(&[]), "");
        assert_eq!(format_index_ranges(&[0]), "0");
        assert_eq!(format_index_ranges(&[0, 1, 2]), "0-2");
        assert_eq!(format_index_ranges(&[0, 1, 2, 5]), "0-2,5");
        assert_eq!(format_index_ranges(&[0, 2, 3, 4, 7, 8]), "0,2-4,7-8");
    }

    /// A failing Job must reach `Failed` the way upstream stages it, or the
    /// api-server rejects the whole status write.
    ///
    /// Live, against a vanilla control plane, every failing-Job spec died here:
    ///
    /// ```text
    /// Failed to reconcile jobs/job-check/failjob: Bad request:
    ///   Error from server (Invalid): Job.batch "failjob" is invalid:
    ///   [status.completionTime: Invalid value: "...": cannot set completionTime
    ///      when there is no Complete=True condition,
    ///    status.conditions: Invalid value: cannot set Failed=True condition
    ///      without the FailureTarget=true condition]
    /// ```
    ///
    /// (pkg/apis/batch/validation/validation.go:505-522). Upstream appends the
    /// interim `FailureTarget` condition first and only then adds `Failed` with
    /// the same reason/message — `job_controller.go:1307-1316` ->
    /// `newFailedConditionForFailureTarget` — and sets `completionTime` only
    /// for Complete jobs. The rejected write left the Job `active: 1` forever,
    /// so `WaitForJobFailed` timed out in all seven Job [Conformance] specs.
    /// A Job that reaches its completions must stage `SuccessCriteriaMet`
    /// before `Complete`, the mirror of the FailureTarget rule.
    ///
    /// Live, a two-of-two successful Job stalled at `succeeded: 1, active: 1`
    /// forever because the api-server rejected every status write:
    ///
    /// ```text
    /// Failed to reconcile jobs/job-complete/ok: Bad request:
    ///   Error from server (Invalid): Job.batch "ok" is invalid:
    ///   status.conditions: Invalid value: cannot set Complete=True condition
    ///     without the SuccessCriteriaMet=true condition
    /// ```
    ///
    /// (pkg/apis/batch/validation/validation.go:525-527). Upstream appends the
    /// interim SuccessCriteriaMet condition and derives Complete from it with
    /// the same reason and message (job_controller.go:1317-1327). Three Job
    /// [Conformance] specs died on "failed to ensure job completion ... Timed
    /// out after 900s" because of this. The success-policy path already staged
    /// the pair; the ordinary completions-reached path did not.
    ///
    /// `completionTime` stays REQUIRED here — validation.go:505-513 demands it
    /// for Complete jobs (the inverse of the failed-job rule).
    /// Job status counters are monotonically non-decreasing: upstream's
    /// api-server rejects any status update that lowers them —
    /// `RejectDecreasingFailedCounter` / `RejectDecreasingSucceededCounter` in
    /// `pkg/apis/batch/validation/validation.go:722-730`, producing
    /// `status.failed: Invalid value: 0: cannot decrease the failed counter`.
    ///
    /// We recompute the counters from the live pod list every reconcile and
    /// additionally subtract pods matched by an `Ignore` podFailurePolicy rule,
    /// so a `failed` count that was already persisted can drop back down. When
    /// it does, a real api-server rejects every subsequent write, the terminal
    /// `Complete=True` condition never lands, and the Job hangs until the e2e
    /// timeout. That is #1955: the `vanilla-swap-controller-manager` leg's
    /// "ignore failure matching on DisruptionTarget condition" spec, where the
    /// evicted pods' failures are ignored after having been counted.
    #[tokio::test]
    async fn job_status_counters_never_decrease() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("monotonic", "default", 3, 3);
        job.spec.backoff_limit = Some(2);
        // The evicted failures were already accounted into the persisted status
        // by an earlier reconcile, before the Ignore rule matched them.
        job.status = Some(JobStatus {
            active: Some(0),
            succeeded: Some(1),
            failed: Some(3),
            conditions: None,
            start_time: Some(chrono::Utc::now()),
            completion_time: None,
            ready: Some(0),
            terminating: None,
            completed_indexes: None,
            failed_indexes: None,
            uncounted_terminated_pods: None,
            observed_generation: None,
        });
        let job_key = build_key("jobs", Some("default"), "monotonic");
        storage.create(&job_key, &job).await.unwrap();

        // The pods that carried those failures are gone (evicted and deleted);
        // only the succeeded replacements remain, so a recompute from live pods
        // yields failed=0 — lower than what is already persisted.
        for name in ["monotonic-1", "monotonic-2", "monotonic-3"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "monotonic", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let status = storage
            .get::<Job>(&job_key)
            .await
            .unwrap()
            .status
            .expect("job must have status");

        assert!(
            status.failed.unwrap_or(0) >= 3,
            "status.failed must never drop below the persisted 3 (got {:?}) — \
             a real api-server rejects the write with \"cannot decrease the \
             failed counter\" and the Job never reaches Complete",
            status.failed
        );
        assert!(
            status.succeeded.unwrap_or(0) >= 1,
            "status.succeeded must never drop below the persisted 1 (got {:?})",
            status.succeeded
        );
    }

    #[tokio::test]
    async fn completed_job_stages_success_criteria_met_before_complete() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("ok", "default", 2, 2);
        let job_key = build_key("jobs", Some("default"), "ok");
        storage.create(&job_key, &job).await.unwrap();

        for name in ["ok-1", "ok-2"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "ok", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let status = storage
            .get::<Job>(&job_key)
            .await
            .unwrap()
            .status
            .expect("job must have status");
        let conditions = status.conditions.unwrap_or_default();

        let complete = conditions
            .iter()
            .find(|c| c.condition_type == "Complete" && c.status == "True")
            .expect("a job that reached its completions must be Complete");
        let met = conditions
            .iter()
            .find(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True")
            .expect(
                "Complete=True requires SuccessCriteriaMet=True, or the api-server rejects the write",
            );
        assert_eq!(
            met.reason, complete.reason,
            "the Complete condition inherits the interim condition's reason"
        );
        assert!(
            status.completion_time.is_some(),
            "completionTime is required for Complete jobs"
        );
    }

    #[tokio::test]
    async fn failed_job_sets_failure_target_and_no_completion_time() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("failjob", "default", 1, 1);
        job.spec.backoff_limit = Some(0);
        let job_key = build_key("jobs", Some("default"), "failjob");
        storage.create(&job_key, &job).await.unwrap();

        // One pod, already Failed: with backoffLimit 0 the job must fail.
        let pod = make_pod(
            "failjob-1",
            "default",
            Phase::Failed,
            "failjob",
            "job-uid-1",
        );
        storage
            .create(&build_key("pods", Some("default"), "failjob-1"), &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let stored: Job = storage.get(&job_key).await.unwrap();
        let status = stored.status.expect("job must have status");
        let conditions = status.conditions.unwrap_or_default();

        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(failed.is_some(), "job must end Failed, got {conditions:?}");

        let target = conditions
            .iter()
            .find(|c| c.condition_type == "FailureTarget" && c.status == "True")
            .expect("Failed=True requires FailureTarget=True, or the api-server rejects the write");
        assert_eq!(
            target.reason,
            failed.unwrap().reason,
            "FailureTarget carries the same reason the Failed condition reports"
        );

        assert!(
            status.completion_time.is_none(),
            "completionTime is only valid for Complete jobs; a failed job must not set it"
        );
    }

    #[tokio::test]
    async fn test_job_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = JobController::new(storage.clone());

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", "default");
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let mut job = make_job("pullsecrets-job", "default", 1, 1);
        storage
            .create("/registry/jobs/default/pullsecrets-job", &job)
            .await
            .unwrap();

        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1, "Job should create one pod");
        let secrets = pods[0]
            .spec
            .as_ref()
            .unwrap()
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_tracks_failures() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=1, 3 completions
        let mut job = make_job("test-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(1);
        job.spec.backoff_limit = Some(100); // high so global limit doesn't kick in

        let job_key = "/registry/jobs/default/test-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed twice (exceeds backoffLimitPerIndex=1)
        let pod1a = make_indexed_pod(
            "pod-1a",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1a", &pod1a)
            .await
            .unwrap();
        let pod1b = make_indexed_pod(
            "pod-1b",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1b", &pod1b)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Re-read the job
        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be failed because: succeeded(0,2) + failed(1) = 3 = completions
        // Index 1 exceeded backoffLimitPerIndex
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed" && c.status == "True"),
            "Job should be marked as Failed"
        );

        // Failed indexes should contain index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(fi.contains(&1), "Index 1 should be in failed_indexes");

        // Completed indexes should contain 0 and 2
        assert!(status.completed_indexes.is_some());
        let ci = parse_index_ranges(status.completed_indexes.as_deref().unwrap());
        assert!(
            ci.contains(&0) && ci.contains(&2),
            "Completed indexes should have 0 and 2"
        );
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_no_retry_for_exhausted_index() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=0, 3 completions
        let mut job = make_job("test-job2", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(0);
        job.spec.backoff_limit = Some(100);

        let job_key = "/registry/jobs/default/test-job2";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 failed once (exceeds limit of 0)
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Failed,
            "test-job2",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "test-job2",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // After reconciliation, no new pod should be created for index 0
        // Check that no new pod with index 0 was created
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_0_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "0")
                    .unwrap_or(false)
            })
            .collect();

        // Should still be just the one failed pod for index 0, no retry
        assert_eq!(
            index_0_pods.len(),
            1,
            "Should not create a retry pod for exhausted index 0"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_index() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failindex-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(100);
        // Set up a FailIndex policy for exit code 42
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "FailIndex",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [42]
                        }
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/failindex-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed with exit code 42 -> should trigger FailIndex
        let pod1 =
            make_failed_pod_with_exit_code("pod-1", "default", "failindex-job", "job-uid-1", 1, 42);
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Failed indexes should include index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(
            fi.contains(&1),
            "Index 1 should be in failed_indexes due to FailIndex policy"
        );

        // No new pod should be created for index 1
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_1_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "1")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            index_1_pods.len(),
            1,
            "No retry pod for FailIndex-ed index 1"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_job() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failjob-job", "default", 3, 3);
        job.spec.backoff_limit = Some(100);
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "FailJob",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [99]
                        }
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/failjob-job";
        storage.create(job_key, &job).await.unwrap();

        // One pod failed with exit code 99 -> should trigger FailJob
        let pod = make_failed_pod_with_exit_code(
            "pod-fail",
            "default",
            "failjob-job",
            "job-uid-1",
            0,
            99,
        );
        storage
            .create("/registry/pods/default/pod-fail", &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("PodFailurePolicy")),
            "Job should be Failed due to PodFailurePolicy"
        );
    }

    #[tokio::test]
    async fn test_local_restart_completion() {
        let storage = Arc::new(MemoryStorage::new());

        // Job with 2 completions, template has restartPolicy: OnFailure
        let mut job = make_job("restart-job", "default", 2, 2);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/restart-job";
        storage.create(job_key, &job).await.unwrap();

        // Pod 1 succeeded (was restarted locally, restart_count > 0, now succeeded)
        let mut pod1 = make_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        if let Some(ref mut status) = pod1.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 3, // restarted 3 times before succeeding
                state: Some(ContainerState::Terminated {
                    exit_code: 0,
                    signal: None,
                    reason: Some("Completed".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Pod 2 succeeded
        let pod2 = make_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete when locally restarted pods succeed"
        );
        assert_eq!(status.succeeded, Some(2));
    }

    #[tokio::test]
    async fn test_restart_policy_on_failure_preserved() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("onfailure-job", "default", 1, 1);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/onfailure-job";
        storage.create(job_key, &job).await.unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Check that the created pod preserved the OnFailure restart policy
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let job_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("job-name"))
                    .map(|v| v == "onfailure-job")
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(job_pods.len(), 1);
        let restart_policy = job_pods[0].spec.as_ref().unwrap().restart_policy.as_deref();
        assert_eq!(
            restart_policy,
            Some("OnFailure"),
            "Pod should preserve OnFailure restart policy from template"
        );
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes() {
        let storage = Arc::new(MemoryStorage::new());

        // Indexed job with 5 completions, successPolicy requiring indexes 0-1
        let mut job = make_job("sp-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-1"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 1 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Indexes 2-4 are still pending
        let pod2 = make_indexed_pod("pod-2", "default", Phase::Pending, "sp-job", "job-uid-1", 2);
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();
        let pod3 = make_indexed_pod("pod-3", "default", Phase::Pending, "sp-job", "job-uid-1", 3);
        storage
            .create("/registry/pods/default/pod-3", &pod3)
            .await
            .unwrap();
        let pod4 = make_indexed_pod("pod-4", "default", Phase::Pending, "sp-job", "job-uid-1", 4);
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via successPolicy even though indexes 2-4 are pending
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via successPolicy"
        );
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
            "SuccessCriteriaMet condition should be set"
        );
        assert!(status.completion_time.is_some());
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes_terminates_running_ready_pod() {
        // Mirrors [sig-apps] "with successPolicy succeededIndexes rule": an
        // indexed job (completions=5, parallelism=2) whose required index 0
        // succeeds while another index is still Running and Ready. Once the
        // success policy is met the job must complete with active/ready/
        // terminating all 0 and completedIndexes "0" — the conformance test
        // asserts job.Status.Ready == 0.
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-idx-job", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let job_key = "/registry/jobs/default/sp-idx-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded (the required index).
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-idx-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 2 is still Running AND Ready — must not be counted once the
        // success policy completes the job.
        let mut pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Running,
            "sp-idx-job",
            "job-uid-1",
            2,
        );
        if let Some(ref mut st) = pod2.status {
            st.conditions = Some(vec![rusternetes_common::resources::PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());

        // Two reconciles: the second simulates a follow-up loop where the
        // terminating pod is still present in storage (kubelet hasn't removed
        // it yet) — status must stay settled at 0s.
        for _ in 0..2 {
            let mut job: Job = storage.get(job_key).await.unwrap();
            controller.reconcile(&mut job).await.unwrap();
        }

        let status: JobStatus = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        assert_eq!(
            status.completed_indexes.as_deref(),
            Some("0"),
            "completedIndexes must be exactly the succeeded index"
        );
        assert_eq!(
            status.active,
            Some(0),
            "active must be 0 after successPolicy"
        );
        assert_eq!(
            status.ready,
            Some(0),
            "ready must be 0 after successPolicy — the still-Running pod is terminating"
        );
        assert_eq!(status.terminating, Some(0), "terminating must be 0");
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededCount": 2
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-count-job";
        storage.create(job_key, &job).await.unwrap();

        // 2 indexes succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Others still pending
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Pending,
            "sp-count-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via succeededCount successPolicy"
        );
    }

    #[tokio::test]
    async fn test_success_policy_not_met_yet() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-notyet", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-2"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-notyet";
        storage.create(job_key, &job).await.unwrap();

        // Only index 0 succeeded — need 0, 1, 2
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-notyet",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "sp-notyet",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Should NOT be complete yet
        let is_complete = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Complete" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !is_complete,
            "Job should NOT be complete when not all required indexes succeeded"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_ignore_action() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimit=0 and a pod failure policy
        // that ignores pods with DisruptionTarget condition
        let mut job = make_job("ignore-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(0); // Would fail immediately if the pod is counted
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "Ignore",
                        "onPodConditions": [
                            {
                                "type": "DisruptionTarget",
                                "status": "True"
                            }
                        ]
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/ignore-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a failed pod with a DisruptionTarget condition — should be ignored
        let mut pod0 = make_indexed_pod(
            "pod-0-fail",
            "default",
            Phase::Failed,
            "ignore-job",
            "job-uid-1",
            0,
        );
        if let Some(ref mut status) = pod0.status {
            status.conditions = Some(vec![PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-0-fail", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "ignore-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // The job should NOT have a Failed condition — the ignored pod shouldn't
        // trigger backoff limit exceeded
        let has_failed = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Failed" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !has_failed,
            "Job should NOT be Failed — the pod with DisruptionTarget should be ignored"
        );

        // status.failed should not count the ignored pod
        assert_eq!(
            status.failed,
            Some(0),
            "Ignored pod should not be counted in status.failed"
        );
    }

    #[tokio::test]
    async fn test_adopt_matching_orphans() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector that matches controller-uid
        let mut job = make_job("adopt-job", "default", 2, 2);
        let job_uid = "adopt-uid-123";
        job.metadata.uid = job_uid.to_string();
        // Set up a selector with controller-uid (like the API server auto-generates)
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });
        // Also set template labels to include controller-uid
        job.spec.template.metadata = Some(ObjectMeta {
            labels: Some({
                let mut m = HashMap::new();
                m.insert("controller-uid".to_string(), job_uid.to_string());
                m.insert("job-name".to_string(), "adopt-job".to_string());
                m
            }),
            ..Default::default()
        });

        let job_key = "/registry/jobs/default/adopt-job";
        storage.create(job_key, &job).await.unwrap();

        // Create an orphan pod that has matching labels but NO ownerReference
        let orphan_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "orphan-pod-1".to_string(),
                namespace: Some("default".to_string()),
                uid: "orphan-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job".to_string());
                    m
                }),
                owner_references: None, // No ownerReference — orphan
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/orphan-pod-1", &orphan_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan pod should now have an ownerReference to the job
        let updated_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod-1")
            .await
            .unwrap();
        let has_owner_ref = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid && r.kind == "Job"))
            .unwrap_or(false);
        assert!(
            has_owner_ref,
            "Orphan pod should be adopted with ownerReference pointing to the job"
        );
    }

    #[tokio::test]
    async fn test_release_non_matching_pods() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector
        let mut job = make_job("release-job", "default", 2, 2);
        let job_uid = "release-uid-456";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/release-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod that has an ownerReference to this job but WRONG labels
        let non_matching_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "wrong-label-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "wrong-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), "different-uid".to_string());
                    m.insert("job-name".to_string(), "release-job".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "release-job".to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/wrong-label-pod", &non_matching_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The non-matching pod should have had its ownerReference removed (released)
        let updated_pod: Pod = storage
            .get("/registry/pods/default/wrong-label-pod")
            .await
            .unwrap();
        let still_owned = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !still_owned,
            "Pod with non-matching labels should be released (ownerReference removed)"
        );
    }

    #[tokio::test]
    async fn test_do_not_adopt_pods_owned_by_another_controller() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("adopt-job2", "default", 2, 2);
        let job_uid = "adopt-uid-789";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/adopt-job2";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod with matching labels but already owned by ANOTHER controller
        let owned_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "other-owned-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "other-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job2".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "other-job".to_string(),
                    uid: "other-job-uid".to_string(),
                    controller: Some(true), // Already owned by another controller
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/other-owned-pod", &owned_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The pod should NOT be adopted — it's owned by another controller
        let updated_pod: Pod = storage
            .get("/registry/pods/default/other-owned-pod")
            .await
            .unwrap();
        let adopted_by_us = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !adopted_by_us,
            "Pod owned by another controller should NOT be adopted"
        );
    }

    #[tokio::test]
    async fn test_auto_selector_adoption_without_explicit_selector() {
        let storage = Arc::new(MemoryStorage::new());

        // Job WITHOUT an explicit selector (backwards compat — old-style)
        let mut job = make_job("legacy-job", "default", 1, 1);
        job.metadata.uid = "legacy-uid".to_string();
        // No selector set — relies on job-name label

        let job_key = "/registry/jobs/default/legacy-job";
        storage.create(job_key, &job).await.unwrap();

        // Create orphan pod with just job-name label, no ownerRef
        let orphan = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "legacy-orphan".to_string(),
                namespace: Some("default".to_string()),
                uid: "legacy-orphan-uid".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), "legacy-job".to_string());
                    m
                }),
                owner_references: None,
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/legacy-orphan", &orphan)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan should be adopted via job-name label fallback
        let updated_pod: Pod = storage
            .get("/registry/pods/default/legacy-orphan")
            .await
            .unwrap();
        let adopted = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.uid == "legacy-uid" && r.kind == "Job")
            })
            .unwrap_or(false);
        assert!(
            adopted,
            "Orphan pod should be adopted via job-name label fallback"
        );
    }

    #[tokio::test]
    async fn test_success_policy_all_indexes_succeeded() {
        // Test 47: "with successPolicy should succeeded when all indexes succeeded"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-all-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-2"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-all-job";
        storage.create(job_key, &job).await.unwrap();

        // All indexes succeed
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-all-job",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        // Should have SuccessCriteriaMet condition
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have SuccessCriteriaMet condition with reason SuccessPolicy"
        );

        // Should have Complete condition
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have Complete condition with reason SuccessPolicy"
        );

        assert!(status.completion_time.is_some());
        assert_eq!(status.succeeded, Some(3));
        // completedIndexes should list all three
        assert_eq!(status.completed_indexes.as_deref(), Some("0-2"));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count_rule() {
        // Test 48: "with successPolicy succeededCount rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count2", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededCount": 3
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-count2";
        storage.create(job_key, &job).await.unwrap();

        // 3 indexes succeed, 2 still running
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }
        for i in 3..5 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when succeededCount rule met"
        );
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Complete condition should be set"
        );
        // K8s sets terminating to 0 when the job completes, even if pods
        // are still being cleaned up. The job status reflects the final state.
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes_rule() {
        // Test 49: "with successPolicy succeededIndexes rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-idx-rule", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        // Only indexes 0 and 4 need to succeed
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0,4"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-idx-rule";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 4 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod4 = make_indexed_pod(
            "pod-4",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            4,
        );
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        // Indexes 1-3 still running
        for i in 1..4 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-idx-rule",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when required indexes succeeded"
        );
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Complete condition should have reason SuccessPolicy"
        );
        // K8s sets terminating to 0 when the job completes
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
        assert_eq!(status.succeeded, Some(2));
        assert!(status.completion_time.is_some());
    }

    /// Test that when a Job completes via successPolicy, the status has
    /// terminating=0 (not the count of pods being terminated).
    /// K8s ref: test/e2e/apps/job.go:596 checks terminating==0
    #[tokio::test]
    async fn test_success_policy_sets_terminating_zero() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-job", "default", 2, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{"succeededCount": 1}]
            }))
            .unwrap(),
        );
        storage
            .create("/registry/jobs/default/sp-job", &job)
            .await
            .unwrap();

        let job_uid = job.metadata.uid.clone();

        // Create one succeeded pod (index 0)
        let mut pod = make_pod("sp-job-0", "default", Phase::Succeeded, "sp-job", &job_uid);
        pod.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "0".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-0", &pod)
            .await
            .unwrap();

        // Create one running pod (index 1) that will need termination
        let mut pod1 = make_pod("sp-job-1", "default", Phase::Running, "sp-job", &job_uid);
        pod1.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "1".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let updated_job: Job = storage.get("/registry/jobs/default/sp-job").await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via success policy
        assert!(
            status.conditions.as_ref().is_some_and(|c| c
                .iter()
                .any(|cond| cond.condition_type == "SuccessCriteriaMet" && cond.status == "True")),
            "Job should have SuccessCriteriaMet condition"
        );

        // terminating MUST be 0, not the count of pods being terminated
        assert_eq!(
            status.terminating,
            Some(0),
            "terminating should be 0 when job completes via successPolicy"
        );

        // ready should be 0
        assert_eq!(status.ready, Some(0));
    }
}
