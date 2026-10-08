use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::workloads::{
    Job, JobCondition, JobStatus, UncountedTerminatedPods,
};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{OwnerReference, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};

use super::expectations::ControllerExpectations;
use super::job_tracking::{
    clean_uncounted_pods_without_finalizers, has_job_tracking_finalizer, push_uncounted_failed,
    push_uncounted_succeeded, remove_tracking_finalizer, uncounted_has_failed,
    uncounted_has_succeeded, FinalizerExpectations, JOB_TRACKING_FINALIZER,
};
use super::replicationcontroller::slow_start_batches_capped;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// Upstream `ConcurrentJobSyncs` default, workers launched by `Run`
/// (pkg/controller/job/config/v1alpha1/defaults.go:34;
/// pkg/controller/job/job_controller.go:268-270). Upstream also starts one
/// `orphanWorker` per worker (:272); that sweep is `reconcile_orphan_pods`
/// here and is unchanged.
const CONCURRENT_JOB_SYNCS: usize = 5;

/// `MaxPodCreateDeletePerSync` (`job_controller.go:79`): "the maximum number of
/// pods that can be created or deleted in a single sync call". Without it
/// slow-start batches double unbounded; this is the cap that makes batching
/// safe, applied to both creations (`:1735-1737`) and deletions (`:1700-1702`).
const MAX_POD_CREATE_DELETE_PER_SYNC: usize = 500;

/// `controller.SlowStartInitialBatchSize` (`controller_utils.go:87`).
const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;

pub struct JobController<S: Storage> {
    storage: Arc<S>,
    /// Per-Job ("ns/name") expectations of in-flight pod creates and deletes.
    /// Upstream `jm.expectations` (`job_controller.go:100`,
    /// `controller.NewControllerExpectations()`); shared implementation in
    /// [`super::expectations`]. Read BEFORE listing pods in `reconcile`
    /// (`:905`) and gating `manageJob` (`:1016`).
    expectations: Arc<ControllerExpectations>,
    /// Pod UIDs whose tracking-finalizer removal has been issued but not yet
    /// observed. Upstream's `uidTrackingExpectations`
    /// (`pkg/controller/job/tracking_utils.go:48`) — the brake that stops a
    /// stale pod list from claiming the same termination twice.
    finalizer_expectations: FinalizerExpectations,
    /// Delayed re-syncs requested by `reconcile`, keyed `namespace/name`.
    /// Stands in for upstream's `jm.queue.AddAfter` call inside `manageJob`
    /// (`enqueueSyncJobWithDelay`, `job_controller.go:620`); the worker drains
    /// it after each sync and calls `WorkQueue::add_after`.
    requeue_delays: std::sync::Mutex<HashMap<String, Duration>>,
}

/// `SyncJobBatchPeriod` (`job_controller.go:64`).
const SYNC_JOB_BATCH_PERIOD: Duration = Duration::from_secs(1);

/// `enqueueSyncJobWithDelay` (`job_controller.go:620`): "custom delay, but not
/// smaller than the batching delay".
fn requeue_delay_for(delay: Duration) -> Duration {
    delay.max(SYNC_JOB_BATCH_PERIOD)
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

/// Port of `controller.IsPodActive` (pkg/controller/controller_utils.go:1085):
/// `v1.PodSucceeded != p.Status.Phase && v1.PodFailed != p.Status.Phase &&
/// p.DeletionTimestamp == nil`. Callers additionally match Running|Pending.
fn is_pod_active(pod: &Pod) -> bool {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
    !matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed))
        && pod.metadata.deletion_timestamp.is_none()
}

/// Pods that are still Running/Pending in storage - active ones this sync just
/// deleted gracefully plus ones already terminating. Upstream's
/// `CountTerminatingPods` (controller_utils.go:1021) is `IsPodTerminating`
/// (:1091): not terminal and `DeletionTimestamp != nil`; here it is taken
/// from the pre-delete listing, so pods just stamped by this sync count too.
fn count_unfinished_pods(pods: &[Pod]) -> i32 {
    pods.iter()
        .filter(|p| {
            matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running) | Some(Phase::Pending)
            )
        })
        .count() as i32
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
            expectations: Arc::new(ControllerExpectations::new()),
            finalizer_expectations: FinalizerExpectations::new(),
            requeue_delays: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// `addPod` / `updatePod` / `deletePod` expectation bookkeeping
    /// (`job_controller.go:339`, `:370-378`, `:480`): a pod ADDED for a Job
    /// observes one creation (`CreationObserved`); a pod that is deleted, or
    /// just stamped with a deletionTimestamp (`updatePod` routes those to
    /// `deletePod(final=false)`), observes one deletion (`DeletionObserved`).
    ///
    /// Deviation: upstream's expectations are the plain counter and lower on
    /// every event; here each pod is tracked by key (see
    /// [`ControllerExpectations::creation_observed_of`] /
    /// [`ControllerExpectations::deletion_observed_of`]) so the several
    /// MODIFIED events and the final DELETED of one pod count once.
    fn observe_pod_event(&self, event: &rusternetes_storage::WatchEvent) {
        let (json, is_added, is_deleted) = match event {
            rusternetes_storage::WatchEvent::Added(_, v) => (v, true, false),
            rusternetes_storage::WatchEvent::Modified(_, v) => (v, false, false),
            rusternetes_storage::WatchEvent::Deleted(_, v) => (v, false, true),
        };
        let Ok(pod) = serde_json::from_str::<Pod>(json) else {
            return;
        };
        let Some(owner) = pod.metadata.owner_references.as_ref().and_then(|refs| {
            refs.iter()
                .find(|r| r.kind == "Job" && r.controller.unwrap_or(true))
        }) else {
            return;
        };
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let exp_key = format!("{}/{}", ns, owner.name);
        let pod_key = format!("{}/{}", ns, pod.metadata.name);
        let deleting = is_deleted || pod.metadata.deletion_timestamp.is_some();
        if deleting {
            self.expectations.deletion_observed_of(&exp_key, &pod_key);
        } else if is_added {
            self.expectations.creation_observed_of(&exp_key, &pod_key);
        }
    }

    /// Port of `deleteJobPods` (`job_controller.go:1160-1196`), including the
    /// caller's `ExpectDeletions` (`:1666`, `:1707`). Returns
    /// `(deleted ready pods, successful deletions, first error)`.
    ///
    /// Each pod first loses its tracking finalizer (`removeTrackingFinalizerPatch`
    /// and `PatchPod`, `:1181-1186`) so it is never counted as a success or
    /// failure; then it is deleted gracefully (`podControl.DeletePod`,
    /// `controller_utils.go:618`). `failDelete` (`:1167`) lowers the deletion
    /// expectation because the informer will never see it, and a NotFound is
    /// benign (neither reduces the success count nor surfaces an error).
    async fn delete_job_pods(
        &self,
        exp_key: &str,
        namespace: &str,
        pods: &[&Pod],
    ) -> (i32, i32, Option<anyhow::Error>) {
        if pods.is_empty() {
            return (0, 0, None);
        }
        let pod_keys: Vec<String> = pods
            .iter()
            .map(|p| format!("{}/{}", namespace, p.metadata.name))
            .collect();
        self.expectations.expect_deletions_of(exp_key, &pod_keys);

        let results = futures::future::join_all(pods.iter().zip(&pod_keys).map(
            |(pod, observed)| async move {
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                let fail = |e: rusternetes_common::Error| {
                    self.expectations.deletion_observed_of(exp_key, observed);
                    e
                };
                if has_job_tracking_finalizer(pod) {
                    let patched = match self.storage.get::<Pod>(&pod_key).await {
                        Ok(mut fresh) => {
                            if remove_tracking_finalizer(&mut fresh) {
                                self.storage.update(&pod_key, &fresh).await.map(|_| ())
                            } else {
                                Ok(())
                            }
                        }
                        Err(e) => Err(e),
                    };
                    if let Err(e) = patched {
                        return (false, Some(fail(e)));
                    }
                }
                let err = self
                    .storage
                    .delete_gracefully(&pod_key)
                    .await
                    .err()
                    .map(fail);
                (true, err)
            },
        ))
        .await;

        let mut ready = 0;
        let mut removed = pods.len() as i32;
        let mut first_err: Option<anyhow::Error> = None;
        for (pod, (reached_delete, err)) in pods.iter().zip(results) {
            if reached_delete && is_pod_ready(pod) {
                ready += 1;
            }
            if let Some(e) = err {
                if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                    warn!("Failed to delete Job pod {}: {}", pod.metadata.name, e);
                    removed -= 1;
                    first_err.get_or_insert(e.into());
                }
            }
        }
        (ready, removed, first_err)
    }

    /// Record a delayed re-sync for a Job, keeping the earliest request.
    fn request_requeue(&self, namespace: &str, name: &str, delay: Duration) {
        let mut map = self.requeue_delays.lock().unwrap();
        map.entry(format!("{namespace}/{name}"))
            .and_modify(|d| *d = (*d).min(delay))
            .or_insert(delay);
    }

    /// Take the delayed re-sync requested by the last `reconcile` of a Job,
    /// already clamped to `SyncJobBatchPeriod`.
    fn take_requeue_delay(&self, namespace: &str, name: &str) -> Option<Duration> {
        self.requeue_delays
            .lock()
            .unwrap()
            .remove(&format!("{namespace}/{name}"))
            .map(requeue_delay_for)
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
    #[allow(clippy::too_many_arguments)]
    fn track_terminated_pods(
        &self,
        job_key: &str,
        persisted: Option<&JobStatus>,
        job_pods: &[Pod],
        never_count_failed: &HashSet<String>,
        is_indexed: bool,
        only_replace_failed_pods: bool,
        delayed_deletion_uids: &HashSet<String>,
        job_terminal: bool,
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
                    // `canRemoveFinalizer` (`job_controller.go:1359`): the last
                    // failed pod of an index is neither counted nor released
                    // until a replacement for the index exists, because it is
                    // the only carrier of the index's failure count and
                    // failure time (`podsWithDelayedDeletionPerIndex`). A Job
                    // that is terminal or being deleted overrides this.
                    if !job_terminal && delayed_deletion_uids.contains(uid) {
                        continue;
                    }
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
        // Upstream's `enactJobFinished` (`job_controller.go:1509-1519`) refuses
        // to add the terminal condition while `.status.uncountedTerminatedPods`
        // is non-empty: only the interim `FailureTarget` / `SuccessCriteriaMet`
        // condition goes out with the first flush, and Complete/Failed follows
        // once the UIDs are folded into the counters. The kube-apiserver
        // enforces the same ("must be empty for finished job",
        // `validateJobStatus`), so writing both at once is rejected and the
        // Job never finishes.
        let uncounted_len = |j: &Job| {
            j.status
                .as_ref()
                .and_then(|s| s.uncounted_terminated_pods.as_ref())
                .map_or(0, |u| {
                    u.succeeded.as_ref().map_or(0, |v| v.len())
                        + u.failed.as_ref().map_or(0, |v| v.len())
                })
        };
        let was_finished = job_is_finished(job);
        let mut deferred_terminal: Option<(
            Vec<JobCondition>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = None;
        if was_finished && uncounted_len(job) > 0 {
            if let Some(status) = job.status.as_mut() {
                let conds = status.conditions.take().unwrap_or_default();
                let (terminal, interim): (Vec<_>, Vec<_>) = conds
                    .into_iter()
                    .partition(|c| c.condition_type == "Complete" || c.condition_type == "Failed");
                status.conditions = Some(interim);
                deferred_terminal = Some((terminal, status.completion_time.take()));
            }
        }
        // Re-attach the delayed terminal condition once nothing is uncounted.
        let restore_terminal =
            |j: &mut Job,
             deferred: &mut Option<(Vec<JobCondition>, Option<chrono::DateTime<chrono::Utc>>)>|
             -> bool {
                if uncounted_len(j) > 0 {
                    return false;
                }
                let Some((terminal, completion_time)) = deferred.take() else {
                    return false;
                };
                if let Some(status) = j.status.as_mut() {
                    status
                        .conditions
                        .get_or_insert_with(Vec::new)
                        .extend(terminal);
                    status.completion_time = completion_time;
                }
                true
            };

        self.write_status(key, job).await?;

        // Upstream's `canRemoveFinalizer` (`job_controller.go:1359`) short-
        // circuits to true the moment the Job is being deleted or has reached a
        // terminal condition: nothing more will ever be counted, so holding the
        // pods back only wedges them in `Terminating`.
        let mut to_release: Vec<Pod> = pods_to_release.to_vec();
        if job.metadata.is_being_deleted() || was_finished {
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
        if restore_terminal(job, &mut deferred_terminal) {
            folded = true;
        }
        if folded {
            self.write_status(key, job).await?;
        }
        Ok(())
    }

    /// Persist `job.status` conditionally on the resourceVersion of the Job
    /// this sync read, skipping the write when nothing changed (a redundant
    /// write wakes every watcher).
    ///
    /// Upstream's `updateStatusHandler` is `Jobs(ns).UpdateStatus(ctx, job)`
    /// on the object the sync was handed (`pkg/controller/job/
    /// job_controller.go:1891-1893`); the registry enforces the
    /// resourceVersion, a lost race is a Conflict, `syncJob` returns it and the
    /// workqueue retries with a fresh read. So: no in-place retry and no write
    /// built from a re-read; a status computed from a stale read must never
    /// overwrite a newer one (#2160). The worker already requeues on Err.
    async fn write_status(&self, key: &str, job: &mut Job) -> Result<()> {
        let persisted = match self.storage.get::<Job>(key).await {
            Ok(j) => j,
            Err(e) => {
                // The Job was deleted between the list and this write.
                debug!("Job {} no longer exists: {}", key, e);
                return Ok(());
            }
        };
        // Counters must not go backwards against what is already persisted, or
        // the api-server refuses this and every later write (#1955).
        if let Some(next) = job.status.as_mut() {
            clamp_counters_monotonic(next, persisted.status.as_ref());
        }
        let stale = job.metadata.resource_version.is_some()
            && persisted.metadata.resource_version != job.metadata.resource_version;
        if !stale && persisted.status == job.status {
            return Ok(());
        }
        // update_status_cas refuses (Conflict) when `job` is older than the
        // stored object; the error goes to the worker for requeue.
        let written = self.storage.update_status_cas(key, &*job).await?;
        job.metadata.resource_version = written.metadata.resource_version;
        Ok(())
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting JobController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        // Upstream starts `workers` goroutines over one queue (job_controller.go:268-270).
        spawn_workers(CONCURRENT_JOB_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            // Initial full reconciliation. The orphan sweep is the counterpart
            // of the pod informer's initial list, whose `addPod` calls route
            // every orphan still holding the finalizer to `enqueueOrphanPod`.
            self.enqueue_all(&queue).await;
            self.reconcile_orphan_pods().await;

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
                                if let rusternetes_storage::WatchEvent::Deleted(_, prev) = &ev {
                                    if let Ok(job) = serde_json::from_str::<Job>(prev) {
                                        self.release_orphans_of_deleted_job(&job).await;
                                    }
                                }
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
                        Ok(()) => {
                            queue.forget(&key).await;
                            if let Some(delay) = self.take_requeue_delay(ns, name) {
                                queue.add_after(key.clone(), delay).await;
                            }
                        }
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile. Drop its
                    // expectations so a Job recreated under the same name
                    // starts clean (`DeleteExpectations`,
                    // job_controller.go:839).
                    self.expectations
                        .delete_expectations(&format!("{}/{}", ns, name));
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
        // `addPod` / `updatePod` / `deletePod`: observe the expectation from
        // the event itself — a DELETED pod can no longer be read back.
        self.observe_pod_event(event);
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
                // `addPod` / `updatePod`: a pod still holding the finalizer
                // with no live, counting Job to remove it is an orphan
                // (`job_controller.go:344-347`, `:417-420`).
                self.release_if_orphan(&pod).await;
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
        for pod in &pods {
            self.release_if_orphan(pod).await;
        }
    }

    /// `deleteJob` -> `enqueueLabelSelector` (`job_controller.go:561-581`):
    /// once a Job is gone, sweep the pods its selector matched, since the
    /// deletion itself produces no pod event to reach them by.
    async fn release_orphans_of_deleted_job(&self, job: &Job) {
        let Some(namespace) = job.metadata.namespace.as_deref() else {
            return;
        };
        let Some(selector) = job.spec.selector.as_ref() else {
            return;
        };
        let Ok(selector) = rusternetes_common::types::label_selector_as_selector(Some(selector))
        else {
            return;
        };
        let Ok(pods) = self
            .storage
            .list::<Pod>(&build_prefix("pods", Some(namespace)))
            .await
        else {
            return;
        };
        for pod in pods
            .iter()
            .filter(|p| selector.matches(p.metadata.labels.as_ref()))
        {
            self.release_if_orphan(pod).await;
        }
    }

    /// Port of `handleSingleOrphanPod` (`job_controller.go:736-767`): strip the
    /// tracking finalizer unless the pod is controlled by something that is not
    /// a batch/v1 Job, or by a live Job that is still counting (or is managed
    /// by an external controller).
    async fn release_if_orphan(&self, pod: &Pod) {
        if !has_job_tracking_finalizer(pod) {
            return;
        }
        let Some(namespace) = pod.metadata.namespace.as_deref() else {
            return;
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
                return;
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
                        return;
                    }
                    // The Job is alive and still counting. Leave it alone.
                    if !job_is_finished(&job) {
                        return;
                    }
                }
            }
        }

        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        let Ok(mut fresh) = self.storage.get::<Pod>(&pod_key).await else {
            return;
        };
        if !remove_tracking_finalizer(&mut fresh) {
            return;
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
                                // Status subresource write (#1723), conditional
                                // on the fresh read's resourceVersion (#2160).
                                let _ = self.storage.update_status_cas(&key, &fresh_job).await;
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

        // Expectations are read BEFORE listing pods: "otherwise a new pod can
        // sneak in and update the expectations after we've retrieved active
        // pods from the store" (job_controller.go:902-905); upstream's
        // `TestRSSyncExpectations` pins the same ordering.
        let exp_key = format!("{}/{}", namespace, name);
        let mut satisfied_expectations = self.expectations.satisfied(&exp_key);
        // First error from the manage phase; upstream returns it only after
        // the status has been written (`manageJobErr`, job_controller.go:1090).
        let mut manage_err: Option<anyhow::Error> = None;

        // Get current pods for this Job
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Rusternetes deviation (no upstream equivalent; same intent as the
        // DaemonSet/ReplicaSet settle-from-observation step): a pod this Job
        // is still waiting to see created that the fresh listing already
        // shows, or deleted that it shows gone or terminating, HAS been
        // observed, so settle it now rather than block on a watch event.
        if !satisfied_expectations {
            for pending in self.expectations.pending_creations(&exp_key) {
                if all_pods
                    .iter()
                    .any(|p| format!("{}/{}", namespace, p.metadata.name) == pending)
                {
                    self.expectations.creation_observed_of(&exp_key, &pending);
                }
            }
            for pending in self.expectations.pending_deletions(&exp_key) {
                let still_live = all_pods.iter().any(|p| {
                    p.metadata.deletion_timestamp.is_none()
                        && format!("{}/{}", namespace, p.metadata.name) == pending
                });
                if !still_live {
                    self.expectations.deletion_observed_of(&exp_key, &pending);
                }
            }
            satisfied_expectations = self.expectations.satisfied(&exp_key);
        }

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

        // For Indexed completion mode, report the durable succeeded-index set.
        // Computed here, before ANY status write: every write site must carry
        // it, or a suspend or deadline pass would blank `.status.completedIndexes`
        // in the same breath as releasing the pods that were the only other
        // record of those indexes.
        let completed_indexes: Option<String> = if is_indexed && !succeeded_index_set.is_empty() {
            let mut indexes: Vec<i32> = succeeded_index_set.iter().copied().collect();
            indexes.sort();
            Some(format_index_ranges(&indexes))
        } else {
            None
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

        // backoffLimitPerIndex: which indexes are exhausted, and which failed
        // pods must be held back as the carrier of an index's failure count.
        // Computed BEFORE tracking because `canRemoveFinalizer` consumes it.
        // Upstream seeds `calculateFailedIndexes` from `.status.failedIndexes`
        // (`indexed_job_utils.go:80`) so a failed index survives its pods being
        // deleted; the persisted set is merged in here for the same reason.
        let backoff_limit_per_index = job.spec.backoff_limit_per_index;
        let mut backoff_failed_index_set: HashSet<i32> = HashSet::new();
        let mut delayed_deletion: HashMap<i32, Pod> = HashMap::new();
        if is_indexed {
            if let Some(per_index_limit) = backoff_limit_per_index {
                backoff_failed_index_set =
                    indexes_over_backoff_limit(job_pods.iter(), per_index_limit);
                if let Some(prev) = job
                    .status
                    .as_ref()
                    .and_then(|s| s.failed_indexes.as_deref())
                {
                    backoff_failed_index_set.extend(parse_index_ranges(prev));
                }
                let failed_now: HashSet<i32> = fail_index_set
                    .union(&backoff_failed_index_set)
                    .copied()
                    .collect();
                delayed_deletion = pods_with_delayed_deletion_per_index(
                    &job_pods,
                    job.spec.completions.unwrap_or(1),
                    &succeeded_index_set,
                    &failed_now,
                    only_replace_failed_pods,
                );
            }
        }
        let delayed_deletion_uids: HashSet<String> = delayed_deletion
            .values()
            .map(|p| p.metadata.uid.clone())
            .collect();
        let tracked = self.track_terminated_pods(
            &format!("{}/{}", namespace, name),
            job.status.as_ref(),
            &job_pods,
            &never_count_failed,
            is_indexed,
            only_replace_failed_pods,
            &delayed_deletion_uids,
            job.metadata.is_being_deleted() || job_is_finished(job),
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
                // `FilterActivePods` (controller_utils.go:1001): a pod with a
                // deletionTimestamp is not active, and not counted ready either
                // (`ready: countReadyPods(activePods)`, job_controller.go:912).
                if !is_pod_active(pod) {
                    continue;
                }
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

        // Handle suspended jobs: delete all active pods (`manageJob`,
        // job_controller.go:1663-1673): `activePodsForRemoval(.., active)`,
        // `ExpectDeletions`, `deleteJobPods`; only when expectations are
        // satisfied (`:1016`).
        if job.spec.suspend.unwrap_or(false) {
            let mut removed = 0;
            let mut removed_ready = 0;
            if satisfied_expectations && active > 0 {
                let active_pods = active_job_pods(job_pods.iter());
                let to_delete = active_pods_for_removal(job, &active_pods, active as usize);
                let (rr, r, err) = self.delete_job_pods(&exp_key, namespace, &to_delete).await;
                removed_ready = rr;
                removed = r;
                manage_err = err;
                info!(
                    "Suspended job {}/{}: deleted {} active pods",
                    namespace, name, removed
                );
            }
            let active = active - removed;
            let ready = ready - removed_ready;
            // Preserve existing start_time
            let existing_start_time = job.status.as_ref().and_then(|s| s.start_time);
            let existing_conditions = job.status.as_ref().and_then(|s| s.conditions.clone());
            job.status = Some(JobStatus {
                active: Some(active),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: existing_conditions,
                start_time: existing_start_time,
                completion_time: None,
                ready: Some(ready),
                terminating: None,
                completed_indexes: completed_indexes.clone(),
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
            return match manage_err {
                Some(e) => Err(e),
                None => Ok(()),
            };
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
                    for pod in job_pods.iter().filter(|p| is_pod_active(p)) {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                            // `deleteActivePods` -> `podControl.DeletePod`
                            // (job_controller.go:1122-1140).
                            let _ = self.storage.delete_gracefully(&pod_key).await;
                        }
                    }
                    // `enactJobFinished` (job_controller.go:1520-1524): hold the
                    // terminal Failed condition back while terminating pods
                    // remain; only FailureTarget is published meanwhile.
                    let terminating = count_unfinished_pods(&job_pods);
                    let mut conditions = failed_job_conditions(
                        "DeadlineExceeded".to_string(),
                        format!(
                            "Job was active longer than specified deadline of {} seconds",
                            deadline
                        ),
                    );
                    if terminating > 0 {
                        conditions.pop(); // drop Failed, keep FailureTarget
                    }
                    job.status = Some(JobStatus {
                        active: Some(0),
                        succeeded: status_succeeded,
                        failed: status_failed,
                        conditions: Some(conditions),
                        start_time: job.status.as_ref().and_then(|s| s.start_time),
                        // completionTime is valid ONLY on a Complete job
                        // (validation.go:505-513: "cannot set completionTime
                        // when there is no Complete=True condition").
                        completion_time: None,
                        ready: Some(ready),
                        terminating: if terminating > 0 {
                            Some(terminating)
                        } else {
                            None
                        },
                        completed_indexes: completed_indexes.clone(),
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

            // Delete remaining active pods through `deleteActivePods` ->
            // `podControl.DeletePod` (job_controller.go:1002, :1122-1140;
            // controller_utils.go:618): a GRACEFUL delete, the pod lingers with
            // a deletionTimestamp until the kubelet and the tracking-finalizer
            // removal reap it.
            for pod in job_pods.iter().filter(|p| is_pod_active(p)) {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    let _ = self.storage.delete_gracefully(&pod_key).await;
                }
            }

            // `enactJobFinished` (job_controller.go:1520-1524): the terminal
            // condition is delayed while terminating pods remain, so that
            // status.terminating == 0 holds whenever Complete is set (the
            // conformance spec asserts it). Meanwhile only the interim
            // SuccessCriteriaMet condition is published.
            let terminating = count_unfinished_pods(&job_pods);
            let mut conditions = complete_job_conditions(
                "SuccessPolicy".to_string(),
                "Matched rules in the SuccessPolicy".to_string(),
            );
            if terminating > 0 {
                conditions.pop(); // drop Complete, keep SuccessCriteriaMet
            }

            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions: Some(conditions),
                start_time,
                completion_time: (terminating == 0).then(chrono::Utc::now),
                ready: Some(0), // Job is complete, no ready pods
                terminating: Some(terminating),
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

            let fresh_active_pods = active_job_pods(fresh_job_pods.iter().copied());
            let fresh_active = fresh_active_pods.len() as i32;

            // `manageJob` (job_controller.go:1653-1830), run only while
            // expectations are satisfied (`:1016`).
            let mut pods_needed = 0i32;
            if satisfied_expectations {
                // `wantActive` (`:1677-1696`).
                let want_active = match job.spec.completions {
                    // No completions: "number active should be equal to
                    // parallelism, unless the job has seen at least once
                    // success, in which leave whatever is running, running."
                    None if succeeded > 0 => fresh_active,
                    None => parallelism,
                    Some(c) => (c - succeeded).clamp(0, parallelism.max(0)),
                };
                let rm_at_least = (fresh_active - want_active).max(0) as usize;
                let mut to_delete = active_pods_for_removal(job, &fresh_active_pods, rm_at_least);
                to_delete.truncate(MAX_POD_CREATE_DELETE_PER_SYNC);
                if !to_delete.is_empty() {
                    // "restrict ourselves to either just pod deletion or pod
                    // creation in any given sync cycle. Of these two, pod
                    // deletion takes precedence." (`:1717-1720`)
                    info!(
                        "Too many pods running for job {}/{}: deleting {} (target {})",
                        namespace,
                        name,
                        to_delete.len(),
                        want_active
                    );
                    let (rr, r, err) = self.delete_job_pods(&exp_key, namespace, &to_delete).await;
                    active = fresh_active - r;
                    ready -= rr;
                    manage_err = err;
                } else {
                    // `diff := wantActive - terminating - active` (`:1722-1728`):
                    // with podReplacementPolicy=Failed a terminating pod is
                    // not replaced until it has failed.
                    let terminating = if only_replace_failed_pods {
                        count_terminating_pods(fresh_job_pods.iter().copied())
                    } else {
                        0
                    };
                    pods_needed = (want_active - terminating - fresh_active)
                        .min(MAX_POD_CREATE_DELETE_PER_SYNC as i32);
                }
            }

            if pods_needed > 0 {
                // For Indexed mode, find which indexes still need pods
                let indexes_to_create: Vec<i32> = if is_indexed {
                    // Track indexes that already have active or succeeded pods
                    let mut active_or_succeeded_indexes: HashSet<i32> = HashSet::new();
                    for pod in fresh_job_pods.iter() {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if (is_pod_active(pod)
                            && matches!(phase, Some(Phase::Running) | Some(Phase::Pending)))
                            || matches!(phase, Some(Phase::Succeeded))
                        {
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
                    // `firstPendingIndexes` (`job_controller.go:1743`): the
                    // first `diff` indexes that are neither active, succeeded
                    // nor failed.
                    let pending: Vec<i32> = (0..completions)
                        .filter(|i| {
                            !active_or_succeeded_indexes.contains(i)
                                && !exhausted_indexes.contains(i)
                        })
                        .take(pods_needed as usize)
                        .collect();
                    if backoff_limit_per_index.is_some() {
                        // `getPodCreationInfoForIndependentIndexes`
                        // (`job_controller.go:1850`): an index is only retried
                        // once its own failure backoff has elapsed; when none
                        // is ready, `enqueueSyncJobWithDelay` re-runs the sync
                        // after the smallest remaining time
                        // (`job_controller.go:1746-1749`).
                        let now = chrono::Utc::now();
                        let mut now_ready: Vec<i32> = Vec::new();
                        let mut min_remaining: Option<Duration> = None;
                        for idx in pending {
                            let remaining =
                                remaining_time_per_index(now, delayed_deletion.get(&idx));
                            if remaining.is_zero() {
                                now_ready.push(idx);
                            } else if min_remaining.is_none_or(|m| remaining < m) {
                                min_remaining = Some(remaining);
                            }
                        }
                        if now_ready.is_empty() {
                            if let Some(m) = min_remaining {
                                self.request_requeue(namespace, name, m);
                            }
                        }
                        now_ready
                    } else {
                        pending
                    }
                } else {
                    (0..pods_needed).collect()
                };

                // `ExpectCreations(diff)` BEFORE any create (`:1755`), so a
                // sync re-entered by our own creates' watch events finds the
                // record unmet and does nothing until they are observed.
                let diff = indexes_to_create.len();
                if diff > 0 {
                    self.expectations.expect_creations(&exp_key, diff as i64);
                    self.expectations.clear_created(&exp_key);
                }
                active = fresh_active + diff as i32;

                // Slow-start batches (`:1771-1827`): sizes start at
                // `SlowStartInitialBatchSize` and double after each fully
                // successful batch, so a failure that would hit every create
                // (quota, admission) costs one create, not `diff`.
                let mut attempted = 0usize;
                for batch in
                    slow_start_batches_capped(diff, SLOW_START_INITIAL_BATCH_SIZE, usize::MAX)
                {
                    let todo = &indexes_to_create[attempted..attempted + batch];
                    attempted += batch;
                    let results = futures::future::join_all(todo.iter().map(|idx| {
                        // `addIndexFailureCountAnnotation` (`indexed_job_utils.go:350`)
                        let failure_counts = if is_indexed && backoff_limit_per_index.is_some() {
                            let replaced = delayed_deletion.get(idx);
                            Some(new_index_failure_counts(
                                replaced,
                                replaced.is_some_and(|p| ignored_pods.contains(&p.metadata.name)),
                            ))
                        } else {
                            None
                        };
                        let job_ref: &Job = job;
                        async move {
                            self.create_pod(job_ref, namespace, *idx, is_indexed, failure_counts)
                                .await
                        }
                    }))
                    .await;
                    let mut batch_failed = false;
                    for result in results {
                        match result {
                            Ok(pod_name) => {
                                self.expectations.record_created(
                                    &exp_key,
                                    &format!("{}/{}", namespace, pod_name),
                                );
                            }
                            Err(e) => {
                                // "Decrement the expected number of creates
                                // because the informer won't observe this pod"
                                // (`:1810-1812`). Upstream skips this for a
                                // NamespaceTerminating cause (`:1796-1801`);
                                // lowering for every failure is the safe
                                // superset, so the record never waits out its
                                // TTL.
                                self.expectations.creation_observed(&exp_key);
                                active -= 1;
                                let err_str = format!("{}", e);
                                if err_str.contains("already exists")
                                    || err_str.contains("AlreadyExists")
                                {
                                    debug!(
                                        "Pod already exists for Job {}/{}, skipping",
                                        namespace, name
                                    );
                                    continue;
                                }
                                warn!("Failed to create pod for Job {}/{}: {}", namespace, name, e);
                                batch_failed = true;
                                manage_err.get_or_insert(e);
                            }
                        }
                    }
                    // "any skipped pods that we never attempted to start
                    // shouldn't be expected" (`:1818-1830`).
                    let skipped = diff - attempted;
                    if batch_failed && skipped > 0 {
                        self.expectations
                            .lower_expectations(&exp_key, skipped as i64, 0);
                        active -= skipped as i32;
                        break;
                    }
                }
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

        // `manageJobErr` surfaces only after the status write
        // (job_controller.go:1090).
        match manage_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Create one pod from the Job's template; returns its generated name.
    async fn create_pod(
        &self,
        job: &Job,
        namespace: &str,
        index: i32,
        is_indexed: bool,
        index_failure_counts: Option<(i32, i32)>,
    ) -> Result<String> {
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

        if let Some((failure_count, ignored_count)) = index_failure_counts {
            annotations.insert(
                JOB_INDEX_FAILURE_COUNT_ANNOTATION.to_string(),
                failure_count.to_string(),
            );
            if ignored_count > 0 {
                annotations.insert(
                    JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION.to_string(),
                    ignored_count.to_string(),
                );
            }
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

        Ok(pod_name)
    }
}

/// `podutil.IsPodReady`: the Ready condition is True.
fn is_pod_ready(pod: &Pod) -> bool {
    pod_ready_condition(pod).is_some()
}

fn pod_ready_condition(pod: &Pod) -> Option<&rusternetes_common::resources::pod::PodCondition> {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.condition_type == "Ready" && c.status == "True")
        })
}

/// `controller.CountTerminatingPods` (`controller_utils.go:1021`): not yet
/// terminal, with a deletionTimestamp.
fn count_terminating_pods<'a>(pods: impl IntoIterator<Item = &'a Pod>) -> i32 {
    pods.into_iter()
        .filter(|p| {
            p.metadata.deletion_timestamp.is_some()
                && !matches!(
                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Succeeded) | Some(Phase::Failed)
                )
        })
        .count() as i32
}

/// `(regularRestarts, sidecarRestarts)` — `maxContainerRestarts`
/// (`controller_utils.go:948`).
fn max_container_restarts(pod: &Pod) -> (u32, u32) {
    let regular = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .map_or(0, |cs| {
            cs.iter().map(|c| c.restart_count).max().unwrap_or(0)
        });
    let sidecars: HashSet<&str> = pod
        .spec
        .as_ref()
        .and_then(|s| s.init_containers.as_ref())
        .map(|ics| {
            ics.iter()
                .filter(|c| c.restart_policy.as_deref() == Some("Always"))
                .map(|c| c.name.as_str())
                .collect()
        })
        .unwrap_or_default();
    let sidecar = pod
        .status
        .as_ref()
        .and_then(|s| s.init_container_statuses.as_ref())
        .map_or(0, |cs| {
            cs.iter()
                .filter(|c| sidecars.contains(c.name.as_str()))
                .map(|c| c.restart_count)
                .max()
                .unwrap_or(0)
        });
    (regular, sidecar)
}

/// `afterOrZero` (`controller_utils.go:913`).
fn after_or_zero(
    t1: Option<chrono::DateTime<chrono::Utc>>,
    t2: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    match (t1, t2) {
        (Some(a), Some(b)) => a > b,
        (a, _) => a.is_none(),
    }
}

/// `controller.ActivePods.Less` (`controller_utils.go:741-776`): true when
/// `a` should be deleted before `b`.
fn active_pods_less(a: &Pod, b: &Pod) -> bool {
    // 1. Unassigned < assigned.
    let node = |p: &Pod| {
        p.spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default()
    };
    let (na, nb) = (node(a), node(b));
    if na != nb && (na.is_empty() || nb.is_empty()) {
        return na.is_empty();
    }
    // 2. PodPending < PodUnknown < PodRunning (`podPhaseToOrdinal`).
    let ordinal = |p: &Pod| match p.status.as_ref().and_then(|s| s.phase.as_ref()) {
        Some(Phase::Unknown) => 1,
        Some(Phase::Running) => 2,
        _ => 0,
    };
    if ordinal(a) != ordinal(b) {
        return ordinal(a) < ordinal(b);
    }
    // 3. Not ready < ready.
    if is_pod_ready(a) != is_pod_ready(b) {
        return !is_pod_ready(a);
    }
    // 4. Ready for less time < ready for more time.
    if let (Some(ca), Some(cb)) = (pod_ready_condition(a), pod_ready_condition(b)) {
        if ca.last_transition_time != cb.last_transition_time {
            return after_or_zero(ca.last_transition_time, cb.last_transition_time);
        }
    }
    // 5. More restarts < fewer restarts (`compareMaxContainerRestarts`).
    let (ra, rb) = (max_container_restarts(a), max_container_restarts(b));
    if ra.0 != rb.0 {
        return ra.0 > rb.0;
    }
    if ra.1 != rb.1 {
        return ra.1 > rb.1;
    }
    // 6. Newer < older.
    if a.metadata.creation_timestamp != b.metadata.creation_timestamp {
        return after_or_zero(a.metadata.creation_timestamp, b.metadata.creation_timestamp);
    }
    false
}

fn sort_active_pods(pods: &mut [&Pod]) {
    pods.sort_by(|a, b| {
        if active_pods_less(a, b) {
            std::cmp::Ordering::Less
        } else if active_pods_less(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

/// `getCompletionIndex` (`indexed_job_utils.go:394`): the annotation only,
/// `-1` (`unknownCompletionIndex`) when absent or invalid.
fn completion_index_annotation(pod: &Pod) -> i32 {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|i| *i >= 0)
        .unwrap_or(-1)
}

/// `activePodsForRemoval` (`job_controller.go:1872`) with
/// `appendDuplicatedIndexPodsForRemoval` /
/// `appendPodsWithSameIndexForRemovalAndRemaining`
/// (`indexed_job_utils.go:295`, `:379`): for an Indexed Job, pods without a
/// valid index, with an out-of-range index, or duplicating an index (all but
/// the best-ranked) are removed regardless of `rm_at_least`; the remainder is
/// ranked by `ActivePods` and trimmed to reach `rm_at_least`.
fn active_pods_for_removal<'a>(job: &Job, pods: &[&'a Pod], rm_at_least: usize) -> Vec<&'a Pod> {
    fn flush<'a>(
        group: &mut Vec<&'a Pod>,
        index: i32,
        rm: &mut Vec<&'a Pod>,
        left: &mut Vec<&'a Pod>,
    ) {
        if index == -1 {
            rm.append(group);
        } else if group.len() == 1 {
            left.append(group);
        } else if !group.is_empty() {
            sort_active_pods(group);
            let keep = group.pop().unwrap();
            rm.append(group);
            left.push(keep);
        }
    }

    let mut rm: Vec<&Pod> = Vec::new();
    let mut left: Vec<&Pod>;
    if job.spec.completion_mode.as_deref() == Some("Indexed") {
        let completions = job.spec.completions.unwrap_or(1);
        left = Vec::new();
        let mut sorted: Vec<&Pod> = pods.to_vec();
        sorted.sort_by_key(|p| completion_index_annotation(p)); // stable: byCompletionIndex
        let mut group: Vec<&Pod> = Vec::new();
        let mut group_index = -1;
        let mut cut_off = false;
        for (i, p) in sorted.iter().enumerate() {
            let ix = completion_index_annotation(p);
            if ix >= completions {
                flush(&mut group, group_index, &mut rm, &mut left);
                rm.extend_from_slice(&sorted[i..]);
                cut_off = true;
                break;
            }
            if ix != group_index {
                flush(&mut group, group_index, &mut rm, &mut left);
                group_index = ix;
            }
            group.push(p);
        }
        if !cut_off {
            flush(&mut group, group_index, &mut rm, &mut left);
        }
    } else {
        left = pods.to_vec();
    }
    if rm.len() < rm_at_least {
        sort_active_pods(&mut left);
        rm.extend(left.into_iter().take(rm_at_least - rm.len()));
    }
    rm
}

/// Pods a Job counts as active: `FilterActivePods` restricted to
/// Running|Pending (as the rest of this controller does).
fn active_job_pods<'a>(pods: impl IntoIterator<Item = &'a Pod>) -> Vec<&'a Pod> {
    pods.into_iter()
        .filter(|p| {
            is_pod_active(p)
                && matches!(
                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Running) | Some(Phase::Pending)
                )
        })
        .collect()
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
    // A failed pod's `job-index-failure-count` annotation holds the failures
    // BEFORE it, so the index has failed `count + 1` times once this pod has.
    // Upstream's `isIndexFailed` (`indexed_job_utils.go:98`) reads exactly this
    // (`getIndexFailureCount >= BackoffLimitPerIndex`), which keeps the count
    // after earlier pods of the index are gone. The pod tally is kept as the
    // other bound for pods that predate the annotation.
    let mut failures_per_index: HashMap<i32, i32> = HashMap::new();
    let mut annotated_per_index: HashMap<i32, i32> = HashMap::new();
    for pod in pods {
        if matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Failed)
        ) {
            if let Some(index) = get_pod_index(pod) {
                *failures_per_index.entry(index).or_insert(0) += 1;
                let total = parse_count_annotation(pod, JOB_INDEX_FAILURE_COUNT_ANNOTATION) + 1;
                let e = annotated_per_index.entry(index).or_insert(0);
                *e = (*e).max(total);
            }
        }
    }
    failures_per_index
        .into_iter()
        .map(|(idx, count)| {
            (
                idx,
                count.max(annotated_per_index.get(&idx).copied().unwrap_or(0)),
            )
        })
        .filter(|(_, count)| *count > per_index_limit)
        .map(|(idx, _)| idx)
        .collect()
}

/// `batch.JobIndexFailureCountAnnotation`
/// (`staging/src/k8s.io/api/batch/v1/types.go`).
const JOB_INDEX_FAILURE_COUNT_ANNOTATION: &str = "batch.kubernetes.io/job-index-failure-count";
/// `batch.JobIndexIgnoredFailureCountAnnotation`.
const JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION: &str =
    "batch.kubernetes.io/job-index-ignored-failure-count";
/// `DefaultJobPodFailureBackOff` (`job_controller.go:70`).
const DEFAULT_JOB_POD_FAILURE_BACKOFF: Duration = Duration::from_secs(10);
/// `MaxJobPodFailureBackOff` (`job_controller.go:72`).
const MAX_JOB_POD_FAILURE_BACKOFF: Duration = Duration::from_secs(600);

/// `parseInt32` over a pod annotation (`indexed_job_utils.go:443`): missing,
/// unparsable or negative values read as 0.
fn parse_count_annotation(pod: &Pod, key: &str) -> i32 {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(0)
}

/// `getFinishedTime` (`backoff_utils.go:174`): latest container finish time,
/// else the Ready=False transition, else `deletionTimestamp - gracePeriod`,
/// else the creation timestamp.
fn pod_finished_time(pod: &Pod) -> chrono::DateTime<chrono::Utc> {
    pod_finish_time_from_containers(pod)
        .or_else(|| pod_finish_time_from_ready_false(pod))
        .or_else(|| {
            // `getFinishTimeFromDeletionTimestamp` (`backoff_utils.go:231`).
            pod.metadata.deletion_timestamp.map(|t| {
                t - chrono::Duration::seconds(
                    pod.metadata.deletion_grace_period_seconds.unwrap_or(0),
                )
            })
        })
        .or(pod.metadata.creation_timestamp)
        .unwrap_or_else(chrono::Utc::now)
}

/// `latestFinishTime` (`backoff_utils.go:207`): the latest `finishedAt` over
/// the statuses passing `check`. Any status that is not terminated, or has no
/// finish time, makes the whole lookup yield `None`.
fn latest_finish_time(
    prev: Option<chrono::DateTime<chrono::Utc>>,
    statuses: &[rusternetes_common::resources::pod::ContainerStatus],
    check: impl Fn(&rusternetes_common::resources::pod::ContainerStatus) -> bool,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use rusternetes_common::resources::pod::ContainerState;
    let mut finish = prev;
    for cs in statuses.iter().filter(|c| check(c)) {
        let Some(ContainerState::Terminated {
            finished_at: Some(t),
            ..
        }) = &cs.state
        else {
            return None;
        };
        let t = chrono::DateTime::parse_from_rfc3339(t)
            .ok()?
            .with_timezone(&chrono::Utc);
        if t.timestamp() == 0 {
            // `FinishedAt.Time.IsZero()`.
            return None;
        }
        if finish.is_none_or(|f| f < t) {
            finish = Some(t);
        }
    }
    finish
}

/// `getFinishTimeFromContainers` (`backoff_utils.go:188`), with the
/// `SidecarContainers` gate (GA in 1.35): restartable init containers always
/// finish after regular ones, so their statuses are folded in too.
fn pod_finish_time_from_containers(pod: &Pod) -> Option<chrono::DateTime<chrono::Utc>> {
    let status = pod.status.as_ref();
    let regular = status
        .and_then(|s| s.container_statuses.as_deref())
        .unwrap_or(&[]);
    let finish = latest_finish_time(None, regular, |_| true);
    let sidecars: HashSet<&str> = pod
        .spec
        .as_ref()
        .and_then(|s| s.init_containers.as_ref())
        .into_iter()
        .flatten()
        .filter(|c| c.restart_policy.as_deref() == Some("Always"))
        .map(|c| c.name.as_str())
        .collect();
    let init = status
        .and_then(|s| s.init_container_statuses.as_deref())
        .unwrap_or(&[]);
    latest_finish_time(finish, init, |c| sidecars.contains(c.name.as_str()))
}

/// `getFinishTimeFromPodReadyFalseCondition` (`backoff_utils.go:224`).
fn pod_finish_time_from_ready_false(pod: &Pod) -> Option<chrono::DateTime<chrono::Utc>> {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .into_iter()
        .flatten()
        .find(|c| c.condition_type == "Ready")
        .filter(|c| c.status == "False")
        .and_then(|c| c.last_transition_time)
}

/// `getRemainingTimePerIndex` (`backoff_utils.go:248`) with
/// `getRemainingTimeForFailuresCount` (`:258`): the failure backoff left for an
/// index, doubling from 10s per failure up to 10m, measured from the last
/// failed pod's finish time.
fn remaining_time_per_index(
    now: chrono::DateTime<chrono::Utc>,
    last_failed_pod: Option<&Pod>,
) -> Duration {
    let Some(pod) = last_failed_pod else {
        return Duration::ZERO;
    };
    let failures = parse_count_annotation(pod, JOB_INDEX_FAILURE_COUNT_ANNOTATION)
        + parse_count_annotation(pod, JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION)
        + 1;
    let mut backoff = DEFAULT_JOB_POD_FAILURE_BACKOFF;
    for _ in 1..failures {
        backoff *= 2;
        if backoff >= MAX_JOB_POD_FAILURE_BACKOFF {
            backoff = MAX_JOB_POD_FAILURE_BACKOFF;
            break;
        }
    }
    let elapsed = (now - pod_finished_time(pod))
        .to_std()
        .unwrap_or(Duration::ZERO);
    backoff.saturating_sub(elapsed)
}

/// `getNewIndexFailureCounts` (`indexed_job_utils.go:360`): the
/// failure/ignored-failure counts for the pod that replaces `replaced`. An
/// ignored failure (a podFailurePolicy `Ignore` match) bumps the ignored count
/// instead of the failure count.
fn new_index_failure_counts(replaced: Option<&Pod>, replaced_ignored: bool) -> (i32, i32) {
    let Some(pod) = replaced else {
        return (0, 0);
    };
    let count = parse_count_annotation(pod, JOB_INDEX_FAILURE_COUNT_ANNOTATION);
    let ignored = parse_count_annotation(pod, JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION);
    if replaced_ignored {
        (count, ignored + 1)
    } else {
        (count + 1, ignored)
    }
}

/// `getPodsWithDelayedDeletionPerIndex` (`indexed_job_utils.go:323`): per
/// completion index, the last failed pod whose finalizer must be kept until a
/// replacement for that index exists. Indexes that are active, succeeded or
/// failed need no carrier. Only pods still holding the tracking finalizer are
/// candidates (`getValidPodsWithFilter` skips already-accounted pods).
fn pods_with_delayed_deletion_per_index(
    job_pods: &[Pod],
    completions: i32,
    succeeded_indexes: &HashSet<i32>,
    failed_indexes: &HashSet<i32>,
    only_replace_failed_pods: bool,
) -> HashMap<i32, Pod> {
    let active_indexes: HashSet<i32> = job_pods
        .iter()
        .filter(|p| {
            !matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Succeeded)
            ) && !is_pod_failed(p, only_replace_failed_pods)
        })
        .filter_map(get_pod_index)
        .collect();
    let absolute = |p: &Pod| {
        parse_count_annotation(p, JOB_INDEX_FAILURE_COUNT_ANNOTATION)
            + parse_count_annotation(p, JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION)
    };
    let mut result: HashMap<i32, Pod> = HashMap::new();
    for pod in job_pods {
        if !has_job_tracking_finalizer(pod) || !is_pod_failed(pod, only_replace_failed_pods) {
            continue;
        }
        let Some(ix) = get_pod_index(pod) else {
            continue;
        };
        if ix >= completions
            || succeeded_indexes.contains(&ix)
            || failed_indexes.contains(&ix)
            || active_indexes.contains(&ix)
        {
            continue;
        }
        let replace = match result.get(&ix) {
            Some(last) => {
                absolute(last) <= absolute(pod) && pod_finished_time(pod) >= pod_finished_time(last)
            }
            None => true,
        };
        if replace {
            result.insert(ix, pod.clone());
        }
    }
    result
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
        reconcile_settled(&controller, &storage, job_key).await;

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
            reconcile_settled(&controller, &storage, job_key).await;
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
        reconcile_settled(&controller, &storage, job_key).await;

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
        reconcile_settled(&controller, &storage, job_key).await;

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
        reconcile_settled(&controller, &storage, job_key).await;

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
        reap_terminating(&storage).await;
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

    // --- orphan pods through the running controller (conformance "Job should
    // delete a job") --------------------------------------------------------
    //
    // `DeleteResourceAndWaitForGC` deletes the Job with background propagation
    // and waits for its pods to be *gone*. The garbage collector deletes them,
    // but each still carries `batch.kubernetes.io/job-tracking`; with the Job
    // gone, only the Job controller's orphan path removes it. Upstream reaches
    // that path from `deleteJob` -> `enqueueLabelSelector`
    // (`pkg/controller/job/job_controller.go:561-581`) and from the pod event
    // handlers (`addPod` :344-347, `updatePod` :417-420, `deletePod` :458-466),
    // all into `handleSingleOrphanPod` (:736-767). Ours existed but only
    // `reconcile_all` — a test entry point — called it, so in a cluster the
    // pods kept the finalizer forever: "there are 2 pods left".

    fn job_with_selector(name: &str) -> Job {
        let mut job = make_job(name, "default", 4, 2);
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(HashMap::from([("job-name".to_string(), name.to_string())])),
            match_expressions: None,
        });
        job
    }

    async fn wait_until_released(storage: &MemoryStorage, pods: &[&str]) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut held = Vec::new();
            for name in pods {
                let pod: Pod = storage
                    .get(&build_key("pods", Some("default"), name))
                    .await
                    .unwrap();
                if has_job_tracking_finalizer(&pod) {
                    held.push(name.to_string());
                }
            }
            if held.is_empty() || tokio::time::Instant::now() > deadline {
                return held;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn run_releases_the_pods_of_a_deleted_job() {
        let storage = Arc::new(MemoryStorage::new());
        let job_key = build_key("jobs", Some("default"), "foo");
        storage
            .create(&job_key, &job_with_selector("foo"))
            .await
            .unwrap();
        for name in ["foo-a", "foo-b"] {
            let pod = make_pod(name, "default", Phase::Running, "foo", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = Arc::new(JobController::new(storage.clone()));
        let handle = tokio::spawn(controller.run());
        // Let the controller settle on the live Job before it goes away.
        tokio::time::sleep(Duration::from_millis(300)).await;

        storage.delete(&job_key).await.unwrap();

        let held = wait_until_released(&storage, &["foo-a", "foo-b"]).await;
        handle.abort();
        assert!(
            held.is_empty(),
            "pods of the deleted Job still hold the job-tracking finalizer: {held:?}"
        );
    }

    #[tokio::test]
    async fn run_releases_pods_orphaned_before_it_started() {
        let storage = Arc::new(MemoryStorage::new());
        // The Job is already gone: only its pods remain.
        for name in ["bar-a", "bar-b"] {
            let pod = make_pod(name, "default", Phase::Running, "bar", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = Arc::new(JobController::new(storage.clone()));
        let handle = tokio::spawn(controller.run());

        let held = wait_until_released(&storage, &["bar-a", "bar-b"]).await;
        handle.abort();
        assert!(
            held.is_empty(),
            "orphaned pods still hold the job-tracking finalizer: {held:?}"
        );
    }

    /// A failed pod for `index` that carries the index-failure-count
    /// annotation and finished `finished_secs_ago` seconds ago.
    fn failed_pod_with_count(
        name: &str,
        index: i32,
        failure_count: i32,
        finished_secs_ago: i64,
    ) -> Pod {
        let mut pod = make_indexed_pod(
            name,
            "default",
            Phase::Failed,
            "idx-job",
            "job-uid-1",
            index,
        );
        pod.metadata.annotations.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-index-failure-count".to_string(),
            failure_count.to_string(),
        );
        let finished = chrono::Utc::now() - chrono::Duration::seconds(finished_secs_ago);
        pod.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
            name: "test".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 1,
                signal: None,
                reason: Some("Error".to_string()),
                message: None,
                started_at: None,
                finished_at: Some(finished.to_rfc3339()),
                container_id: None,
            }),
            last_state: None,
            image: Some("busybox".to_string()),
            image_id: None,
            container_id: None,
            started: None,
            allocated_resources: None,
            resources: None,
            volume_mounts: None,
            user: None,
            allocated_resources_status: None,
            stop_signal: None,
        }]);
        pod
    }

    async fn per_index_job(storage: &Arc<MemoryStorage>, limit: i32) -> Job {
        let mut job = make_job("idx-job", "default", 2, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(limit);
        job.spec.backoff_limit = Some(100);
        storage
            .create("/registry/jobs/default/idx-job", &job)
            .await
            .unwrap();
        job
    }

    /// Port of `getPodsWithDelayedDeletionPerIndex`
    /// (`indexed_job_utils.go:323`) and `addIndexFailureCountAnnotation`
    /// (`indexed_job_utils.go:350`): the replacement for an index whose failed
    /// pod is the only record of its failure count must inherit that count,
    /// and the failed pod must keep its finalizer until the replacement exists.
    #[tokio::test]
    async fn test_delayed_deletion_per_index_carries_failure_count() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        // Index 0 has failed twice already; this pod is the last of them and
        // finished long ago, so no backoff is pending. Index 1 is running.
        let failed = failed_pod_with_count("idx-0-failed", 0, 1, 3600);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let old = pods
            .iter()
            .find(|p| p.metadata.name == "idx-0-failed")
            .unwrap();
        assert!(
            has_job_tracking_finalizer(old),
            "the last failed pod of an index keeps its finalizer until the replacement exists"
        );
        let replacement = pods
            .iter()
            .find(|p| p.metadata.name != "idx-0-failed" && get_pod_index(p) == Some(0))
            .expect("a replacement pod for index 0 must be created");
        assert_eq!(
            replacement
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("batch.kubernetes.io/job-index-failure-count"))
                .map(String::as_str),
            Some("2"),
            "the replacement inherits the failure count plus one"
        );
    }

    /// Once the replacement is active the failed pod is no longer delayed and
    /// its finalizer comes off (`canRemoveFinalizer`, `job_controller.go:1359`).
    #[tokio::test]
    async fn test_delayed_deletion_released_once_replacement_active() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 1, 3600);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        for (n, i) in [("idx-0-new", 0), ("idx-1-run", 1)] {
            let p = make_indexed_pod(n, "default", Phase::Running, "idx-job", "job-uid-1", i);
            storage
                .create(&format!("/registry/pods/default/{n}"), &p)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        // Released: either gone or without the tracking finalizer.
        let held = pods
            .iter()
            .any(|p| p.metadata.name == "idx-0-failed" && has_job_tracking_finalizer(p));
        assert!(
            !held,
            "failed pod must be released once its index is active"
        );
    }

    /// Port of `getPodCreationInfoForIndependentIndexes` /
    /// `getRemainingTimePerIndex` (`job_controller.go:1850`,
    /// `backoff_utils.go:248`): an index that failed just now is not retried
    /// until DefaultJobPodFailureBackOff * 2^(count) has elapsed.
    #[tokio::test]
    async fn test_per_index_failure_backoff_defers_replacement() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 0, 0);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            !pods
                .iter()
                .any(|p| p.metadata.name != "idx-0-failed" && get_pod_index(p) == Some(0)),
            "no replacement may be created inside the per-index backoff window"
        );
    }

    #[test]
    fn test_remaining_time_per_index_doubles_up_to_max() {
        let now = chrono::Utc::now();
        let p = failed_pod_with_count("p", 0, 0, 0);
        // absolute failure count 0 + 1 -> 10s window.
        let r = remaining_time_per_index(now, Some(&p));
        assert!(
            r > Duration::from_secs(8) && r <= Duration::from_secs(10),
            "{r:?}"
        );
        let p = failed_pod_with_count("p", 0, 2, 0);
        // count 3 -> 10s * 2^2 = 40s.
        let r = remaining_time_per_index(now, Some(&p));
        assert!(
            r > Duration::from_secs(38) && r <= Duration::from_secs(40),
            "{r:?}"
        );
        let p = failed_pod_with_count("p", 0, 30, 0);
        let r = remaining_time_per_index(now, Some(&p));
        assert!(
            r <= Duration::from_secs(600) && r > Duration::from_secs(598),
            "{r:?}"
        );
        assert_eq!(remaining_time_per_index(now, None), Duration::ZERO);
    }

    fn terminated_status(
        name: &str,
        finished: Option<chrono::DateTime<chrono::Utc>>,
    ) -> ContainerStatus {
        let mut cs = failed_pod_with_count("tmp", 0, 0, 0)
            .status
            .unwrap()
            .container_statuses
            .unwrap()
            .remove(0);
        cs.name = name.to_string();
        cs.state = Some(ContainerState::Terminated {
            exit_code: 0,
            signal: None,
            reason: None,
            message: None,
            started_at: None,
            finished_at: finished.map(|t| t.to_rfc3339()),
            container_id: None,
        });
        cs
    }

    /// `getFinishTimeFromContainers` (`backoff_utils.go:188`): a restartable
    /// init container (sidecar) finishes after the regular containers, so its
    /// finish time wins.
    #[test]
    fn test_pod_finished_time_includes_sidecar_init_containers() {
        let now = chrono::Utc::now();
        let main_done = now - chrono::Duration::seconds(100);
        let sidecar_done = now - chrono::Duration::seconds(30);
        let mut pod = failed_pod_with_count("p", 0, 0, 0);
        pod.spec.as_mut().unwrap().init_containers = Some(vec![
            Container {
                name: "sidecar".to_string(),
                restart_policy: Some("Always".to_string()),
                ..Default::default()
            },
            Container {
                name: "plain-init".to_string(),
                ..Default::default()
            },
        ]);
        let status = pod.status.as_mut().unwrap();
        status.container_statuses = Some(vec![terminated_status("test", Some(main_done))]);
        // The plain init container finished latest of all but is not a sidecar.
        status.init_container_statuses = Some(vec![
            terminated_status("sidecar", Some(sidecar_done)),
            terminated_status("plain-init", Some(now)),
        ]);
        let got = pod_finished_time(&pod);
        assert_eq!(got.timestamp(), sidecar_done.timestamp());
    }

    /// `latestFinishTime` (`backoff_utils.go:213`): one sidecar that has not
    /// terminated (or has a zero finish time) makes the container lookup yield
    /// nothing, so the Ready=False transition is used instead.
    #[test]
    fn test_pod_finished_time_unfinished_sidecar_falls_back_to_ready_false() {
        let now = chrono::Utc::now();
        let ready_false = now - chrono::Duration::seconds(7);
        let mut pod = failed_pod_with_count("p", 0, 0, 50);
        pod.spec.as_mut().unwrap().init_containers = Some(vec![Container {
            name: "sidecar".to_string(),
            restart_policy: Some("Always".to_string()),
            ..Default::default()
        }]);
        let status = pod.status.as_mut().unwrap();
        let mut running = terminated_status("sidecar", None);
        running.state = Some(ContainerState::Running { started_at: None });
        status.init_container_statuses = Some(vec![running]);
        status.conditions = Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(ready_false),
            observed_generation: None,
        }]);
        assert_eq!(pod_finished_time(&pod).timestamp(), ready_false.timestamp());
    }

    /// `getFinishTimeFromDeletionTimestamp` (`backoff_utils.go:231`):
    /// deletionTimestamp minus the grace period.
    #[test]
    fn test_pod_finished_time_deletion_timestamp_minus_grace() {
        let now = chrono::Utc::now();
        let mut pod = failed_pod_with_count("p", 0, 0, 0);
        pod.status.as_mut().unwrap().container_statuses = None;
        pod.metadata.deletion_timestamp = Some(now);
        pod.metadata.deletion_grace_period_seconds = Some(30);
        let got = pod_finished_time(&pod);
        assert_eq!(
            got.timestamp(),
            (now - chrono::Duration::seconds(30)).timestamp()
        );
    }

    /// `getPodCreationInfoForIndependentIndexes` + `enqueueSyncJobWithDelay`
    /// (`job_controller.go:1746-1749`): when every pending index is inside its
    /// backoff window the sync asks to be re-run after the remaining time
    /// instead of waiting for the resync.
    #[tokio::test]
    async fn test_per_index_backoff_requests_requeue_after_remaining_time() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 0, 0);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();
        let d = controller
            .take_requeue_delay("default", "idx-job")
            .expect("a pending per-index backoff must request a delayed requeue");
        assert!(
            d > Duration::from_secs(8) && d <= Duration::from_secs(10),
            "{d:?}"
        );
        assert!(controller
            .take_requeue_delay("default", "idx-job")
            .is_none());
    }

    /// A non-indexed failed pod of `job-uid-1` that finished
    /// `finished_secs_ago` seconds ago (and still holds the tracking finalizer).
    fn failed_plain_pod(name: &str, finished_secs_ago: i64) -> Pod {
        let mut pod = failed_pod_with_count(name, 0, 0, finished_secs_ago);
        pod.metadata.annotations = None;
        pod.metadata.labels = Some(HashMap::from([(
            "job-name".to_string(),
            "plain-job".to_string(),
        )]));
        pod.metadata.owner_references.as_mut().unwrap()[0].name = "plain-job".to_string();
        pod
    }

    async fn plain_job_with_failed_pod(
        storage: &Arc<MemoryStorage>,
        finished_secs_ago: i64,
    ) -> Job {
        let job = make_job("plain-job", "default", 1, 1);
        storage
            .create("/registry/jobs/default/plain-job", &job)
            .await
            .unwrap();
        let failed = failed_plain_pod("failed-0", finished_secs_ago);
        storage
            .create("/registry/pods/default/failed-0", &failed)
            .await
            .unwrap();
        job
    }

    async fn plain_job_pod_names(storage: &Arc<MemoryStorage>) -> Vec<String> {
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        pods.into_iter().map(|p| p.metadata.name).collect()
    }

    /// `manageJob` (`job_controller.go:1731-1738`): a replacement pod is not
    /// created while `newBackoffRecord.getRemainingTime(DefaultJobPodFailureBackOff,
    /// MaxJobPodFailureBackOff)` is positive; the sync is re-enqueued with that
    /// delay instead.
    #[tokio::test]
    async fn test_replacement_pod_delayed_by_failure_backoff() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 1).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            plain_job_pod_names(&storage).await,
            vec!["failed-0".to_string()],
            "no replacement may be created inside the failure backoff window"
        );
        let d = controller
            .take_requeue_delay("default", "plain-job")
            .expect("the delayed creation must request a delayed requeue");
        assert!(
            d > Duration::from_secs(8) && d <= Duration::from_secs(10),
            "{d:?}"
        );
    }

    /// Once the window has elapsed the replacement is created at once.
    #[tokio::test]
    async fn test_replacement_pod_created_after_failure_backoff_elapsed() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 60).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(plain_job_pod_names(&storage).await.len(), 2);
    }

    /// `podBackoffStore` (`backoff_utils.go:44-91`) outlives a sync: after the
    /// failed pod's finalizer is released it is no longer a "new" failed pod,
    /// yet the next sync must still be inside the backoff window.
    #[tokio::test]
    async fn test_failure_backoff_survives_across_syncs() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 1).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        let _ = controller.take_requeue_delay("default", "plain-job");
        let mut job: Job = storage
            .get("/registry/jobs/default/plain-job")
            .await
            .unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            plain_job_pod_names(&storage).await,
            vec!["failed-0".to_string()],
            "the second sync must still honour the recorded failure"
        );
        assert!(controller
            .take_requeue_delay("default", "plain-job")
            .is_some());
    }

    /// `enqueueSyncJobWithDelay` never delays less than `SyncJobBatchPeriod`
    /// (`job_controller.go:620`).
    #[test]
    fn test_requeue_delay_floor_is_sync_job_batch_period() {
        assert_eq!(
            requeue_delay_for(Duration::from_millis(10)),
            SYNC_JOB_BATCH_PERIOD
        );
        assert_eq!(
            requeue_delay_for(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
    /// `MemoryStorage` that can refuse Pod updates by name and records every
    /// Job write, so a test can stage a PARTIAL `uncountedTerminatedPods` drain
    /// (upstream's "pod patch errors with partial success" case) and inspect
    /// the sequence of statuses that went out.
    struct PartialDrainStorage {
        inner: MemoryStorage,
        fail_pod_updates: std::sync::Mutex<HashSet<String>>,
        job_writes: std::sync::Mutex<Vec<JobStatus>>,
    }

    impl PartialDrainStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_pod_updates: Default::default(),
                job_writes: Default::default(),
            }
        }
        fn record(&self, key: &str, value: &serde_json::Value) {
            if key.starts_with("/registry/jobs/") {
                if let Some(st) = value
                    .get("status")
                    .and_then(|s| serde_json::from_value::<JobStatus>(s.clone()).ok())
                {
                    self.job_writes.lock().unwrap().push(st);
                }
            }
        }
        fn refuse(&self, key: &str) -> bool {
            self.fail_pod_updates
                .lock()
                .unwrap()
                .iter()
                .any(|n| key.ends_with(&format!("/pods/default/{n}")))
        }
    }

    type CResult<T> = std::result::Result<T, rusternetes_common::Error>;

    #[async_trait::async_trait]
    impl Storage for PartialDrainStorage {
        async fn create<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> CResult<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if self.refuse(key) {
                return Err(rusternetes_common::Error::Internal(
                    "injected pod update failure".into(),
                ));
            }
            self.record(key, &serde_json::to_value(value).unwrap());
            self.inner.update(key, value).await
        }
        async fn update_status<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.record(key, &serde_json::to_value(value).unwrap());
            self.inner.update_status(key, value).await
        }
        async fn delete(&self, key: &str) -> CResult<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> CResult<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }
        async fn watch(&self, prefix: &str) -> CResult<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> CResult<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> CResult<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> CResult<bool> {
            self.inner.is_revision_compacted(revision).await
        }
        async fn update_raw(&self, key: &str, value: &serde_json::Value) -> CResult<()> {
            self.inner.update_raw(key, value).await
        }
    }

    fn uncounted_len_of(st: &JobStatus) -> usize {
        st.uncounted_terminated_pods.as_ref().map_or(0, |u| {
            u.succeeded.as_ref().map_or(0, |v| v.len()) + u.failed.as_ref().map_or(0, |v| v.len())
        })
    }

    fn has_terminal(st: &JobStatus) -> bool {
        st.conditions.as_ref().is_some_and(|c| {
            c.iter().any(|c| {
                (c.condition_type == "Complete" || c.condition_type == "Failed")
                    && c.status == "True"
            })
        })
    }

    /// #2378 (follow-up to #2377): only part of `uncountedTerminatedPods`
    /// drains in one sync (one pod's finalizer removal fails). Upstream's
    /// `enactJobFinished` (`job_controller.go:1509-1519`) returns false while
    /// any UID remains, and the next sync re-derives `finishedCondition` from
    /// scratch and finishes the Job. So: no write may carry a terminal
    /// condition while UIDs remain, and the terminal condition lands once.
    #[tokio::test]
    async fn partial_uncounted_drain_defers_terminal_condition_to_next_sync() {
        let storage = Arc::new(PartialDrainStorage::new());
        let job = make_job("part", "default", 2, 2);
        let job_key = build_key("jobs", Some("default"), "part");
        storage.create(&job_key, &job).await.unwrap();
        for name in ["part-1", "part-2"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "part", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }
        storage
            .fail_pod_updates
            .lock()
            .unwrap()
            .insert("part-2".into());

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let after_first: Job = storage.get(&job_key).await.unwrap();
        let st = after_first.status.clone().unwrap();
        assert_eq!(
            uncounted_len_of(&st),
            1,
            "the pod whose finalizer could not be removed stays uncounted: {st:?}"
        );
        assert!(
            !has_terminal(&st),
            "no Complete/Failed while UIDs remain uncounted: {st:?}"
        );
        assert!(
            st.conditions.as_ref().is_some_and(|c| c
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True")),
            "the interim SuccessCriteriaMet condition is written: {st:?}"
        );

        storage.fail_pod_updates.lock().unwrap().clear();
        controller.reconcile_all().await.unwrap();

        let done: Job = storage.get(&job_key).await.unwrap();
        let st = done.status.unwrap();
        assert_eq!(uncounted_len_of(&st), 0, "second sync drains the rest");
        assert_eq!(st.succeeded, Some(2));
        assert!(has_terminal(&st), "second sync re-derives Complete: {st:?}");
        assert!(st.completion_time.is_some());

        let writes = storage.job_writes.lock().unwrap().clone();
        for w in &writes {
            assert!(
                !(has_terminal(w) && uncounted_len_of(w) > 0),
                "a write marked the Job finished while UIDs remained: {w:?}"
            );
        }
        let first_terminal = writes.iter().position(has_terminal);
        let terminal_idx: Vec<usize> = writes
            .iter()
            .enumerate()
            .filter(|(_, w)| has_terminal(w))
            .map(|(i, _)| i)
            .collect();
        assert!(first_terminal.is_some());
        // Once terminal, every later write stays terminal (written once, never
        // flapped off and on).
        assert_eq!(
            terminal_idx,
            (first_terminal.unwrap()..writes.len()).collect::<Vec<_>>(),
            "terminal condition must not flap: {writes:?}"
        );
    }

    /// Same, for the failing path (`FailureTarget` interim, `Failed` final).
    #[tokio::test]
    async fn partial_uncounted_drain_defers_failed_condition_to_next_sync() {
        let storage = Arc::new(PartialDrainStorage::new());
        let mut job = make_job("pfail", "default", 2, 2);
        job.spec.backoff_limit = Some(0);
        let job_key = build_key("jobs", Some("default"), "pfail");
        storage.create(&job_key, &job).await.unwrap();
        for name in ["pfail-1", "pfail-2"] {
            let pod = make_pod(name, "default", Phase::Failed, "pfail", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }
        storage
            .fail_pod_updates
            .lock()
            .unwrap()
            .insert("pfail-2".into());

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();
        let st = storage.get::<Job>(&job_key).await.unwrap().status.unwrap();
        assert_eq!(uncounted_len_of(&st), 1, "{st:?}");
        assert!(!has_terminal(&st), "{st:?}");

        storage.fail_pod_updates.lock().unwrap().clear();
        controller.reconcile_all().await.unwrap();
        let st = storage.get::<Job>(&job_key).await.unwrap().status.unwrap();
        assert_eq!(uncounted_len_of(&st), 0, "{st:?}");
        assert!(has_terminal(&st), "{st:?}");
        for w in storage.job_writes.lock().unwrap().iter() {
            assert!(!(has_terminal(w) && uncounted_len_of(w) > 0), "{w:?}");
        }
    }

    // ---- #2465: deleteActivePods is a graceful DeletePod ----

    /// Replay the kubelet: physically remove pods whose deletionTimestamp the
    /// controller stamped with its graceful delete.
    async fn reap_terminating(storage: &Arc<MemoryStorage>) {
        let pods: Vec<Pod> = storage.list("/registry/pods/").await.unwrap();
        for pod in pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
        {
            let key = format!(
                "/registry/pods/{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name
            );
            let _ = storage.delete(&key).await;
        }
    }

    /// Reconcile, let the kubelet reap the pods the sync deleted gracefully,
    /// reconcile again: the terminal condition is delayed while pods are
    /// terminating (`enactJobFinished`, job_controller.go:1520-1524).
    async fn reconcile_settled(
        controller: &JobController<MemoryStorage>,
        storage: &Arc<MemoryStorage>,
        job_key: &str,
    ) {
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        reap_terminating(storage).await;
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
    }

    /// While a pod is still terminating the Job publishes only the interim
    /// SuccessCriteriaMet condition and a non-zero `terminating`; Complete
    /// follows once the pod is gone (job_controller.go:1520-1524).
    #[tokio::test]
    async fn test_success_policy_complete_is_delayed_while_pods_terminate() {
        let mut job = make_job("dly", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let storage = Arc::new(MemoryStorage::new());
        let job_key = "/registry/jobs/default/dly";
        storage.create(job_key, &job).await.unwrap();
        for (n, phase, idx) in [("p0", Phase::Succeeded, 0), ("p2", Phase::Running, 2)] {
            let pod = make_indexed_pod(n, "default", phase, "dly", "job-uid-1", idx);
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let st = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        let conds = st.conditions.unwrap_or_default();
        assert!(conds
            .iter()
            .any(|c| c.condition_type == "SuccessCriteriaMet"));
        assert!(
            !conds.iter().any(|c| c.condition_type == "Complete"),
            "Complete must wait for the terminating pod"
        );
        assert_eq!(st.terminating, Some(1));

        reap_terminating(&storage).await;
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let st = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        assert!(st
            .conditions
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "Complete"));
        assert_eq!(st.terminating, Some(0));
    }

    /// Create `job` plus one Running pod `p1`; reconcile once; return the
    /// storage and the pod's key.
    async fn graceful_fixture(job: Job) -> (Arc<MemoryStorage>, String) {
        let storage = Arc::new(MemoryStorage::new());
        let job_key = format!("/registry/jobs/default/{}", job.metadata.name);
        storage.create(&job_key, &job).await.unwrap();
        let pod = make_pod(
            "p1",
            "default",
            Phase::Running,
            &job.metadata.name,
            "job-uid-1",
        );
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(&job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        (storage, "/registry/pods/default/p1".to_string())
    }

    /// Suspending deletes active pods through `deleteActivePods` ->
    /// `podControl.DeletePod` (job_controller.go:1122-1140;
    /// controller_utils.go:618): graceful, the pod keeps its finalizer-backed
    /// object with a deletionTimestamp.
    #[tokio::test]
    async fn test_suspend_deletes_active_pods_gracefully() {
        let mut job = make_job("susp", "default", 1, 1);
        job.spec.suspend = Some(true);
        let (storage, pod_key) = graceful_fixture(job).await;
        let pod: Pod = storage
            .get(&pod_key)
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_active_deadline_deletes_active_pods_gracefully() {
        let mut job = make_job("dl", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(1);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        });
        let (storage, pod_key) = graceful_fixture(job).await;
        let pod: Pod = storage
            .get(&pod_key)
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_success_policy_deletes_active_pods_gracefully() {
        let mut job = make_job("sp", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/jobs/default/sp", &job)
            .await
            .unwrap();
        let done = make_indexed_pod("p0", "default", Phase::Succeeded, "sp", "job-uid-1", 0);
        storage
            .create("/registry/pods/default/p0", &done)
            .await
            .unwrap();
        let running = make_indexed_pod("p2", "default", Phase::Running, "sp", "job-uid-1", 2);
        storage
            .create("/registry/pods/default/p2", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/sp").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let pod: Pod = storage
            .get("/registry/pods/default/p2")
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    /// `FilterActivePods` (controller_utils.go:1001, `IsPodActive` :1085)
    /// excludes pods with a deletionTimestamp, so a terminating Running pod
    /// is replaced rather than counted against `parallelism`.
    #[tokio::test]
    async fn test_terminating_pod_is_not_active() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("term", "default", 1, 1);
        storage
            .create("/registry/jobs/default/term", &job)
            .await
            .unwrap();
        let mut pod = make_pod("p1", "default", Phase::Running, "term", "job-uid-1");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/term").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let live = pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .count();
        assert_eq!(live, 1, "a replacement for the terminating pod is created");
    }

    // ---- #2553: manageJob — excess deletion, cap, expectations ----

    async fn live_pods(storage: &Arc<MemoryStorage>) -> Vec<Pod> {
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        pods.into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .collect()
    }

    /// `manageJob` (job_controller.go:1692-1716): `rmAtLeast = active -
    /// wantActive`; the excess pods are deleted and, deletion taking
    /// precedence, nothing is created in the same sync.
    #[tokio::test]
    async fn test_manage_job_deletes_excess_active_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("ex", "default", 5, 1);
        storage
            .create("/registry/jobs/default/ex", &job)
            .await
            .unwrap();
        for n in ["p1", "p2", "p3"] {
            let pod = make_pod(n, "default", Phase::Running, "ex", "job-uid-1");
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/ex").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 3, "no replacement created in a delete sync");
        assert_eq!(live_pods(&storage).await.len(), 1, "2 excess deleted");
        let st: Job = storage.get("/registry/jobs/default/ex").await.unwrap();
        assert_eq!(st.status.unwrap().active, Some(1));
    }

    /// A pod removed as excess must not later be counted as a failure or
    /// success: `deleteJobPods` strips the tracking finalizer first
    /// (`removeTrackingFinalizerPatch`, job_controller.go:1181-1186).
    #[tokio::test]
    async fn test_delete_job_pods_strips_tracking_finalizer() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("fin", "default", 5, 1);
        storage
            .create("/registry/jobs/default/fin", &job)
            .await
            .unwrap();
        for n in ["p1", "p2"] {
            let pod = make_pod(n, "default", Phase::Running, "fin", "job-uid-1");
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/fin").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let deleted: Vec<&Pod> = all
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .collect();
        assert_eq!(deleted.len(), 1);
        assert!(!has_job_tracking_finalizer(deleted[0]));
    }

    /// `MaxPodCreateDeletePerSync = 500` (job_controller.go:79) caps one
    /// sync's creations (`:1735-1737`); the rest wait for the next sync.
    #[tokio::test]
    async fn test_manage_job_caps_creations_per_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("cap", "default", 600, 600);
        storage
            .create("/registry/jobs/default/cap", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/cap").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 500);
    }

    /// `diff := wantActive - terminating - active` for
    /// podReplacementPolicy=Failed (job_controller.go:1722-1728): a pod that
    /// is still terminating is not replaced yet.
    #[tokio::test]
    async fn test_pod_replacement_policy_failed_waits_for_terminating() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("prf", "default", 1, 1);
        job.spec.pod_replacement_policy = Some("Failed".to_string());
        storage
            .create("/registry/jobs/default/prf", &job)
            .await
            .unwrap();
        let mut pod = make_pod("p1", "default", Phase::Running, "prf", "job-uid-1");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/prf").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 1, "no replacement while the pod is terminating");
    }

    /// The brake: while creations are unobserved the sync only updates
    /// status (job_controller.go:1016 `if satisfiedExpectations`).
    #[tokio::test]
    async fn test_unmet_creation_expectations_gate_manage_job() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("gate", "default", 3, 3);
        storage
            .create("/registry/jobs/default/gate", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        controller.expectations.expect_creations("default/gate", 2);
        let mut job: Job = storage.get("/registry/jobs/default/gate").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert!(live_pods(&storage).await.is_empty());

        controller.expectations.creation_observed("default/gate");
        controller.expectations.creation_observed("default/gate");
        let mut job: Job = storage.get("/registry/jobs/default/gate").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(live_pods(&storage).await.len(), 3);
    }

    /// `ExpectCreations` is recorded before the creates, and an `Added` pod
    /// event observes each creation exactly once (`addPod`, :339).
    #[tokio::test]
    async fn test_created_pods_are_expected_then_observed_once() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("obs", "default", 2, 2);
        storage
            .create("/registry/jobs/default/obs", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/obs").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/obs"),
            Some((2, 0))
        );
        let pods = live_pods(&storage).await;
        for pod in &pods {
            let ev = rusternetes_storage::WatchEvent::Added(
                format!("pods/default/{}", pod.metadata.name),
                serde_json::to_string(pod).unwrap(),
            );
            controller.observe_pod_event(&ev);
            controller.observe_pod_event(&ev); // duplicate: not double counted
        }
        assert_eq!(
            controller.expectations.get_expectations("default/obs"),
            Some((0, 0))
        );
        assert!(controller.expectations.satisfied("default/obs"));
    }

    /// Suspend deletes through `deleteJobPods` with `ExpectDeletions`
    /// (job_controller.go:1666-1668), and the terminating-pod event
    /// observes it.
    #[tokio::test]
    async fn test_suspend_expects_deletions_and_observes_them() {
        let mut job = make_job("sus2", "default", 1, 1);
        job.spec.suspend = Some(true);
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/jobs/default/sus2", &job)
            .await
            .unwrap();
        let pod = make_pod("p1", "default", Phase::Running, "sus2", "job-uid-1");
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/sus2").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert!(!controller.expectations.satisfied("default/sus2"));
        let gone: Pod = storage.get("/registry/pods/default/p1").await.unwrap();
        let ev = rusternetes_storage::WatchEvent::Modified(
            "pods/default/p1".to_string(),
            serde_json::to_string(&gone).unwrap(),
        );
        controller.observe_pod_event(&ev);
        assert!(controller.expectations.satisfied("default/sus2"));
    }

    fn pod_for_ordering(name: &str, phase: Phase, node: Option<&str>, ready: bool) -> Pod {
        let mut p = make_pod(name, "default", phase, "j", "job-uid-1");
        p.spec.as_mut().unwrap().node_name = node.map(str::to_string);
        if ready {
            p.status.as_mut().unwrap().conditions =
                Some(vec![rusternetes_common::resources::pod::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]);
        }
        p
    }

    /// `controller.ActivePods` (controller_utils.go:741): unassigned <
    /// assigned, Pending < Running, not-ready < ready — the first are
    /// removed first.
    #[test]
    fn test_active_pods_for_removal_prefers_least_progressed() {
        let job = make_job("j", "default", 5, 1);
        let ready_running = pod_for_ordering("a", Phase::Running, Some("n1"), true);
        let pending_assigned = pod_for_ordering("b", Phase::Pending, Some("n1"), false);
        let unassigned = pod_for_ordering("c", Phase::Pending, None, false);
        let pods = vec![&ready_running, &pending_assigned, &unassigned];
        let rm = active_pods_for_removal(&job, &pods, 2);
        let names: Vec<&str> = rm.iter().map(|p| p.metadata.name.as_str()).collect();
        assert_eq!(names, vec!["c", "b"]);
    }

    /// `appendDuplicatedIndexPodsForRemoval` (indexed_job_utils.go:295): for
    /// an Indexed Job, duplicates of an index and out-of-range indexes are
    /// removed even beyond `rmAtLeast`.
    #[test]
    fn test_active_pods_for_removal_indexed_duplicates() {
        let mut job = make_job("j", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        let a0 = make_indexed_pod("a0", "default", Phase::Running, "j", "job-uid-1", 0);
        let b0 = make_indexed_pod("b0", "default", Phase::Pending, "j", "job-uid-1", 0);
        let a1 = make_indexed_pod("a1", "default", Phase::Running, "j", "job-uid-1", 1);
        let a7 = make_indexed_pod("a7", "default", Phase::Running, "j", "job-uid-1", 7);
        let pods = vec![&a0, &b0, &a1, &a7];
        let rm = active_pods_for_removal(&job, &pods, 0);
        let mut names: Vec<&str> = rm.iter().map(|p| p.metadata.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["a7", "b0"]);
    }
}
