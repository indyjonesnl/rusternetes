use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::workloads::{
    Job, JobCondition, JobStatus, SuccessPolicy, UncountedTerminatedPods,
};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{OwnerReference, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};

use super::controller_utils::{is_pod_ready, sort_active_pods};
use super::expectations::ControllerExpectations;
use super::job_tracking::{
    clean_uncounted_pods_without_finalizers, has_job_tracking_finalizer, push_uncounted_failed,
    push_uncounted_succeeded, remove_tracking_finalizer, uncounted_has_failed,
    uncounted_has_succeeded, FinalizerExpectations, JOB_TRACKING_FINALIZER,
};
use super::pod_informer::{ControllerRef, PodEventTarget, ResolvedController, SharedPodInformer};
use super::replicationcontroller::{
    generate_pod_name, slow_start_batches_capped, MAX_NAME_GENERATION_CREATE_ATTEMPTS,
};
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
    pub storage: Arc<S>,
    /// Per-Job ("ns/name") expectations of in-flight pod creates and deletes.
    /// Upstream `jm.expectations` (`job_controller.go:100`,
    /// `controller.NewControllerExpectations()`); shared implementation in
    /// [`super::expectations`]. Read BEFORE listing pods in `reconcile`
    /// (`:905`) and gating `manageJob` (`:1016`).
    pub expectations: Arc<ControllerExpectations>,
    /// Pod UIDs whose tracking-finalizer removal has been issued but not yet
    /// observed. Upstream's `uidTrackingExpectations`
    /// (`pkg/controller/job/tracking_utils.go:48`) — the brake that stops a
    /// stale pod list from claiming the same termination twice.
    pub finalizer_expectations: FinalizerExpectations,
    /// Delayed re-syncs requested by `reconcile`, keyed `namespace/name`.
    /// Stands in for upstream's `jm.queue.AddAfter` call inside `manageJob`
    /// (`enqueueSyncJobWithDelay`, `job_controller.go:620`); the worker drains
    /// it after each sync and calls `WorkQueue::add_after`.
    pub requeue_delays: std::sync::Mutex<HashMap<String, Duration>>,
    /// Upstream's `podBackoffStore` (`job_controller.go:128`): per-Job failure
    /// history that delays replacement pod creation (`manageJob`, `:1731`).
    pub backoff_store: BackoffStore,
    /// The shared pod informer whose `addPod` / `updatePod` / `deletePod`
    /// handlers (`job_controller.go:213-224`, `:319-492`) this controller
    /// registers on: [`PodEventTarget`] below.
    pub pod_informer: Arc<SharedPodInformer<S>>,
}

/// `SyncJobBatchPeriod` (`job_controller.go:64`).
pub const SYNC_JOB_BATCH_PERIOD: Duration = Duration::from_secs(1);

/// `enqueueSyncJobWithDelay` (`job_controller.go:620`): "custom delay, but not
/// smaller than the batching delay".
pub fn requeue_delay_for(delay: Duration) -> Duration {
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
    /// `jobCtx.newBackoffRecord` (`job_controller.go:934`).
    new_backoff_record: BackoffRecord,
    /// Upstream's `needsFlush`: the record is only stored once the status that
    /// accounts for the new pods is written (`:1396`).
    needs_flush: bool,
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
/// The mirror of [`FinishedCondition`]: the interim `SuccessCriteriaMet`
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

/// `enactJobFinished` (`job_controller.go:1520-1524`): "Delaying marking the
/// Job as finished, because there are still terminating pod(s)". `conditions`
/// is an interim + terminal pair (`FailureTarget`+`Failed` or
/// `SuccessCriteriaMet`+`Complete`); while pods terminate only the interim
/// condition is published and the terminal one follows once they are gone.
fn enact_job_finished(mut conditions: Vec<JobCondition>, terminating: i32) -> Vec<JobCondition> {
    if terminating > 0 {
        conditions.pop();
    }
    conditions
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

/// `jobCtx.finishedCondition` (`job_controller.go:945-998`): the ONE verdict
/// every scenario in `syncJob` feeds - pre-existing SuccessCriteriaMet /
/// FailureTarget, backoffLimit, `pastActiveDeadline`, per-index failures,
/// successPolicy and completions. `Success` is `Complete` (staged through
/// the interim `SuccessCriteriaMet`), `Failure` is `Failed` (staged through
/// `FailureTarget`, `newFailedConditionForFailureTarget`, :1562).
#[derive(Clone, Debug)]
struct FinishedCondition {
    success: bool,
    reason: String,
    message: String,
}

impl FinishedCondition {
    fn success(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: true,
            reason: reason.into(),
            message: message.into(),
        }
    }

    fn failure(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: false,
            reason: reason.into(),
            message: message.into(),
        }
    }

    /// The single terminal handler: `enactJobFinished` (:1520-1545).
    /// Returns the conditions and `completionTime` to publish. `ready` is
    /// false while the finish gate (:1003-1007) is closed or uncounted pods
    /// remain; then the previous conditions stand. While pods terminate only
    /// the interim condition goes out (:1520-1524). `completionTime` is set
    /// only for `Complete` (:1531-1533; validation.go:505-513).
    fn enact(
        &self,
        previous: Option<Vec<JobCondition>>,
        may_finish: bool,
        terminating: i32,
    ) -> (
        Option<Vec<JobCondition>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) {
        if !may_finish {
            return (previous, None);
        }
        let now = chrono::Utc::now();
        let (interim, terminal) = if self.success {
            ("SuccessCriteriaMet", "Complete")
        } else {
            ("FailureTarget", "Failed")
        };
        let cond = |t: &str| JobCondition {
            condition_type: t.to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(self.reason.clone()),
            message: Some(self.message.clone()),
        };
        let conditions = enact_job_finished(vec![cond(interim), cond(terminal)], terminating);
        let completion_time = (self.success && terminating == 0).then_some(now);
        (Some(conditions), completion_time)
    }
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

/// The Job half of the pod event handlers (`job_controller.go:280-317`).
impl<S: Storage + 'static> PodEventTarget for JobController<S> {
    fn queue_prefix(&self) -> &'static str {
        "jobs"
    }

    /// `resolveControllerRef` (`job_controller.go:300-317`): "Don't even try to
    /// look up by Name if it's the wrong Kind", and the Job found by name must
    /// have the ControllerRef's UID.
    async fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> Option<ResolvedController> {
        if controller_ref.kind != "Job" {
            return None;
        }
        let job: Job = self
            .storage
            .get(&build_key("jobs", Some(namespace), &controller_ref.name))
            .await
            .ok()?;
        (job.metadata.uid == controller_ref.uid).then(|| ResolvedController {
            name: job.metadata.name.clone(),
            min_ready_seconds: 0,
        })
    }

    /// `getPodJobs` (`job_controller.go:280-297`) over `GetPodJobs` (client-go
    /// `listers/batch/v1/job_expansion.go`): a pod without labels matches no
    /// Job; "This object has an invalid selector, it does not match the pod";
    /// a nil selector is `labels.Nothing()`, an empty one `labels.Everything()`.
    async fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> Vec<String> {
        if pod_labels.is_empty() {
            return Vec::new();
        }
        let Ok(jobs) = self
            .storage
            .list::<Job>(&build_prefix("jobs", Some(namespace)))
            .await
        else {
            return Vec::new();
        };
        let matching: Vec<String> = jobs
            .into_iter()
            .filter(|job| {
                rusternetes_common::types::label_selector_as_selector(job.spec.selector.as_ref())
                    .is_ok_and(|selector| selector.matches(Some(pod_labels)))
            })
            .map(|job| job.metadata.name)
            .collect();
        if matching.len() > 1 {
            // "ControllerRef will ensure we don't do anything crazy, but more
            // than one item in this list nevertheless constitutes user error."
            warn!(
                "user error! more than one job is selecting pods with labels: {:?}",
                pod_labels
            );
        }
        matching
    }

    fn creation_observed(&self, key: &str) {
        self.expectations.creation_observed(key);
    }

    /// Deviation (pre-existing, see [`ControllerExpectations::creation_observed_of`]):
    /// creations are tracked by pod key so the settle-from-listing step in
    /// `reconcile` and the `addPod` handler never both count one pod.
    fn creation_observed_of(&self, key: &str, pod_key: &str) {
        self.expectations.creation_observed_of(key, pod_key);
    }

    fn deletion_observed(&self, key: &str, pod_key: &str) {
        self.expectations.deletion_observed_of(key, pod_key);
    }

    /// `enqueueSyncJobBatched` (`job_controller.go:593-600`).
    fn enqueue_delay(&self) -> Duration {
        SYNC_JOB_BATCH_PERIOD
    }

    async fn controller_finished(&self, namespace: &str, name: &str) -> bool {
        self.storage
            .get::<Job>(&build_key("jobs", Some(namespace), name))
            .await
            .is_ok_and(|job| job_is_finished(&job))
    }

    fn finalizer_removal_observed(&self, key: &str, uid: &str) {
        self.finalizer_expectations.removal_observed(key, uid);
    }

    /// `enqueueOrphanPod` -> `syncOrphanPod` -> `handleSingleOrphanPod`
    /// (`job_controller.go:688-766`).
    async fn orphan_pod(&self, namespace: &str, pod_name: &str) {
        if let Ok(pod) = self
            .storage
            .get::<Pod>(&build_key("pods", Some(namespace), pod_name))
            .await
        {
            self.release_if_orphan(&pod).await;
        }
    }
}

impl<S: Storage + 'static> JobController<S> {
    #[allow(dead_code)] // the binary shares one informer via `with_pod_informer`
    pub fn new(storage: Arc<S>) -> Self {
        let pod_informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_secs(5),
        ));
        Self::with_pod_informer(storage, pod_informer)
    }

    /// A controller whose pod event handlers hang off an informer shared with
    /// other controllers (upstream hands every controller the one `PodInformer`
    /// of the shared informer factory, `NewController(ctx, podInformer, ...)`,
    /// job_controller.go:172).
    pub fn with_pod_informer(storage: Arc<S>, pod_informer: Arc<SharedPodInformer<S>>) -> Self {
        Self {
            storage,
            pod_informer,
            expectations: Arc::new(ControllerExpectations::new()),
            finalizer_expectations: FinalizerExpectations::new(),
            requeue_delays: std::sync::Mutex::new(HashMap::new()),
            backoff_store: BackoffStore::default(),
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

    /// `deleteActivePods` (`job_controller.go:1002`) plus the gate that
    /// follows it (`:1003-1007`): the Job may only be declared finished when
    /// every active pod was deleted AND the expectations were satisfied
    /// ("Can't declare the Job as finished yet, as there might be remaining
    /// pod finalizers or pods that are not in the informer's cache yet").
    /// Returns `(may_finish, deleted_ready, deleted, first_error)`.
    async fn delete_active_pods_for_finish(
        &self,
        exp_key: &str,
        namespace: &str,
        job_pods: &[Pod],
        satisfied_expectations: bool,
    ) -> (bool, i32, i32, Option<anyhow::Error>) {
        let active_pods = active_job_pods(job_pods.iter());
        let (deleted_ready, deleted, err) =
            self.delete_job_pods(exp_key, namespace, &active_pods).await;
        let may_finish = deleted == active_pods.len() as i32 && satisfied_expectations;
        (may_finish, deleted_ready, deleted, err)
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
    pub fn take_requeue_delay(&self, namespace: &str, name: &str) -> Option<Duration> {
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
        completions: i32,
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
        let cleaned = clean_uncounted_pods_without_finalizers(
            &mut base_succeeded,
            &mut base_failed,
            &mut uncounted,
            &uids_with_finalizer,
        );

        // Phase 1 of this pass: claim newly-finished pods.
        let mut to_release: Vec<Pod> = Vec::new();
        // `getNewFinishedPods` (`job_controller.go:1633`) feeds
        // `newBackoffRecord`: finalizer held, not already parked, valid index.
        let mut new_succeeded: Vec<&Pod> = Vec::new();
        let mut new_failed: Vec<&Pod> = Vec::new();
        let mut needs_flush = cleaned;
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
            let valid_index =
                !is_indexed || get_pod_index(pod).is_some_and(|i| i >= 0 && i < completions);
            match phase {
                Some(Phase::Succeeded) => {
                    if valid_index && !uncounted_has_succeeded(&uncounted, uid) {
                        new_succeeded.push(pod);
                        needs_flush = true;
                    }
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
                    if valid_index && !uncounted_has_failed(&uncounted, uid) {
                        new_failed.push(pod);
                    }
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
                        needs_flush = true;
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

        let new_backoff_record =
            self.backoff_store
                .new_backoff_record(job_key, &new_succeeded, &new_failed);
        TrackedPods {
            new_backoff_record,
            needs_flush,
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
    #[allow(clippy::too_many_arguments)]
    async fn flush_status_and_release(
        &self,
        key: &str,
        job_tracking_key: &str,
        namespace: &str,
        job: &mut Job,
        pods_to_release: &[Pod],
        job_pods: &[Pod],
        backoff_update: Option<&BackoffRecord>,
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
        // `flushUncountedAndRemoveFinalizers` (`job_controller.go:1396`): the
        // record is stored once the status that parks the new pods is written.
        if let Some(record) = backoff_update {
            self.backoff_store.update(job_tracking_key, record);
        }

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

        // Upstream registers its pod handlers on the shared pod informer
        // (`podInformer.Informer().AddEventHandler`, job_controller.go:213-224)
        // and the informer factory runs the one watch; the first controller to
        // get here starts it. The informer's initial list is what routes every
        // orphan still holding the finalizer to `enqueueOrphanPod` (`addPod`).
        self.pod_informer
            .add_event_handler(Arc::clone(&self), queue.clone())
            .await;
        self.pod_informer.start();

        loop {
            // Initial full reconciliation. The orphan sweep stays (#2050): the
            // informer's `addPod` does not clean a pod whose controller Job is
            // already gone (`resolveControllerRef` nil returns, :331-333);
            // upstream leaves that pod to the garbage collector's delete.
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
                    // `job_controller.go:842`: and the backoff record.
                    self.backoff_store.remove(&format!("{}/{}", ns, name));
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

    pub async fn reconcile(&self, job: &mut Job) -> Result<()> {
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
            // `deleteJob` (`job_controller.go:564`).
            self.backoff_store.remove(&job_tracking_key);
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
                    // `syncJob` (`job_controller.go:866`): a finished Job's
                    // backoff record is removed.
                    self.backoff_store.remove(&format!("{namespace}/{name}"));
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
                                        rusternetes_common::resources::ContainerState { terminated: Some(rusternetes_common::resources::ContainerStateTerminated {
                                            exit_code,
                                            ..
                                        }), .. },
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
            // Rusternetes deviation (no upstream equivalent): once
            // SuccessCriteriaMet is published, a pod this controller already
            // deleted (`deleteActivePods`, job_controller.go:1001) that then
            // exits 0 before the kubelet kills it must not add an index.
            // Upstream's runtime reports such a pod Failed (SIGKILL) so it
            // never reaches `calculateSucceededIndexes`; here, since #2784
            // keeps the Job unfinished while pods terminate, it would raise
            // `status.succeeded` above what met the policy (sig-apps
            // "succeededIndexes rule ... some indexes remain pending" waits
            // for exactly 1).
            let success_criteria_met = job
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| {
                    cs.iter()
                        .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True")
                });
            set.extend(collect_indexes_in_phase(
                job_pods
                    .iter()
                    .filter(|p| !(success_criteria_met && p.metadata.deletion_timestamp.is_some())),
                Phase::Succeeded,
            ));
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
            job.spec.completions.unwrap_or(1),
        );
        let new_backoff_record = tracked.new_backoff_record.clone();
        let backoff_update = tracked
            .needs_flush
            .then(|| tracked.new_backoff_record.clone());

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

        // Evaluation order, `syncJob` (job_controller.go:941-998): "1. Evaluate the
        // pre-existing SuccessCriteriaMet and FailureTarget to respect the
        // previous reconcile results". A persisted interim condition IS the
        // finishedCondition; nothing below re-derives (or overrides) it.
        //   jobCtx.finishedCondition = hasSuccessCriteriaMetCondition(&job)
        //   if nil: FailureTarget=True -> newFailedConditionForFailureTarget
        // (`hasSuccessCriteriaMetCondition`, success_policy.go:52;
        // `newFailedConditionForFailureTarget`, job_controller.go:1562).
        let persisted_interim = |t: &str| -> Option<(String, String)> {
            job.status
                .as_ref()?
                .conditions
                .as_ref()?
                .iter()
                .find(|c| c.condition_type == t && c.status == "True")
                .map(|c| {
                    (
                        c.reason.clone().unwrap_or_default(),
                        c.message.clone().unwrap_or_default(),
                    )
                })
        };
        let persisted_success = persisted_interim("SuccessCriteriaMet");
        // FailureTarget is only consulted when there is no SuccessCriteriaMet.
        let persisted_failure = if persisted_success.is_none() {
            persisted_interim("FailureTarget")
        } else {
            None
        };
        let job_suspended = job.spec.suspend.unwrap_or(false);

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

        // maxFailedIndexes is evaluated only inside `hasBackoffLimitPerIndex`
        // (job_controller.go:978-985):
        //   if job.Spec.MaxFailedIndexes != nil && jobCtx.failedIndexes.total() > int(*job.Spec.MaxFailedIndexes)
        // Without backoffLimitPerIndex failed pods count only against
        // backoffLimit.
        let max_failed_indexes_exceeded = is_indexed
            && backoff_limit_per_index.is_some()
            && job
                .spec
                .max_failed_indexes
                .is_some_and(|max| all_failed_index_set.len() as i64 > i64::from(max));

        // For backoffLimitPerIndex, job fails when all indexes are either succeeded or failed
        // Failure scenarios come before deadline, successPolicy and completions
        // (job_controller.go:945-998); a persisted SuccessCriteriaMet means no
        // failure is evaluated at all (:951 "only when the Job doesn't have the
        // SuccessCriteriaMet condition"), and a persisted FailureTarget is
        // already the failure (:953-955).
        // Order matters (job_controller.go:955-995): pod failure policy and
        // backoffLimit, then `pastActiveDeadline`, and only after those the
        // per-index scenarios (maxFailedIndexes, FailedIndexes). So the
        // deadline outranks the per-index ones, and is evaluated only when
        // the earlier scenarios have not already failed the Job.
        let early_failed = persisted_success.is_none()
            && (persisted_failure.is_some()
                || pod_failure_policy_triggered
                || (!(backoff_limit_per_index.is_some() && is_indexed) && failed > backoff_limit));
        let index_failed = persisted_success.is_none()
            && (max_failed_indexes_exceeded
                || (backoff_limit_per_index.is_some() && is_indexed && {
                    let completed_count = succeeded_index_count;
                    let failed_count = all_failed_index_set.len() as i32;
                    (completed_count + failed_count) >= completions
                }));

        // `pastActiveDeadline` (job_controller.go:1595-1602) is one more failure
        // scenario feeding the single finishedCondition (:968-970): evaluated
        // only when nothing else finished the Job (:963), false while
        // suspended or before startTime, and `duration >= allowedDuration`.
        let deadline_exceeded = !early_failed
            && persisted_success.is_none()
            && !job_suspended
            && job
                .spec
                .active_deadline_seconds
                .zip(job.status.as_ref().and_then(|s| s.start_time))
                .is_some_and(|(deadline, start)| {
                    chrono::Utc::now().signed_duration_since(start)
                        >= chrono::Duration::seconds(deadline)
                });
        let is_failed = early_failed || deadline_exceeded || index_failed;

        // `job_controller.go:970-973`: when no scenario finished the Job and
        // the deadline has not passed, `jm.queue.AddAfter(key,
        // activeDeadlineSeconds - Since(startTime))` so the deadline fires on
        // time. Goes through `request_requeue` (clamped to SyncJobBatchPeriod).
        if !early_failed && !deadline_exceeded && persisted_success.is_none() && !job_suspended {
            if let Some((deadline, start)) = job
                .spec
                .active_deadline_seconds
                .zip(job.status.as_ref().and_then(|s| s.start_time))
            {
                let remaining = chrono::Duration::seconds(deadline)
                    - chrono::Utc::now().signed_duration_since(start);
                if let Ok(remaining) = remaining.to_std() {
                    self.request_requeue(namespace, name, remaining);
                }
            }
        }

        // Check if Job is complete
        // For indexed jobs, check number of distinct succeeded indexes
        let is_complete = !is_failed
            && if is_indexed {
                succeeded_index_count >= completions
            } else {
                succeeded >= completions
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
        // `matchSuccessPolicy` (success_policy.go:28), reached only inside
        // `if isIndexedJob(&job)` (job_controller.go:991-996).
        let success_policy_message = if is_indexed {
            match_success_policy(
                job.spec.success_policy.as_ref(),
                completions,
                &succeeded_index_set,
            )
        } else {
            None
        };
        let success_policy_matched = success_policy_message.is_some();

        // Success scenarios are evaluated after failures (:988-994); a
        // persisted SuccessCriteriaMet is honoured regardless of the spec.
        let success_policy_met =
            persisted_success.is_some() || (!is_failed && success_policy_matched);

        // The single `finishedCondition` (job_controller.go:945-998, :1035-1037),
        // in upstream's evaluation order: pre-existing SuccessCriteriaMet,
        // pre-existing FailureTarget, failure scenarios, successPolicy, and
        // lastly completions.
        let finished: Option<FinishedCondition> = if let Some((r, m)) = persisted_success.clone() {
            Some(FinishedCondition::success(r, m))
        } else if let Some((r, m)) = persisted_failure.clone() {
            Some(FinishedCondition::failure(r, m))
        } else if is_failed {
            Some(if deadline_exceeded {
                // job_controller.go:969
                FinishedCondition::failure(
                    "DeadlineExceeded",
                    "Job was active longer than specified deadline",
                )
            } else if pod_failure_policy_triggered {
                FinishedCondition::failure("PodFailurePolicy", pod_failure_message.clone())
            } else if max_failed_indexes_exceeded {
                // job_controller.go:980
                FinishedCondition::failure(
                    "MaxFailedIndexesExceeded",
                    "Job has exceeded the specified maximal number of failed indexes",
                )
            } else if index_failed && !early_failed {
                // job_controller.go:982
                FinishedCondition::failure("FailedIndexes", "Job has failed indexes")
            } else {
                // job_controller.go:964
                FinishedCondition::failure(
                    "BackoffLimitExceeded",
                    "Job has reached the specified backoff limit",
                )
            })
        } else if success_policy_matched {
            // job_controller.go:993
            Some(FinishedCondition::success(
                "SuccessPolicy",
                success_policy_message.clone().unwrap_or_default(),
            ))
        } else if is_complete {
            // newSuccessCondition, job_controller.go:1101-1105
            Some(FinishedCondition::success(
                "CompletionsReached",
                "Reached expected number of succeeded pods",
            ))
        } else {
            None
        };

        if success_policy_met {
            let finished = finished
                .clone()
                .expect("success_policy_met implies a finishedCondition");
            info!("Job {}/{} met success policy criteria", namespace, name);

            // `deleteActivePods` (job_controller.go:1002, :1122-1140;
            // controller_utils.go:618): a GRACEFUL delete, the pod lingers with
            // a deletionTimestamp until the kubelet and the tracking-finalizer
            // removal reap it. The finish gate (:1003-1007) clears the
            // condition unless everything was deleted and expectations hold.
            let (may_finish, deleted_ready, deleted, derr) = self
                .delete_active_pods_for_finish(
                    &exp_key,
                    namespace,
                    &job_pods,
                    satisfied_expectations,
                )
                .await;

            // `enactJobFinished` (job_controller.go:1520-1524): the terminal
            // condition is delayed while terminating pods remain, so that
            // status.terminating == 0 holds whenever Complete is set (the
            // conformance spec asserts it). Meanwhile only the interim
            // SuccessCriteriaMet condition is published.
            let terminating = count_unfinished_pods(&job_pods);
            let (conditions, completion_time) = finished.enact(
                job.status.as_ref().and_then(|s| s.conditions.clone()),
                may_finish,
                terminating,
            );

            job.status = Some(JobStatus {
                active: Some(active - deleted),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions,
                start_time,
                completion_time,
                ready: Some(ready - deleted_ready),
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
                backoff_update.as_ref(),
            )
            .await?;
            return match derr {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }

        // Suspend is handled in `manageJob`, which runs only when no
        // finishedCondition was found (job_controller.go:1016, :945-998), so a
        // failure, success policy or completion outranks it. Delete all active
        // pods (`manageJob`,
        // job_controller.go:1663-1673): `activePodsForRemoval(.., active)`,
        // `ExpectDeletions`, `deleteJobPods`; only when expectations are
        // satisfied (`:1016`).
        if job_suspended && !is_failed && !is_complete {
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
                backoff_update.as_ref(),
            )
            .await?;
            return match manage_err {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }

        // Terminal-path outcome shared with the deferred write below: errors
        // from `deleteActivePods` surface only after the status is written
        // (`manageJobErr`, job_controller.go:1090).
        if is_complete {
            info!("Job {}/{} completed successfully", namespace, name);
            // `complete = succeeded >= completions && active == 0`
            // (job_controller.go:1035), where `active` is what `manageJob`
            // returns after deleting the excess (:1663-1673), and manageJob
            // only runs when expectations are satisfied (:1016).
            let (deleted_ready, deleted, derr) = if satisfied_expectations {
                let active_pods = active_job_pods(job_pods.iter());
                self.delete_job_pods(&exp_key, namespace, &active_pods)
                    .await
            } else {
                (0, 0, None)
            };
            manage_err = derr;
            let active_left = active - deleted;
            let terminating = count_unfinished_pods(&job_pods);
            // `enactJobFinished` (:1520-1524): Complete waits for terminating
            // pods; only the interim SuccessCriteriaMet is published meanwhile.
            let (conditions, completion_time) = finished
                .as_ref()
                .expect("is_complete implies a finishedCondition")
                .enact(
                    job.status.as_ref().and_then(|s| s.conditions.clone()),
                    active_left == 0,
                    terminating,
                );
            job.status = Some(JobStatus {
                active: Some(active_left),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions,
                start_time,
                completion_time,
                ready: Some(ready - deleted_ready),
                terminating: Some(terminating),
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

            // `deleteActivePods` + the finish gate (job_controller.go:1002-1007).
            let (may_finish, deleted_ready, deleted, derr) = self
                .delete_active_pods_for_finish(
                    &exp_key,
                    namespace,
                    &job_pods,
                    satisfied_expectations,
                )
                .await;
            manage_err = derr;
            // `enactJobFinished` (:1520-1524): Failed waits for terminating
            // pods; only FailureTarget is published meanwhile.
            let terminating = count_unfinished_pods(&job_pods);
            let (conditions, completion_time) = finished
                .as_ref()
                .expect("is_failed implies a finishedCondition")
                .enact(
                    job.status.as_ref().and_then(|s| s.conditions.clone()),
                    may_finish,
                    terminating,
                );
            job.status = Some(JobStatus {
                active: Some(active - deleted),
                succeeded: status_succeeded,
                failed: status_failed,
                conditions,
                start_time,
                // completionTime is valid ONLY on a Complete job
                // (validation.go:505-513); `enact` yields None here.
                completion_time,
                ready: Some(ready - deleted_ready),
                terminating: Some(terminating),
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
                    // `manageJob` (`job_controller.go:1729-1737`): without
                    // backoffLimitPerIndex the global failure backoff gates
                    // creation; `enqueueSyncJobWithDelay` re-runs the sync.
                    if pods_needed > 0 && backoff_limit_per_index.is_none() {
                        let remaining = new_backoff_record.remaining_time(
                            chrono::Utc::now(),
                            DEFAULT_JOB_POD_FAILURE_BACKOFF,
                            MAX_JOB_POD_FAILURE_BACKOFF,
                        );
                        if !remaining.is_zero() {
                            self.request_requeue(namespace, name, remaining);
                            pods_needed = 0;
                        }
                    }
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
                                // Typed `apierrors.IsAlreadyExists` (errors.go:542-544);
                                // upstream pods use generateName so it never
                                // branches here — a Rusternetes-only benign case.
                                if super::is_already_exists_anyhow(&e) {
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
            backoff_update.as_ref(),
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
    pub async fn create_pod(
        &self,
        job: &Job,
        namespace: &str,
        index: i32,
        is_indexed: bool,
        index_failure_counts: Option<(i32, i32)>,
    ) -> Result<String> {
        let job_name = &job.metadata.name;
        let pod_name = generate_pod_name(job_name);

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

        // Retry a name collision under a freshly generated name, as upstream's
        // api-server does for `generateName` objects
        // (registry/generic/registry/store.go:464-475,
        // `maxNameGenerationCreateAttempts = 8`). Any other error is returned
        // on the first attempt.
        let mut pod = pod;
        let mut attempt_name = pod_name;
        for attempt in 0..MAX_NAME_GENERATION_CREATE_ATTEMPTS {
            pod.metadata.name = attempt_name.clone();
            let key = build_key("pods", Some(namespace), &attempt_name);
            match self.storage.create(&key, &pod).await {
                Ok(_) => return Ok(attempt_name),
                Err(rusternetes_common::Error::AlreadyExists(_))
                    if attempt + 1 < MAX_NAME_GENERATION_CREATE_ATTEMPTS =>
                {
                    attempt_name = generate_pod_name(job_name);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(rusternetes_common::Error::AlreadyExists(format!(
            "could not generate a free pod name for {}/{} after {} attempts",
            namespace, job_name, MAX_NAME_GENERATION_CREATE_ATTEMPTS
        ))
        .into())
    }
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
pub fn active_pods_for_removal<'a>(
    job: &Job,
    pods: &[&'a Pod],
    rm_at_least: usize,
) -> Vec<&'a Pod> {
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
pub fn get_pod_index(pod: &Pod) -> Option<i32> {
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
pub fn pod_finished_time(pod: &Pod) -> chrono::DateTime<chrono::Utc> {
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
    use rusternetes_common::resources::pod::{ContainerState, ContainerStateTerminated};
    let mut finish = prev;
    for cs in statuses.iter().filter(|c| check(c)) {
        let Some(ContainerState {
            terminated:
                Some(ContainerStateTerminated {
                    finished_at: Some(t),
                    ..
                }),
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
pub fn remaining_time_per_index(
    now: chrono::DateTime<chrono::Utc>,
    last_failed_pod: Option<&Pod>,
) -> Duration {
    let Some(pod) = last_failed_pod else {
        return Duration::ZERO;
    };
    let failures = parse_count_annotation(pod, JOB_INDEX_FAILURE_COUNT_ANNOTATION)
        + parse_count_annotation(pod, JOB_INDEX_IGNORED_FAILURE_COUNT_ANNOTATION)
        + 1;
    remaining_time_for_failures_count(
        now,
        DEFAULT_JOB_POD_FAILURE_BACKOFF,
        MAX_JOB_POD_FAILURE_BACKOFF,
        failures,
        Some(pod_finished_time(pod)),
    )
}

/// `getRemainingTimeForFailuresCount` (`backoff_utils.go:258`) with
/// `DefaultJobPodFailureBackOff` / `MaxJobPodFailureBackOff`: 10s doubling per
/// failure, capped at 10m, minus the time since the last failure.
fn remaining_time_for_failures_count(
    now: chrono::DateTime<chrono::Utc>,
    default_backoff: Duration,
    max_backoff: Duration,
    failures: i32,
    last_failure_time: Option<chrono::DateTime<chrono::Utc>>,
) -> Duration {
    let Some(last_failure_time) = last_failure_time else {
        return Duration::ZERO;
    };
    if failures == 0 {
        return Duration::ZERO;
    }
    let mut backoff = default_backoff;
    for _ in 1..failures {
        backoff *= 2;
        if backoff >= max_backoff {
            backoff = max_backoff;
            break;
        }
    }
    let elapsed = (now - last_failure_time).to_std().unwrap_or(Duration::ZERO);
    backoff.saturating_sub(elapsed)
}

/// `backoffRecord` (`backoff_utils.go:35`): the failures since the last
/// success and when the latest one finished.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BackoffRecord {
    pub failures_after_last_success: i32,
    pub last_failure_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl BackoffRecord {
    /// `backoffRecord.getRemainingTime` (`backoff_utils.go:243`).
    pub fn remaining_time(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        default_backoff: Duration,
        max_backoff: Duration,
    ) -> Duration {
        remaining_time_for_failures_count(
            now,
            default_backoff,
            max_backoff,
            self.failures_after_last_success,
            self.last_failure_time,
        )
    }
}

/// `backoffStore` (`backoff_utils.go:40`), keyed `namespace/name`: the
/// failure history that outlives the pods whose finalizers were released.
#[derive(Default)]
pub struct BackoffStore {
    pub records: std::sync::Mutex<HashMap<String, BackoffRecord>>,
}

impl BackoffStore {
    /// `updateBackoffRecord` (`backoff_utils.go:44`).
    pub fn update(&self, key: &str, record: &BackoffRecord) {
        self.records
            .lock()
            .unwrap()
            .insert(key.to_string(), record.clone());
    }

    /// `removeBackoffRecord` (`backoff_utils.go:64`).
    fn remove(&self, key: &str) {
        self.records.lock().unwrap().remove(key);
    }

    /// `newBackoffRecord` (`backoff_utils.go:92`): the stored record advanced
    /// by the pods that finished since it was stored.
    pub fn new_backoff_record(
        &self,
        key: &str,
        new_succeeded: &[&Pod],
        new_failed: &[&Pod],
    ) -> BackoffRecord {
        let mut backoff = self
            .records
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default();
        let sorted = |pods: &[&Pod]| {
            let mut times: Vec<chrono::DateTime<chrono::Utc>> =
                pods.iter().map(|p| pod_finished_time(p)).collect();
            times.sort();
            times
        };
        let succeeded = sorted(new_succeeded);
        let failed = sorted(new_failed);
        let Some(last_success) = succeeded.last() else {
            if let Some(last_failure) = failed.last() {
                backoff.failures_after_last_success += failed.len() as i32;
                backoff.last_failure_time = Some(*last_failure);
            }
            return backoff;
        };
        backoff.failures_after_last_success = 0;
        backoff.last_failure_time = None;
        for failed_time in failed.iter().rev() {
            if *failed_time <= *last_success {
                break;
            }
            if backoff.last_failure_time.is_none() {
                backoff.last_failure_time = Some(*failed_time);
            }
            backoff.failures_after_last_success += 1;
        }
        backoff
    }
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
/// Port of `parseIndexesFromString` (indexed_job_utils.go:204): intervals of
/// `indexes_str` clipped to `completions`; a corrupted interval is skipped and
/// parsing stops at the first interval starting at or after `completions`.
fn parse_indexes_from_string(indexes_str: &str, completions: i32) -> Vec<(i32, i32)> {
    let mut result: Vec<(i32, i32)> = Vec::new();
    if indexes_str.is_empty() {
        return result;
    }
    for interval_str in indexes_str.split(',') {
        let mut limits = interval_str.split('-');
        let Ok(first) = limits.next().unwrap_or("").parse::<i32>() else {
            continue;
        };
        if first >= completions {
            break;
        }
        let last = match limits.next() {
            Some(l) => match l.parse::<i32>() {
                Ok(l) => l.min(completions - 1),
                Err(_) => continue,
            },
            None => first,
        };
        match result.last_mut() {
            Some(prev) if prev.1 == first - 1 => prev.1 = last,
            _ => result.push((first, last)),
        }
    }
    result
}

/// Port of `matchSuccessPolicy` (success_policy.go:28) and
/// `matchSucceededIndexesRule` (:67). Returns the SuccessCriteriaMet message.
fn match_success_policy(
    policy: Option<&SuccessPolicy>,
    completions: i32,
    succeeded: &HashSet<i32>,
) -> Option<String> {
    let policy = policy?;
    if succeeded.is_empty() {
        return None;
    }
    for (index, rule) in policy.rules.iter().enumerate() {
        let matched = if let Some(ref idx) = rule.succeeded_indexes {
            let required = parse_indexes_from_string(idx, completions);
            // Failed to parse succeededIndexes of the rule: `continue`.
            if required.is_empty() {
                continue;
            }
            let total: i64 = required.iter().map(|(f, l)| i64::from(l - f + 1)).sum();
            let contains = required
                .iter()
                .map(|&(f, l)| succeeded.iter().filter(|&&i| i >= f && i <= l).count() as i64)
                .sum::<i64>();
            contains == total
                || rule
                    .succeeded_count
                    .is_some_and(|c| contains >= i64::from(c))
        } else {
            rule.succeeded_count
                .is_some_and(|c| succeeded.len() as i64 >= i64::from(c))
        };
        if matched {
            return Some(format!("Matched rules at index {index}"));
        }
    }
    None
}

pub fn parse_index_ranges(s: &str) -> HashSet<i32> {
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
pub fn format_index_ranges(indexes: &[i32]) -> String {
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
