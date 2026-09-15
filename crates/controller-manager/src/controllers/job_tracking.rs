//! Job pod tracking via the `batch.kubernetes.io/job-tracking` finalizer.
//!
//! Port of upstream `pkg/controller/job/tracking_utils.go` plus the
//! finalizer/uncounted helpers that live alongside
//! `trackJobStatusAndRemoveFinalizers` in `pkg/controller/job/job_controller.go`.
//!
//! # Why the finalizer exists
//!
//! From the constant's own doc comment
//! (`staging/src/k8s.io/api/batch/v1/types.go:35-44`):
//!
//! > JobTrackingFinalizer is a finalizer for Job's pods. It prevents them from
//! > being deleted before being accounted in the Job status.
//!
//! Without it a pod can be created, terminate and be deleted entirely between
//! two reconciles — with `terminationGracePeriodSeconds: 1`, which the
//! DisruptionTarget conformance spec uses, that window is about a second — and
//! its outcome is then lost.
//!
//! # The two-phase accounting protocol
//!
//! Counters are advanced in two steps so that a pod is counted exactly once,
//! even across a controller restart:
//!
//! 1. A terminal pod that still carries the finalizer has its UID appended to
//!    `status.uncountedTerminatedPods.{succeeded,failed}`, and that status is
//!    written *first*.
//! 2. The finalizer is then removed, which lets the pod be deleted.
//! 3. On a later pass, a UID present in the uncounted list whose pod no longer
//!    carries the finalizer is folded into the real `status.succeeded` /
//!    `status.failed` counter and dropped from the list
//!    ([`clean_uncounted_pods_without_finalizers`]).
//!
//! Because the counters only ever gain the pods observed in step 3, they are
//! monotonically non-decreasing by construction — which is what upstream's
//! api-server requires (`RejectDecreasingFailedCounter`, see #1958).
//!
//! # The brake
//!
//! [`FinalizerExpectations`] is the port of upstream's `uidTrackingExpectations`
//! (`tracking_utils.go:48-125`). After issuing a finalizer removal the UID is
//! recorded as "expected removed"; until that removal is observed, the pod is
//! treated as already processed. Without it, a pod list that has not yet caught
//! up still shows the finalizer, the pod is taken for a fresh termination, and
//! its UID is appended to the uncounted list a second time — double-counting a
//! failure, tripping `backoffLimit`, and failing a Job that should have passed.

use rusternetes_common::resources::workloads::UncountedTerminatedPods;
use rusternetes_common::resources::Pod;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// `batch.kubernetes.io/job-tracking`, upstream `batch.JobTrackingFinalizer`
/// (`staging/src/k8s.io/api/batch/v1/types.go:44`).
pub const JOB_TRACKING_FINALIZER: &str = "batch.kubernetes.io/job-tracking";

/// Upstream `hasJobTrackingFinalizer` (`tracking_utils.go:126-133`).
pub fn has_job_tracking_finalizer(pod: &Pod) -> bool {
    pod.metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|x| x == JOB_TRACKING_FINALIZER))
}

/// Drop the tracking finalizer from a pod's metadata in place.
///
/// Returns whether anything was removed, so a caller can skip a no-op write.
/// Upstream does this as a JSON patch (`removeTrackingFinalizerPatch`); we
/// mutate and update, because the controller already round-trips the object.
pub fn remove_tracking_finalizer(pod: &mut Pod) -> bool {
    let Some(finalizers) = pod.metadata.finalizers.as_mut() else {
        return false;
    };
    let before = finalizers.len();
    finalizers.retain(|f| f != JOB_TRACKING_FINALIZER);
    let removed = finalizers.len() != before;
    if finalizers.is_empty() {
        pod.metadata.finalizers = None;
    }
    removed
}

/// Is this UID already recorded in the uncounted list?
fn uncounted_has(list: &Option<Vec<String>>, uid: &str) -> bool {
    list.as_ref().is_some_and(|v| v.iter().any(|u| u == uid))
}

/// Upstream `jobCtx.uncounted.succeeded.Has(pod.UID)`.
pub fn uncounted_has_succeeded(uncounted: &UncountedTerminatedPods, uid: &str) -> bool {
    uncounted_has(&uncounted.succeeded, uid)
}

/// Upstream `jobCtx.uncounted.failed.Has(pod.UID)`.
pub fn uncounted_has_failed(uncounted: &UncountedTerminatedPods, uid: &str) -> bool {
    uncounted_has(&uncounted.failed, uid)
}

/// Append a UID to the uncounted succeeded list.
pub fn push_uncounted_succeeded(uncounted: &mut UncountedTerminatedPods, uid: &str) {
    uncounted
        .succeeded
        .get_or_insert_with(Vec::new)
        .push(uid.to_string());
}

/// Append a UID to the uncounted failed list.
pub fn push_uncounted_failed(uncounted: &mut UncountedTerminatedPods, uid: &str) {
    uncounted
        .failed
        .get_or_insert_with(Vec::new)
        .push(uid.to_string());
}

/// Fold UIDs whose finalizer is gone into the real counters.
///
/// Port of upstream `cleanUncountedPodsWithoutFinalizers`
/// (`job_controller.go`, immediately after `flushUncountedAndRemoveFinalizers`):
///
/// ```text
/// newUncounted := filterInUncountedUIDs(uncountedStatus.Succeeded, uidsWithFinalizer)
/// if len(newUncounted) != len(uncountedStatus.Succeeded) {
///     updated = true
///     status.Succeeded += int32(len(uncountedStatus.Succeeded) - len(newUncounted))
///     uncountedStatus.Succeeded = newUncounted
/// }
/// ```
///
/// A UID in the uncounted list that is no longer among `uids_with_finalizer`
/// means its finalizer removal has landed, so the pod can no longer be
/// re-observed as a fresh termination: it is safe — and necessary — to count it
/// now. Returns whether the status changed and therefore needs flushing.
pub fn clean_uncounted_pods_without_finalizers(
    succeeded: &mut Option<i32>,
    failed: &mut Option<i32>,
    uncounted: &mut UncountedTerminatedPods,
    uids_with_finalizer: &HashSet<String>,
) -> bool {
    let mut updated = false;

    if let Some(list) = uncounted.succeeded.as_mut() {
        let before = list.len();
        list.retain(|uid| uids_with_finalizer.contains(uid));
        let folded = before - list.len();
        if folded > 0 {
            updated = true;
            *succeeded = Some(succeeded.unwrap_or(0) + folded as i32);
        }
    }

    if let Some(list) = uncounted.failed.as_mut() {
        let before = list.len();
        list.retain(|uid| uids_with_finalizer.contains(uid));
        let folded = before - list.len();
        if folded > 0 {
            updated = true;
            *failed = Some(failed.unwrap_or(0) + folded as i32);
        }
    }

    updated
}

/// UIDs whose tracking-finalizer removal has been issued but not yet observed.
///
/// Port of upstream `uidTrackingExpectations` (`tracking_utils.go:48-125`).
/// Keyed by job key (`<namespace>/<name>`), exactly as upstream keys it by the
/// controller key.
#[derive(Debug, Default)]
pub struct FinalizerExpectations {
    inner: Mutex<HashMap<String, HashSet<String>>>,
}

impl FinalizerExpectations {
    pub fn new() -> Self {
        Self::default()
    }

    /// Upstream `expectFinalizersRemoved`: record that these UIDs have had a
    /// finalizer removal issued against them.
    pub fn expect_removed(&self, job_key: &str, uids: impl IntoIterator<Item = String>) {
        let mut guard = self.inner.lock().expect("finalizer expectations poisoned");
        guard.entry(job_key.to_string()).or_default().extend(uids);
    }

    /// Upstream `finalizerRemovalObserved`: the removal for this UID has been
    /// seen (or the removal failed and must not stay expected).
    pub fn removal_observed(&self, job_key: &str, uid: &str) {
        let mut guard = self.inner.lock().expect("finalizer expectations poisoned");
        if let Some(set) = guard.get_mut(job_key) {
            set.remove(uid);
            if set.is_empty() {
                guard.remove(job_key);
            }
        }
    }

    /// Upstream `getExpectedUIDs`.
    pub fn expected(&self, job_key: &str) -> HashSet<String> {
        let guard = self.inner.lock().expect("finalizer expectations poisoned");
        guard.get(job_key).cloned().unwrap_or_default()
    }

    /// Upstream `deleteExpectations`, for when the Job itself goes away.
    pub fn forget(&self, job_key: &str) {
        let mut guard = self.inner.lock().expect("finalizer expectations poisoned");
        guard.remove(job_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    fn pod_with(finalizers: Option<Vec<String>>) -> Pod {
        Pod {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta {
                name: "p".to_string(),
                finalizers,
                ..Default::default()
            },
            spec: None,
            status: None,
        }
    }

    fn uids(list: &[&str]) -> HashSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn detects_the_tracking_finalizer() {
        assert!(!has_job_tracking_finalizer(&pod_with(None)));
        assert!(!has_job_tracking_finalizer(&pod_with(Some(vec![
            "other/fin".to_string()
        ]))));
        assert!(has_job_tracking_finalizer(&pod_with(Some(vec![
            JOB_TRACKING_FINALIZER.to_string()
        ]))));
    }

    #[test]
    fn removes_only_the_tracking_finalizer() {
        let mut pod = pod_with(Some(vec![
            "keep/me".to_string(),
            JOB_TRACKING_FINALIZER.to_string(),
        ]));
        assert!(remove_tracking_finalizer(&mut pod));
        assert_eq!(
            pod.metadata.finalizers,
            Some(vec!["keep/me".to_string()]),
            "unrelated finalizers must survive"
        );
        assert!(
            !remove_tracking_finalizer(&mut pod),
            "removing twice is a no-op"
        );
    }

    #[test]
    fn clears_the_list_when_the_tracking_finalizer_was_the_only_one() {
        let mut pod = pod_with(Some(vec![JOB_TRACKING_FINALIZER.to_string()]));
        assert!(remove_tracking_finalizer(&mut pod));
        assert_eq!(
            pod.metadata.finalizers, None,
            "an empty finalizer list serializes as absent, not []"
        );
    }

    /// The core of exactly-once: a UID still holding its finalizer stays
    /// parked; one whose finalizer is gone is folded into the counter.
    #[test]
    fn folds_only_uids_whose_finalizer_is_gone() {
        let mut uncounted = UncountedTerminatedPods {
            succeeded: Some(vec!["s-gone".to_string(), "s-held".to_string()]),
            failed: Some(vec!["f-gone".to_string()]),
        };
        let mut succeeded = Some(1);
        let mut failed = Some(2);

        let changed = clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&["s-held"]),
        );

        assert!(changed);
        assert_eq!(succeeded, Some(2), "s-gone folded into the counter");
        assert_eq!(failed, Some(3), "f-gone folded into the counter");
        assert_eq!(uncounted.succeeded, Some(vec!["s-held".to_string()]));
        assert_eq!(uncounted.failed, Some(vec![]));
    }

    #[test]
    fn folding_is_idempotent_and_never_decreases() {
        let mut uncounted = UncountedTerminatedPods {
            succeeded: Some(vec!["a".to_string()]),
            failed: None,
        };
        let mut succeeded = Some(0);
        let mut failed = Some(0);

        assert!(clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&[]),
        ));
        assert_eq!(succeeded, Some(1));

        // A second pass has nothing left to fold, so the counter holds.
        assert!(!clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&[]),
        ));
        assert_eq!(succeeded, Some(1), "a pod is counted exactly once");
    }

    #[test]
    fn expectations_park_and_release_uids() {
        let exp = FinalizerExpectations::new();
        assert!(exp.expected("ns/j").is_empty());

        exp.expect_removed("ns/j", ["u1".to_string(), "u2".to_string()]);
        assert_eq!(exp.expected("ns/j"), uids(&["u1", "u2"]));

        exp.removal_observed("ns/j", "u1");
        assert_eq!(exp.expected("ns/j"), uids(&["u2"]));

        exp.removal_observed("ns/j", "u2");
        assert!(
            exp.expected("ns/j").is_empty(),
            "the entry is dropped once drained"
        );
    }

    #[test]
    fn expectations_are_scoped_per_job() {
        let exp = FinalizerExpectations::new();
        exp.expect_removed("ns/a", ["u1".to_string()]);
        exp.expect_removed("ns/b", ["u2".to_string()]);

        exp.forget("ns/a");
        assert!(exp.expected("ns/a").is_empty());
        assert_eq!(
            exp.expected("ns/b"),
            uids(&["u2"]),
            "forgetting one job must not touch another"
        );
    }
}
