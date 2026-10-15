//! Pod helpers shared by the replica-managing controllers, ported from
//! upstream `pkg/controller/controller_utils.go` and
//! `pkg/api/v1/pod/util.go`.
//!
//! * `ActivePods` / `ActivePodsWithRanks` — the victim ordering
//!   (`controller_utils.go:741-909`), used by the ReplicaSet and
//!   ReplicationController scale-down (`getPodsToDelete`,
//!   `replica_set.go:874`) and by the Job controller.
//! * `IsPodAvailable`, `nextPodAvailabilityCheck`,
//!   `FindMinNextPodAvailabilityCheck` — `minReadySeconds` accounting
//!   (`pkg/api/v1/pod/util.go:283`, `controller_utils.go:1028-1085`).

use chrono::{DateTime, Utc};
use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::Pod;
use rusternetes_common::types::Phase;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// `core.PodDeletionCost` (`pkg/apis/core/annotation_key_constants.go:136`).
pub(crate) const POD_DELETION_COST: &str = "controller.kubernetes.io/pod-deletion-cost";

/// `podutil.IsPodReady`: the Ready condition is True.
pub(crate) fn is_pod_ready(pod: &Pod) -> bool {
    pod_ready_condition(pod).is_some()
}

/// The Ready condition when it is `True`. Upstream's `GetPodReadyCondition`
/// returns it whatever its status, but every caller here (`podReadyTime`,
/// `IsPodAvailable`, `nextPodAvailabilityCheck`) has already established
/// `IsPodReady`, so the two agree on every path.
pub(crate) fn pod_ready_condition(pod: &Pod) -> Option<&PodCondition> {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.condition_type == "Ready" && c.status == "True")
        })
}

fn phase_of(pod: &Pod) -> Option<&Phase> {
    pod.status.as_ref().and_then(|s| s.phase.as_ref())
}

/// `controller.IsPodActive` (`controller_utils.go:1085`): not Succeeded or
/// Failed, and no deletionTimestamp.
pub(crate) fn is_pod_active(pod: &Pod) -> bool {
    !matches!(phase_of(pod), Some(Phase::Succeeded) | Some(Phase::Failed))
        && pod.metadata.deletion_timestamp.is_none()
}

/// `controller.IsPodTerminating` (`controller_utils.go:1092`): not terminal,
/// with a deletionTimestamp.
pub(crate) fn is_pod_terminating(pod: &Pod) -> bool {
    !matches!(phase_of(pod), Some(Phase::Succeeded) | Some(Phase::Failed))
        && pod.metadata.deletion_timestamp.is_some()
}

/// `podutil.IsPodAvailable` (`pkg/api/v1/pod/util.go:283-294`): Ready, and
/// Ready for at least `min_ready_seconds` since the Ready condition's
/// lastTransitionTime (a zero transition time is never available once
/// `minReadySeconds` is set).
pub(crate) fn is_pod_available(pod: &Pod, min_ready_seconds: i32, now: DateTime<Utc>) -> bool {
    let Some(cond) = pod_ready_condition(pod) else {
        return false;
    };
    min_ready_seconds == 0
        || cond
            .last_transition_time
            .is_some_and(|t| t + chrono::Duration::seconds(min_ready_seconds as i64) <= now)
}

/// `nextPodAvailabilityCheck` (`controller_utils.go:1028-1042`).
pub(crate) fn next_pod_availability_check(
    pod: &Pod,
    min_ready_seconds: i32,
    now: DateTime<Utc>,
) -> Option<Duration> {
    if min_ready_seconds <= 0 {
        return None;
    }
    let cond = pod_ready_condition(pod)?;
    let since = cond.last_transition_time?;
    let next = since + chrono::Duration::seconds(min_ready_seconds as i64) - now;
    if next > chrono::Duration::zero() {
        next.to_std().ok()
    } else {
        None
    }
}

/// `findMinNextPodAvailabilitySimpleCheck` (`controller_utils.go:1048-1060`):
/// the shortest wait, and the first pod it belongs to.
pub(crate) fn find_min_next_pod_availability_simple_check<'a>(
    pods: &[&'a Pod],
    min_ready_seconds: i32,
    now: DateTime<Utc>,
) -> Option<(Duration, &'a Pod)> {
    let mut min: Option<(Duration, &Pod)> = None;
    for &pod in pods {
        if let Some(next) = next_pod_availability_check(pod, min_ready_seconds, now) {
            if min.is_none_or(|(m, _)| next < m) {
                min = Some((next, pod));
            }
        }
    }
    min
}

/// `FindMinNextPodAvailabilityCheck` (`controller_utils.go:1069-1085`).
///
/// `last_owner_status_evaluation` is the instant status was computed at;
/// `clock_now` is the clock now (upstream's `clock.Now()`), so the wait is
/// measured from when the caller actually acts, not from the evaluation.
pub(crate) fn find_min_next_pod_availability_check(
    pods: &[&Pod],
    min_ready_seconds: i32,
    last_owner_status_evaluation: DateTime<Utc>,
    clock_now: DateTime<Utc>,
) -> Option<Duration> {
    let (_, check_pod) = find_min_next_pod_availability_simple_check(
        pods,
        min_ready_seconds,
        last_owner_status_evaluation,
    )?;
    // A more precise value, from the earliest pod only (a later pod must not
    // preempt it).
    Some(
        next_pod_availability_check(check_pod, min_ready_seconds, clock_now)
            // Past the check already: fall back to an immediate one.
            .unwrap_or(Duration::ZERO),
    )
}

/// `controller.PodKey` (`controller_utils.go`): "namespace/name".
pub(crate) fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `maxContainerRestarts` (`controller_utils.go:948`): `(regularRestarts,
/// sidecarRestarts)`.
pub(crate) fn max_container_restarts(pod: &Pod) -> (u32, u32) {
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

/// `afterOrZero` (`controller_utils.go:913`): `t1` is after `t2`; a zero
/// (`None`) time counts as after any non-zero time.
pub(crate) fn after_or_zero(t1: Option<DateTime<Utc>>, t2: Option<DateTime<Utc>>) -> bool {
    match (t1, t2) {
        (Some(a), Some(b)) => a > b,
        (a, _) => a.is_none(),
    }
}

/// `podPhaseToOrdinal` (`controller_utils.go:111`): Pending 0 < Unknown 1 <
/// Running 2; any other (or empty) phase is the map's zero value.
fn phase_ordinal(pod: &Pod) -> i32 {
    match phase_of(pod) {
        Some(Phase::Unknown) => 1,
        Some(Phase::Running) => 2,
        _ => 0,
    }
}

fn node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

/// `compareMaxContainerRestarts` (`controller_utils.go:972-985`).
fn compare_max_container_restarts(a: &Pod, b: &Pod) -> Option<bool> {
    let (ra, rb) = (max_container_restarts(a), max_container_restarts(b));
    if ra.0 != rb.0 {
        return Some(ra.0 > rb.0);
    }
    if ra.1 != rb.1 {
        return Some(ra.1 > rb.1);
    }
    None
}

/// `controller.ActivePods.Less` (`controller_utils.go:741-776`): true when
/// `a` should be deleted before `b`.
pub(crate) fn active_pods_less(a: &Pod, b: &Pod) -> bool {
    // 1. Unassigned < assigned.
    let (na, nb) = (node_name(a), node_name(b));
    if na != nb && (na.is_empty() || nb.is_empty()) {
        return na.is_empty();
    }
    // 2. PodPending < PodUnknown < PodRunning.
    if phase_ordinal(a) != phase_ordinal(b) {
        return phase_ordinal(a) < phase_ordinal(b);
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
    // 5. More restarts < fewer restarts.
    if let Some(res) = compare_max_container_restarts(a, b) {
        return res;
    }
    // 6. Newer < older.
    if a.metadata.creation_timestamp != b.metadata.creation_timestamp {
        return after_or_zero(a.metadata.creation_timestamp, b.metadata.creation_timestamp);
    }
    false
}

pub(crate) fn sort_active_pods(pods: &mut [&Pod]) {
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

/// `podReadyTime` (`controller_utils.go:936`): when the pod became Ready;
/// `None` is upstream's zero time.
fn pod_ready_time(pod: &Pod) -> Option<DateTime<Utc>> {
    pod_ready_condition(pod).and_then(|c| c.last_transition_time)
}

/// `GetDeletionCostFromPodAnnotations`
/// (`pkg/apis/core/helper/helpers.go:491-513`): the annotation parsed as an
/// int32, 0 when absent or invalid (a leading `+` or zero is invalid).
pub(crate) fn deletion_cost(pod: &Pod) -> i32 {
    let Some(value) = pod
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(POD_DELETION_COST))
    else {
        return 0;
    };
    let valid_first_digit = match value.as_bytes().first() {
        None => false,
        Some(b'-') => true,
        Some(b'0') => value == "0",
        Some(c) => c.is_ascii_digit(),
    };
    if !valid_first_digit {
        return 0;
    }
    value.parse::<i32>().unwrap_or(0)
}

/// `logarithmicRankDiff` (`controller_utils.go:896-909`): the difference of
/// the base-2 logarithmic ranks of two timestamps relative to `now`.
fn logarithmic_rank_diff(t1: DateTime<Utc>, t2: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    let rank = |t: DateTime<Utc>| -> i64 {
        match (now - t).num_nanoseconds() {
            Some(d) if d > 0 => (d as f64).log2() as i64,
            // Not in the past (or overflowing i64 ns): upstream's `-1` seed.
            _ => -1,
        }
    };
    rank(t1) - rank(t2)
}

/// Which feature gates `ActivePodsWithRanks.Less` consults. Resolved once per
/// sort by the caller: they are process-wide and `Less` runs O(n log n) times.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RankGates {
    /// `PodDeletionCost` (1.22 Beta, on in v1.35).
    pub pod_deletion_cost: bool,
    /// `LogarithmicScaleDown` (GA since 1.31, locked on).
    pub logarithmic_scale_down: bool,
}

impl RankGates {
    pub(crate) fn current() -> Self {
        Self {
            pod_deletion_cost: rusternetes_common::feature_gates::enabled(
                rusternetes_common::feature_gates::Feature::PodDeletionCost,
            ),
            // No `Feature` variant models it; read it by name.
            logarithmic_scale_down: rusternetes_common::feature_gates::gate_enabled(
                "LogarithmicScaleDown",
            )
            .unwrap_or(true),
        }
    }
}

/// `ActivePodsWithRanks.Less` (`controller_utils.go:835-909`): true when `a`
/// (with `rank_a`) should be preferred for deletion over `b`.
///
/// `now` is upstream's `Now`; `None` is its zero value ("comparison happens
/// without scaling").
pub(crate) fn active_pods_with_ranks_less(
    a: &Pod,
    rank_a: usize,
    b: &Pod,
    rank_b: usize,
    now: Option<DateTime<Utc>>,
    gates: RankGates,
) -> bool {
    // 1. Unassigned < assigned.
    let (na, nb) = (node_name(a), node_name(b));
    if na != nb && (na.is_empty() || nb.is_empty()) {
        return na.is_empty();
    }
    // 2. PodPending < PodUnknown < PodRunning.
    if phase_ordinal(a) != phase_ordinal(b) {
        return phase_ordinal(a) < phase_ordinal(b);
    }
    // 3. Not ready < ready.
    if is_pod_ready(a) != is_pod_ready(b) {
        return !is_pod_ready(a);
    }
    // 4. Lower pod-deletion-cost < higher.
    if gates.pod_deletion_cost {
        let (ca, cb) = (deletion_cost(a), deletion_cost(b));
        if ca != cb {
            return ca < cb;
        }
    }
    // 5. Doubled up < not doubled up: more colocated related pods first.
    if rank_a != rank_b {
        return rank_a > rank_b;
    }
    // 6. Been ready for empty time < less time < more time.
    if is_pod_ready(a) && is_pod_ready(b) {
        let (ta, tb) = (pod_ready_time(a), pod_ready_time(b));
        if ta != tb {
            return scaled_after_or_zero(a, ta, b, tb, now, gates);
        }
    }
    // 7. More container restarts < fewer.
    if let Some(res) = compare_max_container_restarts(a, b) {
        return res;
    }
    // 8. Empty creation time < newer < older.
    if a.metadata.creation_timestamp != b.metadata.creation_timestamp {
        return scaled_after_or_zero(
            a,
            a.metadata.creation_timestamp,
            b,
            b.metadata.creation_timestamp,
            now,
            gates,
        );
    }
    false
}

/// The shared tail of rules 6 and 8: `afterOrZero` without scaling, otherwise
/// the logarithmic bucket difference, with the UID as the tiebreak inside a
/// bucket (`controller_utils.go:857-866`, `:871-881`).
fn scaled_after_or_zero(
    a: &Pod,
    t1: Option<DateTime<Utc>>,
    b: &Pod,
    t2: Option<DateTime<Utc>>,
    now: Option<DateTime<Utc>>,
    gates: RankGates,
) -> bool {
    if !gates.logarithmic_scale_down {
        return after_or_zero(t1, t2);
    }
    match (now, t1, t2) {
        (Some(now), Some(t1), Some(t2)) => {
            let diff = logarithmic_rank_diff(t1, t2, now);
            if diff == 0 {
                a.metadata.uid < b.metadata.uid
            } else {
                diff < 0
            }
        }
        _ => after_or_zero(t1, t2),
    }
}

/// `sort.Sort(ActivePodsWithRanks{Pods, Rank, Now})`: returns `pods` ordered
/// most-preferred-for-deletion first.
///
/// Go's `sort.Sort` tolerates a `Less` that is not a strict weak order (the
/// logarithmic buckets mixed with the zero-time rule can be intransitive),
/// whereas Rust's `sort_by` may panic on one. A merge sort over `Less`
/// reproduces "returns some permutation" without that hazard.
pub(crate) fn sort_active_pods_with_ranks<'a>(
    pods: &[&'a Pod],
    ranks: &[usize],
    now: Option<DateTime<Utc>>,
    gates: RankGates,
) -> Vec<&'a Pod> {
    assert_eq!(pods.len(), ranks.len(), "pods and ranks must be parallel");
    let items: Vec<(&'a Pod, usize)> = pods.iter().copied().zip(ranks.iter().copied()).collect();
    merge_sort(items, &|x: &(&Pod, usize), y: &(&Pod, usize)| {
        active_pods_with_ranks_less(x.0, x.1, y.0, y.1, now, gates)
    })
    .into_iter()
    .map(|(pod, _)| pod)
    .collect()
}

fn merge_sort<T: Clone>(mut items: Vec<T>, less: &dyn Fn(&T, &T) -> bool) -> Vec<T> {
    if items.len() <= 1 {
        return items;
    }
    let right = items.split_off(items.len() / 2);
    let left = merge_sort(items, less);
    let right = merge_sort(right, less);
    let mut out = Vec::with_capacity(left.len() + right.len());
    let (mut l, mut r) = (left.into_iter().peekable(), right.into_iter().peekable());
    while let (Some(a), Some(b)) = (l.peek(), r.peek()) {
        // Stable: take from the right only when it is strictly less.
        if less(b, a) {
            out.push(r.next().unwrap());
        } else {
            out.push(l.next().unwrap());
        }
    }
    out.extend(l);
    out.extend(r);
    out
}

/// Number of active pods per node among `related`, used as the rank
/// (`getPodsRankedByRelatedPodsOnSameNode`, `replica_set.go:942-957`).
pub(crate) fn pods_on_node_counts(related: &[&Pod]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for pod in related {
        if is_pod_active(pod) {
            *counts.entry(node_name(pod).to_string()).or_default() += 1;
        }
    }
    counts
}

pub(crate) fn node_of(pod: &Pod) -> &str {
    node_name(pod)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GATES_ON: RankGates = RankGates {
        pod_deletion_cost: true,
        logarithmic_scale_down: true,
    };

    fn t(now: DateTime<Utc>, secs_ago: i64) -> DateTime<Utc> {
        now - chrono::Duration::seconds(secs_ago)
    }

    /// The pod builder of `TestSortingActivePodsWithRanks`
    /// (`controller_utils_test.go:748-785`).
    #[allow(clippy::too_many_arguments)]
    fn pod(
        name: &str,
        node: &str,
        phase: &str,
        ready: bool,
        restarts: u32,
        side_restarts: u32,
        ready_since: Option<DateTime<Utc>>,
        created: Option<DateTime<Utc>>,
        annotations: Option<(&str, &str)>,
    ) -> Pod {
        let mut value = json!({
            "metadata": {"name": name, "uid": ""},
            "spec": {
                "nodeName": node,
                "containers": [],
                "initContainers": [{"name": "sidecar", "image": "x", "restartPolicy": "Always"}],
            },
            "status": {"phase": phase},
        });
        if ready {
            value["status"]["conditions"] = json!([{"type": "Ready", "status": "True"}]);
            value["status"]["containerStatuses"] = json!([{"name": "c", "restartCount": restarts}]);
            value["status"]["initContainerStatuses"] =
                json!([{"name": "sidecar", "restartCount": side_restarts}]);
        }
        if let Some((k, v)) = annotations {
            value["metadata"]["annotations"] = json!({ k: v });
        }
        let mut pod: Pod = serde_json::from_value(value).unwrap();
        pod.metadata.creation_timestamp = created;
        if let Some(cond) = pod
            .status
            .as_mut()
            .and_then(|s| s.conditions.as_mut())
            .and_then(|c| c.first_mut())
        {
            cond.last_transition_time = ready_since;
        }
        pod
    }

    fn less(a: &(Pod, usize), b: &(Pod, usize), now: DateTime<Utc>, gates: RankGates) -> bool {
        active_pods_with_ranks_less(&a.0, a.1, &b.0, b.1, Some(now), gates)
    }

    /// `TestSortingActivePodsWithRanks` equality half
    /// (`controller_utils_test.go:826-850`).
    #[test]
    fn ranks_equality() {
        let now = Utc::now();
        let month = now - chrono::Duration::days(30);
        let h = |hours: i64| t(now, hours * 3600);
        let unscheduled = pod("unscheduled", "", "Pending", false, 0, 0, None, None, None);
        let pending = pod("pending", "node", "Pending", false, 0, 0, None, None, None);
        let unknown = pod(
            "unknown-phase",
            "node",
            "Unknown",
            false,
            0,
            0,
            None,
            None,
            None,
        );
        let not_ready = pod(
            "not-ready",
            "node",
            "Running",
            false,
            0,
            0,
            None,
            None,
            None,
        );
        let ready_now_then = pod(
            "ready-now-created-then",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(month),
            None,
        );
        let ready_now = pod(
            "ready-now",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(now),
            None,
        );
        let ready_then = pod(
            "ready-then",
            "node",
            "Running",
            true,
            0,
            0,
            Some(month),
            Some(month),
            None,
        );
        let high_restarts = pod(
            "ready-high-restarts",
            "node",
            "Running",
            true,
            9001,
            0,
            Some(now),
            Some(now),
            None,
        );
        let unscheduled_5h = pod(
            "unscheduled-5-hours",
            "",
            "Pending",
            false,
            0,
            0,
            Some(h(5)),
            Some(h(5)),
            None,
        );
        let unscheduled_8h = pod(
            "unscheduled-10-hours",
            "",
            "Pending",
            false,
            0,
            0,
            Some(h(8)),
            Some(h(8)),
            None,
        );
        let ready_5h = pod(
            "ready-5-hours",
            "",
            "Running",
            true,
            0,
            0,
            Some(h(5)),
            Some(month),
            None,
        );
        let ready_8h = pod(
            "ready-10-hours",
            "",
            "Running",
            true,
            0,
            0,
            Some(h(8)),
            Some(month),
            None,
        );
        let cases: Vec<(&Pod, &Pod)> = vec![
            (&unscheduled, &unscheduled),
            (&pending, &pending),
            (&unknown, &unknown),
            (&not_ready, &not_ready),
            (&ready_now_then, &ready_now_then),
            (&ready_now, &ready_now),
            (&ready_then, &ready_then),
            (&high_restarts, &high_restarts),
            (&unscheduled_5h, &unscheduled_8h),
            (&ready_5h, &ready_8h),
        ];
        for (i, (p1, p2)) in cases.into_iter().enumerate() {
            let (a, b) = ((p1.clone(), 1), (p2.clone(), 1));
            assert!(
                !less(&a, &b, now, GATES_ON) && !less(&b, &a, now, GATES_ON),
                "equality case {i}: {} vs {}",
                p1.metadata.name,
                p2.metadata.name
            );
        }
    }

    /// `TestSortingActivePodsWithRanks` inequality half
    /// (`controller_utils_test.go:852-885`).
    #[test]
    fn ranks_inequality() {
        let now = Utc::now();
        let month = now - chrono::Duration::days(30);
        let unscheduled = pod("unscheduled", "", "Pending", false, 0, 0, None, None, None);
        let pending = pod("pending", "node", "Pending", false, 0, 0, None, None, None);
        let unknown = pod(
            "unknown-phase",
            "node",
            "Unknown",
            false,
            0,
            0,
            None,
            None,
            None,
        );
        let not_ready = pod(
            "not-ready",
            "node",
            "Running",
            false,
            0,
            0,
            None,
            None,
            None,
        );
        let ready_no_ltt = pod(
            "ready-no-ltt",
            "node",
            "Running",
            true,
            0,
            0,
            None,
            None,
            None,
        );
        let ready_now = pod(
            "ready-now",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(now),
            None,
        );
        let ready_then = pod(
            "ready-then",
            "node",
            "Running",
            true,
            0,
            0,
            Some(month),
            Some(month),
            None,
        );
        let high_restarts = pod(
            "ready-high-restarts",
            "node",
            "Running",
            true,
            9001,
            0,
            Some(now),
            Some(now),
            None,
        );
        let high_side_restarts = pod(
            "ready-high-side-restarts",
            "node",
            "Running",
            true,
            9001,
            9001,
            Some(now),
            Some(now),
            None,
        );
        let ready_now_created_then = pod(
            "ready-now-created-then",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(month),
            None,
        );
        let low_cost = pod(
            "low-deletion-cost",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(month),
            Some((POD_DELETION_COST, "10")),
        );
        let high_cost = pod(
            "high-deletion-cost",
            "node",
            "Running",
            true,
            0,
            0,
            Some(now),
            Some(month),
            Some((POD_DELETION_COST, "100")),
        );
        let ready_2h = pod(
            "ready-2-hours",
            "",
            "Running",
            true,
            0,
            0,
            Some(t(now, 2 * 3600)),
            Some(month),
            None,
        );
        let ready_5h = pod(
            "ready-5-hours",
            "",
            "Running",
            true,
            0,
            0,
            Some(t(now, 5 * 3600)),
            Some(month),
            None,
        );
        let no_cost_gate = RankGates {
            pod_deletion_cost: false,
            ..GATES_ON
        };
        // (lesser, rank, greater, rank, gates)
        let cases: Vec<(&Pod, usize, &Pod, usize, RankGates)> = vec![
            (&unscheduled, 1, &pending, 2, GATES_ON),
            (&unscheduled, 2, &pending, 1, GATES_ON),
            (&pending, 1, &unknown, 2, GATES_ON),
            (&unknown, 1, &not_ready, 2, GATES_ON),
            (&not_ready, 1, &ready_no_ltt, 1, GATES_ON),
            (&ready_no_ltt, 1, &ready_now, 1, GATES_ON),
            (&ready_now, 2, &ready_no_ltt, 1, GATES_ON),
            (&ready_now, 1, &ready_then, 1, GATES_ON),
            (&ready_now, 2, &ready_then, 1, GATES_ON),
            (&high_restarts, 1, &ready_now, 1, GATES_ON),
            (&high_side_restarts, 1, &high_restarts, 1, GATES_ON),
            (&ready_now, 2, &high_restarts, 1, GATES_ON),
            (&ready_now, 1, &ready_now_created_then, 1, GATES_ON),
            (&ready_now_created_then, 2, &ready_now, 1, GATES_ON),
            (&low_cost, 2, &high_cost, 1, GATES_ON),
            (&high_cost, 2, &low_cost, 1, no_cost_gate),
            (&ready_2h, 1, &ready_5h, 1, GATES_ON),
        ];
        for (i, (lesser, lr, greater, gr, gates)) in cases.into_iter().enumerate() {
            let (a, b) = ((lesser.clone(), lr), (greater.clone(), gr));
            assert!(
                less(&a, &b, now, gates),
                "inequality case {i}: {} (rank {lr}) must be less than {} (rank {gr})",
                lesser.metadata.name,
                greater.metadata.name
            );
            assert!(
                !less(&b, &a, now, gates),
                "inequality case {i}: {} (rank {gr}) must not be less than {} (rank {lr})",
                greater.metadata.name,
                lesser.metadata.name
            );
        }
    }

    /// `GetDeletionCostFromPodAnnotations`: a leading `+` or zero is invalid
    /// and parses as 0 (`helpers.go:491-513`).
    #[test]
    fn deletion_cost_parsing() {
        let with = |v: &str| {
            pod(
                "p",
                "n",
                "Running",
                false,
                0,
                0,
                None,
                None,
                Some((POD_DELETION_COST, v)),
            )
        };
        assert_eq!(deletion_cost(&with("10")), 10);
        assert_eq!(deletion_cost(&with("-5")), -5);
        assert_eq!(deletion_cost(&with("0")), 0);
        assert_eq!(deletion_cost(&with("+10")), 0);
        assert_eq!(deletion_cost(&with("008")), 0);
        assert_eq!(deletion_cost(&with("abc")), 0);
        assert_eq!(deletion_cost(&with("99999999999")), 0);
        assert_eq!(
            deletion_cost(&pod("p", "n", "Running", false, 0, 0, None, None, None)),
            0
        );
    }

    fn ready_pod(name: &str, ready: bool, before_secs: i64, now: DateTime<Utc>) -> Pod {
        let mut p = pod(
            name,
            "node0",
            "Running",
            ready,
            0,
            0,
            Some(t(now, before_secs)),
            None,
            None,
        );
        if !ready {
            p.status.as_mut().unwrap().conditions = Some(vec![]);
        }
        p
    }

    /// `TestNextPodAvailabilityCheck` (`controller_utils_test.go:886-962`).
    #[test]
    fn next_availability_check() {
        let now = Utc::now();
        let secs = Duration::from_secs;
        assert_eq!(
            next_pod_availability_check(&ready_pod("p", false, 0, now), 0, now),
            None,
            "not ready"
        );
        assert_eq!(
            next_pod_availability_check(&ready_pod("p", true, 0, now), 0, now),
            None,
            "no minReadySeconds"
        );
        let mut zero_transition = ready_pod("p", true, 0, now);
        zero_transition
            .status
            .as_mut()
            .unwrap()
            .conditions
            .as_mut()
            .unwrap()[0]
            .last_transition_time = None;
        assert_eq!(
            next_pod_availability_check(&zero_transition, 1, now),
            None,
            "lastTransitionTime is zero"
        );
        assert_eq!(
            next_pod_availability_check(&ready_pod("p", true, 0, now), 1, now),
            Some(secs(1))
        );
        assert_eq!(
            next_pod_availability_check(&ready_pod("p", true, 20, now), 30, now),
            Some(secs(10))
        );
        assert_eq!(
            next_pod_availability_check(&ready_pod("p", true, 51, now), 50, now),
            None,
            "available"
        );
    }

    /// `TestFindMinNextPodAvailabilitySimpleCheck`
    /// (`controller_utils_test.go:964-1061`) "unready and ready pods".
    #[test]
    fn find_min_next_availability_simple_check() {
        let now = Utc::now();
        let pods = [
            ready_pod("pod1", false, 0, now),
            ready_pod("pod2", true, 2, now),
            ready_pod("pod3", true, 0, now),
            ready_pod("pod4", true, 4, now),
            ready_pod("pod5", false, 0, now),
        ];
        let refs: Vec<&Pod> = pods.iter().collect();
        let (next, pod) = find_min_next_pod_availability_simple_check(&refs, 10, now).unwrap();
        assert_eq!(next, Duration::from_secs(6));
        assert_eq!(pod.metadata.name, "pod4");
        assert_eq!(
            find_min_next_pod_availability_check(&refs, 10, now, now),
            Some(Duration::from_secs(6)),
            "same clock gives the simple-check result"
        );

        let available = [
            ready_pod("pod2", true, 15, now),
            ready_pod("pod3", true, 11, now),
            ready_pod("pod4", true, 10, now),
        ];
        let refs: Vec<&Pod> = available.iter().collect();
        assert_eq!(
            find_min_next_pod_availability_check(&refs, 10, now, now),
            None,
            "available pods need no check"
        );
    }

    /// `TestFindMinNextPodAvailability` (`controller_utils_test.go:1063-1122`):
    /// the wait is shortened by the delay between evaluation and now, and
    /// falls back to an immediate check once past it.
    #[test]
    fn find_min_next_availability_accounts_for_evaluation_delay() {
        let now = Utc::now();
        let pods = [
            ready_pod("pod2", true, 2, now),
            ready_pod("pod3", true, 0, now),
            ready_pod("pod4", true, 4, now),
        ];
        let refs: Vec<&Pod> = pods.iter().collect();
        let after = |s: i64| now + chrono::Duration::seconds(s);
        assert_eq!(
            find_min_next_pod_availability_check(&refs, 10, now, after(2)),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            find_min_next_pod_availability_check(&refs, 10, now, after(7)),
            Some(Duration::ZERO)
        );
    }

    /// `IsPodAvailable` (`pkg/api/v1/pod/util.go:283-294`).
    #[test]
    fn pod_available_follows_the_ready_transition_time() {
        let now = Utc::now();
        assert!(is_pod_available(&ready_pod("p", true, 0, now), 0, now));
        assert!(!is_pod_available(&ready_pod("p", false, 100, now), 0, now));
        assert!(!is_pod_available(&ready_pod("p", true, 10, now), 60, now));
        assert!(is_pod_available(&ready_pod("p", true, 60, now), 60, now));
        let mut zero = ready_pod("p", true, 0, now);
        zero.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_transition_time = None;
        assert!(!is_pod_available(&zero, 60, now));
        assert!(is_pod_available(&zero, 0, now));
    }
}
