//! Unit tests moved from `src/controllers/controller_utils.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::pod::PodCondition;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_common::types::Phase;
    pub use std::collections::{HashMap, HashSet};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::controller_utils::*;
    use serde_json::json;

    const GATES_ON: RankGates = RankGates {
        pod_deletion_cost: true,
        logarithmic_scale_down: true,
    };

    // controller_utils.go:875,895 read `features.LogarithmicScaleDown` (1.31 GA,
    // default true); `RankGates::current` must resolve it through the `Feature`
    // variant, so flipping the variant flips the gate the ranking sees.
    #[test]
    #[serial_test::serial]
    fn rank_gates_follow_logarithmic_scale_down_feature() {
        use rusternetes_common::feature_gates::{self, Feature};
        feature_gates::reset_to_defaults();
        assert!(feature_gates::enabled(Feature::LogarithmicScaleDown));
        assert!(RankGates::current().logarithmic_scale_down);
        {
            let _off = feature_gates::with_feature(Feature::LogarithmicScaleDown, false);
            assert!(!RankGates::current().logarithmic_scale_down);
        }
        assert!(RankGates::current().logarithmic_scale_down);
    }

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
