//! Unit tests moved from `src/controllers/hpa_pod_grouping.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Duration, Utc};
    pub use rusternetes_common::resources::pod::Pod;
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_controller_manager::controllers::hpa_metrics_client::PodMetricsInfo;
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::{PodCondition, PodStatus};
    use rusternetes_controller_manager::controllers::hpa_pod_grouping::*;

    fn metrics(names: &[&str]) -> PodMetricsInfo {
        rusternetes_controller_manager::controllers::hpa_metrics_client::FakeMetricsClient::pods_info(
            &names
                .iter()
                .map(|n| (*n, 0i64, Some(50)))
                .collect::<Vec<_>>(),
        )
    }

    fn pod(name: &str, phase: Phase, ready: Option<&str>, start_ago: Duration) -> Pod {
        let now = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let conditions = ready.map(|s| {
            vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: s.to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: Some(now - start_ago),
                observed_generation: None,
            }]
        });
        let mut pod = Pod::new(name, Default::default());
        pod.status = Some(PodStatus {
            phase: Some(phase),
            start_time: Some(now - start_ago),
            conditions,
            ..Default::default()
        });
        pod
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_000_000, 0).unwrap()
    }

    #[test]
    fn just_started_unready_pods_excluded() {
        // Running but Ready=False and started 10s ago (within cpuInit) → excluded.
        let pods = vec![
            pod("p1", Phase::Running, Some("False"), Duration::seconds(10)),
            pod("p2", Phase::Running, Some("False"), Duration::seconds(10)),
        ];
        let m = metrics(&["p1", "p2"]);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
        );
        assert!(
            ready.is_empty(),
            "cold-start unready pods must be excluded: {ready:?}"
        );
    }

    #[test]
    fn long_ready_pods_counted() {
        // Ready=True since 10min ago, started 10min ago (past cpuInit) → counted.
        let pods = vec![pod(
            "p1",
            Phase::Running,
            Some("True"),
            Duration::minutes(10),
        )];
        let m = metrics(&["p1"]);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
        );
        assert_eq!(ready.len(), 1);
        assert!(ready.contains("p1"));
    }

    #[test]
    fn pending_failed_missing_excluded() {
        let pods = vec![
            pod("pending", Phase::Pending, None, Duration::seconds(1)),
            pod("failed", Phase::Failed, Some("True"), Duration::minutes(10)),
            pod(
                "nometric",
                Phase::Running,
                Some("True"),
                Duration::minutes(10),
            ),
        ];
        // only "pending"/"failed" have... actually seed metric only for nometric? no:
        let m = metrics(&["pending", "failed"]); // "nometric" intentionally absent
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
        );
        assert!(
            ready.is_empty(),
            "pending/failed/missing all excluded: {ready:?}"
        );
    }

    #[test]
    fn per_sample_window_drives_cpu_init_exclusion() {
        // replica_calculator.go groupPods: within cpuInitializationPeriod a Ready
        // pod is still ignored if metric.Timestamp < lastTransition + metric.Window.
        let pods = vec![pod(
            "p1",
            Phase::Running,
            Some("True"),
            Duration::minutes(1),
        )];
        let mut m = metrics(&["p1"]);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
        );
        assert!(ready.contains("p1"), "short window: counted");
        m.get_mut("p1").unwrap().window = std::time::Duration::from_secs(100 * 365 * 86400);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
        );
        assert!(ready.is_empty(), "long per-sample window: excluded");
    }
}
