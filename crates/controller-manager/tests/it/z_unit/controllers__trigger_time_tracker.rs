//! Unit tests moved from `src/controllers/trigger_time_tracker.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::{Pod, Service};
    pub use std::collections::HashMap;
    pub use std::sync::Mutex;
}

mod tests {
    //! Ports of `trigger_time_tracker_test.go`.
    use super::__parent::*;
    use chrono::{Duration, TimeZone};
    use rusternetes_common::resources::{PodCondition, PodSpec, PodStatus, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::trigger_time_tracker::*;

    const NS: &str = "ttNamespace1";
    const SVC: &str = "my-service";

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs)
    }

    /// `createPod`.
    fn create_pod(name: &str, ready: DateTime<Utc>) -> Pod {
        // Struct-init on purpose: a test fixture, not a controller
        // writing status (tests/it/status_subresource_guard.rs).
        let base = Pod::new(name, PodSpec::default());
        let pod_state = Some(PodStatus {
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: Some(ready),
                observed_generation: None,
            }]),
            ..Default::default()
        });
        Pod {
            metadata: ObjectMeta::new(name).with_namespace(NS),
            status: pod_state,
            ..base
        }
    }

    /// `createService`.
    fn create_service(created: DateTime<Utc>) -> Service {
        let mut s = Service::new(SVC, ServiceSpec::default());
        s.metadata = ObjectMeta::new(SVC).with_namespace(NS);
        s.metadata.creation_timestamp = Some(created);
        s
    }

    fn compute(tr: &TriggerTimeTracker, svc: &Service, pods: &[&Pod]) -> Option<DateTime<Utc>> {
        tr.compute_endpoint_last_change_trigger_time(NS, svc, pods)
    }

    #[test]
    fn new_service_no_pods() {
        let tr = TriggerTimeTracker::new();
        assert_eq!(compute(&tr, &create_service(t(2)), &[]), Some(t(2)));
    }

    #[test]
    fn new_service_existing_pods() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(3));
        let (p1, p2, p3) = (
            create_pod("pod1", t(0)),
            create_pod("pod2", t(1)),
            create_pod("pod3", t(5)),
        );
        // Pods were created before service, but trigger time is the time when
        // service was created.
        assert_eq!(compute(&tr, &svc, &[&p1, &p2, &p3]), Some(t(3)));
    }

    #[test]
    fn pods_added() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(0));
        assert_eq!(compute(&tr, &svc, &[]), Some(t(0)));
        let (p1, p2) = (create_pod("pod1", t(2)), create_pod("pod2", t(1)));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(1)));
    }

    #[test]
    fn pods_updated() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(0));
        let (p1, p2, p3) = (
            create_pod("pod1", t(1)),
            create_pod("pod2", t(2)),
            create_pod("pod3", t(3)),
        );
        assert_eq!(compute(&tr, &svc, &[&p1, &p2, &p3]), Some(t(0)));
        let (p1, p2) = (create_pod("pod1", t(5)), create_pod("pod2", t(4)));
        // pod3 doesn't change.
        assert_eq!(compute(&tr, &svc, &[&p1, &p2, &p3]), Some(t(4)));
    }

    #[test]
    fn pods_updated_no_op() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(0));
        let (p1, p2, p3) = (
            create_pod("pod1", t(1)),
            create_pod("pod2", t(2)),
            create_pod("pod3", t(3)),
        );
        assert_eq!(compute(&tr, &svc, &[&p1, &p2, &p3]), Some(t(0)));
        // Nothing has changed.
        assert_eq!(compute(&tr, &svc, &[&p1, &p2, &p3]), None);
    }

    #[test]
    fn pod_deleted_then_added() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(0));
        let (p1, p2) = (create_pod("pod1", t(1)), create_pod("pod2", t(2)));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(0)));
        assert_eq!(compute(&tr, &svc, &[&p1]), None);
        let p2 = create_pod("pod2", t(4));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(4)));
    }

    #[test]
    fn service_deleted_then_added() {
        let tr = TriggerTimeTracker::new();
        let svc = create_service(t(0));
        let (p1, p2) = (create_pod("pod1", t(1)), create_pod("pod2", t(2)));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(0)));
        tr.delete_service(NS, SVC);
        let svc = create_service(t(3));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(3)));
    }

    #[test]
    fn service_updated_no_pod_change() {
        let tr = TriggerTimeTracker::new();
        let mut svc = create_service(t(0));
        let (p1, p2) = (create_pod("pod1", t(1)), create_pod("pod2", t(2)));
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), Some(t(0)));
        // service's ports have changed.
        svc.spec.selector = Some(HashMap::new());
        // Currently we're not able to calculate trigger time for service
        // updates, hence the returned value is a nil time.
        assert_eq!(compute(&tr, &svc, &[&p1, &p2]), None);
    }

    #[test]
    fn rfc3339_nano_matches_go() {
        assert_eq!(format_rfc3339_nano(t(0)), "2019-01-01T00:00:00Z");
        let frac = t(0) + Duration::nanoseconds(120_000_000);
        assert_eq!(format_rfc3339_nano(frac), "2019-01-01T00:00:00.12Z");
    }
}
