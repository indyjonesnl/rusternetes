//! Port of `TriggerTimeTracker`
//! (`staging/src/k8s.io/endpointslice/util/trigger_time_tracker.go:26-154`).
//!
//! Computes the `endpoints.kubernetes.io/last-change-trigger-time` annotation
//! of an Endpoints (or EndpointSlice) object. Upstream shares one tracker type
//! between the endpoints and endpointslice controllers; this is that one copy.
//!
//! Like upstream it may compute a wrong value if the same object changes
//! several times between two consecutive syncs (`:28-34`).
//!
//! A "zero" `time.Time` is modelled as `None`.

use chrono::{DateTime, Utc};
use rusternetes_common::resources::{Pod, Service};
use std::collections::HashMap;
use std::sync::Mutex;

/// `ServiceKey` (`:53-57`): namespace and name of a Service.
type ServiceKey = (String, String);

/// `ServiceState` (`:60-68`): the state of a Service known to the tracker.
#[derive(Default, Clone)]
struct ServiceState {
    /// `lastServiceTriggerTime`: the service trigger time observed most recently.
    last_service_trigger_time: Option<DateTime<Utc>>,
    /// `lastPodTriggerTimes`: pod name -> trigger time observed during the
    /// most recent `compute_endpoint_last_change_trigger_time`.
    last_pod_trigger_times: HashMap<String, DateTime<Utc>>,
}

/// `TriggerTimeTracker` (`:36-44`).
#[derive(Default)]
pub struct TriggerTimeTracker {
    /// `ServiceStates`, behind `mutex`.
    service_states: Mutex<HashMap<ServiceKey, ServiceState>>,
}

impl TriggerTimeTracker {
    /// `NewTriggerTimeTracker` (`:46-51`).
    pub fn new() -> Self {
        Self::default()
    }

    /// `ComputeEndpointLastChangeTriggerTime` (`:70-129`). Updates the state of
    /// the Service being synced and returns the time to export as the
    /// annotation; `None` (a zero time) means the annotation must not be
    /// exported.
    ///
    /// Upstream guards only the map and relies on the caller never syncing the
    /// same key concurrently, which the controller's work queue guarantees.
    pub fn compute_endpoint_last_change_trigger_time(
        &self,
        namespace: &str,
        service: &Service,
        pods: &[&Pod],
    ) -> Option<DateTime<Utc>> {
        let key = (namespace.to_string(), service.metadata.name.clone());
        let (mut state, was_known) = {
            let guard = self.service_states.lock().unwrap();
            match guard.get(&key) {
                Some(s) => (s.clone(), true),
                None => (ServiceState::default(), false),
            }
        };

        // `minChangedTriggerTime`: the min of all trigger times that changed
        // since the last sync.
        let mut min_changed_trigger_time: Option<DateTime<Utc>> = None;
        let mut pod_trigger_times = HashMap::new();
        for pod in pods {
            if let Some(pod_trigger_time) = get_pod_trigger_time(pod) {
                pod_trigger_times.insert(pod.metadata.name.clone(), pod_trigger_time);
                // `podTriggerTime.After(state.lastPodTriggerTimes[pod.Name])`:
                // a pod never seen before compares against the zero time.
                let changed = state
                    .last_pod_trigger_times
                    .get(&pod.metadata.name)
                    .is_none_or(|last| pod_trigger_time > *last);
                if changed {
                    min_changed_trigger_time = min(min_changed_trigger_time, pod_trigger_time);
                }
            }
        }
        let service_trigger_time = get_service_trigger_time(service);
        if let Some(t) = service_trigger_time {
            if state.last_service_trigger_time.is_none_or(|last| t > last) {
                min_changed_trigger_time = min(min_changed_trigger_time, t);
            }
        }

        state.last_pod_trigger_times = pod_trigger_times;
        state.last_service_trigger_time = service_trigger_time;
        self.service_states.lock().unwrap().insert(key, state);

        if !was_known {
            // New Service, use Service creationTimestamp.
            return service.metadata.creation_timestamp;
        }
        // Regular update of endpoint objects, return min of changed trigger times.
        min_changed_trigger_time
    }

    /// `DeleteService` (`:131-137`).
    pub fn delete_service(&self, namespace: &str, name: &str) {
        self.service_states
            .lock()
            .unwrap()
            .remove(&(namespace.to_string(), name.to_string()));
    }
}

/// `getPodTriggerTime` (`:139-146`): the `lastTransitionTime` of the pod's
/// Ready condition (`getPodReadyCondition`).
fn get_pod_trigger_time(pod: &Pod) -> Option<DateTime<Utc>> {
    pod.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.condition_type == "Ready")?
        .last_transition_time
}

/// `getServiceTriggerTime` (`:148-152`).
fn get_service_trigger_time(service: &Service) -> Option<DateTime<Utc>> {
    service.metadata.creation_timestamp
}

/// `min` (`:154-160`): the smaller of the two, or `new_value` if `current_min`
/// is unset.
fn min(current_min: Option<DateTime<Utc>>, new_value: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match current_min {
        Some(c) if new_value >= c => Some(c),
        _ => Some(new_value),
    }
}

/// Go's `time.Time.UTC().Format(time.RFC3339Nano)`: the fractional seconds
/// are present only when non-zero and have trailing zeros trimmed.
pub fn format_rfc3339_nano(t: DateTime<Utc>) -> String {
    let base = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = t.timestamp_subsec_nanos();
    if nanos == 0 {
        return format!("{base}Z");
    }
    let frac = format!("{nanos:09}");
    format!("{base}.{}Z", frac.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    //! Ports of `trigger_time_tracker_test.go`.
    use super::*;
    use chrono::{Duration, TimeZone};
    use rusternetes_common::resources::{PodCondition, PodSpec, PodStatus, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;

    const NS: &str = "ttNamespace1";
    const SVC: &str = "my-service";

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs)
    }

    /// `createPod`.
    fn create_pod(name: &str, ready: DateTime<Utc>) -> Pod {
        // Struct-init (not `.status = Some(`): a test fixture, not a controller
        // writing status (tests/it/status_subresource_guard.rs).
        let base = Pod::new(name, PodSpec::default());
        let status = Some(PodStatus {
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
            status,
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
