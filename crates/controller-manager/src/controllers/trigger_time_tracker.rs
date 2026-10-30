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

    /// Test-only view of `ServiceStates` (upstream's test reads the exported
    /// map directly: `endpointslice_controller_test.go:340-347`).
    pub fn has_service(&self, namespace: &str, name: &str) -> bool {
        self.service_states
            .lock()
            .unwrap()
            .contains_key(&(namespace.to_string(), name.to_string()))
    }

    /// Test-only: `ServiceStates[key] = ServiceState{}`.
    pub fn insert_empty_service(&self, namespace: &str, name: &str) {
        self.service_states.lock().unwrap().insert(
            (namespace.to_string(), name.to_string()),
            Default::default(),
        );
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
