//! Port of `EndpointSliceTracker`
//! (`staging/src/k8s.io/endpointslice/util/endpointslice_tracker.go`).
//!
//! Tracks the generation of every EndpointSlice the controller itself wrote
//! (and the deletions it expects), so the slice watch can tell the
//! controller's own writes apart from edits made by anyone else.
//!
//! `StaleSlices` (`:89-116`) is not ported: it detects an out-of-date
//! informer cache, and this controller reads slices straight from storage
//! rather than through a lagging cache, so the condition cannot arise.

use rusternetes_common::resources::EndpointSlice;
use std::collections::HashMap;
use std::sync::Mutex;

/// `deletionExpected` (`endpointslice_tracker.go:28`).
const DELETION_EXPECTED: i64 = -1;

/// `GenerationsBySlice`: expected generation by slice UID.
type GenerationsBySlice = HashMap<String, i64>;

/// `types.NamespacedName` of the Service a slice belongs to (`getServiceNN`,
/// `:188-191`): the `kubernetes.io/service-name` label, which may be empty.
fn service_nn(slice: &EndpointSlice) -> (String, String) {
    let name = slice
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("kubernetes.io/service-name"))
        .cloned()
        .unwrap_or_default();
    (slice.metadata.namespace.clone().unwrap_or_default(), name)
}

fn generation(slice: &EndpointSlice) -> i64 {
    slice.metadata.generation.unwrap_or(0)
}

#[derive(Default)]
pub struct EndpointSliceTracker {
    /// `generationsByService`, behind `lock`.
    generations_by_service: Mutex<HashMap<(String, String), GenerationsBySlice>>,
}

impl EndpointSliceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Has` (`:56-66`).
    pub fn has(&self, slice: &EndpointSlice) -> bool {
        let guard = self.generations_by_service.lock().unwrap();
        guard
            .get(&service_nn(slice))
            .is_some_and(|gfs| gfs.contains_key(&slice.metadata.uid))
    }

    /// `ShouldSync` (`:71-81`).
    pub fn should_sync(&self, slice: &EndpointSlice) -> bool {
        let guard = self.generations_by_service.lock().unwrap();
        let Some(gfs) = guard.get(&service_nn(slice)) else {
            return true;
        };
        match gfs.get(&slice.metadata.uid) {
            None => true,
            Some(g) => generation(slice) > *g,
        }
    }

    /// `Update` (`:120-131`).
    pub fn update(&self, slice: &EndpointSlice) {
        let mut guard = self.generations_by_service.lock().unwrap();
        guard
            .entry(service_nn(slice))
            .or_default()
            .insert(slice.metadata.uid.clone(), generation(slice));
    }

    /// `DeleteService` (`:134-140`).
    pub fn delete_service(&self, namespace: &str, name: &str) {
        let mut guard = self.generations_by_service.lock().unwrap();
        guard.remove(&(namespace.to_string(), name.to_string()));
    }

    /// `ExpectDeletion` (`:144-155`).
    pub fn expect_deletion(&self, slice: &EndpointSlice) {
        let mut guard = self.generations_by_service.lock().unwrap();
        guard
            .entry(service_nn(slice))
            .or_default()
            .insert(slice.metadata.uid.clone(), DELETION_EXPECTED);
    }

    /// `HandleDeletion` (`:160-175`): forgets the slice; `true` when the
    /// tracker expected the deletion (or knew nothing of the slice's
    /// service), `false` when it was a surprise.
    pub fn handle_deletion(&self, slice: &EndpointSlice) -> bool {
        let mut guard = self.generations_by_service.lock().unwrap();
        if let Some(gfs) = guard.get_mut(&service_nn(slice)) {
            if let Some(g) = gfs.remove(&slice.metadata.uid) {
                if g != DELETION_EXPECTED {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(svc: &str, uid: &str, generation: i64) -> EndpointSlice {
        let mut es = EndpointSlice::new(format!("{svc}-{uid}"), "IPv4");
        es.metadata.namespace = Some("ns".to_string());
        es.metadata.uid = uid.to_string();
        es.metadata.generation = Some(generation);
        es.metadata.labels = Some(HashMap::from([(
            "kubernetes.io/service-name".to_string(),
            svc.to_string(),
        )]));
        es
    }

    /// `TestEndpointSliceTrackerUpdate` (endpointslice_tracker_test.go:29).
    #[test]
    fn update_then_should_sync_only_for_newer_generation() {
        let t = EndpointSliceTracker::new();
        let s = slice("svc", "a", 1);
        assert!(t.should_sync(&s), "untracked slice must sync");
        assert!(!t.has(&s));
        t.update(&s);
        assert!(t.has(&s));
        assert!(!t.should_sync(&s), "own write at same generation");
        assert!(!t.should_sync(&slice("svc", "a", 0)), "older generation");
        assert!(t.should_sync(&slice("svc", "a", 2)), "external edit");
        assert!(t.should_sync(&slice("svc", "b", 1)), "other uid untracked");
    }

    /// `TestEndpointSliceTrackerDeletion` (endpointslice_tracker_test.go:226).
    #[test]
    fn handle_deletion_reports_unexpected_deletes() {
        let t = EndpointSliceTracker::new();
        let s = slice("svc", "a", 1);
        // Unknown service: expected (nothing to repair).
        assert!(t.handle_deletion(&s));
        t.update(&s);
        assert!(!t.handle_deletion(&s), "tracked slice deleted by someone");
        assert!(!t.has(&s), "handle_deletion forgets the slice");
        t.update(&s);
        t.expect_deletion(&s);
        assert!(t.handle_deletion(&s), "our own delete");
        assert!(!t.has(&s));
    }

    /// `TestEndpointSliceTrackerDeleteService` (endpointslice_tracker_test.go:337).
    #[test]
    fn delete_service_drops_every_slice_of_it() {
        let t = EndpointSliceTracker::new();
        let s = slice("svc", "a", 1);
        t.update(&s);
        t.update(&slice("other", "b", 1));
        t.delete_service("ns", "svc");
        assert!(!t.has(&s));
        assert!(t.has(&slice("other", "b", 1)));
    }
}
