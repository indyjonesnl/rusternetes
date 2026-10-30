//! Unit tests moved from `src/controllers/endpointslice_tracker.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::EndpointSlice;
    pub use std::collections::HashMap;
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::endpointslice_tracker::*;

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
