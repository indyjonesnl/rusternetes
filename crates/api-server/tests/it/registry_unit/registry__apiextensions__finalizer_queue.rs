//! Unit tests moved from `src/registry/apiextensions/finalizer_queue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::apiextensions::condition_controllers::remove_crd_condition;
    pub use rusternetes_api_server::registry::apiextensions::controllers::{
        crd_has_finalizer, TERMINATING,
    };
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;
    pub use rusternetes_common::resources::CustomResourceDefinition;
    pub use rusternetes_common::validation::crd::CUSTOM_RESOURCE_CLEANUP_FINALIZER;
    pub use std::collections::{HashSet, VecDeque};
    pub use std::sync::{Arc, Mutex};
    pub use tokio::sync::Notify;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::controllers::{
        condition, set_crd_condition,
    };
    use rusternetes_api_server::registry::apiextensions::finalizer_queue::*;

    fn deleting() -> CustomResourceDefinition {
        let mut c = CustomResourceDefinition::new("x", "example.com", "", "widgets");
        c.metadata.name = "widgets.example.com".into();
        c.metadata.resource_version = Some("1".into());
        c.metadata.deletion_timestamp = Some(chrono::Utc::now());
        c.metadata.finalizers = Some(vec![CUSTOM_RESOURCE_CLEANUP_FINALIZER.into()]);
        c
    }

    /// `OverlappingBuiltInResources` (crd_finalizer.go:50-56).
    #[test]
    fn overlapping_built_in_resources_match_upstream() {
        assert!(overlapping_built_in_resource(
            "apiregistration.k8s.io",
            "apiservices"
        ));
        assert!(overlapping_built_in_resource(
            "apiextensions.k8s.io",
            "customresourcedefinitions"
        ));
        assert!(!overlapping_built_in_resource("example.com", "widgets"));
        assert!(!overlapping_built_in_resource(
            "apiextensions.k8s.io",
            "apiservices"
        ));
    }

    /// `AddFunc` queues only deleted CRDs that still hold the finalizer.
    #[test]
    fn add_event_queues_only_deleted_crds_with_the_finalizer() {
        assert!(add_event_enqueues(&deleting()));
        let mut live = deleting();
        live.metadata.deletion_timestamp = None;
        assert!(!add_event_enqueues(&live));
        let mut done = deleting();
        done.metadata.finalizers = Some(vec![]);
        assert!(!add_event_enqueues(&done));
    }

    /// `UpdateFunc`: resyncs always, other changes when more than the
    /// Terminating condition differs.
    #[test]
    fn update_event_filters_like_upstream() {
        let old = deleting();
        // Resync: same resourceVersion.
        assert!(update_event_enqueues(&old, &old));
        // Only the Terminating condition (and the rv) changed: no requeue.
        let mut only_terminating = old.clone();
        only_terminating.metadata.resource_version = Some("2".into());
        set_crd_condition(
            &mut only_terminating,
            condition(TERMINATING, "True", "InstanceDeletionInProgress", "x"),
        );
        assert!(!update_event_enqueues(&old, &only_terminating));
        // Another condition changed: requeue.
        let mut other = only_terminating.clone();
        set_crd_condition(&mut other, condition("Established", "True", "r", "m"));
        assert!(update_event_enqueues(&old, &other));
        // Finalized by us, or not deleted: never.
        let mut finalized = other.clone();
        finalized.metadata.finalizers = Some(vec![]);
        assert!(!update_event_enqueues(&old, &finalized));
        let mut live = other.clone();
        live.metadata.deletion_timestamp = None;
        assert!(!update_event_enqueues(&old, &live));
    }

    /// workqueue semantics (queue.go): dedup while queued, one worker per
    /// key, a re-add while processing is queued on `Done`.
    #[test]
    fn queue_dedups_serialises_a_key_and_requeues_on_done() {
        let q = FinalizerQueue::default();
        q.add("a");
        q.add("a");
        q.add("b");
        assert_eq!(q.get().as_deref(), Some("a"));
        // "a" is processing: added again, it waits for Done.
        q.add("a");
        assert_eq!(q.get().as_deref(), Some("b"));
        assert_eq!(q.get(), None);
        q.done("a");
        assert_eq!(q.get().as_deref(), Some("a"));
        q.done("b");
        q.done("a");
        assert_eq!(q.get(), None);
    }

    /// `AddRateLimited` re-adds the key after the item limiter's delay, and
    /// `Forget` resets it (default_rate_limiters.go).
    #[tokio::test(start_paused = true)]
    async fn add_rate_limited_requeues_after_the_backoff() {
        let q = Arc::new(FinalizerQueue::default());
        q.add_rate_limited("a");
        assert_eq!(q.get(), None);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(q.get().as_deref(), Some("a"));
        q.forget("a");
    }
}
