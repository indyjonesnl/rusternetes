//! Unit tests moved from `src/registry/apiextensions/controller_queue.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::apiextensions::condition_controllers::ConditionControllers;
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::{
        ControllerRateLimiter, CrdRest, CRD_FINALIZER_WORKERS,
    };
    pub use rusternetes_api_server::registry::apiextensions::finalizer_queue::FinalizerQueue;
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::{HashSet, VecDeque};
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use tokio::sync::Notify;
    pub use tracing::warn;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::controller_queue::*;

    /// Each condition controller has a workqueue of its own
    /// (nonstructuralschema_controller.go:66-69, apiapproval_controller.go:
    /// 66-69): an `Add` reaches both, dedups per queue, and is independent of
    /// the naming/establishing queue.
    #[test]
    fn condition_controllers_have_their_own_queues() {
        let c = CrdControllers::default();
        c.add_conditions("a.example.com");
        c.add_conditions("a.example.com");
        assert_eq!(c.non_structural.get().as_deref(), Some("a.example.com"));
        assert_eq!(c.non_structural.get(), None);
        assert_eq!(c.api_approval.get().as_deref(), Some("a.example.com"));
        assert_eq!(c.api_approval.get(), None);
        assert_eq!(c.pop(), None, "the naming queue was not fed");
    }
}
