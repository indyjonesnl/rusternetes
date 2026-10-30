//! Unit tests moved from `src/controllers/loadbalancer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_common::{
        cloud_provider::{CloudProvider, LoadBalancerPort, LoadBalancerService as CloudLBService},
        resources::{
            service::{LoadBalancerIngress, LoadBalancerStatus, ServiceStatus},
            Event, EventType, Node, ObjectReference, Service, ServiceType,
        },
    };
    pub use rusternetes_storage::event_recorder::process_event_ttl;
    pub use rusternetes_storage::{extract_key, Storage, WorkQueue};
    pub use std::collections::HashSet;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::loadbalancer::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_controller_creation() {
        // Test that we can create a controller without cloud provider
        let storage = Arc::new(MemoryStorage::new());
        let controller = LoadBalancerController::new(storage, None, "test-cluster".to_string(), 30);

        assert_eq!(controller.cluster_name, "test-cluster");
        assert!(controller.cloud_provider.is_none());
    }
}
