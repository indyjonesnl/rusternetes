//! Unit tests moved from `src/controllers/resourceclaim.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::{
        AllocationResult, DeviceAllocationResult, DeviceClass, DeviceRequestAllocationResult,
        DeviceToleration, ResourceClaim, ResourceClaimStatus, ResourceSlice,
    };
    pub use rusternetes_controller_manager::controllers::device_taint_eviction::tolerates_taint;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{DeviceClaim, DeviceClassSpec, ResourceClaimSpec};
    use rusternetes_controller_manager::controllers::resourceclaim::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_device_matching_no_selectors() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage);

        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: rusternetes_common::resources::dra::ObjectMeta {
                name: "test-class".to_string(),
                ..Default::default()
            },
            spec: DeviceClassSpec {
                selectors: vec![],
                config: vec![],
                suitable_nodes: None,
            },
        };

        // Should match any device when no selectors
        assert!(controller.device_matches_class("gpu-0", &device_class));
        assert!(controller.device_matches_class("any-device", &device_class));
    }

    #[tokio::test]
    async fn test_reconcile_already_allocated() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage.clone());

        let mut claim = ResourceClaim {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceClaim".to_string(),
            metadata: rusternetes_common::resources::dra::ObjectMeta {
                name: "test-claim".to_string(),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: ResourceClaimSpec {
                devices: DeviceClaim::default(),
            },
            status: Some(ResourceClaimStatus {
                allocation: Some(AllocationResult {
                    devices: DeviceAllocationResult {
                        results: vec![],
                        config: vec![],
                    },
                    node_selector: None,
                }),
                devices: vec![],
                reserved_for: vec![],
                deallocation_requested: None,
            }),
        };

        // Should skip already allocated claims
        let result = controller.reconcile_claim(&mut claim).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_suitable_devices_empty_slices() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage);

        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: rusternetes_common::resources::dra::ObjectMeta {
                name: "test-class".to_string(),
                ..Default::default()
            },
            spec: DeviceClassSpec::default(),
        };

        let devices = controller
            .find_suitable_devices(&device_class, Some(1), &[])
            .await
            .unwrap();
        assert_eq!(devices.len(), 0);
    }

    // Ports the toleration cases of `allocatortesting` TestAllocator
    // (allocator_testing.go:3738 tolerated taint, :3790 tolerations in result).
    async fn run_allocation(
        taints: serde_json::Value,
        tolerations: serde_json::Value,
    ) -> Vec<DeviceRequestAllocationResult> {
        let storage = Arc::new(MemoryStorage::new());
        let class: DeviceClass = serde_json::from_value(serde_json::json!({
            "apiVersion": "resource.k8s.io/v1", "kind": "DeviceClass",
            "metadata": {"name": "class-a"}, "spec": {}
        }))
        .unwrap();
        storage
            .create(&build_key("deviceclasses", None, "class-a"), &class)
            .await
            .unwrap();
        let slice: ResourceSlice = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "slice-0"},
            "spec": {"driver": "drv", "nodeName": "n1",
                "pool": {"name": "n1", "resourceSliceCount": 1},
                "devices": [{"name": "dev-0", "taints": taints}]}
        }))
        .unwrap();
        storage
            .create(&build_key("resourceslices", None, "slice-0"), &slice)
            .await
            .unwrap();
        let mut claim: ResourceClaim = serde_json::from_value(serde_json::json!({
            "apiVersion": "resource.k8s.io/v1",
            "kind": "ResourceClaim",
            "metadata": {"name": "claim-0", "namespace": "default"},
            "spec": {"devices": {"requests": [{
                "name": "req-0",
                "exactly": {
                    "deviceClassName": "class-a",
                    "count": 1,
                    "tolerations": tolerations,
                }
            }]}}
        }))
        .unwrap();
        storage
            .create(
                &build_key("resourceclaims", Some("default"), "claim-0"),
                &claim,
            )
            .await
            .unwrap();
        let controller = ResourceClaimController::new(storage.clone());
        controller.reconcile_claim(&mut claim).await.unwrap();
        let stored: ResourceClaim = storage
            .get(&build_key("resourceclaims", Some("default"), "claim-0"))
            .await
            .unwrap();
        stored
            .status
            .and_then(|s| s.allocation)
            .map(|a| a.devices.results)
            .unwrap_or_default()
    }

    fn no_execute_taint() -> serde_json::Value {
        serde_json::json!([{"key": "example.com/taint", "value": "tainted", "effect": "NoExecute"}])
    }

    #[tokio::test]
    async fn allocation_copies_request_tolerations_into_result() {
        let tol = serde_json::json!([
            {"key": "example.com/taint", "operator": "Exists", "effect": "NoExecute",
             "tolerationSeconds": 60}
        ]);
        let results = run_allocation(no_execute_taint(), tol).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].device, "dev-0");
        assert_eq!(results[0].tolerations.len(), 1);
        assert_eq!(results[0].tolerations[0].toleration_seconds, Some(60));
    }

    #[tokio::test]
    async fn allocation_skips_device_with_untolerated_taint() {
        assert!(run_allocation(no_execute_taint(), serde_json::json!([]))
            .await
            .is_empty());
        let other = serde_json::json!([{"key": "other", "operator": "Exists"}]);
        assert!(run_allocation(no_execute_taint(), other).await.is_empty());
    }

    #[tokio::test]
    async fn allocation_untainted_device_has_empty_tolerations() {
        let results = run_allocation(serde_json::json!([]), serde_json::json!([])).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].tolerations.is_empty());
    }
}
