//! Unit tests moved from `src/resources/dra.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::serde_helpers::empty_string_as_none;
    pub use rusternetes_common::resources::NodeSelector;
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::dra::*;

    /// Upstream wire shape: `capacity: {requests: {name: quantity}}` on both
    /// ExactDeviceRequest and DeviceSubRequest (types.go:991, 1106).
    #[test]
    fn capacity_requirements_roundtrip_upstream_json() {
        let j = serde_json::json!({"requests": {"example.com/mem": "2Gi"}});
        let c: CapacityRequirements = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(c.requests["example.com/mem"], "2Gi");
        assert_eq!(serde_json::to_value(&c).unwrap(), j);

        let e: ExactDeviceRequest = serde_json::from_value(serde_json::json!({
            "name": "r", "deviceClassName": "c", "capacity": j
        }))
        .unwrap();
        assert_eq!(e.capacity.unwrap().requests.len(), 1);
        let s: DeviceSubRequest = serde_json::from_value(serde_json::json!({
            "name": "r", "deviceClassName": "c", "capacity": j
        }))
        .unwrap();
        assert_eq!(serde_json::to_value(s.capacity.unwrap()).unwrap(), j);
    }

    #[test]
    fn test_resource_claim_serialization() {
        let claim = ResourceClaim {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceClaim".to_string(),
            metadata: ObjectMeta {
                name: "test-claim".to_string(),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: ResourceClaimSpec {
                devices: DeviceClaim {
                    requests: vec![DeviceRequest {
                        name: "req1".to_string(),
                        exactly: Some(ExactDeviceRequest {
                            device_class_name: "gpu".to_string(),
                            selectors: vec![],
                            allocation_mode: Some(DeviceAllocationMode::ExactCount),
                            count: Some(1),
                            admin_access: None,
                            tolerations: vec![],
                            capacity: None,
                        }),
                        first_available: vec![],
                    }],
                    constraints: vec![],
                    config: vec![],
                },
            },
            status: None,
        };

        let json = serde_json::to_string(&claim).unwrap();
        let deserialized: ResourceClaim = serde_json::from_str(&json).unwrap();
        assert_eq!(claim, deserialized);
    }

    #[test]
    fn test_device_class_serialization() {
        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: ObjectMeta {
                name: "gpu-class".to_string(),
                ..Default::default()
            },
            spec: DeviceClassSpec {
                selectors: vec![],
                config: vec![],
                suitable_nodes: None,
            },
        };

        let json = serde_json::to_string(&device_class).unwrap();
        let deserialized: DeviceClass = serde_json::from_str(&json).unwrap();
        assert_eq!(device_class, deserialized);
    }

    #[test]
    fn test_resource_slice_serialization() {
        let slice = ResourceSlice {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceSlice".to_string(),
            metadata: ObjectMeta {
                name: "test-slice".to_string(),
                ..Default::default()
            },
            spec: ResourceSliceSpec {
                driver: "test.driver".to_string(),
                pool: ResourcePool {
                    name: "pool1".to_string(),
                    generation: 1,
                    resource_slice_count: 1,
                },
                node_name: Some("node1".to_string()),
                node_selector: None,
                all_nodes: None,
                devices: vec![],
                per_device_node_selection: None,
                shared_counters: vec![],
            },
        };

        let json = serde_json::to_string(&slice).unwrap();
        let deserialized: ResourceSlice = serde_json::from_str(&json).unwrap();
        assert_eq!(slice, deserialized);
    }
    /// Upstream JSON names (staging/src/k8s.io/api/resource/v1/types.go:
    /// `shareID` :1686, `consumedCapacity` :1699, `bindingConditions` :1662,
    /// `bindingFailureConditions` :1675, `distinctAttribute` :1313).
    #[test]
    fn gated_dra_fields_use_upstream_json_names() {
        let r: DeviceRequestAllocationResult = serde_json::from_value(serde_json::json!({
            "request": "r", "driver": "d.example.com", "pool": "p", "device": "x",
            "shareID": "11111111-1111-1111-1111-111111111111",
            "consumedCapacity": {"memory": "1Gi"},
            "bindingConditions": ["a"],
            "bindingFailureConditions": ["b"]
        }))
        .unwrap();
        assert_eq!(
            r.share_id.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        assert_eq!(r.consumed_capacity.as_ref().unwrap()["memory"], "1Gi");
        assert_eq!(r.binding_conditions, vec!["a"]);
        assert_eq!(r.binding_failure_conditions, vec!["b"]);
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["shareID"], "11111111-1111-1111-1111-111111111111");
        assert_eq!(v["consumedCapacity"]["memory"], "1Gi");
        assert_eq!(v["bindingConditions"][0], "a");
        assert_eq!(v["bindingFailureConditions"][0], "b");

        let s: AllocatedDeviceStatus = serde_json::from_value(
            serde_json::json!({"driver": "d", "pool": "p", "device": "x", "shareID": "u"}),
        )
        .unwrap();
        assert_eq!(s.share_id.as_deref(), Some("u"));
        assert_eq!(serde_json::to_value(&s).unwrap()["shareID"], "u");

        let c: DeviceConstraint =
            serde_json::from_value(serde_json::json!({"distinctAttribute": "a.example.com/b"}))
                .unwrap();
        assert_eq!(c.distinct_attribute.as_deref(), Some("a.example.com/b"));
        assert_eq!(
            serde_json::to_value(&c).unwrap()["distinctAttribute"],
            "a.example.com/b"
        );

        let sub: DeviceSubRequest = serde_json::from_value(
            serde_json::json!({"name": "n", "deviceClassName": "c", "capacity": {"memory": {"value": "1"}}}),
        )
        .unwrap();
        assert!(sub.capacity.is_some());
    }
}
