//! Unit tests moved from `src/resources/csi.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::LabelSelector;
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::csi::*;

    #[test]
    fn test_csi_driver_serialization() {
        let csi_driver = CSIDriver {
            type_meta: TypeMeta {
                kind: "CSIDriver".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-driver"),
            spec: CSIDriverSpec {
                attach_required: Some(true),
                pod_info_on_mount: Some(false),
                fs_group_policy: Some(FSGroupPolicy::ReadWriteOnceWithFSType),
                storage_capacity: Some(true),
                volume_lifecycle_modes: Some(vec![VolumeLifecycleMode::Persistent]),
                token_requests: None,
                requires_republish: Some(false),
                se_linux_mount: Some(false),
                node_allocatable_update_period_seconds: None,
                ..Default::default()
            },
        };

        let json = serde_json::to_string(&csi_driver).unwrap();
        let deserialized: CSIDriver = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test-driver");
    }

    #[test]
    fn test_csi_node_serialization() {
        let csi_node = CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("node1"),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: "test-driver".to_string(),
                    node_id: "node1-id".to_string(),
                    topology_keys: Some(vec!["topology.kubernetes.io/zone".to_string()]),
                    allocatable: Some(VolumeNodeResources { count: Some(100) }),
                }],
            },
        };

        let json = serde_json::to_string(&csi_node).unwrap();
        let deserialized: CSINode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spec.drivers.len(), 1);
    }

    #[test]
    fn test_volume_attachment_serialization() {
        let va = VolumeAttachment {
            type_meta: TypeMeta {
                kind: "VolumeAttachment".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-va"),
            spec: VolumeAttachmentSpec {
                attacher: "test-driver".to_string(),
                node_name: "node1".to_string(),
                source: VolumeAttachmentSource {
                    persistent_volume_name: Some("pv-123".to_string()),
                    inline_volume_spec: None,
                },
            },
            status: Some(VolumeAttachmentStatus {
                attached: true,
                attachment_metadata: None,
                attach_error: None,
                detach_error: None,
            }),
        };

        let json = serde_json::to_string(&va).unwrap();
        let deserialized: VolumeAttachment = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spec.attacher, "test-driver");
    }

    #[test]
    fn test_csi_storage_capacity_serialization() {
        let csc = CSIStorageCapacity {
            type_meta: TypeMeta {
                kind: "CSIStorageCapacity".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-csc").with_namespace("default"),
            storage_class_name: "fast-ssd".to_string(),
            capacity: Some("100Gi".to_string()),
            maximum_volume_size: Some("10Gi".to_string()),
            node_topology: None,
        };

        let json = serde_json::to_string(&csc).unwrap();
        let deserialized: CSIStorageCapacity = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.storage_class_name, "fast-ssd");
    }

    #[test]
    fn test_volume_attributes_class_serialization() {
        let mut params = HashMap::new();
        params.insert("type".to_string(), "ssd".to_string());
        params.insert("iops".to_string(), "3000".to_string());

        let vac = VolumeAttributesClass {
            type_meta: TypeMeta {
                kind: "VolumeAttributesClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("fast-storage"),
            driver_name: "test-driver".to_string(),
            parameters: Some(params),
        };

        let json = serde_json::to_string(&vac).unwrap();
        let deserialized: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.driver_name, "test-driver");
        assert_eq!(
            deserialized
                .parameters
                .as_ref()
                .unwrap()
                .get("type")
                .unwrap(),
            "ssd"
        );
    }
}
