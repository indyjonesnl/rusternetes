//! Unit tests moved from `src/controllers/dynamic_provisioner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::{
        HostPathType, HostPathVolumeSource, PersistentVolumePhase, PersistentVolumeReclaimPolicy,
    };
    pub use rusternetes_common::resources::{
        EventSource, EventType, PersistentVolume, PersistentVolumeClaim, PersistentVolumeStatus,
        StorageClass,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
        PersistentVolumeMode, ResourceRequirements,
    };
    use rusternetes_controller_manager::controllers::dynamic_provisioner::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_is_provisioner_supported() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DynamicProvisionerController::new(storage);

        assert!(controller.is_provisioner_supported("rusternetes.io/hostpath"));
        assert!(controller.is_provisioner_supported("kubernetes.io/hostpath"));
        assert!(controller.is_provisioner_supported("hostpath"));
        assert!(!controller.is_provisioner_supported("kubernetes.io/aws-ebs"));
    }

    #[tokio::test]
    async fn test_create_pv_for_pvc() {
        // Use MemoryStorage for testing
        let storage = Arc::new(MemoryStorage::new());
        let controller = DynamicProvisionerController::new(storage);

        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "5Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new("test-pvc");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
                access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("fast".to_string()),
                volume_mode: Some(PersistentVolumeMode::Filesystem),
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Pending,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        };

        let storage_class = StorageClass {
            type_meta: TypeMeta {
                kind: "StorageClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("fast"),
            provisioner: "rusternetes.io/hostpath".to_string(),
            parameters: None,
            reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            volume_binding_mode: None,
            allowed_topologies: None,
            allow_volume_expansion: None,
            mount_options: None,
        };

        let pv = controller
            .create_pv_for_pvc(&storage_class, &pvc, "pvc-default-test-pvc")
            .await
            .unwrap();

        // Verify PV metadata
        assert_eq!(pv.metadata.name, "pvc-default-test-pvc");
        assert_eq!(pv.spec.storage_class_name, Some("fast".to_string()));
        assert_eq!(pv.spec.capacity.get("storage"), Some(&"5Gi".to_string()));
        assert_eq!(
            pv.spec.persistent_volume_reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Delete)
        );
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteOnce]
        );
        assert_eq!(
            pv.status.as_ref().unwrap().phase,
            PersistentVolumePhase::Available
        );

        // Verify hostpath volume source
        let hp = pv
            .spec
            .host_path
            .as_ref()
            .expect("Expected HostPath volume source");
        assert_eq!(hp.path, "/tmp/rusternetes/dynamic-pvs/pvc-default-test-pvc");
        assert_eq!(hp.r#type, Some(HostPathType::DirectoryOrCreate));

        // Verify labels
        let labels = pv.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("pvc-name"), Some(&"test-pvc".to_string()));
        assert_eq!(labels.get("pvc-namespace"), Some(&"default".to_string()));
        assert_eq!(
            labels.get("provisioner"),
            Some(&"rusternetes.io/hostpath".to_string())
        );
        assert_eq!(labels.get("storage-class"), Some(&"fast".to_string()));

        // Verify annotations
        let annotations = pv.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations.get("pv.kubernetes.io/provisioned-by"),
            Some(&"rusternetes.io/hostpath".to_string())
        );

        // #1095: the PV is pre-bound — claimRef names the exact PVC it was
        // provisioned for, so the binder can't cross-bind it to another PVC.
        let cr = pv.spec.claim_ref.as_ref().expect("claimRef must be set");
        assert_eq!(cr.kind.as_deref(), Some("PersistentVolumeClaim"));
        assert_eq!(cr.namespace.as_deref(), Some("default"));
        assert_eq!(cr.name.as_deref(), Some("test-pvc"));
        assert_eq!(cr.uid.as_ref(), Some(&pvc.metadata.uid));
    }
}
