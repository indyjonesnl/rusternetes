//! Unit tests moved from `src/resources/volume.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::serde_helpers::empty_string_as_none;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::volume::*;

    // #2562: upstream's volumeMode / reclaimPolicy / volumeBindingMode are
    // pointers (core/v1/types.go:427,578; storage/v1/types.go:56,73), so a
    // present "" is not nil: it is kept and validation answers NotSupported.
    #[test]
    fn pointer_enums_keep_an_empty_string() {
        let pv: PersistentVolumeSpec =
            serde_json::from_value(serde_json::json!({"volumeMode": ""})).unwrap();
        assert_eq!(
            pv.volume_mode,
            Some(PersistentVolumeMode::Unknown(String::new()))
        );
        let pvc: PersistentVolumeClaimSpec =
            serde_json::from_value(serde_json::json!({"volumeMode": ""})).unwrap();
        assert_eq!(
            pvc.volume_mode,
            Some(PersistentVolumeMode::Unknown(String::new()))
        );
        let sc: StorageClass = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "sc"}, "provisioner": "example.com/p",
            "reclaimPolicy": "", "volumeBindingMode": ""
        }))
        .unwrap();
        assert_eq!(
            sc.reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Unknown(String::new()))
        );
        assert_eq!(
            sc.volume_binding_mode,
            Some(VolumeBindingMode::Unknown(String::new()))
        );
        // An absent field is still nil, and round-trips as absent.
        let sc: StorageClass = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "sc"}, "provisioner": "example.com/p"
        }))
        .unwrap();
        assert!(sc.reclaim_policy.is_none() && sc.volume_binding_mode.is_none());
        let back = serde_json::to_value(&sc).unwrap();
        assert!(back.get("reclaimPolicy").is_none());
    }

    #[test]
    fn storage_class_empty_pointer_enums_are_not_supported() {
        let sc: StorageClass = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "sc"}, "provisioner": "example.com/p",
            "reclaimPolicy": "", "volumeBindingMode": ""
        }))
        .unwrap();
        let errs = rusternetes_common::validation::storageclass::validate_storage_class(&sc);
        let fields: Vec<String> = errs.iter().map(|e| e.field.to_string()).collect();
        assert!(
            fields.contains(&"volumeBindingMode".to_string()),
            "{fields:?}"
        );
        assert!(fields.contains(&"reclaimPolicy".to_string()), "{fields:?}");
    }

    #[test]
    fn test_pv_serialization() {
        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "10Gi".to_string());

        let pv = PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pv"),
            spec: PersistentVolumeSpec {
                capacity,
                host_path: Some(HostPathVolumeSource {
                    path: "/mnt/data".to_string(),
                    r#type: Some(HostPathType::DirectoryOrCreate),
                }),
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                gce_persistent_disk: None,
                aws_elastic_block_store: None,
                fc: None,
                more_sources: Default::default(),
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Retain),
                storage_class_name: Some("manual".to_string()),
                mount_options: None,
                volume_mode: Some(PersistentVolumeMode::Filesystem),
                node_affinity: None,
                claim_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        };

        let json = serde_json::to_string(&pv).unwrap();
        let _deserialized: PersistentVolume = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_pvc_serialization() {
        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "5Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pvc").with_namespace("default"),
            spec: PersistentVolumeClaimSpec {
                access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("manual".to_string()),
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

        let json = serde_json::to_string(&pvc).unwrap();
        let _deserialized: PersistentVolumeClaim = serde_json::from_str(&json).unwrap();
    }

    /// `core/v1 PersistentVolumeSource.fc` (`types.go`, `FCVolumeSource`): a
    /// PV carrying a FibreChannel source must keep it (#3288); the kubelet's
    /// `kubernetes.io/fc` plugin reads `spec.fc`.
    #[test]
    fn pv_spec_keeps_fc_source() {
        let fc = serde_json::json!({
            "targetWWNs": ["500a0981891b8dc5"],
            "lun": 0,
            "fsType": "ext4",
            "readOnly": true,
            "wwids": ["3600508b400105e210000900000490000"]
        });
        let pv: PersistentVolume = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "pv-fc"},
            "spec": {"fc": fc}
        }))
        .unwrap();
        let out = serde_json::to_value(&pv).unwrap();
        assert_eq!(out["spec"]["fc"], fc);
    }
}
