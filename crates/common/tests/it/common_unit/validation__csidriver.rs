//! Unit tests moved from `src/validation/csidriver.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::csi::{CSIDriver, FSGroupPolicy, VolumeLifecycleMode};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::validate_immutable_field;
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::csi::{CSIDriverSpec, TokenRequest};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_common::validation::csidriver::*;
    use rusternetes_common::validation::field::ErrorType;

    /// A spec that passes every create-time rule, so each test can mutate one
    /// field and assert the single resulting error.
    fn valid_spec() -> CSIDriverSpec {
        CSIDriverSpec {
            attach_required: Some(true),
            pod_info_on_mount: Some(false),
            storage_capacity: Some(true),
            se_linux_mount: Some(false),
            node_allocatable_update_period_seconds: None,
            service_account_token_in_secrets: None,
            ..Default::default()
        }
    }

    fn driver(spec: CSIDriverSpec) -> CSIDriver {
        CSIDriver {
            type_meta: TypeMeta {
                kind: "CSIDriver".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-driver"),
            spec,
        }
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    #[test]
    fn fully_valid_spec_passes() {
        assert!(validate_csi_driver(&driver(valid_spec())).is_empty());
    }

    #[test]
    fn pod_info_on_mount_required() {
        let mut spec = valid_spec();
        spec.pod_info_on_mount = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(&errs, "spec.podInfoOnMount", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn storage_capacity_required() {
        let mut spec = valid_spec();
        spec.storage_capacity = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(&errs, "spec.storageCapacity", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn node_allocatable_update_period_below_min_invalid() {
        let mut spec = valid_spec();
        spec.node_allocatable_update_period_seconds = Some(9);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.nodeAllocatableUpdatePeriodSeconds",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }

    #[test]
    fn node_allocatable_update_period_at_min_ok() {
        let mut spec = valid_spec();
        spec.node_allocatable_update_period_seconds = Some(10);
        assert!(validate_csi_driver(&driver(spec)).is_empty());
    }

    #[test]
    fn service_account_token_in_secrets_without_token_requests_invalid() {
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(true);
        spec.token_requests = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );

        // Empty (non-nil) tokenRequests is treated the same as nil upstream.
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(false);
        spec.token_requests = Some(vec![]);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }

    #[test]
    fn service_account_token_in_secrets_with_token_requests_ok() {
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(true);
        spec.token_requests = Some(vec![TokenRequest {
            audience: "aud".to_string(),
            expiration_seconds: None,
        }]);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            !has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }

    /// validation.go:505-516 `validateFSGroupPolicy`: an unknown value decodes
    /// (Go string) and is NotSupported, sorted like `supportedFSGroupPolicy.List()`.
    #[test]
    fn unknown_fs_group_policy_is_not_supported() {
        let spec: CSIDriverSpec = serde_json::from_value(serde_json::json!({
            "attachRequired": false, "podInfoOnMount": false,
            "storageCapacity": true, "seLinuxMount": false,
            "fsGroupPolicy": "invalid-mode"
        }))
        .expect("an unknown fsGroupPolicy must decode");
        let errs = validate_csi_driver(&driver(spec));
        assert!(has(&errs, "spec.fsGroupPolicy", ErrorType::NotSupported));
        assert!(
            errs[0]
                .to_string()
                .contains("\"File\", \"None\", \"ReadWriteOnceWithFSType\""),
            "{}",
            errs[0]
        );
    }

    /// validation.go:549-563 `validateVolumeLifecycleModes`: one NotSupported
    /// per bad entry, listing `Persistent, Ephemeral` in that order.
    #[test]
    fn unknown_volume_lifecycle_mode_is_not_supported() {
        let spec: CSIDriverSpec = serde_json::from_value(serde_json::json!({
            "attachRequired": false, "podInfoOnMount": false,
            "storageCapacity": true, "seLinuxMount": false,
            "volumeLifecycleModes": ["Persistent", "no-such-mode"]
        }))
        .expect("an unknown volumeLifecycleModes entry must decode");
        let errs = validate_csi_driver(&driver(spec));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(has(
            &errs,
            "spec.volumeLifecycleModes",
            ErrorType::NotSupported
        ));
        assert!(
            errs[0]
                .to_string()
                .contains("\"Persistent\", \"Ephemeral\""),
            "{}",
            errs[0]
        );
    }

    /// validation.go:505-516 `validateFSGroupPolicy`: only a nil pointer is
    /// skipped; a non-nil empty string is not in `supportedFSGroupPolicy`, so
    /// `fsGroupPolicy: ""` is NotSupported (#2505), while an absent field is fine.
    #[test]
    fn empty_string_fs_group_policy_is_not_supported_but_absent_is_fine() {
        let bad: CSIDriverSpec = serde_json::from_value(serde_json::json!({
            "attachRequired": false, "podInfoOnMount": false,
            "storageCapacity": true, "seLinuxMount": false,
            "fsGroupPolicy": ""
        }))
        .expect("an empty fsGroupPolicy must decode");
        assert_eq!(
            bad.fs_group_policy,
            Some(FSGroupPolicy::Unknown(String::new()))
        );
        let errs = validate_csi_driver(&driver(bad));
        assert!(
            has(&errs, "spec.fsGroupPolicy", ErrorType::NotSupported),
            "{errs:?}"
        );

        let ok: CSIDriverSpec = serde_json::from_value(serde_json::json!({
            "attachRequired": false, "podInfoOnMount": false,
            "storageCapacity": true, "seLinuxMount": false
        }))
        .unwrap();
        assert!(ok.fs_group_policy.is_none());
        assert!(!has(
            &validate_csi_driver(&driver(ok)),
            "spec.fsGroupPolicy",
            ErrorType::NotSupported
        ));
    }

    /// validation_test.go:2031-2035 "FSGroupPolicy invalidated" (update case):
    /// changing a valid policy to "invalid" is rejected; changing to "File"
    /// ("change FSGroupPolicy", :1934-1937) is allowed.
    #[test]
    fn update_to_invalid_fs_group_policy_rejected() {
        let mk = |v: &str| {
            let spec: CSIDriverSpec = serde_json::from_value(serde_json::json!({
                "attachRequired": false, "podInfoOnMount": false,
                "storageCapacity": true, "seLinuxMount": false,
                "fsGroupPolicy": v
            }))
            .unwrap();
            driver(spec)
        };
        let old = mk("None");
        let errs = validate_csi_driver_update(&mk("invalid"), &old);
        assert!(
            has(&errs, "spec.fsGroupPolicy", ErrorType::NotSupported),
            "{errs:?}"
        );
        let errs = validate_csi_driver_update(&mk(""), &old);
        assert!(
            has(&errs, "spec.fsGroupPolicy", ErrorType::NotSupported),
            "{errs:?}"
        );
        assert!(validate_csi_driver_update(&mk("File"), &old).is_empty());
    }
}
