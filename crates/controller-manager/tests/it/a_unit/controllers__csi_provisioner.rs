//! Unit tests moved from `src/controllers/csi_provisioner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::csi::{VolumeAttachment, VolumeAttributesClass};
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::{
        CSIVolumeSource, PersistentVolumeAccessMode, PersistentVolumeClaimPhase,
        PersistentVolumeMode, PersistentVolumePhase, PersistentVolumeReclaimPolicy, ReferenceGrant,
        SecretReference, StorageClass, VolumeBindingMode, VolumeSnapshot, VolumeSnapshotContent,
    };
    pub use rusternetes_common::resources::{
        EventSource, EventType, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
        PersistentVolumeStatus, Secret,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
    pub use rusternetes_controller_manager::controllers::csi_topology::{
        generate_volume_node_affinity, volume_is_accessible, TopologyError, TopologyInputs,
        TopologyStore,
    };
    pub use rusternetes_csi::controller_client::{
        check_driver_capabilities, ControllerError, CsiControllerClient, DriverCapabilities,
        ProvisioningState, RequiredCapabilities,
    };
    pub use rusternetes_csi::proto::controller_service_capability::rpc::Type as ControllerRpc;
    pub use rusternetes_csi::proto::volume_capability::access_mode::Mode as AccessModeKind;
    pub use rusternetes_csi::proto::volume_capability::{
        AccessMode, AccessType, BlockVolume, MountVolume,
    };
    pub use rusternetes_csi::proto::volume_content_source::{
        SnapshotSource, Type as ContentSourceType, VolumeSource,
    };
    pub use rusternetes_csi::proto::{
        CapacityRange, CreateVolumeRequest, DeleteVolumeRequest, VolumeCapability,
        VolumeContentSource,
    };
    pub use rusternetes_storage::WorkQueueConfig;
    pub use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::csi_provisioner::*;
    use PersistentVolumeAccessMode::*;

    /// `access_modes_test.go`'s table, both mappings.
    #[test]
    fn access_modes_map_like_csi_lib_utils() {
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOnce], false),
            Ok(AccessModeKind::SingleNodeWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOnce], true),
            Ok(AccessModeKind::SingleNodeMultiWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOncePod], false),
            Ok(AccessModeKind::SingleNodeWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteOncePod], true),
            Ok(AccessModeKind::SingleNodeSingleWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadWriteMany, ReadOnlyMany], true),
            Ok(AccessModeKind::MultiNodeMultiWriter)
        );
        assert_eq!(
            to_csi_access_mode(&[ReadOnlyMany], false),
            Ok(AccessModeKind::MultiNodeReaderOnly)
        );
        assert!(to_csi_access_mode(&[ReadOnlyMany, ReadWriteOnce], false).is_err());
        assert!(to_csi_access_mode(&[ReadWriteOncePod, ReadWriteOnce], false).is_err());
        assert!(to_csi_access_mode(&[], false).is_err());
    }

    #[test]
    fn prefixed_parameters_are_stripped_and_unknown_ones_rejected() {
        let p: HashMap<String, String> = [
            ("type", "ssd"),
            ("csi.storage.k8s.io/fstype", "xfs"),
            ("csi.storage.k8s.io/provisioner-secret-name", "s"),
            ("csiFoo", "kept: not under the reserved prefix"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .into();
        let out = remove_prefixed_parameters(&p).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.contains_key("type") && out.contains_key("csiFoo"));
        let bad: HashMap<String, String> =
            [("csi.storage.k8s.io/bogus".to_string(), "x".to_string())].into();
        assert!(remove_prefixed_parameters(&bad)
            .unwrap_err()
            .contains("found unknown parameter key"));
    }

    fn pvc(name: &str, ns: &str, ann: &[(&str, &str)]) -> PersistentVolumeClaim {
        let mut meta = ObjectMeta::new(name).with_namespace(ns);
        meta.annotations = Some(
            ann.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        PersistentVolumeClaim {
            type_meta: TypeMeta::default(),
            metadata: meta,
            spec: Default::default(),
            status: None,
        }
    }

    fn params(p: &[(&str, &str)]) -> HashMap<String, String> {
        p.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// `TestGetSecretReference` (controller_test.go) cases: tokens, the
    /// deprecated keys, the default keys and every refusal.
    #[test]
    fn secret_references_resolve_like_get_secret_reference() {
        let sp = &PROVISIONER_SECRET_PARAMS;
        let c = pvc("pvc1", "ns1", &[("example.com/k", "from-ann")]);
        let r = |p: &[(&str, &str)]| get_secret_reference(sp, &params(p), "pv1", &c);
        let named = |n: &str, ns: &str| {
            Ok(Some(SecretReference {
                name: Some(n.into()),
                namespace: Some(ns.into()),
            }))
        };

        assert_eq!(r(&[]), Ok(None));
        assert_eq!(
            r(&[
                (
                    "csi.storage.k8s.io/provisioner-secret-name",
                    "${pv.name}-${pvc.name}"
                ),
                (
                    "csi.storage.k8s.io/provisioner-secret-namespace",
                    "${pvc.namespace}"
                ),
            ]),
            named("pv1-pvc1", "ns1")
        );
        // Deprecated keys and `$name` shorthand.
        assert_eq!(
            r(&[
                ("csiProvisionerSecretName", "${pv.name}"),
                ("csiProvisionerSecretNamespace", "ns"),
            ]),
            named("pv1", "ns")
        );
        // `$pv` is the variable (`.name` stays literal) and `pv` is unknown.
        assert!(r(&[
            ("csiProvisionerSecretName", "$pv.name"),
            ("csiProvisionerSecretNamespace", "ns"),
        ])
        .unwrap_err()
        .contains("invalid tokens: [\"pv\"]"));
        assert_eq!(
            r(&[
                (
                    "csi.storage.k8s.io/provisioner-secret-name",
                    "${pvc.annotations['example.com/k']}"
                ),
                ("csi.storage.k8s.io/provisioner-secret-namespace", "ns"),
            ]),
            named("from-ann", "ns")
        );
        // Falls back to the default keys.
        assert_eq!(
            r(&[
                ("csi.storage.k8s.io/secret-name", "dflt"),
                ("csi.storage.k8s.io/secret-namespace", "ns"),
            ]),
            named("dflt", "ns")
        );
        // Refusals.
        let both = r(&[
            ("csiProvisionerSecretName", "a"),
            ("csi.storage.k8s.io/provisioner-secret-name", "a"),
        ])
        .unwrap_err();
        assert!(both.contains("with both \"csi\" and \"csi.storage.k8s.io/\" keys"));
        let half = r(&[("csi.storage.k8s.io/provisioner-secret-name", "a")]).unwrap_err();
        assert!(half.contains("Both must be specified"));
        let empty = r(&[
            ("csi.storage.k8s.io/provisioner-secret-name", ""),
            ("csi.storage.k8s.io/provisioner-secret-namespace", "ns"),
        ])
        .unwrap_err();
        assert!(empty.contains("value of either namespace or name is empty"));
        let bad_ns = r(&[
            ("csi.storage.k8s.io/provisioner-secret-name", "a"),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                "${pv.name}.X",
            ),
        ])
        .unwrap_err();
        assert!(bad_ns.contains("resolved to \"pv1.X\" which is not a valid namespace name"));
        let bad_name = r(&[
            ("csi.storage.k8s.io/provisioner-secret-name", "Bad_Name"),
            ("csi.storage.k8s.io/provisioner-secret-namespace", "ns"),
        ])
        .unwrap_err();
        assert!(bad_name.contains("\"Bad_Name\" is not a valid secret name"));
        // The namespace template cannot read the PVC name.
        let ns_tok = r(&[
            ("csi.storage.k8s.io/provisioner-secret-name", "a"),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                "${pvc.name}",
            ),
        ])
        .unwrap_err();
        assert!(ns_tok.contains("invalid tokens: [\"pvc.name\"]"));
    }

    /// Go `os.Expand` edge cases `resolveTemplate` inherits.
    #[test]
    fn resolve_template_follows_os_expand() {
        let p = params(&[("a", "1"), ("b.c", "2")]);
        assert_eq!(resolve_template("x${a}y${b.c}", &p).unwrap(), "x1y2");
        assert_eq!(resolve_template("$a-z", &p).unwrap(), "1-z");
        assert_eq!(resolve_template("cost$", &p).unwrap(), "cost$");
        assert_eq!(resolve_template("a${}b", &p).unwrap(), "ab");
        assert_eq!(
            resolve_template("${z}${y}", &p).unwrap_err(),
            "invalid tokens: [\"y\" \"z\"]"
        );
    }

    #[test]
    fn volume_names_follow_make_volume_name() {
        assert_eq!(make_volume_name("pvc", "abc-1").unwrap(), "pvc-abc-1");
        assert!(make_volume_name("", "abc").is_err());
        assert!(make_volume_name("pvc", "").is_err());
    }

    /// `TestRateLimiter` (ratelimiter_test.go:25-35), plus the exponential
    /// growth and `Forget`.
    #[test]
    fn owner_rate_limiter_stays_within_max_and_forgets() {
        let max = Duration::from_secs(10);
        let l = OwnerRateLimiter::new(Duration::from_secs(1), max);
        for _ in 0..100 {
            assert!(l.when("uid", "k") <= max);
        }
        assert_eq!(l.num_requeues("uid"), 100);
        l.forget_key("k");
        assert_eq!(l.num_requeues("uid"), 0);
        // Zero base delay (the tests): no wait at all.
        let z = OwnerRateLimiter::new(Duration::ZERO, Duration::ZERO);
        assert_eq!(z.when("u", "k"), Duration::ZERO);
    }

    #[test]
    fn pv_save_backoff_doubles_from_5ms_to_1000s() {
        assert_eq!(backoff(1), Duration::from_millis(5));
        assert_eq!(backoff(3), Duration::from_millis(20));
        assert_eq!(backoff(60), PV_SAVE_MAX);
    }
}
