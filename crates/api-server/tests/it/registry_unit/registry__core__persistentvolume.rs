//! Unit tests moved from `src/registry/core/persistentvolume.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::pod_warnings::warnings_for_volume_node_selector_term;
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeMode, PersistentVolumeReclaimPolicy,
    };
    pub use rusternetes_common::resources::{
        PersistentVolume, PersistentVolumePhase, PersistentVolumeStatus,
    };
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::persistentvolume::{
        validate_persistent_volume, validate_persistent_volume_plugin,
        validate_persistent_volume_status_update, validate_persistent_volume_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::persistentvolume::*;

    fn pv() -> PersistentVolume {
        let mut pv: PersistentVolume = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": "pv", "resourceVersion": "1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/tmp/pv"}
            },
            "status": {"phase": "Bound"}
        }))
        .unwrap();
        convert_to_internal(&mut pv);
        pv
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.namespace_scoped());
        assert!(!StatusStrategy.allow_create_on_update());
    }

    #[test]
    fn defaults_reclaim_policy_and_volume_mode() {
        let pv = pv();
        assert_eq!(
            pv.spec.persistent_volume_reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Retain)
        );
        assert_eq!(pv.spec.volume_mode, Some(PersistentVolumeMode::Filesystem));
    }

    /// `SetObjectDefaults_PersistentVolume` (zz_generated.defaults.go:151-155):
    /// `iscsiInterface == ""` -> "default" (`+default="default"`,
    /// staging/src/k8s.io/api/core/v1/types.go:1558).
    #[test]
    fn defaults_iscsi_interface() {
        for (iface, want) in [
            (None, "default"),
            (Some(""), "default"),
            (Some("tcp"), "tcp"),
        ] {
            let mut iscsi = serde_json::json!({"targetPortal": "p", "iqn": "q", "lun": 0});
            if let Some(i) = iface {
                iscsi["iscsiInterface"] = i.into();
            }
            let mut pv: PersistentVolume = serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolume",
                "metadata": {"name": "pv"},
                "spec": {"capacity": {"storage": "1Gi"}, "iscsi": iscsi}
            }))
            .unwrap();
            convert_to_internal(&mut pv);
            assert_eq!(
                pv.spec.iscsi.unwrap().iscsi_interface.as_deref(),
                Some(want)
            );
        }
    }

    /// `SetObjectDefaults_PersistentVolume` (pkg/apis/core/v1/zz_generated.defaults.go:
    /// 137-181): rbd, azureDisk and scaleIO members default when empty/nil
    /// and keep explicit values.
    #[test]
    fn defaults_rbd_azure_disk_scale_io_sources() {
        let mut pv = pv();
        pv.spec.more_sources = serde_json::from_value(serde_json::json!({
            "rbd": {"monitors": ["m"], "image": "i"},
            "azureDisk": {"diskName": "d", "diskURI": "u"},
            "scaleIO": {"gateway": "g", "system": "s"}
        }))
        .unwrap();
        convert_to_internal(&mut pv);
        let v = serde_json::to_value(&pv.spec.more_sources).unwrap();
        assert_eq!(v["rbd"]["pool"], "rbd");
        assert_eq!(v["rbd"]["user"], "admin");
        assert_eq!(v["rbd"]["keyring"], "/etc/ceph/keyring");
        assert_eq!(v["azureDisk"]["cachingMode"], "ReadWrite");
        assert_eq!(v["azureDisk"]["fsType"], "ext4");
        assert_eq!(v["azureDisk"]["readOnly"], false);
        assert_eq!(v["azureDisk"]["kind"], "Shared");
        assert_eq!(v["scaleIO"]["storageMode"], "ThinProvisioned");
        assert_eq!(v["scaleIO"]["fsType"], "xfs");
    }

    #[test]
    fn explicit_rbd_azure_disk_scale_io_values_are_kept() {
        let mut pv = pv();
        pv.spec.more_sources = serde_json::from_value(serde_json::json!({
            "rbd": {"monitors": ["m"], "image": "i", "pool": "p", "user": "u", "keyring": "/k"},
            "azureDisk": {"diskName": "d", "diskURI": "u", "cachingMode": "None",
                "fsType": "xfs", "readOnly": true, "kind": "Dedicated"},
            "scaleIO": {"gateway": "g", "system": "s", "storageMode": "ThickProvisioned", "fsType": "ext4"}
        }))
        .unwrap();
        let before = serde_json::to_value(&pv.spec.more_sources).unwrap();
        convert_to_internal(&mut pv);
        assert_eq!(serde_json::to_value(&pv.spec.more_sources).unwrap(), before);
    }

    /// `TestStatusCreate` (strategy_test.go:247): create
    /// resets status to Pending and stamps the transition time.
    #[test]
    fn prepare_for_create_resets_status_to_pending() {
        let mut pv = pv();
        Strategy.prepare_for_create(&ctx(), &mut pv);
        let status = pv.status.as_ref().unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Pending);
        assert!(status.last_phase_transition_time.is_some());
        let errs = Strategy.validate(&ctx(), &pv);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestStatusUpdate` (strategy_test.go:43): a phase change stamps a new
    /// transition time; an unchanged phase keeps the stored one; a client
    /// time on a phase change is kept.
    #[test]
    fn status_update_tracks_the_phase_transition_time() {
        let mut old = pv();
        let then = Utc::now() - chrono::Duration::hours(1);
        old.status.as_mut().unwrap().last_phase_transition_time = Some(then);

        let mut same = old.clone();
        same.status.as_mut().unwrap().last_phase_transition_time = None;
        same.spec.capacity.insert("storage".into(), "9Gi".into());
        StatusStrategy.prepare_for_update(&ctx(), &mut same, &old);
        assert_eq!(same.spec.capacity, old.spec.capacity);
        assert_eq!(same.status.unwrap().last_phase_transition_time, Some(then));

        let mut released = old.clone();
        released.status.as_mut().unwrap().phase = PersistentVolumePhase::Released;
        StatusStrategy.prepare_for_update(&ctx(), &mut released, &old);
        let stamped = released.status.unwrap().last_phase_transition_time.unwrap();
        assert!(stamped > then);

        let chosen = then - chrono::Duration::hours(1);
        let mut client = old.clone();
        let status = client.status.as_mut().unwrap();
        status.phase = PersistentVolumePhase::Failed;
        status.last_phase_transition_time = Some(chosen);
        StatusStrategy.prepare_for_update(&ctx(), &mut client, &old);
        assert_eq!(
            client.status.unwrap().last_phase_transition_time,
            Some(chosen)
        );
    }

    /// `warningsForPersistentVolumeSpecAndMeta` (util.go:61-110).
    #[test]
    fn deprecated_annotations_and_recycle_warn() {
        let mut pv = pv();
        pv.metadata.annotations = Some(
            [(
                "volume.beta.kubernetes.io/storage-class".to_string(),
                "x".to_string(),
            )]
            .into(),
        );
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &pv),
            vec![
                r#"metadata.annotations[volume.beta.kubernetes.io/storage-class]: deprecated since v1.8; use "storageClassName" attribute instead"#.to_string(),
                "spec.persistentVolumeReclaimPolicy: The Recycle reclaim policy is deprecated. Instead, the recommended approach is to use dynamic provisioning.".to_string(),
            ]
        );
    }
}
