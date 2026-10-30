//! Unit tests moved from `src/validation/persistentvolume.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::pv_sources::{
        AzureDiskVolumeSource, AzureFilePersistentVolumeSource, CephFSPersistentVolumeSource,
        CinderPersistentVolumeSource, FlexPersistentVolumeSource, FlockerVolumeSource,
        GlusterfsPersistentVolumeSource, PhotonPersistentDiskVolumeSource, PortworxVolumeSource,
        QuobyteVolumeSource, RBDPersistentVolumeSource, ScaleIOPersistentVolumeSource,
        StorageOSPersistentVolumeSource, VsphereVirtualDiskVolumeSource,
    };
    pub use rusternetes_common::resources::volume::{
        AWSElasticBlockStoreVolumeSource, CSIVolumeSource, FCVolumeSource,
        GCEPersistentDiskVolumeSource, HostPathType, HostPathVolumeSource, ISCSIVolumeSource,
        LocalVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
        PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeMode,
        PersistentVolumeReclaimPolicy, PersistentVolumeSpec, SecretReference, VolumeNodeAffinity,
    };
    pub use rusternetes_common::validation::csinode::validate_csi_driver_name;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_namespace_name, validate_object_meta,
        validate_object_meta_update,
    };
    pub use rusternetes_common::validation::pod::{
        validate_nfs_volume_source, validate_node_selector,
    };
}

mod create_tests {
    use super::__parent::*;
    use rusternetes_common::validation::persistentvolume::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn valid_hostpath() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": "/data"}
            }
        })
    }

    #[test]
    fn clean_path_resolves_root() {
        assert_eq!(clean_path("/"), "/");
        assert_eq!(clean_path("//"), "/");
        assert_eq!(clean_path("/."), "/");
        assert_eq!(clean_path("/foo/.."), "/");
        assert_eq!(clean_path("/foo/../bar"), "/bar");
        assert_eq!(clean_path("/data"), "/data");
    }

    #[test]
    fn valid_pv_passes() {
        assert!(validate_persistent_volume(&pv(valid_hostpath())).is_empty());
    }

    #[test]
    fn capacity_required() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_must_be_storage_only() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "1Gi", "cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("capacity") && e.detail.contains("supported values")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_missing_storage_key_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "-1Gi"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("greater than or equal to 0")),
            "{errs:?}"
        );
    }

    #[test]
    fn access_modes_required() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!([]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("accessModes")),
            "{errs:?}"
        );
    }

    #[test]
    fn rwop_with_other_modes_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!(["ReadWriteOncePod", "ReadWriteOnce"]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not use ReadWriteOncePod with other access modes")),
            "{errs:?}"
        );
    }

    #[test]
    fn no_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"].as_object_mut().unwrap().remove("hostPath");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must specify a volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn more_than_one_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["nfs"] = serde_json::json!({"server": "1.2.3.4", "path": "/exports"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not specify more than 1 volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_requires_node_affinity() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"}
            }
        }));
        let errs = validate_persistent_volume(&v);
        assert!(
            errs.iter().any(|e| e.field.ends_with("nodeAffinity")
                && e.to_string()
                    .contains("Local volume requires node affinity")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_with_node_affinity_passes() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"},
                "nodeAffinity": {"required": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
                ]}}
            }
        }));
        assert!(validate_persistent_volume(&v).is_empty());
    }

    #[test]
    fn hostpath_root_with_recycle_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["hostPath"]["path"] = serde_json::json!("/");
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not be 'recycle' for a hostPath mount of '/'")),
            "{errs:?}"
        );
    }

    #[test]
    fn hostpath_nonroot_with_recycle_ok() {
        let mut v = valid_hostpath();
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        // path is /data, not '/', so Recycle is allowed
        assert!(validate_persistent_volume(&pv(v)).is_empty());
    }

    #[test]
    fn invalid_storage_class_name_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["storageClassName"] = serde_json::json!("Bad_Name");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("storageClassName")),
            "{errs:?}"
        );
    }
}

mod update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::persistentvolume::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn hostpath_pv(path: &str) -> PersistentVolume {
        pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": path}
            }
        }))
    }

    #[test]
    fn unchanged_passes() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/data");
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn changed_source_rejected() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/other");
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("persistentvolumesource is immutable")),
            "{errs:?}"
        );
    }

    /// validation.go:2282-2284: the immutability error appends
    /// `diff.Diff(oldPv.Spec.PersistentVolumeSource, newPv.Spec.PersistentVolumeSource)`,
    /// a unified diff (context 3) of `json.MarshalIndent` of the INTERNAL
    /// `core.PersistentVolumeSource` (Go field names, declaration order, nil
    /// pointers as `null`).
    #[test]
    fn changed_source_error_carries_diff_text() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/other");
        let errs = validate_persistent_volume_update(&new, &old);
        let e = errs
            .iter()
            .find(|e| e.field == "spec.persistentvolumesource")
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert_eq!(
            e.detail,
            concat!(
                "spec.persistentvolumesource is immutable after creation\n",
                "@@ -2,7 +2,7 @@\n",
                "  \"GCEPersistentDisk\": null,\n",
                "  \"AWSElasticBlockStore\": null,\n",
                "  \"HostPath\": {\n",
                "-  \"Path\": \"/data\",\n",
                "+  \"Path\": \"/other\",\n",
                "   \"Type\": null\n",
                "  },\n",
                "  \"Glusterfs\": null,\n",
            )
        );
    }

    #[test]
    fn changed_volume_mode_rejected() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_mode =
            Some(rusternetes_common::resources::volume::PersistentVolumeMode::Filesystem);
        let mut new = hostpath_pv("/data");
        new.spec.volume_mode =
            Some(rusternetes_common::resources::volume::PersistentVolumeMode::Block);
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("volumeMode") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn first_time_csi_expand_secret_ref_allowed() {
        let old = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1"}}
        }));
        let new = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1",
                    "controllerExpandSecretRef": {"name": "s", "namespace": "ns"}}}
        }));
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            !errs
                .iter()
                .any(|e| e.to_string().contains("persistentvolumesource")),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_immutable_once_set() {
        let na = serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
        ]}});
        let mut old = hostpath_pv("/data");
        old.spec.node_affinity = serde_json::from_value(na.clone()).unwrap();
        // unchanged -> ok
        let mut same = hostpath_pv("/data");
        same.spec.node_affinity = serde_json::from_value(na).unwrap();
        assert!(validate_persistent_volume_update(&same, &old).is_empty());
        // changed -> immutable
        let mut changed = hostpath_pv("/data");
        changed.spec.node_affinity = serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n2"]}]}
        ]}})).unwrap();
        let errs = validate_persistent_volume_update(&changed, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("nodeAffinity") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_may_be_set_when_old_nil() {
        let old = hostpath_pv("/data"); // no nodeAffinity
        let mut new = hostpath_pv("/data");
        new.spec.node_affinity =
            serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "k", "operator": "Exists"}]}
            ]}}))
            .unwrap();
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn vac_name_may_not_be_cleared() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_attributes_class_name = Some("gold".to_string());
        let new = hostpath_pv("/data"); // VAC cleared
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("non-nil value to nil is forbidden")),
            "{errs:?}"
        );
        // changing to another class is allowed
        let mut changed = hostpath_pv("/data");
        changed.spec.volume_attributes_class_name = Some("silver".to_string());
        assert!(!validate_persistent_volume_update(&changed, &old)
            .iter()
            .any(|e| e.field.ends_with("volumeAttributesClassName")));
    }
}

/// Every `PersistentVolumeSource` member (#3609), ported from upstream
/// `TestValidatePersistentVolumes` / `TestValidatePersistentVolumeSourceUpdate`
/// (`pkg/apis/core/validation/validation_test.go:114`, `:740`).
mod all_sources_tests {
    use super::__parent::*;
    use rusternetes_common::validation::persistentvolume::*;

    fn pv_with(source: serde_json::Value) -> PersistentVolume {
        let mut v = serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
            }
        });
        for (k, val) in source.as_object().unwrap() {
            v["spec"][k] = val.clone();
        }
        serde_json::from_value(v).unwrap()
    }

    /// One valid example per member, in upstream order
    /// (`ValidatePersistentVolumeSpec`, validation.go:2037-2210).
    fn valid_sources() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            ("gcePersistentDisk", serde_json::json!({"pdName": "p"})),
            ("awsElasticBlockStore", serde_json::json!({"volumeID": "v"})),
            (
                "glusterfs",
                serde_json::json!({"endpoints": "ep", "path": "vol", "endpointsNamespace": "default"}),
            ),
            ("flocker", serde_json::json!({"datasetName": "d"})),
            (
                "rbd",
                serde_json::json!({"monitors": ["m"], "image": "i", "secretRef": {"name": "s", "namespace": "default"}}),
            ),
            (
                "quobyte",
                serde_json::json!({"registry": "host:7861", "volume": "v"}),
            ),
            ("cephfs", serde_json::json!({"monitors": ["m"]})),
            (
                "cinder",
                serde_json::json!({"volumeID": "v", "secretRef": {"name": "s", "namespace": "default"}}),
            ),
            (
                "fc",
                serde_json::json!({"targetWWNs": ["500a0982"], "lun": 0}),
            ),
            (
                "flexVolume",
                serde_json::json!({"driver": "vendor/drv", "options": {"a": "b"}}),
            ),
            (
                "azureFile",
                serde_json::json!({"secretName": "s", "shareName": "sh", "secretNamespace": "default"}),
            ),
            (
                "vsphereVolume",
                serde_json::json!({"volumePath": "[ds] v.vmdk"}),
            ),
            ("photonPersistentDisk", serde_json::json!({"pdID": "p"})),
            ("portworxVolume", serde_json::json!({"volumeID": "v"})),
            (
                "azureDisk",
                serde_json::json!({"diskName": "d", "diskURI": "/subscriptions/x", "kind": "Managed", "cachingMode": "ReadOnly"}),
            ),
            (
                "scaleIO",
                serde_json::json!({"gateway": "g", "system": "s", "volumeName": "v", "secretRef": {"name": "s", "namespace": "default"}}),
            ),
            ("storageos", serde_json::json!({"volumeName": "vol"})),
        ]
    }

    #[test]
    fn every_source_roundtrips_and_validates() {
        for (name, body) in valid_sources() {
            let pv = pv_with(serde_json::json!({ name: body.clone() }));
            let out = serde_json::to_value(&pv).unwrap();
            assert_eq!(out["spec"][name], body, "{name} must survive a round trip");
            let errs = validate_persistent_volume(&pv);
            assert!(errs.is_empty(), "{name}: {errs:?}");
        }
    }

    #[test]
    fn fc_source_is_validated_and_counted() {
        // validateFCVolumeSource (validation.go:928-948): neither set.
        let errs = validate_persistent_volume(&pv_with(serde_json::json!({"fc": {}})));
        assert!(
            errs.iter().any(|e| e.field == "spec.fc.targetWWNs"),
            "{errs:?}"
        );
        // targetWWNs without lun.
        let errs =
            validate_persistent_volume(&pv_with(serde_json::json!({"fc": {"targetWWNs": ["a"]}})));
        assert!(errs.iter().any(|e| e.field == "spec.fc.lun"), "{errs:?}");
        // fc counts toward the union.
        let errs = validate_persistent_volume(&pv_with(serde_json::json!({
            "hostPath": {"path": "/foo"},
            "fc": {"wwids": ["w"]},
        })));
        assert!(
            errs.iter().any(|e| e.field == "spec.fc"
                && e.detail.contains("may not specify more than 1 volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn too_many_sources_is_forbidden() {
        // upstream "too-many-sources": hostPath + gcePersistentDisk.
        let pv = pv_with(serde_json::json!({
            "hostPath": {"path": "/foo"},
            "gcePersistentDisk": {"pdName": "foo"},
        }));
        let errs = validate_persistent_volume(&pv);
        assert!(
            errs.iter().any(|e| e.field == "spec.gcePersistentDisk"
                && e.detail.contains("may not specify more than 1 volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn source_update_is_forbidden_for_every_member() {
        for (name, body) in valid_sources() {
            let old = pv_with(serde_json::json!({ name: body.clone() }));
            // Change one scalar string member of the source.
            let mut changed = body.clone();
            let key = changed
                .as_object()
                .unwrap()
                .iter()
                .find(|(_, v)| v.is_string() || v.is_array())
                .map(|(k, _)| k.clone())
                .unwrap();
            changed[key] = if changed[&key].is_array() {
                serde_json::json!(["changed-value"])
            } else {
                serde_json::json!("changed-value")
            };
            let new = pv_with(serde_json::json!({ name: changed }));
            let errs = validate_persistent_volume_update(&new, &old);
            assert!(
                errs.iter()
                    .any(|e| e.field == "spec.persistentvolumesource"),
                "{name}: {errs:?}"
            );
        }
    }

    #[test]
    fn hostpath_to_flexvolume_is_forbidden() {
        // upstream "condition-update-source-type".
        let old = pv_with(serde_json::json!({"hostPath": {"path": "/foo"}}));
        let new = pv_with(
            serde_json::json!({"flexVolume": {"driver": "kubernetes.io/blue", "fsType": "ext4"}}),
        );
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.persistentvolumesource"),
            "{errs:?}"
        );
        assert!(
            !errs
                .iter()
                .any(|e| e.detail.contains("must specify a volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn required_fields_per_source() {
        let cases = [
            (
                "glusterfs",
                serde_json::json!({}),
                "spec.glusterfs.endpoints",
            ),
            (
                "glusterfs",
                serde_json::json!({"endpoints": "e"}),
                "spec.glusterfs.path",
            ),
            (
                "glusterfs",
                serde_json::json!({"endpoints": "e", "path": "p", "endpointsNamespace": ""}),
                "spec.glusterfs.endpointsNamespace",
            ),
            ("flocker", serde_json::json!({}), "spec.flocker"),
            ("rbd", serde_json::json!({}), "spec.rbd.monitors"),
            (
                "rbd",
                serde_json::json!({"monitors": ["m"]}),
                "spec.rbd.image",
            ),
            (
                "quobyte",
                serde_json::json!({"volume": "v"}),
                "spec.quobyte.registry",
            ),
            ("cephfs", serde_json::json!({}), "spec.cephfs.monitors"),
            ("cinder", serde_json::json!({}), "spec.cinder.volumeID"),
            (
                "cinder",
                serde_json::json!({"volumeID": "v", "secretRef": {"name": "s"}}),
                "spec.cinder.secretRef.namespace",
            ),
            (
                "flexVolume",
                serde_json::json!({}),
                "spec.flexVolume.driver",
            ),
            (
                "azureFile",
                serde_json::json!({"shareName": "s", "secretNamespace": "n"}),
                "spec.azureFile.secretName",
            ),
            (
                "azureFile",
                serde_json::json!({"secretName": "s", "secretNamespace": "n"}),
                "spec.azureFile.shareName",
            ),
            (
                "vsphereVolume",
                serde_json::json!({}),
                "spec.vsphereVolume.volumePath",
            ),
            (
                "photonPersistentDisk",
                serde_json::json!({}),
                "spec.photonPersistentDisk.pdID",
            ),
            (
                "portworxVolume",
                serde_json::json!({}),
                "spec.portworxVolume.volumeID",
            ),
            (
                "azureDisk",
                serde_json::json!({}),
                "spec.azureDisk.diskName",
            ),
            (
                "azureDisk",
                serde_json::json!({"diskName": "d"}),
                "spec.azureDisk.diskURI",
            ),
            ("scaleIO", serde_json::json!({}), "spec.scaleIO.gateway"),
            (
                "storageos",
                serde_json::json!({}),
                "spec.storageos.volumeName",
            ),
            (
                "gcePersistentDisk",
                serde_json::json!({}),
                "spec.persistentDisk.pdName",
            ),
            (
                "gcePersistentDisk",
                serde_json::json!({"pdName": "p", "partition": 256}),
                "spec.persistentDisk.partition",
            ),
            (
                "awsElasticBlockStore",
                serde_json::json!({}),
                "spec.awsElasticBlockStore.volumeID",
            ),
        ];
        for (name, body, field) in cases {
            let pv = pv_with(serde_json::json!({ name: body }));
            let errs = validate_persistent_volume(&pv);
            assert!(
                errs.iter().any(|e| e.field == field),
                "{name} -> {field}: {errs:?}"
            );
        }
    }
}
