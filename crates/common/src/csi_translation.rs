//! In-tree -> CSI PersistentVolume translation: a port of
//! `staging/src/k8s.io/csi-translation-lib` (`translate.go`,
//! `plugins/in_tree_volume.go`, `plugins/gce_pd.go`, `plugins/aws_ebs.go`).
//!
//! Shared by the external-attacher (`CsiAttacher`) and the scheduler's
//! VolumeBinding plugin (`tryTranslatePVToCSI`). Plugins ported so far: GCE PD
//! and AWS EBS; the other in-tree plugins of `translate.go:34-42` (Cinder,
//! AzureDisk, AzureFile, vSphere, Portworx) are tracked follow-ups.
//!
//! NOT ported: `TranslateCSIPVToInTree`, `TranslateInTreeStorageClassToCSI`
//! and `TranslateInTreeInlineVolumeToCSI` (tracked follow-ups).

use crate::resources::PersistentVolume;

/// `GCEPDDriverName` (`gce_pd.go:33`).
pub const GCE_PD_DRIVER_NAME: &str = "pd.csi.storage.gke.io";
/// `GCEPDInTreePluginName` (`gce_pd.go:35`).
pub const GCE_PD_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/gce-pd";
/// `AWSEBSDriverName` (`aws_ebs.go:35`).
pub const AWS_EBS_DRIVER_NAME: &str = "ebs.csi.aws.com";
/// `AWSEBSInTreePluginName` (`aws_ebs.go:37`).
pub const AWS_EBS_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/aws-ebs";

/// `CSITranslator` (`translate.go:48`).
#[derive(Debug, Clone, Copy, Default)]
pub struct CsiTranslator;

impl CsiTranslator {
    pub fn new() -> Self {
        Self
    }

    /// `TranslateInTreePVToCSI` (`translate.go:101-112`); the input is not modified.
    pub fn translate_in_tree_pv_to_csi(
        &self,
        _pv: &PersistentVolume,
    ) -> Result<PersistentVolume, String> {
        Err("not implemented".into())
    }

    /// `IsPVMigratable` (`translate.go:176-183`).
    pub fn is_pv_migratable(&self, _pv: &PersistentVolume) -> bool {
        false
    }

    /// `IsMigratableIntreePluginByName` (`translate.go:128-136`).
    pub fn is_migratable_in_tree_plugin_by_name(&self, _name: &str) -> bool {
        false
    }

    /// `IsMigratedCSIDriverByName` (`translate.go:140-145`).
    pub fn is_migrated_csi_driver_by_name(&self, _name: &str) -> bool {
        false
    }

    /// `GetInTreePluginNameFromSpec` for a PV (`translate.go:157-163`).
    pub fn get_in_tree_plugin_name_from_pv(
        &self,
        _pv: &PersistentVolume,
    ) -> Result<String, String> {
        Err("not implemented".into())
    }

    /// `GetCSINameFromInTreeName` (`translate.go:185-195`).
    pub fn get_csi_name_from_in_tree_name(&self, _name: &str) -> Result<String, String> {
        Err("not implemented".into())
    }

    /// `GetInTreeNameFromCSIName` (`translate.go:198-205`).
    pub fn get_in_tree_name_from_csi_name(&self, _name: &str) -> Result<String, String> {
        Err("not implemented".into())
    }

    /// `RepairVolumeHandle` (`translate.go:207-212`).
    pub fn repair_volume_handle(
        &self,
        _driver_name: &str,
        _volume_handle: &str,
        _node_id: &str,
    ) -> Result<String, String> {
        Err("not implemented".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::volume::{
        AWSElasticBlockStoreVolumeSource, GCEPersistentDiskVolumeSource, NodeSelector,
        NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeAccessMode, VolumeNodeAffinity,
    };
    use crate::types::{ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    const ZONE_BETA: &str = "failure-domain.beta.kubernetes.io/zone";
    const ZONE_GA: &str = "topology.kubernetes.io/zone";

    fn blank_pv(name: &str) -> PersistentVolume {
        PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta::new(name),
            spec: Default::default(),
            status: None,
        }
    }

    fn gce_pv(labels: &[(&str, &str)]) -> PersistentVolume {
        let mut pv = blank_pv("pv1");
        pv.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        );
        pv.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
            pd_name: "disk1".into(),
            fs_type: Some("ext4".into()),
            partition: Some(2),
            read_only: Some(true),
        });
        pv
    }

    fn ebs_pv(id: &str) -> PersistentVolume {
        let mut pv = blank_pv("pv2");
        pv.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
            volume_id: id.into(),
            fs_type: None,
            partition: None,
            read_only: None,
        });
        pv
    }

    fn affinity(key: &str, values: &[&str]) -> Option<VolumeNodeAffinity> {
        Some(VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: key.into(),
                        operator: "In".into(),
                        values: Some(values.iter().map(|s| s.to_string()).collect()),
                    }]),
                    match_fields: None,
                }],
            }),
        })
    }

    fn keys_and_values(pv: &PersistentVolume) -> Vec<(String, Vec<String>)> {
        pv.spec
            .node_affinity
            .as_ref()
            .and_then(|a| a.required.as_ref())
            .into_iter()
            .flat_map(|r| r.node_selector_terms.iter())
            .flat_map(|t| t.match_expressions.iter().flatten())
            .map(|r| (r.key.clone(), r.values.clone().unwrap_or_default()))
            .collect()
    }

    /// `TestTranslateInTreePVToCSIVolIDFmt` (`gce_pd_test.go`): zonal label.
    #[test]
    fn gce_zonal_pv_translates_to_csi() {
        let pv = gce_pv(&[(ZONE_GA, "us-central1-a")]);
        let t = CsiTranslator::new();
        assert!(t.is_pv_migratable(&pv));
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert!(out.spec.gce_persistent_disk.is_none());
        let csi = out.spec.csi.clone().unwrap();
        assert_eq!(csi.driver, "pd.csi.storage.gke.io");
        assert_eq!(
            csi.volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/zones/us-central1-a/disks/disk1")
        );
        assert_eq!(csi.fs_type.as_deref(), Some("ext4"));
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(csi.volume_attributes.unwrap()["partition"], "2");
        // zone label -> node affinity on the CSI topology key
        assert_eq!(
            keys_and_values(&out),
            vec![(
                "topology.gke.io/zone".to_string(),
                vec!["us-central1-a".to_string()]
            )]
        );
    }

    /// Multi-zone label (`__`-delimited) is a regional disk.
    #[test]
    fn gce_regional_pv_uses_region_in_handle() {
        let pv = gce_pv(&[(ZONE_BETA, "us-central1-a__us-central1-b")]);
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            out.spec.csi.unwrap().volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/regions/us-central1/disks/disk1")
        );
    }

    #[test]
    fn gce_no_zone_is_unspecified_and_rwx_becomes_rwo() {
        let mut pv = gce_pv(&[]);
        pv.spec.access_modes = vec![
            PersistentVolumeAccessMode::ReadWriteMany,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ];
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            out.spec.csi.as_ref().unwrap().volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/zones/UNSPECIFIED/disks/disk1")
        );
        // `backwardCompatibleAccessModes` (`gce_pd.go:137-171`)
        assert_eq!(
            out.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteOnce]
        );
    }

    /// An existing in-tree node affinity key is replaced (`replaceTopology`).
    #[test]
    fn gce_affinity_key_is_replaced() {
        let mut pv = gce_pv(&[]);
        pv.spec.node_affinity = affinity(ZONE_BETA, &["us-east1-b"]);
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            keys_and_values(&out),
            vec![("topology.gke.io/zone".into(), vec!["us-east1-b".into()])]
        );
    }

    #[test]
    fn ebs_pv_translates_and_strips_aws_url() {
        let t = CsiTranslator::new();
        let mut pv = ebs_pv("aws://us-east-1a/vol-0abc");
        pv.metadata.labels = Some([(ZONE_GA.to_string(), "us-east-1a".to_string())].into());
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "ebs.csi.aws.com");
        assert_eq!(csi.volume_handle.as_deref(), Some("vol-0abc"));
        assert_eq!(csi.volume_attributes.as_ref().unwrap()["partition"], "0");
        assert_eq!(
            keys_and_values(&out),
            vec![(
                "topology.ebs.csi.aws.com/zone".into(),
                vec!["us-east-1a".into()]
            )]
        );
        assert!(t
            .translate_in_tree_pv_to_csi(&ebs_pv("aws://us-east-1a/notavol"))
            .is_err());
    }

    #[test]
    fn non_in_tree_pv_is_not_migratable_and_errors() {
        let pv = blank_pv("pv3");
        let t = CsiTranslator::new();
        assert!(!t.is_pv_migratable(&pv));
        assert!(t.translate_in_tree_pv_to_csi(&pv).is_err());
    }

    #[test]
    fn name_mappings() {
        let t = CsiTranslator::new();
        assert!(t.is_migratable_in_tree_plugin_by_name("kubernetes.io/gce-pd"));
        assert!(!t.is_migratable_in_tree_plugin_by_name("kubernetes.io/nfs"));
        assert!(t.is_migrated_csi_driver_by_name("ebs.csi.aws.com"));
        assert_eq!(
            t.get_csi_name_from_in_tree_name("kubernetes.io/aws-ebs")
                .unwrap(),
            "ebs.csi.aws.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("pd.csi.storage.gke.io")
                .unwrap(),
            "kubernetes.io/gce-pd"
        );
        assert_eq!(
            t.get_in_tree_plugin_name_from_pv(&gce_pv(&[])).unwrap(),
            "kubernetes.io/gce-pd"
        );
    }

    /// `TestRepairVolumeHandle` (`gce_pd_test.go`).
    #[test]
    fn gce_repair_volume_handle() {
        let t = CsiTranslator::new();
        let node = "projects/p1/zones/us-central1-c/instances/n1";
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/UNSPECIFIED/zones/UNSPECIFIED/disks/d",
                node
            )
            .unwrap(),
            "projects/p1/zones/us-central1-c/disks/d"
        );
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/UNSPECIFIED/regions/UNSPECIFIED/disks/d",
                node
            )
            .unwrap(),
            "projects/p1/regions/us-central1/disks/d"
        );
        // already specified: untouched
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/x/zones/z-z-z/disks/d",
                node
            )
            .unwrap(),
            "projects/x/zones/z-z-z/disks/d"
        );
        assert!(t
            .repair_volume_handle("pd.csi.storage.gke.io", "short", node)
            .is_err());
        // EBS: identity (`aws_ebs.go:227-229`)
        assert_eq!(
            t.repair_volume_handle("ebs.csi.aws.com", "vol-1", "i-1")
                .unwrap(),
            "vol-1"
        );
        assert!(t.repair_volume_handle("other", "x", "y").is_err());
    }
}
