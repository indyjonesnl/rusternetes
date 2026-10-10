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

use crate::resources::volume::{
    CSIVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    PersistentVolumeAccessMode, VolumeNodeAffinity,
};
use crate::resources::PersistentVolume;
use std::collections::{BTreeSet, HashMap};

/// `GCEPDDriverName` (`gce_pd.go:33`).
pub const GCE_PD_DRIVER_NAME: &str = "pd.csi.storage.gke.io";
/// `GCEPDInTreePluginName` (`gce_pd.go:35`).
pub const GCE_PD_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/gce-pd";
/// `AWSEBSDriverName` (`aws_ebs.go:35`).
pub const AWS_EBS_DRIVER_NAME: &str = "ebs.csi.aws.com";
/// `AWSEBSInTreePluginName` (`aws_ebs.go:37`).
pub const AWS_EBS_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/aws-ebs";

/// `labelMultiZoneDelimiter` (`plugins/const.go:21`).
const LABEL_MULTI_ZONE_DELIMITER: &str = "__";
/// `v1.LabelFailureDomainBetaZone` / `...BetaRegion` / `LabelTopologyZone` /
/// `LabelTopologyRegion` (`staging/src/k8s.io/api/core/v1/well_known_labels.go`).
const LABEL_FAILURE_DOMAIN_BETA_ZONE: &str = "failure-domain.beta.kubernetes.io/zone";
const LABEL_FAILURE_DOMAIN_BETA_REGION: &str = "failure-domain.beta.kubernetes.io/region";
const LABEL_TOPOLOGY_ZONE: &str = "topology.kubernetes.io/zone";
const LABEL_TOPOLOGY_REGION: &str = "topology.kubernetes.io/region";
/// `GCEPDTopologyKey` (`gce_pd.go:38`).
const GCE_PD_TOPOLOGY_KEY: &str = "topology.gke.io/zone";
/// `AWSEBSTopologyKey` (`aws_ebs.go:39`).
const AWS_EBS_TOPOLOGY_KEY: &str = "topology.ebs.csi.aws.com/zone";
/// `UnspecifiedValue` (`gce_pd.go:54`).
const UNSPECIFIED_VALUE: &str = "UNSPECIFIED";

/// `InTreePlugin` (`in_tree_volume.go:31`), the PV-side subset.
trait InTreePlugin {
    /// Input PV is already a private copy and may be modified.
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String>;
    fn can_support(&self, pv: &PersistentVolume) -> bool;
    fn in_tree_plugin_name(&self) -> &'static str;
    fn csi_plugin_name(&self) -> &'static str;
    fn repair_volume_handle(&self, volume_handle: &str, node_id: &str) -> Result<String, String>;
}

/// `inTreePlugins` (`translate.go:32-42`), the ported subset.
static GCE_PD: GcePersistentDiskTranslator = GcePersistentDiskTranslator;
static AWS_EBS: AwsElasticBlockStoreTranslator = AwsElasticBlockStoreTranslator;
fn in_tree_plugins() -> [&'static dyn InTreePlugin; 2] {
    [&GCE_PD, &AWS_EBS]
}

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
        pv: &PersistentVolume,
    ) -> Result<PersistentVolume, String> {
        let mut copied = pv.clone();
        for plugin in in_tree_plugins() {
            if plugin.can_support(&copied) {
                plugin.translate_in_tree_pv_to_csi(&mut copied)?;
                return Ok(copied);
            }
        }
        Err(format!(
            "could not find in-tree plugin translation logic for {:?}",
            copied.metadata.name
        ))
    }

    /// `IsPVMigratable` (`translate.go:176-183`).
    pub fn is_pv_migratable(&self, pv: &PersistentVolume) -> bool {
        in_tree_plugins().iter().any(|p| p.can_support(pv))
    }

    /// `IsMigratableIntreePluginByName` (`translate.go:128-136`).
    pub fn is_migratable_in_tree_plugin_by_name(&self, name: &str) -> bool {
        in_tree_plugins()
            .iter()
            .any(|p| p.in_tree_plugin_name() == name)
    }

    /// `IsMigratedCSIDriverByName` (`translate.go:140-145`).
    pub fn is_migrated_csi_driver_by_name(&self, name: &str) -> bool {
        in_tree_plugins()
            .iter()
            .any(|p| p.csi_plugin_name() == name)
    }

    /// `GetInTreePluginNameFromSpec` for a PV (`translate.go:157-163`).
    pub fn get_in_tree_plugin_name_from_pv(&self, pv: &PersistentVolume) -> Result<String, String> {
        in_tree_plugins()
            .iter()
            .find(|p| p.can_support(pv))
            .map(|p| p.in_tree_plugin_name().to_string())
            .ok_or_else(|| {
                format!(
                    "could not find in-tree plugin name from persistent volume {}",
                    pv.metadata.name
                )
            })
    }

    /// `GetCSINameFromInTreeName` (`translate.go:185-195`).
    pub fn get_csi_name_from_in_tree_name(&self, name: &str) -> Result<String, String> {
        in_tree_plugins()
            .iter()
            .find(|p| p.in_tree_plugin_name() == name)
            .map(|p| p.csi_plugin_name().to_string())
            .ok_or_else(|| format!("could not find CSI Driver name for plugin {name}"))
    }

    /// `GetInTreeNameFromCSIName` (`translate.go:198-205`).
    pub fn get_in_tree_name_from_csi_name(&self, name: &str) -> Result<String, String> {
        in_tree_plugins()
            .iter()
            .find(|p| p.csi_plugin_name() == name)
            .map(|p| p.in_tree_plugin_name().to_string())
            .ok_or_else(|| format!("could not find In-Tree driver name for CSI plugin {name}"))
    }

    /// `RepairVolumeHandle` (`translate.go:207-212`).
    pub fn repair_volume_handle(
        &self,
        driver_name: &str,
        volume_handle: &str,
        node_id: &str,
    ) -> Result<String, String> {
        match in_tree_plugins()
            .iter()
            .find(|p| p.csi_plugin_name() == driver_name)
        {
            Some(p) => p.repair_volume_handle(volume_handle, node_id),
            None => Err(format!(
                "could not find In-Tree driver name for CSI plugin {driver_name}"
            )),
        }
    }
}

// ---- topology helpers (`plugins/in_tree_volume.go`) -------------------------

fn match_expressions_mut(pv: &mut PersistentVolume) -> Vec<&mut NodeSelectorRequirement> {
    pv.spec
        .node_affinity
        .as_mut()
        .and_then(|a| a.required.as_mut())
        .into_iter()
        .flat_map(|r| r.node_selector_terms.iter_mut())
        .flat_map(|t| t.match_expressions.iter_mut().flatten())
        .collect()
}

/// `replaceTopology` (`in_tree_volume.go:84-100`).
fn replace_topology(pv: &mut PersistentVolume, old_key: &str, new_key: &str) {
    for r in match_expressions_mut(pv) {
        if r.key == old_key {
            r.key = new_key.to_string();
        }
    }
}

/// `getTopologyValues` (`in_tree_volume.go:107-132`): unique, sorted.
fn get_topology_values(pv: &PersistentVolume, key: &str) -> Vec<String> {
    let mut values = BTreeSet::new();
    if let Some(req) = pv
        .spec
        .node_affinity
        .as_ref()
        .and_then(|a| a.required.as_ref())
    {
        for t in &req.node_selector_terms {
            for r in t.match_expressions.iter().flatten() {
                if r.key == key {
                    values.extend(r.values.iter().flatten().cloned());
                }
            }
        }
    }
    values.into_iter().collect()
}

/// `addTopology` (`in_tree_volume.go:135-179`).
fn add_topology(pv: &mut PersistentVolume, key: &str, zones: &[String]) -> Result<(), String> {
    let filtered: BTreeSet<String> = zones
        .iter()
        .map(|z| z.trim().to_string())
        .filter(|z| !z.is_empty())
        .collect();
    if filtered.is_empty() {
        return Err("there are no valid zones to add to pv".into());
    }
    let required = pv
        .spec
        .node_affinity
        .get_or_insert_with(|| VolumeNodeAffinity { required: None })
        .required
        .get_or_insert_with(|| NodeSelector {
            node_selector_terms: Vec::new(),
        });
    if required.node_selector_terms.is_empty() {
        required.node_selector_terms.push(NodeSelectorTerm {
            match_expressions: None,
            match_fields: None,
        });
    }
    // add the CSI topology to each term
    for term in &mut required.node_selector_terms {
        term.match_expressions
            .get_or_insert_with(Vec::new)
            .push(NodeSelectorRequirement {
                key: key.to_string(),
                operator: "In".to_string(),
                values: Some(filtered.iter().cloned().collect()),
            });
    }
    Ok(())
}

/// `TopologyKeyExist` (`in_tree_volume.go:244-258`).
fn topology_key_exist(key: &str, vna: Option<&VolumeNodeAffinity>) -> bool {
    vna.and_then(|v| v.required.as_ref()).is_some_and(|r| {
        r.node_selector_terms
            .iter()
            .any(|t| t.match_expressions.iter().flatten().any(|e| e.key == key))
    })
}

/// `getTopologyLabel` (`in_tree_volume.go:225-241`).
fn get_topology_label(pv: &PersistentVolume) -> (&'static str, &'static str) {
    let affinity = pv.spec.node_affinity.as_ref();
    let labels = pv.metadata.labels.as_ref();
    if topology_key_exist(LABEL_TOPOLOGY_ZONE, affinity) {
        return (LABEL_TOPOLOGY_ZONE, LABEL_TOPOLOGY_REGION);
    }
    if topology_key_exist(LABEL_FAILURE_DOMAIN_BETA_ZONE, affinity) {
        return (
            LABEL_FAILURE_DOMAIN_BETA_ZONE,
            LABEL_FAILURE_DOMAIN_BETA_REGION,
        );
    }
    if labels.is_some_and(|l| l.contains_key(LABEL_TOPOLOGY_ZONE)) {
        return (LABEL_TOPOLOGY_ZONE, LABEL_TOPOLOGY_REGION);
    }
    if labels.is_some_and(|l| l.contains_key(LABEL_FAILURE_DOMAIN_BETA_ZONE)) {
        return (
            LABEL_FAILURE_DOMAIN_BETA_ZONE,
            LABEL_FAILURE_DOMAIN_BETA_REGION,
        );
    }
    // No labels or NodeAffinity exist, default to GA version
    (LABEL_TOPOLOGY_ZONE, LABEL_TOPOLOGY_REGION)
}

/// `translateTopologyFromInTreeToCSI` (`in_tree_volume.go:185-214`).
fn translate_topology_from_in_tree_to_csi(pv: &mut PersistentVolume, csi_topology_key: &str) {
    let (zone_label, region_label) = get_topology_label(pv);

    // If Zone kubernetes topology exist, replace it to use csiTopologyKey
    let zones = get_topology_values(pv, zone_label);
    if !zones.is_empty() {
        replace_topology(pv, zone_label, csi_topology_key);
    } else if let Some(label) = pv
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(zone_label))
        .cloned()
    {
        // if nothing is in the NodeAffinity, try to fetch the topology from PV labels
        let zones: Vec<String> = label
            .split(LABEL_MULTI_ZONE_DELIMITER)
            .map(String::from)
            .collect();
        // upstream ignores the error here
        let _ = add_topology(pv, csi_topology_key, &zones);
    }

    // if the in-tree PV has beta region label, replace it with GA label to ensure
    // the scheduler is able to schedule it on new nodes with only GA kubernetes label
    if region_label == LABEL_FAILURE_DOMAIN_BETA_REGION
        && !get_topology_values(pv, region_label).is_empty()
    {
        replace_topology(pv, region_label, LABEL_TOPOLOGY_REGION);
    }
}

// ---- GCE PD (`plugins/gce_pd.go`) -------------------------------------------

struct GcePersistentDiskTranslator;

/// Volume handle token indexes (`gce_pd.go:46-50`).
const VOL_ID_PROJECT_VALUE: usize = 1;
const VOL_ID_REGIONALITY_VALUE: usize = 2;
const VOL_ID_ZONE_VALUE: usize = 3;
const VOL_ID_DISK_NAME_VALUE: usize = 5;
const VOL_ID_TOTAL_ELEMENTS: usize = 6;

/// `backwardCompatibleAccessModes` (`gce_pd.go:137-171`).
fn backward_compatible_access_modes(
    ams: &[PersistentVolumeAccessMode],
) -> Vec<PersistentVolumeAccessMode> {
    if ams.is_empty() {
        return Vec::new();
    }
    let mut rwo = false;
    let mut rox = false;
    for am in ams {
        match am {
            // ReadWriteMany is unsupported in CSI, but in-tree did no
            // validation and treated it as ReadWriteOnce
            PersistentVolumeAccessMode::ReadWriteMany
            | PersistentVolumeAccessMode::ReadWriteOnce => rwo = true,
            PersistentVolumeAccessMode::ReadOnlyMany => rox = true,
            _ => {}
        }
    }
    // [ROX, RWO] is unsupported in CSI, but in-tree treated it as RWO.
    if rwo {
        vec![PersistentVolumeAccessMode::ReadWriteOnce]
    } else if rox {
        vec![PersistentVolumeAccessMode::ReadOnlyMany]
    } else {
        vec![PersistentVolumeAccessMode::ReadWriteOnce]
    }
}

/// `gceGetRegionFromZones` (`gce_pd.go:380-400`).
fn gce_get_region_from_zones(zones: &[&str]) -> Result<String, String> {
    if zones.is_empty() {
        return Err("no zones specified".into());
    }
    let mut regions = BTreeSet::new();
    for zone in zones {
        // Zone expected format {locale}-{region}-{zone}
        let split: Vec<&str> = zone.split('-').collect();
        if split.len() != 3 {
            return Err(format!(
                "zone in unexpected format, expected: {{locale}}-{{region}}-{{zone}}, got: {zone}"
            ));
        }
        regions.insert(split[0..2].join("-"));
    }
    if regions.len() != 1 {
        return Err(format!(
            "multiple or no regions gotten from zones, got: {regions:?}"
        ));
    }
    Ok(regions.into_iter().next().unwrap())
}

impl InTreePlugin for GcePersistentDiskTranslator {
    /// `TranslateInTreePVToCSI` (`gce_pd.go:204-258`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(gce) = pv.spec.gce_persistent_disk.clone() else {
            return Err("pv is nil or GCE Persistent Disk source not defined on pv".into());
        };
        // depend on which version it migrates from, the label could be
        // failuredomain beta or topology GA version
        let labels = pv.metadata.labels.as_ref();
        let mut zones_label = labels
            .and_then(|l| l.get(LABEL_FAILURE_DOMAIN_BETA_ZONE))
            .cloned()
            .unwrap_or_default();
        if zones_label.is_empty() {
            zones_label = labels
                .and_then(|l| l.get(LABEL_TOPOLOGY_ZONE))
                .cloned()
                .unwrap_or_default();
        }
        let zones: Vec<&str> = zones_label.split(LABEL_MULTI_ZONE_DELIMITER).collect();
        let vol_id = if zones.len() == 1 && !zones[0].is_empty() {
            // Zonal
            format!(
                "projects/{UNSPECIFIED_VALUE}/zones/{}/disks/{}",
                zones[0], gce.pd_name
            )
        } else if zones.len() > 1 {
            // Regional
            let region = gce_get_region_from_zones(&zones)
                .map_err(|e| format!("failed to get region from zones: {e}"))?;
            format!(
                "projects/{UNSPECIFIED_VALUE}/regions/{region}/disks/{}",
                gce.pd_name
            )
        } else {
            // Unspecified
            format!(
                "projects/{UNSPECIFIED_VALUE}/zones/{UNSPECIFIED_VALUE}/disks/{}",
                gce.pd_name
            )
        };
        let partition = match gce.partition {
            Some(p) if p != 0 => p.to_string(),
            _ => String::new(),
        };
        let csi = CSIVolumeSource {
            driver: GCE_PD_DRIVER_NAME.to_string(),
            volume_handle: Some(vol_id),
            read_only: gce.read_only.filter(|r| *r),
            fs_type: gce.fs_type.clone(),
            volume_attributes: Some(HashMap::from([("partition".to_string(), partition)])),
            ..Default::default()
        };
        translate_topology_from_in_tree_to_csi(pv, GCE_PD_TOPOLOGY_KEY);
        pv.spec.gce_persistent_disk = None;
        pv.spec.csi = Some(csi);
        pv.spec.access_modes = backward_compatible_access_modes(&pv.spec.access_modes);
        Ok(())
    }

    /// `CanSupport` (`gce_pd.go:317-319`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.gce_persistent_disk.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        GCE_PD_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        GCE_PD_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`gce_pd.go:339-378`).
    fn repair_volume_handle(&self, volume_handle: &str, node_id: &str) -> Result<String, String> {
        let tok: Vec<&str> = volume_handle.split('/').collect();
        if tok.len() < VOL_ID_TOTAL_ELEMENTS {
            return Err(format!(
                "volume handle has wrong number of elements; got {}, wanted {} or more",
                tok.len(),
                VOL_ID_TOTAL_ELEMENTS
            ));
        }
        if tok[VOL_ID_PROJECT_VALUE] != UNSPECIFIED_VALUE {
            return Ok(volume_handle.to_string());
        }
        let node_tok: Vec<&str> = node_id.split('/').collect();
        if node_tok.len() < VOL_ID_TOTAL_ELEMENTS {
            return Err(format!(
                "node handle has wrong number of elements; got {}, wanted {} or more",
                node_tok.len(),
                VOL_ID_TOTAL_ELEMENTS
            ));
        }
        match tok[VOL_ID_REGIONALITY_VALUE] {
            "zones" => {
                let zone = if tok[VOL_ID_ZONE_VALUE] == UNSPECIFIED_VALUE {
                    node_tok[VOL_ID_ZONE_VALUE]
                } else {
                    tok[VOL_ID_ZONE_VALUE]
                };
                Ok(format!(
                    "projects/{}/zones/{}/disks/{}",
                    node_tok[VOL_ID_PROJECT_VALUE], zone, tok[VOL_ID_DISK_NAME_VALUE]
                ))
            }
            "regions" => {
                let region = if tok[VOL_ID_ZONE_VALUE] == UNSPECIFIED_VALUE {
                    gce_get_region_from_zones(&[node_tok[VOL_ID_ZONE_VALUE]]).map_err(|e| {
                        format!(
                            "failed to get region from zone {}: {e}",
                            node_tok[VOL_ID_ZONE_VALUE]
                        )
                    })?
                } else {
                    tok[VOL_ID_ZONE_VALUE].to_string()
                };
                Ok(format!(
                    "projects/{}/regions/{}/disks/{}",
                    node_tok[VOL_ID_PROJECT_VALUE], region, tok[VOL_ID_DISK_NAME_VALUE]
                ))
            }
            other => Err(format!(
                "expected volume handle to have zones or regions regionality value, got: {other}"
            )),
        }
    }
}

// ---- AWS EBS (`plugins/aws_ebs.go`) -----------------------------------------

struct AwsElasticBlockStoreTranslator;

/// `KubernetesVolumeIDToEBSVolumeID` (`aws_ebs.go:243-279`): `aws://<zone>/<id>`,
/// `aws:///<id>` or a bare `<id>`; `awsVolumeRegMatch` is `^vol-[^/]*$`.
fn kubernetes_volume_id_to_ebs_volume_id(kubernetes_id: &str) -> Result<String, String> {
    let Some(rest) = kubernetes_id.strip_prefix("aws://") else {
        // Assume a bare aws volume id (vol-1234...)
        return Ok(kubernetes_id.to_string());
    };
    // url.Path: everything from the first '/' after the host.
    let path = rest.find('/').map_or("", |i| &rest[i..]);
    let aws_id = path.trim_matches('/');
    // We sanity check the resulting volume; the two known formats are
    // vol-12345678 and vol-12345678abcdef01
    if !(aws_id.starts_with("vol-") && !aws_id.contains('/')) {
        return Err(format!("Invalid format for AWS volume ({kubernetes_id})"));
    }
    Ok(aws_id.to_string())
}

impl InTreePlugin for AwsElasticBlockStoreTranslator {
    /// `TranslateInTreePVToCSI` (`aws_ebs.go:139-168`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(ebs) = pv.spec.aws_elastic_block_store.clone() else {
            return Err("pv is nil or AWS EBS not defined on pv".into());
        };
        let volume_handle = kubernetes_volume_id_to_ebs_volume_id(&ebs.volume_id)
            .map_err(|e| format!("failed to translate Kubernetes ID to EBS Volume ID {e}"))?;
        let csi = CSIVolumeSource {
            driver: AWS_EBS_DRIVER_NAME.to_string(),
            volume_handle: Some(volume_handle),
            read_only: ebs.read_only.filter(|r| *r),
            fs_type: ebs.fs_type.clone(),
            volume_attributes: Some(HashMap::from([(
                "partition".to_string(),
                ebs.partition.unwrap_or(0).to_string(),
            )])),
            ..Default::default()
        };
        translate_topology_from_in_tree_to_csi(pv, AWS_EBS_TOPOLOGY_KEY);
        pv.spec.aws_elastic_block_store = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `CanSupport` (`aws_ebs.go:206-208`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.aws_elastic_block_store.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        AWS_EBS_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        AWS_EBS_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`aws_ebs.go:227-229`).
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
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
