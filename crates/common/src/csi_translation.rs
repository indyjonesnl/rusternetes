//! In-tree -> CSI PersistentVolume translation: a port of
//! `staging/src/k8s.io/csi-translation-lib` (`translate.go`,
//! `plugins/in_tree_volume.go`, `plugins/gce_pd.go`, `plugins/aws_ebs.go`,
//! `plugins/portworx.go`, `plugins/vsphere_volume.go`, `plugins/azure_file.go`,
//! `plugins/azure_disk.go`, `plugins/openstack_cinder.go`).
//!
//! Shared by the external-attacher (`CsiAttacher`) and the scheduler's
//! VolumeBinding plugin (`tryTranslatePVToCSI`). Plugins ported so far: GCE PD,
//! AWS EBS, Portworx, vSphere, Azure File, Azure Disk and Cinder: every in-tree
//! plugin of `translate.go:34-42`.
//!
//! `TranslateCSIPVToInTree` (`translate.go:116-128`) is ported for the same
//! two plugins, with `translateTopologyFromCSIToInTree` /
//! `regionTopologyHandler` (`in_tree_volume.go:275-397`).
//!
//! `TranslateInTreeInlineVolumeToCSI` (`translate.go:67-90`), with
//! `IsInlineMigratable` (`:196-204`) and the volume branch of
//! `GetInTreePluginNameFromSpec` (`:145-164`), is ported for the same two
//! plugins (`gce_pd.go:166-202`, `aws_ebs.go:104-135`).

use crate::resources::pod::Volume;
use crate::resources::volume::PersistentVolumeMode;
use crate::resources::volume::{
    AWSElasticBlockStoreVolumeSource, CSIVolumeSource, GCEPersistentDiskVolumeSource, NodeSelector,
    NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeAccessMode, SecretReference,
    StorageClass, TopologySelectorLabelRequirement, TopologySelectorTerm, VolumeNodeAffinity,
};
use crate::resources::PersistentVolume;
use crate::types::ObjectMeta;
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
pub const LABEL_FAILURE_DOMAIN_BETA_ZONE: &str = "failure-domain.beta.kubernetes.io/zone";
const LABEL_FAILURE_DOMAIN_BETA_REGION: &str = "failure-domain.beta.kubernetes.io/region";
pub const LABEL_TOPOLOGY_ZONE: &str = "topology.kubernetes.io/zone";
const LABEL_TOPOLOGY_REGION: &str = "topology.kubernetes.io/region";
/// `GCEPDTopologyKey` (`gce_pd.go:38`).
pub const GCE_PD_TOPOLOGY_KEY: &str = "topology.gke.io/zone";
/// `AWSEBSTopologyKey` (`aws_ebs.go:39`).
pub const AWS_EBS_TOPOLOGY_KEY: &str = "topology.ebs.csi.aws.com/zone";
/// `UnspecifiedValue` (`gce_pd.go:54`).
const UNSPECIFIED_VALUE: &str = "UNSPECIFIED";

/// `InTreePlugin` (`in_tree_volume.go:31`), the PV-side subset.
pub trait InTreePlugin {
    /// Input PV is already a private copy and may be modified.
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String>;
    /// `TranslateInTreeStorageClassToCSI` (`in_tree_volume.go:34-36`): input is
    /// already a private copy and may be modified.
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String>;
    /// `TranslateCSIPVToInTree` (`in_tree_volume.go:48-51`): input is
    /// already a private copy and may be modified.
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String>;
    /// `TranslateInTreeInlineVolumeToCSI` (`in_tree_volume.go:38-42`): wraps
    /// the inline source's CSI translation in a new PV.
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        pod_namespace: &str,
    ) -> Result<PersistentVolume, String>;
    fn can_support(&self, pv: &PersistentVolume) -> bool;
    /// `CanSupportInline` (`in_tree_volume.go:57-59`).
    fn can_support_inline(&self, volume: &Volume) -> bool;
    fn in_tree_plugin_name(&self) -> &'static str;
    fn csi_plugin_name(&self) -> &'static str;
    fn repair_volume_handle(&self, volume_handle: &str, node_id: &str) -> Result<String, String>;
}

/// `inTreePlugins` (`translate.go:32-42`), the ported subset.
static GCE_PD: GcePersistentDiskTranslator = GcePersistentDiskTranslator;
static AWS_EBS: AwsElasticBlockStoreTranslator = AwsElasticBlockStoreTranslator;
static PORTWORX: PortworxCsiTranslator = PortworxCsiTranslator;
static VSPHERE: VsphereCsiTranslator = VsphereCsiTranslator;
static AZURE_FILE: AzureFileCsiTranslator = AzureFileCsiTranslator;
static AZURE_DISK: AzureDiskCsiTranslator = AzureDiskCsiTranslator;
static CINDER: OsCinderCsiTranslator = OsCinderCsiTranslator;
fn in_tree_plugins() -> [&'static dyn InTreePlugin; 7] {
    [
        &GCE_PD,
        &AWS_EBS,
        &PORTWORX,
        &VSPHERE,
        &AZURE_FILE,
        &AZURE_DISK,
        &CINDER,
    ]
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

    /// `TranslateInTreeInlineVolumeToCSI` (`translate.go:67-90`). Inline
    /// volumes only support `Filesystem` (not block), so a VolumeMode the
    /// plugin left unset is set to Filesystem here: the default PV
    /// initialization that populates it does not apply to inline volumes.
    pub fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        for plugin in in_tree_plugins() {
            if plugin.can_support_inline(volume) {
                let mut pv =
                    plugin.translate_in_tree_inline_volume_to_csi(volume, pod_namespace)?;
                if pv.spec.volume_mode.is_none() {
                    pv.spec.volume_mode = Some(PersistentVolumeMode::Filesystem);
                }
                return Ok(pv);
            }
        }
        Err(format!(
            "could not find in-tree plugin translation logic for {:?}",
            volume.name
        ))
    }

    /// `IsInlineMigratable` (`translate.go:196-204`).
    pub fn is_inline_migratable(&self, volume: &Volume) -> bool {
        in_tree_plugins()
            .iter()
            .any(|p| p.can_support_inline(volume))
    }

    /// `GetInTreePluginNameFromSpec` (`translate.go:145-164`): the PV is
    /// consulted when given, else the inline volume.
    pub fn get_in_tree_plugin_name_from_spec(
        &self,
        pv: Option<&PersistentVolume>,
        volume: Option<&Volume>,
    ) -> Result<String, String> {
        if let Some(pv) = pv {
            return self.get_in_tree_plugin_name_from_pv(pv);
        }
        let Some(volume) = volume else {
            return Err("both persistent volume and volume are nil".to_string());
        };
        in_tree_plugins()
            .iter()
            .find(|p| p.can_support_inline(volume))
            .map(|p| p.in_tree_plugin_name().to_string())
            .ok_or_else(|| {
                format!(
                    "could not find in-tree plugin name from volume {}",
                    volume.name
                )
            })
    }

    /// `TranslateCSIPVToInTree` (`translate.go:116-128`): the plugin is
    /// chosen by the CSI source's driver name; the input is not modified.
    pub fn translate_csi_pv_to_in_tree(
        &self,
        pv: &PersistentVolume,
    ) -> Result<PersistentVolume, String> {
        let Some(csi) = pv.spec.csi.as_ref() else {
            return Err("CSI persistent volume was nil".to_string());
        };
        let mut copied = pv.clone();
        for plugin in in_tree_plugins() {
            if csi.driver == plugin.csi_plugin_name() {
                plugin.translate_csi_pv_to_in_tree(&mut copied)?;
                return Ok(copied);
            }
        }
        Err(format!(
            "could not find in-tree plugin translation logic for {}",
            csi.driver
        ))
    }

    /// `TranslateInTreeStorageClassToCSI` (`translate.go:54-63`); the input is
    /// not modified.
    pub fn translate_in_tree_storage_class_to_csi(
        &self,
        in_tree_plugin_name: &str,
        sc: &StorageClass,
    ) -> Result<StorageClass, String> {
        let mut new_sc = sc.clone();
        for plugin in in_tree_plugins() {
            if in_tree_plugin_name == plugin.in_tree_plugin_name() {
                plugin.translate_in_tree_storage_class_to_csi(&mut new_sc)?;
                return Ok(new_sc);
            }
        }
        Err(format!(
            "could not find in-tree storage class parameter translation logic for {in_tree_plugin_name:?}"
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

// ---- StorageClass translation helpers (`plugins/in_tree_volume.go`) ---------

/// `fsTypeKey` / `csiFsTypeKey` / `zoneKey` / `zonesKey` (`in_tree_volume.go:73-79`).
const FS_TYPE_KEY: &str = "fstype";
const CSI_FS_TYPE_KEY: &str = "csi.storage.k8s.io/fstype";
const ZONE_KEY: &str = "zone";
const ZONES_KEY: &str = "zones";
/// `iopsPerGBKey` / `allowIncreaseIOPSKey` (`aws_ebs.go:42-47`).
const IOPS_PER_GB_KEY: &str = "iopspergb";
const ALLOW_INCREASE_IOPS_KEY: &str = "allowautoiopspergbincrease";

/// `generateToplogySelectors` (`gce_pd.go:68-79`).
fn generate_topology_selectors(key: &str, values: Vec<String>) -> Vec<TopologySelectorTerm> {
    vec![TopologySelectorTerm {
        match_label_expressions: Some(vec![TopologySelectorLabelRequirement {
            key: key.to_string(),
            values,
        }]),
    }]
}

/// `translateAllowedTopologies` (`in_tree_volume.go:312-336`).
fn translate_allowed_topologies(
    terms: &[TopologySelectorTerm],
    key: &str,
) -> Vec<TopologySelectorTerm> {
    terms
        .iter()
        .map(|term| TopologySelectorTerm {
            match_label_expressions: term.match_label_expressions.as_ref().map(|exps| {
                exps.iter()
                    .map(|exp| {
                        if exp.key == LABEL_FAILURE_DOMAIN_BETA_ZONE
                            || exp.key == LABEL_TOPOLOGY_ZONE
                        {
                            TopologySelectorLabelRequirement {
                                key: key.to_string(),
                                values: exp.values.clone(),
                            }
                        } else {
                            // "Other topologies are passed through unchanged."
                            exp.clone()
                        }
                    })
                    .collect()
            }),
        })
        .collect()
}

/// The shared tail of `gcePersistentDiskCSITranslator` /
/// `awsElasticBlockStoreCSITranslator.TranslateInTreeStorageClassToCSI`
/// (`gce_pd.go:82-114`, `aws_ebs.go:61-97`): `ebs` selects the one extra
/// parameter rule (`iopsPerGBKey`) EBS has over GCE PD.
fn translate_zone_storage_class(
    sc: &mut StorageClass,
    topology_key: &str,
    ebs: bool,
) -> Result<(), String> {
    let mut generated: Vec<TopologySelectorTerm> = Vec::new();
    let mut np: HashMap<String, String> = HashMap::new();
    for (k, v) in sc.parameters.iter().flatten() {
        match k.to_lowercase().as_str() {
            // "prefixed fstype parameter is stripped out by external provisioner"
            FS_TYPE_KEY => {
                np.insert(CSI_FS_TYPE_KEY.to_string(), v.clone());
            }
            // "Strip out zone and zones parameters and translate them into
            // topologies instead"
            ZONE_KEY => generated = generate_topology_selectors(topology_key, vec![v.clone()]),
            ZONES_KEY => {
                generated = generate_topology_selectors(
                    topology_key,
                    v.split(',').map(str::to_string).collect(),
                )
            }
            IOPS_PER_GB_KEY if ebs => {
                // "Keep iopsPerGBKey", and "preserve current in-tree volume
                // plugin behavior and allow the CSI driver to bump volume IOPS
                // when volume size * iopsPerGB is too low."
                np.insert(k.clone(), v.clone());
                np.insert(ALLOW_INCREASE_IOPS_KEY.to_string(), "true".to_string());
            }
            _ => {
                np.insert(k.clone(), v.clone());
            }
        }
    }

    let existing = sc.allowed_topologies.clone().unwrap_or_default();
    if !generated.is_empty() && !existing.is_empty() {
        return Err(
            "cannot simultaneously set allowed topologies and zone/zones parameters".to_string(),
        );
    } else if !generated.is_empty() {
        sc.allowed_topologies = Some(generated);
    } else if !existing.is_empty() {
        sc.allowed_topologies = Some(translate_allowed_topologies(&existing, topology_key));
    }
    sc.parameters = Some(np);
    Ok(())
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
        .get_or_insert(VolumeNodeAffinity { required: None })
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
pub fn topology_key_exist(key: &str, vna: Option<&VolumeNodeAffinity>) -> bool {
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

/// `regionParserFn` (`in_tree_volume.go:260`).
pub type RegionParser = fn(&[&str]) -> Result<String, String>;

/// `translateTopologyFromCSIToInTree` (`in_tree_volume.go:275-307`): replace
/// the CSI topology key with the Kubernetes zone label, add the region when a
/// `region_parser` is given, then label the PV with its zones if it is not
/// labelled already.
pub fn translate_topology_from_csi_to_in_tree(
    pv: &mut PersistentVolume,
    csi_topology_key: &str,
    region_parser: Option<RegionParser>,
) -> Result<(), String> {
    let (zone_label, _) = get_topology_label(pv);

    // 1. Replace all CSI topology to Kubernetes Zone label
    replace_topology(pv, csi_topology_key, zone_label);

    // 2. Take care of region topology if a regionParser is passed
    if let Some(region_parser) = region_parser {
        region_topology_handler(pv, region_parser)
            .map_err(|e| format!("Failed to handle region topology. error: {e}"))?;
    }

    // 3. Add labels about Kubernetes Topology
    let zone_vals = get_topology_values(pv, zone_label);
    if !zone_vals.is_empty() {
        pv.metadata
            .labels
            .get_or_insert_with(HashMap::new)
            .entry(zone_label.to_string())
            .or_insert_with(|| zone_vals.join(LABEL_MULTI_ZONE_DELIMITER));
    }
    Ok(())
}

/// `regionTopologyHandler` (`in_tree_volume.go:342-397`): every term without
/// a region requirement gets one parsed from that term's zones, and the PV is
/// labelled with the region when exactly one exists.
fn region_topology_handler(
    pv: &mut PersistentVolume,
    region_parser: RegionParser,
) -> Result<(), String> {
    let (zone_label, region_label) = get_topology_label(pv);
    // Make sure the necessary fields exist
    let Some(required) = pv
        .spec
        .node_affinity
        .as_mut()
        .and_then(|a| a.required.as_mut())
        .filter(|r| !r.node_selector_terms.is_empty())
    else {
        return Ok(());
    };

    // process each term
    for term in &mut required.node_selector_terms {
        // In the first loop, see if regionLabel already exist
        let mut region_exist = false;
        let mut zone_vals: Vec<String> = Vec::new();
        for r in term.match_expressions.iter().flatten() {
            if r.key == region_label {
                region_exist = true;
                break;
            } else if r.key == zone_label {
                zone_vals.extend(r.values.iter().flatten().cloned());
            }
        }
        if region_exist {
            // Regionlabel already exist in this term, skip it
            continue;
        }
        // If no regionLabel found, generate region label from the zoneLabel
        // we collect from this term
        let zones: Vec<&str> = zone_vals.iter().map(String::as_str).collect();
        let region_val = region_parser(&zones)?;
        term.match_expressions
            .get_or_insert_with(Vec::new)
            .push(NodeSelectorRequirement {
                key: region_label.to_string(),
                operator: "In".to_string(),
                values: Some(vec![region_val]),
            });
    }

    // Add region label
    let region_vals = get_topology_values(pv, region_label);
    if let [region] = region_vals.as_slice() {
        // We should only have exactly 1 region value
        pv.metadata
            .labels
            .get_or_insert_with(HashMap::new)
            .entry(region_label.to_string())
            .or_insert_with(|| region.clone());
    }
    Ok(())
}

/// `%v` of a `sets.String` (a `map[string]sets.Empty`): `map[a:{} b:{}]`.
fn go_string_set(set: &BTreeSet<String>) -> String {
    let items: Vec<String> = set.iter().map(|k| format!("{k}:{{}}")).collect();
    format!("map[{}]", items.join(" "))
}

/// `strconv.Atoi`, with its error text.
fn atoi(s: &str) -> Result<i64, String> {
    s.parse::<i64>().map_err(|e| {
        let why = match e.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        format!("strconv.Atoi: parsing {s:?}: {why}")
    })
}

// ---- GCE PD (`plugins/gce_pd.go`) -------------------------------------------

pub struct GcePersistentDiskTranslator;

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

/// The PV an inline translation returns: upstream sets only `ObjectMeta.Name`
/// (`gce_pd.go:186-190`, `aws_ebs.go:113-118`), no TypeMeta, uid or timestamp.
fn inline_pv(name: String) -> PersistentVolume {
    PersistentVolume {
        type_meta: Default::default(),
        metadata: ObjectMeta {
            name,
            ..Default::default()
        },
        spec: Default::default(),
        status: None,
    }
}

/// `gceGetRegionFromZones` (`gce_pd.go:380-400`).
pub fn gce_get_region_from_zones(zones: &[&str]) -> Result<String, String> {
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
            "multiple or no regions gotten from zones, got: {}",
            go_string_set(&regions)
        ));
    }
    Ok(regions.into_iter().next().unwrap())
}

/// `pdNameFromVolumeID` (`gce_pd.go:373-379`).
fn pd_name_from_volume_id(id: &str) -> Result<String, String> {
    let split: Vec<&str> = id.split('/').collect();
    if split.len() < VOL_ID_TOTAL_ELEMENTS {
        return Err(format!(
            "failed to get id components.Got: {}, wanted {} components or more. ",
            split.len(),
            VOL_ID_TOTAL_ELEMENTS
        ));
    }
    Ok(split[VOL_ID_DISK_NAME_VALUE].to_string())
}

impl InTreePlugin for GcePersistentDiskTranslator {
    /// `TranslateInTreePVToCSI` (`gce_pd.go:204-258`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        translate_zone_storage_class(sc, GCE_PD_TOPOLOGY_KEY, false)
    }
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

    /// `TranslateCSIPVToInTree` (`gce_pd.go:269-306`). Go's zero values are
    /// omitted on the wire, so an empty fsType and a false readOnly stay unset.
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let pd_name = pd_name_from_volume_id(csi.volume_handle.as_deref().unwrap_or_default())?;
        let mut gce = GCEPersistentDiskVolumeSource {
            pd_name,
            fs_type: csi.fs_type.clone().filter(|f| !f.is_empty()),
            partition: None,
            read_only: csi.read_only.filter(|r| *r),
        };
        if let Some(partition) = csi
            .volume_attributes
            .as_ref()
            .and_then(|a| a.get("partition"))
            .filter(|p| !p.is_empty())
        {
            let part = atoi(partition)
                .map_err(|e| format!("Failed to convert partition {partition} to integer: {e}"))?;
            gce.partition = Some(part as i32).filter(|p| *p != 0);
        }

        // translate CSI topology to In-tree topology for rollback compatibility
        translate_topology_from_csi_to_in_tree(
            pv,
            GCE_PD_TOPOLOGY_KEY,
            Some(gce_get_region_from_zones),
        )
        .map_err(|e| format!("failed to translate topology. PV:{pv:?}. Error:{e}"))?;

        pv.spec.csi = None;
        pv.spec.gce_persistent_disk = Some(gce);
        Ok(())
    }

    /// `CanSupport` (`gce_pd.go:317-319`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.gce_persistent_disk.is_some()
    }

    /// `CanSupportInline` (`gce_pd.go:316-318`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.gce_persistent_disk.is_some()
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`gce_pd.go:166-202`). The handle
    /// is always zone-unspecified: an inline volume carries no zone labels.
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(pd_source) = volume.legacy_sources.gce_persistent_disk.as_ref() else {
            return Err("volume is nil or GCE PD not defined on volume".into());
        };
        let partition = match pd_source.partition {
            Some(p) if p != 0 => p.to_string(),
            _ => String::new(),
        };
        let read_only = pd_source.read_only.unwrap_or(false);
        let am = if read_only {
            PersistentVolumeAccessMode::ReadOnlyMany
        } else {
            PersistentVolumeAccessMode::ReadWriteOnce
        };
        let csi = CSIVolumeSource {
            driver: GCE_PD_DRIVER_NAME.to_string(),
            volume_handle: Some(format!(
                "projects/{UNSPECIFIED_VALUE}/zones/{UNSPECIFIED_VALUE}/disks/{}",
                pd_source.pd_name
            )),
            read_only: Some(read_only).filter(|r| *r),
            fs_type: pd_source.fs_type.clone(),
            volume_attributes: Some(HashMap::from([("partition".to_string(), partition)])),
            ..Default::default()
        };
        // Must be unique per disk as it is used as the unique part of the
        // staging path
        let mut pv = inline_pv(format!("{GCE_PD_DRIVER_NAME}-{}", pd_source.pd_name));
        pv.spec.csi = Some(csi);
        pv.spec.access_modes = vec![am];
        pv.spec.volume_mode = Some(PersistentVolumeMode::Filesystem);
        Ok(pv)
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

pub struct AwsElasticBlockStoreTranslator;

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

/// `getAwsRegionFromZones` (`aws_ebs.go:280-305`). AWS zones can be in four
/// forms: us-west-2a, us-gov-east-1a, us-west-2-lax-1a (local zone) and
/// us-east-1-wl1-bos-wlz-1 (wavelength).
pub fn get_aws_region_from_zones(zones: &[&str]) -> Result<String, String> {
    if zones.is_empty() {
        return Err("no zones specified".into());
    }
    let mut regions = BTreeSet::new();
    for zone in zones {
        let mut split: Vec<&str> = zone.split('-').collect();
        let n = split.len();
        if (n == 3 || n == 4) && split[n - 1].len() == 2 {
            // this would break if we ever have a location with more than 9
            // regions, ie us-west-10.
            split[n - 1] = &split[n - 1][..1];
            regions.insert(split.join("-"));
        } else if n == 5 || n == 7 {
            // local zone or wavelength
            regions.insert(split[..3].join("-"));
        } else {
            return Err(format!(
                "Unexpected zone format: {zone} is not a valid AWS zone"
            ));
        }
    }
    if regions.len() != 1 {
        return Err(format!(
            "multiple or no regions gotten from zones, got: {}",
            go_string_set(&regions)
        ));
    }
    Ok(regions.into_iter().next().unwrap())
}

impl InTreePlugin for AwsElasticBlockStoreTranslator {
    /// `TranslateInTreePVToCSI` (`aws_ebs.go:139-168`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        translate_zone_storage_class(sc, AWS_EBS_TOPOLOGY_KEY, true)
    }
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

    /// `TranslateCSIPVToInTree` (`aws_ebs.go:170-201`). A `partition`
    /// attribute that is present must parse, even when empty.
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let mut ebs = AWSElasticBlockStoreVolumeSource {
            volume_id: csi.volume_handle.clone().unwrap_or_default(),
            fs_type: csi.fs_type.clone().filter(|f| !f.is_empty()),
            partition: None,
            read_only: csi.read_only.filter(|r| *r),
        };
        if let Some(partition) = csi
            .volume_attributes
            .as_ref()
            .and_then(|a| a.get("partition"))
        {
            let part = atoi(partition)
                .map_err(|e| format!("failed to convert partition {partition} to integer: {e}"))?;
            ebs.partition = Some(part as i32).filter(|p| *p != 0);
        }

        // translate CSI topology to In-tree topology for rollback compatibility
        translate_topology_from_csi_to_in_tree(
            pv,
            AWS_EBS_TOPOLOGY_KEY,
            Some(get_aws_region_from_zones),
        )
        .map_err(|e| format!("failed to translate topology. PV:{pv:?}. Error:{e}"))?;

        pv.spec.csi = None;
        pv.spec.aws_elastic_block_store = Some(ebs);
        Ok(())
    }

    /// `CanSupport` (`aws_ebs.go:206-208`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.aws_elastic_block_store.is_some()
    }

    /// `CanSupportInline` (`aws_ebs.go:213-215`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.aws_elastic_block_store.is_some()
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`aws_ebs.go:104-135`). Unlike GCE
    /// PD it leaves `VolumeMode` unset; the dispatcher fills it in.
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(ebs_source) = volume.legacy_sources.aws_elastic_block_store.as_ref() else {
            return Err("volume is nil or AWS EBS not defined on volume".into());
        };
        let volume_handle = kubernetes_volume_id_to_ebs_volume_id(&ebs_source.volume_id)
            .map_err(|e| format!("failed to translate Kubernetes ID to EBS Volume ID {e}"))?;
        // Must be unique per disk as it is used as the unique part of the
        // staging path
        let mut pv = inline_pv(format!("{AWS_EBS_DRIVER_NAME}-{volume_handle}"));
        pv.spec.csi = Some(CSIVolumeSource {
            driver: AWS_EBS_DRIVER_NAME.to_string(),
            volume_handle: Some(volume_handle),
            read_only: ebs_source.read_only.filter(|r| *r),
            fs_type: ebs_source.fs_type.clone(),
            volume_attributes: Some(HashMap::from([(
                "partition".to_string(),
                ebs_source.partition.unwrap_or(0).to_string(),
            )])),
            ..Default::default()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        Ok(pv)
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

// ---- Portworx (`plugins/portworx.go`) ---------------------------------------

/// `PortworxDriverName` (`portworx.go:32`).
pub const PORTWORX_DRIVER_NAME: &str = "pxd.portworx.com";
/// `PortworxVolumePluginName` (`portworx.go:31`).
pub const PORTWORX_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/portworx-volume";
/// `OpenStorageAuthSecretNameKey` / `...NamespaceKey` (`portworx.go:34-35`).
const OPEN_STORAGE_AUTH_SECRET_NAME_KEY: &str = "openstorage.io/auth-secret-name";
const OPEN_STORAGE_AUTH_SECRET_NAMESPACE_KEY: &str = "openstorage.io/auth-secret-namespace";
/// `csiParameterPrefix` (`portworx.go:37`).
const CSI_PARAMETER_PREFIX: &str = "csi.storage.k8s.io/";
/// The six `prefixed*Secret{Name,Namespace}Key` pairs (`portworx.go:39-55`).
const PORTWORX_CSI_SECRET_KINDS: [&str; 6] = [
    "provisioner",
    "controller-publish",
    "node-publish",
    "node-stage",
    "controller-expand",
    "node-expand",
];

struct PortworxCsiTranslator;

impl InTreePlugin for PortworxCsiTranslator {
    /// `TranslateInTreeStorageClassToCSI` (`portworx.go:68-98`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        let mut params: HashMap<String, String> = HashMap::new();
        for (k, v) in sc.parameters.iter().flatten() {
            match k.to_lowercase().as_str() {
                OPEN_STORAGE_AUTH_SECRET_NAME_KEY => {
                    for kind in PORTWORX_CSI_SECRET_KINDS {
                        params.insert(
                            format!("{CSI_PARAMETER_PREFIX}{kind}-secret-name"),
                            v.clone(),
                        );
                    }
                }
                OPEN_STORAGE_AUTH_SECRET_NAMESPACE_KEY => {
                    for kind in PORTWORX_CSI_SECRET_KINDS {
                        params.insert(
                            format!("{CSI_PARAMETER_PREFIX}{kind}-secret-namespace"),
                            v.clone(),
                        );
                    }
                }
                // "All other parameters can be copied as is"
                _ => {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        if !params.is_empty() {
            sc.parameters = Some(params);
        }
        sc.provisioner = PORTWORX_DRIVER_NAME.to_string();
        Ok(())
    }

    /// `TranslateInTreePVToCSI` (`portworx.go:128-158`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(px) = pv.spec.more_sources.portworx_volume.clone() else {
            return Err("pv is nil or PortworxVolume not defined on pv".into());
        };
        // `metav1.HasAnnotation` is a key-presence check.
        let annotations = pv.metadata.annotations.as_ref();
        let secret_ref = match (
            annotations.and_then(|a| a.get(OPEN_STORAGE_AUTH_SECRET_NAME_KEY)),
            annotations.and_then(|a| a.get(OPEN_STORAGE_AUTH_SECRET_NAMESPACE_KEY)),
        ) {
            (Some(name), Some(namespace)) => Some(SecretReference {
                name: Some(name.clone()),
                namespace: Some(namespace.clone()),
            }),
            _ => None,
        };
        // `fsType` is a plain string upstream; empty means unset on the wire.
        let csi = CSIVolumeSource {
            driver: PORTWORX_DRIVER_NAME.to_string(),
            volume_handle: Some(px.volume_id),
            fs_type: Some(px.fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(HashMap::new()),
            controller_publish_secret_ref: secret_ref.clone(),
            node_stage_secret_ref: secret_ref.clone(),
            node_publish_secret_ref: secret_ref.clone(),
            controller_expand_secret_ref: secret_ref.clone(),
            node_expand_secret_ref: secret_ref,
            ..Default::default()
        };
        pv.spec.more_sources.portworx_volume = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `TranslateCSIPVToInTree` (`portworx.go:160-176`).
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let px = crate::resources::pv_sources::PortworxVolumeSource {
            volume_id: csi.volume_handle.unwrap_or_default(),
            fs_type: csi.fs_type.unwrap_or_default(),
            read_only: csi.read_only.unwrap_or(false),
        };
        pv.spec.csi = None;
        pv.spec.more_sources.portworx_volume = Some(px);
        Ok(())
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`portworx.go:100-126`).
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(px) = volume
            .legacy_sources
            .portworx_volume
            .as_ref()
            .and_then(|v| {
                serde_json::from_value::<crate::resources::pv_sources::PortworxVolumeSource>(
                    v.clone(),
                )
                .ok()
            })
        else {
            return Err("volume is nil or PortworxVolume not defined on volume".into());
        };
        let am = if px.read_only {
            PersistentVolumeAccessMode::ReadOnlyMany
        } else {
            PersistentVolumeAccessMode::ReadWriteOnce
        };
        let mut pv = inline_pv(format!("{PORTWORX_DRIVER_NAME}-{}", px.volume_id));
        pv.spec.csi = Some(CSIVolumeSource {
            driver: PORTWORX_DRIVER_NAME.to_string(),
            volume_handle: Some(px.volume_id),
            fs_type: Some(px.fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(HashMap::new()),
            ..Default::default()
        });
        pv.spec.access_modes = vec![am];
        Ok(pv)
    }

    /// `CanSupport` (`portworx.go:182-184`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.more_sources.portworx_volume.is_some()
    }

    /// `CanSupportInline` (`portworx.go:187-189`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.portworx_volume.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        PORTWORX_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        PORTWORX_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`portworx.go:204-206`): identity.
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
    }
}

// ---- vSphere (`plugins/vsphere_volume.go`) ----------------------------------

/// `VSphereDriverName` (`vsphere_volume.go:33`).
pub const VSPHERE_DRIVER_NAME: &str = "csi.vsphere.vmware.com";
/// `VSphereInTreePluginName` (`vsphere_volume.go:35`).
pub const VSPHERE_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/vsphere-volume";
/// `vSphereCSITopologyZoneKey` / `...RegionKey` (`vsphere_volume.go:38,41`).
const VSPHERE_CSI_TOPOLOGY_ZONE_KEY: &str = "topology.csi.vmware.com/zone";
const VSPHERE_CSI_TOPOLOGY_REGION_KEY: &str = "topology.csi.vmware.com/region";
/// `paramStoragePolicyName` / `paramcsiMigration` (`vsphere_volume.go:44,47`).
const VSPHERE_PARAM_STORAGE_POLICY_NAME: &str = "storagepolicyname";
const VSPHERE_PARAM_CSI_MIGRATION: &str = "csimigration";

/// `translateTopologyFromInTreevSphereToCSI` (`vsphere_volume.go:243-269`).
fn translate_topology_from_in_tree_vsphere_to_csi(pv: &mut PersistentVolume) {
    let (zone_label, region_label) = get_topology_label(pv);
    for (label_key, csi_key) in [
        (zone_label, VSPHERE_CSI_TOPOLOGY_ZONE_KEY),
        (region_label, VSPHERE_CSI_TOPOLOGY_REGION_KEY),
    ] {
        if !get_topology_values(pv, label_key).is_empty() {
            replace_topology(pv, label_key, csi_key);
        } else if let Some(label) = pv
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(label_key))
            .filter(|l| !l.is_empty())
            .cloned()
        {
            // upstream ignores the error here
            let _ = add_topology(pv, csi_key, &[label]);
        }
    }
}

/// `AttributeInitialVolumeFilepath` (`vsphere_volume.go:63`).
const VSPHERE_ATTRIBUTE_INITIAL_VOLUME_FILEPATH: &str = "initialvolumefilepath";

/// `translateTopologyFromCSIToInTreevSphere` (`vsphere_volume.go:271-317`).
fn translate_topology_from_csi_to_in_tree_vsphere(pv: &mut PersistentVolume) {
    let (zone_label, region_label) = get_topology_label(pv);
    // Replace all CSI topology to Kubernetes Zone / Region label
    replace_topology(pv, VSPHERE_CSI_TOPOLOGY_ZONE_KEY, zone_label);
    replace_topology(pv, VSPHERE_CSI_TOPOLOGY_REGION_KEY, region_label);
    for label_key in [zone_label, region_label] {
        let vals = get_topology_values(pv, label_key);
        if let Some(first) = vals.first() {
            pv.metadata
                .labels
                .get_or_insert_with(Default::default)
                .entry(label_key.to_string())
                .or_insert_with(|| first.clone());
        }
    }
}

struct VsphereCsiTranslator;

impl InTreePlugin for VsphereCsiTranslator {
    /// `TranslateInTreeStorageClassToCSI` (`vsphere_volume.go:68-110`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        let mut params: HashMap<String, String> = HashMap::new();
        for (k, v) in sc.parameters.iter().flatten() {
            let (key, value) = match k.to_lowercase().as_str() {
                FS_TYPE_KEY => (CSI_FS_TYPE_KEY, v),
                VSPHERE_PARAM_STORAGE_POLICY_NAME => (VSPHERE_PARAM_STORAGE_POLICY_NAME, v),
                "datastore" => ("datastore-migrationparam", v),
                "diskformat" => ("diskformat-migrationparam", v),
                "hostfailurestotolerate" => ("hostfailurestotolerate-migrationparam", v),
                "forceprovisioning" => ("forceprovisioning-migrationparam", v),
                "cachereservation" => ("cachereservation-migrationparam", v),
                "diskstripes" => ("diskstripes-migrationparam", v),
                "objectspacereservation" => ("objectspacereservation-migrationparam", v),
                "iopslimit" => ("iopslimit-migrationparam", v),
                // "StorageClass parameter is not supported" (dropped)
                _ => continue,
            };
            params.insert(key.to_string(), value.clone());
        }
        params.insert(VSPHERE_PARAM_CSI_MIGRATION.to_string(), "true".to_string());
        if let Some(existing) = sc.allowed_topologies.as_ref().filter(|t| !t.is_empty()) {
            sc.allowed_topologies = Some(translate_allowed_topologies(
                existing,
                VSPHERE_CSI_TOPOLOGY_ZONE_KEY,
            ));
        }
        sc.parameters = Some(params);
        Ok(())
    }

    /// `TranslateInTreePVToCSI` (`vsphere_volume.go:141-163`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(vs) = pv.spec.more_sources.vsphere_volume.clone() else {
            return Err("pv is nil or VsphereVolume not defined on pv".into());
        };
        let mut attributes = HashMap::new();
        if !vs.storage_policy_name.is_empty() {
            attributes.insert(
                VSPHERE_PARAM_STORAGE_POLICY_NAME.to_string(),
                vs.storage_policy_name,
            );
        }
        let csi = CSIVolumeSource {
            driver: VSPHERE_DRIVER_NAME.to_string(),
            volume_handle: Some(vs.volume_path),
            fs_type: Some(vs.fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(attributes),
            ..Default::default()
        };
        translate_topology_from_in_tree_vsphere_to_csi(pv);
        pv.spec.more_sources.vsphere_volume = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`vsphere_volume.go:112-139`).
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(vs) = volume.legacy_sources.vsphere_volume.as_ref().and_then(|v| {
            serde_json::from_value::<crate::resources::pv_sources::VsphereVirtualDiskVolumeSource>(
                v.clone(),
            )
            .ok()
        }) else {
            return Err("volume is nil or VsphereVolume not defined on volume".into());
        };
        // Must be unique per disk as it is used as the unique part of the
        // staging path
        let mut pv = inline_pv(format!("{VSPHERE_DRIVER_NAME}-{}", vs.volume_path));
        let mut attributes = HashMap::new();
        if !vs.storage_policy_name.is_empty() {
            attributes.insert(
                VSPHERE_PARAM_STORAGE_POLICY_NAME.to_string(),
                vs.storage_policy_name,
            );
        }
        pv.spec.csi = Some(CSIVolumeSource {
            driver: VSPHERE_DRIVER_NAME.to_string(),
            volume_handle: Some(vs.volume_path),
            fs_type: Some(vs.fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(attributes),
            ..Default::default()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        Ok(pv)
    }

    /// `TranslateCSIPVToInTree` (`vsphere_volume.go:165-190`).
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let vs = crate::resources::pv_sources::VsphereVirtualDiskVolumeSource {
            fs_type: csi.fs_type.clone().unwrap_or_default(),
            volume_path: csi
                .volume_attributes
                .as_ref()
                .and_then(|a| a.get(VSPHERE_ATTRIBUTE_INITIAL_VOLUME_FILEPATH))
                .cloned()
                .unwrap_or_default(),
            ..Default::default()
        };
        // translate CSI topology to In-tree topology for rollback compatibility.
        translate_topology_from_csi_to_in_tree_vsphere(pv);
        pv.spec.csi = None;
        pv.spec.more_sources.vsphere_volume = Some(vs);
        Ok(())
    }

    /// `CanSupport` (`vsphere_volume.go:218-220`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.more_sources.vsphere_volume.is_some()
    }

    /// `CanSupportInline` (`vsphere_volume.go:224-226`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.vsphere_volume.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        VSPHERE_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        VSPHERE_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`vsphere_volume.go:232-234`): identity.
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
    }
}

// ---- Azure File (`plugins/azure_file.go`) -----------------------------------

/// `AzureFileDriverName` (`azure_file.go:34`).
pub const AZURE_FILE_DRIVER_NAME: &str = "file.csi.azure.com";
/// `AzureFileInTreePluginName` (`azure_file.go:36`).
pub const AZURE_FILE_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/azure-file";
/// `volumeIDTemplate` is `%s#%s#%s#%s#%s`; `shareNameField`;
/// `secretNameTemplate`; `resourceGroupAnnotation` (`azure_file.go:38-48`).
const AZURE_FILE_SEPARATOR: &str = "#";
const AZURE_FILE_SHARE_NAME_FIELD: &str = "sharename";
const AZURE_FILE_RESOURCE_GROUP_ANNOTATION: &str = "kubernetes.io/azure-file-resource-group";
/// `secretNameField` / `secretNamespaceField` / `defaultSecretNamespace`
/// (`azure_file.go:43-47`).
const AZURE_FILE_SECRET_NAME_FIELD: &str = "secretname";
const AZURE_FILE_SECRET_NAMESPACE_FIELD: &str = "secretnamespace";
const AZURE_FILE_DEFAULT_SECRET_NAMESPACE: &str = "default";

/// `secretNameFormatRE` (`azure_file.go:52`).
static AZURE_FILE_SECRET_NAME_FORMAT_RE: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| {
        regex::Regex::new(r"azure-storage-account-(.+)-secret").expect("static regex")
    });

/// `getStorageAccountName` (`azure_file.go:327-334`).
pub fn get_storage_account_name(secret_name: &str) -> Result<String, String> {
    AZURE_FILE_SECRET_NAME_FORMAT_RE
        .captures(secret_name)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| {
            format!(
                "could not get account name from {secret_name}, correct format: {}",
                AZURE_FILE_SECRET_NAME_FORMAT_RE.as_str()
            )
        })
}

struct AzureFileCsiTranslator;

impl InTreePlugin for AzureFileCsiTranslator {
    /// `TranslateInTreeStorageClassToCSI` (`azure_file.go:56-58`): `return sc, nil`.
    fn translate_in_tree_storage_class_to_csi(&self, _sc: &mut StorageClass) -> Result<(), String> {
        Ok(())
    }

    /// `TranslateInTreePVToCSI` (`azure_file.go:97-148`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(azure) = pv.spec.more_sources.azure_file.clone() else {
            return Err("pv is nil or Azure File source not defined on pv".into());
        };
        // `getStorageAccountName` failing falls back to the secret name.
        let account_name =
            get_storage_account_name(&azure.secret_name).unwrap_or(azure.secret_name.clone());
        let resource_group = pv
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(AZURE_FILE_RESOURCE_GROUP_ANNOTATION))
            .cloned()
            .unwrap_or_default();
        let namespace = match &azure.secret_namespace {
            Some(ns) => ns.clone(),
            None => pv
                .spec
                .claim_ref
                .as_ref()
                .and_then(|r| r.namespace.clone())
                .unwrap_or_default(),
        };
        if namespace.is_empty() {
            return Err(
                "could not find a secret namespace in PersistentVolumeSource or ClaimRef".into(),
            );
        }
        let sep = AZURE_FILE_SEPARATOR;
        let volume_id = format!(
            "{resource_group}{sep}{account_name}{sep}{}{sep}{}{sep}{namespace}",
            azure.share_name, pv.metadata.name
        );
        let csi = CSIVolumeSource {
            driver: AZURE_FILE_DRIVER_NAME.to_string(),
            node_stage_secret_ref: Some(SecretReference {
                name: Some(azure.secret_name),
                namespace: Some(namespace),
            }),
            // Go `bool` without omitempty-pointer: false is the unset value.
            read_only: Some(azure.read_only).filter(|r| *r),
            volume_attributes: Some(HashMap::from([(
                AZURE_FILE_SHARE_NAME_FIELD.to_string(),
                azure.share_name,
            )])),
            volume_handle: Some(volume_id),
            ..Default::default()
        };
        pv.spec.more_sources.azure_file = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`azure_file.go:62-110`).
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(azure) = volume.legacy_sources.azure_file.as_ref() else {
            return Err("volume is nil or Azure File not defined on volume".into());
        };
        let field = |k: &str| {
            azure
                .get(k)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        let secret_name = field("secretName");
        let share_name = field("shareName");
        let read_only = azure
            .get("readOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let account_name = get_storage_account_name(&secret_name).unwrap_or(secret_name.clone());
        let secret_namespace = if pod_namespace.is_empty() {
            AZURE_FILE_DEFAULT_SECRET_NAMESPACE
        } else {
            pod_namespace
        };
        let sep = AZURE_FILE_SEPARATOR;
        let volume_id = format!(
            "{sep}{account_name}{sep}{share_name}{sep}{}{sep}{secret_namespace}",
            volume.name
        );
        // Must be unique as it is used as the unique part of the staging path
        let mut pv = inline_pv(volume_id.clone());
        pv.spec.csi = Some(CSIVolumeSource {
            driver: AZURE_FILE_DRIVER_NAME.to_string(),
            volume_handle: Some(volume_id),
            read_only: Some(read_only).filter(|r| *r),
            volume_attributes: Some(HashMap::from([(
                AZURE_FILE_SHARE_NAME_FIELD.to_string(),
                share_name,
            )])),
            node_stage_secret_ref: Some(SecretReference {
                name: Some(secret_name),
                namespace: Some(secret_namespace.to_string()),
            }),
            ..Default::default()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteMany];
        Ok(pv)
    }

    /// `TranslateCSIPVToInTree` (`azure_file.go:150-221`).
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let mut azure = crate::resources::pv_sources::AzureFilePersistentVolumeSource {
            read_only: csi.read_only.unwrap_or(false),
            ..Default::default()
        };
        for (k, v) in csi.volume_attributes.iter().flatten() {
            match k.to_lowercase().as_str() {
                AZURE_FILE_SHARE_NAME_FIELD => azure.share_name = v.clone(),
                AZURE_FILE_SECRET_NAME_FIELD => azure.secret_name = v.clone(),
                AZURE_FILE_SECRET_NAMESPACE_FIELD => azure.secret_namespace = Some(v.clone()),
                _ => {}
            }
        }
        let mut resource_group = String::new();
        if let Some(r) = csi
            .node_stage_secret_ref
            .as_ref()
            .filter(|r| r.name.as_deref().is_some_and(|n| !n.is_empty()))
        {
            azure.secret_name = r.name.clone().unwrap_or_default();
            azure.secret_namespace = Some(r.namespace.clone().unwrap_or_default());
        }
        if azure.share_name.is_empty() || azure.secret_name.is_empty() {
            // `getFileShareInfo` (`azure_file.go:306-318`)
            let handle = csi.volume_handle.clone().unwrap_or_default();
            let segments: Vec<&str> = handle.split(AZURE_FILE_SEPARATOR).collect();
            if segments.len() < 3 {
                return Err(format!(
                    "error parsing volume id: {handle:?}, should at least contain two #"
                ));
            }
            if azure.share_name.is_empty() {
                azure.share_name = segments[2].to_string();
            }
            if azure.secret_name.is_empty() {
                azure.secret_name = format!("azure-storage-account-{}-secret", segments[1]);
            }
            resource_group = segments[0].to_string();
        }
        if azure.secret_namespace.is_none() {
            azure.secret_namespace = Some(AZURE_FILE_DEFAULT_SECRET_NAMESPACE.to_string());
        }
        pv.spec.csi = None;
        pv.spec.more_sources.azure_file = Some(azure);
        if !resource_group.is_empty() {
            pv.metadata
                .annotations
                .get_or_insert_with(Default::default)
                .insert(
                    AZURE_FILE_RESOURCE_GROUP_ANNOTATION.to_string(),
                    resource_group,
                );
        }
        Ok(())
    }

    /// `CanSupport` (`azure_file.go:223-225`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.more_sources.azure_file.is_some()
    }

    /// `CanSupportInline` (`azure_file.go:230-232`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.azure_file.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        AZURE_FILE_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        AZURE_FILE_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`azure_file.go:239-241`): identity.
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
    }
}

// ---- Azure Disk (`plugins/azure_disk.go`) -----------------------------------

/// `AzureDiskDriverName` (`azure_disk.go:32`).
pub const AZURE_DISK_DRIVER_NAME: &str = "disk.csi.azure.com";
/// `AzureDiskTopologyKey` (`azure_disk.go:34`).
const AZURE_DISK_TOPOLOGY_KEY: &str = "topology.disk.csi.azure.com/zone";
/// `AzureDiskInTreePluginName` (`azure_disk.go:36`).
pub const AZURE_DISK_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/azure-disk";
/// `azureDiskKind` / `azureDiskCachingMode` / `azureDiskFSType`
/// (`azure_disk.go:40-42`).
const AZURE_DISK_KIND: &str = "kind";
const AZURE_DISK_CACHING_MODE: &str = "cachingmode";
const AZURE_DISK_FS_TYPE: &str = "fstype";
/// `managed = string(v1.AzureManagedDisk)` (`azure_disk.go:48`).
const AZURE_DISK_MANAGED: &str = "Managed";

/// `managedDiskPathRE` / `unmanagedDiskPathRE` (`azure_disk.go:46-47`).
const MANAGED_DISK_PATH_RE: &str =
    ".*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/disks/(.+)";
const UNMANAGED_DISK_PATH_RE: &str = "http(?:.*)://(?:.*)/vhds/(.+)";
static MANAGED_DISK_PATH_REGEX: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(MANAGED_DISK_PATH_RE).expect("static regex"));
static UNMANAGED_DISK_PATH_REGEX: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(UNMANAGED_DISK_PATH_RE).expect("static regex"));
/// `unzonedCSIRegionRE` (`azure_disk.go:49`).
static UNZONED_CSI_REGION_REGEX: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new("^[0-9]+$").expect("static regex"));

/// `isManagedDisk` (`azure_disk.go:260-265`).
pub fn is_managed_disk(disk_uri: &str) -> bool {
    // `len(diskURI) > 4 && strings.ToLower(diskURI[:4]) == "http"`: Go slices
    // bytes, so compare bytes (a non-ASCII prefix can never equal "http").
    let b = disk_uri.as_bytes();
    !(b.len() > 4 && b[..4].eq_ignore_ascii_case(b"http"))
}

/// `getDiskName` (`azure_disk.go:267-279`).
pub fn get_disk_name(disk_uri: &str) -> Result<String, String> {
    let (re, pattern) = if is_managed_disk(disk_uri) {
        (&*MANAGED_DISK_PATH_REGEX, MANAGED_DISK_PATH_RE)
    } else {
        (&*UNMANAGED_DISK_PATH_REGEX, UNMANAGED_DISK_PATH_RE)
    };
    re.captures(disk_uri)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| {
            format!("could not get disk name from {disk_uri}, correct format: {pattern}")
        })
}

/// `replaceFailureDomainsToCSI` (`azure_disk.go:282-309`): topology values for
/// failure domains ("<number>") become "", the value the CSI driver expects.
fn azure_disk_replace_failure_domains_to_csi(
    terms: Vec<TopologySelectorTerm>,
) -> Vec<TopologySelectorTerm> {
    terms
        .into_iter()
        .map(|mut term| {
            for exp in term.match_label_expressions.iter_mut().flatten() {
                if exp.key == AZURE_DISK_TOPOLOGY_KEY {
                    for v in exp.values.iter_mut() {
                        if UNZONED_CSI_REGION_REGEX.is_match(v) {
                            *v = String::new();
                        }
                    }
                }
            }
            term
        })
        .collect()
}

struct AzureDiskCsiTranslator;

/// `kind` check shared by the inline and PV paths (`azure_disk.go:106`,
/// `:160`): `!strings.EqualFold(kind, managed)`.
fn azure_disk_check_kind(kind: Option<&str>) -> Result<(), String> {
    match kind {
        Some(k) if !k.to_lowercase().eq(&AZURE_DISK_MANAGED.to_lowercase()) => {
            Err(format!("kind({k}) is not supported in csi migration"))
        }
        _ => Ok(()),
    }
}

impl InTreePlugin for AzureDiskCsiTranslator {
    /// `TranslateInTreeStorageClassToCSI` (`azure_disk.go:64-97`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        let mut generated: Vec<TopologySelectorTerm> = Vec::new();
        let mut params: HashMap<String, String> = HashMap::new();
        for (k, v) in sc.parameters.iter().flatten() {
            match k.to_lowercase().as_str() {
                ZONE_KEY => {
                    generated =
                        generate_topology_selectors(AZURE_DISK_TOPOLOGY_KEY, vec![v.clone()])
                }
                ZONES_KEY => {
                    generated = generate_topology_selectors(
                        AZURE_DISK_TOPOLOGY_KEY,
                        v.split(',').map(str::to_string).collect(),
                    )
                }
                _ => {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        let existing = sc.allowed_topologies.clone().unwrap_or_default();
        if !generated.is_empty() && !existing.is_empty() {
            return Err(
                "cannot simultaneously set allowed topologies and zone/zones parameters"
                    .to_string(),
            );
        } else if !generated.is_empty() {
            sc.allowed_topologies = Some(generated);
        } else if !existing.is_empty() {
            sc.allowed_topologies = Some(translate_allowed_topologies(
                &existing,
                AZURE_DISK_TOPOLOGY_KEY,
            ));
        }
        // `sc.AllowedTopologies = t.replaceFailureDomainsToCSI(...)`: nil stays nil.
        sc.allowed_topologies = sc
            .allowed_topologies
            .take()
            .map(azure_disk_replace_failure_domains_to_csi);
        sc.parameters = Some(params);
        Ok(())
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`azure_disk.go:100-141`).
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(azure) = volume.legacy_sources.azure_disk.as_ref() else {
            return Err("volume is nil or Azure Disk not defined on volume".into());
        };
        let str_field = |k: &str| azure.get(k).and_then(|v| v.as_str());
        azure_disk_check_kind(str_field("kind"))?;
        let data_disk_uri = str_field("diskURI").unwrap_or_default().to_string();
        // Must be unique per disk as it is used as the unique part of the
        // staging path
        let mut pv = inline_pv(data_disk_uri.clone());
        let mut attrs =
            HashMap::from([(AZURE_DISK_KIND.to_string(), AZURE_DISK_MANAGED.to_string())]);
        let mut csi = CSIVolumeSource {
            driver: AZURE_DISK_DRIVER_NAME.to_string(),
            volume_handle: Some(data_disk_uri),
            ..Default::default()
        };
        if let Some(ro) = azure.get("readOnly").and_then(|v| v.as_bool()) {
            csi.read_only = Some(ro).filter(|r| *r);
        }
        if let Some(mode) = str_field("cachingMode").filter(|m| !m.is_empty()) {
            attrs.insert(AZURE_DISK_CACHING_MODE.to_string(), mode.to_string());
        }
        if let Some(fs) = str_field("fsType") {
            csi.fs_type = Some(fs.to_string());
            attrs.insert(AZURE_DISK_FS_TYPE.to_string(), fs.to_string());
        }
        attrs.insert(AZURE_DISK_KIND.to_string(), AZURE_DISK_MANAGED.to_string());
        csi.volume_attributes = Some(attrs);
        pv.spec.csi = Some(csi);
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        Ok(pv)
    }

    /// `TranslateInTreePVToCSI` (`azure_disk.go:144-183`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(azure) = pv.spec.more_sources.azure_disk.clone() else {
            return Err("pv is nil or Azure Disk source not defined on pv".into());
        };
        azure_disk_check_kind(azure.kind.as_deref())?;
        let mut attrs =
            HashMap::from([(AZURE_DISK_KIND.to_string(), AZURE_DISK_MANAGED.to_string())]);
        let mut csi = CSIVolumeSource {
            driver: AZURE_DISK_DRIVER_NAME.to_string(),
            volume_handle: Some(azure.data_disk_uri),
            ..Default::default()
        };
        if let Some(mode) = azure.caching_mode {
            attrs.insert(AZURE_DISK_CACHING_MODE.to_string(), mode);
        }
        if let Some(fs) = azure.fs_type {
            csi.fs_type = Some(fs.clone());
            attrs.insert(AZURE_DISK_FS_TYPE.to_string(), fs);
        }
        attrs.insert(AZURE_DISK_KIND.to_string(), AZURE_DISK_MANAGED.to_string());
        if let Some(ro) = azure.read_only {
            csi.read_only = Some(ro).filter(|r| *r);
        }
        csi.volume_attributes = Some(attrs);
        pv.spec.more_sources.azure_disk = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `TranslateCSIPVToInTree` (`azure_disk.go:186-232`).
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let disk_uri = csi.volume_handle.clone().unwrap_or_default();
        let disk_name = get_disk_name(&disk_uri)?;
        let mut azure = crate::resources::pv_sources::AzureDiskVolumeSource {
            disk_name,
            data_disk_uri: disk_uri,
            // `FSType: &csiSource.FSType`, `ReadOnly: &csiSource.ReadOnly`
            fs_type: Some(csi.fs_type.clone().unwrap_or_default()),
            read_only: Some(csi.read_only.unwrap_or(false)),
            kind: Some(AZURE_DISK_MANAGED.to_string()),
            ..Default::default()
        };
        if let Some(attrs) = csi.volume_attributes.as_ref() {
            for (k, v) in attrs {
                match k.to_lowercase().as_str() {
                    AZURE_DISK_CACHING_MODE if !v.is_empty() => {
                        azure.caching_mode = Some(v.clone());
                    }
                    AZURE_DISK_FS_TYPE if !v.is_empty() => azure.fs_type = Some(v.clone()),
                    _ => {}
                }
            }
        }
        pv.spec.csi = None;
        pv.spec.more_sources.azure_disk = Some(azure);
        Ok(())
    }

    /// `CanSupport` (`azure_disk.go:235-237`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.more_sources.azure_disk.is_some()
    }

    /// `CanSupportInline` (`azure_disk.go:242-244`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.azure_disk.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        AZURE_DISK_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        AZURE_DISK_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`azure_disk.go:256-258`): identity.
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
    }
}

// ---- Cinder (`plugins/openstack_cinder.go`) ---------------------------------

/// `CinderDriverName` (`openstack_cinder.go:32`).
pub const CINDER_DRIVER_NAME: &str = "cinder.csi.openstack.org";
/// `CinderTopologyKey` (`openstack_cinder.go:34`).
const CINDER_TOPOLOGY_KEY: &str = "topology.cinder.csi.openstack.org/zone";
/// `CinderInTreePluginName` (`openstack_cinder.go:36`).
pub const CINDER_IN_TREE_PLUGIN_NAME: &str = "kubernetes.io/cinder";

struct OsCinderCsiTranslator;

impl InTreePlugin for OsCinderCsiTranslator {
    /// `TranslateInTreeStorageClassToCSI` (`openstack_cinder.go:49-76`).
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String> {
        let mut params: HashMap<String, String> = HashMap::new();
        for (k, v) in sc.parameters.iter().flatten() {
            match k.to_lowercase().as_str() {
                FS_TYPE_KEY => {
                    params.insert(CSI_FS_TYPE_KEY.to_string(), v.clone());
                }
                // "All other parameters are supported by the CSI driver. This
                // includes also "availability", therefore do not translate it
                // to sc.AllowedTopologies"
                _ => {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(existing) = sc.allowed_topologies.as_ref().filter(|t| !t.is_empty()) {
            sc.allowed_topologies =
                Some(translate_allowed_topologies(existing, CINDER_TOPOLOGY_KEY));
        }
        sc.parameters = Some(params);
        Ok(())
    }

    /// `TranslateInTreePVToCSI` (`openstack_cinder.go:106-129`).
    fn translate_in_tree_pv_to_csi(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(cinder) = pv.spec.more_sources.cinder.clone() else {
            return Err("pv is nil or Cinder not defined on pv".into());
        };
        let csi = CSIVolumeSource {
            driver: CINDER_DRIVER_NAME.to_string(),
            volume_handle: Some(cinder.volume_id),
            read_only: Some(cinder.read_only).filter(|r| *r),
            fs_type: Some(cinder.fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(HashMap::new()),
            ..Default::default()
        };
        translate_topology_from_in_tree_to_csi(pv, CINDER_TOPOLOGY_KEY);
        pv.spec.more_sources.cinder = None;
        pv.spec.csi = Some(csi);
        Ok(())
    }

    /// `TranslateInTreeInlineVolumeToCSI` (`openstack_cinder.go:78-104`).
    fn translate_in_tree_inline_volume_to_csi(
        &self,
        volume: &Volume,
        _pod_namespace: &str,
    ) -> Result<PersistentVolume, String> {
        let Some(cinder) = volume.legacy_sources.cinder.as_ref() else {
            return Err("volume is nil or Cinder not defined on volume".into());
        };
        let volume_id = cinder
            .get("volumeID")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let fs_type = cinder
            .get("fsType")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let read_only = cinder
            .get("readOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // Must be unique per disk as it is used as the unique part of the
        // staging path
        let mut pv = inline_pv(format!("{CINDER_DRIVER_NAME}-{volume_id}"));
        pv.spec.csi = Some(CSIVolumeSource {
            driver: CINDER_DRIVER_NAME.to_string(),
            volume_handle: Some(volume_id),
            read_only: Some(read_only).filter(|r| *r),
            fs_type: Some(fs_type).filter(|f| !f.is_empty()),
            volume_attributes: Some(HashMap::new()),
            ..Default::default()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        Ok(pv)
    }

    /// `TranslateCSIPVToInTree` (`openstack_cinder.go:131-160`).
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String> {
        let Some(csi) = pv.spec.csi.clone() else {
            return Err("pv is nil or CSI source not defined on pv".into());
        };
        let cinder = crate::resources::pv_sources::CinderPersistentVolumeSource {
            volume_id: csi.volume_handle.clone().unwrap_or_default(),
            fs_type: csi.fs_type.clone().unwrap_or_default(),
            read_only: csi.read_only.unwrap_or(false),
            secret_ref: None,
        };
        // translate CSI topology to In-tree topology for rollback
        // compatibility. It is not possible to guess Cinder Region from the
        // Zone, therefore leave it empty.
        translate_topology_from_csi_to_in_tree(pv, CINDER_TOPOLOGY_KEY, None)
            .map_err(|e| format!("failed to translate topology. PV:{pv:?}. Error:{e}"))?;
        pv.spec.csi = None;
        pv.spec.more_sources.cinder = Some(cinder);
        Ok(())
    }

    /// `CanSupport` (`openstack_cinder.go:162-164`).
    fn can_support(&self, pv: &PersistentVolume) -> bool {
        pv.spec.more_sources.cinder.is_some()
    }

    /// `CanSupportInline` (`openstack_cinder.go:169-171`).
    fn can_support_inline(&self, volume: &Volume) -> bool {
        volume.legacy_sources.cinder.is_some()
    }

    fn in_tree_plugin_name(&self) -> &'static str {
        CINDER_IN_TREE_PLUGIN_NAME
    }

    fn csi_plugin_name(&self) -> &'static str {
        CINDER_DRIVER_NAME
    }

    /// `RepairVolumeHandle` (`openstack_cinder.go:181-183`): identity.
    fn repair_volume_handle(&self, volume_handle: &str, _node_id: &str) -> Result<String, String> {
        Ok(volume_handle.to_string())
    }
}
