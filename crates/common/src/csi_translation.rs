//! In-tree -> CSI PersistentVolume translation: a port of
//! `staging/src/k8s.io/csi-translation-lib` (`translate.go`,
//! `plugins/in_tree_volume.go`, `plugins/gce_pd.go`, `plugins/aws_ebs.go`).
//!
//! Shared by the external-attacher (`CsiAttacher`) and the scheduler's
//! VolumeBinding plugin (`tryTranslatePVToCSI`). Plugins ported so far: GCE PD
//! and AWS EBS; the other in-tree plugins of `translate.go:34-42` (Cinder,
//! AzureDisk, AzureFile, vSphere, Portworx) are tracked follow-ups.
//!
//! `TranslateCSIPVToInTree` (`translate.go:116-128`) is ported for the same
//! two plugins, with `translateTopologyFromCSIToInTree` /
//! `regionTopologyHandler` (`in_tree_volume.go:275-397`).
//!
//! NOT ported: `TranslateInTreeInlineVolumeToCSI` (tracked follow-up; the pod
//! `volumes[]` legacy sources are still opaque JSON).

use crate::resources::volume::{
    AWSElasticBlockStoreVolumeSource, CSIVolumeSource, GCEPersistentDiskVolumeSource, NodeSelector,
    NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeAccessMode, StorageClass,
    TopologySelectorLabelRequirement, TopologySelectorTerm, VolumeNodeAffinity,
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
    /// `TranslateInTreeStorageClassToCSI` (`in_tree_volume.go:34-36`): input is
    /// already a private copy and may be modified.
    fn translate_in_tree_storage_class_to_csi(&self, sc: &mut StorageClass) -> Result<(), String>;
    /// `TranslateCSIPVToInTree` (`in_tree_volume.go:48-51`): input is
    /// already a private copy and may be modified.
    fn translate_csi_pv_to_in_tree(&self, pv: &mut PersistentVolume) -> Result<(), String>;
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

/// `regionParserFn` (`in_tree_volume.go:260`).
type RegionParser = fn(&[&str]) -> Result<String, String>;

/// `translateTopologyFromCSIToInTree` (`in_tree_volume.go:275-307`): replace
/// the CSI topology key with the Kubernetes zone label, add the region when a
/// `region_parser` is given, then label the PV with its zones if it is not
/// labelled already.
fn translate_topology_from_csi_to_in_tree(
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

/// `getAwsRegionFromZones` (`aws_ebs.go:280-305`). AWS zones can be in four
/// forms: us-west-2a, us-gov-east-1a, us-west-2-lax-1a (local zone) and
/// us-east-1-wl1-bos-wlz-1 (wavelength).
fn get_aws_region_from_zones(zones: &[&str]) -> Result<String, String> {
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

    fn sc(params: &[(&str, &str)], topo: Option<Vec<TopologySelectorTerm>>) -> StorageClass {
        StorageClass {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("sc"),
            provisioner: String::new(),
            parameters: Some(
                params
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            reclaim_policy: None,
            volume_binding_mode: None,
            allowed_topologies: topo,
            allow_volume_expansion: None,
            mount_options: None,
        }
    }

    fn topo(key: &str, values: &[&str]) -> Option<Vec<TopologySelectorTerm>> {
        Some(vec![TopologySelectorTerm {
            match_label_expressions: Some(vec![TopologySelectorLabelRequirement {
                key: key.into(),
                values: values.iter().map(|v| v.to_string()).collect(),
            }]),
        }])
    }

    fn sc_eq(a: &StorageClass, b: &StorageClass) -> bool {
        a.parameters == b.parameters
            && serde_json::to_value(&a.allowed_topologies).unwrap()
                == serde_json::to_value(&b.allowed_topologies).unwrap()
    }

    /// `TestTranslatePDInTreeStorageClassToCSI` (`plugins/gce_pd_test.go:38`).
    #[test]
    fn gce_pd_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let tr = |c: &StorageClass| {
            t.translate_in_tree_storage_class_to_csi(GCE_PD_IN_TREE_PLUGIN_NAME, c)
        };
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = tr(&input).expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(
            sc(&[("fstype", "myfs")], None),
            sc(&[("csi.storage.k8s.io/fstype", "myfs")], None),
        );
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("zone", "foo")], None),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        ok(
            sc(&[("zones", "foo,bar,baz")], None),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo", "bar", "baz"])),
        );
        ok(
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        ok(
            sc(&[], topo(LABEL_FAILURE_DOMAIN_BETA_ZONE, &["foo"])),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        assert!(tr(&sc(&[("zone", "foo")], topo(GCE_PD_TOPOLOGY_KEY, &["foo"]))).is_err());
    }

    /// `TestTranslateEBSInTreeStorageClassToCSI` (`plugins/aws_ebs_test.go:83`).
    #[test]
    fn aws_ebs_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let tr = |c: &StorageClass| {
            t.translate_in_tree_storage_class_to_csi(AWS_EBS_IN_TREE_PLUGIN_NAME, c)
        };
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = tr(&input).expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("fstype", "ext3")], None),
            sc(&[("csi.storage.k8s.io/fstype", "ext3")], None),
        );
        ok(
            sc(&[("iopsPerGB", "100")], None),
            sc(
                &[("iopsPerGB", "100"), ("allowautoiopspergbincrease", "true")],
                None,
            ),
        );
        ok(
            sc(&[("zones", "a,b")], None),
            sc(&[], topo(AWS_EBS_TOPOLOGY_KEY, &["a", "b"])),
        );
    }

    /// `TranslateInTreeStorageClassToCSI` (`translate.go:60-62`): an unknown
    /// plugin name is an error; the input class is never modified
    /// (`sc.DeepCopy()`, :55).
    #[test]
    fn storage_class_translation_rejects_unknown_plugin_and_keeps_input() {
        let t = CsiTranslator::new();
        let input = sc(&[("fstype", "x")], None);
        assert!(t
            .translate_in_tree_storage_class_to_csi("kubernetes.io/nope", &input)
            .unwrap_err()
            .contains("could not find in-tree storage class parameter translation logic"));
        let _ = t.translate_in_tree_storage_class_to_csi(GCE_PD_IN_TREE_PLUGIN_NAME, &input);
        assert_eq!(input.parameters.unwrap().get("fstype").unwrap(), "x");
    }

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

    // ---- TranslateCSIPVToInTree -------------------------------------------

    const REGION_GA: &str = "topology.kubernetes.io/region";
    const REGION_BETA: &str = "failure-domain.beta.kubernetes.io/region";

    fn req(key: &str, values: &[&str]) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: key.into(),
            operator: "In".into(),
            values: Some(values.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn term(reqs: Vec<NodeSelectorRequirement>) -> NodeSelectorTerm {
        NodeSelectorTerm {
            match_expressions: Some(reqs),
            match_fields: None,
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn pv_with(
        labels: Option<HashMap<String, String>>,
        terms: Vec<NodeSelectorTerm>,
    ) -> PersistentVolume {
        let mut pv = blank_pv("gcepd");
        pv.metadata.labels = labels;
        pv.spec.node_affinity = Some(VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: terms,
            }),
        });
        pv
    }

    fn terms_json(pv: &PersistentVolume) -> serde_json::Value {
        serde_json::to_value(
            &pv.spec
                .node_affinity
                .as_ref()
                .unwrap()
                .required
                .as_ref()
                .unwrap()
                .node_selector_terms,
        )
        .unwrap()
    }

    /// `TestTranslateTopologyFromCSIToInTree` (`in_tree_volume_test.go:106-485`),
    /// the GCE PD rows (the Cinder row waits for the Cinder plugin).
    #[test]
    fn translate_topology_from_csi_to_in_tree_matches_upstream_table() {
        let gce: Option<RegionParser> = Some(gce_get_region_from_zones);
        let ga_labels = labels(&[(ZONE_GA, "us-east1-a"), (REGION_GA, "us-east1")]);
        let beta_labels = labels(&[(ZONE_BETA, "us-west2-b"), (REGION_BETA, "us-west2")]);
        let ga_zone_first = vec![term(vec![
            req(ZONE_GA, &["us-east1-a"]),
            req(REGION_GA, &["us-east1"]),
        ])];
        let ga_region_first = vec![term(vec![
            req(REGION_GA, &["us-east1"]),
            req(ZONE_GA, &["us-east1-a"]),
        ])];
        let beta_zone_first = vec![term(vec![
            req(ZONE_BETA, &["us-west2-b"]),
            req(REGION_BETA, &["us-west2"]),
        ])];
        let beta_region_first = vec![term(vec![
            req(REGION_BETA, &["us-west2"]),
            req(ZONE_BETA, &["us-west2-b"]),
        ])];
        let existing = labels(&[(ZONE_GA, "existingZone"), (REGION_GA, "existingRegion")]);

        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            Option<RegionParser>,
            PersistentVolume,
            Vec<NodeSelectorTerm>,
            Option<HashMap<String, String>>,
        )> = vec![
            (
                "Remove CSI Topology Key and do not change existing GA Kubernetes topology",
                gce,
                pv_with(
                    ga_labels.clone(),
                    vec![term(vec![
                        req(REGION_GA, &["us-east1"]),
                        req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"]),
                    ])],
                ),
                ga_region_first.clone(),
                ga_labels.clone(),
            ),
            (
                "Remove CSI Topology Key and do not change existing Beta Kubernetes topology",
                gce,
                pv_with(
                    beta_labels.clone(),
                    vec![term(vec![
                        req(REGION_BETA, &["us-west2"]),
                        req(GCE_PD_TOPOLOGY_KEY, &["us-west2-b"]),
                    ])],
                ),
                beta_region_first,
                beta_labels.clone(),
            ),
            (
                "Remove CSI Topology Key and add Kubernetes topology from NodeAffinity, ignore labels",
                gce,
                pv_with(
                    existing.clone(),
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                ga_zone_first.clone(),
                existing.clone(),
            ),
            (
                "No CSI topology label exists and no change to the NodeAffinity",
                gce,
                pv_with(existing.clone(), vec![]),
                vec![],
                existing.clone(),
            ),
            (
                "Generate GA labels and kubernetes topology only from CSI topology",
                gce,
                pv_with(
                    None,
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                ga_zone_first,
                ga_labels,
            ),
            (
                "Generate Beta labels and kubernetes topology from Beta NodeAffinity",
                gce,
                pv_with(None, vec![term(vec![req(ZONE_BETA, &["us-west2-b"])])]),
                beta_zone_first,
                beta_labels.clone(),
            ),
            (
                "regionParser is missing and only zone labels get generated",
                None,
                pv_with(
                    None,
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                vec![term(vec![req(ZONE_GA, &["us-east1-a"])])],
                labels(&[(ZONE_GA, "us-east1-a")]),
            ),
            (
                "Replace multi-term CSI Topology Key and add Region Kubernetes topology for both",
                gce,
                pv_with(
                    None,
                    vec![
                        term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])]),
                        term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-c"])]),
                    ],
                ),
                vec![
                    term(vec![
                        req(ZONE_GA, &["us-east1-a"]),
                        req(REGION_GA, &["us-east1"]),
                    ]),
                    term(vec![
                        req(ZONE_GA, &["us-east1-c"]),
                        req(REGION_GA, &["us-east1"]),
                    ]),
                ],
                labels(&[(ZONE_GA, "us-east1-a__us-east1-c"), (REGION_GA, "us-east1")]),
            ),
        ];

        for (name, parser, mut pv, want_terms, want_labels) in cases {
            translate_topology_from_csi_to_in_tree(&mut pv, GCE_PD_TOPOLOGY_KEY, parser)
                .unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
            assert_eq!(
                terms_json(&pv),
                serde_json::to_value(&want_terms).unwrap(),
                "{name}"
            );
            assert_eq!(pv.metadata.labels, want_labels, "{name}");
        }
    }

    /// `regionTopologyHandler` (`in_tree_volume.go:356-361`): a term with no
    /// zone value reaches the region parser with no zones, and that error
    /// fails the translation.
    #[test]
    fn a_term_without_zones_fails_the_region_parser() {
        let mut pv = pv_with(None, vec![term(vec![req("other", &["x"])])]);
        let err = translate_topology_from_csi_to_in_tree(
            &mut pv,
            GCE_PD_TOPOLOGY_KEY,
            Some(gce_get_region_from_zones),
        )
        .unwrap_err();
        assert_eq!(
            err,
            "Failed to handle region topology. error: no zones specified"
        );
    }

    /// `TestGetAwsRegionFromZones` (`aws_ebs_test.go:213-283`).
    #[test]
    fn get_aws_region_from_zones_matches_upstream_table() {
        let ok: &[(&[&str], &str)] = &[
            (&["us-west-2a", "us-west-2b"], "us-west-2"),
            (&["us-gov-east-1a"], "us-gov-east-1"),
            (&["us-east-1-wl1-bos-wlz-1"], "us-east-1"),
            (&["us-west-2-lax-1a"], "us-west-2"),
        ];
        for (zones, want) in ok {
            assert_eq!(
                get_aws_region_from_zones(zones).unwrap(),
                *want,
                "{zones:?}"
            );
        }
        let bad: &[&[&str]] = &[
            &[],
            &["us-west-2a", "us-east-1a"],
            &["us-west-2"],
            &["us-west-2ab"],
            &["us-west"],
        ];
        for zones in bad {
            assert!(get_aws_region_from_zones(zones).is_err(), "{zones:?}");
        }
        assert_eq!(
            get_aws_region_from_zones(&["us-west"]).unwrap_err(),
            "Unexpected zone format: us-west is not a valid AWS zone"
        );
    }

    /// `TestTranslationStability` (`translate_test.go:47-102`): in-tree ->
    /// CSI -> in-tree is the identity.
    #[test]
    fn translation_round_trip_is_stable() {
        let t = CsiTranslator::new();
        let mut gce = blank_pv("");
        gce.metadata = ObjectMeta::default();
        gce.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
            pd_name: "test-disk".into(),
            fs_type: Some("ext4".into()),
            partition: None,
            read_only: None,
        });
        let mut ebs = blank_pv("");
        ebs.metadata = ObjectMeta::default();
        ebs.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
            volume_id: "vol01".into(),
            fs_type: Some("ext3".into()),
            partition: Some(1),
            read_only: Some(true),
        });
        for pv in [gce, ebs] {
            let csi = t.translate_in_tree_pv_to_csi(&pv).unwrap();
            let back = t.translate_csi_pv_to_in_tree(&csi).unwrap();
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                serde_json::to_value(&pv).unwrap()
            );
        }
    }

    /// `TestTopologyTranslation` (`translate_test.go:104-258`), GCE PD and
    /// AWS EBS rows: the round trip removes the CSI key and leaves a
    /// Kubernetes zone key.
    #[test]
    fn topology_round_trip_restores_kubernetes_keys() {
        let t = CsiTranslator::new();
        let beta = labels(&[(ZONE_BETA, "us-east-1a"), (REGION_BETA, "us-east-1")]);
        let ga = labels(&[(ZONE_GA, "us-east-1a"), (REGION_GA, "us-east-1")]);
        let regional_beta = labels(&[(ZONE_BETA, "europe-west1-b__europe-west1-c")]);
        let regional_ga = labels(&[(ZONE_GA, "europe-west1-b__europe-west1-c")]);
        let topo = |key: &str, values: &[&str]| vec![term(vec![req(key, values)])];
        let gce = |labels: Option<HashMap<String, String>>, terms: Vec<NodeSelectorTerm>| {
            let mut pv = blank_pv("");
            pv.metadata.labels = labels;
            if !terms.is_empty() {
                pv.spec.node_affinity = Some(VolumeNodeAffinity {
                    required: Some(NodeSelector {
                        node_selector_terms: terms,
                    }),
                });
            }
            pv.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
                pd_name: "test-disk".into(),
                fs_type: Some("ext4".into()),
                ..Default::default()
            });
            pv
        };
        let ebs = |labels: Option<HashMap<String, String>>, terms: Vec<NodeSelectorTerm>| {
            let mut pv = gce(labels, terms);
            pv.spec.gce_persistent_disk = None;
            pv.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
                volume_id: "vol01".into(),
                fs_type: Some("ext3".into()),
                partition: Some(1),
                read_only: Some(true),
            });
            pv
        };
        let cases: Vec<(&str, PersistentVolume)> = vec![
            (GCE_PD_TOPOLOGY_KEY, gce(beta.clone(), vec![])),
            (GCE_PD_TOPOLOGY_KEY, gce(ga.clone(), vec![])),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(None, topo(GCE_PD_TOPOLOGY_KEY, &["us-east-2a"])),
            ),
            (GCE_PD_TOPOLOGY_KEY, gce(regional_beta.clone(), vec![])),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(None, topo(ZONE_GA, &["europe-west1-b", "europe-west1-c"])),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    regional_beta,
                    topo(ZONE_BETA, &["europe-west1-f", "europe-west1-g"]),
                ),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    regional_ga,
                    topo(ZONE_GA, &["europe-west1-f", "europe-west1-g"]),
                ),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    None,
                    vec![
                        term(vec![req(ZONE_GA, &["europe-west1-f"])]),
                        term(vec![req(ZONE_GA, &["europe-west1-g"])]),
                    ],
                ),
            ),
            (AWS_EBS_TOPOLOGY_KEY, ebs(beta.clone(), vec![])),
            (
                AWS_EBS_TOPOLOGY_KEY,
                ebs(beta, topo(ZONE_BETA, &["us-east-2a"])),
            ),
            (AWS_EBS_TOPOLOGY_KEY, ebs(ga.clone(), vec![])),
            (
                AWS_EBS_TOPOLOGY_KEY,
                ebs(ga, topo(ZONE_GA, &["us-east-2a"])),
            ),
        ];
        for (key, pv) in cases {
            let csi = t.translate_in_tree_pv_to_csi(&pv).unwrap();
            let back = t
                .translate_csi_pv_to_in_tree(&csi)
                .unwrap_or_else(|e| panic!("{pv:?}: {e}"));
            let affinity = back.spec.node_affinity.as_ref();
            assert!(!topology_key_exist(key, affinity), "{back:?}");
            assert!(
                topology_key_exist(ZONE_BETA, affinity) || topology_key_exist(ZONE_GA, affinity),
                "{back:?}"
            );
        }
    }

    /// `gcePersistentDiskCSITranslator.TranslateCSIPVToInTree`
    /// (`gce_pd.go:269-306`): the PD name is the handle's sixth element and a
    /// non-empty `partition` attribute is parsed.
    #[test]
    fn gce_csi_pv_translates_to_in_tree() {
        let t = CsiTranslator::new();
        let mut pv = blank_pv("pv1");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: GCE_PD_DRIVER_NAME.into(),
            volume_handle: Some("projects/p/zones/us-central1-a/disks/d1".into()),
            fs_type: Some("xfs".into()),
            read_only: Some(true),
            volume_attributes: Some(HashMap::from([("partition".into(), "3".into())])),
            ..Default::default()
        });
        let out = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        assert!(out.spec.csi.is_none());
        assert_eq!(
            out.spec.gce_persistent_disk,
            Some(GCEPersistentDiskVolumeSource {
                pd_name: "d1".into(),
                fs_type: Some("xfs".into()),
                partition: Some(3),
                read_only: Some(true),
            })
        );
        // the input is not modified
        assert!(pv.spec.csi.is_some());

        pv.spec.csi.as_mut().unwrap().volume_handle = Some("projects/p/disks/d1".into());
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "failed to get id components.Got: 4, wanted 6 components or more. "
        );
        let csi = pv.spec.csi.as_mut().unwrap();
        csi.volume_handle = Some("projects/p/zones/z/disks/d1".into());
        csi.volume_attributes = Some(HashMap::from([("partition".into(), "x".into())]));
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "Failed to convert partition x to integer: strconv.Atoi: parsing \"x\": invalid syntax"
        );
    }

    /// `awsElasticBlockStoreCSITranslator.TranslateCSIPVToInTree`
    /// (`aws_ebs.go:170-201`): the handle is the volume ID, and a present
    /// `partition` attribute must parse (an empty one too).
    #[test]
    fn ebs_csi_pv_translates_to_in_tree() {
        let t = CsiTranslator::new();
        let mut pv = blank_pv("pv2");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: AWS_EBS_DRIVER_NAME.into(),
            volume_handle: Some("vol-0abc".into()),
            volume_attributes: Some(HashMap::from([("partition".into(), "2".into())])),
            ..Default::default()
        });
        let out = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        assert_eq!(
            out.spec.aws_elastic_block_store,
            Some(AWSElasticBlockStoreVolumeSource {
                volume_id: "vol-0abc".into(),
                fs_type: None,
                partition: Some(2),
                read_only: None,
            })
        );
        pv.spec.csi.as_mut().unwrap().volume_attributes =
            Some(HashMap::from([("partition".into(), "".into())]));
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "failed to convert partition  to integer: strconv.Atoi: parsing \"\": invalid syntax"
        );
    }

    /// `CSITranslator.TranslateCSIPVToInTree` (`translate.go:116-128`).
    #[test]
    fn translate_csi_pv_to_in_tree_rejects_non_csi_and_unknown_drivers() {
        let t = CsiTranslator::new();
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&blank_pv("x")).unwrap_err(),
            "CSI persistent volume was nil"
        );
        let mut pv = blank_pv("x");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: "other.csi".into(),
            ..Default::default()
        });
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "could not find in-tree plugin translation logic for other.csi"
        );
    }
}
