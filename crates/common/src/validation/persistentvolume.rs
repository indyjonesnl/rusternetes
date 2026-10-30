//! PersistentVolume validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolume` (release-1.35).
//!
//! Scope: capacity (storage required, storage-only, non-negative), access modes
//! (≥1 + ReadWriteOncePod exclusivity), exactly one volume source,
//! nodeAffinity-required-for-Local, the hostPath-'/'-with-Recycle prohibition,
//! and storageClassName, plus the per-source field validation each
//! `numVolumes++` carries upstream and `validateVolumeNodeAffinity`.
//!
//! reclaimPolicy / volumeMode / accessModes enum-membership (upstream
//! `supportedReclaimPolicy` / `supportedVolumeModes` / `supportedAccessModes`)
//! is enforced upstream-of-validation by Rusternetes' typed enums: an unknown
//! string fails to deserialize before this validator runs, so no explicit
//! `NotSupported` check is reproduced here.

use crate::quantity::Quantity;
use crate::resources::pv_sources::{
    AzureDiskVolumeSource, AzureFilePersistentVolumeSource, CephFSPersistentVolumeSource,
    CinderPersistentVolumeSource, FlexPersistentVolumeSource, FlockerVolumeSource,
    GlusterfsPersistentVolumeSource, PhotonPersistentDiskVolumeSource, PortworxVolumeSource,
    QuobyteVolumeSource, RBDPersistentVolumeSource, ScaleIOPersistentVolumeSource,
    StorageOSPersistentVolumeSource, VsphereVirtualDiskVolumeSource,
};
use crate::resources::volume::{
    AWSElasticBlockStoreVolumeSource, CSIVolumeSource, FCVolumeSource,
    GCEPersistentDiskVolumeSource, HostPathType, HostPathVolumeSource, ISCSIVolumeSource,
    LocalVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSpec, SecretReference, VolumeNodeAffinity,
};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_namespace_name, validate_object_meta,
    validate_object_meta_update,
};
use crate::validation::pod::{validate_nfs_volume_source, validate_node_selector};
use once_cell::sync::Lazy;
use regex::Regex;

/// Upstream `iscsiInitiatorIqnRegex` / `iscsiInitiatorEuiRegex` /
/// `iscsiInitiatorNaaRegex` (`pkg/apis/core/validation/validation.go:89-91`).
/// Go's `[[:alnum:]]` is `[0-9A-Za-z]`; the Rust `regex` crate spells POSIX
/// classes the same way inside a character class, so the patterns carry over
/// verbatim apart from that.
static ISCSI_IQN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"iqn\.\d{4}-\d{2}\.([[:alnum:]\-.]+)(:[^,;*&$|\s]+)$").expect("iscsi iqn regex")
});
static ISCSI_EUI_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^eui.[[:alnum:]]{16}$").expect("iscsi eui regex"));
static ISCSI_NAA_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^naa.[[:alnum:]]{32}$").expect("iscsi naa regex"));

/// Lexically normalise a path, mirroring Go's `path.Clean` for the cases that
/// matter to the hostPath-root check: collapse repeated slashes and resolve
/// `.` / `..` elements, returning "/" for any path that reduces to the root.
pub fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(&last) = out.last() {
                    if last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Port of upstream `validatePathNoBacksteps`
/// (`pkg/apis/core/validation/validation.go:754-764`): one error is enough,
/// even for `../../..`.
fn validate_path_no_backsteps(target_path: &str, fld_path: &Path) -> ErrorList {
    if target_path.split('/').any(|part| part == "..") {
        return vec![Error::invalid(
            fld_path,
            target_path.to_string(),
            "must not contain '..'",
        )];
    }
    Vec::new()
}

/// `supportedAccessModes` (validation.go:1936-1940) as `sets.List` sorts it.
pub(crate) const SUPPORTED_ACCESS_MODES: &[&str] = &[
    "ReadOnlyMany",
    "ReadWriteMany",
    "ReadWriteOnce",
    "ReadWriteOncePod",
];

/// Port of upstream `validateHostPathVolumeSource`
/// (`pkg/apis/core/validation/validation.go:816-826`). The `type` enum is a
/// closed Rust enum, so upstream's `validateHostPathType` membership check is
/// answered at decode time.
fn validate_host_path_volume_source(hp: &HostPathVolumeSource, fld_path: &Path) -> ErrorList {
    if hp.path.is_empty() {
        return vec![Error::required(&fld_path.child("path"), "")];
    }
    let mut errs = validate_path_no_backsteps(&hp.path, &fld_path.child("path"));
    // validation.go:824 `validateHostPathType` (:1395). `supportedHostPathTypes`
    // lists `HostPathUnset` (""), and `sets.List` sorts.
    if let Some(HostPathType::Unknown(v)) = &hp.r#type {
        errs.push(Error::not_supported(
            &fld_path.child("type"),
            v.clone(),
            &[
                "",
                "BlockDevice",
                "CharDevice",
                "Directory",
                "DirectoryOrCreate",
                "File",
                "FileOrCreate",
                "Socket",
            ],
        ));
    }
    errs
}

/// Port of upstream `validateLocalVolumeSource`
/// (`pkg/apis/core/validation/validation.go:1735-1744`).
fn validate_local_volume_source(ls: &LocalVolumeSource, fld_path: &Path) -> ErrorList {
    if ls.path.is_empty() {
        return vec![Error::required(&fld_path.child("path"), "")];
    }
    validate_path_no_backsteps(&ls.path, &fld_path.child("path"))
}

/// `pdPartitionErrorMsg` (validation.go:79) = `validation.InclusiveRangeError(1, 255)`.
const PD_PARTITION_ERROR_MSG: &str = "must be between 1 and 255, inclusive";

/// `validateGCEPersistentDiskVolumeSource` (validation.go:950-959). The
/// caller passes `fldPath.Child("persistentDisk")`, as upstream does.
fn validate_gce_persistent_disk_volume_source(
    pd: &GCEPersistentDiskVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if pd.pd_name.is_empty() {
        errs.push(Error::required(&fld_path.child("pdName"), ""));
    }
    let partition = pd.partition.unwrap_or(0);
    if !(0..=255).contains(&partition) {
        errs.push(Error::invalid(
            &fld_path.child("partition"),
            partition,
            PD_PARTITION_ERROR_MSG,
        ));
    }
    errs
}

/// `validateFCVolumeSource` (validation.go:928-948).
pub(crate) fn validate_fc_volume_source(fc: &FCVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let wwns = fc.target_wwns.as_deref().unwrap_or(&[]);
    let wwids = fc.wwids.as_deref().unwrap_or(&[]);
    if wwns.is_empty() && wwids.is_empty() {
        errs.push(Error::required(
            &fld_path.child("targetWWNs"),
            "must specify either targetWWNs or wwids, but not both",
        ));
    }
    if !wwns.is_empty() && !wwids.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("targetWWNs"),
            BadValue::marshal(&wwns),
            "targetWWNs and wwids can not be specified simultaneously",
        ));
    }
    if !wwns.is_empty() {
        match fc.lun {
            None => errs.push(Error::required(
                &fld_path.child("lun"),
                "lun is required if targetWWNs is specified",
            )),
            Some(lun) if !(0..=255).contains(&lun) => errs.push(Error::invalid(
                &fld_path.child("lun"),
                lun,
                "must be between 0 and 255, inclusive",
            )),
            _ => {}
        }
    }
    errs
}

/// `validateAWSElasticBlockStoreVolumeSource` (validation.go:961-970).
fn validate_aws_elastic_block_store_volume_source(
    pd: &AWSElasticBlockStoreVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if pd.volume_id.is_empty() {
        errs.push(Error::required(&fld_path.child("volumeID"), ""));
    }
    let partition = pd.partition.unwrap_or(0);
    if !(0..=255).contains(&partition) {
        errs.push(Error::invalid(
            &fld_path.child("partition"),
            partition,
            PD_PARTITION_ERROR_MSG,
        ));
    }
    errs
}

/// `validateGlusterfsPersistentVolumeSource` (validation.go:1078-1097).
fn validate_glusterfs_persistent_volume_source(
    glusterfs: &GlusterfsPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if glusterfs.endpoints_name.is_empty() {
        errs.push(Error::required(&fld_path.child("endpoints"), ""));
    }
    if glusterfs.path.is_empty() {
        errs.push(Error::required(&fld_path.child("path"), ""));
    }
    if let Some(ns) = &glusterfs.endpoints_namespace {
        let child = fld_path.child("endpointsNamespace");
        if ns.is_empty() {
            errs.push(Error::invalid(
                &child,
                ns.clone(),
                "if the endpointnamespace is set, it must be a valid namespace name",
            ));
        } else {
            for msg in validate_namespace_name(ns, false) {
                errs.push(Error::invalid(&child, ns.clone(), msg));
            }
        }
    }
    errs
}

/// `validateFlockerVolumeSource` (validation.go:1099-1112).
fn validate_flocker_volume_source(flocker: &FlockerVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if flocker.dataset_name.is_empty() && flocker.dataset_uuid.is_empty() {
        errs.push(Error::required(
            fld_path,
            "one of datasetName and datasetUUID is required",
        ));
    }
    if !flocker.dataset_name.is_empty() && !flocker.dataset_uuid.is_empty() {
        errs.push(Error::invalid(
            fld_path,
            "resource",
            "datasetName and datasetUUID can not be specified simultaneously",
        ));
    }
    if flocker.dataset_name.contains('/') {
        errs.push(Error::invalid(
            &fld_path.child("datasetName"),
            flocker.dataset_name.clone(),
            "must not contain '/'",
        ));
    }
    errs
}

/// `validateRBDPersistentVolumeSource` (validation.go:1515-1524).
fn validate_rbd_persistent_volume_source(
    rbd: &RBDPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if rbd.ceph_monitors.is_empty() {
        errs.push(Error::required(&fld_path.child("monitors"), ""));
    }
    if rbd.rbd_image.is_empty() {
        errs.push(Error::required(&fld_path.child("image"), ""));
    }
    errs
}

/// Go's `net.SplitHostPort` success criteria (`net/ipsock.go`), which is all
/// `validateQuobyteVolumeSource` consumes (it discards host and port).
fn split_host_port_ok(hostport: &str) -> bool {
    let Some(i) = hostport.rfind(':') else {
        return false; // missing port in address
    };
    let (j, k);
    if hostport.starts_with('[') {
        let Some(end) = hostport.find(']') else {
            return false; // missing ']' in address
        };
        if end + 1 == hostport.len() {
            return false; // missing port in address
        }
        if end + 1 != i {
            return false; // too many colons / missing port
        }
        j = 1;
        k = end + 1;
    } else {
        if hostport[..i].contains(':') {
            return false; // too many colons in address
        }
        j = 0;
        k = 0;
    }
    if hostport[j..].contains('[') {
        return false; // unexpected '[' in address
    }
    if hostport[k..].contains(']') {
        return false; // unexpected ']' in address
    }
    true
}

/// `validateQuobyteVolumeSource` (validation.go:1048-1066).
fn validate_quobyte_volume_source(quobyte: &QuobyteVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if quobyte.registry.is_empty() {
        errs.push(Error::required(
            &fld_path.child("registry"),
            "must be a host:port pair or multiple pairs separated by commas",
        ));
    } else if quobyte.tenant.len() >= 65 {
        errs.push(Error::required(
            &fld_path.child("tenant"),
            "must be a UUID and may not exceed a length of 64 characters",
        ));
    } else {
        for pair in quobyte.registry.split(',') {
            if !split_host_port_ok(pair) {
                errs.push(Error::invalid(
                    &fld_path.child("registry"),
                    quobyte.registry.clone(),
                    "must be a host:port pair or multiple pairs separated by commas",
                ));
            }
        }
    }
    if quobyte.volume.is_empty() {
        errs.push(Error::required(&fld_path.child("volume"), ""));
    }
    errs
}

/// `validateCephFSPersistentVolumeSource` (validation.go:1563-1569).
fn validate_cephfs_persistent_volume_source(
    cephfs: &CephFSPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    if cephfs.monitors.is_empty() {
        return vec![Error::required(&fld_path.child("monitors"), "")];
    }
    Vec::new()
}

/// The `secretRef.name` / `secretRef.namespace` `Required` pair shared by the
/// Cinder and StorageOS persistent validators.
fn require_secret_ref_name_and_namespace(
    name: Option<&str>,
    namespace: Option<&str>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.unwrap_or("").is_empty() {
        errs.push(Error::required(
            &fld_path.child("secretRef").child("name"),
            "",
        ));
    }
    if namespace.unwrap_or("").is_empty() {
        errs.push(Error::required(
            &fld_path.child("secretRef").child("namespace"),
            "",
        ));
    }
    errs
}

/// `validateCinderPersistentVolumeSource` (validation.go:1539-1553).
fn validate_cinder_persistent_volume_source(
    cd: &CinderPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if cd.volume_id.is_empty() {
        errs.push(Error::required(&fld_path.child("volumeID"), ""));
    }
    if let Some(r) = &cd.secret_ref {
        errs.extend(require_secret_ref_name_and_namespace(
            r.name.as_deref(),
            r.namespace.as_deref(),
            fld_path,
        ));
    }
    errs
}

/// `validateFlexPersistentVolumeSource` (validation.go:1592-1611).
fn validate_flex_persistent_volume_source(
    fv: &FlexPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if fv.driver.is_empty() {
        errs.push(Error::required(&fld_path.child("driver"), ""));
    }
    // Make sure user-specified options don't use kubernetes namespaces. Go
    // ranges a map in random order; sort for a deterministic error list.
    let mut keys: Vec<&String> = fv.options.keys().collect();
    keys.sort();
    for k in keys {
        let namespace = k.split('/').next().unwrap_or(k);
        let normalized = format!(".{}", namespace.to_lowercase());
        if normalized.ends_with(".kubernetes.io") || normalized.ends_with(".k8s.io") {
            errs.push(Error::invalid(
                &fld_path.child("options").key(k.clone()),
                k.clone(),
                "kubernetes.io and k8s.io namespaces are reserved",
            ));
        }
    }
    errs
}

/// `validateAzureFilePV` (validation.go:1624-1638).
fn validate_azure_file_pv(azure: &AzureFilePersistentVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if azure.secret_name.is_empty() {
        errs.push(Error::required(&fld_path.child("secretName"), ""));
    }
    if azure.share_name.is_empty() {
        errs.push(Error::required(&fld_path.child("shareName"), ""));
    }
    if let Some(ns) = &azure.secret_namespace {
        if ns.is_empty() {
            errs.push(Error::required(&fld_path.child("secretNamespace"), ""));
        }
    }
    errs
}

/// `validateVsphereVolumeSource` (validation.go:1683-1689).
fn validate_vsphere_volume_source(
    cd: &VsphereVirtualDiskVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    if cd.volume_path.is_empty() {
        return vec![Error::required(&fld_path.child("volumePath"), "")];
    }
    Vec::new()
}

/// `validatePhotonPersistentDiskVolumeSource` (validation.go:1691-1697).
fn validate_photon_persistent_disk_volume_source(
    cd: &PhotonPersistentDiskVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    if cd.pd_id.is_empty() {
        return vec![Error::required(&fld_path.child("pdID"), "")];
    }
    Vec::new()
}

/// `validatePortworxVolumeSource` (validation.go:1699-1705).
fn validate_portworx_volume_source(pwx: &PortworxVolumeSource, fld_path: &Path) -> ErrorList {
    if pwx.volume_id.is_empty() {
        return vec![Error::required(&fld_path.child("volumeID"), "")];
    }
    Vec::new()
}

/// `validateAzureDisk` (validation.go:1640-1681).
fn validate_azure_disk(azure: &AzureDiskVolumeSource, fld_path: &Path) -> ErrorList {
    // `sets.List` order of the supported sets.
    const CACHING_MODES: &[&str] = &["None", "ReadOnly", "ReadWrite"];
    const DISK_KINDS: &[&str] = &["Dedicated", "Managed", "Shared"];
    const DISK_URI_MANAGED: &[&str] = &[
        "/subscriptions/{sub-id}/resourcegroups/{group-name}/providers/microsoft.compute/disks/{disk-id}",
    ];
    const DISK_URI_BLOB: &[&str] =
        &["https://{account-name}.blob.core.windows.net/{container-name}/{disk-name}.vhd"];

    let mut errs: ErrorList = Vec::new();
    if azure.disk_name.is_empty() {
        errs.push(Error::required(&fld_path.child("diskName"), ""));
    }
    if azure.data_disk_uri.is_empty() {
        errs.push(Error::required(&fld_path.child("diskURI"), ""));
    }
    if let Some(mode) = &azure.caching_mode {
        if !CACHING_MODES.contains(&mode.as_str()) {
            errs.push(Error::not_supported(
                &fld_path.child("cachingMode"),
                mode.clone(),
                CACHING_MODES,
            ));
        }
    }
    if let Some(kind) = &azure.kind {
        if !DISK_KINDS.contains(&kind.as_str()) {
            errs.push(Error::not_supported(
                &fld_path.child("kind"),
                kind.clone(),
                DISK_KINDS,
            ));
        }
        // validate that DiskUri is the correct format
        if kind == "Managed" && !azure.data_disk_uri.starts_with("/subscriptions/") {
            errs.push(Error::not_supported(
                &fld_path.child("diskURI"),
                azure.data_disk_uri.clone(),
                DISK_URI_MANAGED,
            ));
        }
        if kind != "Managed" && !azure.data_disk_uri.starts_with("https://") {
            errs.push(Error::not_supported(
                &fld_path.child("diskURI"),
                azure.data_disk_uri.clone(),
                DISK_URI_BLOB,
            ));
        }
    }
    errs
}

/// `validateScaleIOPersistentVolumeSource` (validation.go:1721-1733).
fn validate_scaleio_persistent_volume_source(
    sio: &ScaleIOPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if sio.gateway.is_empty() {
        errs.push(Error::required(&fld_path.child("gateway"), ""));
    }
    if sio.system.is_empty() {
        errs.push(Error::required(&fld_path.child("system"), ""));
    }
    if sio.volume_name.is_empty() {
        errs.push(Error::required(&fld_path.child("volumeName"), ""));
    }
    errs
}

/// `validateStorageOSPersistentVolumeSource` (validation.go:1764-1783).
fn validate_storageos_persistent_volume_source(
    storageos: &StorageOSPersistentVolumeSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if storageos.volume_name.is_empty() {
        errs.push(Error::required(&fld_path.child("volumeName"), ""));
    } else {
        for msg in is_dns1123_label(&storageos.volume_name) {
            errs.push(Error::invalid(
                &fld_path.child("volumeName"),
                storageos.volume_name.clone(),
                msg,
            ));
        }
    }
    if !storageos.volume_namespace.is_empty() {
        for msg in is_dns1123_label(&storageos.volume_namespace) {
            errs.push(Error::invalid(
                &fld_path.child("volumeNamespace"),
                storageos.volume_namespace.clone(),
                msg,
            ));
        }
    }
    if let Some(r) = &storageos.secret_ref {
        errs.extend(require_secret_ref_name_and_namespace(
            r.name.as_deref(),
            r.namespace.as_deref(),
            fld_path,
        ));
    }
    errs
}

/// Port of upstream `validatePVSecretReference`
/// (`pkg/apis/core/validation/validation.go:1829-1843`): unlike the pod-side
/// `LocalObjectReference`, a PV secret reference carries a namespace and both
/// halves are required.
fn validate_pv_secret_reference(secret_ref: &SecretReference, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match secret_ref.name.as_deref().unwrap_or("") {
        "" => errs.push(Error::required(&fld_path.child("name"), "")),
        name => {
            for msg in is_dns1123_subdomain(name) {
                errs.push(Error::invalid(
                    &fld_path.child("name"),
                    name.to_string(),
                    msg,
                ));
            }
        }
    }
    match secret_ref.namespace.as_deref().unwrap_or("") {
        "" => errs.push(Error::required(&fld_path.child("namespace"), "")),
        ns => {
            for msg in is_dns1123_label(ns) {
                errs.push(Error::invalid(
                    &fld_path.child("namespace"),
                    ns.to_string(),
                    msg,
                ));
            }
        }
    }
    errs
}

/// Port of upstream `validateCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:1848-1869`). Note this is the
/// *persistent* variant: `volumeHandle` is required and every secret reference
/// is a namespaced `SecretReference`, where the pod-inline `CSIVolumeSource`
/// (`:1871-1887`) has neither.
fn validate_csi_persistent_volume_source(csi: &CSIVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs = validate_csi_driver_name(&csi.driver, &fld_path.child("driver"));

    if csi.volume_handle.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("volumeHandle"), ""));
    }
    for (secret_ref, child) in [
        (
            &csi.controller_publish_secret_ref,
            "controllerPublishSecretRef",
        ),
        (
            &csi.controller_expand_secret_ref,
            "controllerExpandSecretRef",
        ),
        (&csi.node_publish_secret_ref, "nodePublishSecretRef"),
        (&csi.node_expand_secret_ref, "nodeExpandSecretRef"),
    ] {
        if let Some(r) = secret_ref {
            errs.extend(validate_pv_secret_reference(r, &fld_path.child(child)));
        }
    }
    errs
}

/// Port of upstream `validateISCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:879-928`). The differences from the
/// pod-inline `validateISCSIVolumeSource` are the `<pv name>:<targetPortal>`
/// length bound that `initiatorName` imposes and the namespaced `secretRef`.
fn validate_iscsi_persistent_volume_source(
    iscsi: &ISCSIVolumeSource,
    pv_name: &str,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if iscsi.target_portal.is_empty() {
        errs.push(Error::required(&fld_path.child("targetPortal"), ""));
    }
    if iscsi.initiator_name.is_some() && pv_name.len() + 1 + iscsi.target_portal.len() > 64 {
        errs.push(Error::invalid(
            &fld_path.child("targetportal"),
            iscsi.target_portal.clone(),
            "Total length of <volume name>:<iscsi.targetPortal> must be under 64 characters if              iscsi.initiatorName is specified.",
        ));
    }
    if iscsi.iqn.is_empty() {
        errs.push(Error::required(&fld_path.child("iqn"), ""));
    } else {
        errs.extend(validate_iscsi_name(&iscsi.iqn, &fld_path.child("iqn")));
    }
    if !(0..=255).contains(&iscsi.lun) {
        errs.push(Error::invalid(
            &fld_path.child("lun"),
            iscsi.lun,
            "must be between 0 and 255, inclusive",
        ));
    }
    let chap =
        iscsi.chap_auth_discovery.unwrap_or(false) || iscsi.chap_auth_session.unwrap_or(false);
    match (&iscsi.secret_ref, chap) {
        (None, true) => errs.push(Error::required(&fld_path.child("secretRef"), "")),
        (Some(r), _) => {
            if r.name.as_deref().unwrap_or("").is_empty() {
                errs.push(Error::required(
                    &fld_path.child("secretRef").child("name"),
                    "",
                ));
            }
        }
        (None, false) => {}
    }
    if let Some(initiator) = &iscsi.initiator_name {
        errs.extend(validate_iscsi_name(
            initiator,
            &fld_path.child("initiatorname"),
        ));
    }
    errs
}

/// The iqn / initiatorName format check shared by both halves of
/// `validateISCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:888-898`, `:918-927`): a recognised
/// prefix, then the prefix's own regex.
fn validate_iscsi_name(name: &str, fld_path: &Path) -> ErrorList {
    let ok = if name.starts_with("iqn") {
        ISCSI_IQN_RE.is_match(name)
    } else if name.starts_with("eui") {
        ISCSI_EUI_RE.is_match(name)
    } else if name.starts_with("naa") {
        ISCSI_NAA_RE.is_match(name)
    } else {
        false
    };
    if ok {
        Vec::new()
    } else {
        vec![Error::invalid(
            fld_path,
            name.to_string(),
            "must be valid format",
        )]
    }
}

/// Port of upstream `validateVolumeNodeAffinity`
/// (`pkg/apis/core/validation/validation.go:8701-8714`): `required` is not
/// optional once `nodeAffinity` is set.
fn validate_volume_node_affinity(na: &VolumeNodeAffinity, fld_path: &Path) -> ErrorList {
    let Some(required) = &na.required else {
        return vec![Error::required(
            &fld_path.child("required"),
            "must specify required node constraints",
        )];
    };
    validate_node_selector(&to_pod_node_selector(required), &fld_path.child("required"))
}

/// `volume.rs` carries its own structurally-identical copy of the node-selector
/// types, so convert to the `resources::pod` shapes that
/// `validation::pod::validate_node_selector` consumes — the same approach
/// `pvc.rs::to_meta_label_selector` takes for label selectors.
fn to_pod_node_selector(sel: &NodeSelector) -> crate::resources::pod::NodeSelector {
    crate::resources::pod::NodeSelector {
        node_selector_terms: sel.node_selector_terms.iter().map(to_pod_term).collect(),
    }
}

fn to_pod_term(term: &NodeSelectorTerm) -> crate::resources::pod::NodeSelectorTerm {
    let convert = |reqs: &Option<Vec<NodeSelectorRequirement>>| {
        reqs.as_ref().map(|rs| {
            rs.iter()
                .map(|r| crate::resources::pod::NodeSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        })
    };
    crate::resources::pod::NodeSelectorTerm {
        match_expressions: convert(&term.match_expressions),
        match_fields: convert(&term.match_fields),
    }
}

/// Validate a `PersistentVolumeSpec`. Mirrors the core of upstream
/// `ValidatePersistentVolume`.
pub fn validate_persistent_volume_spec(
    spec: &PersistentVolumeSpec,
    pv_name: &str,
    inline: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let capacity_path = fld_path.child("capacity");

    // Inline-volume-only deltas (upstream `validateInlinePersistentVolumeSpec`,
    // core validation.go:1968-1978): claimRef + capacity are forbidden and a CSI
    // source is required, because an inline PV (VolumeAttachment.inlineVolumeSpec)
    // is not a standalone object.
    if inline {
        if spec.claim_ref.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("claimRef"),
                "may not be specified in the context of inline volumes",
            ));
        }
        if !spec.capacity.is_empty() {
            errs.push(Error::forbidden(
                &capacity_path,
                "may not be specified in the context of inline volumes",
            ));
        }
        if spec.csi.is_none() {
            errs.push(Error::required(
                &fld_path.child("csi"),
                "has to be specified in the context of inline volumes",
            ));
        }
    }

    // capacity is required (upstream line ~2002). Then it must hold exactly the
    // `storage` resource and nothing else (upstream line ~2005-2007).
    // Upstream uses two independent `if`s (not else-if): an empty capacity is
    // both Required AND NotSupported (storage absent). Match that exactly.
    // These run only for standalone PVs — inline volumes forbid capacity above.
    if !inline {
        if spec.capacity.is_empty() {
            errs.push(Error::required(&capacity_path, ""));
        }
        if !spec.capacity.contains_key("storage") || spec.capacity.len() > 1 {
            errs.push(Error::not_supported(
                &capacity_path,
                "<capacity>",
                &["storage"],
            ));
        }

        // Every capacity quantity must parse and be a non-negative value
        // (upstream validateBasicResource + ValidatePositiveQuantityValue, ~2009-2012).
        for (resource, value) in &spec.capacity {
            let key_path = capacity_path.key(resource.clone());
            match Quantity::parse(value) {
                Err(_) => errs.push(Error::invalid(
                    &key_path,
                    value.clone(),
                    "must be a valid resource quantity",
                )),
                Ok(q) => {
                    if q.is_negative() {
                        errs.push(Error::invalid(
                            &key_path,
                            value.clone(),
                            "must be greater than or equal to 0",
                        ));
                    }
                }
            }
        }
    }

    // accessModes: at least one; ReadWriteOncePod may not combine with others.
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }
    // validation.go:1985-1996: an unsupported mode is NotSupported and is not
    // counted as "another" mode next to ReadWriteOncePod.
    for mode in &spec.access_modes {
        if let PersistentVolumeAccessMode::Unknown(v) = mode {
            errs.push(Error::not_supported(
                &fld_path.child("accessModes"),
                v.clone(),
                SUPPORTED_ACCESS_MODES,
            ));
        }
    }
    let has_rwop = spec
        .access_modes
        .iter()
        .any(|m| matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    let has_other = spec.access_modes.iter().any(|m| {
        !matches!(
            m,
            PersistentVolumeAccessMode::ReadWriteOncePod | PersistentVolumeAccessMode::Unknown(_)
        )
    });
    if has_rwop && has_other {
        errs.push(Error::forbidden(
            &fld_path.child("accessModes"),
            "may not use ReadWriteOncePod with other access modes",
        ));
    }

    // nodeAffinity, when set, must carry `required` and a valid node selector
    // (upstream `validateVolumeNodeAffinity`, validation.go:8701-8714). Upstream
    // runs this *before* the source block, so a PV that is wrong in both places
    // reports the affinity error first.
    if let Some(na) = &spec.node_affinity {
        if !inline {
            errs.extend(validate_volume_node_affinity(
                na,
                &fld_path.child("nodeAffinity"),
            ));
        }
    }

    // Exactly one volume source must be specified, and that source's own fields
    // are validated next to its `numVolumes++` (upstream
    // `ValidatePersistentVolumeSpec`, validation.go:2037-2135). Counting the
    // sources without validating them was the gap this closes: a PV whose only
    // source was `{"nfs": {"server": "x"}}` was written with no path at all.
    let mut num_volumes = 0usize;
    macro_rules! source {
        ($opt:expr, $child:expr, $validate:expr) => {
            if let Some(src) = $opt {
                let child_path = fld_path.child($child);
                if num_volumes > 0 {
                    errs.push(Error::forbidden(
                        &child_path,
                        "may not specify more than 1 volume type",
                    ));
                } else {
                    num_volumes += 1;
                    #[allow(clippy::redundant_closure_call)]
                    errs.extend($validate(src, &child_path));
                }
            }
        };
    }
    // Upstream order, validation.go:2038-2210. Each member's "may not specify
    // more than 1 volume type" error is keyed by its own field name (note the
    // upstream quirks `gcePersistentDisk`/`persistentDisk` and `cephFS`/
    // `cephfs`: Forbidden path and validator path differ).
    let more = &spec.more_sources;
    source!(
        &spec.host_path,
        "hostPath",
        validate_host_path_volume_source
    );
    if let Some(gce) = &spec.gce_persistent_disk {
        if num_volumes > 0 {
            errs.push(Error::forbidden(
                &fld_path.child("gcePersistentDisk"),
                "may not specify more than 1 volume type",
            ));
        } else {
            num_volumes += 1;
            errs.extend(validate_gce_persistent_disk_volume_source(
                gce,
                &fld_path.child("persistentDisk"),
            ));
        }
    }
    source!(
        &spec.aws_elastic_block_store,
        "awsElasticBlockStore",
        validate_aws_elastic_block_store_volume_source
    );
    source!(
        &more.glusterfs,
        "glusterfs",
        validate_glusterfs_persistent_volume_source
    );
    source!(&more.flocker, "flocker", validate_flocker_volume_source);
    source!(&spec.nfs, "nfs", validate_nfs_volume_source);
    source!(&more.rbd, "rbd", validate_rbd_persistent_volume_source);
    source!(&more.quobyte, "quobyte", validate_quobyte_volume_source);
    if let Some(cephfs) = &more.cephfs {
        if num_volumes > 0 {
            errs.push(Error::forbidden(
                &fld_path.child("cephFS"),
                "may not specify more than 1 volume type",
            ));
        } else {
            num_volumes += 1;
            errs.extend(validate_cephfs_persistent_volume_source(
                cephfs,
                &fld_path.child("cephfs"),
            ));
        }
    }
    source!(&spec.iscsi, "iscsi", |iscsi, p| {
        validate_iscsi_persistent_volume_source(iscsi, pv_name, p)
    });
    source!(
        &more.cinder,
        "cinder",
        validate_cinder_persistent_volume_source
    );
    source!(&spec.fc, "fc", validate_fc_volume_source);
    // validation.go:2124-2127: flexVolume alone has no "more than 1 volume
    // type" branch; it always counts and validates.
    if let Some(flex) = &more.flex_volume {
        num_volumes += 1;
        errs.extend(validate_flex_persistent_volume_source(
            flex,
            &fld_path.child("flexVolume"),
        ));
    }
    source!(&more.azure_file, "azureFile", validate_azure_file_pv);
    source!(
        &more.vsphere_volume,
        "vsphereVolume",
        validate_vsphere_volume_source
    );
    source!(
        &more.photon_persistent_disk,
        "photonPersistentDisk",
        validate_photon_persistent_disk_volume_source
    );
    source!(
        &more.portworx_volume,
        "portworxVolume",
        validate_portworx_volume_source
    );
    source!(&more.azure_disk, "azureDisk", validate_azure_disk);
    source!(
        &more.scale_io,
        "scaleIO",
        validate_scaleio_persistent_volume_source
    );
    source!(&spec.local, "local", validate_local_volume_source);
    source!(
        &more.storageos,
        "storageos",
        validate_storageos_persistent_volume_source
    );
    source!(&spec.csi, "csi", validate_csi_persistent_volume_source);
    if num_volumes == 0 {
        errs.push(Error::required(fld_path, "must specify a volume type"));
    }

    // A Local volume requires node affinity (upstream line ~2194-2197).
    if spec.local.is_some() && spec.node_affinity.is_none() {
        errs.push(Error::required(
            &fld_path.child("nodeAffinity"),
            "Local volume requires node affinity",
        ));
    }

    // A hostPath mount of '/' may not use the Recycle reclaim policy
    // (upstream line ~2222-2225).
    if let Some(hp) = &spec.host_path {
        if clean_path(&hp.path) == "/"
            && spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Recycle)
        {
            errs.push(Error::forbidden(
                &fld_path.child("persistentVolumeReclaimPolicy"),
                "may not be 'recycle' for a hostPath mount of '/'",
            ));
        }
    }

    // reclaimPolicy: inline volumes may only use Retain (upstream
    // validation.go:~2018). Standalone PVs accept the full supported set
    // (enum validation handled elsewhere); the hostPath '/' Recycle case above
    // is independent.
    if !inline {
        // validation.go:2021-2023 (`supportedReclaimPolicy`, sorted).
        if let Some(PersistentVolumeReclaimPolicy::Unknown(v)) =
            &spec.persistent_volume_reclaim_policy
        {
            errs.push(Error::not_supported(
                &fld_path.child("persistentVolumeReclaimPolicy"),
                v.clone(),
                &["Delete", "Recycle", "Retain"],
            ));
        }
        // validation.go:2242-2244 (`supportedVolumeModes`, sorted).
        if let Some(PersistentVolumeMode::Unknown(v)) = &spec.volume_mode {
            errs.push(Error::not_supported(
                &fld_path.child("volumeMode"),
                v.clone(),
                &["Block", "Filesystem"],
            ));
        }
    }
    if inline {
        if let Some(policy) = &spec.persistent_volume_reclaim_policy {
            if *policy != PersistentVolumeReclaimPolicy::Retain {
                errs.push(Error::forbidden(
                    &fld_path.child("persistentVolumeReclaimPolicy"),
                    "may only be Retain in the context of inline volumes",
                ));
            }
        }
        // nodeAffinity may not be specified for inline volumes (validation.go:~2228).
        if spec.node_affinity.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("nodeAffinity"),
                "may not be specified in the context of inline volumes",
            ));
        }
        // volumeMode, when set, must be Filesystem for inline volumes
        // (validation.go:~2237).
        if let Some(mode) = &spec.volume_mode {
            if *mode != PersistentVolumeMode::Filesystem {
                errs.push(Error::forbidden(
                    &fld_path.child("volumeMode"),
                    "may not specify volumeMode other than Filesystem in the context of inline volumes",
                ));
            }
        }
    }

    // storageClassName: forbidden for inline volumes; otherwise, when set, must
    // be a DNS-1123 subdomain (upstream validation.go:~2228-2233).
    if let Some(scn) = &spec.storage_class_name {
        if inline {
            if !scn.is_empty() {
                errs.push(Error::forbidden(
                    &fld_path.child("storageClassName"),
                    "may not be specified in the context of inline volumes",
                ));
            }
        } else if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// `api.MountOptionAnnotation` (pkg/apis/core/types.go:348).
const MOUNT_OPTION_ANNOTATION: &str = "volume.beta.kubernetes.io/mount-options";

/// Upstream `ValidatePersistentVolume` (validation.go:2262-2267): ObjectMeta
/// (cluster-scoped; `ValidatePersistentVolumeName` is `NameIsDNSSubdomain`)
/// and the spec. The PV strategy adds [`validate_persistent_volume_plugin`].
pub fn validate_persistent_volume(pv: &PersistentVolume) -> ErrorList {
    let mut errs = validate_object_meta(
        &pv.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_persistent_volume_spec(
        &pv.spec,
        &pv.metadata.name,
        false,
        &Path::new("spec"),
    ));
    errs
}

/// Upstream `volumevalidation.ValidatePersistentVolume`
/// (pkg/volume/validation/pv_validation.go:30-59): the mount-options
/// annotation is forbidden unless the volume type supports mount options.
/// Of the sources modelled here, NFS and iSCSI do.
pub fn validate_persistent_volume_plugin(pv: &PersistentVolume) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if pv.spec.nfs.is_some() || pv.spec.iscsi.is_some() {
        return errs;
    }
    let annotated = pv
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(MOUNT_OPTION_ANNOTATION));
    if annotated {
        errs.push(Error::forbidden(
            &Path::new("metadata")
                .child("annotations")
                .key(MOUNT_OPTION_ANNOTATION),
            "may not specify mount options for this volume type",
        ));
    }
    errs
}

/// Upstream `ValidatePersistentVolumeStatusUpdate` (validation.go:2309-2315):
/// the ObjectMeta update and a required `resourceVersion`.
pub fn validate_persistent_volume_status_update(
    new: &PersistentVolume,
    old: &PersistentVolume,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    if new
        .metadata
        .resource_version
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&Path::new("resourceVersion"), ""));
    }
    errs
}

/// JSON view of just the volume-source union of a `PersistentVolumeSpec`
/// (`PersistentVolumeSource` upstream) — the fields that are immutable after
/// creation. Capacity / accessModes / reclaimPolicy etc. are intentionally
/// excluded.
fn persistent_volume_source(spec: &PersistentVolumeSpec) -> serde_json::Value {
    let mut members = serde_json::json!({
        "hostPath": spec.host_path,
        "nfs": spec.nfs,
        "iscsi": spec.iscsi,
        "local": spec.local,
        "csi": spec.csi,
        "gcePersistentDisk": spec.gce_persistent_disk,
        "awsElasticBlockStore": spec.aws_elastic_block_store,
        "fc": spec.fc,
    });
    // The remaining members (`PersistentVolumeMoreSources`): upstream compares
    // the whole `core.PersistentVolumeSource` with `Semantic.DeepEqual`
    // (validation.go:2283).
    if let (Some(dst), Ok(serde_json::Value::Object(src))) = (
        members.as_object_mut(),
        serde_json::to_value(&spec.more_sources),
    ) {
        dst.extend(src);
    }
    members
}

/// Validate a `PersistentVolume` on update. Mirrors upstream
/// `ValidatePersistentVolumeUpdate`: re-run create validation, then enforce that
/// the volume source and `volumeMode` are immutable. The CSI
/// `controllerExpandSecretRef` may be set when it was previously unset (allowed
/// for volume expansion), so it is excluded from the source-immutability diff in
/// that case.
pub fn validate_persistent_volume_update(
    new: &PersistentVolume,
    old: &PersistentVolume,
) -> ErrorList {
    let mut errs = validate_persistent_volume(new);

    // Allow first-time setting of csi.controllerExpandSecretRef: normalise the
    // new spec to drop it before the source diff when old had none.
    let mut new_spec = new.spec.clone();
    let old_had_expand_ref = old
        .spec
        .csi
        .as_ref()
        .map(|c| c.controller_expand_secret_ref.is_some())
        .unwrap_or(false);
    if !old_had_expand_ref {
        if let Some(csi) = new_spec.csi.as_mut() {
            csi.controller_expand_secret_ref = None;
        }
    }

    if persistent_volume_source(&new_spec) != persistent_volume_source(&old.spec) {
        // validation.go:2283-2284: `diff.Diff(old source, new source)`.
        let source_diff = crate::diff_schema::persistent_volume_source_diff(
            &serde_json::to_value(&old.spec).unwrap_or_default(),
            &serde_json::to_value(&new_spec).unwrap_or_default(),
        );
        errs.push(Error::forbidden(
            &Path::new("spec").child("persistentvolumesource"),
            format!("spec.persistentvolumesource is immutable after creation\n{source_diff}"),
        ));
    }

    if new.spec.volume_mode != old.spec.volume_mode {
        errs.push(Error::invalid(
            &Path::new("spec").child("volumeMode"),
            format!("{:?}", new.spec.volume_mode),
            "field is immutable",
        ));
    }

    // nodeAffinity: immutable once set (upstream validatePvNodeAffinity, with the
    // default-off MutablePVNodeAffinity gate). A nil → set transition is allowed.
    // The beta→GA topology-label masking carve-out is not modelled.
    if old.spec.node_affinity.is_some() {
        let na_eq = serde_json::to_value(&new.spec.node_affinity).ok()
            == serde_json::to_value(&old.spec.node_affinity).ok();
        if !na_eq {
            errs.push(Error::invalid(
                &Path::new("spec").child("nodeAffinity"),
                "<nodeAffinity>".to_string(),
                "field is immutable",
            ));
        }
    }

    // volumeAttributesClassName: with the VolumeAttributesClass feature enabled
    // (beta-on by 1.35), an existing class may be changed but not cleared.
    if old.spec.volume_attributes_class_name.is_some()
        && new.spec.volume_attributes_class_name.is_none()
    {
        errs.push(Error::forbidden(
            &Path::new("spec").child("volumeAttributesClassName"),
            "update from non-nil value to nil is forbidden",
        ));
    }

    errs
}
