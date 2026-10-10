//! The CSI external-provisioner loop (#2882): a port of
//! kubernetes-csi/external-provisioner `pkg/controller/controller.go`
//! (`csiProvisioner`: `ShouldProvision`, `Provision`, `prepareProvision`,
//! `cleanupVolume`) running inside sig-storage-lib-external-provisioner
//! `controller/controller.go` (`syncClaim`, `shouldProvision`,
//! `provisionClaimOperation`, `provisionVolumeErrorHandling`) and its
//! `volume_store.go` (`queueStore`), the store `csi-provisioner` selects
//! (`cmd/csi-provisioner/csi-provisioner.go:406`,
//! `CreateProvisionedPVLimiter`).
//!
//! One [`CsiProvisioner`] serves one CSI driver, as one external-provisioner
//! sidecar does: it handles claims annotated
//! `volume.kubernetes.io/storage-provisioner` (or the beta key) with its
//! driver name and talks to that driver's Controller service over its unix
//! socket ([`CsiControllerClient`]).
//!
//! Slices 1-2 (this file): a claim with or without a VolumeSnapshot / PVC
//! data source (`getVolumeContentSource`). NOT yet ported, each tracked on
//! #2882 and rejected loudly rather than silently ignored: a cross-namespace
//! `dataSourceRef` (`IsGranted`, #2981) and topology
//! (`GenerateAccessibilityRequirements`, `GenerateVolumeNodeAffinity`).
//!
//! Slice 4 (#2964): the provisioner / publish / stage / expand / modify secret
//! parameters (`getSecretReference`, `getCredentials`, incl. the
//! StorageClass-derived deletion secrets `getSecretsFromSC`, #2992) and the
//! claim's VolumeAttributesClass (`MutableParameters`).
//!
//! Slice 3 (#2965): the delete path (`syncVolume`, `isProvisionerForVolume`,
//! `handleProtectionFinalizer`, `shouldDelete`, `deleteVolumeOperation`,
//! external-provisioner `Delete` / `handleSecretsForDeletion` /
//! `canDeleteVolume`) and the slow-retry set for infeasible
//! (`InvalidArgument`) requests (`delayProvisioningIfRecentlyInfeasible`,
//! `markForSlowRetry`, csi-lib-utils `slowset`). NOT ported here: CSI
//! migration of in-tree PVs (`IsPVMigratable` / `TranslateInTreePVToCSI`).

use super::csi_topology::{
    generate_volume_node_affinity, TopologyError, TopologyInputs, TopologyStore,
};
use anyhow::{anyhow, Context, Result};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::csi::{VolumeAttachment, VolumeAttributesClass};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    CSIVolumeSource, PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeMode,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy, SecretReference, StorageClass,
    VolumeBindingMode, VolumeSnapshot, VolumeSnapshotContent,
};
use rusternetes_common::resources::{
    EventSource, EventType, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
    PersistentVolumeStatus, Secret,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use rusternetes_csi::controller_client::{
    check_driver_capabilities, ControllerError, CsiControllerClient, DriverCapabilities,
    ProvisioningState, RequiredCapabilities,
};
use rusternetes_csi::proto::controller_service_capability::rpc::Type as ControllerRpc;
use rusternetes_csi::proto::volume_capability::access_mode::Mode as AccessModeKind;
use rusternetes_csi::proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use rusternetes_csi::proto::volume_content_source::{
    SnapshotSource, Type as ContentSourceType, VolumeSource,
};
use rusternetes_csi::proto::{
    CapacityRange, CreateVolumeRequest, DeleteVolumeRequest, VolumeCapability, VolumeContentSource,
};
use rusternetes_storage::WorkQueueConfig;
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// `annStorageProvisioner` (lib controller.go:70).
const ANN_STORAGE_PROVISIONER: &str = "volume.kubernetes.io/storage-provisioner";
/// `annBetaStorageProvisioner` (lib controller.go:69).
const ANN_BETA_STORAGE_PROVISIONER: &str = "volume.beta.kubernetes.io/storage-provisioner";
/// `annMigratedTo` (lib controller.go:66).
const ANN_MIGRATED_TO: &str = "pv.kubernetes.io/migrated-to";
/// `annSelectedNode` / `annAlphaSelectedNode` (lib controller.go:74, :77).
const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
/// `v1.BetaStorageClassAnnotation`, read by `GetPersistentVolumeClass`.
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";
const ANN_ALPHA_SELECTED_NODE: &str = "volume.alpha.kubernetes.io/selected-node";
/// `annDynamicallyProvisioned` (lib controller.go:62).
const ANN_DYNAMICALLY_PROVISIONED: &str = "pv.kubernetes.io/provisioned-by";
/// `finalizerPV` (lib controller.go:80).
pub const FINALIZER_PV: &str = "external-provisioner.volume.kubernetes.io/finalizer";
/// `annDeletionProvisionerSecretRefName` / `...Namespace`
/// (external-provisioner controller.go:151-152).
const ANN_DELETION_SECRET_NAME: &str = "volume.kubernetes.io/provisioner-deletion-secret-name";
const ANN_DELETION_SECRET_NAMESPACE: &str =
    "volume.kubernetes.io/provisioner-deletion-secret-namespace";
/// `provisionerIDKey` (external-provisioner controller.go:315).
/// `annModifyControllerSecretRefName` / `...Namespace` (controller.go:156-157).
const ANN_MODIFY_SECRET_NAME: &str = "volume.kubernetes.io/controller-modify-secret-name";
const ANN_MODIFY_SECRET_NAMESPACE: &str = "volume.kubernetes.io/controller-modify-secret-namespace";
const PROVISIONER_ID_KEY: &str = "storage.kubernetes.io/csiProvisionerIdentity";
/// `csiParameterPrefix` and the well-known keys under it (controller.go:83-109).
const CSI_PARAMETER_PREFIX: &str = "csi.storage.k8s.io/";
const PREFIXED_FSTYPE_KEY: &str = "csi.storage.k8s.io/fstype";
const PREFIXED_KNOWN_KEYS: &[&str] = &[
    "fstype",
    "secret-name",
    "secret-namespace",
    "provisioner-secret-name",
    "provisioner-secret-namespace",
    "controller-publish-secret-name",
    "controller-publish-secret-namespace",
    "node-stage-secret-name",
    "node-stage-secret-namespace",
    "node-publish-secret-name",
    "node-publish-secret-namespace",
    "controller-expand-secret-name",
    "controller-expand-secret-namespace",
    "node-expand-secret-name",
    "node-expand-secret-namespace",
    "controller-modify-secret-name",
    "controller-modify-secret-namespace",
];
const PVC_NAME_KEY: &str = "csi.storage.k8s.io/pvc/name";
const PVC_NAMESPACE_KEY: &str = "csi.storage.k8s.io/pvc/namespace";
const PV_NAME_KEY: &str = "csi.storage.k8s.io/pv/name";
/// `snapshotKind` / `snapshotAPIGroup` / `pvcKind` (controller.go:130-132).
const SNAPSHOT_KIND: &str = "VolumeSnapshot";
const SNAPSHOT_API_GROUP: &str = "snapshot.storage.k8s.io";
const PVC_KIND: &str = "PersistentVolumeClaim";
/// `pvcCloneFinalizer` (controller.go:164).
const PVC_CLONE_FINALIZER: &str = "provisioner.storage.kubernetes.io/cloning-protection";
/// `snapshotSourceProtectionFinalizer` (controller.go:168).
const SNAPSHOT_SOURCE_PROTECTION_FINALIZER: &str =
    "provisioner.storage.kubernetes.io/volumesnapshot-as-source-protection";
/// `annAllowVolumeModeChange` (controller.go:170).
const ANN_ALLOW_VOLUME_MODE_CHANGE: &str =
    "snapshot.storage.kubernetes.io/allow-volume-mode-change";
/// `deleteVolumeRetryCount` (controller.go:141).
const DELETE_VOLUME_RETRY_COUNT: usize = 5;
/// `--volume-name-prefix` default (csi-provisioner.go:81).
const VOLUME_NAME_PREFIX: &str = "pvc";
/// `--retry-interval-start` / `--retry-interval-max` (csi-provisioner.go:83-84).
const RETRY_INTERVAL_START: Duration = Duration::from_secs(1);
const RETRY_INTERVAL_MAX: Duration = Duration::from_secs(300);
/// `DefaultTypedControllerRateLimiter`'s per-item exponential limiter
/// (`5ms` base, `1000s` cap), used by the background PV store.
const PV_SAVE_BASE: Duration = Duration::from_millis(5);
const PV_SAVE_MAX: Duration = Duration::from_secs(1000);
/// Concurrent claim workers (`--worker-threads`, csi-provisioner.go:85,
/// default 100; this process runs fewer, the claims are cheap).
const WORKERS: usize = 8;

/// Why a provisioning step stopped without an error to retry.
#[derive(Debug)]
enum Failure {
    /// `controller.IgnoredError`: another provisioner/populator owns it.
    Ignored(String),
    /// `errStopProvision`.
    Stop,
    Error(anyhow::Error),
}

impl From<anyhow::Error> for Failure {
    fn from(e: anyhow::Error) -> Self {
        Failure::Error(e)
    }
}

type Outcome<T> = std::result::Result<T, (ProvisioningState, Failure)>;

/// Why `csiProvisioner.Delete` stopped. (`IgnoredError` is only returned for
/// a node deployment, which is not ported.)
#[derive(Debug)]
enum DeleteFailure {
    /// `controller.VolumeInUseError`: postponed, not failed.
    InUse(String),
    Error(anyhow::Error),
}

impl From<anyhow::Error> for DeleteFailure {
    fn from(e: anyhow::Error) -> Self {
        DeleteFailure::Error(e)
    }
}

/// `slowset.ObjectData`.
struct SlowEntry {
    timestamp: Instant,
    storage_class_uid: String,
}

/// A provisioned PV the API server has not accepted yet (`queueStore`).
struct Unsaved {
    pv: PersistentVolume,
    attempts: u32,
    next_try: Instant,
}

/// `GetPersistentVolumeClaimClass` (component-helpers `helpers.go`).
fn claim_class(claim: &PersistentVolumeClaim) -> String {
    if let Some(c) = claim.spec.storage_class_name.as_ref() {
        return c.clone();
    }
    claim
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("volume.beta.kubernetes.io/storage-class"))
        .cloned()
        .unwrap_or_default()
}

/// `checkFinalizer` (controller.go:2088).
fn has_finalizer(meta: &ObjectMeta, finalizer: &str) -> bool {
    meta.finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|f| f == finalizer))
}

fn annotation<'a>(meta: &'a ObjectMeta, key: &str) -> Option<&'a String> {
    meta.annotations.as_ref().and_then(|a| a.get(key))
}

/// `makeVolumeName` with `--volume-name-uuid-length=-1` (controller.go:483-498).
pub fn make_volume_name(prefix: &str, pvc_uid: &str) -> std::result::Result<String, String> {
    if prefix.is_empty() {
        return Err("volume name prefix cannot be of length 0".into());
    }
    if pvc_uid.is_empty() {
        return Err("corrupted PVC object, it is missing UID".into());
    }
    Ok(format!("{prefix}-{pvc_uid}"))
}

/// `accessmodes.ToCSIAccessMode` (kubernetes-csi/csi-lib-utils
/// `accessmodes/access_modes.go`), for one claim access mode at a time as
/// `getVolumeCapability` calls it (controller.go:522).
pub fn to_csi_access_mode(
    modes: &[PersistentVolumeAccessMode],
    supports_single_node_multi_writer: bool,
) -> std::result::Result<AccessModeKind, String> {
    use PersistentVolumeAccessMode::*;
    let has = |m: PersistentVolumeAccessMode| modes.contains(&m);
    let (rwop, rwx, rox, rwo) = (
        has(ReadWriteOncePod),
        has(ReadWriteMany),
        has(ReadOnlyMany),
        has(ReadWriteOnce),
    );
    let unique: HashSet<String> = modes.iter().map(|m| format!("{m:?}")).collect();
    if rwop {
        if unique.len() > 1 {
            return Err("Kubernetes does not support use of ReadWriteOncePod with other access modes on the same PersistentVolume".into());
        }
        return Ok(if supports_single_node_multi_writer {
            AccessModeKind::SingleNodeSingleWriter
        } else {
            AccessModeKind::SingleNodeWriter
        });
    }
    if rwx {
        return Ok(AccessModeKind::MultiNodeMultiWriter);
    }
    if rox && rwo {
        return Err(
            "CSI does not support ReadOnlyMany and ReadWriteOnce on the same PersistentVolume"
                .into(),
        );
    }
    if rox {
        return Ok(AccessModeKind::MultiNodeReaderOnly);
    }
    if rwo {
        return Ok(if supports_single_node_multi_writer {
            AccessModeKind::SingleNodeMultiWriter
        } else {
            AccessModeKind::SingleNodeWriter
        });
    }
    Err(format!("unsupported AccessMode combination: {modes:?}"))
}

/// `removePrefixedParameters` (controller.go:1145-1182).
pub fn remove_prefixed_parameters(
    param: &HashMap<String, String>,
) -> std::result::Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    for (k, v) in param {
        if let Some(rest) = k.strip_prefix(CSI_PARAMETER_PREFIX) {
            if !PREFIXED_KNOWN_KEYS.contains(&rest) {
                return Err(format!(
                    "found unknown parameter key \"{k}\" with reserved namespace {CSI_PARAMETER_PREFIX}"
                ));
            }
        } else {
            out.insert(k.clone(), v.clone());
        }
    }
    Ok(out)
}

/// `secretParamsMap` (controller.go:140-158): the parameter keys naming one
/// kind of secret. `deprecated_*` are the pre-prefix keys, NOT stripped from
/// the parameters passed to CreateVolume.
struct SecretParams {
    name: &'static str,
    deprecated_name_key: Option<&'static str>,
    deprecated_namespace_key: Option<&'static str>,
    name_key: &'static str,
    namespace_key: &'static str,
}

/// `defaultSecretParams` .. `controllerModifySecretParams` (controller.go:174-229).
const DEFAULT_SECRET_PARAMS: SecretParams = SecretParams {
    name: "Default",
    deprecated_name_key: None,
    deprecated_namespace_key: None,
    name_key: "csi.storage.k8s.io/secret-name",
    namespace_key: "csi.storage.k8s.io/secret-namespace",
};
const PROVISIONER_SECRET_PARAMS: SecretParams = SecretParams {
    name: "Provisioner",
    deprecated_name_key: Some("csiProvisionerSecretName"),
    deprecated_namespace_key: Some("csiProvisionerSecretNamespace"),
    name_key: "csi.storage.k8s.io/provisioner-secret-name",
    namespace_key: "csi.storage.k8s.io/provisioner-secret-namespace",
};
const NODE_PUBLISH_SECRET_PARAMS: SecretParams = SecretParams {
    name: "NodePublish",
    deprecated_name_key: Some("csiNodePublishSecretName"),
    deprecated_namespace_key: Some("csiNodePublishSecretNamespace"),
    name_key: "csi.storage.k8s.io/node-publish-secret-name",
    namespace_key: "csi.storage.k8s.io/node-publish-secret-namespace",
};
const CONTROLLER_PUBLISH_SECRET_PARAMS: SecretParams = SecretParams {
    name: "ControllerPublish",
    deprecated_name_key: Some("csiControllerPublishSecretName"),
    deprecated_namespace_key: Some("csiControllerPublishSecretNamespace"),
    name_key: "csi.storage.k8s.io/controller-publish-secret-name",
    namespace_key: "csi.storage.k8s.io/controller-publish-secret-namespace",
};
const NODE_STAGE_SECRET_PARAMS: SecretParams = SecretParams {
    name: "NodeStage",
    deprecated_name_key: Some("csiNodeStageSecretName"),
    deprecated_namespace_key: Some("csiNodeStageSecretNamespace"),
    name_key: "csi.storage.k8s.io/node-stage-secret-name",
    namespace_key: "csi.storage.k8s.io/node-stage-secret-namespace",
};
const CONTROLLER_EXPAND_SECRET_PARAMS: SecretParams = SecretParams {
    name: "ControllerExpand",
    deprecated_name_key: None,
    deprecated_namespace_key: None,
    name_key: "csi.storage.k8s.io/controller-expand-secret-name",
    namespace_key: "csi.storage.k8s.io/controller-expand-secret-namespace",
};
const NODE_EXPAND_SECRET_PARAMS: SecretParams = SecretParams {
    name: "NodeExpand",
    deprecated_name_key: None,
    deprecated_namespace_key: None,
    name_key: "csi.storage.k8s.io/node-expand-secret-name",
    namespace_key: "csi.storage.k8s.io/node-expand-secret-namespace",
};
const CONTROLLER_MODIFY_SECRET_PARAMS: SecretParams = SecretParams {
    name: "ControllerModify",
    deprecated_name_key: None,
    deprecated_namespace_key: None,
    name_key: "csi.storage.k8s.io/controller-modify-secret-name",
    namespace_key: "csi.storage.k8s.io/controller-modify-secret-namespace",
};

/// `verifyAndGetSecretNameAndNamespaceTemplate` (controller.go:1860-1902).
fn verify_and_get_secret_templates(
    secret: &SecretParams,
    sc_params: &HashMap<String, String>,
) -> std::result::Result<(String, String), String> {
    let (mut name_t, mut ns_t) = (String::new(), String::new());
    let (mut num_name, mut num_ns) = (0, 0);
    if let Some((k, t)) = secret
        .deprecated_name_key
        .and_then(|k| sc_params.get(k).map(|t| (k, t)))
    {
        name_t = t.clone();
        num_name += 1;
        warn!(
            "\"{k}\" is deprecated and will be removed in a future release, please use \"{}\" instead",
            secret.name_key
        );
    }
    if let Some((k, t)) = secret
        .deprecated_namespace_key
        .and_then(|k| sc_params.get(k).map(|t| (k, t)))
    {
        ns_t = t.clone();
        num_ns += 1;
        warn!(
            "\"{k}\" is deprecated and will be removed in a future release, please use \"{}\" instead",
            secret.namespace_key
        );
    }
    if let Some(t) = sc_params.get(secret.name_key) {
        name_t = t.clone();
        num_name += 1;
    }
    if let Some(t) = sc_params.get(secret.namespace_key) {
        ns_t = t.clone();
        num_ns += 1;
    }
    if num_name > 1 || num_ns > 1 {
        Err(format!(
            "{} secrets specified in parameters with both \"csi\" and \"{CSI_PARAMETER_PREFIX}\" keys",
            secret.name
        ))
    } else if num_name != num_ns {
        Err(format!(
            "either name and namespace for {} secrets specified, Both must be specified",
            secret.name
        ))
    } else if num_name == 1 {
        if name_t.is_empty() || ns_t.is_empty() {
            return Err(format!(
                "{} secrets specified in parameters but value of either namespace or name is empty",
                secret.name
            ));
        }
        Ok((name_t, ns_t))
    } else {
        Ok((String::new(), String::new()))
    }
}

/// Go `os.Expand`'s `getShellName`: the variable name at the start of `s`
/// (after the `$`) and the bytes consumed.
fn shell_name(s: &str) -> (&str, usize) {
    let b = s.as_bytes();
    let special = |c: u8| b"*#$@!?-0123456789".contains(&c);
    if b[0] == b'{' {
        if b.len() > 2 && special(b[1]) && b[2] == b'}' {
            return (&s[1..2], 3);
        }
        for i in 1..b.len() {
            if b[i] == b'}' {
                if i == 1 {
                    return ("", 2); // bad syntax: ${}
                }
                return (&s[1..i], i + 1);
            }
        }
        return ("", 1); // bad syntax: no closing brace
    }
    if special(b[0]) {
        return (&s[..1], 1);
    }
    let mut i = 0;
    while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
        i += 1;
    }
    (&s[..i], i)
}

/// `resolveTemplate` (controller.go:1989-2002) over Go's `os.Expand`.
fn resolve_template(
    template: &str,
    params: &HashMap<String, String>,
) -> std::result::Result<String, String> {
    let mut missing = std::collections::BTreeSet::new();
    let mut out = String::new();
    let b = template.as_bytes();
    let (mut i, mut j) = (0, 0);
    while j < b.len() {
        if b[j] == b'$' && j + 1 < b.len() {
            out.push_str(&template[i..j]);
            let (name, w) = shell_name(&template[j + 1..]);
            if name.is_empty() && w > 0 {
                // "Encountered invalid syntax; eat the characters."
            } else if name.is_empty() {
                out.push('$');
            } else if let Some(v) = params.get(name) {
                out.push_str(v);
            } else {
                missing.insert(name.to_string());
            }
            j += w;
            i = j + 1;
        }
        j += 1;
    }
    if i < template.len() {
        out.push_str(&template[i..]);
    }
    if missing.is_empty() {
        Ok(out)
    } else {
        let quoted: Vec<String> = missing.iter().map(|m| format!("{m:?}")).collect();
        Err(format!("invalid tokens: [{}]", quoted.join(" ")))
    }
}

/// `getSecretReference` (controller.go:1922-1987): the secret named by the
/// class parameters, its name and namespace templates resolved against the PV
/// and claim. No lookup of the secret itself is performed.
fn get_secret_reference(
    secret_params: &SecretParams,
    sc_params: &HashMap<String, String>,
    pv_name: &str,
    pvc: &PersistentVolumeClaim,
) -> std::result::Result<Option<SecretReference>, String> {
    let (mut name_t, mut ns_t) = verify_and_get_secret_templates(secret_params, sc_params)
        .map_err(|e| format!("failed to get name and namespace template from params: {e}"))?;
    // "if didn't find secrets for specific call, try to check default values"
    if name_t.is_empty() && ns_t.is_empty() {
        (name_t, ns_t) = verify_and_get_secret_templates(&DEFAULT_SECRET_PARAMS, sc_params)
            .map_err(|e| {
                format!("failed to get default name and namespace template from params: {e}")
            })?;
    }
    if name_t.is_empty() && ns_t.is_empty() {
        return Ok(None);
    }

    let pvc_ns = pvc.metadata.namespace.clone().unwrap_or_default();

    // Namespace: the PV name or the PVC namespace (neither is under the PVC
    // user's control).
    let ns_params = HashMap::from([
        ("pv.name".to_string(), pv_name.to_string()),
        ("pvc.namespace".to_string(), pvc_ns.clone()),
    ]);
    let namespace = resolve_template(&ns_t, &ns_params)
        .map_err(|e| format!("error resolving value {ns_t:?}: {e}"))?;
    if !is_dns1123_label(&namespace).is_empty() {
        return Err(if ns_t != namespace {
            format!("{ns_t:?} resolved to {namespace:?} which is not a valid namespace name")
        } else {
            format!("{ns_t:?} is not a valid namespace name")
        });
    }

    // Name: also the PVC name and annotations (under the PVC user's control).
    let mut name_params = HashMap::from([
        ("pv.name".to_string(), pv_name.to_string()),
        ("pvc.name".to_string(), pvc.metadata.name.clone()),
        ("pvc.namespace".to_string(), pvc_ns),
    ]);
    for (k, v) in pvc.metadata.annotations.iter().flatten() {
        name_params.insert(format!("pvc.annotations['{k}']"), v.clone());
    }
    let name = resolve_template(&name_t, &name_params)
        .map_err(|e| format!("error resolving value {name_t:?}: {e}"))?;
    if !is_dns1123_subdomain(&name).is_empty() {
        return Err(if name_t != name {
            format!("{name_t:?} resolved to {name:?} which is not a valid secret name")
        } else {
            format!("{name_t:?} is not a valid secret name")
        });
    }
    Ok(Some(SecretReference {
        name: Some(name),
        namespace: Some(namespace),
    }))
}

/// `CheckPersistentVolumeClaimModeBlock`.
fn is_block(claim: &PersistentVolumeClaim) -> bool {
    matches!(claim.spec.volume_mode, Some(PersistentVolumeMode::Block))
}

/// The `*v1.ObjectReference` `dataSource` (controller.go:2096) normalises a
/// claim's `dataSource` / `dataSourceRef` to; `api_group` is upstream's
/// `APIVersion` field, which it fills with the API group.
#[derive(Debug, Clone)]
struct DataSource {
    kind: String,
    name: String,
    api_group: String,
    namespace: String,
}

/// `dataSource` (controller.go:2096-2142). The cross-namespace branch needs
/// the `CrossNamespaceVolumeDataSource` gate (alpha, off by default) and a
/// ReferenceGrant check (`IsGranted`); neither is ported (#2981), so, as with
/// the gate disabled, a `dataSourceRef.namespace` is refused.
fn data_source(claim: &PersistentVolumeClaim) -> std::result::Result<Option<DataSource>, String> {
    let ns = claim.metadata.namespace.clone().unwrap_or_default();
    if let Some(d) = &claim.spec.data_source {
        return Ok(Some(DataSource {
            kind: d.kind.clone(),
            name: d.name.clone(),
            api_group: d.api_group.clone().unwrap_or_default(),
            namespace: ns,
        }));
    }
    if let Some(d) = &claim.spec.data_source_ref {
        if d.namespace.as_deref().is_some_and(|n| !n.is_empty()) {
            return Err("dataSourceRef namespace specified but the CrossNamespaceVolumeDataSource feature is disabled".to_string());
        }
        return Ok(Some(DataSource {
            kind: d.kind.clone(),
            name: d.name.clone(),
            api_group: d.api_group.clone().unwrap_or_default(),
            namespace: ns,
        }));
    }
    Ok(None)
}

/// `Quantity.Value()` of a `storage` entry, `0` when absent (Go's zero value).
fn storage_bytes(q: Option<&String>) -> i64 {
    q.and_then(|q| Quantity::parse(q).ok())
        .map_or(0, |q| q.value() as i64)
}

/// What `prepareProvision` returns.
struct Prepared {
    fs_type: String,
    req: CreateVolumeRequest,
    /// `csiPVSource`: the secret refs `Provision` copies onto the PV.
    controller_publish_secret_ref: Option<SecretReference>,
    node_stage_secret_ref: Option<SecretReference>,
    node_publish_secret_ref: Option<SecretReference>,
    controller_expand_secret_ref: Option<SecretReference>,
    node_expand_secret_ref: Option<SecretReference>,
    /// `provDeletionSecrets` (the provisioner secret) and `provModifySecrets`.
    deletion_secret: Option<SecretReference>,
    modify_secret: Option<SecretReference>,
}

pub struct CsiProvisioner<S: Storage> {
    storage: Arc<S>,
    driver_name: String,
    client: CsiControllerClient,
    /// `identity` (csi-provisioner.go:298), stamped into every volume's
    /// attributes as `storage.kubernetes.io/csiProvisionerIdentity`.
    identity: String,
    default_fs_type: String,
    extra_create_metadata: bool,
    controller_publish_read_only: bool,
    prevent_volume_mode_conversion: bool,
    recorder: EventRecorder<S>,
    /// `claimsInProgress`: claims whose CreateVolume may still be running in
    /// the driver; kept so a claim deleted meanwhile is still driven to
    /// completion (or cleaned up).
    claims_in_progress: Mutex<HashMap<String, PersistentVolumeClaim>>,
    /// `queueStore.volumes`.
    unsaved: Mutex<HashMap<String, Unsaved>>,
    /// `RetryIntervalMax` (`--retry-interval-max`): the queue's backoff cap
    /// and the `slowSet`'s retention (lib controller.go:682).
    retry_interval_max: Duration,
    /// `slowSet`, keyed by claim UID.
    slow_set: Mutex<HashMap<String, SlowEntry>>,
    /// `--strict-topology` (csi-provisioner.go:91, default false).
    strict_topology: bool,
    /// `--immediate-topology` (csi-provisioner.go:92, default true).
    immediate_topology: bool,
    /// `pvcNodeStore` (cache.go `InMemoryStore`), keyed by PVC UID.
    pvc_node_store: TopologyStore,
    capabilities: tokio::sync::OnceCell<DriverCapabilities>,
}

impl<S: Storage + 'static> CsiProvisioner<S> {
    pub fn new(
        storage: Arc<S>,
        driver_name: impl Into<String>,
        client: CsiControllerClient,
    ) -> Self {
        let driver_name = driver_name.into();
        // csi-provisioner.go:298: `<unix ts>-<rand 0..9999>-<provisioner name>`.
        let identity = format!(
            "{}-{}-{}",
            chrono::Utc::now().timestamp(),
            rand::random::<u32>() % 10000,
            driver_name
        );
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            driver_name,
            client,
            identity,
            default_fs_type: String::new(),
            extra_create_metadata: false,
            controller_publish_read_only: false,
            // `--prevent-volume-mode-conversion` defaults to true
            // (csi-provisioner.go:112).
            prevent_volume_mode_conversion: true,
            claims_in_progress: Mutex::new(HashMap::new()),
            unsaved: Mutex::new(HashMap::new()),
            retry_interval_max: RETRY_INTERVAL_MAX,
            slow_set: Mutex::new(HashMap::new()),
            strict_topology: false,
            immediate_topology: true,
            pvc_node_store: TopologyStore::new(),
            capabilities: tokio::sync::OnceCell::new(),
        }
    }

    /// `--strict-topology`: pass only the selected node's topology.
    pub fn with_strict_topology(mut self, on: bool) -> Self {
        self.strict_topology = on;
        self
    }

    /// `--immediate-topology`: with immediate binding, pass the aggregated
    /// cluster topology (true, the default) or none.
    pub fn with_immediate_topology(mut self, on: bool) -> Self {
        self.immediate_topology = on;
        self
    }

    /// `--default-fstype`.
    pub fn with_default_fs_type(mut self, fs_type: impl Into<String>) -> Self {
        self.default_fs_type = fs_type.into();
        self
    }

    /// `--extra-create-metadata`.
    pub fn with_extra_create_metadata(mut self, on: bool) -> Self {
        self.extra_create_metadata = on;
        self
    }

    /// `--controller-publish-readonly`.
    pub fn with_controller_publish_read_only(mut self, on: bool) -> Self {
        self.controller_publish_read_only = on;
        self
    }

    /// `RetryIntervalMax` (`--retry-interval-max`).
    pub fn with_retry_interval_max(mut self, max: Duration) -> Self {
        self.retry_interval_max = max;
        self
    }

    /// `--prevent-volume-mode-conversion`.
    pub fn with_prevent_volume_mode_conversion(mut self, on: bool) -> Self {
        self.prevent_volume_mode_conversion = on;
        self
    }

    // ---- events -----------------------------------------------------------

    fn claim_ref(claim: &PersistentVolumeClaim) -> ObjectReference {
        ObjectReference {
            kind: Some("PersistentVolumeClaim".to_string()),
            namespace: claim.metadata.namespace.clone(),
            name: Some(claim.metadata.name.clone()),
            uid: Some(claim.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: claim.metadata.resource_version.clone(),
            field_path: None,
        }
    }

    async fn event(
        &self,
        claim: &PersistentVolumeClaim,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        self.event_for(&Self::claim_ref(claim), event_type, reason, message)
            .await;
    }

    fn volume_ref(pv: &PersistentVolume) -> ObjectReference {
        ObjectReference {
            kind: Some("PersistentVolume".to_string()),
            namespace: None,
            name: Some(pv.metadata.name.clone()),
            uid: Some(pv.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: pv.metadata.resource_version.clone(),
            field_path: None,
        }
    }

    async fn event_for(
        &self,
        object: &ObjectReference,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let source = EventSource {
            component: format!("{}_{}", self.driver_name, self.identity),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(object, &source, event_type, reason, message)
            .await
        {
            warn!("failed to record {reason} event: {e}");
        }
    }

    // ---- sync -------------------------------------------------------------

    async fn driver_capabilities(
        &self,
    ) -> std::result::Result<&DriverCapabilities, ControllerError> {
        self.capabilities
            .get_or_try_init(|| self.client.get_driver_capabilities())
            .await
    }

    async fn get_storage_class(&self, name: &str) -> Result<StorageClass> {
        self.storage
            .get(&build_key("storageclasses", None, name))
            .await
            .map_err(|_| anyhow!("storageClass {name:?} not found"))
    }

    /// `shouldProvision` (lib controller.go:1242-1283) behind the
    /// `Qualifier` (`ShouldProvision`, external-provisioner
    /// controller.go:1557-1592; no node deployment, so `checkNode` is always
    /// "owned").
    async fn should_provision(&self, claim: &PersistentVolumeClaim) -> Result<bool> {
        if claim
            .spec
            .volume_name
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        {
            return Ok(false);
        }
        let provisioner = annotation(&claim.metadata, ANN_STORAGE_PROVISIONER)
            .or_else(|| annotation(&claim.metadata, ANN_BETA_STORAGE_PROVISIONER));
        let migrated_to = annotation(&claim.metadata, ANN_MIGRATED_TO);
        if provisioner.map(String::as_str) != Some(self.driver_name.as_str())
            && migrated_to.map(String::as_str) != Some(self.driver_name.as_str())
        {
            return Ok(false);
        }
        let class = self.get_storage_class(&claim_class(claim)).await?;
        if matches!(
            class.volume_binding_mode,
            Some(VolumeBindingMode::WaitForFirstConsumer)
        ) {
            // "annSelectedNode is required to provision volume."
            return Ok(
                annotation(&claim.metadata, ANN_SELECTED_NODE).is_some_and(|n| !n.is_empty())
            );
        }
        Ok(true)
    }

    /// `syncClaim` (lib controller.go:1098-1147).
    pub async fn sync_claim(&self, claim: &PersistentVolumeClaim) -> Result<()> {
        self.delay_provisioning_if_recently_infeasible(claim)
            .await?;
        if !self.should_provision(claim).await? {
            return Ok(());
        }
        let uid = claim.metadata.uid.clone();
        match self.provision_claim_operation(claim).await {
            Ok(()) => {
                self.claims_in_progress.lock().unwrap().remove(&uid);
                Ok(())
            }
            Err((state, failure)) => {
                let result = match failure {
                    // "Our caller would requeue if we pass on this special
                    // error; return nil instead."
                    Failure::Stop | Failure::Ignored(_) => Ok(()),
                    Failure::Error(e) => Err(e),
                };
                match state {
                    ProvisioningState::Finished => {
                        self.claims_in_progress.lock().unwrap().remove(&uid);
                    }
                    ProvisioningState::InBackground => {
                        self.claims_in_progress
                            .lock()
                            .unwrap()
                            .insert(uid, claim.clone());
                    }
                    // Don't change claimsInProgress.
                    ProvisioningState::NoChange | ProvisioningState::Reschedule => {}
                }
                result
            }
        }
    }

    /// `provisionClaimOperation` (lib controller.go:1436-1543).
    async fn provision_claim_operation(&self, claim: &PersistentVolumeClaim) -> Outcome<()> {
        let class_name = claim_class(claim);
        let ns = claim.metadata.namespace.as_deref().unwrap_or("");

        // "A previous doProvisionClaim may just have finished ... Check that
        // PV (with deterministic name) hasn't been provisioned yet."
        let pv_name = make_volume_name(VOLUME_NAME_PREFIX, &claim.metadata.uid)
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(anyhow!(e))))?;
        let pv_key = build_key("persistentvolumes", None, &pv_name);
        if self.storage.get::<PersistentVolume>(&pv_key).await.is_ok() {
            debug!("PersistentVolume {pv_name} already exists, skipping");
            return Err((ProvisioningState::Finished, Failure::Stop));
        }

        // canProvision: the CSI provisioner supports block (`SupportsBlock`).

        let class = self
            .get_storage_class(&class_name)
            .await
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(e)))?;
        if class.provisioner != self.driver_name {
            error!(
                "Unknown provisioner {:?} requested in claim's StorageClass",
                class.provisioner
            );
            return Err((ProvisioningState::Finished, Failure::Stop));
        }

        let selected_node = annotation(&claim.metadata, ANN_SELECTED_NODE)
            .or_else(|| annotation(&claim.metadata, ANN_ALPHA_SELECTED_NODE))
            .cloned()
            .unwrap_or_default();

        self.event(
            claim,
            EventType::Normal,
            "Provisioning",
            &format!(
                "External provisioner is provisioning volume for claim \"{ns}/{}\"",
                claim.metadata.name
            ),
        )
        .await;

        let mut volume = match self
            .provision(claim, &class, &selected_node, &pv_name)
            .await
        {
            Ok(v) => v,
            Err((_, Failure::Ignored(reason))) => {
                // "Provision ignored, do nothing and hope another provisioner
                // will provision it."
                debug!("Volume provision ignored: {reason}");
                return Err((ProvisioningState::Finished, Failure::Stop));
            }
            Err((state, Failure::Error(err))) => {
                // isInfeasibleError (lib :1587-1599): gRPC InvalidArgument.
                if err
                    .downcast_ref::<ControllerError>()
                    .and_then(ControllerError::status)
                    .is_some_and(|s| s.code() == tonic::Code::InvalidArgument)
                {
                    info!("Detected infeasible volume provisioning request: {err}");
                    self.event(
                        claim,
                        EventType::Warning,
                        "ProvisioningFailed",
                        &format!(
                            "Volume provisioning failed with infeasible error. Retries will be delayed. {err}"
                        ),
                    )
                    .await;
                    self.mark_for_slow_retry(claim, &class);
                    return Err((ProvisioningState::Finished, Failure::Error(err)));
                }
                return self
                    .provision_volume_error_handling(state, err, claim)
                    .await;
            }
            Err((state, Failure::Stop)) => return Err((state, Failure::Stop)),
        };

        // "Set ClaimRef and the PV controller will bind and set
        // annBoundByController for us"
        volume.spec.claim_ref = Some(Self::claim_ref(claim));
        // `addFinalizer` is true for csi-provisioner (csi-provisioner.go:409).
        let finalizers = volume.metadata.finalizers.get_or_insert_with(Vec::new);
        if !finalizers.iter().any(|f| f == FINALIZER_PV) {
            finalizers.push(FINALIZER_PV.to_string());
        }
        volume
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(
                ANN_DYNAMICALLY_PROVISIONED.to_string(),
                class.provisioner.clone(),
            );
        volume.spec.storage_class_name = Some(class_name);

        self.store_volume(claim, volume).await;
        Ok(())
    }

    // ---- slow retry of infeasible claims -------------------------------------

    /// `delayProvisioningIfRecentlyInfeasible` (lib controller.go:1545-1564).
    /// `slowset.Run`'s `removeAllExpired` (csi-lib-utils slowset.go) runs on a
    /// 100ms ticker upstream; expiry is applied on access here.
    async fn delay_provisioning_if_recently_infeasible(
        &self,
        claim: &PersistentVolumeClaim,
    ) -> Result<()> {
        let key = claim.metadata.uid.clone();
        let Ok(current_class) = self.get_storage_class(&claim_class(claim)).await else {
            return Ok(());
        };
        let max = self.retry_interval_max;
        let mut set = self.slow_set.lock().unwrap();
        set.retain(|_, e| e.timestamp.elapsed() <= max);
        if let Some(info) = set.get(&key) {
            if info.storage_class_uid != current_class.metadata.uid {
                set.remove(&key);
                return Ok(());
            }
            // `TimeRemaining(key) > 0`
            if max > info.timestamp.elapsed() {
                return Err(anyhow!(
                    "skipping volume provisioning for pvc {key}, because provisioning previously failed with infeasible error"
                ));
            }
        }
        Ok(())
    }

    /// `markForSlowRetry` (lib controller.go:1566-1585), for an infeasible
    /// error (the caller checked `isInfeasibleError`). `SlowSet.Add` keeps an
    /// existing entry.
    fn mark_for_slow_retry(&self, claim: &PersistentVolumeClaim, class: &StorageClass) {
        self.slow_set
            .lock()
            .unwrap()
            .entry(claim.metadata.uid.clone())
            .or_insert_with(|| SlowEntry {
                timestamp: Instant::now(),
                storage_class_uid: class.metadata.uid.clone(),
            });
    }

    // ---- delete path -----------------------------------------------------------

    /// `knownProvisioner` (lib controller.go:1233-1238); no additional names.
    fn known_provisioner(&self, name: &str) -> bool {
        name == self.driver_name
    }

    /// `isProvisionerForVolume` (lib controller.go:1175-1201).
    fn is_provisioner_for_volume(&self, volume: &PersistentVolume) -> bool {
        let migrated = annotation(&volume.metadata, ANN_MIGRATED_TO)
            .map(String::as_str)
            .unwrap_or_default();
        if let Some(provisioned_by) = annotation(&volume.metadata, ANN_DYNAMICALLY_PROVISIONED) {
            // "The current provisioner is not responsible for adding the
            // finalizer"
            self.known_provisioner(provisioned_by) || self.known_provisioner(migrated)
        } else if let Some(csi) = volume.spec.csi.as_ref() {
            // Statically provisioned volume of type CSI.
            self.known_provisioner(&csi.driver)
        } else {
            self.known_provisioner(migrated)
        }
    }

    /// `handleProtectionFinalizer` (lib controller.go:1203-1229), with
    /// `addFinalizer` true (csi-provisioner.go:409). The patch is a full
    /// update here; a conflict surfaces as an error and the queue retries.
    async fn handle_protection_finalizer(
        &self,
        volume: &PersistentVolume,
    ) -> Result<PersistentVolume> {
        let reclaim = volume.spec.persistent_volume_reclaim_policy.as_ref();
        let mut finalizers = volume.metadata.finalizers.clone().unwrap_or_default();
        let mut modified = false;

        // "Add the finalizer only if ... finalizer doesn't exist and PV is
        // not already under deletion."
        let bound = volume
            .status
            .as_ref()
            .is_some_and(|s| s.phase == PersistentVolumePhase::Bound);
        if reclaim == Some(&PersistentVolumeReclaimPolicy::Delete)
            && volume.metadata.deletion_timestamp.is_none()
            && bound
            && !finalizers.iter().any(|f| f == FINALIZER_PV)
        {
            finalizers.push(FINALIZER_PV.to_string());
            modified = true;
        }
        // "... the reclaim policy is changed to `Retain` or `Recycle`"
        if matches!(
            reclaim,
            Some(PersistentVolumeReclaimPolicy::Retain | PersistentVolumeReclaimPolicy::Recycle)
        ) {
            let before = finalizers.len();
            finalizers.retain(|f| f != FINALIZER_PV);
            modified = finalizers.len() != before;
        }

        if !modified {
            return Ok(volume.clone());
        }
        let mut patched = volume.clone();
        patched.metadata.finalizers = (!finalizers.is_empty()).then_some(finalizers.clone());
        let key = build_key("persistentvolumes", None, &volume.metadata.name);
        self.storage.update(&key, &patched).await.map_err(|e| {
            anyhow!(
                "failed to modify finalizers to {finalizers:?} on volume {} err: {e}",
                volume.metadata.name
            )
        })
    }

    /// `shouldDelete` (lib controller.go:1285-1319), `addFinalizer` true.
    fn should_delete(volume: &PersistentVolume) -> bool {
        // "The finalizer was removed, i.e. the volume has been already deleted."
        if !has_finalizer(&volume.metadata, FINALIZER_PV)
            && volume.metadata.deletion_timestamp.is_some()
        {
            return false;
        }
        volume
            .status
            .as_ref()
            .is_some_and(|s| s.phase == PersistentVolumePhase::Released)
            && volume.spec.persistent_volume_reclaim_policy
                == Some(PersistentVolumeReclaimPolicy::Delete)
    }

    /// `syncVolume` (lib controller.go:1149-1173).
    pub async fn sync_volume(&self, volume: &PersistentVolume) -> Result<()> {
        if !self.is_provisioner_for_volume(volume) {
            // "Current provisioner is not responsible for the volume"
            return Ok(());
        }
        let volume = self.handle_protection_finalizer(volume).await?;
        if Self::should_delete(&volume) {
            debug!("shouldDelete PV {}", volume.metadata.name);
            return self.delete_volume_operation(&volume).await;
        }
        Ok(())
    }

    /// `deleteVolumeOperation` (lib controller.go:1636-1704).
    async fn delete_volume_operation(&self, volume: &PersistentVolume) -> Result<()> {
        let object = Self::volume_ref(volume);
        match self.delete_csi_volume(volume).await {
            Ok(()) => {}
            Err(DeleteFailure::InUse(reason)) => {
                // "Volume is still in use, retry later without treating it
                // as a failure."
                debug!("Volume deletion postponed: {reason}");
                self.event_for(&object, EventType::Normal, "VolumeDelete", &reason)
                    .await;
                return Err(anyhow!(reason));
            }
            Err(DeleteFailure::Error(e)) => {
                error!("Volume deletion failed: {e:#}");
                self.event_for(
                    &object,
                    EventType::Warning,
                    "VolumeFailedDelete",
                    &e.to_string(),
                )
                .await;
                return Err(e);
            }
        }

        // `PersistentVolumes().Delete`: a client DELETE, which stamps
        // `deletionTimestamp` on a PV held by a finalizer.
        let key = build_key("persistentvolumes", None, &volume.metadata.name);
        self.storage
            .delete_gracefully(&key)
            .await
            .context("failed to delete persistentvolume")?;

        if volume
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| !f.is_empty())
        {
            // "Remove external-provisioner finalizer": "need to get the pv
            // again because the delete has updated the object with a
            // deletion timestamp".
            let mut fresh: PersistentVolume = match self.storage.get(&key).await {
                Ok(v) => v,
                Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
                Err(e) => {
                    return Err(anyhow!(
                        "failed to get persistentvolume to update finalizer: {e}"
                    ))
                }
            };
            let before = fresh.metadata.finalizers.as_ref().map_or(0, Vec::len);
            if let Some(f) = fresh.metadata.finalizers.as_mut() {
                f.retain(|f| f != FINALIZER_PV);
            }
            if fresh.metadata.finalizers.as_ref().map_or(0, Vec::len) != before {
                if fresh
                    .metadata
                    .finalizers
                    .as_ref()
                    .is_some_and(|f| f.is_empty())
                {
                    fresh.metadata.finalizers = None;
                }
                match self.storage.update(&key, &fresh).await {
                    Ok(_) => {}
                    Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
                    Err(e) => {
                        return Err(anyhow!(
                            "failed to remove finalizer for persistentvolume: {e}"
                        ))
                    }
                }
                // The API server removes an object whose last finalizer went
                // away while it is being deleted; a direct store has no
                // server in the path to do it.
                if fresh.metadata.deletion_timestamp.is_some()
                    && fresh.metadata.finalizers.is_none()
                {
                    match self.storage.delete(&key).await {
                        Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
                        Err(e) => return Err(anyhow!("failed to delete persistentvolume: {e}")),
                    }
                }
            }
        }
        debug!("PersistentVolume {} deleted", volume.metadata.name);
        Ok(())
    }

    /// `csiProvisioner.Delete` (external-provisioner controller.go:1390-1456).
    async fn delete_csi_volume(
        &self,
        volume: &PersistentVolume,
    ) -> std::result::Result<(), DeleteFailure> {
        let Some(csi) = volume.spec.csi.as_ref() else {
            return Err(anyhow!("invalid CSI PV").into());
        };
        // `volumeHandleToId`
        let volume_id = csi.volume_handle.clone().unwrap_or_default();

        let caps = self
            .driver_capabilities()
            .await
            .map_err(anyhow::Error::new)?;
        check_driver_capabilities(caps, &RequiredCapabilities::default())
            .map_err(anyhow::Error::new)?;

        let secrets = self.secrets_for_deletion(volume).await?;
        self.can_delete_volume(volume, caps).await?;

        // One DeleteVolume under the client's `--timeout`.
        self.client
            .delete_volume(DeleteVolumeRequest { volume_id, secrets })
            .await
            .map_err(|e| anyhow::Error::new(e).into())
    }

    /// `handleSecretsForDeletion` / `getSecretsFromSC`
    /// (controller.go:1458-1524).
    async fn secrets_for_deletion(
        &self,
        volume: &PersistentVolume,
    ) -> Result<HashMap<String, String>> {
        let name = annotation(&volume.metadata, ANN_DELETION_SECRET_NAME);
        let namespace = annotation(&volume.metadata, ANN_DELETION_SECRET_NAMESPACE);
        if let (Some(name), Some(namespace)) = (name, namespace) {
            if !name.is_empty() && !namespace.is_empty() {
                return Ok(match self.get_credentials(namespace, name).await {
                    Ok(c) => c,
                    Err(e) => {
                        // "Continue with deletion, as the secret may have
                        // already been deleted."
                        error!(
                            "failed to get credentials for volume {}: {e}",
                            volume.metadata.name
                        );
                        HashMap::new()
                    }
                });
            }
            return Ok(HashMap::new());
        }

        // `getSecretsFromSC`: only the "no secret parameters" outcome is
        // ported (a class with secret parameters needs `getSecretReference`).
        let class_name = annotation(&volume.metadata, ANN_BETA_STORAGE_CLASS)
            .cloned()
            .or_else(|| volume.spec.storage_class_name.clone())
            .unwrap_or_default();
        if class_name.is_empty() {
            return Ok(HashMap::new());
        }
        match self.get_storage_class(&class_name).await {
            Ok(class) => {
                if volume.spec.claim_ref.is_none() {
                    warn!(
                        "claim reference does not exists in volume: {}, proceeding to delete without secrets.",
                        volume.metadata.name
                    );
                    return Ok(HashMap::new());
                }
                // The claim is rebuilt from the claimRef: only its name and
                // namespace are known (no annotations).
                let claim_ref = volume.spec.claim_ref.as_ref().expect("checked above");
                let mut meta = ObjectMeta::new(claim_ref.name.clone().unwrap_or_default());
                meta.namespace = claim_ref.namespace.clone();
                let pvc = PersistentVolumeClaim {
                    type_meta: TypeMeta::default(),
                    metadata: meta,
                    spec: Default::default(),
                    status: None,
                };
                let sc_params = class.parameters.clone().unwrap_or_default();
                let secret_ref = get_secret_reference(
                    &PROVISIONER_SECRET_PARAMS,
                    &sc_params,
                    &volume.metadata.name,
                    &pvc,
                )
                .map_err(|e| {
                    anyhow!(
                        "failed to get secretreference for volume {}: {e}",
                        volume.metadata.name
                    )
                })?;
                return Ok(match self.credentials(secret_ref.as_ref()).await {
                    Ok(c) => c,
                    Err(e) => {
                        // "Continue with deletion, as the secret may have
                        // already been deleted."
                        error!(
                            "Failed to get credentials for volume {}: {e}",
                            volume.metadata.name
                        );
                        HashMap::new()
                    }
                });
            }
            Err(e) => warn!(
                "failed to get storageclass: {class_name}, proceeding to delete without secrets. {e}"
            ),
        }
        Ok(HashMap::new())
    }

    /// `getCredentials` for a possibly-absent reference (controller.go:2004-2008).
    async fn credentials(&self, r: Option<&SecretReference>) -> Result<HashMap<String, String>> {
        match r {
            None => Ok(HashMap::new()),
            Some(r) => {
                self.get_credentials(
                    r.namespace.as_deref().unwrap_or_default(),
                    r.name.as_deref().unwrap_or_default(),
                )
                .await
            }
        }
    }

    /// `getCredentials` (controller.go:2004-2019).
    async fn get_credentials(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<HashMap<String, String>> {
        let secret: Secret = self
            .storage
            .get(&build_key("secrets", Some(namespace), name))
            .await
            .map_err(|e| anyhow!("error getting secret {name} in namespace {namespace}: {e}"))?;
        Ok(secret
            .data
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
            .collect())
    }

    /// `canDeleteVolume` (controller.go:1526-1550). The `vaLister` exists only
    /// when the driver reports PUBLISH_UNPUBLISH_VOLUME
    /// (csi-provisioner.go:312).
    async fn can_delete_volume(
        &self,
        volume: &PersistentVolume,
        caps: &DriverCapabilities,
    ) -> std::result::Result<(), DeleteFailure> {
        if !caps
            .controller
            .contains(&ControllerRpc::PublishUnpublishVolume)
        {
            return Ok(());
        }
        let attachments: Vec<VolumeAttachment> = self
            .storage
            .list("/registry/volumeattachments/")
            .await
            .map_err(|e| anyhow!("failed to list volumeattachments: {e}"))?;
        for va in attachments {
            if va.spec.source.persistent_volume_name.as_deref() == Some(&volume.metadata.name) {
                return Err(DeleteFailure::InUse(format!(
                    "persistentvolume {} is still attached to node {}, waiting for detach",
                    volume.metadata.name, va.spec.node_name
                )));
            }
        }
        Ok(())
    }

    /// `provisionVolumeErrorHandling` (lib controller.go:1601-1634).
    async fn provision_volume_error_handling(
        &self,
        mut state: ProvisioningState,
        err: anyhow::Error,
        claim: &PersistentVolumeClaim,
    ) -> Outcome<()> {
        self.event(
            claim,
            EventType::Warning,
            "ProvisioningFailed",
            &err.to_string(),
        )
        .await;
        if annotation(&claim.metadata, ANN_SELECTED_NODE).is_some()
            && state == ProvisioningState::Reschedule
        {
            // "`selectedNode` must be removed to notify scheduler to schedule
            // again." (`rescheduleProvisioning`, lib :1393).
            return match self.reschedule_provisioning(claim).await {
                Ok(()) => {
                    info!("Volume rescheduled because: {err}");
                    Err((ProvisioningState::Finished, Failure::Stop))
                }
                Err(e) => {
                    warn!("Volume rescheduling failed: {e}");
                    Err((ProvisioningState::Finished, Failure::Error(err)))
                }
            };
        }
        // "ProvisioningReschedule shouldn't have been returned for volumes
        // without selected node ... treat it like ProvisioningFinished."
        if state == ProvisioningState::Reschedule {
            state = ProvisioningState::Finished;
        }
        Err((state, Failure::Error(err)))
    }

    /// `rescheduleProvisioning` (lib controller.go:1393-1434).
    async fn reschedule_provisioning(&self, claim: &PersistentVolumeClaim) -> Result<()> {
        let ns = claim.metadata.namespace.as_deref().unwrap_or("");
        let key = build_key("persistentvolumeclaims", Some(ns), &claim.metadata.name);
        let mut c = claim.clone();
        if let Some(a) = c.metadata.annotations.as_mut() {
            a.remove(ANN_SELECTED_NODE);
        }
        self.storage.update(&key, &c).await?;
        Ok(())
    }

    // ---- Provision ----------------------------------------------------------

    /// `getVolumeCapabilities` / `getVolumeCapability` (controller.go:515-577).
    fn volume_capabilities(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        fs_type: &str,
        single_node_multi_writer: bool,
    ) -> std::result::Result<Vec<VolumeCapability>, String> {
        let mut caps = Vec::new();
        for mode in &claim.spec.access_modes {
            let kind = to_csi_access_mode(std::slice::from_ref(mode), single_node_multi_writer)?;
            let access_type = if is_block(claim) {
                AccessType::Block(BlockVolume {})
            } else {
                AccessType::Mount(MountVolume {
                    fs_type: fs_type.to_string(),
                    mount_flags: sc.mount_options.clone().unwrap_or_default(),
                    volume_mount_group: String::new(),
                })
            };
            caps.push(VolumeCapability {
                access_type: Some(access_type),
                access_mode: Some(AccessMode { mode: kind as i32 }),
            });
        }
        Ok(caps)
    }

    /// `prepareProvision` (controller.go:579-830), minus the parts listed in
    /// the module docs.
    async fn prepare_provision(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        selected_node: &str,
        pv_name: &str,
    ) -> Outcome<Prepared> {
        let fin = |e: String| (ProvisioningState::Finished, Failure::Error(anyhow!(e)));

        // dataSource normalisation + required capabilities (:586-:625).
        let mut rc = RequiredCapabilities::default();
        let data_source = data_source(claim).map_err(fin)?;
        if let Some(ds) = &data_source {
            // PVC.Spec.DataSource.Name is the name of the VolumeSnapshot API object
            if ds.name.is_empty() {
                return Err(fin(format!(
                    "the PVC source not found for PVC {}",
                    claim.metadata.name
                )));
            }
            match ds.kind.as_str() {
                SNAPSHOT_KIND => {
                    if ds.api_group != SNAPSHOT_API_GROUP {
                        return Err(fin(format!(
                            "the PVC source does not belong to the right APIGroup. Expected {SNAPSHOT_API_GROUP}, Got {}",
                            ds.api_group
                        )));
                    }
                    rc.snapshot = true;
                }
                PVC_KIND => rc.clone = true,
                other => {
                    // "Assume external data populator to create the volume,
                    // and there is no more work for us to do"
                    self.event(
                        claim,
                        EventType::Normal,
                        "Provisioning",
                        "Assuming an external populator will provision the volume",
                    )
                    .await;
                    return Err((
                        ProvisioningState::Finished,
                        Failure::Ignored(format!(
                            "data source ({other}) is not handled by the provisioner, assuming an external populator will provision it"
                        )),
                    ));
                }
            }
        }
        // `vacName` (:638-:647): a claim's VolumeAttributesClass needs
        // MODIFY_VOLUME.
        let vac_name = claim
            .spec
            .volume_attributes_class_name
            .clone()
            .unwrap_or_default();
        if !vac_name.is_empty() {
            rc.modify_volume = true;
        }

        let caps = self.driver_capabilities().await.map_err(|e| {
            (
                e.provisioning_state(false),
                Failure::Error(anyhow::Error::new(e)),
            )
        })?;
        check_driver_capabilities(caps, &rc)
            .map_err(|e| (ProvisioningState::Finished, Failure::Error(e.into())))?;

        if claim.spec.selector.is_some() {
            return Err(fin("claim Selector is not supported".into()));
        }

        // fstype from the class parameters (:665-:683).
        let params = sc.parameters.clone().unwrap_or_default();
        let mut fs_types_found = 0;
        let mut fs_type = String::new();
        for (k, v) in &params {
            if k.to_lowercase() == "fstype" || k == PREFIXED_FSTYPE_KEY {
                fs_type = v.clone();
                fs_types_found += 1;
            }
        }
        if fs_types_found > 1 {
            return Err(fin(format!(
                "fstype specified in parameters with both \"fstype\" and \"{PREFIXED_FSTYPE_KEY}\" keys"
            )));
        }
        if fs_type.is_empty() && !self.default_fs_type.is_empty() {
            fs_type = self.default_fs_type.clone();
        }

        let requested = claim
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .map(|q| Quantity::parse(q))
            .transpose()
            .map_err(|e| fin(format!("invalid storage request: {e}")))?;
        let required_bytes = requested.map_or(0, |q| q.value() as i64);

        let volume_caps = self
            .volume_capabilities(
                claim,
                sc,
                &fs_type,
                caps.controller.contains(
                    &rusternetes_csi::proto::controller_service_capability::rpc::Type::SingleNodeMultiWriter,
                ),
            )
            .map_err(fin)?;

        // Resolve the provision secret credentials (:743-:752); every
        // failure here is ProvisioningNoChange.
        let no_change = |e: String| (ProvisioningState::NoChange, Failure::Error(anyhow!(e)));
        let provisioner_secret_ref =
            get_secret_reference(&PROVISIONER_SECRET_PARAMS, &params, pv_name, claim)
                .map_err(no_change)?;
        let provisioner_credentials = self
            .credentials(provisioner_secret_ref.as_ref())
            .await
            .map_err(|e| (ProvisioningState::NoChange, Failure::Error(e)))?;

        // Controller publish, node stage, node publish, expand and modify
        // secret references (:754-:777).
        let secret_ref = |p: &SecretParams| {
            get_secret_reference(p, &params, pv_name, claim)
                .map_err(|e| (ProvisioningState::NoChange, Failure::Error(anyhow!(e))))
        };
        let controller_publish_secret_ref = secret_ref(&CONTROLLER_PUBLISH_SECRET_PARAMS)?;
        let node_stage_secret_ref = secret_ref(&NODE_STAGE_SECRET_PARAMS)?;
        let node_publish_secret_ref = secret_ref(&NODE_PUBLISH_SECRET_PARAMS)?;
        let controller_expand_secret_ref = secret_ref(&CONTROLLER_EXPAND_SECRET_PARAMS)?;
        let node_expand_secret_ref = secret_ref(&NODE_EXPAND_SECRET_PARAMS)?;
        let modify_secret = secret_ref(&CONTROLLER_MODIFY_SECRET_PARAMS)?;

        let mut parameters = remove_prefixed_parameters(&params).map_err(|e| {
            fin(format!(
                "failed to strip CSI Parameters of prefixed keys: {e}"
            ))
        })?;
        if self.extra_create_metadata {
            parameters.insert(PVC_NAME_KEY.into(), claim.metadata.name.clone());
            parameters.insert(
                PVC_NAMESPACE_KEY.into(),
                claim.metadata.namespace.clone().unwrap_or_default(),
            );
            parameters.insert(PV_NAME_KEY.into(), pv_name.to_string());
        }

        let mut req = CreateVolumeRequest {
            name: pv_name.to_string(),
            parameters,
            volume_capabilities: volume_caps,
            capacity_range: Some(CapacityRange {
                required_bytes,
                limit_bytes: 0,
            }),
            secrets: provisioner_credentials,
            ..Default::default()
        };

        // The claim's VolumeAttributesClass (:814-:825).
        if !vac_name.is_empty() {
            let vac: VolumeAttributesClass = self
                .storage
                .get(&build_key("volumeattributesclasses", None, &vac_name))
                .await
                .map_err(|e| {
                    (
                        ProvisioningState::NoChange,
                        Failure::Error(anyhow!("volumeattributesclasses {vac_name:?}: {e}")),
                    )
                })?;
            if vac.driver_name != self.driver_name {
                return Err(fin(format!(
                    "VAC {vac_name} referenced in PVC is for driver {} which does not match driver name {}",
                    vac.driver_name, self.driver_name
                )));
            }
            req.mutable_parameters = vac.parameters.unwrap_or_default();
        }

        if let Some(ds) = data_source.as_ref().filter(|_| rc.clone || rc.snapshot) {
            let source = self
                .get_volume_content_source(claim, sc, ds)
                .await
                .map_err(|e| {
                    (
                        ProvisioningState::NoChange,
                        Failure::Error(anyhow!(
                            "error getting handle for DataSource Type {} by Name {}: {e}",
                            ds.kind,
                            ds.name
                        )),
                    )
                })?;
            req.volume_content_source = source;
            if rc.clone {
                self.set_clone_finalizer(ds)
                    .await
                    .map_err(|e| (ProvisioningState::NoChange, Failure::Error(e)))?;
            }
            if rc.snapshot {
                self.set_snapshot_finalizer(ds)
                    .await
                    .map_err(|e| (ProvisioningState::NoChange, Failure::Error(e)))?;
            }
        }
        // `if p.supportsTopology()` (controller.go:720-739). Upstream runs this
        // before resolving the secrets; both failures are NoChange here.
        if caps.supports_topology() {
            let requirements = TopologyInputs {
                storage: &*self.storage,
                store: &self.pvc_node_store,
                driver_name: &self.driver_name,
                pvc_uid: &claim.metadata.uid,
                pvc_name: &claim.metadata.name,
                allowed_topologies: sc.allowed_topologies.as_deref().unwrap_or_default(),
                selected_node_name: selected_node,
                strict_topology: self.strict_topology,
                immediate_topology: self.immediate_topology,
            }
            .generate_accessibility_requirements()
            .await
            .map_err(|e| match e {
                // "The node or CSINode object can't be found, ask the
                // scheduler for a reschedule"
                TopologyError::NotFound(m) => {
                    (ProvisioningState::Reschedule, Failure::Error(anyhow!(m)))
                }
                TopologyError::Other(m) => (
                    ProvisioningState::NoChange,
                    Failure::Error(anyhow!("error generating accessibility requirements: {m}")),
                ),
            })?;
            req.accessibility_requirements = requirements;
        }
        Ok(Prepared {
            fs_type,
            req,
            controller_publish_secret_ref,
            node_stage_secret_ref,
            node_publish_secret_ref,
            controller_expand_secret_ref,
            node_expand_secret_ref,
            deletion_secret: provisioner_secret_ref,
            modify_secret,
        })
    }

    // ---- data sources ---------------------------------------------------------

    /// `getVolumeContentSource` (controller.go:1184-1196).
    async fn get_volume_content_source(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        ds: &DataSource,
    ) -> Result<Option<VolumeContentSource>> {
        match ds.kind.as_str() {
            SNAPSHOT_KIND => self.get_snapshot_source(claim, sc, ds).await.map(Some),
            PVC_KIND => self.get_pvc_source(claim, sc, ds).await.map(Some),
            // "treat it as a noop and extend as needed"
            _ => Ok(None),
        }
    }

    /// `getPVCSource` (controller.go:1198-1287).
    async fn get_pvc_source(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        ds: &DataSource,
    ) -> Result<VolumeContentSource> {
        let source: PersistentVolumeClaim = self
            .storage
            .get(&build_key(
                "persistentvolumeclaims",
                Some(&ds.namespace),
                &ds.name,
            ))
            .await
            .map_err(|e| {
                anyhow!(
                    "error getting PVC {} (namespace {:?}) from api server: {e}",
                    ds.name,
                    claim.metadata.namespace.as_deref().unwrap_or("")
                )
            })?;
        let phase = source.status.as_ref().map(|s| s.phase.clone());
        if phase != Some(PersistentVolumeClaimPhase::Bound) {
            return Err(anyhow!(
                "the PVC DataSource {} must have a status of Bound.  Got {:?}",
                ds.name,
                phase
            ));
        }
        if source.metadata.deletion_timestamp.is_some() {
            return Err(anyhow!(
                "the PVC DataSource {} is currently being deleted",
                ds.name
            ));
        }
        if source.spec.storage_class_name.is_none() {
            return Err(anyhow!(
                "the source PVC ({}) storageclass cannot be empty",
                source.metadata.name
            ));
        }
        if claim.spec.storage_class_name.is_none() {
            return Err(anyhow!(
                "the requested PVC ({}) storageclass cannot be empty",
                claim.metadata.name
            ));
        }

        let requested = storage_bytes(
            claim
                .spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        );
        let source_size = storage_bytes(
            source
                .spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        );
        if requested < source_size {
            return Err(anyhow!(
                "error, new PVC request must be greater than or equal in size to the specified PVC data source, requested {requested} but source is {source_size}"
            ));
        }

        let volume_name = source.spec.volume_name.clone().unwrap_or_default();
        if volume_name.is_empty() {
            return Err(anyhow!(
                "volume name is empty in source PVC {}",
                source.metadata.name
            ));
        }
        let invalid = || anyhow!("claim in dataSource not bound or invalid");
        let source_pv: PersistentVolume = self
            .storage
            .get(&build_key("persistentvolumes", None, &volume_name))
            .await
            .map_err(|e| {
                warn!(
                    "error getting volume {volume_name} for PVC {}/{}: {e}",
                    ds.namespace, source.metadata.name
                );
                invalid()
            })?;
        let Some(csi) = source_pv.spec.csi.as_ref() else {
            warn!("error getting volume source from {volume_name}");
            return Err(invalid());
        };
        if csi.driver != sc.provisioner {
            warn!("the source volume {volume_name} is handled by a different CSI driver than requested by StorageClass");
            return Err(invalid());
        }
        let Some(claim_ref) = source_pv.spec.claim_ref.as_ref() else {
            warn!("the source volume {volume_name} is not bound");
            return Err(invalid());
        };
        if claim_ref.uid.as_deref() != Some(source.metadata.uid.as_str())
            || claim_ref.namespace.as_deref() != source.metadata.namespace.as_deref()
            || claim_ref.name.as_deref() != Some(source.metadata.name.as_str())
        {
            warn!("the source volume {volume_name} is bound to a different PVC than requested");
            return Err(invalid());
        }
        if source_pv.status.as_ref().map(|s| &s.phase) != Some(&PersistentVolumePhase::Bound) {
            warn!("the source volume {volume_name} status should be Bound");
            return Err(invalid());
        }

        let is_fs = |m: &Option<PersistentVolumeMode>| {
            m.as_ref()
                .is_none_or(|m| *m == PersistentVolumeMode::Filesystem)
        };
        if is_fs(&claim.spec.volume_mode) && !is_fs(&source_pv.spec.volume_mode) {
            return Err(anyhow!(
                "the source PVC and destination PVCs must have the same volume mode for cloning.  Source is Block, but new PVC requested Filesystem"
            ));
        }
        if claim.spec.volume_mode == Some(PersistentVolumeMode::Block)
            && source_pv.spec.volume_mode != Some(PersistentVolumeMode::Block)
        {
            return Err(anyhow!(
                "the source PVC and destination PVCs must have the same volume mode for cloning.  Source is Filesystem, but new PVC requested Block"
            ));
        }

        Ok(VolumeContentSource {
            r#type: Some(ContentSourceType::Volume(VolumeSource {
                volume_id: csi.volume_handle.clone().unwrap_or_default(),
            })),
        })
    }

    /// `getSnapshotSource` (controller.go:1289-1393).
    async fn get_snapshot_source(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        ds: &DataSource,
    ) -> Result<VolumeContentSource> {
        let snapshot: VolumeSnapshot = self
            .storage
            .get(&build_key("volumesnapshots", Some(&ds.namespace), &ds.name))
            .await
            .map_err(|e| anyhow!("error getting snapshot {} from api server: {e}", ds.name))?;

        // "If the finalizer exists, it means provisioning was started before
        // deletion began, so we should continue to prevent resource leaks. If
        // the finalizer doesn't exist, this is a new provisioning attempt and
        // should be rejected."
        if snapshot.metadata.deletion_timestamp.is_some()
            && !has_finalizer(&snapshot.metadata, SNAPSHOT_SOURCE_PROTECTION_FINALIZER)
        {
            return Err(anyhow!("snapshot {} is being deleted", ds.name));
        }

        let Some(content_name) = snapshot
            .status
            .as_ref()
            .and_then(|s| s.bound_volume_snapshot_content_name.clone())
        else {
            return Err(anyhow!("snapshot {} not bound", ds.name));
        };
        let content: VolumeSnapshotContent = self
            .storage
            .get(&build_key("volumesnapshotcontents", None, &content_name))
            .await
            .map_err(|e| {
                warn!(
                    "error getting snapshotcontent {content_name} for snapshot {}/{}: {e}",
                    ds.namespace, ds.name
                );
                anyhow!(
                    "error getting snapshotcontent {content_name} for snapshot {}",
                    ds.name
                )
            })?;

        let r = &content.spec.volume_snapshot_ref;
        if r.uid.as_deref() != Some(snapshot.metadata.uid.as_str())
            || r.namespace.as_deref() != snapshot.metadata.namespace.as_deref()
            || r.name.as_deref() != Some(snapshot.metadata.name.as_str())
        {
            return Err(anyhow!(
                "snapshotcontent {content_name} for snapshot {} is bound to a different snapshot",
                ds.name
            ));
        }
        if content.spec.driver != sc.provisioner {
            return Err(anyhow!(
                "snapshotcontent {content_name} for snapshot {} is not handled by CSI driver of StorageClass {}",
                ds.name,
                sc.metadata.name
            ));
        }
        if snapshot.status.as_ref().and_then(|s| s.ready_to_use) != Some(true) {
            return Err(anyhow!("snapshot {} is not Ready", ds.name));
        }
        let Some(handle) = content
            .status
            .as_ref()
            .and_then(|s| s.snapshot_handle.clone())
        else {
            return Err(anyhow!("snapshot handle {} is not available", ds.name));
        };

        if let Some(restore) = snapshot
            .status
            .as_ref()
            .and_then(|s| s.restore_size.as_ref())
        {
            let Some(requested) = claim
                .spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage"))
            else {
                return Err(anyhow!(
                    "error getting capacity for PVC {} when creating snapshot {}",
                    claim.metadata.name,
                    snapshot.metadata.name
                ));
            };
            let vol_size = storage_bytes(Some(requested));
            let restore_size = storage_bytes(Some(restore));
            // "the volume size should be equal to or larger than its
            // snapshot size." A larger one is the plugin's expansion to do.
            if vol_size < restore_size {
                return Err(anyhow!(
                    "requested volume size {vol_size} is less than the size {restore_size} for the source snapshot {}",
                    snapshot.metadata.name
                ));
            }
        }

        if self.prevent_volume_mode_conversion {
            if let (Some(src_mode), Some(mode)) = (
                content.spec.source_volume_mode.as_deref(),
                claim.spec.volume_mode.as_ref(),
            ) {
                let mode = match mode {
                    PersistentVolumeMode::Filesystem => "Filesystem",
                    PersistentVolumeMode::Block => "Block",
                    PersistentVolumeMode::Unknown(m) => m.as_str(),
                };
                if src_mode != mode {
                    let who = format!(
                        "requested volume {}/{} modifies the mode of the source volume but does not have permission to do so.",
                        claim.metadata.namespace.as_deref().unwrap_or(""),
                        claim.metadata.name
                    );
                    let allow = annotation(&content.metadata, ANN_ALLOW_VOLUME_MODE_CHANGE)
                        .ok_or_else(|| {
                            anyhow!(
                                "{who} {ANN_ALLOW_VOLUME_MODE_CHANGE} annotation is not present on snapshotcontent {}",
                                content.metadata.name
                            )
                        })?;
                    // Go's strconv.ParseBool.
                    let allowed = match allow.as_str() {
                        "1" | "t" | "T" | "true" | "TRUE" | "True" => true,
                        "0" | "f" | "F" | "false" | "FALSE" | "False" => false,
                        other => {
                            return Err(anyhow!(
                                "{who} failed to convert {ANN_ALLOW_VOLUME_MODE_CHANGE} annotation value to boolean with error: strconv.ParseBool: parsing {other:?}: invalid syntax"
                            ))
                        }
                    };
                    if !allowed {
                        return Err(anyhow!(
                            "{who} {ANN_ALLOW_VOLUME_MODE_CHANGE} is set to false on snapshotcontent {}",
                            content.metadata.name
                        ));
                    }
                }
            }
        }

        Ok(VolumeContentSource {
            r#type: Some(ContentSourceType::Snapshot(SnapshotSource {
                snapshot_id: handle,
            })),
        })
    }

    /// `setCloneFinalizer` (controller.go:1054-1068).
    async fn set_clone_finalizer(&self, ds: &DataSource) -> Result<()> {
        let key = build_key("persistentvolumeclaims", Some(&ds.namespace), &ds.name);
        let mut source: PersistentVolumeClaim = self.storage.get(&key).await?;
        if has_finalizer(&source.metadata, PVC_CLONE_FINALIZER) {
            return Ok(());
        }
        source
            .metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push(PVC_CLONE_FINALIZER.to_string());
        self.storage.update(&key, &source).await?;
        Ok(())
    }

    /// `setSnapshotFinalizer` (controller.go:1070-1095). Upstream tolerates a
    /// Forbidden update for RBAC back-compat; this process writes storage
    /// directly, so there is no such case.
    async fn set_snapshot_finalizer(&self, ds: &DataSource) -> Result<()> {
        let key = build_key("volumesnapshots", Some(&ds.namespace), &ds.name);
        let mut snapshot: VolumeSnapshot = self.storage.get(&key).await?;
        if has_finalizer(&snapshot.metadata, SNAPSHOT_SOURCE_PROTECTION_FINALIZER) {
            return Ok(());
        }
        snapshot
            .metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push(SNAPSHOT_SOURCE_PROTECTION_FINALIZER.to_string());
        self.storage.update(&key, &snapshot).await?;
        Ok(())
    }

    /// `removeSnapshotFinalizer` (controller.go:1097-1140): a missing
    /// snapshot, or one without the finalizer, is nothing to do.
    async fn remove_snapshot_finalizer(&self, namespace: &str, name: &str) -> Result<()> {
        let key = build_key("volumesnapshots", Some(namespace), name);
        let mut snapshot: VolumeSnapshot = match self.storage.get(&key).await {
            Ok(s) => s,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !has_finalizer(&snapshot.metadata, SNAPSHOT_SOURCE_PROTECTION_FINALIZER) {
            return Ok(());
        }
        if let Some(f) = snapshot.metadata.finalizers.as_mut() {
            f.retain(|f| f != SNAPSHOT_SOURCE_PROTECTION_FINALIZER);
        }
        match self.storage.update(&key, &snapshot).await {
            Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// `cleanupVolume` (controller.go:2074-2086).
    async fn cleanup_volume(&self, volume_id: &str) -> Result<()> {
        let mut last = None;
        for _ in 0..DELETE_VOLUME_RETRY_COUNT {
            match self
                .client
                .delete_volume(DeleteVolumeRequest {
                    volume_id: volume_id.to_string(),
                    ..Default::default()
                })
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => last = Some(e),
            }
        }
        Err(anyhow::Error::new(last.expect("retry count is non-zero")))
    }

    /// `Provision` (controller.go:838-1052).
    async fn provision(
        &self,
        claim: &PersistentVolumeClaim,
        sc: &StorageClass,
        selected_node: &str,
        pv_name: &str,
    ) -> Outcome<PersistentVolume> {
        let provisioner = annotation(&claim.metadata, ANN_STORAGE_PROVISIONER)
            .or_else(|| annotation(&claim.metadata, ANN_BETA_STORAGE_PROVISIONER));
        if provisioner.map(String::as_str) != Some(self.driver_name.as_str())
            && annotation(&claim.metadata, ANN_MIGRATED_TO).map(String::as_str)
                != Some(self.driver_name.as_str())
        {
            return Err((
                ProvisioningState::Finished,
                Failure::Ignored(format!(
                    "PVC annotated with external-provisioner name {} does not match provisioner driver name {}. This could mean the PVC is not migrated",
                    provisioner.map(String::as_str).unwrap_or(""),
                    self.driver_name
                )),
            ));
        }

        // prepareProvision returns ProvisioningNoChange alongside its
        // non-final failures; a `?` keeps the state it chose.
        let Prepared {
            fs_type,
            req,
            controller_publish_secret_ref,
            node_stage_secret_ref,
            node_publish_secret_ref,
            controller_expand_secret_ref,
            node_expand_secret_ref,
            deletion_secret,
            modify_secret,
        } = self
            .prepare_provision(claim, sc, selected_node, pv_name)
            .await?;
        let vol_size_bytes = req.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
        let vol_caps = req.volume_capabilities.clone();

        let supports_topology = self
            .driver_capabilities()
            .await
            .map(|c| c.supports_topology())
            .unwrap_or(false);
        let vol = match self.client.create_volume(req).await {
            Ok(v) => v,
            Err(e) => {
                // "Giving up after an error and telling the pod scheduler to
                // retry with a different node only makes sense if" the driver
                // supports topology and a node was selected (:884-:900).
                let may_reschedule = supports_topology && !selected_node.is_empty();
                let state = e.provisioning_state(may_reschedule);
                debug!("CreateVolume failed, may reschedule = {may_reschedule} => state = {state:?}: {e}");
                // "Delete the entry in in memory cache if the error is final"
                if supports_topology
                    && matches!(
                        state,
                        ProvisioningState::Finished | ProvisioningState::Reschedule
                    )
                {
                    self.pvc_node_store.delete(&claim.metadata.uid);
                }
                return Err((state, Failure::Error(anyhow::Error::new(e))));
            }
        };

        let mut volume_attributes: HashMap<String, String> =
            HashMap::from([(PROVISIONER_ID_KEY.to_string(), self.identity.clone())]);
        volume_attributes.extend(vol.volume_context.clone());
        let mut resp_cap = vol.capacity_bytes;

        // "According to CSI spec CreateVolume should be able to return
        // capacity = 0, which means it is unknown."
        if resp_cap == 0 {
            resp_cap = vol_size_bytes;
        } else if resp_cap < vol_size_bytes {
            let mut cap_err = format!(
                "created volume capacity {resp_cap} less than requested capacity {vol_size_bytes}"
            );
            if let Err(e) = self.cleanup_volume(&vol.volume_id).await {
                cap_err = format!(
                    "{cap_err}. Cleanup of volume {pv_name} failed, volume is orphaned: {e}"
                );
            }
            // "use InBackground to retry the call, hoping the volume is
            // deleted correctly next time."
            return Err((
                ProvisioningState::InBackground,
                Failure::Error(anyhow!(cap_err)),
            ));
        }

        // A data source must come back in the volume's content source
        // (:931-:945): without it the driver made a blank volume. (The
        // cross-namespace dataSourceRef arm of the condition is unreachable,
        // see `data_source`.)
        if claim.spec.data_source.is_some() && vol.content_source.is_none() {
            let mut source_err = "volume content source missing".to_string();
            if let Err(e) = self.cleanup_volume(&vol.volume_id).await {
                source_err = format!(
                    "{source_err}. cleanup of volume {pv_name} failed, volume is orphaned: {e}"
                );
            }
            return Err((
                ProvisioningState::InBackground,
                Failure::Error(anyhow!(source_err)),
            ));
        }

        // `ReadOnlyMany` on its own with --controller-publish-readonly (:965-:971).
        let pv_read_only = vol_caps.len() == 1
            && vol_caps[0].access_mode.as_ref().map(|m| m.mode)
                == Some(AccessModeKind::MultiNodeReaderOnly as i32)
            && self.controller_publish_read_only;

        let block = is_block(claim);
        let mut annotations = HashMap::new();
        // The provisioner secret, or empty strings without one (:982-:990).
        let (del_name, del_ns) = deletion_secret
            .map(|r| (r.name.unwrap_or_default(), r.namespace.unwrap_or_default()))
            .unwrap_or_default();
        annotations.insert(ANN_DELETION_SECRET_NAME.to_string(), del_name);
        annotations.insert(ANN_DELETION_SECRET_NAMESPACE.to_string(), del_ns);
        // Only when modify secrets are configured (:992-:996).
        if let Some(r) = modify_secret {
            annotations.insert(
                ANN_MODIFY_SECRET_NAME.to_string(),
                r.name.unwrap_or_default(),
            );
            annotations.insert(
                ANN_MODIFY_SECRET_NAMESPACE.to_string(),
                r.namespace.unwrap_or_default(),
            );
        }

        let mut meta = ObjectMeta::new(pv_name);
        meta.annotations = Some(annotations);
        let pv = PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: PersistentVolumeSpec {
                access_modes: claim.spec.access_modes.clone(),
                mount_options: sc.mount_options.clone(),
                capacity: HashMap::from([(
                    "storage".to_string(),
                    // `bytesToQuantity`: `resource.NewQuantity(bytes, BinarySI)`.
                    Quantity::from_value(resp_cap, Format::BinarySI).canonical_string(),
                )]),
                persistent_volume_reclaim_policy: sc.reclaim_policy.clone(),
                volume_mode: claim.spec.volume_mode.clone(),
                csi: Some(CSIVolumeSource {
                    driver: self.driver_name.clone(),
                    volume_handle: Some(vol.volume_id.clone()),
                    read_only: Some(pv_read_only),
                    // "Set FSType if PV is not Block Volume"
                    fs_type: if block { None } else { Some(fs_type) },
                    volume_attributes: Some(volume_attributes),
                    controller_publish_secret_ref,
                    node_stage_secret_ref,
                    node_publish_secret_ref,
                    controller_expand_secret_ref,
                    node_expand_secret_ref,
                }),
                // `pv.Spec.VolumeAttributesClassName = vacName` (:1016-:1018).
                volume_attributes_class_name: claim
                    .spec
                    .volume_attributes_class_name
                    .clone()
                    .filter(|v| !v.is_empty()),
                // `pv.Spec.NodeAffinity = GenerateVolumeNodeAffinity(
                // rep.Volume.AccessibleTopology)` (controller.go:1003-1005).
                node_affinity: if supports_topology {
                    generate_volume_node_affinity(&vol.accessible_topology)
                } else {
                    None
                },
                ..Default::default()
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Pending,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        };
        info!(
            "successfully created PV {} for PVC {} and csi volume name {}",
            pv.metadata.name, claim.metadata.name, vol.volume_id
        );

        // "Remove entry from the in memory cache" (:1039-:1041).
        if supports_topology {
            self.pvc_node_store.delete(&claim.metadata.uid);
        }

        // "Remove snapshot finalizer if this PVC was provisioned from a
        // snapshot" (:1042-:1049); a failure doesn't fail provisioning.
        if let Some(d) = claim
            .spec
            .data_source
            .as_ref()
            .filter(|d| d.kind == SNAPSHOT_KIND)
        {
            let ns = claim.metadata.namespace.as_deref().unwrap_or("");
            if let Err(e) = self.remove_snapshot_finalizer(ns, &d.name).await {
                warn!(
                    "Failed to remove snapshot finalizer from {ns}/{}: {e}",
                    d.name
                );
            }
        }

        Ok(pv)
    }

    // ---- volume store (`queueStore`) -----------------------------------------

    /// `queueStore.StoreVolume`: try once; on failure keep the PV and retry in
    /// the background, "consum[ing] any error".
    async fn store_volume(&self, claim: &PersistentVolumeClaim, pv: PersistentVolume) {
        match self.save_volume(&pv).await {
            Ok(()) => {
                self.send_success_event(claim, &pv.metadata.name).await;
            }
            Err(e) => {
                error!("Failed to save volume {}: {e}", pv.metadata.name);
                self.unsaved.lock().unwrap().insert(
                    pv.metadata.name.clone(),
                    Unsaved {
                        pv,
                        attempts: 1,
                        next_try: Instant::now() + backoff(1),
                    },
                );
            }
        }
    }

    /// `queueStore.doSaveVolume`: create the PV, `AlreadyExists` is success.
    async fn save_volume(&self, pv: &PersistentVolume) -> Result<()> {
        let key = build_key("persistentvolumes", None, &pv.metadata.name);
        match self.storage.create(&key, pv).await {
            Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => Ok(()),
            Err(e) => Err(anyhow!("error saving volume {}: {e}", pv.metadata.name)),
        }
    }

    async fn send_success_event(&self, claim: &PersistentVolumeClaim, pv_name: &str) {
        self.event(
            claim,
            EventType::Normal,
            "ProvisioningSucceeded",
            &format!("Successfully provisioned volume {pv_name}"),
        )
        .await;
    }

    /// `queueStore.processNextWorkItem`, one pass over the unsaved PVs that
    /// are due.
    pub async fn save_unsaved_volumes(&self) {
        let due: Vec<PersistentVolume> = {
            let now = Instant::now();
            self.unsaved
                .lock()
                .unwrap()
                .values()
                .filter(|u| u.next_try <= now)
                .map(|u| u.pv.clone())
                .collect()
        };
        for pv in due {
            let name = pv.metadata.name.clone();
            match self.save_volume(&pv).await {
                Ok(()) => {
                    self.unsaved.lock().unwrap().remove(&name);
                    if let Some(claim) = self.claim_for_volume(&pv).await {
                        self.send_success_event(&claim, &name).await;
                    }
                }
                Err(e) => {
                    warn!("{e}");
                    if let Some(u) = self.unsaved.lock().unwrap().get_mut(&name) {
                        u.attempts += 1;
                        u.next_try = Instant::now() + backoff(u.attempts);
                    }
                }
            }
        }
    }

    async fn claim_for_volume(&self, pv: &PersistentVolume) -> Option<PersistentVolumeClaim> {
        let r = pv.spec.claim_ref.as_ref()?;
        let claim: PersistentVolumeClaim = self
            .storage
            .get(&build_key(
                "persistentvolumeclaims",
                r.namespace.as_deref(),
                r.name.as_deref()?,
            ))
            .await
            .ok()?;
        (Some(&claim.metadata.uid) == r.uid.as_ref()).then_some(claim)
    }

    // ---- run loop ------------------------------------------------------------

    fn queue_key(claim: &PersistentVolumeClaim) -> String {
        format!(
            "persistentvolumeclaims/{}/{}",
            claim.metadata.namespace.as_deref().unwrap_or(""),
            claim.metadata.name
        )
    }

    /// `syncClaimHandler` (lib controller.go:1063-1081): a key whose claim is
    /// gone is driven from `claimsInProgress`, else dropped.
    async fn sync_key(&self, key: &str) -> Result<()> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() != 3 {
            return Ok(());
        }
        let storage_key = build_key("persistentvolumeclaims", Some(parts[1]), parts[2]);
        let claim = match self
            .storage
            .get::<PersistentVolumeClaim>(&storage_key)
            .await
        {
            Ok(c) => c,
            Err(_) => {
                let in_progress = self
                    .claims_in_progress
                    .lock()
                    .unwrap()
                    .values()
                    .find(|c| Self::queue_key(c) == key)
                    .cloned();
                match in_progress {
                    Some(c) => c,
                    None => return Ok(()),
                }
            }
        };
        self.sync_claim(&claim)
            .await
            .with_context(|| format!("error syncing claim {key:?}"))
    }

    /// `syncVolumeHandler` (lib controller.go:1083-1096): a volume that is
    /// gone is "already deleted, nothing to do anymore".
    async fn sync_volume_key(&self, key: &str) -> Result<()> {
        let Some(name) = key.strip_prefix("persistentvolumes/") else {
            return Ok(());
        };
        let volume = match self
            .storage
            .get::<PersistentVolume>(&build_key("persistentvolumes", None, name))
            .await
        {
            Ok(v) => v,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        self.sync_volume(&volume)
            .await
            .with_context(|| format!("error syncing volume {key:?}"))
    }

    /// `processNextVolumeWorkItem` (lib controller.go:1020-1060);
    /// `failedDeleteThreshold(0)` retries without limit.
    async fn volume_worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_volume_key(&key).await {
                Err(e) => {
                    error!("{e:#}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
                Ok(()) => queue.forget(&key).await,
            }
            queue.done(&key).await;
        }
    }

    /// The PV informer feeding `volumeQueue`.
    async fn run_volumes(self: Arc<Self>) {
        use futures::StreamExt;

        let queue = WorkQueue::with_config(WorkQueueConfig {
            base_delay: RETRY_INTERVAL_START,
            max_delay: self.retry_interval_max,
        });
        for _ in 0..WORKERS {
            tokio::spawn(Arc::clone(&self).volume_worker(queue.clone()));
        }
        loop {
            self.enqueue_all_volumes(&queue).await;
            let prefix = rusternetes_storage::build_prefix("persistentvolumes", None);
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish PV watch: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            let mut broken = false;
            while !broken {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => queue.add(extract_key(&ev)).await,
                        Some(Err(e)) => { warn!("PV watch error: {e}, reconnecting"); broken = true; }
                        None => { warn!("PV watch stream ended, reconnecting"); broken = true; }
                    },
                    _ = resync.tick() => self.enqueue_all_volumes(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all_volumes(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolume>("/registry/persistentvolumes/")
            .await
        {
            Ok(items) => {
                for v in &items {
                    queue
                        .add(format!("persistentvolumes/{}", v.metadata.name))
                        .await;
                }
            }
            Err(e) => error!("Failed to list persistentvolumes: {e}"),
        }
    }

    async fn worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_key(&key).await {
                // failedProvisionThreshold(0): retry without limit
                // (csi-provisioner.go:402).
                Err(e) => {
                    error!("{e:#}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
                Ok(()) => queue.forget(&key).await,
            }
            queue.done(&key).await;
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting CSI provisioner for driver {}", self.driver_name);
        let queue = WorkQueue::with_config(WorkQueueConfig {
            base_delay: RETRY_INTERVAL_START,
            max_delay: self.retry_interval_max,
        });
        for _ in 0..WORKERS {
            tokio::spawn(Arc::clone(&self).worker(queue.clone()));
        }
        tokio::spawn(Arc::clone(&self).run_volumes());
        let saver = Arc::clone(&self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                saver.save_unsaved_volumes().await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;
            let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", None);
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            let mut broken = false;
            while !broken {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => queue.add(extract_key(&ev)).await,
                        Some(Err(e)) => { warn!("Watch error: {e}, reconnecting"); broken = true; }
                        None => { warn!("Watch stream ended, reconnecting"); broken = true; }
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for c in &items {
                    queue.add(Self::queue_key(c)).await;
                }
            }
            Err(e) => error!("Failed to list persistentvolumeclaims: {e}"),
        }
    }
}

/// `NewItemExponentialFailureRateLimiter(5ms, 1000s)`: `base * 2^(n-1)`.
fn backoff(attempts: u32) -> Duration {
    PV_SAVE_BASE
        .checked_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
        .map_or(PV_SAVE_MAX, |d| d.min(PV_SAVE_MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn pv_save_backoff_doubles_from_5ms_to_1000s() {
        assert_eq!(backoff(1), Duration::from_millis(5));
        assert_eq!(backoff(3), Duration::from_millis(20));
        assert_eq!(backoff(60), PV_SAVE_MAX);
    }
}
