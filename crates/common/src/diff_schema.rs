//! Schema-driven [`GoJson`] rendering of Go's INTERNAL (`pkg/apis/core`)
//! types, for the `diff.Diff(old, new)` texts upstream validation appends to
//! its "may not be changed" errors (validation.go:2283, :5833, :6206).
//!
//! The internal types (`pkg/apis/core/types.go`, release-1.35) carry no json
//! tags, so `json.MarshalIndent` emits Go field names in declaration order
//! with no omitempty; an embedded struct without a tag is inlined; a nil
//! pointer/slice/map is `null` and a struct value is always an object. Each
//! [`Field`] table below is the field list of the named Go struct, paired
//! with the key our serde model serialises it under (the v1 json tag). The
//! few `metav1` types the internal types reference (`ObjectMeta`,
//! `LabelSelector`, ...) DO carry json tags: their tables use the tag name
//! as the output key and [`Field::omit`] for `omitempty`.

use serde_json::Value;

use crate::diff::GoJson;
use crate::quantity::Quantity;

#[derive(Clone, Copy)]
pub enum Kind {
    /// `string` and named string types: `""` when absent.
    Str,
    /// `bool`: `false` when absent.
    Bool,
    /// Integer value type: `0` when absent.
    Int,
    /// `*string` and pointers to named strings (and `*metav1.Time`): `null`
    /// when absent.
    PtrStr,
    /// `*bool`.
    PtrBool,
    /// `*int64` / `*int32`.
    PtrInt,
    /// `[]string` and slices of named strings (nil slice is `null`).
    StrSlice,
    /// `[]int32` / `[]int64`.
    IntSlice,
    /// `map[string]string`: sorted keys, nil is `null`.
    StrMap,
    /// Struct value: an object of its zero value when absent.
    Struct(&'static [Field]),
    /// Pointer to struct: `null` when absent.
    PtrStruct(&'static [Field]),
    /// Slice of structs: `null` when absent.
    Slice(&'static [Field]),
    /// `resource.Quantity` value: canonical string, `"0"` when absent.
    Quantity,
    /// `*resource.Quantity`: canonical string, `null` when absent.
    PtrQuantity,
    /// `ResourceList`: sorted keys, canonical quantity strings, nil is `null`.
    QuantityMap,
    /// `intstr.IntOrString` value: a number or a string, `0` when absent.
    IntOrStr,
    /// `*metav1.FieldsV1`: raw JSON bytes whose key order is the one
    /// `fieldpath.Set.emitContentsV1` wrote (see [`fields_v1_order`]).
    FieldsV1,
    /// Not modelled: rendered from the serde value as is. Documented gap.
    Raw,
}

#[derive(Clone, Copy)]
pub struct Field {
    pub go: &'static str,
    pub key: &'static str,
    pub kind: Kind,
    /// json `omitempty` (only the tagged `metav1` types use it).
    pub omit: bool,
}

const fn f(go: &'static str, key: &'static str, kind: Kind) -> Field {
    Field {
        go,
        key,
        kind,
        omit: false,
    }
}

impl Field {
    const fn omit(mut self) -> Self {
        self.omit = true;
        self
    }
}

/// `metav1.LabelSelector` (apimachinery meta/v1 types.go:1272): json-tagged,
/// so the keys are the tag names and `omitempty` applies.
const LABEL_SELECTOR_REQUIREMENT: &[Field] = &[
    f("key", "key", Kind::Str),
    f("operator", "operator", Kind::Str),
    f("values", "values", Kind::StrSlice).omit(),
];
const LABEL_SELECTOR: &[Field] = &[
    f("matchLabels", "matchLabels", Kind::StrMap).omit(),
    f(
        "matchExpressions",
        "matchExpressions",
        Kind::Slice(LABEL_SELECTOR_REQUIREMENT),
    )
    .omit(),
];
/// `metav1.OwnerReference` (meta/v1 types.go:296-320).
const OWNER_REFERENCE: &[Field] = &[
    f("apiVersion", "apiVersion", Kind::Str),
    f("kind", "kind", Kind::Str),
    f("name", "name", Kind::Str),
    f("uid", "uid", Kind::Str),
    f("controller", "controller", Kind::PtrBool).omit(),
    f("blockOwnerDeletion", "blockOwnerDeletion", Kind::PtrBool).omit(),
];
/// `metav1.ManagedFieldsEntry` (meta/v1 types.go:1340-1377). `FieldsV1` is
/// raw JSON bytes in Go (`FieldsV1.MarshalJSON` returns `Raw`, so
/// `json.MarshalIndent` keeps the stored key order); see [`Kind::FieldsV1`].
pub const MANAGED_FIELDS_ENTRY: &[Field] = &[
    f("manager", "manager", Kind::Str).omit(),
    f("operation", "operation", Kind::Str).omit(),
    f("apiVersion", "apiVersion", Kind::Str).omit(),
    f("time", "time", Kind::PtrStr).omit(),
    f("fieldsType", "fieldsType", Kind::Str).omit(),
    f("fieldsV1", "fieldsV1", Kind::FieldsV1).omit(),
    f("subresource", "subresource", Kind::Str).omit(),
];

const AWS_ELASTIC_BLOCK_STORE_VOLUME_SOURCE: &[Field] = &[
    f("VolumeID", "volumeID", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("Partition", "partition", Kind::Int),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const AFFINITY: &[Field] = &[
    f(
        "NodeAffinity",
        "nodeAffinity",
        Kind::PtrStruct(NODE_AFFINITY),
    ),
    f("PodAffinity", "podAffinity", Kind::PtrStruct(POD_AFFINITY)),
    f(
        "PodAntiAffinity",
        "podAntiAffinity",
        Kind::PtrStruct(POD_ANTI_AFFINITY),
    ),
];

const APP_ARMOR_PROFILE: &[Field] = &[
    f("Type", "type", Kind::Str),
    f("LocalhostProfile", "localhostProfile", Kind::PtrStr),
];

const AZURE_DISK_VOLUME_SOURCE: &[Field] = &[
    f("DiskName", "diskName", Kind::Str),
    f("DataDiskURI", "diskURI", Kind::Str),
    f("CachingMode", "cachingMode", Kind::PtrStr),
    f("FSType", "fsType", Kind::PtrStr),
    f("ReadOnly", "readOnly", Kind::PtrBool),
    f("Kind", "kind", Kind::PtrStr),
];

const AZURE_FILE_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("SecretName", "secretName", Kind::Str),
    f("ShareName", "shareName", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("SecretNamespace", "secretNamespace", Kind::PtrStr),
];

const AZURE_FILE_VOLUME_SOURCE: &[Field] = &[
    f("SecretName", "secretName", Kind::Str),
    f("ShareName", "shareName", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const CSI_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("Driver", "driver", Kind::Str),
    f("VolumeHandle", "volumeHandle", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("FSType", "fsType", Kind::Str),
    f("VolumeAttributes", "volumeAttributes", Kind::StrMap),
    f(
        "ControllerPublishSecretRef",
        "controllerPublishSecretRef",
        Kind::PtrStruct(SECRET_REFERENCE),
    ),
    f(
        "NodeStageSecretRef",
        "nodeStageSecretRef",
        Kind::PtrStruct(SECRET_REFERENCE),
    ),
    f(
        "NodePublishSecretRef",
        "nodePublishSecretRef",
        Kind::PtrStruct(SECRET_REFERENCE),
    ),
    f(
        "ControllerExpandSecretRef",
        "controllerExpandSecretRef",
        Kind::PtrStruct(SECRET_REFERENCE),
    ),
    f(
        "NodeExpandSecretRef",
        "nodeExpandSecretRef",
        Kind::PtrStruct(SECRET_REFERENCE),
    ),
];

const CSI_VOLUME_SOURCE: &[Field] = &[
    f("Driver", "driver", Kind::Str),
    f("ReadOnly", "readOnly", Kind::PtrBool),
    f("FSType", "fsType", Kind::PtrStr),
    f("VolumeAttributes", "volumeAttributes", Kind::StrMap),
    f(
        "NodePublishSecretRef",
        "nodePublishSecretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
];

const CAPABILITIES: &[Field] = &[
    f("Add", "add", Kind::StrSlice),
    f("Drop", "drop", Kind::StrSlice),
];

const CEPH_FS_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("Monitors", "monitors", Kind::StrSlice),
    f("Path", "path", Kind::Str),
    f("User", "user", Kind::Str),
    f("SecretFile", "secretFile", Kind::Str),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const CEPH_FS_VOLUME_SOURCE: &[Field] = &[
    f("Monitors", "monitors", Kind::StrSlice),
    f("Path", "path", Kind::Str),
    f("User", "user", Kind::Str),
    f("SecretFile", "secretFile", Kind::Str),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const CINDER_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("VolumeID", "volumeID", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
];

const CINDER_VOLUME_SOURCE: &[Field] = &[
    f("VolumeID", "volumeID", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
];

const CLUSTER_TRUST_BUNDLE_PROJECTION: &[Field] = &[
    f("Name", "name", Kind::PtrStr),
    f("SignerName", "signerName", Kind::PtrStr),
    f(
        "LabelSelector",
        "labelSelector",
        Kind::PtrStruct(LABEL_SELECTOR),
    ),
    f("Optional", "optional", Kind::PtrBool),
    f("Path", "path", Kind::Str),
];

const CONFIG_MAP_ENV_SOURCE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];

const CONFIG_MAP_KEY_SELECTOR: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Key", "key", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];

const CONFIG_MAP_PROJECTION: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Items", "items", Kind::Slice(KEY_TO_PATH)),
    f("Optional", "optional", Kind::PtrBool),
];

const CONFIG_MAP_VOLUME_SOURCE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Items", "items", Kind::Slice(KEY_TO_PATH)),
    f("DefaultMode", "defaultMode", Kind::PtrInt),
    f("Optional", "optional", Kind::PtrBool),
];

const CONTAINER: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Image", "image", Kind::Str),
    f("Command", "command", Kind::StrSlice),
    f("Args", "args", Kind::StrSlice),
    f("WorkingDir", "workingDir", Kind::Str),
    f("Ports", "ports", Kind::Slice(CONTAINER_PORT)),
    f("EnvFrom", "envFrom", Kind::Slice(ENV_FROM_SOURCE)),
    f("Env", "env", Kind::Slice(ENV_VAR)),
    f(
        "Resources",
        "resources",
        Kind::Struct(RESOURCE_REQUIREMENTS),
    ),
    f(
        "ResizePolicy",
        "resizePolicy",
        Kind::Slice(CONTAINER_RESIZE_POLICY),
    ),
    f("RestartPolicy", "restartPolicy", Kind::PtrStr),
    f(
        "RestartPolicyRules",
        "restartPolicyRules",
        Kind::Slice(CONTAINER_RESTART_RULE),
    ),
    f("VolumeMounts", "volumeMounts", Kind::Slice(VOLUME_MOUNT)),
    f("VolumeDevices", "volumeDevices", Kind::Slice(VOLUME_DEVICE)),
    f("LivenessProbe", "livenessProbe", Kind::PtrStruct(PROBE)),
    f("ReadinessProbe", "readinessProbe", Kind::PtrStruct(PROBE)),
    f("StartupProbe", "startupProbe", Kind::PtrStruct(PROBE)),
    f("Lifecycle", "lifecycle", Kind::PtrStruct(LIFECYCLE)),
    f(
        "TerminationMessagePath",
        "terminationMessagePath",
        Kind::Str,
    ),
    f(
        "TerminationMessagePolicy",
        "terminationMessagePolicy",
        Kind::Str,
    ),
    f("ImagePullPolicy", "imagePullPolicy", Kind::Str),
    f(
        "SecurityContext",
        "securityContext",
        Kind::PtrStruct(SECURITY_CONTEXT),
    ),
    f("Stdin", "stdin", Kind::Bool),
    f("StdinOnce", "stdinOnce", Kind::Bool),
    f("TTY", "tty", Kind::Bool),
];

const CONTAINER_PORT: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("HostPort", "hostPort", Kind::Int),
    f("ContainerPort", "containerPort", Kind::Int),
    f("Protocol", "protocol", Kind::Str),
    f("HostIP", "hostIP", Kind::Str),
];

const CONTAINER_RESIZE_POLICY: &[Field] = &[
    f("ResourceName", "resourceName", Kind::Str),
    f("RestartPolicy", "restartPolicy", Kind::Str),
];

const CONTAINER_RESTART_RULE: &[Field] = &[
    f("Action", "action", Kind::Str),
    f(
        "ExitCodes",
        "exitCodes",
        Kind::PtrStruct(CONTAINER_RESTART_RULE_ON_EXIT_CODES),
    ),
];

const CONTAINER_RESTART_RULE_ON_EXIT_CODES: &[Field] = &[
    f("Operator", "operator", Kind::Str),
    f("Values", "values", Kind::IntSlice),
];

const DOWNWARD_API_PROJECTION: &[Field] =
    &[f("Items", "items", Kind::Slice(DOWNWARD_API_VOLUME_FILE))];

const DOWNWARD_API_VOLUME_FILE: &[Field] = &[
    f("Path", "path", Kind::Str),
    f(
        "FieldRef",
        "fieldRef",
        Kind::PtrStruct(OBJECT_FIELD_SELECTOR),
    ),
    f(
        "ResourceFieldRef",
        "resourceFieldRef",
        Kind::PtrStruct(RESOURCE_FIELD_SELECTOR),
    ),
    f("Mode", "mode", Kind::PtrInt),
];

const DOWNWARD_API_VOLUME_SOURCE: &[Field] = &[
    f("Items", "items", Kind::Slice(DOWNWARD_API_VOLUME_FILE)),
    f("DefaultMode", "defaultMode", Kind::PtrInt),
];

const EMPTY_DIR_VOLUME_SOURCE: &[Field] = &[
    f("Medium", "medium", Kind::Str),
    f("SizeLimit", "sizeLimit", Kind::PtrQuantity),
];

const ENV_FROM_SOURCE: &[Field] = &[
    f("Prefix", "prefix", Kind::Str),
    f(
        "ConfigMapRef",
        "configMapRef",
        Kind::PtrStruct(CONFIG_MAP_ENV_SOURCE),
    ),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_ENV_SOURCE)),
];

const ENV_VAR: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Value", "value", Kind::Str),
    f("ValueFrom", "valueFrom", Kind::PtrStruct(ENV_VAR_SOURCE)),
];

const ENV_VAR_SOURCE: &[Field] = &[
    f(
        "FieldRef",
        "fieldRef",
        Kind::PtrStruct(OBJECT_FIELD_SELECTOR),
    ),
    f(
        "ResourceFieldRef",
        "resourceFieldRef",
        Kind::PtrStruct(RESOURCE_FIELD_SELECTOR),
    ),
    f(
        "ConfigMapKeyRef",
        "configMapKeyRef",
        Kind::PtrStruct(CONFIG_MAP_KEY_SELECTOR),
    ),
    f(
        "SecretKeyRef",
        "secretKeyRef",
        Kind::PtrStruct(SECRET_KEY_SELECTOR),
    ),
    f(
        "FileKeyRef",
        "fileKeyRef",
        Kind::PtrStruct(FILE_KEY_SELECTOR),
    ),
];

/// `core.EphemeralContainer` (pkg/apis/core/types.go): the untagged embedded
/// `EphemeralContainerCommon` is inlined, then `TargetContainerName`.
pub const EPHEMERAL_CONTAINER: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Image", "image", Kind::Str),
    f("Command", "command", Kind::StrSlice),
    f("Args", "args", Kind::StrSlice),
    f("WorkingDir", "workingDir", Kind::Str),
    f("Ports", "ports", Kind::Slice(CONTAINER_PORT)),
    f("EnvFrom", "envFrom", Kind::Slice(ENV_FROM_SOURCE)),
    f("Env", "env", Kind::Slice(ENV_VAR)),
    f(
        "Resources",
        "resources",
        Kind::Struct(RESOURCE_REQUIREMENTS),
    ),
    f(
        "ResizePolicy",
        "resizePolicy",
        Kind::Slice(CONTAINER_RESIZE_POLICY),
    ),
    f("RestartPolicy", "restartPolicy", Kind::PtrStr),
    f(
        "RestartPolicyRules",
        "restartPolicyRules",
        Kind::Slice(CONTAINER_RESTART_RULE),
    ),
    f("VolumeMounts", "volumeMounts", Kind::Slice(VOLUME_MOUNT)),
    f("VolumeDevices", "volumeDevices", Kind::Slice(VOLUME_DEVICE)),
    f("LivenessProbe", "livenessProbe", Kind::PtrStruct(PROBE)),
    f("ReadinessProbe", "readinessProbe", Kind::PtrStruct(PROBE)),
    f("StartupProbe", "startupProbe", Kind::PtrStruct(PROBE)),
    f("Lifecycle", "lifecycle", Kind::PtrStruct(LIFECYCLE)),
    f(
        "TerminationMessagePath",
        "terminationMessagePath",
        Kind::Str,
    ),
    f(
        "TerminationMessagePolicy",
        "terminationMessagePolicy",
        Kind::Str,
    ),
    f("ImagePullPolicy", "imagePullPolicy", Kind::Str),
    f(
        "SecurityContext",
        "securityContext",
        Kind::PtrStruct(SECURITY_CONTEXT),
    ),
    f("Stdin", "stdin", Kind::Bool),
    f("StdinOnce", "stdinOnce", Kind::Bool),
    f("TTY", "tty", Kind::Bool),
    f("TargetContainerName", "targetContainerName", Kind::Str),
];

pub const EPHEMERAL_VOLUME_SOURCE: &[Field] = &[f(
    "VolumeClaimTemplate",
    "volumeClaimTemplate",
    Kind::PtrStruct(PERSISTENT_VOLUME_CLAIM_TEMPLATE),
)];

const EXEC_ACTION: &[Field] = &[f("Command", "command", Kind::StrSlice)];

const FC_VOLUME_SOURCE: &[Field] = &[
    f("TargetWWNs", "targetWWNs", Kind::StrSlice),
    f("Lun", "lun", Kind::PtrInt),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("WWIDs", "wwids", Kind::StrSlice),
];

const FILE_KEY_SELECTOR: &[Field] = &[
    f("VolumeName", "volumeName", Kind::Str),
    f("Path", "path", Kind::Str),
    f("Key", "key", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];

const FLEX_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("Driver", "driver", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("Options", "options", Kind::StrMap),
];

const FLEX_VOLUME_SOURCE: &[Field] = &[
    f("Driver", "driver", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("Options", "options", Kind::StrMap),
];

const FLOCKER_VOLUME_SOURCE: &[Field] = &[
    f("DatasetName", "datasetName", Kind::Str),
    f("DatasetUUID", "datasetUUID", Kind::Str),
];

const GCE_PERSISTENT_DISK_VOLUME_SOURCE: &[Field] = &[
    f("PDName", "pdName", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("Partition", "partition", Kind::Int),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const GRPC_ACTION: &[Field] = &[
    f("Port", "port", Kind::Int),
    f("Service", "service", Kind::PtrStr),
];

const GIT_REPO_VOLUME_SOURCE: &[Field] = &[
    f("Repository", "repository", Kind::Str),
    f("Revision", "revision", Kind::Str),
    f("Directory", "directory", Kind::Str),
];

const GLUSTERFS_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("EndpointsName", "endpoints", Kind::Str),
    f("Path", "path", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("EndpointsNamespace", "endpointsNamespace", Kind::PtrStr),
];

const GLUSTERFS_VOLUME_SOURCE: &[Field] = &[
    f("EndpointsName", "endpoints", Kind::Str),
    f("Path", "path", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const HTTP_GET_ACTION: &[Field] = &[
    f("Path", "path", Kind::Str),
    f("Port", "port", Kind::IntOrStr),
    f("Host", "host", Kind::Str),
    f("Scheme", "scheme", Kind::Str),
    f("HTTPHeaders", "httpHeaders", Kind::Slice(HTTP_HEADER)),
];

const HTTP_HEADER: &[Field] = &[f("Name", "name", Kind::Str), f("Value", "value", Kind::Str)];

const HOST_ALIAS: &[Field] = &[
    f("IP", "ip", Kind::Str),
    f("Hostnames", "hostnames", Kind::StrSlice),
];

const HOST_PATH_VOLUME_SOURCE: &[Field] = &[
    f("Path", "path", Kind::Str),
    f("Type", "type", Kind::PtrStr),
];

const ISCSI_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("TargetPortal", "targetPortal", Kind::Str),
    f("IQN", "iqn", Kind::Str),
    f("Lun", "lun", Kind::Int),
    f("ISCSIInterface", "iscsiInterface", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("Portals", "portals", Kind::StrSlice),
    f("DiscoveryCHAPAuth", "chapAuthDiscovery", Kind::Bool),
    f("SessionCHAPAuth", "chapAuthSession", Kind::Bool),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
    f("InitiatorName", "initiatorName", Kind::PtrStr),
];

const ISCSI_VOLUME_SOURCE: &[Field] = &[
    f("TargetPortal", "targetPortal", Kind::Str),
    f("IQN", "iqn", Kind::Str),
    f("Lun", "lun", Kind::Int),
    f("ISCSIInterface", "iscsiInterface", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("Portals", "portals", Kind::StrSlice),
    f("DiscoveryCHAPAuth", "chapAuthDiscovery", Kind::Bool),
    f("SessionCHAPAuth", "chapAuthSession", Kind::Bool),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
    f("InitiatorName", "initiatorName", Kind::PtrStr),
];

const IMAGE_VOLUME_SOURCE: &[Field] = &[
    f("Reference", "reference", Kind::Str),
    f("PullPolicy", "pullPolicy", Kind::Str),
];

const KEY_TO_PATH: &[Field] = &[
    f("Key", "key", Kind::Str),
    f("Path", "path", Kind::Str),
    f("Mode", "mode", Kind::PtrInt),
];

const LIFECYCLE: &[Field] = &[
    f("PostStart", "postStart", Kind::PtrStruct(LIFECYCLE_HANDLER)),
    f("PreStop", "preStop", Kind::PtrStruct(LIFECYCLE_HANDLER)),
    f("StopSignal", "stopSignal", Kind::PtrStr),
];

const LIFECYCLE_HANDLER: &[Field] = &[
    f("Exec", "exec", Kind::PtrStruct(EXEC_ACTION)),
    f("HTTPGet", "httpGet", Kind::PtrStruct(HTTP_GET_ACTION)),
    f("TCPSocket", "tcpSocket", Kind::PtrStruct(TCP_SOCKET_ACTION)),
    f("Sleep", "sleep", Kind::PtrStruct(SLEEP_ACTION)),
];

const LOCAL_OBJECT_REFERENCE: &[Field] = &[f("Name", "name", Kind::Str)];

const LOCAL_VOLUME_SOURCE: &[Field] = &[
    f("Path", "path", Kind::Str),
    f("FSType", "fsType", Kind::PtrStr),
];

const NFS_VOLUME_SOURCE: &[Field] = &[
    f("Server", "server", Kind::Str),
    f("Path", "path", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const NODE_AFFINITY: &[Field] = &[
    f(
        "RequiredDuringSchedulingIgnoredDuringExecution",
        "requiredDuringSchedulingIgnoredDuringExecution",
        Kind::PtrStruct(NODE_SELECTOR),
    ),
    f(
        "PreferredDuringSchedulingIgnoredDuringExecution",
        "preferredDuringSchedulingIgnoredDuringExecution",
        Kind::Slice(PREFERRED_SCHEDULING_TERM),
    ),
];

const NODE_SELECTOR: &[Field] = &[f(
    "NodeSelectorTerms",
    "nodeSelectorTerms",
    Kind::Slice(NODE_SELECTOR_TERM),
)];

const NODE_SELECTOR_REQUIREMENT: &[Field] = &[
    f("Key", "key", Kind::Str),
    f("Operator", "operator", Kind::Str),
    f("Values", "values", Kind::StrSlice),
];

const NODE_SELECTOR_TERM: &[Field] = &[
    f(
        "MatchExpressions",
        "matchExpressions",
        Kind::Slice(NODE_SELECTOR_REQUIREMENT),
    ),
    f(
        "MatchFields",
        "matchFields",
        Kind::Slice(NODE_SELECTOR_REQUIREMENT),
    ),
];

const OBJECT_FIELD_SELECTOR: &[Field] = &[
    f("APIVersion", "apiVersion", Kind::Str),
    f("FieldPath", "fieldPath", Kind::Str),
];

const OBJECT_REFERENCE: &[Field] = &[
    f("Kind", "kind", Kind::Str),
    f("Namespace", "namespace", Kind::Str),
    f("Name", "name", Kind::Str),
    f("UID", "uid", Kind::Str),
    f("APIVersion", "apiVersion", Kind::Str),
    f("ResourceVersion", "resourceVersion", Kind::Str),
    f("FieldPath", "fieldPath", Kind::Str),
];

const PERSISTENT_VOLUME_CLAIM_SPEC: &[Field] = &[
    f("AccessModes", "accessModes", Kind::StrSlice),
    f("Selector", "selector", Kind::PtrStruct(LABEL_SELECTOR)),
    f(
        "Resources",
        "resources",
        Kind::Struct(VOLUME_RESOURCE_REQUIREMENTS),
    ),
    f("VolumeName", "volumeName", Kind::Str),
    f("StorageClassName", "storageClassName", Kind::PtrStr),
    f("VolumeMode", "volumeMode", Kind::PtrStr),
    f(
        "DataSource",
        "dataSource",
        Kind::PtrStruct(TYPED_LOCAL_OBJECT_REFERENCE),
    ),
    f(
        "DataSourceRef",
        "dataSourceRef",
        Kind::PtrStruct(TYPED_OBJECT_REFERENCE),
    ),
    f(
        "VolumeAttributesClassName",
        "volumeAttributesClassName",
        Kind::PtrStr,
    ),
];

const PERSISTENT_VOLUME_CLAIM_TEMPLATE: &[Field] = &[
    f("name", "name", Kind::Str).omit(),
    f("generateName", "generateName", Kind::Str).omit(),
    f("namespace", "namespace", Kind::Str).omit(),
    f("selfLink", "selfLink", Kind::Str).omit(),
    f("uid", "uid", Kind::Str).omit(),
    f("resourceVersion", "resourceVersion", Kind::Str).omit(),
    f("generation", "generation", Kind::Int).omit(),
    f("creationTimestamp", "creationTimestamp", Kind::PtrStr).omit(),
    f("deletionTimestamp", "deletionTimestamp", Kind::PtrStr).omit(),
    f(
        "deletionGracePeriodSeconds",
        "deletionGracePeriodSeconds",
        Kind::PtrInt,
    )
    .omit(),
    f("labels", "labels", Kind::StrMap).omit(),
    f("annotations", "annotations", Kind::StrMap).omit(),
    f(
        "ownerReferences",
        "ownerReferences",
        Kind::Slice(OWNER_REFERENCE),
    )
    .omit(),
    f("finalizers", "finalizers", Kind::StrSlice).omit(),
    f(
        "managedFields",
        "managedFields",
        Kind::Slice(MANAGED_FIELDS_ENTRY),
    )
    .omit(),
    f("Spec", "spec", Kind::Struct(PERSISTENT_VOLUME_CLAIM_SPEC)),
];

const PERSISTENT_VOLUME_CLAIM_VOLUME_SOURCE: &[Field] = &[
    f("ClaimName", "claimName", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

/// `core.PersistentVolumeSource` (pkg/apis/core/types.go:232), the union
/// embedded in `PersistentVolumeSpec`.
pub const PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f(
        "GCEPersistentDisk",
        "gcePersistentDisk",
        Kind::PtrStruct(GCE_PERSISTENT_DISK_VOLUME_SOURCE),
    ),
    f(
        "AWSElasticBlockStore",
        "awsElasticBlockStore",
        Kind::PtrStruct(AWS_ELASTIC_BLOCK_STORE_VOLUME_SOURCE),
    ),
    f(
        "HostPath",
        "hostPath",
        Kind::PtrStruct(HOST_PATH_VOLUME_SOURCE),
    ),
    f(
        "Glusterfs",
        "glusterfs",
        Kind::PtrStruct(GLUSTERFS_PERSISTENT_VOLUME_SOURCE),
    ),
    f("NFS", "nfs", Kind::PtrStruct(NFS_VOLUME_SOURCE)),
    f("RBD", "rbd", Kind::PtrStruct(RBD_PERSISTENT_VOLUME_SOURCE)),
    f("Quobyte", "quobyte", Kind::PtrStruct(QUOBYTE_VOLUME_SOURCE)),
    f(
        "ISCSI",
        "iscsi",
        Kind::PtrStruct(ISCSI_PERSISTENT_VOLUME_SOURCE),
    ),
    f(
        "FlexVolume",
        "flexVolume",
        Kind::PtrStruct(FLEX_PERSISTENT_VOLUME_SOURCE),
    ),
    f(
        "Cinder",
        "cinder",
        Kind::PtrStruct(CINDER_PERSISTENT_VOLUME_SOURCE),
    ),
    f(
        "CephFS",
        "cephfs",
        Kind::PtrStruct(CEPH_FS_PERSISTENT_VOLUME_SOURCE),
    ),
    f("FC", "fc", Kind::PtrStruct(FC_VOLUME_SOURCE)),
    f("Flocker", "flocker", Kind::PtrStruct(FLOCKER_VOLUME_SOURCE)),
    f(
        "AzureFile",
        "azureFile",
        Kind::PtrStruct(AZURE_FILE_PERSISTENT_VOLUME_SOURCE),
    ),
    f(
        "VsphereVolume",
        "vsphereVolume",
        Kind::PtrStruct(VSPHERE_VIRTUAL_DISK_VOLUME_SOURCE),
    ),
    f(
        "AzureDisk",
        "azureDisk",
        Kind::PtrStruct(AZURE_DISK_VOLUME_SOURCE),
    ),
    f(
        "PhotonPersistentDisk",
        "photonPersistentDisk",
        Kind::PtrStruct(PHOTON_PERSISTENT_DISK_VOLUME_SOURCE),
    ),
    f(
        "PortworxVolume",
        "portworxVolume",
        Kind::PtrStruct(PORTWORX_VOLUME_SOURCE),
    ),
    f(
        "ScaleIO",
        "scaleIO",
        Kind::PtrStruct(SCALE_IO_PERSISTENT_VOLUME_SOURCE),
    ),
    f("Local", "local", Kind::PtrStruct(LOCAL_VOLUME_SOURCE)),
    f(
        "StorageOS",
        "storageos",
        Kind::PtrStruct(STORAGE_OS_PERSISTENT_VOLUME_SOURCE),
    ),
    f("CSI", "csi", Kind::PtrStruct(CSI_PERSISTENT_VOLUME_SOURCE)),
];

const PHOTON_PERSISTENT_DISK_VOLUME_SOURCE: &[Field] = &[
    f("PdID", "pdID", Kind::Str),
    f("FSType", "fsType", Kind::Str),
];

const POD_AFFINITY: &[Field] = &[
    f(
        "RequiredDuringSchedulingIgnoredDuringExecution",
        "requiredDuringSchedulingIgnoredDuringExecution",
        Kind::Slice(POD_AFFINITY_TERM),
    ),
    f(
        "PreferredDuringSchedulingIgnoredDuringExecution",
        "preferredDuringSchedulingIgnoredDuringExecution",
        Kind::Slice(WEIGHTED_POD_AFFINITY_TERM),
    ),
];

const POD_AFFINITY_TERM: &[Field] = &[
    f(
        "LabelSelector",
        "labelSelector",
        Kind::PtrStruct(LABEL_SELECTOR),
    ),
    f("Namespaces", "namespaces", Kind::StrSlice),
    f("TopologyKey", "topologyKey", Kind::Str),
    f(
        "NamespaceSelector",
        "namespaceSelector",
        Kind::PtrStruct(LABEL_SELECTOR),
    ),
    f("MatchLabelKeys", "matchLabelKeys", Kind::StrSlice),
    f("MismatchLabelKeys", "mismatchLabelKeys", Kind::StrSlice),
];

const POD_ANTI_AFFINITY: &[Field] = &[
    f(
        "RequiredDuringSchedulingIgnoredDuringExecution",
        "requiredDuringSchedulingIgnoredDuringExecution",
        Kind::Slice(POD_AFFINITY_TERM),
    ),
    f(
        "PreferredDuringSchedulingIgnoredDuringExecution",
        "preferredDuringSchedulingIgnoredDuringExecution",
        Kind::Slice(WEIGHTED_POD_AFFINITY_TERM),
    ),
];

const POD_CERTIFICATE_PROJECTION: &[Field] = &[
    f("SignerName", "signerName", Kind::Str),
    f("KeyType", "keyType", Kind::Str),
    f("MaxExpirationSeconds", "maxExpirationSeconds", Kind::PtrInt),
    f("CredentialBundlePath", "credentialBundlePath", Kind::Str),
    f("KeyPath", "keyPath", Kind::Str),
    f("CertificateChainPath", "certificateChainPath", Kind::Str),
    f("UserAnnotations", "userAnnotations", Kind::StrMap),
];

const POD_DNS_CONFIG: &[Field] = &[
    f("Nameservers", "nameservers", Kind::StrSlice),
    f("Searches", "searches", Kind::StrSlice),
    f("Options", "options", Kind::Slice(POD_DNS_CONFIG_OPTION)),
];

const POD_DNS_CONFIG_OPTION: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Value", "value", Kind::PtrStr),
];

const POD_OS: &[Field] = &[f("Name", "name", Kind::Str)];

const POD_READINESS_GATE: &[Field] = &[f("ConditionType", "conditionType", Kind::Str)];

const POD_RESOURCE_CLAIM: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("ResourceClaimName", "resourceClaimName", Kind::PtrStr),
    f(
        "ResourceClaimTemplateName",
        "resourceClaimTemplateName",
        Kind::PtrStr,
    ),
];

const POD_SCHEDULING_GATE: &[Field] = &[f("Name", "name", Kind::Str)];

const POD_SECURITY_CONTEXT: &[Field] = &[
    f("HostNetwork", "hostNetwork", Kind::Bool),
    f("HostPID", "hostPID", Kind::Bool),
    f("HostIPC", "hostIPC", Kind::Bool),
    f(
        "ShareProcessNamespace",
        "shareProcessNamespace",
        Kind::PtrBool,
    ),
    f("HostUsers", "hostUsers", Kind::PtrBool),
    f(
        "SELinuxOptions",
        "seLinuxOptions",
        Kind::PtrStruct(SE_LINUX_OPTIONS),
    ),
    f(
        "WindowsOptions",
        "windowsOptions",
        Kind::PtrStruct(WINDOWS_SECURITY_CONTEXT_OPTIONS),
    ),
    f("RunAsUser", "runAsUser", Kind::PtrInt),
    f("RunAsGroup", "runAsGroup", Kind::PtrInt),
    f("RunAsNonRoot", "runAsNonRoot", Kind::PtrBool),
    f("SupplementalGroups", "supplementalGroups", Kind::IntSlice),
    f(
        "SupplementalGroupsPolicy",
        "supplementalGroupsPolicy",
        Kind::PtrStr,
    ),
    f("FSGroup", "fsGroup", Kind::PtrInt),
    f("FSGroupChangePolicy", "fsGroupChangePolicy", Kind::PtrStr),
    f("Sysctls", "sysctls", Kind::Slice(SYSCTL)),
    f(
        "SeccompProfile",
        "seccompProfile",
        Kind::PtrStruct(SECCOMP_PROFILE),
    ),
    f(
        "AppArmorProfile",
        "appArmorProfile",
        Kind::PtrStruct(APP_ARMOR_PROFILE),
    ),
    f("SELinuxChangePolicy", "seLinuxChangePolicy", Kind::PtrStr),
];

/// `core.PodSpec` (pkg/apis/core/types.go:3672). Render it from
/// [`pod_spec_diff`], which applies the v1->internal conversion first.
pub const POD_SPEC: &[Field] = &[
    f("Volumes", "volumes", Kind::Slice(VOLUME)),
    f("InitContainers", "initContainers", Kind::Slice(CONTAINER)),
    f("Containers", "containers", Kind::Slice(CONTAINER)),
    f(
        "EphemeralContainers",
        "ephemeralContainers",
        Kind::Slice(EPHEMERAL_CONTAINER),
    ),
    f("RestartPolicy", "restartPolicy", Kind::Str),
    f(
        "TerminationGracePeriodSeconds",
        "terminationGracePeriodSeconds",
        Kind::PtrInt,
    ),
    f(
        "ActiveDeadlineSeconds",
        "activeDeadlineSeconds",
        Kind::PtrInt,
    ),
    f("DNSPolicy", "dnsPolicy", Kind::Str),
    f("NodeSelector", "nodeSelector", Kind::StrMap),
    f("ServiceAccountName", "serviceAccountName", Kind::Str),
    f(
        "AutomountServiceAccountToken",
        "automountServiceAccountToken",
        Kind::PtrBool,
    ),
    f("NodeName", "nodeName", Kind::Str),
    f(
        "SecurityContext",
        "securityContext",
        Kind::Struct(POD_SECURITY_CONTEXT),
    ),
    f(
        "ImagePullSecrets",
        "imagePullSecrets",
        Kind::Slice(LOCAL_OBJECT_REFERENCE),
    ),
    f("Hostname", "hostname", Kind::Str),
    f("Subdomain", "subdomain", Kind::Str),
    f("SetHostnameAsFQDN", "setHostnameAsFQDN", Kind::PtrBool),
    f("Affinity", "affinity", Kind::PtrStruct(AFFINITY)),
    f("SchedulerName", "schedulerName", Kind::Str),
    f("Tolerations", "tolerations", Kind::Slice(TOLERATION)),
    f("HostAliases", "hostAliases", Kind::Slice(HOST_ALIAS)),
    f("PriorityClassName", "priorityClassName", Kind::Str),
    f("Priority", "priority", Kind::PtrInt),
    f("PreemptionPolicy", "preemptionPolicy", Kind::PtrStr),
    f("DNSConfig", "dnsConfig", Kind::PtrStruct(POD_DNS_CONFIG)),
    f(
        "ReadinessGates",
        "readinessGates",
        Kind::Slice(POD_READINESS_GATE),
    ),
    f("RuntimeClassName", "runtimeClassName", Kind::PtrStr),
    f("Overhead", "overhead", Kind::QuantityMap),
    f("EnableServiceLinks", "enableServiceLinks", Kind::PtrBool),
    f(
        "TopologySpreadConstraints",
        "topologySpreadConstraints",
        Kind::Slice(TOPOLOGY_SPREAD_CONSTRAINT),
    ),
    f("OS", "os", Kind::PtrStruct(POD_OS)),
    f(
        "SchedulingGates",
        "schedulingGates",
        Kind::Slice(POD_SCHEDULING_GATE),
    ),
    f(
        "ResourceClaims",
        "resourceClaims",
        Kind::Slice(POD_RESOURCE_CLAIM),
    ),
    f(
        "Resources",
        "resources",
        Kind::PtrStruct(RESOURCE_REQUIREMENTS),
    ),
    f("HostnameOverride", "hostnameOverride", Kind::PtrStr),
    f(
        "WorkloadRef",
        "workloadRef",
        Kind::PtrStruct(WORKLOAD_REFERENCE),
    ),
];

const PORTWORX_VOLUME_SOURCE: &[Field] = &[
    f("VolumeID", "volumeID", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const PREFERRED_SCHEDULING_TERM: &[Field] = &[
    f("Weight", "weight", Kind::Int),
    f("Preference", "preference", Kind::Struct(NODE_SELECTOR_TERM)),
];

const PROBE: &[Field] = &[
    f("Exec", "exec", Kind::PtrStruct(EXEC_ACTION)),
    f("HTTPGet", "httpGet", Kind::PtrStruct(HTTP_GET_ACTION)),
    f("TCPSocket", "tcpSocket", Kind::PtrStruct(TCP_SOCKET_ACTION)),
    f("GRPC", "grpc", Kind::PtrStruct(GRPC_ACTION)),
    f("InitialDelaySeconds", "initialDelaySeconds", Kind::Int),
    f("TimeoutSeconds", "timeoutSeconds", Kind::Int),
    f("PeriodSeconds", "periodSeconds", Kind::Int),
    f("SuccessThreshold", "successThreshold", Kind::Int),
    f("FailureThreshold", "failureThreshold", Kind::Int),
    f(
        "TerminationGracePeriodSeconds",
        "terminationGracePeriodSeconds",
        Kind::PtrInt,
    ),
];

const PROJECTED_VOLUME_SOURCE: &[Field] = &[
    f("Sources", "sources", Kind::Slice(VOLUME_PROJECTION)),
    f("DefaultMode", "defaultMode", Kind::PtrInt),
];

const QUOBYTE_VOLUME_SOURCE: &[Field] = &[
    f("Registry", "registry", Kind::Str),
    f("Volume", "volume", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("User", "user", Kind::Str),
    f("Group", "group", Kind::Str),
    f("Tenant", "tenant", Kind::Str),
];

const RBD_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("CephMonitors", "monitors", Kind::StrSlice),
    f("RBDImage", "image", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("RBDPool", "pool", Kind::Str),
    f("RadosUser", "user", Kind::Str),
    f("Keyring", "keyring", Kind::Str),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const RBD_VOLUME_SOURCE: &[Field] = &[
    f("CephMonitors", "monitors", Kind::StrSlice),
    f("RBDImage", "image", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("RBDPool", "pool", Kind::Str),
    f("RadosUser", "user", Kind::Str),
    f("Keyring", "keyring", Kind::Str),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const RESOURCE_CLAIM: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Request", "request", Kind::Str),
];

const RESOURCE_FIELD_SELECTOR: &[Field] = &[
    f("ContainerName", "containerName", Kind::Str),
    f("Resource", "resource", Kind::Str),
    f("Divisor", "divisor", Kind::Quantity),
];

const RESOURCE_REQUIREMENTS: &[Field] = &[
    f("Limits", "limits", Kind::QuantityMap),
    f("Requests", "requests", Kind::QuantityMap),
    f("Claims", "claims", Kind::Slice(RESOURCE_CLAIM)),
];

const SE_LINUX_OPTIONS: &[Field] = &[
    f("User", "user", Kind::Str),
    f("Role", "role", Kind::Str),
    f("Type", "type", Kind::Str),
    f("Level", "level", Kind::Str),
];

const SCALE_IO_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("Gateway", "gateway", Kind::Str),
    f("System", "system", Kind::Str),
    f("SecretRef", "secretRef", Kind::PtrStruct(SECRET_REFERENCE)),
    f("SSLEnabled", "sslEnabled", Kind::Bool),
    f("ProtectionDomain", "protectionDomain", Kind::Str),
    f("StoragePool", "storagePool", Kind::Str),
    f("StorageMode", "storageMode", Kind::Str),
    f("VolumeName", "volumeName", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const SCALE_IO_VOLUME_SOURCE: &[Field] = &[
    f("Gateway", "gateway", Kind::Str),
    f("System", "system", Kind::Str),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
    f("SSLEnabled", "sslEnabled", Kind::Bool),
    f("ProtectionDomain", "protectionDomain", Kind::Str),
    f("StoragePool", "storagePool", Kind::Str),
    f("StorageMode", "storageMode", Kind::Str),
    f("VolumeName", "volumeName", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
];

const SECCOMP_PROFILE: &[Field] = &[
    f("Type", "type", Kind::Str),
    f("LocalhostProfile", "localhostProfile", Kind::PtrStr),
];

const SECRET_ENV_SOURCE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];

const SECRET_KEY_SELECTOR: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Key", "key", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];

const SECRET_PROJECTION: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Items", "items", Kind::Slice(KEY_TO_PATH)),
    f("Optional", "optional", Kind::PtrBool),
];

const SECRET_REFERENCE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Namespace", "namespace", Kind::Str),
];

const SECRET_VOLUME_SOURCE: &[Field] = &[
    f("SecretName", "secretName", Kind::Str),
    f("Items", "items", Kind::Slice(KEY_TO_PATH)),
    f("DefaultMode", "defaultMode", Kind::PtrInt),
    f("Optional", "optional", Kind::PtrBool),
];

const SECURITY_CONTEXT: &[Field] = &[
    f(
        "Capabilities",
        "capabilities",
        Kind::PtrStruct(CAPABILITIES),
    ),
    f("Privileged", "privileged", Kind::PtrBool),
    f(
        "SELinuxOptions",
        "seLinuxOptions",
        Kind::PtrStruct(SE_LINUX_OPTIONS),
    ),
    f(
        "WindowsOptions",
        "windowsOptions",
        Kind::PtrStruct(WINDOWS_SECURITY_CONTEXT_OPTIONS),
    ),
    f("RunAsUser", "runAsUser", Kind::PtrInt),
    f("RunAsGroup", "runAsGroup", Kind::PtrInt),
    f("RunAsNonRoot", "runAsNonRoot", Kind::PtrBool),
    f(
        "ReadOnlyRootFilesystem",
        "readOnlyRootFilesystem",
        Kind::PtrBool,
    ),
    f(
        "AllowPrivilegeEscalation",
        "allowPrivilegeEscalation",
        Kind::PtrBool,
    ),
    f("ProcMount", "procMount", Kind::PtrStr),
    f(
        "SeccompProfile",
        "seccompProfile",
        Kind::PtrStruct(SECCOMP_PROFILE),
    ),
    f(
        "AppArmorProfile",
        "appArmorProfile",
        Kind::PtrStruct(APP_ARMOR_PROFILE),
    ),
];

const SERVICE_ACCOUNT_TOKEN_PROJECTION: &[Field] = &[
    f("Audience", "audience", Kind::Str),
    f("ExpirationSeconds", "expirationSeconds", Kind::Int),
    f("Path", "path", Kind::Str),
];

const SLEEP_ACTION: &[Field] = &[f("Seconds", "seconds", Kind::Int)];

const STORAGE_OS_PERSISTENT_VOLUME_SOURCE: &[Field] = &[
    f("VolumeName", "volumeName", Kind::Str),
    f("VolumeNamespace", "volumeNamespace", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("SecretRef", "secretRef", Kind::PtrStruct(OBJECT_REFERENCE)),
];

const STORAGE_OS_VOLUME_SOURCE: &[Field] = &[
    f("VolumeName", "volumeName", Kind::Str),
    f("VolumeNamespace", "volumeNamespace", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f(
        "SecretRef",
        "secretRef",
        Kind::PtrStruct(LOCAL_OBJECT_REFERENCE),
    ),
];

const SYSCTL: &[Field] = &[f("Name", "name", Kind::Str), f("Value", "value", Kind::Str)];

const TCP_SOCKET_ACTION: &[Field] = &[
    f("Port", "port", Kind::IntOrStr),
    f("Host", "host", Kind::Str),
];

const TOLERATION: &[Field] = &[
    f("Key", "key", Kind::Str),
    f("Operator", "operator", Kind::Str),
    f("Value", "value", Kind::Str),
    f("Effect", "effect", Kind::Str),
    f("TolerationSeconds", "tolerationSeconds", Kind::PtrInt),
];

const TOPOLOGY_SPREAD_CONSTRAINT: &[Field] = &[
    f("MaxSkew", "maxSkew", Kind::Int),
    f("TopologyKey", "topologyKey", Kind::Str),
    f("WhenUnsatisfiable", "whenUnsatisfiable", Kind::Str),
    f(
        "LabelSelector",
        "labelSelector",
        Kind::PtrStruct(LABEL_SELECTOR),
    ),
    f("MinDomains", "minDomains", Kind::PtrInt),
    f("NodeAffinityPolicy", "nodeAffinityPolicy", Kind::PtrStr),
    f("NodeTaintsPolicy", "nodeTaintsPolicy", Kind::PtrStr),
    f("MatchLabelKeys", "matchLabelKeys", Kind::StrSlice),
];

const TYPED_LOCAL_OBJECT_REFERENCE: &[Field] = &[
    f("APIGroup", "apiGroup", Kind::PtrStr),
    f("Kind", "kind", Kind::Str),
    f("Name", "name", Kind::Str),
];

const TYPED_OBJECT_REFERENCE: &[Field] = &[
    f("APIGroup", "apiGroup", Kind::PtrStr),
    f("Kind", "kind", Kind::Str),
    f("Name", "name", Kind::Str),
    f("Namespace", "namespace", Kind::PtrStr),
];

const VOLUME: &[Field] = &[
    f("Name", "name", Kind::Str),
    f(
        "HostPath",
        "hostPath",
        Kind::PtrStruct(HOST_PATH_VOLUME_SOURCE),
    ),
    f(
        "EmptyDir",
        "emptyDir",
        Kind::PtrStruct(EMPTY_DIR_VOLUME_SOURCE),
    ),
    f(
        "GCEPersistentDisk",
        "gcePersistentDisk",
        Kind::PtrStruct(GCE_PERSISTENT_DISK_VOLUME_SOURCE),
    ),
    f(
        "AWSElasticBlockStore",
        "awsElasticBlockStore",
        Kind::PtrStruct(AWS_ELASTIC_BLOCK_STORE_VOLUME_SOURCE),
    ),
    f(
        "GitRepo",
        "gitRepo",
        Kind::PtrStruct(GIT_REPO_VOLUME_SOURCE),
    ),
    f("Secret", "secret", Kind::PtrStruct(SECRET_VOLUME_SOURCE)),
    f("NFS", "nfs", Kind::PtrStruct(NFS_VOLUME_SOURCE)),
    f("ISCSI", "iscsi", Kind::PtrStruct(ISCSI_VOLUME_SOURCE)),
    f(
        "Glusterfs",
        "glusterfs",
        Kind::PtrStruct(GLUSTERFS_VOLUME_SOURCE),
    ),
    f(
        "PersistentVolumeClaim",
        "persistentVolumeClaim",
        Kind::PtrStruct(PERSISTENT_VOLUME_CLAIM_VOLUME_SOURCE),
    ),
    f("RBD", "rbd", Kind::PtrStruct(RBD_VOLUME_SOURCE)),
    f("Quobyte", "quobyte", Kind::PtrStruct(QUOBYTE_VOLUME_SOURCE)),
    f(
        "FlexVolume",
        "flexVolume",
        Kind::PtrStruct(FLEX_VOLUME_SOURCE),
    ),
    f("Cinder", "cinder", Kind::PtrStruct(CINDER_VOLUME_SOURCE)),
    f("CephFS", "cephfs", Kind::PtrStruct(CEPH_FS_VOLUME_SOURCE)),
    f("Flocker", "flocker", Kind::PtrStruct(FLOCKER_VOLUME_SOURCE)),
    f(
        "DownwardAPI",
        "downwardAPI",
        Kind::PtrStruct(DOWNWARD_API_VOLUME_SOURCE),
    ),
    f("FC", "fc", Kind::PtrStruct(FC_VOLUME_SOURCE)),
    f(
        "AzureFile",
        "azureFile",
        Kind::PtrStruct(AZURE_FILE_VOLUME_SOURCE),
    ),
    f(
        "ConfigMap",
        "configMap",
        Kind::PtrStruct(CONFIG_MAP_VOLUME_SOURCE),
    ),
    f(
        "VsphereVolume",
        "vsphereVolume",
        Kind::PtrStruct(VSPHERE_VIRTUAL_DISK_VOLUME_SOURCE),
    ),
    f(
        "AzureDisk",
        "azureDisk",
        Kind::PtrStruct(AZURE_DISK_VOLUME_SOURCE),
    ),
    f(
        "PhotonPersistentDisk",
        "photonPersistentDisk",
        Kind::PtrStruct(PHOTON_PERSISTENT_DISK_VOLUME_SOURCE),
    ),
    f(
        "Projected",
        "projected",
        Kind::PtrStruct(PROJECTED_VOLUME_SOURCE),
    ),
    f(
        "PortworxVolume",
        "portworxVolume",
        Kind::PtrStruct(PORTWORX_VOLUME_SOURCE),
    ),
    f(
        "ScaleIO",
        "scaleIO",
        Kind::PtrStruct(SCALE_IO_VOLUME_SOURCE),
    ),
    f(
        "StorageOS",
        "storageos",
        Kind::PtrStruct(STORAGE_OS_VOLUME_SOURCE),
    ),
    f("CSI", "csi", Kind::PtrStruct(CSI_VOLUME_SOURCE)),
    f(
        "Ephemeral",
        "ephemeral",
        Kind::PtrStruct(EPHEMERAL_VOLUME_SOURCE),
    ),
    f("Image", "image", Kind::PtrStruct(IMAGE_VOLUME_SOURCE)),
];

const VOLUME_DEVICE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("DevicePath", "devicePath", Kind::Str),
];

const VOLUME_MOUNT: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("RecursiveReadOnly", "recursiveReadOnly", Kind::PtrStr),
    f("MountPath", "mountPath", Kind::Str),
    f("SubPath", "subPath", Kind::Str),
    f("MountPropagation", "mountPropagation", Kind::PtrStr),
    f("SubPathExpr", "subPathExpr", Kind::Str),
];

const VOLUME_PROJECTION: &[Field] = &[
    f("Secret", "secret", Kind::PtrStruct(SECRET_PROJECTION)),
    f(
        "DownwardAPI",
        "downwardAPI",
        Kind::PtrStruct(DOWNWARD_API_PROJECTION),
    ),
    f(
        "ConfigMap",
        "configMap",
        Kind::PtrStruct(CONFIG_MAP_PROJECTION),
    ),
    f(
        "ServiceAccountToken",
        "serviceAccountToken",
        Kind::PtrStruct(SERVICE_ACCOUNT_TOKEN_PROJECTION),
    ),
    f(
        "ClusterTrustBundle",
        "clusterTrustBundle",
        Kind::PtrStruct(CLUSTER_TRUST_BUNDLE_PROJECTION),
    ),
    f(
        "PodCertificate",
        "podCertificate",
        Kind::PtrStruct(POD_CERTIFICATE_PROJECTION),
    ),
];

const VOLUME_RESOURCE_REQUIREMENTS: &[Field] = &[
    f("Limits", "limits", Kind::QuantityMap),
    f("Requests", "requests", Kind::QuantityMap),
];

const VSPHERE_VIRTUAL_DISK_VOLUME_SOURCE: &[Field] = &[
    f("VolumePath", "volumePath", Kind::Str),
    f("FSType", "fsType", Kind::Str),
    f("StoragePolicyName", "storagePolicyName", Kind::Str),
    f("StoragePolicyID", "storagePolicyID", Kind::Str),
];

const WEIGHTED_POD_AFFINITY_TERM: &[Field] = &[
    f("Weight", "weight", Kind::Int),
    f(
        "PodAffinityTerm",
        "podAffinityTerm",
        Kind::Struct(POD_AFFINITY_TERM),
    ),
];

const WINDOWS_SECURITY_CONTEXT_OPTIONS: &[Field] = &[
    f(
        "GMSACredentialSpecName",
        "gmsaCredentialSpecName",
        Kind::PtrStr,
    ),
    f("GMSACredentialSpec", "gmsaCredentialSpec", Kind::PtrStr),
    f("RunAsUserName", "runAsUserName", Kind::PtrStr),
    f("HostProcess", "hostProcess", Kind::PtrBool),
];

const WORKLOAD_REFERENCE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("PodGroup", "podGroup", Kind::Str),
    f("PodGroupReplicaKey", "podGroupReplicaKey", Kind::Str),
];

/// `omitempty` (encoding/json `isEmptyValue`): false, 0, nil pointer/slice/map,
/// empty string/array/map. Struct values are never empty.
fn is_empty_value(kind: Kind, j: &GoJson) -> bool {
    match j {
        GoJson::Null => true,
        GoJson::Bool(b) => !*b,
        GoJson::Number(n) => n == "0",
        GoJson::String(s) => s.is_empty() && !matches!(kind, Kind::Quantity | Kind::PtrQuantity),
        GoJson::Array(a) => a.is_empty(),
        // A non-nil `*FieldsV1` is never empty (`omitempty` tests the pointer).
        GoJson::Object(o) => {
            o.is_empty() && !matches!(kind, Kind::Struct(_) | Kind::PtrStruct(_) | Kind::FieldsV1)
        }
    }
}

pub fn render_struct(fields: &[Field], v: Option<&Value>) -> GoJson {
    GoJson::Object(
        fields
            .iter()
            .filter_map(|fl| {
                let j = render(fl.kind, v.and_then(|o| o.get(fl.key)));
                (!(fl.omit && is_empty_value(fl.kind, &j))).then(|| (fl.go.to_string(), j))
            })
            .collect(),
    )
}

fn canonical_quantity(raw: &str) -> String {
    Quantity::parse(raw)
        .map(|q| q.canonical_string())
        .unwrap_or_else(|_| raw.to_string())
}

/// Render `v` as Go's `json.MarshalIndent` would emit a value of `kind`.
pub fn render(kind: Kind, v: Option<&Value>) -> GoJson {
    let v = v.filter(|v| !v.is_null());
    match kind {
        Kind::Str => GoJson::str(v.and_then(Value::as_str).unwrap_or_default()),
        Kind::Bool => GoJson::Bool(v.and_then(Value::as_bool).unwrap_or(false)),
        Kind::Int => GoJson::Number(v.map_or("0".to_string(), Value::to_string)),
        Kind::PtrStr => v.and_then(Value::as_str).map_or(GoJson::Null, GoJson::str),
        Kind::PtrBool => v
            .and_then(Value::as_bool)
            .map_or(GoJson::Null, GoJson::Bool),
        Kind::PtrInt => v.map_or(GoJson::Null, |n| GoJson::Number(n.to_string())),
        Kind::StrSlice => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(
                a.iter()
                    .map(|s| GoJson::str(s.as_str().unwrap_or_default()))
                    .collect(),
            )
        }),
        Kind::IntSlice => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(a.iter().map(|n| GoJson::Number(n.to_string())).collect())
        }),
        Kind::StrMap => v.and_then(Value::as_object).map_or(GoJson::Null, |m| {
            let mut e: Vec<(String, GoJson)> = m
                .iter()
                .map(|(k, s)| (k.clone(), GoJson::str(s.as_str().unwrap_or_default())))
                .collect();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            GoJson::Object(e)
        }),
        Kind::Struct(fields) => render_struct(fields, v),
        Kind::PtrStruct(fields) => v.map_or(GoJson::Null, |v| render_struct(fields, Some(v))),
        Kind::Slice(fields) => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(a.iter().map(|e| render_struct(fields, Some(e))).collect())
        }),
        Kind::Quantity => GoJson::str(
            v.and_then(Value::as_str)
                .map_or_else(|| "0".to_string(), canonical_quantity),
        ),
        Kind::PtrQuantity => v
            .and_then(Value::as_str)
            .map_or(GoJson::Null, |raw| GoJson::str(canonical_quantity(raw))),
        Kind::QuantityMap => v.and_then(Value::as_object).map_or(GoJson::Null, |m| {
            let mut e: Vec<(String, GoJson)> = m
                .iter()
                .map(|(k, raw)| (k.clone(), render(Kind::Quantity, Some(raw))))
                .collect();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            GoJson::Object(e)
        }),
        // intstr.IntOrString.MarshalJSON: the int or the string; zero value
        // is the int 0.
        Kind::IntOrStr => v.map_or(GoJson::Number("0".to_string()), raw_json),
        Kind::FieldsV1 => v.map_or(GoJson::Null, fields_v1_json),
        Kind::Raw => v.map_or(GoJson::Null, raw_json),
    }
}

/// A `FieldsV1` object key, deserialised per
/// `fieldpath.DeserializePathElement` (serialize-pe.go:76-128).
enum PathElem {
    /// `"."`: the set's own membership, always written first
    /// (serialize.go:90-94).
    Dot,
    Field(String),
    Key(Vec<(String, Value)>),
    Value(Value),
    Index(i64),
    Unknown(String),
}

fn parse_path_elem(k: &str) -> PathElem {
    if k == "." {
        return PathElem::Dot;
    }
    let (Some(rest), true) = (k.get(2..), k.as_bytes().get(1) == Some(&b':')) else {
        return PathElem::Unknown(k.to_string());
    };
    let unknown = || PathElem::Unknown(k.to_string());
    match k.as_bytes()[0] {
        b'f' => PathElem::Field(rest.to_string()),
        b'v' => serde_json::from_str(rest).map_or_else(|_| unknown(), PathElem::Value),
        // serde_json::Map is sorted by name == `fields.Sort()`.
        b'k' => match serde_json::from_str::<Value>(rest) {
            Ok(Value::Object(m)) => PathElem::Key(m.into_iter().collect()),
            _ => unknown(),
        },
        b'i' => rest.parse().map_or_else(|_| unknown(), PathElem::Index),
        _ => unknown(),
    }
}

/// `value.Compare` (structured-merge-diff value/value.go:264-345): a total
/// order across types, numbers < strings < bools < lists < maps < null;
/// ints and floats compare numerically.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Number(_) => 0,
            Value::String(_) => 1,
            Value::Bool(_) => 2,
            Value::Array(_) => 3,
            Value::Object(_) => 4,
            Value::Null => 5,
        }
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => x
                .as_f64()
                .partial_cmp(&y.as_f64())
                .unwrap_or(Ordering::Equal),
        },
        (Value::String(x), Value::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                let c = compare_values(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((kp, p), (kq, q)) in x.iter().zip(y) {
                let c = kp
                    .as_bytes()
                    .cmp(kq.as_bytes())
                    .then_with(|| compare_values(p, q));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

/// `PathElement.Compare` (fieldpath/element.go:87-127) over [`PathElem`]:
/// field names < keys < values < indexes. `FieldList.Compare`
/// (value/fields.go:65-89) orders keys name-then-value, lexically.
fn compare_path_elems(a: &PathElem, b: &PathElem) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn rank(e: &PathElem) -> u8 {
        match e {
            PathElem::Dot => 0,
            PathElem::Field(_) => 1,
            PathElem::Key(_) => 2,
            PathElem::Value(_) => 3,
            PathElem::Index(_) => 4,
            PathElem::Unknown(_) => 5,
        }
    }
    match (a, b) {
        (PathElem::Field(x), PathElem::Field(y)) | (PathElem::Unknown(x), PathElem::Unknown(y)) => {
            x.as_bytes().cmp(y.as_bytes())
        }
        (PathElem::Key(x), PathElem::Key(y)) => {
            for ((nx, vx), (ny, vy)) in x.iter().zip(y) {
                let c = nx
                    .as_bytes()
                    .cmp(ny.as_bytes())
                    .then_with(|| compare_values(vx, vy));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (PathElem::Value(x), PathElem::Value(y)) => compare_values(x, y),
        (PathElem::Index(x), PathElem::Index(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// The order `fieldpath.Set.emitContentsV1` (fieldpath/serialize.go:79-)
/// writes the keys of a `FieldsV1` object in: `"."` first, then the merged,
/// sorted members and children. `serde_json::Value` loses that order (its
/// maps are sorted by key string, which would put `i:` before `k:`), so it
/// is re-derived here.
pub fn fields_v1_order(a: &str, b: &str) -> std::cmp::Ordering {
    compare_path_elems(&parse_path_elem(a), &parse_path_elem(b))
}

/// `FieldsV1.Raw` as `json.MarshalIndent` emits it: the stored bytes, i.e.
/// the keys in [`fields_v1_order`] at every level.
fn fields_v1_json(v: &Value) -> GoJson {
    match v {
        Value::Object(o) => {
            let mut e: Vec<(&String, &Value)> = o.iter().collect();
            e.sort_by(|a, b| fields_v1_order(a.0, b.0));
            GoJson::Object(
                e.into_iter()
                    .map(|(k, v)| (k.clone(), fields_v1_json(v)))
                    .collect(),
            )
        }
        other => raw_json(other),
    }
}

fn raw_json(v: &Value) -> GoJson {
    match v {
        Value::Null => GoJson::Null,
        Value::Bool(b) => GoJson::Bool(*b),
        Value::Number(n) => GoJson::Number(n.to_string()),
        Value::String(s) => GoJson::str(s.clone()),
        Value::Array(a) => GoJson::Array(a.iter().map(raw_json).collect()),
        Value::Object(o) => {
            GoJson::Object(o.iter().map(|(k, v)| (k.clone(), raw_json(v))).collect())
        }
    }
}

/// `diff.Diff(old, new)` of two values of the struct `fields` describes.
pub fn struct_diff(fields: &[Field], old: &Value, new: &Value) -> String {
    crate::diff::diff(
        &render_struct(fields, Some(old)),
        &render_struct(fields, Some(new)),
    )
}

/// The internal `core.PodSpec` view of a v1 pod spec's JSON, per
/// `Convert_v1_PodSpec_To_core_PodSpec` (pkg/apis/core/v1/conversion.go:349-372):
/// `DeprecatedServiceAccount` stands in for an empty `ServiceAccountName`, and
/// `SecurityContext` is always allocated and receives the v1 top-level
/// `hostNetwork`/`hostPID`/`hostIPC`/`shareProcessNamespace`/`hostUsers`.
pub fn internal_pod_spec(spec: &Value) -> Value {
    let mut out = spec.clone();
    let Some(obj) = out.as_object_mut() else {
        return out;
    };
    if obj
        .get("serviceAccountName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .is_empty()
    {
        if let Some(sa) = obj.get("serviceAccount").cloned() {
            obj.insert("serviceAccountName".to_string(), sa);
        }
    }
    let mut sc = match obj.get("securityContext") {
        Some(Value::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    for k in [
        "hostNetwork",
        "hostPID",
        "hostIPC",
        "shareProcessNamespace",
        "hostUsers",
    ] {
        match obj.get(k) {
            Some(v) => sc.insert(k.to_string(), v.clone()),
            None => sc.remove(k),
        };
    }
    obj.insert("securityContext".to_string(), Value::Object(sc));
    out
}

/// `diff.Diff(oldPod.Spec, mungedPodSpec)` (validation.go:5833).
pub fn pod_spec_diff(old: &Value, new: &Value) -> String {
    struct_diff(POD_SPEC, &internal_pod_spec(old), &internal_pod_spec(new))
}

/// `diff.Diff(oldPv.Spec.PersistentVolumeSource, newPv.Spec.PersistentVolumeSource)`
/// (validation.go:2283); the source fields are the (embedded, so inlined)
/// top-level keys of the v1 `PersistentVolumeSpec` JSON.
pub fn persistent_volume_source_diff(old_spec: &Value, new_spec: &Value) -> String {
    struct_diff(PERSISTENT_VOLUME_SOURCE, old_spec, new_spec)
}
