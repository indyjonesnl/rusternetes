use crate::resources::service_account::ObjectReference;
use crate::resources::volume::LabelSelector;
use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CSIDriver represents information about a CSI volume driver installed on a cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIDriver {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: CSIDriverSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIDriverSpec {
    /// attachRequired indicates this CSI volume driver requires an attach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_required: Option<bool>,

    /// podInfoOnMount indicates this CSI volume driver requires additional pod information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_info_on_mount: Option<bool>,

    /// fsGroupPolicy defines if the volume supports changing ownership and permission of the volume
    ///
    /// No `empty_string_as_none`: upstream's field is a `*FSGroupPolicy`, so a
    /// non-nil "" reaches `validateFSGroupPolicy` (validation.go:505-516) and is
    /// NotSupported. It decodes as `Unknown("")`, distinct from absent (#2505).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_group_policy: Option<FSGroupPolicy>,

    /// storageCapacity indicates that the CSI volume driver wants pod scheduling to consider storage capacity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_capacity: Option<bool>,

    /// volumeLifecycleModes defines what kind of volumes this CSI volume driver supports
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_lifecycle_modes: Option<Vec<VolumeLifecycleMode>>,

    /// tokenRequests indicates the CSI driver needs service account tokens
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_requests: Option<Vec<TokenRequest>>,

    /// requiresRepublish indicates the CSI driver wants NodePublishVolume to be periodically called
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_republish: Option<bool>,

    /// seLinuxMount specifies if the CSI driver supports "-o context" mount option
    #[serde(skip_serializing_if = "Option::is_none", rename = "seLinuxMount")]
    pub se_linux_mount: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_allocatable_update_period_seconds: Option<i64>,

    /// serviceAccountTokenInSecrets indicates tokens from tokenRequests are also
    /// stored in a Secret accessible to the driver (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_token_in_secrets: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FSGroupPolicy {
    ReadWriteOnceWithFSType,
    File,
    None,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2496).
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeLifecycleMode {
    Persistent,
    Ephemeral,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2496).
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequest {
    /// audience is the intended audience of the token.
    ///
    /// An empty audience is legal: `validateTokenRequests`
    /// (`pkg/apis/storage/validation/validation.go`) rejects only *duplicate*
    /// audiences and out-of-range expirations.
    #[serde(default)]
    pub audience: String,

    /// expirationSeconds is the requested duration of validity of the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
}

/// CSINode holds information about all CSI drivers installed on a node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSINode {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: CSINodeSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CSINodeSpec {
    /// drivers is a list of information of all CSI Drivers existing on a node
    #[serde(default, deserialize_with = "crate::deserialize_null_default")]
    pub drivers: Vec<CSINodeDriver>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSINodeDriver {
    /// name represents the name of the CSI driver
    #[serde(default)]
    pub name: String,

    /// nodeID of the node from the driver point of view
    #[serde(rename = "nodeID", default)]
    pub node_id: String,

    /// topologyKeys is the list of keys supported by the driver
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_keys: Option<Vec<String>>,

    /// allocatable represents the volume resources of a node that are available for scheduling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocatable: Option<VolumeNodeResources>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeNodeResources {
    /// count indicates the maximum number of unique volumes managed by the CSI driver
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
}

/// VolumeAttachment captures the intent to attach or detach the specified volume to/from the specified node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachment {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: VolumeAttachmentSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VolumeAttachmentStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentSpec {
    /// attacher indicates the name of the volume driver that MUST handle this request
    #[serde(default)]
    pub attacher: String,

    /// nodeName represents the node that the volume should be attached to
    #[serde(default)]
    pub node_name: String,

    /// source represents the volume that should be attached. A value upstream,
    /// and `ValidateVolumeAttachmentSpec`
    /// (`pkg/apis/storage/validation/validation.go:165-175`) answers an empty
    /// one with "must specify exactly one of inlineVolumeSpec and
    /// persistentVolumeName".
    #[serde(default)]
    pub source: VolumeAttachmentSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentSource {
    /// persistentVolumeName represents the name of the persistent volume to attach
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_name: Option<String>,

    /// inlineVolumeSpec contains all the information necessary to attach a persistent volume defined by a pod's inline VolumeSource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_volume_spec: Option<InlineVolumeSpec>,
}

/// A VolumeAttachment's `inlineVolumeSpec` is a full `PersistentVolumeSpec`
/// upstream (`VolumeAttachmentSource.InlineVolumeSpec *v1.PersistentVolumeSpec`),
/// so it carries capacity/accessModes/the volume source and is validated by
/// `ValidatePersistentVolumeSpec`. Alias it to the real type rather than a stub.
pub type InlineVolumeSpec = crate::resources::volume::PersistentVolumeSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIVolumeSource {
    /// driver is the name of the driver to use for this volume
    #[serde(default)]
    pub driver: String,

    /// volumeHandle is the unique volume name returned by the CSI volume plugin's CreateVolume
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_handle: Option<String>,

    /// readOnly value to pass to ControllerPublishVolumeRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    /// fsType to mount
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,

    /// volumeAttributes of the volume to publish
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_attributes: Option<HashMap<String, String>>,

    /// nodePublishSecretRef is a reference to the secret object containing sensitive information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_publish_secret_ref: Option<ObjectReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentStatus {
    /// attached indicates the volume is successfully attached. A plain bool
    /// upstream, so an absent key is `false`, not an error.
    #[serde(default)]
    pub attached: bool,

    /// attachmentMetadata is populated with any information returned by the attach operation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_metadata: Option<HashMap<String, String>>,

    /// attachError represents the last error encountered during attach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_error: Option<VolumeError>,

    /// detachError represents the last error encountered during detach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_error: Option<VolumeError>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeError {
    /// time the error was encountered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,

    /// message is a string detailing the encountered error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// errorCode is a programmatic error code categorising the error (gRPC code)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<i32>,
}

/// CSIStorageCapacity stores the result of one CSI GetCapacity call
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIStorageCapacity {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,

    /// storageClassName represents the name of the StorageClass
    #[serde(default)]
    pub storage_class_name: String,

    /// capacity is the value reported by the CSI driver in its GetCapacityResponse
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt",
        deserialize_with = "crate::quantity::wire::deserialize_opt"
    )]
    pub capacity: Option<String>,

    /// maximumVolumeSize is the value reported by the CSI driver in its GetCapacityResponse
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt",
        deserialize_with = "crate::quantity::wire::deserialize_opt"
    )]
    pub maximum_volume_size: Option<String>,

    /// nodeTopology defines which nodes have access to the storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_topology: Option<LabelSelector>,
}

/// VolumeAttributesClass represents a specification of mutable volume attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttributesClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,

    /// driverName is the name of the CSI driver that this class applies to
    #[serde(default)]
    pub driver_name: String,

    /// parameters hold volume attributes defined by the CSI driver
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,
}
