//! The `core/v1 PersistentVolumeSource` members that `PersistentVolumeSpec`
//! (`volume.rs`) does not carry as flat fields of its own.
//!
//! Upstream embeds `PersistentVolumeSource` inline in `PersistentVolumeSpec`
//! (`staging/src/k8s.io/api/core/v1/types.go:239-340`, 22 members). The Rust
//! spec keeps hostPath/nfs/iscsi/local/csi/gcePersistentDisk/
//! awsElasticBlockStore/fc flat; the remaining members live here and are inlined
//! into the spec with `#[serde(flatten)]`, so the wire shape is identical.
//! Field types, `json` names and `omitempty`-ness are taken one-for-one from
//! `types.go`: a Go `string`/`bool` with `omitempty` is a `String`/`bool` that
//! is skipped when empty/false, a Go pointer is an `Option`.

use crate::resources::service_account::ObjectReference;
use crate::resources::volume::SecretReference;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn is_false(b: &bool) -> bool {
    !*b
}

/// The inlined `PersistentVolumeSource` members not modelled as flat fields of
/// `PersistentVolumeSpec`. Every member is optional; at most one may be set
/// across the whole union (`ValidatePersistentVolumeSpec`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeMoreSources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glusterfs: Option<GlusterfsPersistentVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rbd: Option<RBDPersistentVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cinder: Option<CinderPersistentVolumeSource>,
    #[serde(default, rename = "cephfs", skip_serializing_if = "Option::is_none")]
    pub cephfs: Option<CephFSPersistentVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flocker: Option<FlockerVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flex_volume: Option<FlexPersistentVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_file: Option<AzureFilePersistentVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vsphere_volume: Option<VsphereVirtualDiskVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quobyte: Option<QuobyteVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_disk: Option<AzureDiskVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub photon_persistent_disk: Option<PhotonPersistentDiskVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portworx_volume: Option<PortworxVolumeSource>,
    #[serde(default, rename = "scaleIO", skip_serializing_if = "Option::is_none")]
    pub scale_io: Option<ScaleIOPersistentVolumeSource>,
    #[serde(default, rename = "storageos", skip_serializing_if = "Option::is_none")]
    pub storageos: Option<StorageOSPersistentVolumeSource>,
}

/// `core/v1 GlusterfsPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlusterfsPersistentVolumeSource {
    #[serde(default, rename = "endpoints")]
    pub endpoints_name: String,
    #[serde(default)]
    pub path: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoints_namespace: Option<String>,
}

/// `core/v1 RBDPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RBDPersistentVolumeSource {
    #[serde(default, rename = "monitors")]
    pub ceph_monitors: Vec<String>,
    #[serde(default, rename = "image")]
    pub rbd_image: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, rename = "pool", skip_serializing_if = "String::is_empty")]
    pub rbd_pool: String,
    #[serde(default, rename = "user", skip_serializing_if = "String::is_empty")]
    pub rados_user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub keyring: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

/// `core/v1 CinderPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CinderPersistentVolumeSource {
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
}

/// `core/v1 CephFSPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CephFSPersistentVolumeSource {
    #[serde(default)]
    pub monitors: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

/// `core/v1 FlockerVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FlockerVolumeSource {
    #[serde(
        default,
        rename = "datasetName",
        skip_serializing_if = "String::is_empty"
    )]
    pub dataset_name: String,
    #[serde(
        default,
        rename = "datasetUUID",
        skip_serializing_if = "String::is_empty"
    )]
    pub dataset_uuid: String,
}

/// `core/v1 FlexPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlexPersistentVolumeSource {
    #[serde(default)]
    pub driver: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

/// `core/v1 AzureFilePersistentVolumeSource`. `secretNamespace` is a pointer
/// with no `omitempty` upstream, so an unset one serialises as `null`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureFilePersistentVolumeSource {
    #[serde(default)]
    pub secret_name: String,
    #[serde(default)]
    pub share_name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default)]
    pub secret_namespace: Option<String>,
}

/// `core/v1 VsphereVirtualDiskVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VsphereVirtualDiskVolumeSource {
    #[serde(default, rename = "volumePath")]
    pub volume_path: String,
    #[serde(default, rename = "fsType", skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(
        default,
        rename = "storagePolicyName",
        skip_serializing_if = "String::is_empty"
    )]
    pub storage_policy_name: String,
    #[serde(
        default,
        rename = "storagePolicyID",
        skip_serializing_if = "String::is_empty"
    )]
    pub storage_policy_id: String,
}

/// `core/v1 QuobyteVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuobyteVolumeSource {
    #[serde(default)]
    pub registry: String,
    #[serde(default)]
    pub volume: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tenant: String,
}

/// `core/v1 AzureDiskVolumeSource` (`cachingMode`/`kind` are the string types
/// `AzureDataDiskCachingMode` / `AzureDataDiskKind`; their membership is
/// checked by `validateAzureDisk`, as upstream).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AzureDiskVolumeSource {
    #[serde(default, rename = "diskName")]
    pub disk_name: String,
    #[serde(default, rename = "diskURI")]
    pub data_disk_uri: String,
    #[serde(
        default,
        rename = "cachingMode",
        skip_serializing_if = "Option::is_none"
    )]
    pub caching_mode: Option<String>,
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// `core/v1 PhotonPersistentDiskVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PhotonPersistentDiskVolumeSource {
    #[serde(default, rename = "pdID")]
    pub pd_id: String,
    #[serde(default, rename = "fsType", skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
}

/// `core/v1 PortworxVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PortworxVolumeSource {
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
    #[serde(default, rename = "fsType", skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, rename = "readOnly", skip_serializing_if = "is_false")]
    pub read_only: bool,
}

/// `core/v1 ScaleIOPersistentVolumeSource`. `secretRef` is a pointer with no
/// `omitempty` upstream, so an unset one serialises as `null`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleIOPersistentVolumeSource {
    #[serde(default)]
    pub gateway: String,
    #[serde(default)]
    pub system: String,
    #[serde(default)]
    pub secret_ref: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub ssl_enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub protection_domain: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub storage_pool: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub storage_mode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub volume_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

/// `core/v1 StorageOSPersistentVolumeSource`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageOSPersistentVolumeSource {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub volume_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub volume_namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fs_type: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<ObjectReference>,
}
