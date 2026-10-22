//! `StorageVersion` (`internal.apiserver.k8s.io/v1alpha1`).
//!
//! Port of `staging/src/k8s.io/api/apiserverinternal/v1alpha1/types.go`
//! (`StorageVersion` :27-41, `StorageVersionSpec` :44, `StorageVersionStatus`
//! :48-65, `ServerStorageVersion` :69-86, `StorageVersionConditionType` and
//! `ConditionStatus` consts :88-100, `StorageVersionCondition` :102-122, list
//! :124-134). The group-version is alpha and in
//! `alphaAPIGroupVersionsDisabledByDefault`
//! (`pkg/controlplane/instance.go:487-497`); the API server writes these
//! objects itself under `StorageVersionAPI` (`pkg/features/kube_features.go:2084`).

use crate::types::{ListMeta, ObjectMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `AllEncodingVersionsEqual` (types.go:91).
pub const ALL_ENCODING_VERSIONS_EQUAL: &str = "AllEncodingVersionsEqual";

fn default_api_version() -> String {
    "internal.apiserver.k8s.io/v1alpha1".to_string()
}

fn default_kind() -> String {
    "StorageVersion".to_string()
}

fn default_list_kind() -> String {
    "StorageVersionList".to_string()
}

/// Storage version of a specific resource. The name is `<group>.<resource>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersion {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Go's `Spec` is a non-pointer struct, so it always serializes (`{}`).
    #[serde(default)]
    pub spec: StorageVersionSpec,
    /// Go's `Status` is a non-pointer struct, so it always serializes (`{}`).
    #[serde(default)]
    pub status: StorageVersionStatus,
}

impl Default for StorageVersion {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            metadata: ObjectMeta::default(),
            spec: StorageVersionSpec::default(),
            status: StorageVersionStatus::default(),
        }
    }
}

/// An empty spec, present to comply with Kubernetes API style.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageVersionSpec {}

/// The versions each API server instance can decode and encodes to.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionStatus {
    /// The reported versions per API server instance (listMapKey apiServerID).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_versions: Vec<ServerStorageVersion>,
    /// Set when all API server instances agree on the encoding version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub common_encoding_version: Option<String>,
    /// listMapKey type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StorageVersionCondition>,
}

/// One API server instance's report.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerStorageVersion {
    #[serde(
        default,
        rename = "apiServerID",
        skip_serializing_if = "String::is_empty"
    )]
    pub api_server_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub encoding_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decodable_versions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub served_versions: Vec<String>,
}

/// `StorageVersionCondition` (types.go:102-122). Its own type upstream: the
/// reason and message are required, unlike `metav1.Condition`'s.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionCondition {
    #[serde(default, rename = "type")]
    pub condition_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub observed_generation: i64,
    /// Go's `metav1.Time` is a non-pointer value (zero marshals as `null`);
    /// modelled as an `Option` that is omitted when unset.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub last_transition_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionList {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_list_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ListMeta,
    #[serde(default)]
    pub items: Vec<StorageVersion>,
}
