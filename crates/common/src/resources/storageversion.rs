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

/// `SetCommonEncodingVersion`
/// (`staging/src/k8s.io/apiserver/pkg/storageversion/updater.go:40-80`, with
/// `setStatusCondition` :94-116): recompute `commonEncodingVersion` and the
/// `AllEncodingVersionsEqual` condition from `status.storageVersions`.
pub fn set_common_encoding_version(sv: &mut StorageVersion) {
    let old_common = sv.status.common_encoding_version.take();
    if let Some(first) = sv.status.storage_versions.first() {
        let first_version = first.encoding_version.clone();
        if sv
            .status
            .storage_versions
            .iter()
            .all(|s| s.encoding_version == first_version)
        {
            sv.status.common_encoding_version = Some(first_version);
        }
    }

    let is_set = sv.status.common_encoding_version.is_some();
    let new = StorageVersionCondition {
        condition_type: ALL_ENCODING_VERSIONS_EQUAL.to_string(),
        status: if is_set { "True" } else { "False" }.to_string(),
        observed_generation: sv.metadata.generation.unwrap_or(0),
        last_transition_time: Some(Utc::now()),
        reason: if is_set {
            "CommonEncodingVersionSet"
        } else {
            "CommonEncodingVersionUnset"
        }
        .to_string(),
        message: if is_set {
            "Common encoding version set"
        } else {
            "Common encoding version unset"
        }
        .to_string(),
    };
    let force_transition = matches!(
        (&old_common, &sv.status.common_encoding_version),
        (Some(o), Some(n)) if o != n
    );
    match sv
        .status
        .conditions
        .iter_mut()
        .find(|c| c.condition_type == new.condition_type)
    {
        None => sv.status.conditions.push(new),
        Some(existing) => {
            if existing.status != new.status || force_transition {
                existing.last_transition_time = new.last_transition_time;
            }
            existing.status = new.status;
            existing.reason = new.reason;
            existing.message = new.message;
            existing.observed_generation = new.observed_generation;
        }
    }
}
