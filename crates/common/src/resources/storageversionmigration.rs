//! `StorageVersionMigration` (`storagemigration.k8s.io/v1beta1`).
//!
//! Port of `staging/src/k8s.io/api/storagemigration/v1beta1/types.go`
//! (`StorageVersionMigration` :27-41, `StorageVersionMigrationSpec` :44-50,
//! `MigrationConditionType` consts :52-62, `StorageVersionMigrationStatus`
//! :64-76, list :78-90). Served only while the `StorageVersionMigrator`
//! feature gate is on (`pkg/registry/storagemigration/rest/
//! storage_storagemigration.go`; `pkg/features/kube_features.go:1829`).

use crate::types::{Condition, ListMeta, ObjectMeta};
use serde::{Deserialize, Serialize};

/// `MigrationRunning` (`pkg/apis/storagemigration/types.go`).
pub const MIGRATION_RUNNING: &str = "Running";
/// `MigrationSucceeded`.
pub const MIGRATION_SUCCEEDED: &str = "Succeeded";
/// `MigrationFailed`.
pub const MIGRATION_FAILED: &str = "Failed";

fn default_api_version() -> String {
    "storagemigration.k8s.io/v1beta1".to_string()
}

fn default_kind() -> String {
    "StorageVersionMigration".to_string()
}

fn default_list_kind() -> String {
    "StorageVersionMigrationList".to_string()
}

/// A migration of stored data to the latest storage version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionMigration {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Go's `Spec` is a non-pointer struct, so it always serializes.
    #[serde(default)]
    pub spec: StorageVersionMigrationSpec,
    /// Go's `Status` is a non-pointer struct, so it always serializes (`{}`).
    #[serde(default)]
    pub status: StorageVersionMigrationStatus,
}

impl Default for StorageVersionMigration {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            metadata: ObjectMeta::default(),
            spec: StorageVersionMigrationSpec::default(),
            status: StorageVersionMigrationStatus::default(),
        }
    }
}

/// `metav1.GroupResource` (`group_version.go:66-69`): both fields always
/// serialize.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GroupResource {
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub resource: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageVersionMigrationSpec {
    /// The resource that is being migrated. Immutable.
    #[serde(default)]
    pub resource: GroupResource,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionMigrationStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// ResourceVersion to compare with the GC cache for performing the migration.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageVersionMigrationList {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_list_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ListMeta,
    #[serde(default)]
    pub items: Vec<StorageVersionMigration>,
}
