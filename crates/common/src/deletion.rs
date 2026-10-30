// Deletion handling with finalizer support
//
// Implements:
// - Graceful deletion with finalizers
// - Deletion timestamp management
// - Cascade deletion support

use crate::types::DeletionPropagation;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Deletion request with options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptions {
    /// Deletion propagation policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub propagation_policy: Option<DeletionPropagation>,

    /// Grace period seconds before deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<i64>,

    /// Preconditions for deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,

    /// Whether to orphan dependents (deprecated, use propagation_policy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphan_dependents: Option<bool>,

    /// Whether to perform a dry run
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<Vec<String>>,

    /// IgnoreStoreReadErrorWithClusterBreakingPotential allows the API server
    /// to bypass the storage read step during deletion. This is only intended
    /// for break-glass recovery (cluster-breaking scenarios) and is mutually
    /// exclusive with every other DeleteOptions field. Mirrors upstream
    /// `metav1.DeleteOptions.IgnoreStoreReadErrorWithClusterBreakingPotential`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_store_read_error_with_cluster_breaking_potential: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preconditions {
    /// UID must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// ResourceVersion must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            propagation_policy: Some(DeletionPropagation::Background),
            grace_period_seconds: Some(30),
            preconditions: None,
            orphan_dependents: None,
            dry_run: None,
            ignore_store_read_error_with_cluster_breaking_potential: None,
        }
    }
}

/// Result of a deletion attempt
#[derive(Debug, Clone, PartialEq)]
pub enum DeletionResult {
    /// Resource was deleted immediately
    Deleted,

    /// Resource was marked for deletion (has finalizers)
    MarkedForDeletion,

    /// Deletion was deferred (precondition failed)
    Deferred(String),
}

/// Process a deletion request
pub fn process_deletion(
    resource: &mut Value,
    options: &DeleteOptions,
) -> Result<DeletionResult, DeletionError> {
    // Extract metadata
    let metadata = resource
        .get_mut("metadata")
        .and_then(|v| v.as_object_mut())
        .ok_or(DeletionError::InvalidResource(
            "Missing metadata".to_string(),
        ))?;

    // Check preconditions
    if let Some(preconditions) = &options.preconditions {
        if let Some(required_uid) = &preconditions.uid {
            let current_uid = metadata
                .get("uid")
                .and_then(|v| v.as_str())
                .ok_or(DeletionError::InvalidResource("Missing UID".to_string()))?;

            if current_uid != required_uid {
                return Ok(DeletionResult::Deferred(
                    "UID precondition failed".to_string(),
                ));
            }
        }

        if let Some(required_version) = &preconditions.resource_version {
            let current_version = metadata
                .get("resourceVersion")
                .and_then(|v| v.as_str())
                .ok_or(DeletionError::InvalidResource(
                    "Missing resourceVersion".to_string(),
                ))?;

            if current_version != required_version {
                return Ok(DeletionResult::Deferred(
                    "ResourceVersion precondition failed".to_string(),
                ));
            }
        }
    }

    // Check if resource has finalizers
    let has_finalizers = metadata
        .get("finalizers")
        .and_then(|v| v.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false);

    // If already marked for deletion, check if we can delete now
    if metadata.contains_key("deletionTimestamp") {
        if has_finalizers {
            return Ok(DeletionResult::MarkedForDeletion);
        } else {
            return Ok(DeletionResult::Deleted);
        }
    }

    // Set deletion timestamp
    let deletion_timestamp = Utc::now();
    metadata.insert(
        "deletionTimestamp".to_string(),
        serde_json::to_value(deletion_timestamp)
            .map_err(|e| DeletionError::Internal(e.to_string()))?,
    );

    // Set grace period
    if let Some(grace_period) = options.grace_period_seconds {
        metadata.insert(
            "deletionGracePeriodSeconds".to_string(),
            serde_json::Value::Number(grace_period.into()),
        );
    }

    // Add finalizers based on propagation policy
    let propagation = options
        .propagation_policy
        .unwrap_or(DeletionPropagation::Background);

    match propagation {
        DeletionPropagation::Foreground => {
            add_finalizer(metadata, "foregroundDeletion")?;
        }
        DeletionPropagation::Orphan => {
            add_finalizer(metadata, "orphan")?;
        }
        DeletionPropagation::Background => {
            // No additional finalizer needed
        }
    }

    // Check again after potentially adding finalizers
    let has_finalizers_now = metadata
        .get("finalizers")
        .and_then(|v| v.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false);

    if has_finalizers_now {
        Ok(DeletionResult::MarkedForDeletion)
    } else {
        Ok(DeletionResult::Deleted)
    }
}

/// Add a finalizer to metadata
fn add_finalizer(
    metadata: &mut serde_json::Map<String, Value>,
    finalizer: &str,
) -> Result<(), DeletionError> {
    let finalizers = metadata
        .entry("finalizers".to_string())
        .or_insert_with(|| Value::Array(vec![]));

    if let Some(arr) = finalizers.as_array_mut() {
        let finalizer_value = Value::String(finalizer.to_string());
        if !arr.contains(&finalizer_value) {
            arr.push(finalizer_value);
        }
        Ok(())
    } else {
        Err(DeletionError::InvalidResource(
            "Finalizers is not an array".to_string(),
        ))
    }
}

/// Remove a finalizer from metadata
pub fn remove_finalizer(
    metadata: &mut serde_json::Map<String, Value>,
    finalizer: &str,
) -> Result<(), DeletionError> {
    if let Some(finalizers) = metadata.get_mut("finalizers") {
        if let Some(arr) = finalizers.as_array_mut() {
            let finalizer_value = Value::String(finalizer.to_string());
            arr.retain(|v| v != &finalizer_value);
        }
    }
    Ok(())
}

/// Check if a resource is being deleted
pub fn is_being_deleted(resource: &Value) -> bool {
    resource
        .get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .is_some()
}

/// Check if a resource can be deleted (no finalizers)
pub fn can_be_deleted(resource: &Value) -> bool {
    is_being_deleted(resource) && !has_finalizers(resource)
}

/// Check if a resource has finalizers
pub fn has_finalizers(resource: &Value) -> bool {
    resource
        .get("metadata")
        .and_then(|m| m.get("finalizers"))
        .and_then(|f| f.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false)
}

/// Errors that can occur during deletion
#[derive(Debug, thiserror::Error)]
pub enum DeletionError {
    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
