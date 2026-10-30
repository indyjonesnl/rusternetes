/// Concurrency control module for optimistic locking with resourceVersion
use rusternetes_common::Error;

/// Extract resourceVersion from metadata.
///
/// Returns `None` when the field is absent *or* when it is an empty string.
/// Kubernetes treats `resourceVersion: ""` identically to an absent
/// resourceVersion (no optimistic-concurrency check). Callers that
/// previously received `Some("")` would have tried to parse it as an i64
/// and failed with "Invalid resourceVersion: ".
pub fn extract_resource_version(metadata: &serde_json::Value) -> Option<String> {
    metadata
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Validate that the provided resourceVersion matches the expected version
pub fn validate_resource_version(
    expected: Option<&str>,
    actual: Option<&str>,
) -> Result<(), Error> {
    match (expected, actual) {
        (Some(expected_rv), Some(actual_rv)) => {
            if expected_rv != actual_rv {
                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: expected '{}', got '{}'",
                    expected_rv, actual_rv
                )));
            }
            Ok(())
        }
        (Some(expected_rv), None) => Err(Error::Conflict(format!(
            "resourceVersion mismatch: expected '{}', got none",
            expected_rv
        ))),
        _ => Ok(()), // If no expected version specified, allow update
    }
}

/// Convert etcd mod_revision to resourceVersion string
pub fn mod_revision_to_resource_version(mod_revision: i64) -> String {
    mod_revision.to_string()
}

/// Parse resourceVersion string to mod_revision
pub fn resource_version_to_mod_revision(resource_version: &str) -> Result<i64, Error> {
    resource_version.parse::<i64>().map_err(|_| {
        Error::InvalidResource(format!("Invalid resourceVersion: {}", resource_version))
    })
}
