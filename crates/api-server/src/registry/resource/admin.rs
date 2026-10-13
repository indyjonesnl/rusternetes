//! `AuthorizedForAdmin` / `AuthorizedForAdminStatus`
//! (`pkg/registry/resource/utils.go:32-101`): admin access to devices needs the
//! `resource.kubernetes.io/admin-access: "true"` label on the containing
//! namespace.
//!
//! Upstream calls these from the strategies' `Validate` / `ValidateUpdate`
//! (resourceclaim/strategy.go:102, :196; resourceclaimtemplate/strategy.go:64)
//! and appends their errors ahead of the validation errors. Ours are driven
//! from the strategies' `validate_async` / `validate_update_async` hooks, which
//! `before_create` / `before_update` join with the synchronous validators.

use rusternetes_common::resources::dra::{DeviceRequest, DeviceRequestAllocationResult};
use rusternetes_common::resources::Namespace;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_storage::{build_key, Storage, StorageBackend};

/// `resource.DRAAdminNamespaceLabelKey` (pkg/apis/resource/types.go:778).
const ADMIN_NAMESPACE_LABEL_KEY: &str = "resource.kubernetes.io/admin-access";

/// The namespace check both functions share (utils.go:53-63).
async fn check_namespace(storage: &StorageBackend, namespace: &str, path: &Path) -> ErrorList {
    let ns: Namespace = match storage.get(&build_key("namespaces", None, namespace)).await {
        Ok(ns) => ns,
        // `field.InternalError(adminAccessPath, fmt.Errorf("could not retrieve
        // namespace to verify admin access: %w", err))` (utils.go:54-56).
        Err(e) => {
            return vec![FieldError::internal(
                path,
                format!("could not retrieve namespace to verify admin access: {e}"),
            )]
        }
    };
    let labelled = ns
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(ADMIN_NAMESPACE_LABEL_KEY))
        .is_some_and(|v| v == "true");
    if labelled {
        Vec::new()
    } else {
        vec![FieldError::forbidden(
            path,
            format!(
                "admin access to devices requires the `{ADMIN_NAMESPACE_LABEL_KEY}: true` label on the containing namespace"
            ),
        )]
    }
}

/// `AuthorizedForAdmin` (utils.go:32-64). Only `exactly` can carry
/// `adminAccess`; the spec is immutable so the old object is not consulted.
pub async fn authorized_for_admin(
    storage: &StorageBackend,
    requests: &[DeviceRequest],
    namespace: &str,
) -> ErrorList {
    for (i, r) in requests.iter().enumerate() {
        if r.exactly
            .as_ref()
            .is_some_and(|e| e.admin_access == Some(true))
        {
            let path = Path::new("spec")
                .child("devices")
                .child("requests")
                .index(i)
                .child("adminAccess");
            return check_namespace(storage, namespace, &path).await;
        }
    }
    Vec::new()
}

/// `adminRequested` (utils.go:93-101): the first result with `adminAccess`
/// set, and its path.
fn admin_requested(results: &[DeviceRequestAllocationResult]) -> Option<Path> {
    results
        .iter()
        .position(|r| r.admin_access == Some(true))
        .map(|i| {
            Path::new("status")
                .child("allocation")
                .child("devices")
                .child("results")
                .index(i)
                .child("adminAccess")
        })
}

/// `AuthorizedForAdminStatus` (utils.go:66-91). Skipped when the old status
/// already had admin access granted, since `status.allocation` is immutable.
pub async fn authorized_for_admin_status(
    storage: &StorageBackend,
    new: &[DeviceRequestAllocationResult],
    old: &[DeviceRequestAllocationResult],
    namespace: &str,
) -> ErrorList {
    if admin_requested(old).is_some() {
        return Vec::new();
    }
    match admin_requested(new) {
        Some(path) => check_namespace(storage, namespace, &path).await,
        None => Vec::new(),
    }
}
