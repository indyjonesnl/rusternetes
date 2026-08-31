use chrono::Utc;
use rusternetes_common::Result;
use rusternetes_storage::Storage;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

/// Handle deletion of a resource that may have finalizers.
///
/// This function implements the Kubernetes finalizer protocol:
/// 1. If the resource has finalizers AND does NOT have a deletionTimestamp:
///    - Set deletionTimestamp to current time
///    - Update the resource in storage
///    - Return Ok(true) to indicate the resource was marked for deletion
/// 2. If the resource has finalizers AND has a deletionTimestamp:
///    - Do nothing (wait for controllers to remove finalizers)
///    - Return Ok(true) to indicate the resource is being finalized
/// 3. If the resource has NO finalizers (or empty finalizers list):
///    - Delete the resource from storage immediately
///    - Return Ok(false) to indicate the resource was deleted
///
/// # Arguments
///
/// * `storage` - The storage backend
/// * `key` - The storage key for the resource
/// * `resource` - The resource to potentially delete
///
/// # Returns
///
/// * `Ok(true)` - Resource has finalizers and was marked for deletion or is being finalized
/// * `Ok(false)` - Resource had no finalizers and was deleted from storage
/// * `Err(_)` - An error occurred
///
/// # Example
///
/// ```no_run
/// use rusternetes_api_server::handlers::finalizers::handle_delete_with_finalizers;
/// use rusternetes_common::resources::Pod;
/// use rusternetes_common::Result;
/// use rusternetes_storage::Storage;
/// use tracing::info;
///
/// async fn delete_pod<S: Storage>(storage: &S, key: &str) -> Result<()> {
///     // Get the resource
///     let pod: Pod = storage.get(key).await?;
///
///     // Handle deletion with finalizers
///     let marked_for_deletion = handle_delete_with_finalizers(
///         storage,
///         key,
///         &pod,
///     ).await?;
///
///     if marked_for_deletion {
///         info!("Pod marked for deletion, waiting for finalizers to be removed");
///     } else {
///         info!("Pod deleted immediately (no finalizers)");
///     }
///
///     Ok(())
/// }
/// ```
pub async fn handle_delete_with_finalizers<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
) -> Result<bool>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    handle_delete_with_finalizers_and_propagation(storage, key, resource, None).await
}

/// `DeleteCollection` variant: delete one item of a collection, tolerating an
/// item that has already gone away.
///
/// A collection delete lists first and then deletes each item, so anything else
/// deleting concurrently — a controller finishing a reclaim, the garbage
/// collector, another client — makes an item vanish between the two steps. That
/// is a race, not a failure of the request, and upstream says so explicitly
/// (`staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`,
/// `DeleteCollection`):
///
/// ```go
/// if _, _, err := e.Delete(ctx, accessor.GetName(), deleteValidation, options.DeepCopy());
///     err != nil && !apierrors.IsNotFound(err) {
///     klog.V(4).InfoS("Delete object in DeleteCollection failed", "object", klog.KObj(accessor), "err", err)
///     errs <- err
///     return
/// }
/// ```
///
/// Propagating the `NotFound` instead is what made
/// `[sig-storage] PersistentVolumes CSI Conformance should run through the
/// lifecycle of a PV and a PVC` fail intermittently: the spec deletes its PVC by
/// collection, waits for it to go, then deletes the PV by collection — and our
/// own PV reclaim had already removed the volume, so the second DeleteCollection
/// answered
/// `Failed to delete PV "pvc-c697e": persistentvolumes "pv-7783-d0990" not found`.
///
/// Returns `Ok(Some(true))` when the item was deleted outright, `Ok(Some(false))`
/// when it is now pending finalizers, and `Ok(None)` when it had already been
/// deleted by someone else.
pub async fn delete_collection_item<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
) -> Result<Option<bool>>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    match handle_delete_with_finalizers_and_propagation(storage, key, resource, None).await {
        Ok(pending_finalizers) => Ok(Some(!pending_finalizers)),
        Err(rusternetes_common::Error::NotFound(_)) => {
            debug!("{key} already deleted concurrently; skipping it in the collection delete");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// The two garbage-collection finalizers a graceful DELETE may leave on an
/// object, computed from the effective propagation policy.
///
/// Port of upstream `deletionFinalizersForGarbageCollection`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:976),
/// which strips both GC finalizers and adds back whichever the policy calls
/// for, using `shouldOrphanDependents` (store.go:883) and
/// `shouldDeleteDependents` (store.go:932) for the precedence:
///
/// 1. the deprecated `deleteOptions.orphanDependents` bool wins over everything
///    (`true` orphans, `false` means "not orphan" and never foreground),
/// 2. then `deleteOptions.propagationPolicy`,
/// 3. then a GC finalizer already on the object,
/// 4. otherwise neither (background).
///
/// Finalizer literals are upstream's `metav1.FinalizerOrphanDependents` /
/// `FinalizerDeleteDependents` (apimachinery/pkg/apis/meta/v1/types.go:105-106).
pub fn gc_deletion_finalizers(
    existing: Option<&Vec<String>>,
    propagation_policy: Option<&str>,
    orphan_dependents: Option<bool>,
) -> Vec<String> {
    const ORPHAN: &str = "orphan";
    const FOREGROUND: &str = "foregroundDeletion";

    let existing: &[String] = existing.map(|v| v.as_slice()).unwrap_or(&[]);

    // upstream shouldOrphanDependents
    let should_orphan = if let Some(orphan) = orphan_dependents {
        orphan
    } else {
        match propagation_policy {
            Some("Orphan") => true,
            Some("Background") | Some("Foreground") => false,
            _ => existing
                .iter()
                .find_map(|f| match f.as_str() {
                    ORPHAN => Some(true),
                    FOREGROUND => Some(false),
                    _ => None,
                })
                .unwrap_or(false),
        }
    };

    // upstream shouldDeleteDependents
    let should_delete_dependents = if orphan_dependents.is_some() {
        false
    } else {
        match propagation_policy {
            Some("Foreground") => true,
            Some("Background") | Some("Orphan") => false,
            _ => existing
                .iter()
                .find_map(|f| match f.as_str() {
                    FOREGROUND => Some(true),
                    ORPHAN => Some(false),
                    _ => None,
                })
                .unwrap_or(false),
        }
    };

    let mut finalizers: Vec<String> = existing
        .iter()
        .filter(|f| f.as_str() != ORPHAN && f.as_str() != FOREGROUND)
        .cloned()
        .collect();
    if should_orphan {
        finalizers.push(ORPHAN.to_string());
    }
    if should_delete_dependents {
        finalizers.push(FOREGROUND.to_string());
    }
    finalizers
}

/// Read the effective propagation policy and the deprecated `orphanDependents`
/// bool out of a DELETE request: query parameters first, then the
/// `DeleteOptions` body. Upstream decodes both into the same `DeleteOptions`,
/// so either transport is valid.
pub fn parse_delete_propagation(
    params: &std::collections::HashMap<String, String>,
    body_delete_options: Option<&serde_json::Value>,
) -> (Option<String>, Option<bool>) {
    let policy = params.get("propagationPolicy").cloned().or_else(|| {
        body_delete_options
            .and_then(|v| v.get("propagationPolicy"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    });
    let orphan = params
        .get("orphanDependents")
        .and_then(|v| v.parse::<bool>().ok())
        .or_else(|| {
            body_delete_options
                .and_then(|v| v.get("orphanDependents"))
                .and_then(|v| v.as_bool())
        });
    (policy, orphan)
}

/// Apply upstream's `ShouldDeleteDuringUpdate` to an object that a PUT has just
/// stored: if the update drained the last finalizer off an object already
/// pending deletion, the object must disappear as part of that request.
///
/// Returns `true` when the object was removed.
///
/// Upstream runs this inside the generic registry's `Update`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:565,
/// applied via `deleteWithoutFinalizers`), so it covers PUT and PATCH alike.
/// We had it on the PATCH path only
/// ([`crate::handlers::generic_patch::should_delete_during_update`]), which left
/// the GC's end-of-pass sweep as the only thing finishing an object whose last
/// finalizer was removed by a PUT — and most controllers remove finalizers with
/// Update, not Patch (#1831).
///
/// ## Why the stored object is compared against itself
///
/// Upstream's predicate reads the *new* object's finalizers and the *old*
/// object's `deletionTimestamp` / `deletionGracePeriodSeconds`. Those two old
/// fields are exactly what [`crate::handlers::lifecycle::inherit_server_owned_metadata`]
/// reinstates from storage before the write — upstream's `BeforeUpdate` rule
/// that "an update can never remove/change a deletion timestamp"
/// (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:139-146). So on a
/// handler that inherits, the stored object carries the old values and one
/// object is enough; the rule itself still lives in a single place.
///
/// **Only call this from a handler that calls `inherit_server_owned_metadata`.**
/// Without that inheritance a client could put a `deletionTimestamp` of its own
/// in the request body and have the object deleted, which upstream prevents by
/// overwriting that field before the predicate ever runs.
pub async fn finish_deletion_if_finalizers_drained<S, T>(
    storage: &S,
    key: &str,
    stored: &T,
) -> Result<bool>
where
    S: Storage,
    T: Serialize,
{
    let stored_json =
        serde_json::to_value(stored).map_err(rusternetes_common::Error::Serialization)?;
    // Namespaces are the one resource whose registry overrides the predicate:
    // `ShouldDeleteNamespaceDuringUpdate`
    // (pkg/registry/core/namespace/storage/storage.go:258) is
    //
    //     return len(ns.Spec.Finalizers) == 0 && genericregistry.ShouldDeleteDuringUpdate(...)
    //
    // A Terminating namespace normally has an EMPTY `metadata.finalizers` and a
    // `spec.finalizers` of ["kubernetes"], which the namespace controller clears
    // through /finalize only once the namespace is drained. Without this the
    // generic predicate would delete the namespace on the first PUT it received,
    // while its contents were still being removed. `spec.finalizers` exists on
    // no other resource, so the check is safe to apply unconditionally.
    let spec_finalizers_remain = stored_json
        .get("spec")
        .and_then(|sp| sp.get("finalizers"))
        .and_then(|f| f.as_array())
        .is_some_and(|a| !a.is_empty());
    if spec_finalizers_remain {
        return Ok(false);
    }

    if !crate::handlers::generic_patch::should_delete_during_update(&stored_json, &stored_json) {
        return Ok(false);
    }
    match storage.delete(key).await {
        Ok(_) => {
            info!("{key} deleted (finalizers drained during update)");
            Ok(true)
        }
        // Something else finished it first; the outcome the caller wanted holds.
        Err(rusternetes_common::Error::NotFound(_)) => Ok(true),
        Err(e) => Err(e),
    }
}

/// Handle deletion with propagation policy support.
/// When propagation_policy is "Foreground", adds the "foregroundDeletion" finalizer
/// so the garbage collector knows to delete dependents before the owner.
/// When propagation_policy is "Orphan", adds the "orphan" finalizer so dependents
/// are not deleted.
pub async fn handle_delete_with_finalizers_and_propagation<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
    propagation_policy: Option<&str>,
) -> Result<bool>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    // Retry the finalizer-add update on optimistic-concurrency conflicts. The
    // `resource` handed to us was read at the start of the delete handler; a
    // controller (e.g. the RC controller bumping `status`) may bump its
    // resourceVersion before we write the deletionTimestamp + propagation
    // finalizer. The etcd backend rejects the stale-RV write with
    // `Error::Conflict`; upstream performs this as a GuaranteedUpdate that
    // re-reads and retries. We re-read the latest object each attempt and
    // re-apply, so a lost CAS race no longer surfaces as a failed DELETE.
    const MAX_ATTEMPTS: usize = 5;
    let mut current = resource.clone();

    for attempt in 0..MAX_ATTEMPTS {
        let metadata = current.metadata();

        // If already marked for deletion, handle as before
        if metadata.deletion_timestamp.is_some() {
            let has_finalizers = metadata.finalizers.as_ref().is_some_and(|f| !f.is_empty());

            if has_finalizers {
                debug!(
                    "Resource {} already marked for deletion at {:?}, waiting for finalizers to be removed",
                    key, metadata.deletion_timestamp
                );
                info!(
                    "Resource {} has {} finalizers remaining: {:?}",
                    key,
                    metadata.finalizers.as_ref().unwrap().len(),
                    metadata.finalizers.as_ref().unwrap()
                );
                return Ok(true);
            } else {
                // No finalizers left, delete now
                debug!("Resource {} has no finalizers remaining, deleting", key);
                storage.delete(key).await?;
                return Ok(false);
            }
        }

        // Not yet marked for deletion — apply propagation policy finalizers first
        let mut updated_resource = current.clone();
        let meta = updated_resource.metadata_mut();

        // Add propagation policy finalizer if needed
        match propagation_policy {
            Some("Foreground") => {
                let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
                if !finalizers.contains(&"foregroundDeletion".to_string()) {
                    finalizers.push("foregroundDeletion".to_string());
                    info!("Added foregroundDeletion finalizer to {}", key);
                }
            }
            Some("Orphan") => {
                let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
                if !finalizers.contains(&"orphan".to_string()) {
                    finalizers.push("orphan".to_string());
                    info!("Added orphan finalizer to {}", key);
                }
            }
            _ => {
                // Background or unspecified — no extra finalizer
            }
        }

        // Check if the resource has finalizers (including any we just added)
        let has_finalizers = meta.finalizers.as_ref().is_some_and(|f| !f.is_empty());

        if !has_finalizers {
            // No finalizers - delete immediately
            debug!("Resource {} has no finalizers, deleting immediately", key);
            storage.delete(key).await?;
            return Ok(false);
        }

        // Resource has finalizers — set deletionTimestamp and update in storage
        meta.deletion_timestamp = Some(Utc::now());

        info!(
            "Resource {} marked for deletion with finalizers: {:?}",
            key, meta.finalizers
        );

        match storage.update(key, &updated_resource).await {
            Ok(_) => return Ok(true),
            Err(rusternetes_common::Error::Conflict(msg)) if attempt + 1 < MAX_ATTEMPTS => {
                debug!(
                    "Conflict marking {} for deletion (attempt {}), re-reading and retrying: {}",
                    key,
                    attempt + 1,
                    msg
                );
                // Re-read the latest version so the next attempt's CAS uses a
                // fresh resourceVersion (and observes any concurrent changes,
                // including a deletionTimestamp set by another writer).
                current = storage.get(key).await?;
            }
            Err(e) => return Err(e),
        }
    }

    Err(rusternetes_common::Error::Conflict(format!(
        "failed to mark {key} for deletion after {MAX_ATTEMPTS} attempts due to repeated conflicts"
    )))
}

/// Trait for resources that have metadata with finalizers.
/// This allows the handle_delete_with_finalizers function to work with any
/// Kubernetes resource type.
pub trait HasMetadata {
    /// Get an immutable reference to the resource's metadata
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta;

    /// Get a mutable reference to the resource's metadata
    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta;
}

// Implement HasMetadata for common resource types
// Note: This can be extended with a macro if needed for many types

impl HasMetadata for rusternetes_common::resources::Namespace {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Pod {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Deployment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Service {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ConfigMap {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Secret {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceAccount {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicaSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::DaemonSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StatefulSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Job {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CronJob {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolume {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolumeClaim {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StorageClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Ingress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IngressClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::NetworkPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ResourceQuota {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::LimitRange {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodDisruptionBudget {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::HorizontalPodAutoscaler {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Node {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshot {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotContent {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIDriver {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSINode {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIStorageCapacity {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttachment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttributesClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::MutatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicyBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CertificateSigningRequest {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::FlowSchema {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityLevelConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// NOTE: DRA resources (ResourceClaim, ResourceClaimTemplate, DeviceClass, ResourceSlice)
// use a different ObjectMeta type (rusternetes_common::resources::dra::ObjectMeta)
// which is incompatible with rusternetes_common::types::ObjectMeta.
// Therefore, we cannot implement HasMetadata for DRA resources, and they do not support finalizers.

impl HasMetadata for rusternetes_common::resources::Role {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRole {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodTemplate {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ControllerRevision {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceCIDR {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IPAddress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResourceDefinition {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Event {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Lease {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RuntimeClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::EndpointSlice {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Endpoints {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicationController {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResource {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Pod, PodSpec};
    use rusternetes_storage::memory::MemoryStorage;

    fn make_test_pod(name: &str) -> Pod {
        let spec = PodSpec {
            containers: vec![],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.ensure_uid();
        pod.metadata.ensure_creation_timestamp();
        pod
    }

    #[tokio::test]
    async fn test_delete_without_finalizers() {
        let storage = MemoryStorage::new();
        let pod = make_test_pod("test-pod");
        let key = "test/pods/default/test-pod";

        storage.create(key, &pod).await.unwrap();

        let deleted = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();

        assert!(
            !deleted,
            "Resource without finalizers should be deleted immediately"
        );

        let result = storage.get::<Pod>(key).await;
        assert!(result.is_err(), "Resource should be deleted from storage");
    }

    #[tokio::test]
    async fn test_delete_with_finalizers() {
        let storage = MemoryStorage::new();
        let mut pod = make_test_pod("test-pod-finalizers");
        pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);
        let key = "test/pods/default/test-pod-finalizers";

        storage.create(key, &pod).await.unwrap();

        let marked = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();
        assert!(
            marked,
            "Resource with finalizers should be marked for deletion"
        );

        let updated_pod: Pod = storage.get(key).await.unwrap();
        assert!(
            updated_pod.metadata.deletion_timestamp.is_some(),
            "Resource should have deletionTimestamp"
        );
        assert_eq!(
            updated_pod.metadata.finalizers,
            Some(vec!["test.finalizer.io".to_string()]),
            "Finalizers should still be present"
        );

        // Second delete should also return marked (no-op)
        let marked_again = handle_delete_with_finalizers(&storage, key, &updated_pod)
            .await
            .unwrap();
        assert!(marked_again, "Resource should still be marked for deletion");

        storage.delete(key).await.unwrap();
    }

    /// A delete that adds a propagation finalizer (Orphan/Foreground) must
    /// survive an optimistic-concurrency conflict on the finalizer-add update.
    ///
    /// Reproduces `[sig-api-machinery] Garbage collector should orphan pods
    /// created by rc if delete options say so` (garbage_collector.go:407),
    /// which failed with the DELETE call itself returning an error: the etcd
    /// backend does a resourceVersion CAS on `update`, and the RC controller
    /// bumps `rc.status` between the delete handler's read and the
    /// finalizer-add write, so the stale-RV write loses with `Error::Conflict`.
    /// Background delete uses `storage.delete` (no CAS), which is why only the
    /// Orphan/Foreground paths were affected. Upstream performs this as a
    /// GuaranteedUpdate that retries on conflict.
    #[tokio::test]
    async fn orphan_delete_retries_on_conflict() {
        let storage = MemoryStorage::new();
        let rc = make_test_pod("rc-orphan");
        let key = "test/pods/default/rc-orphan";
        storage.create(key, &rc).await.unwrap();

        // Force the first finalizer-add update() to conflict, mimicking a
        // concurrent status bump by the RC controller under the etcd CAS.
        storage.inject_conflicts(1);

        let marked =
            handle_delete_with_finalizers_and_propagation(&storage, key, &rc, Some("Orphan"))
                .await
                .expect("orphan delete must succeed despite a concurrent-update conflict");
        assert!(
            marked,
            "orphan delete must mark the resource for deletion (true)"
        );

        let updated: Pod = storage.get(key).await.unwrap();
        assert!(
            updated.metadata.deletion_timestamp.is_some(),
            "deletionTimestamp must be set after the retried finalizer-add"
        );
        assert_eq!(
            updated.metadata.finalizers,
            Some(vec!["orphan".to_string()]),
            "orphan finalizer must be present after the retried finalizer-add"
        );
    }

    #[tokio::test]
    async fn test_finalizer_removed_then_deleted() {
        let storage = MemoryStorage::new();
        let mut pod = make_test_pod("test-pod-remove-finalizer");
        pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);
        let key = "test/pods/default/test-pod-remove-finalizer";

        storage.create(key, &pod).await.unwrap();

        let marked = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();
        assert!(marked);

        // Simulate controller removing finalizer
        let mut updated_pod: Pod = storage.get(key).await.unwrap();
        updated_pod.metadata.finalizers = None;
        storage.update(key, &updated_pod).await.unwrap();

        let deleted = handle_delete_with_finalizers(&storage, key, &updated_pod)
            .await
            .unwrap();
        assert!(!deleted, "Resource without finalizers should be deleted");

        let result = storage.get::<Pod>(key).await;
        assert!(result.is_err(), "Resource should be deleted from storage");
    }
    /// #1776: a collection delete must tolerate an item that vanished between
    /// the list and the per-item delete.
    ///
    /// The PV/PVC lifecycle spec deletes its PVC by collection, waits for it to
    /// go, then deletes the PV by collection — by which time our own reclaim has
    /// often already removed the volume, and propagating that NotFound failed
    /// the whole request:
    ///   Failed to delete PV "pvc-c697e": persistentvolumes "pv-7783-d0990" not found
    /// Upstream store.go::DeleteCollection ignores NotFound per item.
    #[tokio::test]
    async fn delete_collection_item_tolerates_an_already_deleted_item() {
        let storage = MemoryStorage::new();
        let pod = make_test_pod("vanished");
        let key = "pods/default/vanished";

        // Never stored: this is the state after a concurrent deleter won.
        let outcome = delete_collection_item(&storage, key, &pod)
            .await
            .expect("a concurrently deleted item must not fail the collection delete");

        assert!(
            outcome.is_none(),
            "an already-deleted item reports None so the caller can skip it"
        );
    }

    /// The normal path still reports the deletion, so DeleteCollection's count
    /// stays accurate.
    #[tokio::test]
    async fn delete_collection_item_reports_a_real_deletion() {
        let storage = MemoryStorage::new();
        let pod = make_test_pod("present");
        let key = "pods/default/present";
        storage.create(key, &pod).await.unwrap();

        let outcome = delete_collection_item(&storage, key, &pod)
            .await
            .expect("deleting a present item succeeds");

        assert_eq!(
            outcome,
            Some(true),
            "an item with no finalizers is deleted outright"
        );
        assert!(
            storage.get::<Pod>(key).await.is_err(),
            "and it is really gone"
        );
    }

    /// An item that still has finalizers is reported as pending, not deleted —
    /// the same distinction the single-item path makes.
    #[tokio::test]
    async fn delete_collection_item_reports_a_pending_finalizer() {
        let storage = MemoryStorage::new();
        let mut pod = make_test_pod("held");
        pod.metadata.finalizers = Some(vec!["example.com/hold".to_string()]);
        let key = "pods/default/held";
        storage.create(key, &pod).await.unwrap();

        let outcome = delete_collection_item(&storage, key, &pod)
            .await
            .expect("marking for deletion succeeds");

        assert_eq!(
            outcome,
            Some(false),
            "an item with finalizers is pending, not deleted"
        );
        let stored: Pod = storage.get(key).await.expect("still present");
        assert!(
            stored.metadata.deletion_timestamp.is_some(),
            "and it carries a deletionTimestamp"
        );
    }

    /// Precedence table straight out of upstream `shouldOrphanDependents`
    /// (store.go:883) and `shouldDeleteDependents` (store.go:932).
    #[test]
    fn gc_deletion_finalizers_follows_upstream_precedence() {
        let none: Option<&Vec<String>> = None;
        type Case<'a> = (
            Option<&'a Vec<String>>,
            Option<&'a str>,
            Option<bool>,
            Vec<&'a str>,
        );
        let cases: &[Case] = &[
            // No policy, no finalizers → background, nothing added.
            (none, None, None, vec![]),
            (none, Some("Background"), None, vec![]),
            (none, Some("Foreground"), None, vec!["foregroundDeletion"]),
            (none, Some("Orphan"), None, vec!["orphan"]),
            // The deprecated bool wins over propagationPolicy, both ways.
            (none, Some("Foreground"), Some(true), vec!["orphan"]),
            (none, Some("Orphan"), Some(false), vec![]),
        ];
        for (existing, policy, orphan, expected) in cases {
            let got = gc_deletion_finalizers(*existing, *policy, *orphan);
            assert_eq!(
                got, *expected,
                "policy={policy:?} orphanDependents={orphan:?}"
            );
        }

        // A GC finalizer already on the object decides when no option is set,
        // and unrelated finalizers are always preserved in place.
        let existing = vec![
            "example.com/keep".to_string(),
            "foregroundDeletion".to_string(),
        ];
        assert_eq!(
            gc_deletion_finalizers(Some(&existing), None, None),
            vec!["example.com/keep", "foregroundDeletion"]
        );
        // An explicit policy replaces the GC finalizer that was there.
        assert_eq!(
            gc_deletion_finalizers(Some(&existing), Some("Orphan"), None),
            vec!["example.com/keep", "orphan"]
        );
        assert_eq!(
            gc_deletion_finalizers(Some(&existing), Some("Background"), None),
            vec!["example.com/keep"]
        );
    }

    #[test]
    fn parse_delete_propagation_reads_query_then_body() {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("propagationPolicy".to_string(), "Foreground".to_string());
        assert_eq!(
            parse_delete_propagation(&params, None),
            (Some("Foreground".to_string()), None)
        );

        // Body DeleteOptions when the query says nothing.
        let body = serde_json::json!({
            "propagationPolicy": "Orphan",
            "orphanDependents": true,
        });
        assert_eq!(
            parse_delete_propagation(&HashMap::new(), Some(&body)),
            (Some("Orphan".to_string()), Some(true))
        );

        // Query wins over body.
        assert_eq!(
            parse_delete_propagation(&params, Some(&body)),
            (Some("Foreground".to_string()), Some(true))
        );
    }
}
