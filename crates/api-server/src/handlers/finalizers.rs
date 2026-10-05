use crate::registry::metadata::HasMetadata;
use chrono::Utc;
use rusternetes_common::Result;
use rusternetes_middleware::DeleteOptionsCtx;
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
/// use rusternetes_middleware::DeleteOptionsCtx;
/// use rusternetes_storage::Storage;
/// use tracing::info;
///
/// async fn delete_pod<S: Storage>(storage: &S, key: &str) -> Result<()> {
///     // Get the resource
///     let pod: Pod = storage.get(key).await?;
///
///     // Handle deletion with finalizers. In a handler `opts` comes from the
///     // `Extension<DeleteOptionsCtx>` the middleware decodes off the request;
///     // the default is upstream's default propagation.
///     let opts = DeleteOptionsCtx::default();
///     let marked_for_deletion = handle_delete_with_finalizers(
///         storage,
///         key,
///         &pod,
///         &opts,
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
    opts: &DeleteOptionsCtx,
) -> Result<bool>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    handle_delete_with_finalizers_and_propagation(storage, key, resource, opts).await
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

/// Handle deletion with propagation policy support.
/// When propagation_policy is "Foreground", adds the "foregroundDeletion" finalizer
/// so the garbage collector knows to delete dependents before the owner.
/// When propagation_policy is "Orphan", adds the "orphan" finalizer so dependents
/// are not deleted.
pub async fn handle_delete_with_finalizers_and_propagation<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
    opts: &DeleteOptionsCtx,
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
            // A second DELETE still re-applies the GC finalizers, unless a
            // GRACEFUL deletion is already under way. Upstream:
            // `updateForGracefulDeletionAndFinalizers` (store.go:1062-1077)
            // returns `errAlreadyDeleting` on `pendingGraceful` and only then
            // skips `deletionFinalizersForGarbageCollection` --
            //
            //   // Note that this occurs after checking pendingGraceful, so
            //   // finalizers cannot be updated via DeleteOptions if deletion
            //   // has started.
            //
            // and `pendingGraceful` is false for any kind whose strategy is not
            // a `RESTGracefulDeleteStrategy` (rest/delete.go:101-106) -- which
            // is every kind except Pod. So for a ConfigMap held by a finalizer,
            // a DELETE with a different policy REPLACES the GC finalizer.
            // Returning early here applied the pod rule to everything.
            let graceful_deletion_pending = metadata
                .deletion_grace_period_seconds
                .is_some_and(|g| g > 0);

            let existing_finalizers = metadata.finalizers.clone().unwrap_or_default();
            let recomputed = gc_deletion_finalizers(
                metadata.finalizers.as_ref(),
                opts.propagation_policy.as_deref(),
                opts.orphan_dependents,
            );

            if !graceful_deletion_pending && recomputed != existing_finalizers {
                let mut updated = current.clone();
                let meta = updated.metadata_mut();
                meta.finalizers = if recomputed.is_empty() {
                    None
                } else {
                    Some(recomputed.clone())
                };
                match storage.update(key, &updated).await {
                    Ok(_) => {
                        info!(
                            "Re-applied GC finalizers on already-deleting {}: {:?} -> {:?}",
                            key, existing_finalizers, recomputed
                        );
                        if recomputed.is_empty() {
                            storage.delete(key).await?;
                            return Ok(false);
                        }
                        return Ok(true);
                    }
                    Err(rusternetes_common::Error::Conflict(msg)) if attempt + 1 < MAX_ATTEMPTS => {
                        debug!(
                            "Conflict re-applying GC finalizers on {} (attempt {}): {}",
                            key,
                            attempt + 1,
                            msg
                        );
                        current = storage.get(key).await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

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

        // Recompute the GC finalizers from scratch, which is what upstream
        // does: `deletionFinalizersForGarbageCollection`
        // (registry/generic/registry/store.go:984-997) strips BOTH
        // `orphan` and `foregroundDeletion` and then re-adds only the one that
        // applies. Appending instead -- as this did -- leaves both on an object
        // deleted first with Foreground and then with Orphan, and never honours
        // the deprecated `orphanDependents` bool that upstream lets override
        // everything (store.go:898-901).
        let gc = gc_deletion_finalizers(
            meta.finalizers.as_ref(),
            opts.propagation_policy.as_deref(),
            opts.orphan_dependents,
        );
        if gc.is_empty() {
            meta.finalizers = None;
        } else {
            meta.finalizers = Some(gc);
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

        let deleted = handle_delete_with_finalizers(&storage, key, &pod, &Default::default())
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

        let marked = handle_delete_with_finalizers(&storage, key, &pod, &Default::default())
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
        let marked_again =
            handle_delete_with_finalizers(&storage, key, &updated_pod, &Default::default())
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

        let marked = handle_delete_with_finalizers_and_propagation(
            &storage,
            key,
            &rc,
            &DeleteOptionsCtx {
                propagation_policy: Some("Orphan".to_string()),
                orphan_dependents: None,
            },
        )
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

        let marked = handle_delete_with_finalizers(&storage, key, &pod, &Default::default())
            .await
            .unwrap();
        assert!(marked);

        // Simulate controller removing finalizer
        let mut updated_pod: Pod = storage.get(key).await.unwrap();
        updated_pod.metadata.finalizers = None;
        storage.update(key, &updated_pod).await.unwrap();

        let deleted =
            handle_delete_with_finalizers(&storage, key, &updated_pod, &Default::default())
                .await
                .unwrap();
        assert!(!deleted, "Resource without finalizers should be deleted");

        let result = storage.get::<Pod>(key).await;
        assert!(result.is_err(), "Resource should be deleted from storage");
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
}
