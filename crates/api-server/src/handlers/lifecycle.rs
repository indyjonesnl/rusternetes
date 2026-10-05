//! Resource lifecycle helpers for generation tracking and optimistic concurrency control.
//!
//! These helpers implement the Kubernetes conformance behaviors:
//! 1. `metadata.generation` tracking: incremented when spec changes, not on status-only updates
//! 2. `metadata.resourceVersion` conflict detection: 409 Conflict on stale updates
//! 3. `spec.selector` immutability: 422 Invalid on selector changes for apps/v1 workloads

use rusternetes_common::types::ObjectMeta;

/// Increment generation if spec has changed (by comparing old vs new JSON, ignoring metadata and status).
///
/// This should be called during resource updates (PUT), but NOT during status-only updates.
///
/// The comparison is performed after normalising empty `{}` objects to
/// absent on both sides (via `validation::pod::strip_empty_objects`) so a
/// no-op Go round-trip — which always emits `"resources":{}` on every
/// container because Go's `omitempty` does not detect zero-valued struct
/// values — does NOT trip a false-positive generation bump. This mirrors
/// upstream's `apiequality.Semantic.DeepEqual` semantics used by the same
/// strategy hook in `pkg/registry/core/pod/strategy.go::PrepareForUpdate`.
pub fn maybe_increment_generation(
    old_json: &serde_json::Value,
    new_json: &serde_json::Value,
    metadata: &mut ObjectMeta,
) {
    let mut old_spec = old_json.clone();
    let mut new_spec = new_json.clone();
    if let Some(obj) = old_spec.as_object_mut() {
        obj.remove("metadata");
        obj.remove("status");
    }
    if let Some(obj) = new_spec.as_object_mut() {
        obj.remove("metadata");
        obj.remove("status");
    }
    rusternetes_common::validation::pod::strip_empty_objects(&mut old_spec);
    rusternetes_common::validation::pod::strip_empty_objects(&mut new_spec);

    // The sequence is owned by the stored object, never by the request body.
    // Upstream `BeforeUpdate` unconditionally reinstates the stored generation
    // before the strategy hook can bump it
    // (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:127 —
    // `objectMeta.SetGeneration(oldMeta.GetGeneration())`), so a client that
    // PUTs a locally-built object with no generation — what the dynamic
    // client's `Update()` sends — cannot reset the counter to 1.
    let stored = old_json
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(|g| g.as_i64())
        .unwrap_or(0);
    metadata.generation = Some(stored);

    if old_spec != new_spec {
        metadata.generation = Some(stored + 1);
    }
}

// ---------------------------------------------------------------------------
// AllowCreateOnUpdate
// ---------------------------------------------------------------------------

/// Whether a PUT to a name that does not exist creates the object.
///
/// Upstream this is a per-strategy method, `AllowCreateOnUpdate()`, consulted
/// in exactly one place — `Store.Update`
/// (`registry/generic/registry/store.go:638` and `:646-650`):
///
/// ```go
/// if existingResourceVersion == 0 {
///     if !e.UpdateStrategy.AllowCreateOnUpdate() && !forceAllowCreate {
///         return nil, nil, apierrors.NewNotFound(qualifiedResource, name)
///     }
/// }
/// ```
///
/// It returns `true` for ten resources in the whole tree, listed below with
/// the strategy file that opts in. Everything else — ConfigMap, Secret, Pod,
/// Deployment, Service, … — answers 404.
///
/// Rusternetes had it the other way round: thirty update handlers carried their
/// own `Err(NotFound) => storage.create(...)` fallback, so a PUT to an absent
/// object created it with server-assigned metadata the client never asked for
/// (#1905).
///
/// `forceAllowCreate` is upstream's server-side-apply escape hatch; there is no
/// caller for it here yet, so it is not modelled.
pub fn allow_create_on_update(group: &str, resource: &str) -> bool {
    matches!(
        (group, resource),
        // pkg/registry/coordination/lease/strategy.go:84
        ("coordination.k8s.io", "leases")
            // pkg/registry/coordination/leasecandidate/strategy.go
            | ("coordination.k8s.io", "leasecandidates")
            // pkg/registry/rbac/{role,rolebinding,clusterrole,clusterrolebinding}/strategy.go
            | ("rbac.authorization.k8s.io", "roles")
            | ("rbac.authorization.k8s.io", "rolebindings")
            | ("rbac.authorization.k8s.io", "clusterroles")
            | ("rbac.authorization.k8s.io", "clusterrolebindings")
            // pkg/registry/core/limitrange/strategy.go:68
            | ("", "limitranges")
            // pkg/registry/core/event/strategy.go:74 -- both the core and the
            // events.k8s.io endpoint reach the same registry.
            | ("", "events")
            | ("events.k8s.io", "events")
            // pkg/registry/core/endpoint/strategy.go:70
            | ("", "endpoints")
            // pkg/registry/core/service/strategy.go (`svcStrategy`)
            | ("", "services")
            // pkg/registry/node/runtimeclass/strategy.go
            | ("node.k8s.io", "runtimeclasses")
    )
}

/// The `existingResourceVersion == 0` gate from `Store.Update`: answer
/// `NotFound` for a PUT to an object that does not exist, unless the resource
/// is one of the ten in [`allow_create_on_update`].
///
/// Call it right after authorization and **before validation**, which is where
/// upstream answers: the check sits at the top of `GuaranteedUpdate`'s
/// `tryUpdate`, ahead of `BeforeCreate`/`BeforeUpdate` and every validator
/// (store.go:646-700). A handler that validates first would answer 422 for a
/// missing object where upstream answers 404.
///
/// The message matches `NewNotFound(qualifiedResource, name)` —
/// `{resource} "{name}" not found` — which `Error::NotFound` renders into a
/// `Status` with matching `details`.
pub async fn reject_create_on_update<S>(
    storage: &S,
    key: &str,
    group: &str,
    resource: &str,
    name: &str,
) -> rusternetes_common::Result<()>
where
    S: rusternetes_storage::Storage,
{
    if allow_create_on_update(group, resource) {
        return Ok(());
    }
    match storage.get::<serde_json::Value>(key).await {
        Ok(_) => Ok(()),
        Err(rusternetes_common::Error::NotFound(_)) => Err(rusternetes_common::Error::NotFound(
            format!("{resource} \"{name}\" not found"),
        )),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_maybe_increment_generation_spec_changed() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 1},
            "status": {"ready": true}
        });
        let new = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 3},
            "status": {"ready": true}
        });
        let mut meta = ObjectMeta {
            generation: Some(1),
            ..Default::default()
        };
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(2));
    }

    #[test]
    fn test_maybe_increment_generation_no_spec_change() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 1},
            "status": {"ready": false}
        });
        let new = serde_json::json!({
            "metadata": {"name": "test-changed", "generation": 2},
            "spec": {"replicas": 1},
            "status": {"ready": true}
        });
        let mut meta = ObjectMeta {
            generation: Some(1),
            ..Default::default()
        };
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(1));
    }

    /// Upstream `registry/rest/update.go:127` does
    /// `objectMeta.SetGeneration(oldMeta.GetGeneration())` *before* the
    /// per-resource `PrepareForUpdate` hook runs, so whatever generation a
    /// client happens to send is always discarded in favour of the stored one.
    /// A dynamic-client `Update()` built from a locally-constructed object
    /// sends no generation at all; basing the bump on that value restarts the
    /// sequence at 1 and leaves `status.observedGeneration` ahead of
    /// `metadata.generation` — seen in conformance
    /// "[sig-apps] Deployment should run the lifecycle of a Deployment".
    #[test]
    fn generation_is_based_on_the_stored_object_not_the_request_body() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 2},
            "spec": {"replicas": 1},
        });
        let new = serde_json::json!({
            "metadata": {"name": "test"},
            "spec": {"replicas": 3},
        });
        // The incoming object carries no generation, exactly as a
        // locally-built object PUT through the dynamic client would.
        let mut meta = ObjectMeta::default();
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(3));
    }

    #[test]
    fn generation_is_restored_from_the_stored_object_when_the_spec_is_unchanged() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 7},
            "spec": {"replicas": 1},
        });
        let new = serde_json::json!({
            "metadata": {"name": "test"},
            "spec": {"replicas": 1},
        });
        let mut meta = ObjectMeta::default();
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(7));
    }

    #[test]
    fn a_client_supplied_generation_cannot_advance_the_sequence() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 2},
            "spec": {"replicas": 1},
        });
        let new = serde_json::json!({
            "metadata": {"name": "test", "generation": 99},
            "spec": {"replicas": 3},
        });
        let mut meta = ObjectMeta {
            generation: Some(99),
            ..Default::default()
        };
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(3));
    }
}
