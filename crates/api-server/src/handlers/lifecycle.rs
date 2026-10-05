//! Resource lifecycle helpers for generation tracking and optimistic concurrency control.
//!
//! These helpers implement the Kubernetes conformance behaviors:
//! 1. `metadata.generation` tracking: incremented when spec changes, not on status-only updates
//! 2. `metadata.resourceVersion` conflict detection: 409 Conflict on stale updates
//! 3. `spec.selector` immutability: 422 Invalid on selector changes for apps/v1 workloads

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
