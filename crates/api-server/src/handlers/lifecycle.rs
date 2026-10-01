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

/// Reinstate the server-owned metadata a PUT body may not set, taking it from
/// the stored object.
///
/// Port of upstream `BeforeUpdate`
/// (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:123-146):
///
/// ```text
/// // Use the existing UID if none is provided
/// if len(objectMeta.GetUID()) == 0 { objectMeta.SetUID(oldMeta.GetUID()) }
/// // ClusterName is ignored / creationTimestamp is preserved
/// if oldCreationTime := oldMeta.GetCreationTimestamp(); !oldCreationTime.IsZero() {
///     objectMeta.SetCreationTimestamp(oldMeta.GetCreationTimestamp())
/// }
/// if !oldMeta.GetDeletionTimestamp().IsZero() { objectMeta.SetDeletionTimestamp(...) }
/// if oldMeta.GetDeletionGracePeriodSeconds() != nil && objectMeta.GetDeletionGracePeriodSeconds() == nil { ... }
/// ```
///
/// Why this matters (#1605): a client that builds the object locally and PUTs it
/// — exactly what the dynamic client's `Update()` does, and what
/// `[sig-apps] Deployment should run the lifecycle of a Deployment` uses — sends
/// no `uid` and no `creationTimestamp`. Storing those blanks **orphans every
/// child**: a ReplicaSet's `ownerReferences[].uid` then matches no live owner, so
/// the garbage collector deletes the ReplicaSets and their pods while the
/// Deployment's status still describes them. The workload silently loses its
/// pods and `observedGeneration` freezes.
///
/// `generation` is handled separately by [`maybe_increment_generation`], which
/// reinstates the stored value before deciding whether to bump it.
pub fn inherit_server_owned_metadata(new_meta: &mut ObjectMeta, old_meta: &ObjectMeta) {
    // Upstream only fills the UID when the request omits it; a *mismatched* UID
    // is caught later by the common metadata validation, not silently rewritten.
    if new_meta.uid.is_empty() {
        new_meta.uid = old_meta.uid.clone();
    }

    if old_meta.creation_timestamp.is_some() {
        new_meta.creation_timestamp = old_meta.creation_timestamp;
    }

    // A client cannot start or clear a deletion through a plain update.
    if old_meta.deletion_timestamp.is_some() {
        new_meta.deletion_timestamp = old_meta.deletion_timestamp;
    }
    if old_meta.deletion_grace_period_seconds.is_some()
        && new_meta.deletion_grace_period_seconds.is_none()
    {
        new_meta.deletion_grace_period_seconds = old_meta.deletion_grace_period_seconds;
    }
}

/// Read-modify-write PUT that applies [`inherit_server_owned_metadata`] against
/// the stored object, for the handlers that would otherwise persist the
/// client's body blind.
///
/// Upstream never has this problem because there is exactly one update path:
/// `Store.Update`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go)
/// fetches the current object inside a `GuaranteedUpdate` and hands both to
/// `rest.BeforeUpdate` (registry/rest/update.go:131-146) before anything is
/// written. Every resource gets the behaviour by construction.
///
/// Rusternetes has one handler per resource, so the rule has to be re-applied
/// per handler — and it repeatedly was not (#1605, #1788, #1793, #1795). This
/// helper exists so a handler that needs the stored object *only* for its
/// metadata does not have to open-code the read, the inherit and the write, and
/// so the next resource added gets it by calling one function.
///
/// **Missing-object behaviour is deliberately unchanged.** When the object is
/// absent the write is still attempted, so a caller that upserts on
/// `NotFound` keeps upserting and a caller that propagates `NotFound` keeps
/// propagating. Choosing 404-vs-create is `AllowCreateOnUpdate` upstream and is
/// a per-resource decision, not something a shared helper may make silently.
pub async fn update_inheriting_server_owned_metadata<S, T>(
    storage: &S,
    key: &str,
    new: &mut T,
) -> rusternetes_common::Result<T>
where
    S: rusternetes_storage::Storage,
    T: crate::handlers::finalizers::HasMetadata
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync,
{
    match storage.get::<T>(key).await {
        Ok(stored) => {
            let stored_meta = stored.metadata().clone();
            inherit_server_owned_metadata(new.metadata_mut(), &stored_meta);
        }
        Err(rusternetes_common::Error::NotFound(_)) => {}
        Err(e) => return Err(e),
    }
    storage.update(key, new).await
}

/// `serde_json::Value` form of [`inherit_server_owned_metadata`], for the
/// handlers that persist a raw document rather than a typed struct (CRDs, and
/// anything else stored as an untyped object).
///
/// Same port, same upstream source — `BeforeUpdate`
/// (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:131-146). Kept
/// beside the typed version so the two cannot drift: upstream has one rule,
/// and a second copy of it living inside a handler is how the typed path came
/// to be fixed in five places and missed in fifty (#1793).
///
/// A missing or non-object `metadata` on either side is left alone rather than
/// synthesised — this function reinstates fields, it does not repair shape.
pub fn inherit_server_owned_metadata_json(
    new_obj: &mut serde_json::Value,
    old_obj: &serde_json::Value,
) {
    let Some(old_meta) = old_obj.get("metadata").and_then(|m| m.as_object()) else {
        return;
    };
    let Some(new_meta) = new_obj.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };

    // Use the existing UID if none is provided.
    let uid_absent = new_meta
        .get("uid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .is_empty();
    if uid_absent {
        if let Some(uid) = old_meta.get("uid") {
            new_meta.insert("uid".to_string(), uid.clone());
        }
    }

    // Ignore changes to the creation timestamp, and never let an update start
    // or clear a deletion.
    for field in ["creationTimestamp", "deletionTimestamp"] {
        if let Some(v) = old_meta.get(field) {
            if !v.is_null() {
                new_meta.insert(field.to_string(), v.clone());
            }
        }
    }
    if let Some(v) = old_meta.get("deletionGracePeriodSeconds") {
        if !v.is_null()
            && new_meta
                .get("deletionGracePeriodSeconds")
                .is_none_or(|n| n.is_null())
        {
            new_meta.insert("deletionGracePeriodSeconds".to_string(), v.clone());
        }
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

    /// #1605: a PUT that omits uid/creationTimestamp must not blank them —
    /// blanking the uid orphans every child and the GC deletes the workload's
    /// ReplicaSets and pods.
    #[test]
    fn inherit_server_owned_metadata_fills_uid_and_creation_timestamp() {
        let created = chrono::Utc::now();
        let old = ObjectMeta {
            uid: "0ca12357-84c2-45e4-9a08-b1891e1931da".to_string(),
            creation_timestamp: Some(created),
            ..Default::default()
        };
        // What the dynamic client's Update() sends: a locally built object.
        let mut new_meta = ObjectMeta {
            uid: String::new(),
            creation_timestamp: None,
            ..Default::default()
        };

        inherit_server_owned_metadata(&mut new_meta, &old);

        assert_eq!(
            new_meta.uid, old.uid,
            "a PUT omitting the uid must inherit the stored one"
        );
        assert_eq!(
            new_meta.creation_timestamp,
            Some(created),
            "a PUT omitting creationTimestamp must inherit the stored one"
        );
    }

    #[test]
    fn inherit_server_owned_metadata_keeps_a_client_supplied_uid() {
        let old = ObjectMeta {
            uid: "stored-uid".to_string(),
            ..Default::default()
        };
        let mut new_meta = ObjectMeta {
            uid: "client-uid".to_string(),
            ..Default::default()
        };

        inherit_server_owned_metadata(&mut new_meta, &old);

        assert_eq!(
            new_meta.uid, "client-uid",
            "a supplied uid is left for the metadata validation to reject, not silently rewritten"
        );
    }

    #[test]
    fn inherit_server_owned_metadata_preserves_a_pending_deletion() {
        let deleted_at = chrono::Utc::now();
        let old = ObjectMeta {
            uid: "u".to_string(),
            deletion_timestamp: Some(deleted_at),
            deletion_grace_period_seconds: Some(30),
            ..Default::default()
        };
        let mut new_meta = ObjectMeta {
            uid: "u".to_string(),
            ..Default::default()
        };

        inherit_server_owned_metadata(&mut new_meta, &old);

        assert_eq!(
            new_meta.deletion_timestamp,
            Some(deleted_at),
            "an update must not clear a pending deletion"
        );
        assert_eq!(new_meta.deletion_grace_period_seconds, Some(30));
    }

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
