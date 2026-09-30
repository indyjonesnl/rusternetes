//! Lease strategy and storage — port of
//! `pkg/registry/coordination/lease/strategy.go` and
//! `pkg/registry/coordination/lease/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::Lease;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::lease::validate_lease;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `CoordinatedLeaderElection` is beta and off by default in 1.35
/// (pkg/features/kube_features.go).
const COORDINATED_LEADER_ELECTION: bool = false;

/// `leaseStrategy` (strategy.go:33-39).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Lease> for Strategy {
    /// `PrepareForCreate` (strategy.go:47-55): with CoordinatedLeaderElection
    /// off, `strategy` and `preferredHolder` are dropped.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Lease) {
        if !COORDINATED_LEADER_ELECTION {
            if let Some(spec) = obj.spec.as_mut() {
                spec.strategy = None;
                spec.preferred_holder = None;
            }
        }
    }

    /// `Validate`: `ValidateLease`.
    fn validate(&self, _ctx: &RequestContext, obj: &Lease) -> ErrorList {
        validate_lease(obj)
    }
}

impl RestUpdateStrategy<Lease> for Strategy {
    /// `AllowCreateOnUpdate` is true: a PUT may create a Lease.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// `PrepareForUpdate` (strategy.go:58-70): with the gate off, a field may
    /// be kept only if the stored Lease already has it.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Lease, old: &Lease) {
        if COORDINATED_LEADER_ELECTION {
            return;
        }
        let old_spec = old.spec.as_ref();
        if let Some(spec) = obj.spec.as_mut() {
            if old_spec.is_none_or(|s| s.strategy.is_none()) {
                spec.strategy = None;
            }
            if old_spec.is_none_or(|s| s.preferred_holder.is_none()) {
                spec.preferred_holder = None;
            }
        }
    }

    /// `ValidateUpdate`: `ValidateLeaseUpdate` re-runs `ValidateLeaseSpec` on
    /// the new object.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Lease, _old: &Lease) -> ErrorList {
        validate_lease(obj)
    }

    /// `AllowUnconditionalUpdate` is false: a PUT to an existing Lease must
    /// carry a resourceVersion.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `leaseStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<Lease> for Strategy {}

/// `NewREST` (storage/storage.go:36-57): the Lease store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Lease, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("coordination.k8s.io", "leases"),
        Arc::new(Strategy),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(spec: serde_json::Value) -> Lease {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
            "metadata": {"name": "l", "namespace": "default"}, "spec": spec
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
    }

    /// `TestLeaseStrategy`-style check of the CoordinatedLeaderElection-off
    /// drop (strategy.go:47-70).
    #[test]
    fn gated_fields_are_dropped_unless_already_stored() {
        let gated = serde_json::json!({
            "holderIdentity": "h", "strategy": "OldestEmulationVersion",
            "preferredHolder": "p"
        });
        let mut created = lease(gated.clone());
        Strategy.prepare_for_create(&ctx(), &mut created);
        let spec = created.spec.as_ref().unwrap();
        assert!(spec.strategy.is_none() && spec.preferred_holder.is_none());

        let old = lease(gated.clone());
        let mut kept = lease(gated.clone());
        Strategy.prepare_for_update(&ctx(), &mut kept, &old);
        assert_eq!(
            kept.spec.as_ref().unwrap().strategy.as_deref(),
            Some("OldestEmulationVersion")
        );

        let mut dropped = lease(gated);
        Strategy.prepare_for_update(&ctx(), &mut dropped, &lease(serde_json::json!({})));
        assert!(dropped.spec.as_ref().unwrap().strategy.is_none());
    }
}
