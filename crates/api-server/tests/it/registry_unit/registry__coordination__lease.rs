//! Unit tests moved from `src/registry/coordination/lease.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::Lease;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::lease::validate_lease;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::coordination::lease::*;

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
