//! Unit tests moved from `src/registry/scheduling/priorityclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::generic::{
        CreateOptions, Deleted, Store, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::rest::{
        DeletedCollection, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo,
        ValidateObject, ValidateObjectUpdate,
    };
    pub use rusternetes_common::deletion::DeleteOptions;
    pub use rusternetes_common::resources::PriorityClass;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::priorityclass::{
        validate_priority_class, validate_priority_class_update,
    };
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::scheduling::priorityclass::*;

    fn pc(name: &str, value: i32) -> PriorityClass {
        let mut pc: PriorityClass = serde_json::from_value(serde_json::json!({
            "apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
            "metadata": {"name": name}, "value": value
        }))
        .unwrap();
        convert_to_internal(&mut pc);
        pc
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    /// `TestPriorityClassStrategy` (strategy_test.go).
    #[test]
    fn strategy_matches_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());

        let mut created = pc("valid-class", 10);
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(created.metadata.generation, Some(1));
        assert!(Strategy.validate(&ctx(), &created).is_empty());

        let mut changed = created.clone();
        changed.value = 11;
        Strategy.prepare_for_update(&ctx(), &mut changed, &created);
        assert!(!Strategy
            .validate_update(&ctx(), &changed, &created)
            .is_empty());
    }

    /// `SetDefaults_PriorityClass`: preemptionPolicy defaults to
    /// PreemptLowerPriority and an explicit value is kept.
    #[test]
    fn preemption_policy_defaults_to_preempt_lower_priority() {
        assert_eq!(
            pc("a", 1).preemption_policy.as_deref(),
            Some("PreemptLowerPriority")
        );
        let mut never = pc("a", 1);
        never.preemption_policy = Some("Never".into());
        convert_to_internal(&mut never);
        assert_eq!(never.preemption_policy.as_deref(), Some("Never"));
    }
}
