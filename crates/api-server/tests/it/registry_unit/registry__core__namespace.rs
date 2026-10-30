//! Unit tests moved from `src/registry/core/namespace.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::generic::{
        CreateOptions, Deleted, Store, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject,
        ValidateObjectUpdate,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::deletion::{DeleteOptions, Preconditions};
    pub use rusternetes_common::resources::{Namespace, NamespaceSpec, NamespaceStatus};
    pub use rusternetes_common::types::{DeletionPropagation, Phase};
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::namespace::{
        validate_namespace, validate_namespace_finalize_update, validate_namespace_status_update,
        validate_namespace_update,
    };
    pub use rusternetes_common::Result;
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::namespace::*;

    fn ns(json: serde_json::Value) -> Namespace {
        let mut ns: Namespace = serde_json::from_value(json).unwrap();
        convert_to_internal(&mut ns);
        ns
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!FinalizeStrategy.allow_create_on_update());
    }

    /// `TestNamespaceStrategy` (strategy_test.go:32-71).
    #[test]
    fn create_sets_active_and_the_kubernetes_finalizer() {
        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "10"},
            "status": {"phase": "Terminating"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        assert_eq!(n.status.as_ref().unwrap().phase, Some(Phase::Active));
        assert_eq!(spec_finalizers(&n), ["kubernetes"]);
        let errs = Strategy.validate(&ctx(), &n);
        assert!(errs.is_empty(), "{errs:?}");

        // An existing list keeps its order and gains `kubernetes` last.
        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo"},
            "spec": {"finalizers": ["example.com/a"]}
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        assert_eq!(spec_finalizers(&n), ["example.com/a", "kubernetes"]);
    }

    /// `TestNamespaceStrategy`: an update keeps spec.finalizers and status.
    #[test]
    fn update_keeps_finalizers_and_status() {
        let old = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }));
        let mut new = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": []},
            "status": {"phase": "Terminating"}
        }));
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(spec_finalizers(&new), ["kubernetes"]);
        assert_eq!(new.status.as_ref().unwrap().phase, Some(Phase::Active));
    }

    /// `TestNamespaceFinalizeStrategy` (strategy_test.go:117-147).
    #[test]
    fn finalize_changes_finalizers_but_not_status() {
        let old = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["kubernetes", "example.com/org"]},
            "status": {"phase": "Active"}
        }));
        let mut new = ns(serde_json::json!({
            "metadata": {"name": "foo", "resourceVersion": "1"},
            "spec": {"finalizers": ["example.com/foo"]},
            "status": {"phase": "Terminating"}
        }));
        FinalizeStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(spec_finalizers(&new), ["example.com/foo"]);
        assert_eq!(new.status.as_ref().unwrap().phase, Some(Phase::Active));
    }

    /// `Canonicalize` (strategy.go:106-131): a user may not remove or change
    /// the name label.
    #[test]
    fn canonicalize_restores_the_name_label() {
        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        n.metadata.labels = Some(HashMap::from([(
            LABEL_METADATA_NAME.to_string(),
            "bar".to_string(),
        )]));
        RestUpdateStrategy::canonicalize(&Strategy, &mut n);
        assert_eq!(n.metadata.labels.unwrap()[LABEL_METADATA_NAME], "foo");
    }

    /// `TestDeleteNamespaceWithIncompleteFinalizers` and friends
    /// (storage_test.go): the first delete sets the orphan / foreground
    /// finalizers the options ask for.
    #[test]
    fn termination_sets_the_requested_gc_finalizers() {
        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        let options = DeleteOptions {
            propagation_policy: Some(DeletionPropagation::Foreground),
            ..Default::default()
        };
        start_termination(&mut n, &options);
        assert!(n.metadata.deletion_timestamp.is_some());
        assert_eq!(n.status.as_ref().unwrap().phase, Some(Phase::Terminating));
        assert_eq!(
            n.metadata.finalizers,
            Some(vec!["foregroundDeletion".to_string()])
        );

        let mut n = ns(serde_json::json!({
            "metadata": {"name": "foo", "finalizers": ["foregroundDeletion"]}
        }));
        let options = DeleteOptions {
            orphan_dependents: Some(true),
            ..Default::default()
        };
        start_termination(&mut n, &options);
        assert_eq!(n.metadata.finalizers, Some(vec!["orphan".to_string()]));

        let mut n = ns(serde_json::json!({"metadata": {"name": "foo"}}));
        start_termination(&mut n, &DeleteOptions::default());
        assert_eq!(n.metadata.finalizers, None);
    }
}
