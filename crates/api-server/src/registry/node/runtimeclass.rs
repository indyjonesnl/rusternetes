//! RuntimeClass strategy and storage — port of
//! `pkg/registry/node/runtimeclass/strategy.go` and
//! `pkg/registry/node/runtimeclass/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::RuntimeClass;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::runtimeclass::{
    get_warnings_for_runtime_class, validate_runtime_class, validate_runtime_class_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `strategy` (strategy.go:33-40).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<RuntimeClass> for Strategy {
    /// `PrepareForCreate` does nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut RuntimeClass) {}

    fn validate(&self, _ctx: &RequestContext, obj: &RuntimeClass) -> ErrorList {
        validate_runtime_class(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &RuntimeClass) -> Vec<String> {
        get_warnings_for_runtime_class(obj)
    }
}

impl RestUpdateStrategy<RuntimeClass> for Strategy {
    /// `AllowCreateOnUpdate` is true: a PUT may create a RuntimeClass.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// `PrepareForUpdate` does nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut RuntimeClass,
        _old: &RuntimeClass,
    ) {
    }

    /// `ValidateUpdate`: the create validation plus `handler` immutability.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &RuntimeClass,
        old: &RuntimeClass,
    ) -> ErrorList {
        let mut errs = validate_runtime_class(obj);
        errs.extend(validate_runtime_class_update(obj, old));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &RuntimeClass,
        _old: &RuntimeClass,
    ) -> Vec<String> {
        get_warnings_for_runtime_class(obj)
    }

    /// `AllowUnconditionalUpdate` is false: a PUT to an existing
    /// RuntimeClass must carry a resourceVersion.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `strategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<RuntimeClass> for Strategy {}

/// `NewREST` (storage/storage.go:38-60): the RuntimeClass store. node/v1
/// registers no defaulters.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<RuntimeClass, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("node.k8s.io", "runtimeclasses"),
        Arc::new(Strategy),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rc(handler: &str) -> RuntimeClass {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
            "metadata": {"name": "rc"}, "handler": handler
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_matches_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
        assert!(Strategy.validate(&ctx(), &rc("runc")).is_empty());
        let errs = Strategy.validate_update(&ctx(), &rc("crun"), &rc("runc"));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].field, "handler");
    }

    /// `GetWarningsForRuntimeClass` (pkg/api/node/util.go:47-60).
    #[test]
    fn a_deprecated_node_label_warns() {
        let mut obj = rc("runc");
        obj.scheduling = serde_json::from_value(serde_json::json!({
            "nodeSelector": {"beta.kubernetes.io/os": "linux", "kubernetes.io/os": "linux"}
        }))
        .unwrap();
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &obj),
            vec![
                "scheduling.nodeSelector: deprecated since v1.14; use \"kubernetes.io/os\" instead"
                    .to_string()
            ]
        );
    }
}
