//! Unit tests moved from `src/registry/node/runtimeclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::RuntimeClass;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::runtimeclass::{
        get_warnings_for_runtime_class, validate_runtime_class, validate_runtime_class_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::node::runtimeclass::*;

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
