//! Unit tests moved from `src/registry/core/node.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::resources::Node;
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::ipaddress::get_warnings_for_cidr;
    pub use rusternetes_common::validation::node::{validate_node, validate_node_update};
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::node::*;
    use rusternetes_common::resources::NodeStatus;

    fn node(status: serde_json::Value) -> Node {
        let mut node: Node = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "n", "resourceVersion": "1"},
            "spec": {"podCIDRs": ["10.0.0.0/24"]},
            "status": status
        }))
        .unwrap();
        convert_to_internal(&mut node);
        node
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
        assert!(!StatusStrategy.namespace_scoped());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `SetDefaults_NodeStatus` (v1/defaults.go:346-354).
    #[test]
    fn allocatable_defaults_to_capacity() {
        let n = node(serde_json::json!({"capacity": {"cpu": "2"}}));
        assert_eq!(
            n.status.unwrap().allocatable,
            Some([("cpu".to_string(), "2".to_string())].into())
        );
    }

    /// `TestNodeStrategy` (strategy_test.go): create keeps status, but not
    /// `status.config` or, with its gate off, `status.declaredFeatures`.
    #[test]
    fn create_keeps_status_but_drops_disabled_fields() {
        let mut n = node(serde_json::json!({
            "capacity": {"cpu": "2"},
            "config": {"error": "x"},
            "declaredFeatures": ["A"]
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        let status = n.status.as_ref().unwrap();
        assert_eq!(status.capacity.as_ref().unwrap()["cpu"], "2");
        assert!(status.config.is_none());
        assert!(status.declared_features.is_none());
        let errs = Strategy.validate(&ctx(), &n);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn update_keeps_status_and_status_update_keeps_spec() {
        let old = node(serde_json::json!({"capacity": {"cpu": "2"}}));
        let mut new = node(serde_json::json!({"capacity": {"cpu": "9"}}));
        new.spec.as_mut().unwrap().unschedulable = Some(true);
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(
            new.status.as_ref().unwrap().capacity,
            old.status.as_ref().unwrap().capacity
        );
        assert_eq!(new.spec.as_ref().unwrap().unschedulable, Some(true));

        let mut status =
            node(serde_json::json!({"capacity": {"cpu": "9"}, "config": {"error": "x"}}));
        status.spec.as_mut().unwrap().unschedulable = Some(true);
        StatusStrategy.prepare_for_update(&ctx(), &mut status, &old);
        assert_eq!(status.spec.as_ref().unwrap().unschedulable, None);
        let s: &NodeStatus = status.status.as_ref().unwrap();
        assert_eq!(s.capacity.as_ref().unwrap()["cpu"], "9");
        assert!(s.config.is_none(), "config was not in use");
    }

    /// `nodeWarnings` (strategy.go:306-311) with `GetWarningsForCIDR`.
    #[test]
    fn an_ambiguous_pod_cidr_warns() {
        let mut n = node(serde_json::json!({}));
        n.spec.as_mut().unwrap().pod_cidrs = Some(vec!["10.0.0.8/24".into()]);
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &n),
            vec![
                r#"spec.podCIDRs[0]: CIDR value "10.0.0.8/24" is ambiguous in this context (should be "10.0.0.0/24" or "10.0.0.8/32"?)"#
            ]
        );
    }
}
