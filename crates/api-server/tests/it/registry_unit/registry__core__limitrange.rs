//! Unit tests moved from `src/registry/core/limitrange.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::LimitRange;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::limitrange::validate_limit_range;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::limitrange::*;

    fn limit_range() -> LimitRange {
        let mut lr: LimitRange = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "LimitRange",
            "metadata": {"name": "l", "namespace": "default", "resourceVersion": "1"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "min": {"memory": "64Mi"}},
                {"type": "Pod", "max": {"cpu": "4"}}
            ]}
        }))
        .unwrap();
        convert_to_internal(&mut lr);
        lr
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
    }

    /// `SetDefaults_LimitRangeItem`: a Container item's default limit comes
    /// from `max`, its default request from the default limit and then `min`.
    /// A Pod item is left alone.
    #[test]
    fn container_items_are_defaulted() {
        let lr = limit_range();
        let container = &lr.spec.limits[0];
        assert_eq!(container.default.as_ref().unwrap()["cpu"], "2");
        let request = container.default_request.as_ref().unwrap();
        assert_eq!(request["cpu"], "2");
        assert_eq!(request["memory"], "64Mi");
        let pod = &lr.spec.limits[1];
        assert!(pod.default.is_none() && pod.default_request.is_none());
    }

    /// An explicit default is not overwritten by `max`, and the default
    /// request follows it. Empty maps stay unset (`omitempty`).
    #[test]
    fn an_explicit_default_is_kept() {
        let mut lr: LimitRange = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "l"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "default": {"cpu": "1"}},
                {"type": "Container"}
            ]}
        }))
        .unwrap();
        convert_to_internal(&mut lr);
        let item = &lr.spec.limits[0];
        assert_eq!(item.default.as_ref().unwrap()["cpu"], "1");
        assert_eq!(item.default_request.as_ref().unwrap()["cpu"], "1");
        let bare = &lr.spec.limits[1];
        assert!(bare.default.is_none() && bare.default_request.is_none());
    }

    #[test]
    fn prepare_for_create_names_an_unnamed_limit_range() {
        let mut lr = limit_range();
        lr.metadata.name.clear();
        Strategy.prepare_for_create(&ctx(), &mut lr);
        assert!(uuid::Uuid::parse_str(&lr.metadata.name).is_ok());
        let errs = Strategy.validate(&ctx(), &lr);
        assert!(errs.is_empty(), "{errs:?}");
    }
}
