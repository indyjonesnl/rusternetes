//! Unit tests moved from `src/registry/core/endpoint.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::Endpoints;
    pub use rusternetes_common::validation::endpoints::{
        validate_endpoints_create, validate_endpoints_update,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::metav1::get_warnings_for_ip;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::endpoint::*;

    fn endpoints(ip: &str) -> Endpoints {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"name": "e", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": ip}], "ports": [{"port": 80, "protocol": ""}]}]
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
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
    }

    #[test]
    fn an_empty_protocol_defaults_to_tcp() {
        let mut e = endpoints("10.0.0.1");
        convert_to_internal(&mut e);
        assert_eq!(e.subsets[0].ports.as_ref().unwrap()[0].protocol, "TCP");
    }

    /// strategy.go:88-106: warnings unless the endpoints controller owns it.
    #[test]
    fn non_canonical_ips_warn_unless_controller_managed() {
        let mut e = endpoints("2001:db8:0:0::2");
        let warnings = Strategy.warnings_on_create(&ctx(), &e);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].starts_with("subsets[0].addresses[0].ip: IPv6 address"));

        e.metadata.labels =
            Some([(LABEL_MANAGED_BY.to_string(), CONTROLLER_NAME.to_string())].into());
        assert!(Strategy.warnings_on_create(&ctx(), &e).is_empty());
        assert!(Strategy.warnings_on_update(&ctx(), &e, &e).is_empty());
    }
}
