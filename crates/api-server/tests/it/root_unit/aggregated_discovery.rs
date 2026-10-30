//! Unit tests moved from `src/aggregated_discovery.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::apiservice_registration::{
        DiscoveryFreshness, VersionDiscovery,
    };
    pub use rusternetes_api_server::handlers::aggregator::{
        build_aggregator_client, resolve_aggregator_target_with_storage,
    };
    pub use rusternetes_storage::Storage;
    pub use serde_json::{json, Map, Value};
    pub use std::collections::BTreeMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::aggregated_discovery::*;

    #[test]
    fn content_type_gvk_matches_v2_and_v2beta1_only() {
        assert!(is_aggregated_discovery_content_type(ACCEPT_V2));
        assert!(is_aggregated_discovery_content_type(ACCEPT_V2_BETA1));
        // TestLegacyFallback: "incorrect content type" and plain JSON.
        assert!(!is_aggregated_discovery_content_type(
            "application/json;g=apidiscovery.k8s.io;v=v1alpha1;as=APIGroupDiscoveryList"
        ));
        assert!(!is_aggregated_discovery_content_type("application/json"));
        assert!(!is_aggregated_discovery_content_type(""));
    }

    /// installer.go ConvertGroupVersionIntoToDiscovery, the v2alpha1 case from
    /// TestLegacyFallbackNoCache.
    #[test]
    fn legacy_conversion_defaults_response_kinds_and_creates_parents() {
        let list = vec![
            json!({"name": "parent-without-kind"}),
            json!({"name": "missing-parent/subresource-without-parent", "kind": "SubresourceWithoutParent"}),
            json!({"name": "parent-without-kind/subresource", "kind": "Subresource"}),
            json!({"name": "parent-without-kind/subresource-without-kind"}),
        ];
        let out = convert_group_version_into_to_discovery(&list).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["resource"], "parent-without-kind");
        assert_eq!(out[0]["scope"], "Cluster");
        assert_eq!(out[0]["responseKind"], gvk_value("", "", ""));
        let subs = out[0]["subresources"].as_array().unwrap();
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0]["subresource"], "subresource");
        assert_eq!(subs[0]["responseKind"]["kind"], "Subresource");
        assert_eq!(subs[1]["subresource"], "subresource-without-kind");
        assert_eq!(subs[1]["responseKind"], gvk_value("", "", ""));
        assert_eq!(out[1]["resource"], "missing-parent");
        assert_eq!(
            out[1]["subresources"][0]["responseKind"]["kind"],
            "SubresourceWithoutParent"
        );
    }

    #[test]
    fn legacy_conversion_rejects_scope_mismatch() {
        let list = vec![
            json!({"name": "foos", "namespaced": true}),
            json!({"name": "foos/status", "namespaced": false}),
        ];
        let err = convert_group_version_into_to_discovery(&list).unwrap_err();
        assert!(err.contains("scope do not match"), "{err}");
    }

    #[test]
    fn parse_defaults_nil_response_kinds() {
        let body = json!({"items": [{"metadata": {"name": "g"}, "versions": [{
            "version": "v1",
            "resources": [{"resource": "r", "responseKind": null,
                "subresources": [{"subresource": "s"}]}]
        }]}]});
        let m = parse_discovery_list(body.to_string().as_bytes()).unwrap();
        let v = &m[&("g".to_string(), "v1".to_string())];
        assert_eq!(v.resources[0]["responseKind"], json!({}));
        assert_eq!(v.resources[0]["subresources"][0]["responseKind"], json!({}));
    }

    #[test]
    fn legacy_paths() {
        assert_eq!(legacy_path("", "v1"), "/api/v1");
        assert_eq!(legacy_path("g.io", "v1"), "/apis/g.io/v1");
    }
}
