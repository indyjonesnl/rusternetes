//! Unit tests moved from `src/handlers/custom_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use axum::{
        extract::{Path, Query, State},
        Extension, Json,
    };
    pub use chrono::Utc;
    pub use rusternetes_api_server::handlers::metric_selectors::{
        decode_list_options, decode_metric_label_selector, metric_not_found, metric_selector_echo,
        unknown_error,
    };
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        label_selector::LabelSelector,
        resources::custom_metrics::{
            MetricIdentifier, MetricSelector, MetricValue, MetricValueList, ObjectReference,
        },
        Result,
    };
    pub use rusternetes_discovery::deferred_restmapper::{
        DeferredRestMapper, DiscoveryClient, MemCacheClient, RESET_PERIOD,
    };
    pub use rusternetes_discovery::restmapper::{
        is_builtin_group, served_group_resources, ApiGroupResources, Gvr, MapperError,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, info, warn};
}

mod served_mapper_tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::custom_metrics::*;

    fn crd() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com", "scope": "Cluster",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "versions": [{"name": "v1", "served": true}]
            },
            "status": {"conditions": [{"type": "Established", "status": "True"}]}
        })
    }

    /// #3595: the mapper is built once and kept (memcache.go:209-217 `Fresh`
    /// is `cacheValid`); a CRD established after the cache was populated is
    /// not seen by a miss, only after the periodic `Reset`
    /// (controllermanager.go:516-519), instead of re-listing the CRDs on
    /// every request.
    #[tokio::test]
    async fn served_mapper_is_cached_until_reset() {
        let storage = Arc::new(StorageBackend::new_memory());
        let mapper = new_served_rest_mapper(storage.clone());
        let widget = Gvr::new("example.com", "", "widgets");
        // The first lookup populates the cache (a miss on a cold cache).
        assert!(mapper.resource_for(&widget).await.is_err());

        storage
            .create(
                &build_key("customresourcedefinitions", None, "widgets.example.com"),
                &crd(),
            )
            .await
            .unwrap();
        // Warm cache: not re-discovered on a miss.
        assert!(mapper.resource_for(&widget).await.is_err());
        mapper.reset().await;
        assert_eq!(
            mapper.resource_for(&widget).await.unwrap().resource,
            "widgets"
        );
    }
}
