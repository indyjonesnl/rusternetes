//! Unit tests moved from `src/handlers/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        extract::{Path, Query, State},
        Extension, Json,
    };
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        resources::{NodeMetrics, PodMetrics},
        List, Result,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod overlay_tests {
    use super::__parent::*;
    use chrono::Utc;
    use rusternetes_api_server::handlers::metrics::*;
    use rusternetes_common::resources::Node;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::BTreeMap;

    /// metrics-server `pkg/storage/node.go:57-62` builds each NodeMetrics with
    /// `Labels: node.Labels`, so a GET carries the node's labels whether the
    /// stored sample has labels or not (the kubelet publishes none).
    #[tokio::test]
    async fn node_metrics_carry_node_labels() {
        let storage = MemoryStorage::new();
        let mut node = Node::new("n1");
        node.metadata.labels = Some(HashMap::from([("zone".to_string(), "a".to_string())]));

        // No stored sample: None (Get answers NotFound).
        assert!(node_metrics(&storage, &node).await.is_none());

        // Kubelet-published sample without labels.
        let stored = NodeMetrics {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("n1"),
            timestamp: Utc::now(),
            window: "30s".to_string(),
            usage: BTreeMap::new(),
        };
        storage
            .create("/registry/metrics.k8s.io/nodes/n1", &stored)
            .await
            .unwrap();
        let m = node_metrics(&storage, &node).await.unwrap();
        assert_eq!(m.metadata.labels, node.metadata.labels);
    }
}
