//! Unit tests moved from `src/handlers/configmap.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Bytes,
        extract::{Path, Query, State},
        http::HeaderMap,
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        admission::{GroupVersionKind, GroupVersionResource},
        authz::{Decision, RequestAttributes},
        resources::ConfigMap,
        List, Result,
    };
    pub use rusternetes_storage::build_prefix;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, info};
}

mod finalizer_drain_put_tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::configmap::*;
    use rusternetes_api_server::state::ApiServerState;
    use rusternetes_storage::{build_key, Storage};
    use serde_json::json;

    async fn test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::TokenManager;
        use rusternetes_common::authz::AlwaysAllowAuthorizer;
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::StorageBackend;

        Arc::new(ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn rusternetes_common::authz::Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        ))
    }

    /// A PUT that removes the last finalizer from an object already pending
    /// deletion must remove the object as part of that request, exactly as
    /// upstream's registry `Update` does via `ShouldDeleteDuringUpdate`
    /// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:565).
    ///
    /// We had this on the PATCH path only, so a controller removing its
    /// finalizer with Update — which is what most controllers do — left the
    /// object Terminating until the GC's next sweep noticed (#1831).
    #[tokio::test]
    async fn put_draining_the_last_finalizer_deletes_the_object() {
        let state = test_state().await;
        let key = build_key("configmaps", Some("default"), "cm-fin");

        let seeded = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "name": "cm-fin", "namespace": "default", "uid": "cm-fin-uid",
                "deletionTimestamp": "2026-07-25T00:00:00Z",
                "finalizers": ["example.com/blocker"],
            },
            "data": {"k": "v"},
        });
        let _: serde_json::Value = state.storage.create(&key, &seeded).await.unwrap();

        // The client PUTs the object back without its finalizer — and, like a
        // typed client building the object locally, without a deletionTimestamp.
        let sent: ConfigMap = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm-fin", "namespace": "default", "uid": "cm-fin-uid"},
            "data": {"k": "v2"},
        }))
        .unwrap();

        let _resp = update(
            State(state.clone()),
            Extension(AuthContext {
                user: rusternetes_common::auth::UserInfo::anonymous(),
            }),
            Path(("default".to_string(), "cm-fin".to_string())),
            Query(HashMap::new()),
            Bytes::from(serde_json::to_vec(&sent).unwrap()),
        )
        .await
        .expect("update must succeed");

        let after: rusternetes_common::Result<ConfigMap> = state.storage.get(&key).await;
        assert!(
            after.is_err(),
            "the object must be gone once the PUT drained its last finalizer"
        );
    }

    /// The same PUT while a finalizer remains must keep the object.
    #[tokio::test]
    async fn put_keeping_a_finalizer_keeps_the_object() {
        let state = test_state().await;
        let key = build_key("configmaps", Some("default"), "cm-keep");

        let seeded = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "name": "cm-keep", "namespace": "default", "uid": "cm-keep-uid",
                "deletionTimestamp": "2026-07-25T00:00:00Z",
                "finalizers": ["a", "b"],
            },
            "data": {"k": "v"},
        });
        let _: serde_json::Value = state.storage.create(&key, &seeded).await.unwrap();

        let sent: ConfigMap = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "name": "cm-keep", "namespace": "default", "uid": "cm-keep-uid",
                "finalizers": ["b"],
            },
            "data": {"k": "v2"},
        }))
        .unwrap();

        let _resp = update(
            State(state.clone()),
            Extension(AuthContext {
                user: rusternetes_common::auth::UserInfo::anonymous(),
            }),
            Path(("default".to_string(), "cm-keep".to_string())),
            Query(HashMap::new()),
            Bytes::from(serde_json::to_vec(&sent).unwrap()),
        )
        .await
        .expect("update must succeed");

        let after: ConfigMap = state
            .storage
            .get(&key)
            .await
            .expect("object with a remaining finalizer must survive");
        assert_eq!(
            after.metadata.finalizers.as_deref(),
            Some(&["b".to_string()][..])
        );
    }
}
