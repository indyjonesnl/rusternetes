//! ConfigMap endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the ConfigMap strategy
//! ([`crate::registry::core::configmap`]) — upstream's
//! `pkg/registry/core/configmap/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Reads (get, list,
//! watch) are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::ConfigMap,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// The ConfigMap `RequestScope`: `v1` `ConfigMap` served as `configmaps`,
/// backed by `configmap.NewREST`'s store.
fn scope(state: &ApiServerState) -> RequestScope<ConfigMap> {
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "configmaps".to_string(),
        },
        subresource: None,
        store: crate::registry::core::configmap::new_store(state.storage.clone()),
        apply: Some(crate::ssa::apply_configmap),
        // ConfigMap has no defaulter and no conversion logic.
        convert_to_internal: None,
    }
}

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_configmap(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!(
            "Configmap watch request for namespace {}: rv={:?}, sendInitialEvents={:?}",
            namespace,
            params.get("resourceVersion"),
            params.get("sendInitialEvents"),
        );
        let watch_params = crate::handlers::watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .map(|v| rusternetes_common::query::k8s_query_bool(v)),
            send_initial_events: params
                .get("sendInitialEvents")
                .map(|v| rusternetes_common::query::k8s_query_bool(v)),
        };
        return crate::handlers::watch::watch_namespaced::<ConfigMap>(
            state,
            auth_ctx,
            namespace,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing configmaps in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", Some(&namespace));
    let mut configmaps: Vec<ConfigMap> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Deterministic sort by name so `?continue` chains are stable
    // regardless of underlying storage iteration order.
    configmaps.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// List all configmaps across all namespaces
pub async fn list_all_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .map(|v| rusternetes_common::query::k8s_query_bool(v)),
            send_initial_events: params
                .get("sendInitialEvents")
                .map(|v| rusternetes_common::query::k8s_query_bool(v)),
        };
        return crate::handlers::watch::watch_cluster_scoped::<ConfigMap>(
            state,
            auth_ctx,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all configmaps");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", None);
    let mut configmaps = state.storage.list::<ConfigMap>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Cluster-scoped LIST sorts by (namespace, name) so paginated
    // chains across namespaces are deterministic.
    configmaps.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// Apply `?limit` / `?continue` semantics over an already-sorted slice
/// of ConfigMaps and wrap the response as a `ConfigMapList` with
/// `metadata.continue`, `metadata.remainingItemCount`, and
/// `metadata.resourceVersion`. Mirrors the pattern used by the Pod
/// handler.
async fn paginate_configmaps_response(
    state: &Arc<ApiServerState>,
    configmaps: Vec<ConfigMap>,
    params: &HashMap<String, String>,
    list_kind: &str,
) -> Result<axum::response::Response> {
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &configmaps).await;

    let paginated =
        match rusternetes_common::paginate(configmaps, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
                }
                // Malformed continue token / encoding error -> 400 BadRequest
                // Status object (kind=Status), matching upstream apiserver.
                return Err(rusternetes_common::Error::BadRequest(e.message));
            }
        };

    let mut list = List::new(list_kind, "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(axum::Json(list).into_response())
}

#[cfg(test)]
mod finalizer_drain_put_tests {
    use super::*;
    use crate::state::ApiServerState;
    use rusternetes_storage::build_key;
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

    /// Namespaces override the predicate upstream: a Terminating namespace has
    /// an empty `metadata.finalizers` and a `spec.finalizers` of
    /// `["kubernetes"]`, cleared through /finalize only once the namespace is
    /// drained. `ShouldDeleteNamespaceDuringUpdate`
    /// (pkg/registry/core/namespace/storage/storage.go:258) is
    /// `len(ns.Spec.Finalizers) == 0 && ShouldDeleteDuringUpdate(...)`, so a
    /// plain PUT must NOT remove it.
    #[tokio::test]
    async fn put_does_not_delete_a_namespace_with_spec_finalizers() {
        let state = test_state().await;
        let key = build_key("namespaces", None, "ns-term");
        let seeded = json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {
                "name": "ns-term", "uid": "ns-term-uid",
                "deletionTimestamp": "2026-07-25T00:00:00Z",
            },
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Terminating"},
        });
        let _: serde_json::Value = state.storage.create(&key, &seeded).await.unwrap();

        let deleted = crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
            &*state.storage,
            &key,
            &seeded,
        )
        .await
        .unwrap();
        assert!(
            !deleted,
            "a namespace still holding spec.finalizers must survive"
        );
        assert!(state.storage.get::<serde_json::Value>(&key).await.is_ok());

        // Once /finalize clears spec.finalizers, the same object must go.
        let finalized = json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {
                "name": "ns-term", "uid": "ns-term-uid",
                "deletionTimestamp": "2026-07-25T00:00:00Z",
            },
            "spec": {"finalizers": []},
            "status": {"phase": "Terminating"},
        });
        let deleted = crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
            &*state.storage,
            &key,
            &finalized,
        )
        .await
        .unwrap();
        assert!(deleted, "a drained namespace must be removed");
    }
}
