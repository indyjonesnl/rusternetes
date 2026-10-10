//! CSIStorageCapacity endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the CSIStorageCapacity strategy
//! ([`crate::registry::storage::csistoragecapacity`]) — upstream's
//! `pkg/registry/storage/csistoragecapacity/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::storage::csistoragecapacity;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::CSIStorageCapacity,
    List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The CSIStorageCapacity `RequestScope`: `storage.k8s.io/v1` `CSIStorageCapacity` served as
/// `csistoragecapacities`, backed by `csistoragecapacity.NewREST`.
fn scope(state: &ApiServerState) -> RequestScope<CSIStorageCapacity> {
    RequestScope {
        kind: GroupVersionKind {
            group: "storage.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CSIStorageCapacity".to_string(),
        },
        resource: GroupVersionResource {
            group: "storage.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "csistoragecapacities".to_string(),
        },
        subresource: None,
        store: Box::new(csistoragecapacity::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<CSIStorageCapacity>),
        convert_to_internal: None,
        patch_conversion: None,
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

pub async fn create_csistoragecapacity(
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

pub async fn get_csistoragecapacity(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await
}

pub async fn list_csistoragecapacities(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    // Handle watch requests
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
        return crate::handlers::watch::watch_namespaced::<CSIStorageCapacity>(
            state,
            auth_ctx,
            namespace,
            "csistoragecapacities",
            "storage.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing CSIStorageCapacities in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "csistoragecapacities")
        .with_api_group("storage.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("csistoragecapacities", Some(&namespace));
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut cscs: Vec<CSIStorageCapacity> =
        crate::handlers::list_options::list_items(&*state.storage, &prefix, &params).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut cscs, &params)?;

    let mut list = List::new("CSIStorageCapacityList", "storage.k8s.io/v1", cscs);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(Json(list).into_response())
}

pub async fn list_all_csistoragecapacities(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    // Handle watch requests
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
        return crate::handlers::watch::watch_cluster_scoped::<CSIStorageCapacity>(
            state,
            auth_ctx,
            "csistoragecapacities",
            "storage.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all CSIStorageCapacities across all namespaces");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "csistoragecapacities")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("csistoragecapacities", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut cscs: Vec<CSIStorageCapacity> =
        crate::handlers::list_options::list_items(&*state.storage, &prefix, &params).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut cscs, &params)?;

    let mut list = List::new("CSIStorageCapacityList", "storage.k8s.io/v1", cscs);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(Json(list).into_response())
}

pub async fn update_csistoragecapacity(
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

pub async fn patch_csistoragecapacity(
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

pub async fn delete_csistoragecapacity(
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

pub async fn deletecollection_csistoragecapacities(
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
