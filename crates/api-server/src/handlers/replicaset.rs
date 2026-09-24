//! ReplicaSet endpoints.
//!
//! Writes, and the `/status` and `/scale` subresources, go through the
//! generic endpoint handlers and [`crate::registry::generic::Store`] with the
//! ReplicaSet strategies ([`crate::registry::apps::replicaset`]) — upstream's
//! `pkg/registry/apps/replicaset/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::apps::replicaset;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::{ReplicaSet, Scale},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The ReplicaSet `RequestScope`: `apps/v1` `ReplicaSet` served as
/// `replicasets` (or its `/status`), backed by `replicaset.NewREST`'s stores.
fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<ReplicaSet> {
    let store = match subresource {
        Some(_) => replicaset::new_status_store(state.storage.clone()),
        None => replicaset::new_store(state.storage.clone()),
    };
    RequestScope {
        kind: GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "ReplicaSet".to_string(),
        },
        resource: GroupVersionResource {
            group: "apps".to_string(),
            version: "v1".to_string(),
            resource: "replicasets".to_string(),
        },
        subresource,
        store: Box::new(store),
        apply: Some(crate::ssa::apply_legacy::<ReplicaSet>),
        convert_to_internal: Some(replicaset::convert_to_internal),
    }
}

/// The `/scale` `RequestScope`: `autoscaling/v1` `Scale` served as
/// `replicasets/scale`, over `ScaleREST` (storage.go:77).
fn scale_scope(state: &ApiServerState) -> RequestScope<Scale> {
    RequestScope {
        kind: GroupVersionKind {
            group: "autoscaling".to_string(),
            version: "v1".to_string(),
            kind: "Scale".to_string(),
        },
        resource: GroupVersionResource {
            group: "apps".to_string(),
            version: "v1".to_string(),
            resource: "replicasets".to_string(),
        },
        subresource: Some("scale"),
        store: Box::new(replicaset::new_scale_rest(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<Scale>),
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
        &scope(&state, None),
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
        &scope(&state, None),
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
        &scope(&state, None),
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
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_replicaset(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

/// GET `/status`: `StatusREST.Get` is the store's `Get` (storage.go:147-150).
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

/// PUT `/status`: `StatusREST.Update` (storage.go:152-157).
pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/status`: a patch into `StatusREST.Update`.
pub async fn patch_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

/// GET `/scale`: `ScaleREST.Get` (storage.go:202-214).
pub async fn get_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response> {
    let response = endpoints::get_resource(
        &state,
        &scale_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await?;
    Ok(endpoints::negotiate(&headers, response).await)
}

/// PUT `/scale`: `ScaleREST.Update` (storage.go:216-235).
pub async fn update_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let response = endpoints::update_resource(
        &state,
        &scale_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await?;
    Ok(endpoints::negotiate(&headers, response).await)
}

/// PATCH `/scale`: a patch of the Scale into `ScaleREST.Update`.
pub async fn patch_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let response = endpoints::patch_resource(
        &state,
        &scale_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    Ok(endpoints::negotiate(&headers, response).await)
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
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
        return crate::handlers::watch::watch_namespaced::<ReplicaSet>(
            state,
            auth_ctx,
            namespace,
            "replicasets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing replicasets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicasets", Some(&namespace));
    let mut replicasets: Vec<ReplicaSet> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut replicasets, &params)?;

    // Get the store revision for the list; never "0"/"" so informers can watch.
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &replicasets).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            replicasets,
            Some(resource_version),
            "ReplicaSet",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ReplicaSetList", "apps/v1", replicasets);
    list.metadata.resource_version = Some(resource_version);
    Ok(Json(list).into_response())
}

/// List all replicasets across all namespaces
pub async fn list_all_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
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
        return crate::handlers::watch::watch_cluster_scoped::<ReplicaSet>(
            state,
            auth_ctx,
            "replicasets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all replicasets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicasets").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicasets", None);
    let mut replicasets = state.storage.list::<ReplicaSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut replicasets, &params)?;

    // Get the store revision for the list; never "0"/"" so informers can watch.
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &replicasets).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            replicasets,
            Some(resource_version),
            "ReplicaSet",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ReplicaSetList", "apps/v1", replicasets);
    list.metadata.resource_version = Some(resource_version);
    Ok(Json(list).into_response())
}
