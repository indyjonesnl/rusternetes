//! ReplicationController endpoints.
//!
//! Writes, and the `/status` and `/scale` subresources, go through the
//! generic endpoint handlers and [`crate::registry::generic::Store`] with the
//! ReplicationController strategies ([`crate::registry::core::replicationcontroller`]) — upstream's
//! `pkg/registry/core/replicationcontroller/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::replicationcontroller;
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
    resources::{ReplicationController, Scale},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The ReplicationController `RequestScope`: `v1` `ReplicationController`
/// served as `replicationcontrollers` (or its `/status`), backed by
/// `replicationcontroller.NewREST`'s stores.
fn scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<ReplicationController> {
    let store = match subresource {
        Some(_) => replicationcontroller::new_status_store(state.storage.clone()),
        None => replicationcontroller::new_store(state.storage.clone()),
    };
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "replicationcontrollers".to_string(),
        },
        subresource,
        store: Box::new(store),
        apply: Some(crate::ssa::apply_legacy::<ReplicationController>),
        convert_to_internal: Some(replicationcontroller::convert_to_internal),
    }
}

/// The `/scale` `RequestScope`: `autoscaling/v1` `Scale` served as
/// `replicationcontrollers/scale`, over `ScaleREST` (storage.go:76).
fn scale_scope(state: &ApiServerState) -> RequestScope<Scale> {
    RequestScope {
        kind: GroupVersionKind {
            group: "autoscaling".to_string(),
            version: "v1".to_string(),
            kind: "Scale".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "replicationcontrollers".to_string(),
        },
        subresource: Some("scale"),
        store: Box::new(replicationcontroller::new_scale_rest(state.storage.clone())),
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

pub async fn create_replicationcontroller(
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

pub async fn get_replicationcontroller(
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

pub async fn update_replicationcontroller(
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

pub async fn patch_replicationcontroller(
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

pub async fn delete_replicationcontroller(
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

pub async fn deletecollection_replicationcontrollers(
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

/// GET `/status`: `StatusREST.Get` is the store's `Get` (storage.go:144-147).
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

/// PUT `/status`: `StatusREST.Update` (storage.go:149-154).
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

/// GET `/scale`: `ScaleREST.Get` (storage.go:192-199).
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

/// PUT `/scale`: `ScaleREST.Update` (storage.go:201-216).
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

pub async fn list_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<ReplicationController>(
            state,
            auth_ctx,
            namespace,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing replicationcontrollers in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", Some(&namespace));
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &rcs).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let mut list = List::new("ReplicationControllerList", "v1", rcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all replicationcontrollers across all namespaces
pub async fn list_all_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ReplicationController>(
            state,
            auth_ctx,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all replicationcontrollers");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", None);
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &rcs).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let mut list = List::new("ReplicationControllerList", "v1", rcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
