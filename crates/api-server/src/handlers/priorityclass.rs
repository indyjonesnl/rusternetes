//! PriorityClass endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the PriorityClass strategy
//! ([`crate::registry::scheduling::priorityclass`]) — upstream's
//! `pkg/registry/scheduling/priorityclass/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. The one-default rule
//! is the Priority admission plugin, which runs in the Store's validating
//! admission. Lists and watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::scheduling::priorityclass;
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
    resources::PriorityClass,
    List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The PriorityClass `RequestScope`: `scheduling.k8s.io/v1` `PriorityClass`
/// served as `priorityclasses`, backed by `priorityclass.NewREST`.
fn scope(state: &ApiServerState) -> RequestScope<PriorityClass> {
    RequestScope {
        kind: GroupVersionKind {
            group: "scheduling.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "PriorityClass".to_string(),
        },
        resource: GroupVersionResource {
            group: "scheduling.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "priorityclasses".to_string(),
        },
        subresource: None,
        store: Box::new(priorityclass::new_rest(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<PriorityClass>),
        convert_to_internal: Some(priorityclass::convert_to_internal),
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

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(&state, &scope(&state), &auth_ctx.user, None, &name, &params).await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_priorityclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<PriorityClass>(
            state,
            auth_ctx,
            "priorityclasses",
            "scheduling.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing PriorityClasses");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "priorityclasses")
        .with_api_group("scheduling.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("priorityclasses", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut priority_classes = crate::handlers::list_options::list_items::<PriorityClass, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut priority_classes, &params)?;

    let mut list = List::new(
        "PriorityClassList",
        "scheduling.k8s.io/v1",
        priority_classes,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(Json(list).into_response())
}
