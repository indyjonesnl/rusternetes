//! Generic Store endpoints for ServiceCIDR and its `/status`; list/watch retain
//! their existing implementation.
//! Port of pkg/registry/networking/servicecidr/storage/storage.go and apiserver endpoints/handlers.
//!
//! The `networking.k8s.io/service-cidr-finalizer` protection is client-side
//! upstream (pkg/controller/servicecidrs/servicecidrs_controller.go:61, :420-456):
//! the controller adds the finalizer and removes it once no IPAddress is left
//! in the range. The server runs no extra delete check
//! (servicecidr/strategy.go has no `CheckGracefulDelete`), so the counterpart
//! is `crates/controller-manager/src/controllers/servicecidr.rs`.
use crate::endpoints::handlers::{self as endpoints, RequestScope};
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
    resources::ServiceCIDR,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<ServiceCIDR> {
    let store: Box<dyn crate::registry::rest::RestStorage<ServiceCIDR>> = match subresource {
        Some(_) => Box::new(crate::registry::networking::servicecidr::new_status_store(
            state.storage.clone(),
        )),
        None => Box::new(crate::registry::networking::servicecidr::new_store(
            state.storage.clone(),
        )),
    };
    RequestScope {
        kind: GroupVersionKind {
            group: "networking.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "ServiceCIDR".to_string(),
        },
        resource: GroupVersionResource {
            group: "networking.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "servicecidrs".to_string(),
        },
        subresource,
        store,
        apply: Some(crate::ssa::apply_legacy::<ServiceCIDR>),
        convert_to_internal: None,
        patch_conversion: None,
    }
}

fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

pub async fn create_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn get_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

pub async fn update_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

/// GET `/status`: `StatusREST.Get` is the store's `Get` (storage.go:83-85).
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

/// PUT `/status`: `StatusREST.Update` (storage.go:88-92).
pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        None,
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
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn list_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    debug!("Listing ServiceCIDRs");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ServiceCIDR>(
            state,
            auth_ctx,
            "servicecidrs",
            "networking.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "servicecidrs")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("servicecidrs", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let servicecidrs = state.storage.list::<ServiceCIDR>(&prefix).await?;

    let mut list = List::new("ServiceCIDRList", "networking.k8s.io/v1", servicecidrs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
