//! Generic Store endpoints for IngressClass; list/watch retain their existing implementation.
//! Port of pkg/registry/networking/ingressclass/storage/storage.go and apiserver endpoints/handlers.
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
    resources::IngressClass,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::{collections::HashMap, sync::Arc};
use tracing::debug;
fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<IngressClass> {
    let store = crate::registry::networking::ingressclass::new_store(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "networking.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "IngressClass".to_string(),
        },
        resource: GroupVersionResource {
            group: "networking.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "ingressclasses".to_string(),
        },
        subresource,
        store: Box::new(store),
        apply: Some(crate::ssa::apply_legacy::<IngressClass>),
        convert_to_internal: Some(crate::registry::networking::ingressclass::convert_to_internal),
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

pub async fn create_ingressclass(
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

pub async fn get_ingressclass(
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

pub async fn update_ingressclass(
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

pub async fn patch_ingressclass(
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

pub async fn delete_ingressclass(
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

pub async fn deletecollection_ingressclasses(
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

pub async fn list_ingressclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<IngressClass>(
            state,
            auth_ctx,
            "ingressclasses",
            "networking.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing IngressClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "ingressclasses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("ingressclasses", None);
    let mut ingress_classes: Vec<IngressClass> = state.storage.list(&prefix).await?;
    crate::handlers::filtering::apply_selectors(&mut ingress_classes, &params)?;

    let mut list = List::new("IngressClassList", "networking.k8s.io/v1", ingress_classes);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
