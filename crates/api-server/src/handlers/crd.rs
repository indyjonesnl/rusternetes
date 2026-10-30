//! CustomResourceDefinition endpoints.
//!
//! Writes, and the `/status` subresource, go through the generic endpoint
//! handlers and [`crate::registry::generic::Store`] with the
//! CustomResourceDefinition strategies
//! ([`crate::registry::apiextensions::customresourcedefinition`]) — the
//! apiextensions-apiserver's `pkg/registry/customresourcedefinition` wired
//! into `endpoints/handlers/{create,update,patch,delete}.go`. Lists and
//! watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::apiextensions::customresourcedefinition;
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
    resources::CustomResourceDefinition,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The CustomResourceDefinition `RequestScope`: `apiextensions.k8s.io/v1`
/// `CustomResourceDefinition` served as `customresourcedefinitions` (or its
/// `/status`), backed by `customresourcedefinition.NewREST` and
/// `NewStatusREST`.
fn scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<CustomResourceDefinition> {
    let store: Box<dyn crate::registry::rest::RestStorage<CustomResourceDefinition>> =
        match subresource {
            Some(_) => Box::new(customresourcedefinition::CrdStatusRest::with_controllers(
                state.storage.clone(),
                state.crd_controllers.clone(),
            )),
            None => Box::new(customresourcedefinition::CrdRest::with_controllers(
                state.storage.clone(),
                state.crd_controllers.clone(),
            )),
        };
    RequestScope {
        kind: GroupVersionKind {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
        },
        resource: GroupVersionResource {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "customresourcedefinitions".to_string(),
        },
        subresource,
        store,
        apply: Some(crate::ssa::apply_legacy::<CustomResourceDefinition>),
        convert_to_internal: Some(customresourcedefinition::convert_to_internal),
        patch_conversion: None,
        forbid_create: false,
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

pub async fn create_crd(
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

pub async fn get_crd(
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

pub async fn update_crd(
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

pub async fn patch_crd(
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

pub async fn delete_crd(
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

pub async fn deletecollection_customresourcedefinitions(
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

/// GET `/status`: `StatusREST.Get` is the store's `Get` (etcd.go:195-198).
pub async fn get_crd_status(
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

/// PUT `/status`.
pub async fn update_crd_status(
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

/// PATCH `/status`.
pub async fn patch_crd_status(
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

/// List all CustomResourceDefinitions
pub async fn list_crds(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped_json(
            state,
            auth_ctx,
            "customresourcedefinitions",
            "apiextensions.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all CustomResourceDefinitions");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("customresourcedefinitions", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut crds = state
        .storage
        .list::<CustomResourceDefinition>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut crds, &params)?;

    let mut list = List::new(
        "CustomResourceDefinitionList",
        "apiextensions.k8s.io/v1",
        crds,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(crate::audit::json_response(list).into_response())
}
