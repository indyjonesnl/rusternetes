//! ReferenceGrant endpoints.
//!
//! Writes go through the generic endpoint
//! handlers and [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::gateway::referencegrant`] — the apiextensions-apiserver's
//! custom-resource strategy wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::gateway::referencegrant;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Extension,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::ReferenceGrant,
    List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The ReferenceGrant `RequestScope`: `gateway.networking.k8s.io/v1beta1`
/// `ReferenceGrant` served as `referencegrants`.
fn scope(state: &ApiServerState) -> RequestScope<ReferenceGrant> {
    RequestScope {
        kind: GroupVersionKind {
            group: "gateway.networking.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            kind: "ReferenceGrant".to_string(),
        },
        resource: GroupVersionResource {
            group: "gateway.networking.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            resource: "referencegrants".to_string(),
        },
        subresource: None,
        store: Box::new(referencegrant::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<ReferenceGrant>),
        convert_to_internal: None,
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

pub async fn create_referencegrant(
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

pub async fn get_referencegrant(
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

pub async fn update_referencegrant(
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

pub async fn patch_referencegrant(
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

pub async fn delete_referencegrant(
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

pub async fn deletecollection_referencegrants(
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

pub async fn list_referencegrants(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_namespaced::<ReferenceGrant>(
            state,
            auth_ctx,
            namespace,
            "referencegrants",
            "gateway.networking.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing ReferenceGrants in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "referencegrants")
        .with_api_group("gateway.networking.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("referencegrants", Some(&namespace));
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut vss = crate::handlers::list_options::list_items::<ReferenceGrant, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vss, &params)?;

    let mut list = List::new(
        "ReferenceGrantList",
        "gateway.networking.k8s.io/v1beta1",
        vss,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(axum::response::IntoResponse::into_response(
        crate::audit::json_response(list),
    ))
}

pub async fn list_all_referencegrants(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ReferenceGrant>(
            state,
            auth_ctx,
            "referencegrants",
            "gateway.networking.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing all ReferenceGrants");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "referencegrants")
        .with_api_group("gateway.networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("referencegrants", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut vss = crate::handlers::list_options::list_items::<ReferenceGrant, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vss, &params)?;

    let mut list = List::new(
        "ReferenceGrantList",
        "gateway.networking.k8s.io/v1beta1",
        vss,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(axum::response::IntoResponse::into_response(
        crate::audit::json_response(list),
    ))
}
