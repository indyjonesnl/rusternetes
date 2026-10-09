//! FlowSchema and PriorityLevelConfiguration endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::flowcontrol`] — upstream's
//! `pkg/registry/flowcontrol/{flowschema,prioritylevelconfiguration}/storage`
//! wired into `endpoints/handlers/{create,update,patch,delete}.go`. Lists and
//! watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::flowcontrol::{flowschema, prioritylevelconfiguration};
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
    resources::{FlowSchema, PriorityLevelConfiguration},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// The PriorityLevelConfiguration `RequestScope`: `flowcontrol.apiserver.k8s.io/v1` served as
/// `prioritylevelconfigurations`, backed by the PriorityLevelConfiguration `NewREST`.
fn plc_scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<PriorityLevelConfiguration> {
    let (store, status_store) = prioritylevelconfiguration::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "flowcontrol.apiserver.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "PriorityLevelConfiguration".to_string(),
        },
        resource: GroupVersionResource {
            group: "flowcontrol.apiserver.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "prioritylevelconfigurations".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<PriorityLevelConfiguration>),
        convert_to_internal: Some(prioritylevelconfiguration::convert_to_internal),
        patch_conversion: None,
    }
}

pub async fn create_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn get_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

pub async fn update_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_prioritylevelconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &plc_scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn list_priority_level_configurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<PriorityLevelConfiguration>(
            state,
            auth_ctx,
            "prioritylevelconfigurations",
            "flowcontrol.apiserver.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing PriorityLevelConfigurations");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("prioritylevelconfigurations", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut items = state
        .storage
        .list::<PriorityLevelConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "PriorityLevelConfigurationList",
        "flowcontrol.apiserver.k8s.io/v1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// GET `/status`.
pub async fn get_priority_level_configuration_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &plc_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

/// PUT `/status`.
pub async fn update_priority_level_configuration_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &plc_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/status`.
pub async fn patch_priority_level_configuration_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &plc_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

/// The FlowSchema `RequestScope`: `flowcontrol.apiserver.k8s.io/v1` served as
/// `flowschemas`, backed by the FlowSchema `NewREST`.
fn fs_scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<FlowSchema> {
    let (store, status_store) = flowschema::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "flowcontrol.apiserver.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "FlowSchema".to_string(),
        },
        resource: GroupVersionResource {
            group: "flowcontrol.apiserver.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "flowschemas".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<FlowSchema>),
        convert_to_internal: Some(flowschema::convert_to_internal),
        patch_conversion: None,
    }
}

pub async fn create_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn get_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

pub async fn update_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_flowschemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &fs_scope(&state, None),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn list_flow_schemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<FlowSchema>(
            state,
            auth_ctx,
            "flowschemas",
            "flowcontrol.apiserver.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing FlowSchemas");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("flowschemas", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut items = state.storage.list::<FlowSchema>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new("FlowSchemaList", "flowcontrol.apiserver.k8s.io/v1", items);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// GET `/status`.
pub async fn get_flow_schema_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &fs_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

/// PUT `/status`.
pub async fn update_flow_schema_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &fs_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/status`.
pub async fn patch_flow_schema_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &fs_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}
