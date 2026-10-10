//! VolumeSnapshotContent endpoints.
//!
//! Writes, and the `/status` subresource, go through the generic endpoint
//! handlers and [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::snapshot::volumesnapshotcontent`] — the
//! apiextensions-apiserver's custom-resource strategy wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::snapshot::volumesnapshotcontent;
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
    resources::VolumeSnapshotContent,
    List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The VolumeSnapshotContent `RequestScope`: `snapshot.storage.k8s.io/v1`
/// served as `volumesnapshotcontents` (or its `/status`).
fn scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<VolumeSnapshotContent> {
    let (store, status_store) = volumesnapshotcontent::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "snapshot.storage.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "VolumeSnapshotContent".to_string(),
        },
        resource: GroupVersionResource {
            group: "snapshot.storage.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "volumesnapshotcontents".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<VolumeSnapshotContent>),
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

pub async fn create_volumesnapshotcontent(
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

pub async fn get_volumesnapshotcontent(
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

pub async fn update_volumesnapshotcontent(
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

pub async fn patch_volumesnapshotcontent(
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

pub async fn delete_volumesnapshotcontent(
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

pub async fn deletecollection_volumesnapshotcontents(
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

/// GET `/status`: the status subresource's Get is the store's Get.
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

/// PUT `/status`.
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

/// PATCH `/status`.
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

pub async fn list_volumesnapshotcontents(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all VolumeSnapshotContents");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeSnapshotContent>(
            state,
            auth_ctx,
            "volumesnapshotcontents",
            "snapshot.storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumesnapshotcontents")
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumesnapshotcontents", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut vscs = crate::handlers::list_options::list_items::<VolumeSnapshotContent, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vscs, &params)?;

    let mut list = List::new(
        "VolumeSnapshotContentList",
        "snapshot.storage.k8s.io/v1",
        vscs,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}
