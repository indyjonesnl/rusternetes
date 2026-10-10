//! Namespace endpoints.
//!
//! Writes, `/status` and `/finalize` go through the generic endpoint handlers
//! with the Namespace storage ([`crate::registry::core::namespace`]) —
//! upstream's `pkg/registry/core/namespace/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Namespaces serve no
//! DeleteCollection (the REST does not implement `rest.CollectionDeleter`).
//! Lists and watches are still served here directly.
//!
//! Creating a namespace has no side effects here: the ServiceAccounts
//! controller creates `default` (pkg/controller/serviceaccount/
//! serviceaccounts_controller.go) and the root-CA publisher creates
//! `kube-root-ca.crt` (pkg/controller/certificates/rootcacertpublisher).

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::namespace;
use crate::{handlers::watch, middleware::AuthContext, state::ApiServerState};
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
    resources::Namespace,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The Namespace `RequestScope`: `v1` `Namespace` served as `namespaces` (or
/// its `/status` or `/finalize`), backed by `namespace.NewREST`'s storage.
fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<Namespace> {
    let store: Box<dyn crate::registry::rest::RestStorage<Namespace>> = match subresource {
        Some("status") => Box::new(namespace::new_status_store(state.storage.clone())),
        Some(_) => Box::new(namespace::new_finalize_store(state.storage.clone())),
        None => Box::new(namespace::new_rest(state.storage.clone())),
    };
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "Namespace".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "namespaces".to_string(),
        },
        subresource,
        store,
        apply: Some(crate::ssa::apply_legacy::<Namespace>),
        convert_to_internal: Some(namespace::convert_to_internal),
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

pub async fn get(
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

pub async fn update(
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

pub async fn delete_ns(
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

/// GET `/status`: `StatusREST.Get` is the store's `Get` (storage/storage.go:322-325).
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

/// PUT `/status`: `StatusREST.Update` (storage/storage.go:327-332).
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

/// PUT `/finalize`: `FinalizeREST.Update` (storage/storage.go:353-358).
pub async fn finalize(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("finalize")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        debug!("Watching namespaces");
        // Parse WatchParams from the query parameters
        let watch_params = watch::WatchParams {
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
        return watch::watch_cluster_scoped::<Namespace>(
            state,
            auth_ctx,
            "namespaces",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing namespaces");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "namespaces").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("namespaces", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut namespaces = state.storage.list::<Namespace>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut namespaces, &params)?;

    let mut list = List::new("NamespaceList", "v1", namespaces);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
