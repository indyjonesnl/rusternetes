//! PodTemplate endpoints.
//!
//! Writes go through the
//! generic endpoint handlers and [`crate::registry::generic::Store`] with the
//! PodTemplate strategies ([`crate::registry::core::podtemplate`]) — upstream's
//! `pkg/registry/core/podtemplate/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::podtemplate;
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
    resources::PodTemplate,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The PodTemplate `RequestScope`: `v1` `PodTemplate` served as
/// `podtemplates`, backed by `podtemplate.NewREST`'s store.
fn scope(state: &ApiServerState) -> RequestScope<PodTemplate> {
    let store = podtemplate::new_store(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "PodTemplate".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "podtemplates".to_string(),
        },
        subresource: None,
        store: Box::new(store),
        apply: Some(crate::ssa::apply_legacy::<PodTemplate>),
        convert_to_internal: Some(podtemplate::convert_to_internal),
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

pub async fn create_podtemplate(
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

pub async fn get_podtemplate(
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

pub async fn update_podtemplate(
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

pub async fn patch_podtemplate(
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

pub async fn delete_podtemplate(
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

pub async fn deletecollection_podtemplates(
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

pub async fn list_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

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
        return crate::handlers::watch::watch_namespaced::<PodTemplate>(
            state,
            auth_ctx,
            namespace,
            "podtemplates",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing podtemplates in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // ValidateListOptions + the resourceVersion floor (#2224).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let prefix = build_prefix("podtemplates", Some(&namespace));
    let mut podtemplates: Vec<PodTemplate> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut podtemplates, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    podtemplates.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Apply pagination
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();
    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };
    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &podtemplates).await;

    let paginated =
        match rusternetes_common::paginate(podtemplates, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, Json(status)).into_response());
                }
                return Err(rusternetes_common::Error::InvalidResource(e.message));
            }
        };

    let mut list = List::new("PodTemplateList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(Json(list).into_response())
}

/// List all podtemplates across all namespaces
pub async fn list_all_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

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
        return crate::handlers::watch::watch_cluster_scoped::<PodTemplate>(
            state,
            auth_ctx,
            "podtemplates",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all podtemplates");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "podtemplates").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // ValidateListOptions + the resourceVersion floor (#2224).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let prefix = build_prefix("podtemplates", None);
    let mut podtemplates: Vec<PodTemplate> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut podtemplates, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    podtemplates.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Apply pagination
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();
    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };
    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &podtemplates).await;

    let paginated =
        match rusternetes_common::paginate(podtemplates, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, Json(status)).into_response());
                }
                return Err(rusternetes_common::Error::InvalidResource(e.message));
            }
        };

    let mut list = List::new("PodTemplateList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(Json(list).into_response())
}
