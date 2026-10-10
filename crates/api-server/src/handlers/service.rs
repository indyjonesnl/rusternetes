//! Service endpoints.
//!
//! Writes and the `/status` subresource go through the generic endpoint
//! handlers and [`crate::registry::generic::Store`] with the Service
//! strategies and storage hooks ([`crate::registry::core::service`]) —
//! upstream's `pkg/registry/core/service/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly, but every item they emit runs through the
//! Store's `Decorator` ([`service_storage::default_on_read`]) as upstream's
//! `Store.List` and `Store.WatchPredicate` do (store.go:381-383, :1463-1465;
//! `defaultOnReadServiceList` storage.go:243-252).

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::service::storage as service_storage;
use crate::{handlers::watch::WatchParams, middleware::AuthContext, state::ApiServerState};
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
    resources::Service,
    List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The Service `RequestScope`: `v1` `Service` served as `services` (or its
/// `/status`), backed by `service.NewREST`'s stores.
fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<Service> {
    let (store, status_store) = service_storage::new_stores(
        state.storage.clone(),
        state.cluster_ip_allocators.clone(),
        state.node_port_allocator.clone(),
    );
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "Service".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "services".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<Service>),
        convert_to_internal: Some(service_storage::convert_to_internal),
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

async fn get_in(
    state: &ApiServerState,
    subresource: Option<&'static str>,
    user: &rusternetes_common::auth::UserInfo,
    namespace: &str,
    name: &str,
    params: &std::collections::HashMap<String, String>,
) -> Result<Response> {
    endpoints::get_resource(
        state,
        &scope(state, subresource),
        user,
        Some(namespace),
        name,
        params,
    )
    .await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    get_in(&state, None, &auth_ctx.user, &namespace, &name, &params).await
}

pub async fn update(
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

#[allow(clippy::too_many_arguments)]
async fn patch_in(
    state: &ApiServerState,
    subresource: Option<&'static str>,
    user: &rusternetes_common::auth::UserInfo,
    namespace: &str,
    name: &str,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        state,
        &scope(state, subresource),
        user,
        Some(namespace),
        name,
        params,
        patch_content_type(headers),
        body,
    )
    .await
}

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    patch_in(
        &state,
        None,
        &auth_ctx.user,
        &namespace,
        &name,
        &params,
        &headers,
        &body,
    )
    .await
}

pub async fn delete_service(
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

pub async fn deletecollection_services(
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

/// GET `/status`: `StatusREST.Get` is the store's `Get` (storage.go:181-184).
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    get_in(
        &state,
        Some("status"),
        &auth_ctx.user,
        &namespace,
        &name,
        &params,
    )
    .await
}

/// PUT `/status`: `StatusREST.Update` (storage.go:186-191).
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
    patch_in(
        &state,
        Some("status"),
        &auth_ctx.user,
        &namespace,
        &name,
        &params,
        &headers,
        &body,
    )
    .await
}

/// The Store `Decorator` as a watch converter: every streamed Service (initial
/// ADDED, live events, delete `prev` values) is decoded, run through
/// `defaultOnReadService` (storage.go:255-274) and re-encoded. Ported from
/// `Store.WatchPredicate` (staging/.../registry/generic/registry/store.go:
/// 1463-1465: `if e.Decorator != nil { return newDecoratedWatcher(ctx, w,
/// e.Decorator), nil }`). Best-effort: an object that does not decode as a
/// Service is passed through unchanged.
pub(crate) fn default_on_read_watch_converter(
    primary: rusternetes_common::resources::IPFamily,
) -> crate::handlers::watch::WatchObjectConverter {
    Arc::new(move |val: serde_json::Value| {
        let primary = primary.clone();
        Box::pin(async move {
            match serde_json::from_value::<Service>(val.clone()) {
                Ok(mut svc) => {
                    service_storage::default_on_read(&mut svc, &primary);
                    serde_json::to_value(&svc).unwrap_or(val)
                }
                Err(_) => val,
            }
        })
    })
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<WatchParams>,
    Query(list_options): Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        debug!("Watch request for services in namespace: {}", namespace);
        return crate::handlers::watch::watch_services(
            State(state),
            Extension(auth_ctx),
            Path(namespace),
            Query(params),
        )
        .await;
    }

    debug!("Listing services in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "services")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("services", Some(&namespace));
    let primary = state.cluster_ip_allocators.primary_family();
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &list_options).await?;

    let mut services = crate::handlers::list_options::list_items::<Service, _>(
        &*state.storage,
        &prefix,
        &list_options,
    )
    .await?;
    // `Store.Decorator` on every listed item (`Store.List` store.go:381-383;
    // `defaultOnReadServiceList` storage.go:243-252).
    services
        .iter_mut()
        .for_each(|svc| service_storage::default_on_read(svc, &primary));

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut services, &params_map)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version = crate::handlers::list_options::list_resource_version(
        &state.storage,
        &list_options,
        &services,
    )
    .await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            services,
            Some(resource_version.to_string()),
            "Service",
        );
        return Ok(Json(table).into_response());
    }

    // Wrap in proper List object
    let mut list = List::new("ServiceList", "v1", services);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(
            &state.storage,
            &list_options,
            &list.items,
        )
        .await,
    );
    Ok(Json(list).into_response())
}

/// List all services across all namespaces
pub async fn list_all_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<WatchParams>,
    Query(list_options): Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    // Debug log the params
    debug!("list_all_services called with watch={:?}", params.watch);

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        debug!("Watch request for all services");
        let converter =
            default_on_read_watch_converter(state.cluster_ip_allocators.primary_family());
        return crate::handlers::watch::watch_cluster_scoped_converted::<Service>(
            state, auth_ctx, "services", "", params, converter, None,
        )
        .await;
    }

    debug!("Listing all services");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "services").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("services", None);
    let primary = state.cluster_ip_allocators.primary_family();
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &list_options).await?;

    let mut services = crate::handlers::list_options::list_items::<Service, _>(
        &*state.storage,
        &prefix,
        &list_options,
    )
    .await?;
    // `Store.Decorator` on every listed item (`Store.List` store.go:381-383;
    // `defaultOnReadServiceList` storage.go:243-252).
    services
        .iter_mut()
        .for_each(|svc| service_storage::default_on_read(svc, &primary));

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut services, &params_map)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version = crate::handlers::list_options::list_resource_version(
        &state.storage,
        &list_options,
        &services,
    )
    .await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            services,
            Some(resource_version.to_string()),
            "Service",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ServiceList", "v1", services);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(
            &state.storage,
            &list_options,
            &list.items,
        )
        .await,
    );
    Ok(Json(list).into_response())
}
