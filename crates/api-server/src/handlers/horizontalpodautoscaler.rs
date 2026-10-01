//! HorizontalPodAutoscaler endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the HPA strategy
//! ([`crate::registry::autoscaling::horizontalpodautoscaler`]) — upstream's
//! `pkg/registry/autoscaling/horizontalpodautoscaler/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::autoscaling::horizontalpodautoscaler;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Uri},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::HorizontalPodAutoscaler,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// The version a request addressed: both `autoscaling/v1` and `autoscaling/v2`
/// are served from one storage and the data's apiVersion is checked against it.
fn served_version(uri: &Uri) -> &'static str {
    if uri.path().starts_with("/apis/autoscaling/v1/") {
        "v1"
    } else {
        "v2"
    }
}

/// The HPA `RequestScope`: `autoscaling/v1|v2` `HorizontalPodAutoscaler` served
/// as `horizontalpodautoscalers`, backed by the HPA `NewREST`.
fn scope(
    state: &ApiServerState,
    uri: &Uri,
    subresource: Option<&'static str>,
) -> RequestScope<HorizontalPodAutoscaler> {
    let version = served_version(uri);
    let (store, status_store) = horizontalpodautoscaler::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "autoscaling".to_string(),
            version: version.to_string(),
            kind: "HorizontalPodAutoscaler".to_string(),
        },
        resource: GroupVersionResource {
            group: "autoscaling".to_string(),
            version: version.to_string(),
            resource: "horizontalpodautoscalers".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<HorizontalPodAutoscaler>),
        convert_to_internal: Some(horizontalpodautoscaler::convert_to_internal),
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

/// The HorizontalPodAutoscaler API is served exclusively as `autoscaling/v2`
/// (the LIST handlers emit `autoscaling/v2`), so watch events AND bookmarks must
/// be stamped v2 too. Without this the generic watch machinery stamps
/// `autoscaling/v1` (`resource_type_to_kind_and_version` defaults every group to
/// `/v1`), and the controller-manager's `autoscaling/v2` HPA informer rejects
/// every event ("Unexpected watch event object type … expected *v2 … actual
/// *v1") and never syncs. Because the ResourceQuota QuotaMonitor waits on that
/// HPA informer's cache sync, the whole quota controller wedges and every
/// ResourceQuota conformance spec times out (#1670).
fn hpa_v2_watch_gvk() -> (
    crate::handlers::watch::WatchObjectConverter,
    Option<(String, String)>,
) {
    let converter: crate::handlers::watch::WatchObjectConverter =
        Arc::new(|mut val: serde_json::Value| {
            Box::pin(async move {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("apiVersion".into(), serde_json::json!("autoscaling/v2"));
                    obj.insert("kind".into(), serde_json::json!("HorizontalPodAutoscaler"));
                }
                val
            })
        });
    (
        converter,
        Some((
            "HorizontalPodAutoscaler".to_string(),
            "autoscaling/v2".to_string(),
        )),
    )
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_horizontalpodautoscalers(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        let (converter, bookmark_gvk) = hpa_v2_watch_gvk();
        return crate::handlers::watch::watch_namespaced_converted::<HorizontalPodAutoscaler>(
            state,
            auth_ctx,
            namespace,
            "horizontalpodautoscalers",
            "autoscaling",
            watch_params,
            converter,
            bookmark_gvk,
        )
        .await;
    }

    info!(
        "Listing horizontalpodautoscalers in namespace: {}",
        namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("horizontalpodautoscalers", Some(&namespace));
    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let mut list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        let (converter, bookmark_gvk) = hpa_v2_watch_gvk();
        return crate::handlers::watch::watch_cluster_scoped_converted::<HorizontalPodAutoscaler>(
            state,
            auth_ctx,
            "horizontalpodautoscalers",
            "autoscaling",
            watch_params,
            converter,
            bookmark_gvk,
        )
        .await;
    }

    debug!("Listing all horizontalpodautoscalers");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "horizontalpodautoscalers")
        .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("horizontalpodautoscalers", None);
    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let mut list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// GET `/status`: `StatusREST.Get` is the store's `Get`.
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, &uri, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

/// PUT `/status`: `StatusREST.Update`.
pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, &uri, Some("status")),
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
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, &uri, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}
