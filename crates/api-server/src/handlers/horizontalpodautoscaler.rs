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
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::{header, HeaderMap, Uri},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::{
        autoscaling_v1::{hpa_v1_to_v2, hpa_v2_to_v1},
        HorizontalPodAutoscaler,
    },
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
        patch_conversion: (version == "v1").then_some(V1_PATCH_CONVERSION),
    }
}

/// The conversion a PATCH to an `autoscaling/v1` endpoint applies around the
/// patch: it is written against v1, so it applies there and the result goes
/// back to the stored v2 object (patch.go:323-338, :449-462).
const V1_PATCH_CONVERSION: crate::endpoints::handlers::RequestVersionConversion =
    crate::endpoints::handlers::RequestVersionConversion {
        to_request_version: hpa_v2_to_v1,
        from_request_version: |_stored, patched| hpa_v1_to_v2(&patched),
    };

/// A request body in the shape the scope decodes. An `autoscaling/v1` body is
/// converted onto the stored v2 object before decoding, as the codec does
/// (`Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler`,
/// pkg/apis/autoscaling/v1/conversion.go:375). `apiVersion` stays v1 so the
/// scope's check against the served version still holds; `convert_to_internal`
/// stamps the stored version.
fn request_body(uri: &Uri, body: &Bytes) -> Bytes {
    if served_version(uri) != "v1" {
        return body.clone();
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.clone();
    };
    if !value.is_object() {
        return body.clone();
    }
    match serde_json::to_vec(&hpa_v1_to_v2(&value)) {
        Ok(out) => Bytes::from(out),
        Err(_) => body.clone(),
    }
}

/// A stored HPA (or list of them) in the served version's schema
/// (`Convert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler`,
/// conversion.go:280).
fn to_served_json(uri: &Uri, value: &mut serde_json::Value) {
    if served_version(uri) != "v1" {
        return;
    }
    match value.get("kind").and_then(|k| k.as_str()) {
        Some("HorizontalPodAutoscaler") => *value = hpa_v2_to_v1(value),
        Some("HorizontalPodAutoscalerList") => {
            value["apiVersion"] = serde_json::json!("autoscaling/v1");
            if let Some(items) = value.get_mut("items").and_then(|i| i.as_array_mut()) {
                for item in items.iter_mut() {
                    *item = hpa_v2_to_v1(item);
                }
            }
        }
        _ => {}
    }
}

async fn response(uri: &Uri, response: Response) -> Result<Response> {
    if served_version(uri) != "v1" || !response.status().is_success() {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let out = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => {
            to_served_json(uri, &mut value);
            serde_json::to_vec(&value).unwrap_or_else(|_| bytes.to_vec())
        }
        Err(_) => bytes.to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    Ok(Response::from_parts(parts, Body::from(out)))
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
fn hpa_watch_gvk(
    uri: &Uri,
) -> (
    crate::handlers::watch::WatchObjectConverter,
    Option<(String, String)>,
) {
    let v1 = served_version(uri) == "v1";
    let converter: crate::handlers::watch::WatchObjectConverter =
        Arc::new(move |mut val: serde_json::Value| {
            Box::pin(async move {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("apiVersion".into(), serde_json::json!("autoscaling/v2"));
                    obj.insert("kind".into(), serde_json::json!("HorizontalPodAutoscaler"));
                }
                // An `autoscaling/v1` watch streams v1 objects.
                if v1 {
                    val = hpa_v2_to_v1(&val);
                }
                val
            })
        });
    (
        converter,
        Some((
            "HorizontalPodAutoscaler".to_string(),
            if v1 {
                "autoscaling/v1"
            } else {
                "autoscaling/v2"
            }
            .to_string(),
        )),
    )
}

/// `/apis/autoscaling/{v1,v2}/watch/namespaces/:ns/horizontalpodautoscalers`.
/// Every event, the initial-list `ADDED`s, `DELETED` and `BOOKMARK` included,
/// is encoded for the requested version, as upstream's `serveWatchHandler`
/// does with `scope.Serializer.EncoderForVersion`
/// (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:123-144).
pub async fn watch_namespace(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<crate::handlers::watch::WatchParams>,
) -> Result<Response> {
    let (converter, bookmark_gvk) = hpa_watch_gvk(&uri);
    crate::handlers::watch::watch_namespaced_converted::<
        rusternetes_common::resources::CustomResource,
    >(
        state,
        auth_ctx,
        namespace,
        "horizontalpodautoscalers",
        "autoscaling",
        params,
        converter,
        bookmark_gvk,
    )
    .await
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let out = endpoints::create_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &request_body(&uri, &body),
    )
    .await?;
    response(&uri, out).await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    let out = endpoints::get_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await?;
    response(&uri, out).await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let out = endpoints::update_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &request_body(&uri, &body),
    )
    .await?;
    response(&uri, out).await
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
    let out = endpoints::patch_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    response(&uri, out).await
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let out = endpoints::delete_resource(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await?;
    response(&uri, out).await
}

pub async fn deletecollection_horizontalpodautoscalers(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let out = endpoints::delete_collection(
        &state,
        &scope(&state, &uri, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await?;
    response(&uri, out).await
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        let (converter, bookmark_gvk) = hpa_watch_gvk(&uri);
        return crate::handlers::watch::watch_namespaced_converted::<
            rusternetes_common::resources::CustomResource,
        >(
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
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let mut list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    let mut value = serde_json::to_value(&list)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    to_served_json(&uri, &mut value);
    Ok(Json(value).into_response())
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        let (converter, bookmark_gvk) = hpa_watch_gvk(&uri);
        return crate::handlers::watch::watch_cluster_scoped_converted::<
            rusternetes_common::resources::CustomResource,
        >(
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
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let mut list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    let mut value = serde_json::to_value(&list)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    to_served_json(&uri, &mut value);
    Ok(Json(value).into_response())
}

/// GET `/status`: `StatusREST.Get` is the store's `Get`.
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    let out = endpoints::get_resource(
        &state,
        &scope(&state, &uri, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await?;
    response(&uri, out).await
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
    let out = endpoints::update_resource(
        &state,
        &scope(&state, &uri, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &request_body(&uri, &body),
    )
    .await?;
    response(&uri, out).await
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
    let out = endpoints::patch_resource(
        &state,
        &scope(&state, &uri, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    response(&uri, out).await
}
