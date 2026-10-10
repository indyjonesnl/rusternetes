//! `certificates.k8s.io/v1beta1` PodCertificateRequest endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the PodCertificateRequest
//! strategies ([`crate::registry::certificates::podcertificaterequest`]),
//! upstream's `pkg/registry/certificates/podcertificaterequest/storage`.
//!
//! The storage exists only under the `PodCertificateRequest` feature gate
//! (`pkg/registry/certificates/rest/storage_certificates.go`, off by default
//! in v1.35, `pkg/features/kube_features.go:1602`); with it off every verb is a
//! 404, as for a resource that was never installed.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::certificates::podcertificaterequest;
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
    feature_gates::{enabled, Feature},
    resources::podcertificaterequest::PodCertificateRequest,
    Error, List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// The 404 of a resource whose storage was not installed because the
/// `PodCertificateRequest` gate is off.
pub fn gate() -> Result<()> {
    if enabled(Feature::PodCertificateRequest) {
        Ok(())
    } else {
        Err(Error::NotFound(
            "the server could not find the requested resource".to_string(),
        ))
    }
}

fn scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<PodCertificateRequest> {
    let (store, status_store) = podcertificaterequest::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "certificates.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            kind: "PodCertificateRequest".to_string(),
        },
        resource: GroupVersionResource {
            group: "certificates.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            resource: "podcertificaterequests".to_string(),
        },
        subresource,
        store: match subresource {
            Some("status") => Box::new(status_store),
            _ => Box::new(store),
        },
        apply: Some(crate::ssa::apply_legacy::<PodCertificateRequest>),
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

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    endpoints::get_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::patch_resource(
        &state,
        &scope(&state, None),
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
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn deletecollection(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

async fn list_in(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: Option<String>,
    params: HashMap<String, String>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return match namespace {
            Some(ns) => {
                crate::handlers::watch::watch_namespaced::<PodCertificateRequest>(
                    state,
                    auth_ctx,
                    ns,
                    "podcertificaterequests",
                    "certificates.k8s.io",
                    watch_params,
                )
                .await
            }
            None => {
                crate::handlers::watch::watch_cluster_scoped::<PodCertificateRequest>(
                    state,
                    auth_ctx,
                    "podcertificaterequests",
                    "certificates.k8s.io",
                    watch_params,
                )
                .await
            }
        };
    }

    let mut attrs = RequestAttributes::new(auth_ctx.user, "list", "podcertificaterequests")
        .with_api_group("certificates.k8s.io");
    if let Some(ns) = &namespace {
        attrs = attrs.with_namespace(ns);
    }
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let prefix = build_prefix("podcertificaterequests", namespace.as_deref());
    let mut items = state.storage.list::<PodCertificateRequest>(&prefix).await?;

    // `getAttrs` (storage.go) makes spec.signerName / spec.podName /
    // spec.nodeName selectable next to the metadata fields; the selectors run
    // over the serialized object, where those paths already resolve.
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "PodCertificateRequestList",
        "certificates.k8s.io/v1beta1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    list_in(state, auth_ctx, Some(namespace), params).await
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    list_in(state, auth_ctx, None, params).await
}

// Status subresource.
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    endpoints::get_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await
}

pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn patch_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}
