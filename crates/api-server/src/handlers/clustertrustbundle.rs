//! `certificates.k8s.io/v1beta1` ClusterTrustBundle endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the ClusterTrustBundle strategy
//! ([`crate::registry::certificates::clustertrustbundle`]), upstream's
//! `pkg/registry/certificates/clustertrustbundle/storage`.
//!
//! The storage exists only under the `ClusterTrustBundle` feature gate
//! (`pkg/registry/certificates/rest/storage_certificates.go:94-103`, off by
//! default in v1.35, `pkg/features/kube_features.go:1199-1202`); with it off
//! every verb is a 404, as for a resource that was never installed.
use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::certificates::clustertrustbundle;
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
    resources::ClusterTrustBundle,
    Error, List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// The 404 of a resource whose storage was not installed because the
/// `ClusterTrustBundle` gate is off.
pub fn gate() -> Result<()> {
    if enabled(Feature::ClusterTrustBundle) {
        Ok(())
    } else {
        Err(Error::NotFound(
            "the server could not find the requested resource".to_string(),
        ))
    }
}

fn scope(state: &ApiServerState) -> RequestScope<ClusterTrustBundle> {
    RequestScope {
        kind: GroupVersionKind {
            group: "certificates.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            kind: "ClusterTrustBundle".to_string(),
        },
        resource: GroupVersionResource {
            group: "certificates.k8s.io".to_string(),
            version: "v1beta1".to_string(),
            resource: "clustertrustbundles".to_string(),
        },
        subresource: None,
        store: Box::new(clustertrustbundle::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<ClusterTrustBundle>),
        convert_to_internal: None,
        patch_conversion: None,
    }
}

fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

pub async fn create_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::create_resource(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

pub async fn get_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    endpoints::get_resource(&state, &scope(&state), &auth_ctx.user, None, &name, &params).await
}

pub async fn update_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::update_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::patch_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::delete_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    endpoints::delete_collection(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

/// List, or watch when `?watch=true`. `spec.signerName` and `metadata.name`
/// field selectors and label selectors apply (storage.go:68-79 `getAttrs`).
pub async fn list_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterTrustBundle>(
            state,
            auth_ctx,
            "clustertrustbundles",
            "certificates.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clustertrustbundles")
        .with_api_group("certificates.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("clustertrustbundles", None);
    let mut items = state.storage.list::<ClusterTrustBundle>(&prefix).await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "ClusterTrustBundleList",
        "certificates.k8s.io/v1beta1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
