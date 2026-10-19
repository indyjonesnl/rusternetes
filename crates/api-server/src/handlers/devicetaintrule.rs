//! `resource.k8s.io/v1alpha3` DeviceTaintRule endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the DeviceTaintRule strategies
//! ([`crate::registry::resource::devicetaintrule`]), upstream's
//! `pkg/registry/resource/devicetaintrule/storage`.
//!
//! Upstream installs the storage whenever `resource.k8s.io/v1alpha3` is
//! enabled (`pkg/registry/resource/rest/storage_resource.go:117-130`), a
//! group-version that is in `alphaAPIGroupVersionsDisabledByDefault`
//! (`pkg/controlplane/instance.go:493`). Rusternetes has no `--runtime-config`,
//! so the `DRADeviceTaintRules` gate (alpha, off in v1.35,
//! `pkg/features/kube_features.go:1240`) stands in for it, as
//! `DRADeviceTaintRules` does for `resource.k8s.io/v1alpha3`; with it
//! off every verb is a 404.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::resource::devicetaintrule;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    feature_gates::{enabled, Feature},
    resources::devicetaintrule::DeviceTaintRule,
    Error, List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;

/// The 404 of a resource whose storage was not installed because the
/// `DRADeviceTaintRules` gate is off.
pub fn gate() -> Result<()> {
    if enabled(Feature::DRADeviceTaintRules) {
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
) -> RequestScope<DeviceTaintRule> {
    let (store, status_store) = devicetaintrule::new_stores(state.storage.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "resource.k8s.io".to_string(),
            version: "v1alpha3".to_string(),
            kind: "DeviceTaintRule".to_string(),
        },
        resource: GroupVersionResource {
            group: "resource.k8s.io".to_string(),
            version: "v1alpha3".to_string(),
            resource: "devicetaintrules".to_string(),
        },
        subresource,
        store: match subresource {
            Some("status") => Box::new(status_store),
            _ => Box::new(store),
        },
        apply: Some(crate::ssa::apply_legacy::<DeviceTaintRule>),
        convert_to_internal: Some(devicetaintrule::convert_to_internal),
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

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
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
    gate()?;
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
    gate()?;
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

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn deletecollection(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<DeviceTaintRule>(
            state,
            auth_ctx,
            "devicetaintrules",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "devicetaintrules")
        .with_api_group("resource.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let prefix = build_prefix("devicetaintrules", None);
    let mut items = crate::handlers::list_options::list_items::<DeviceTaintRule, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new("DeviceTaintRuleList", "resource.k8s.io/v1alpha3", items);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(crate::audit::json_response(list).into_response())
}

// Status subresource.
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
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

pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
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

pub async fn patch_status(
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
