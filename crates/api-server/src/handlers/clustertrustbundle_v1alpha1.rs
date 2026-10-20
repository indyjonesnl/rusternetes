//! `certificates.k8s.io/v1alpha1` ClusterTrustBundle endpoints.
//!
//! Upstream's `v1alpha1Storage`
//! (`pkg/registry/certificates/rest/storage_certificates.go:122-138`) installs
//! the very same `clustertrustbundlestore.NewREST` that v1beta1 uses, so both
//! versions read and write one storage whose version is v1beta1
//! (`pkg/kubeapiserver/default_storage_factory_builder.go:87`). The v1alpha1
//! and v1beta1 types are field-for-field identical
//! (`staging/src/k8s.io/api/certificates/v1alpha1/types.go`), so the conversion
//! (`pkg/apis/certificates/v1alpha1/zz_generated.conversion.go`) is the
//! `apiVersion` alone: a request body is stored as v1beta1
//! ([`convert_to_internal`]) and every response, list item and watch event is
//! encoded back as v1alpha1 ([`to_v1alpha1`]), as `EncoderForVersion` does.
//! There is no v1alpha1 defaulting.
//!
//! Served only while the `ClusterTrustBundle` gate is on, which stands in for
//! `--runtime-config=certificates.k8s.io/v1alpha1`; see
//! [`crate::handlers::clustertrustbundle::gate`] and
//! `rusternetes_discovery::get_certificates_v1alpha1_resources`.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::handlers::clustertrustbundle::{gate, patch_content_type};
use crate::registry::certificates::clustertrustbundle;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::ClusterTrustBundle,
    Error, List, Result,
};
use rusternetes_storage::build_prefix;
use std::collections::HashMap;
use std::sync::Arc;

const GROUP: &str = "certificates.k8s.io";
const VERSION: &str = "v1alpha1";
const STORED_API_VERSION: &str = "certificates.k8s.io/v1beta1";
const SERVED_API_VERSION: &str = "certificates.k8s.io/v1alpha1";

/// A body decoded at v1alpha1 is stored as v1beta1, the storage version.
fn convert_to_internal(bundle: &mut ClusterTrustBundle) {
    bundle.type_meta.api_version = STORED_API_VERSION.to_string();
}

/// A stored object, list, or watch event in the v1alpha1 encoding.
pub(crate) fn to_v1alpha1(value: &mut serde_json::Value) {
    let restamp = |v: &mut serde_json::Value| {
        if v.get("apiVersion").and_then(|a| a.as_str()) == Some(STORED_API_VERSION) {
            v["apiVersion"] = serde_json::json!(SERVED_API_VERSION);
        }
    };
    restamp(value);
    if let Some(items) = value.get_mut("items").and_then(|i| i.as_array_mut()) {
        items.iter_mut().for_each(restamp);
    }
}

async fn convert_response(response: Response) -> Result<Response> {
    if !response.status().is_success() {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let out = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => {
            to_v1alpha1(&mut value);
            serde_json::to_vec(&value).unwrap_or_else(|_| bytes.to_vec())
        }
        Err(_) => bytes.to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    Ok(Response::from_parts(parts, Body::from(out)))
}

/// Every streamed object (initial `ADDED`s, `DELETED`, ...) as v1alpha1.
fn watch_converter() -> crate::handlers::watch::WatchObjectConverter {
    Arc::new(|mut val: serde_json::Value| {
        Box::pin(async move {
            to_v1alpha1(&mut val);
            val
        })
    })
}

fn bookmark_gvk() -> Option<(String, String)> {
    Some((
        "ClusterTrustBundle".to_string(),
        SERVED_API_VERSION.to_string(),
    ))
}

fn scope(state: &ApiServerState) -> RequestScope<ClusterTrustBundle> {
    RequestScope {
        kind: GroupVersionKind {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            kind: "ClusterTrustBundle".to_string(),
        },
        resource: GroupVersionResource {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            resource: "clustertrustbundles".to_string(),
        },
        subresource: None,
        store: Box::new(clustertrustbundle::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<ClusterTrustBundle>),
        convert_to_internal: Some(convert_to_internal),
        patch_conversion: None,
        forbid_create: false,
    }
}

pub async fn create_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out =
        endpoints::create_resource(&state, &scope(&state), &auth_ctx.user, None, &params, &body)
            .await?;
    convert_response(out).await
}

pub async fn get_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    let out = endpoints::get_resource(&state, &scope(&state), &auth_ctx.user, None, &name, &params)
        .await?;
    convert_response(out).await
}

pub async fn update_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out = endpoints::update_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await?;
    convert_response(out).await
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
    let out = endpoints::patch_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    convert_response(out).await
}

pub async fn delete_clustertrustbundle(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out = endpoints::delete_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await?;
    convert_response(out).await
}

pub async fn deletecollection_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out =
        endpoints::delete_collection(&state, &scope(&state), &auth_ctx.user, None, &params, &body)
            .await?;
    convert_response(out).await
}

/// List, or watch when `?watch=true`; see
/// [`crate::handlers::clustertrustbundle::list_clustertrustbundles`].
pub async fn list_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped_converted::<ClusterTrustBundle>(
            state,
            auth_ctx,
            "clustertrustbundles",
            GROUP,
            crate::handlers::watch::watch_params_from_query(&params),
            watch_converter(),
            bookmark_gvk(),
        )
        .await;
    }

    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "clustertrustbundles").with_api_group(GROUP);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("clustertrustbundles", None);
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;
    let mut items = crate::handlers::list_options::list_items::<ClusterTrustBundle, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new("ClusterTrustBundleList", SERVED_API_VERSION, items);
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    let mut value = serde_json::to_value(&list).map_err(|e| Error::Internal(e.to_string()))?;
    to_v1alpha1(&mut value);
    Ok(crate::audit::json_response(value).into_response())
}

/// Watch `clustertrustbundles` at v1alpha1 (the deprecated `/watch/` route).
pub async fn watch_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<crate::handlers::watch::WatchParams>,
) -> Result<Response> {
    gate()?;
    crate::handlers::watch::watch_cluster_scoped_converted::<ClusterTrustBundle>(
        state,
        auth_ctx,
        "clustertrustbundles",
        GROUP,
        params,
        watch_converter(),
        bookmark_gvk(),
    )
    .await
}
