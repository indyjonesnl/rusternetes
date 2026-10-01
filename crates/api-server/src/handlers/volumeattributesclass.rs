//! VolumeAttributesClass endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the VolumeAttributesClass strategy
//! ([`crate::registry::storage::volumeattributesclass`]) — upstream's
//! `pkg/registry/storage/volumeattributesclass/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::storage::volumeattributesclass;
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
    resources::csi::VolumeAttributesClass,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The VolumeAttributesClass `RequestScope`: `storage.k8s.io/v1` `VolumeAttributesClass` served as
/// `volumeattributesclasses`, backed by `volumeattributesclass.NewREST`.
fn scope(state: &ApiServerState) -> RequestScope<VolumeAttributesClass> {
    RequestScope {
        kind: GroupVersionKind {
            group: "storage.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "VolumeAttributesClass".to_string(),
        },
        resource: GroupVersionResource {
            group: "storage.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "volumeattributesclasses".to_string(),
        },
        subresource: None,
        store: Box::new(volumeattributesclass::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<VolumeAttributesClass>),
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

pub async fn create_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

pub async fn get_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Response> {
    endpoints::get_resource(&state, &scope(&state), &auth_ctx.user, None, &name).await
}

pub async fn update_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

pub async fn patch_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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

pub async fn delete_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

pub async fn deletecollection_volumeattributesclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(&state, &scope(&state), &auth_ctx.user, None, &params, &body).await
}

pub async fn list_volumeattributesclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all VolumeAttributesClasses");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeAttributesClass>(
            state,
            auth_ctx,
            "volumeattributesclasses",
            "storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumeattributesclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumeattributesclasses", None);
    let mut vacs = state.storage.list::<VolumeAttributesClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vacs, &params)?;

    let mut list = List::new("VolumeAttributesClassList", "storage.k8s.io/v1", vacs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    fn create_test_vac(name: &str) -> VolumeAttributesClass {
        let mut params = HashMap::new();
        params.insert("type".to_string(), "ssd".to_string());
        params.insert("iops".to_string(), "3000".to_string());

        VolumeAttributesClass {
            type_meta: TypeMeta {
                kind: "VolumeAttributesClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            driver_name: "test-driver".to_string(),
            parameters: Some(params),
        }
    }

    #[tokio::test]
    async fn test_volumeattributesclass_serialization() {
        let vac = create_test_vac("fast-storage");
        let json = serde_json::to_string(&vac).unwrap();
        let deserialized: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "fast-storage");
        assert_eq!(deserialized.driver_name, "test-driver");
    }
}
