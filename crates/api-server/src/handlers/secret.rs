//! Secret endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the Secret strategy
//! ([`crate::registry::core::secret`]) — upstream's
//! `pkg/registry/core/secret/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Reads (get, list,
//! watch) are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
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
    resources::{PodSpec, Secret},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// K8s default file-mode bits for Secret/ConfigMap/DownwardAPI/Projected
/// volumes when `defaultMode` is omitted by the client. Decimal 420 == 0o644.
///
/// K8s ref: `pkg/apis/core/v1/defaults.go`:
/// - `SetDefaults_SecretVolumeSource`
/// - `SetDefaults_ConfigMapVolumeSource`
/// - `SetDefaults_DownwardAPIVolumeSource`
/// - `SetDefaults_ProjectedVolumeSource`
#[allow(dead_code)]
pub const DEFAULT_VOLUME_FILE_MODE: i32 = 0o644;

/// Apply K8s defaulting for volume `defaultMode` bits to every Secret,
/// ConfigMap, DownwardAPI, and Projected volume source on a [`PodSpec`].
///
/// Per-item `mode` (on `KeyToPath` / `DownwardAPIVolumeFile`) is intentionally
/// left untouched — the kubelet falls back to `defaultMode` when a per-item
/// mode is unset, which is the K8s contract.
///
/// Explicit caller-supplied values (including `0`) are never overwritten:
/// only `None` is defaulted. This matches the upstream Go defaulting which
/// only fills in nil pointers.
#[allow(dead_code)]
pub fn apply_volume_mode_defaults(spec: &mut PodSpec) {
    let Some(volumes) = spec.volumes.as_mut() else {
        return;
    };
    for vol in volumes.iter_mut() {
        if let Some(sv) = vol.secret.as_mut() {
            if sv.default_mode.is_none() {
                sv.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(cm) = vol.config_map.as_mut() {
            if cm.default_mode.is_none() {
                cm.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(dapi) = vol.downward_api.as_mut() {
            if dapi.default_mode.is_none() {
                dapi.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(proj) = vol.projected.as_mut() {
            if proj.default_mode.is_none() {
                proj.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
    }
}

/// The Secret `RequestScope`: `v1` `Secret` served as `secrets`, backed by
/// `secret.NewREST`'s store.
fn scope(state: &ApiServerState) -> RequestScope<Secret> {
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "Secret".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "secrets".to_string(),
        },
        subresource: None,
        store: crate::registry::core::secret::new_store(state.storage.clone()),
        apply: Some(crate::ssa::apply_secret),
        convert_to_internal: Some(crate::registry::core::secret::convert_to_internal),
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
        &scope(&state),
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
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
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

pub async fn patch(
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

pub async fn delete_secret(
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

pub async fn deletecollection_secrets(
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

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_namespaced::<Secret>(
            state,
            auth_ctx,
            namespace,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing secrets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", Some(&namespace));
    let mut secrets: Vec<Secret> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let mut list = List::new("SecretList", "v1", secrets);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all secrets across all namespaces
pub async fn list_all_secrets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_cluster_scoped::<Secret>(
            state,
            auth_ctx,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all secrets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", None);
    let mut secrets = state.storage.list::<Secret>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let mut list = List::new("SecretList", "v1", secrets);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
