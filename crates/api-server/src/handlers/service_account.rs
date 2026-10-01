//! ServiceAccount endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the ServiceAccount strategy
//! ([`crate::registry::core::serviceaccount`]) — upstream's
//! `pkg/registry/core/serviceaccount/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::serviceaccount;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    auth::ServiceAccountClaims,
    authz::{Decision, RequestAttributes},
    resources::{Secret, ServiceAccount},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// The ServiceAccount `RequestScope`: core `v1` `ServiceAccount` served as
/// `serviceaccounts`, backed by `serviceaccount.NewREST`.
fn scope(state: &ApiServerState) -> RequestScope<ServiceAccount> {
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "ServiceAccount".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "serviceaccounts".to_string(),
        },
        subresource: None,
        store: Box::new(serviceaccount::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<ServiceAccount>),
        convert_to_internal: None,
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

/// Rusternetes-specific (upstream stopped auto-generating token Secrets in
/// 1.24, `LegacyServiceAccountTokenNoAutoGeneration`): a created ServiceAccount
/// gets a long-lived token Secret right away rather than at the next
/// serviceaccount-controller pass. A failure here never fails the create.
async fn create_token_secret(state: &ApiServerState, namespace: &str, created: &ServiceAccount) {
    let namespace = namespace.to_string();
    // Generate ServiceAccount token and store it in a Secret
    let sa_uid = created.metadata.uid.clone();
    let sa_name = created.metadata.name.clone();
    info!("ServiceAccount UID: {}, Name: {}", sa_uid, sa_name);

    // Generate JWT token (valid for 10 years - Kubernetes default for static tokens)
    let claims = ServiceAccountClaims::new(
        sa_name.clone(),
        namespace.clone(),
        sa_uid.clone(),
        87600, // 10 years in hours
    );

    let token = match state.token_manager.generate_token(claims) {
        Ok(token) => token,
        Err(e) => {
            info!("Warning: Failed to generate ServiceAccount token {sa_name}: {e}");
            return;
        }
    };

    // Create Secret to store the token
    let secret_name = format!("{}-token", sa_name);
    let mut string_data = HashMap::new();
    string_data.insert("token".to_string(), token.clone());
    string_data.insert("namespace".to_string(), namespace.clone());

    // Add CA certificate if available
    if let Some(ref ca_cert) = state.ca_cert_pem {
        string_data.insert("ca.crt".to_string(), ca_cert.clone());
    }

    let mut secret =
        Secret::new(&secret_name, &namespace).with_type("kubernetes.io/service-account-token");

    // Add labels and annotations
    secret.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert(
            "kubernetes.io/service-account.name".to_string(),
            sa_name.clone(),
        );
        labels
    });
    secret.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert(
            "kubernetes.io/service-account.uid".to_string(),
            sa_uid.clone(),
        );
        annotations
    });
    secret.string_data = Some(string_data);

    // Normalize: convert stringData to base64-encoded data before storing
    secret.normalize();

    // Store the secret
    let secret_key = build_key("secrets", Some(&namespace), &secret_name);
    info!(
        "Attempting to create ServiceAccount token secret: {}",
        secret_name
    );
    match state.storage.create(&secret_key, &secret).await {
        Ok(_) => {
            info!(
                "Successfully created ServiceAccount token secret: {}",
                secret_name
            );
        }
        Err(e) => {
            info!(
                "Warning: Failed to create ServiceAccount token secret {}: {}",
                secret_name, e
            );
            // Don't fail the ServiceAccount creation if secret creation fails
        }
    }
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let response = endpoints::create_resource(
        &state,
        &scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await?;
    if response.status() != StatusCode::CREATED || crate::handlers::dryrun::is_dry_run(&params) {
        return Ok(response);
    }
    let (parts, resp_body) = response.into_parts();
    let bytes = axum::body::to_bytes(resp_body, usize::MAX)
        .await
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    if let Ok(created) = serde_json::from_slice::<ServiceAccount>(&bytes) {
        create_token_secret(&state, &namespace, &created).await;
    }
    Ok(Response::from_parts(parts, axum::body::Body::from(bytes)))
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

pub async fn delete_service_account(
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

pub async fn deletecollection_serviceaccounts(
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
        return crate::handlers::watch::watch_namespaced::<ServiceAccount>(
            state,
            auth_ctx,
            namespace,
            "serviceaccounts",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing service accounts in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("serviceaccounts", Some(&namespace));
    let mut service_accounts: Vec<ServiceAccount> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut service_accounts, &params)?;

    let mut list = List::new("ServiceAccountList", "v1", service_accounts);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all serviceaccounts across all namespaces
pub async fn list_all_serviceaccounts(
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
        return crate::handlers::watch::watch_cluster_scoped::<ServiceAccount>(
            state,
            auth_ctx,
            "serviceaccounts",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all serviceaccounts");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "serviceaccounts").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("serviceaccounts", None);
    let mut service_accounts = state.storage.list::<ServiceAccount>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut service_accounts, &params)?;

    let mut list = List::new("ServiceAccountList", "v1", service_accounts);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
