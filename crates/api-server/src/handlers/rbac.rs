//! RBAC endpoints: Role, RoleBinding, ClusterRole, ClusterRoleBinding.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the RBAC strategies
//! ([`crate::registry::rbac`]) — upstream's `pkg/registry/rbac/<kind>/storage`
//! and `policybased` wrappers wired into `endpoints/handlers/{create,update,
//! patch,delete}.go`. Lists and watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::rbac::{clusterrole, clusterrolebinding, role, rolebinding};
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
    resources::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

fn gvk(kind: &str) -> GroupVersionKind {
    GroupVersionKind {
        group: "rbac.authorization.k8s.io".to_string(),
        version: "v1".to_string(),
        kind: kind.to_string(),
    }
}

fn gvr(resource: &str) -> GroupVersionResource {
    GroupVersionResource {
        group: "rbac.authorization.k8s.io".to_string(),
        version: "v1".to_string(),
        resource: resource.to_string(),
    }
}

/// The Role `RequestScope`: `rbac.authorization.k8s.io/v1` `Role` served
/// as `roles`, backed by `role::new_store` (the policybased storage).
fn role_scope(state: &ApiServerState) -> RequestScope<Role> {
    RequestScope {
        kind: gvk("Role"),
        resource: gvr("roles"),
        subresource: None,
        store: Box::new(role::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<Role>),
        convert_to_internal: None,
    }
}

pub async fn create_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn get_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

pub async fn update_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &role_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn list_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<Role>(
            state,
            auth_ctx,
            namespace,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing roles in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", Some(&namespace));
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let mut list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all roles across all namespaces
pub async fn list_all_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<Role>(
            state,
            auth_ctx,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all roles");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", None);
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let mut list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// The RoleBinding `RequestScope`: `rbac.authorization.k8s.io/v1` `RoleBinding` served
/// as `rolebindings`, backed by `rolebinding::new_store` (the policybased storage).
fn rolebinding_scope(state: &ApiServerState) -> RequestScope<RoleBinding> {
    RequestScope {
        kind: gvk("RoleBinding"),
        resource: gvr("rolebindings"),
        subresource: None,
        store: Box::new(rolebinding::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<RoleBinding>),
        convert_to_internal: Some(rolebinding::convert_to_internal),
    }
}

pub async fn create_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn get_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

pub async fn update_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &rolebinding_scope(&state),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

pub async fn list_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<RoleBinding>(
            state,
            auth_ctx,
            namespace,
            "rolebindings",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing rolebindings in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", Some(&namespace));
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let mut list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all rolebindings across all namespaces
pub async fn list_all_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Honor `?watch=true` on the all-namespaces collection (informer/Lens path).
    // Mirrors list_all_roles: the all-NS watch is just the no-namespace prefix,
    // so it routes through watch_cluster_scoped.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<RoleBinding>(
            state,
            auth_ctx,
            "rolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing all rolebindings");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", None);
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let mut list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// The ClusterRole `RequestScope`: `rbac.authorization.k8s.io/v1` `ClusterRole` served
/// as `clusterroles`, backed by `clusterrole::new_store` (the policybased storage).
fn clusterrole_scope(state: &ApiServerState) -> RequestScope<ClusterRole> {
    RequestScope {
        kind: gvk("ClusterRole"),
        resource: gvr("clusterroles"),
        subresource: None,
        store: Box::new(clusterrole::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<ClusterRole>),
        convert_to_internal: None,
    }
}

pub async fn create_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn get_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &name,
    )
    .await
}

pub async fn update_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &clusterrole_scope(&state),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn list_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterroles");

    // Honor `?watch=true` on the collection endpoint (what Lens / client-go
    // informers use). Without this the legacy `/watch/clusterroles` subpath is
    // the only way to stream, so list-then-watch clients never see updates.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRole>(
            state,
            auth_ctx,
            "clusterroles",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterroles", None);
    let mut clusterroles = state.storage.list::<ClusterRole>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterroles, &params)?;

    let mut list = List::new(
        "ClusterRoleList",
        "rbac.authorization.k8s.io/v1",
        clusterroles,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// The ClusterRoleBinding `RequestScope`: `rbac.authorization.k8s.io/v1` `ClusterRoleBinding` served
/// as `clusterrolebindings`, backed by `clusterrolebinding::new_store` (the policybased storage).
fn clusterrolebinding_scope(state: &ApiServerState) -> RequestScope<ClusterRoleBinding> {
    RequestScope {
        kind: gvk("ClusterRoleBinding"),
        resource: gvr("clusterrolebindings"),
        subresource: None,
        store: Box::new(clusterrolebinding::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<ClusterRoleBinding>),
        convert_to_internal: Some(clusterrolebinding::convert_to_internal),
    }
}

pub async fn create_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::create_resource(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn get_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &name,
    )
    .await
}

pub async fn update_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn patch_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &clusterrolebinding_scope(&state),
        &auth_ctx.user,
        None,
        &params,
        &body,
    )
    .await
}

pub async fn list_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterrolebindings");

    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRoleBinding>(
            state,
            auth_ctx,
            "clusterrolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterrolebindings", None);
    let mut clusterrolebindings = state.storage.list::<ClusterRoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterrolebindings, &params)?;

    let mut list = List::new(
        "ClusterRoleBindingList",
        "rbac.authorization.k8s.io/v1",
        clusterrolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
