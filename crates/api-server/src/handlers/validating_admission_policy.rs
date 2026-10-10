//! ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::admissionregistration`] — upstream's
//! `pkg/registry/admissionregistration/{validatingadmissionpolicy,validatingadmissionpolicybinding}/storage`
//! wired into `endpoints/handlers/{create,update,patch,delete}.go`. Lists and
//! watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::admissionregistration::{
    validatingadmissionpolicy, validatingadmissionpolicybinding,
};
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
    resources::{ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
pub(crate) fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// The create / get / update / patch / delete / deletecollection handlers of a
/// cluster-scoped resource served by a [`RequestScope`]: thin wrappers over
/// `endpoints::{create,get,update,patch,delete}_resource` and
/// `delete_collection`, as in the sibling per-resource handler files. An
/// optional trailing `gate: path` is a `fn() -> Result<()>` run first by every
/// handler (the 404 of a resource whose gate is off).
macro_rules! store_crud_handlers {
    (
        scope: $scope:ident,
        create: $create:ident,
        get: $get:ident,
        update: $update:ident,
        patch: $patch:ident,
        delete: $delete:ident,
        deletecollection: $deletecollection:ident
        $(, gate: $gate:path)?
        $(, convert: $convert:path)? $(,)?
    ) => {
        pub async fn $create(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::create_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &params,
                &body,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }

        pub async fn $get(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::get_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }

        pub async fn $update(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::update_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                &body,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }

        pub async fn $patch(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::patch_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                $crate::handlers::validating_admission_policy::patch_content_type(&headers),
                &body,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }

        pub async fn $delete(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::delete_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                &body,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }

        pub async fn $deletecollection(
            axum::extract::State(state): axum::extract::State<
                std::sync::Arc<$crate::state::ApiServerState>,
            >,
            axum::Extension(auth_ctx): axum::Extension<$crate::middleware::AuthContext>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::response::Response> {
            $( $gate()?; )?
            let out = $crate::endpoints::handlers::delete_collection(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &params,
                &body,
            )
            .await?;
            $( let out = $convert(out).await?; )?
            Ok(out)
        }
    };
}
pub(crate) use store_crud_handlers;

// ===== ValidatingAdmissionPolicy =====

/// The ValidatingAdmissionPolicy `RequestScope`: `admissionregistration.k8s.io/v1`
/// served as `validatingadmissionpolicies`, backed by the policy `NewREST`
/// (the status store for the `status` subresource).
fn policy_scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<ValidatingAdmissionPolicy> {
    let (store, status_store) =
        validatingadmissionpolicy::new_stores(state.storage.clone(), state.authorizer.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "ValidatingAdmissionPolicy".to_string(),
        },
        resource: GroupVersionResource {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "validatingadmissionpolicies".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<ValidatingAdmissionPolicy>),
        convert_to_internal: Some(validatingadmissionpolicy::convert_to_internal),
        patch_conversion: None,
    }
}

fn policy_main_scope(state: &ApiServerState) -> RequestScope<ValidatingAdmissionPolicy> {
    policy_scope(state, None)
}

store_crud_handlers!(
    scope: policy_main_scope,
    create: create_validating_admission_policy,
    get: get_validating_admission_policy,
    update: update_validating_admission_policy,
    patch: patch_validating_admission_policy,
    delete: delete_validating_admission_policy,
    deletecollection: deletecollection_validatingadmissionpolicies,
);

pub async fn list_validating_admission_policies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingAdmissionPolicy>(
            state,
            auth_ctx,
            "validatingadmissionpolicies",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingAdmissionPolicies");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingadmissionpolicies", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut policies = state
        .storage
        .list::<ValidatingAdmissionPolicy>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut policies, &params)?;

    let mut list = List::new(
        "ValidatingAdmissionPolicyList",
        "admissionregistration.k8s.io/v1",
        policies,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// GET `/status`.
pub async fn get_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await
}

/// PUT `/status`.
pub async fn update_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/status`.
pub async fn patch_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

// ===== ValidatingAdmissionPolicyBinding =====

/// The ValidatingAdmissionPolicyBinding `RequestScope`, backed by the binding
/// `NewREST`.
fn binding_scope(state: &ApiServerState) -> RequestScope<ValidatingAdmissionPolicyBinding> {
    RequestScope {
        kind: GroupVersionKind {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "ValidatingAdmissionPolicyBinding".to_string(),
        },
        resource: GroupVersionResource {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "validatingadmissionpolicybindings".to_string(),
        },
        subresource: None,
        store: Box::new(validatingadmissionpolicybinding::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<ValidatingAdmissionPolicyBinding>),
        convert_to_internal: Some(validatingadmissionpolicybinding::convert_to_internal),
        patch_conversion: None,
    }
}

store_crud_handlers!(
    scope: binding_scope,
    create: create_validating_admission_policy_binding,
    get: get_validating_admission_policy_binding,
    update: update_validating_admission_policy_binding,
    patch: patch_validating_admission_policy_binding,
    delete: delete_validating_admission_policy_binding,
    deletecollection: deletecollection_validatingadmissionpolicybindings,
);

pub async fn list_validating_admission_policy_bindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingAdmissionPolicyBinding>(
            state,
            auth_ctx,
            "validatingadmissionpolicybindings",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingAdmissionPolicyBindings");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicybindings")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingadmissionpolicybindings", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut bindings = state
        .storage
        .list::<ValidatingAdmissionPolicyBinding>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut bindings, &params)?;

    let mut list = List::new(
        "ValidatingAdmissionPolicyBindingList",
        "admissionregistration.k8s.io/v1",
        bindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}
