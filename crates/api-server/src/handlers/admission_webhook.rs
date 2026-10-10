//! ValidatingWebhookConfiguration and MutatingWebhookConfiguration endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::admissionregistration::webhookconfiguration`] — upstream's
//! `pkg/registry/admissionregistration/{validating,mutating}webhookconfiguration/storage`
//! wired into `endpoints/handlers/{create,update,patch,delete}.go`. Lists and
//! watches are still served here directly.

use crate::endpoints::handlers::RequestScope;
use crate::registry::admissionregistration::webhookconfiguration::{mutating, validating};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration},
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::sync::Arc;
use tracing::debug;

/// The six CRUD handlers of a webhook-configuration resource: the same
/// `endpoints::handlers` calls as
/// [`crate::handlers::validating_admission_policy::store_crud_handlers`], with
/// the response run through `endpoints::negotiate` (upstream's
/// `transformResponseObject` serializer choice, `endpoints/handlers/response.go`)
/// so `Accept: application/vnd.kubernetes.protobuf` gets a protobuf body.
macro_rules! webhook_crud_handlers {
    (
        scope: $scope:ident,
        create: $create:ident,
        get: $get:ident,
        update: $update:ident,
        patch: $patch:ident,
        delete: $delete:ident,
        deletecollection: $deletecollection:ident $(,)?
    ) => {
        pub async fn $create(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::create_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &params,
                &body,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }

        pub async fn $get(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::get_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }

        pub async fn $update(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::update_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                &body,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }

        pub async fn $patch(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::patch_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                crate::handlers::validating_admission_policy::patch_content_type(&headers),
                &body,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }

        pub async fn $delete(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Path(name): axum::extract::Path<String>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::delete_resource(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &name,
                &params,
                &body,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }

        pub async fn $deletecollection(
            State(state): State<Arc<ApiServerState>>,
            Extension(auth_ctx): Extension<AuthContext>,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> Result<Response> {
            let out = crate::endpoints::handlers::delete_collection(
                &state,
                &$scope(&state),
                &auth_ctx.user,
                None,
                &params,
                &body,
            )
            .await?;
            Ok(crate::endpoints::handlers::negotiate(&headers, out).await)
        }
    };
}

// ===== ValidatingWebhookConfiguration =====

/// The ValidatingWebhookConfiguration `RequestScope`, backed by its `NewREST`.
fn validating_scope(state: &ApiServerState) -> RequestScope<ValidatingWebhookConfiguration> {
    RequestScope {
        kind: GroupVersionKind {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
        },
        resource: GroupVersionResource {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "validatingwebhookconfigurations".to_string(),
        },
        subresource: None,
        store: Box::new(validating::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<ValidatingWebhookConfiguration>),
        convert_to_internal: Some(validating::convert_to_internal),
        patch_conversion: None,
    }
}

webhook_crud_handlers!(
    scope: validating_scope,
    create: create_validating_webhook,
    get: get_validating_webhook,
    update: update_validating_webhook,
    patch: patch_validating_webhook,
    delete: delete_validating_webhook,
    deletecollection: deletecollection_validatingwebhookconfigurations,
);

pub async fn list_validating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingWebhookConfiguration>(
            state,
            auth_ctx,
            "validatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingwebhookconfigurations", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut configs = state
        .storage
        .list::<ValidatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "ValidatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(crate::endpoints::handlers::negotiate(&headers, Json(list).into_response()).await)
}

// ===== MutatingWebhookConfiguration =====

/// The MutatingWebhookConfiguration `RequestScope`, backed by its `NewREST`.
fn mutating_scope(state: &ApiServerState) -> RequestScope<MutatingWebhookConfiguration> {
    RequestScope {
        kind: GroupVersionKind {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "MutatingWebhookConfiguration".to_string(),
        },
        resource: GroupVersionResource {
            group: "admissionregistration.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "mutatingwebhookconfigurations".to_string(),
        },
        subresource: None,
        store: Box::new(mutating::new_store(state.storage.clone())),
        apply: Some(crate::ssa::apply_legacy::<MutatingWebhookConfiguration>),
        convert_to_internal: Some(mutating::convert_to_internal),
        patch_conversion: None,
    }
}

webhook_crud_handlers!(
    scope: mutating_scope,
    create: create_mutating_webhook,
    get: get_mutating_webhook,
    update: update_mutating_webhook,
    patch: patch_mutating_webhook,
    delete: delete_mutating_webhook,
    deletecollection: deletecollection_mutatingwebhookconfigurations,
);

pub async fn list_mutating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<MutatingWebhookConfiguration>(
            state,
            auth_ctx,
            "mutatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing MutatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("mutatingwebhookconfigurations", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut configs = state
        .storage
        .list::<MutatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "MutatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(crate::endpoints::handlers::negotiate(&headers, Json(list).into_response()).await)
}
