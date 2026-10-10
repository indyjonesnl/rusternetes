//! CustomResourceDefinition endpoints.
//!
//! Writes, and the `/status` subresource, go through the generic endpoint
//! handlers and [`crate::registry::generic::Store`] with the
//! CustomResourceDefinition strategies
//! ([`crate::registry::apiextensions::customresourcedefinition`]) — the
//! apiextensions-apiserver's `pkg/registry/customresourcedefinition` wired
//! into `endpoints/handlers/{create,update,patch,delete}.go`. Lists and
//! watches are still served here directly.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::apiextensions::customresourcedefinition;
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
    resources::CustomResourceDefinition,
    List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The CustomResourceDefinition `RequestScope`: `apiextensions.k8s.io/v1`
/// `CustomResourceDefinition` served as `customresourcedefinitions` (or its
/// `/status`), backed by `customresourcedefinition.NewREST` and
/// `NewStatusREST`.
fn scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<CustomResourceDefinition> {
    let store: Box<dyn crate::registry::rest::RestStorage<CustomResourceDefinition>> =
        match subresource {
            Some(_) => Box::new(customresourcedefinition::new_status_store(
                state.storage.clone(),
            )),
            None => Box::new(customresourcedefinition::new_rest(state.storage.clone())),
        };
    RequestScope {
        kind: GroupVersionKind {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
        },
        resource: GroupVersionResource {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "customresourcedefinitions".to_string(),
        },
        subresource,
        store,
        apply: Some(crate::ssa::apply_legacy::<CustomResourceDefinition>),
        convert_to_internal: Some(customresourcedefinition::convert_to_internal),
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

pub async fn create_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

pub async fn get_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
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

pub async fn update_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

pub async fn patch_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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

pub async fn delete_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

pub async fn deletecollection_customresourcedefinitions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

/// GET `/status`: `StatusREST.Get` is the store's `Get` (etcd.go:195-198).
pub async fn get_crd_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
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

/// PUT `/status`.
pub async fn update_crd_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
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

/// PATCH `/status`.
pub async fn patch_crd_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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

/// List all CustomResourceDefinitions
pub async fn list_crds(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped_json(
            state,
            auth_ctx,
            "customresourcedefinitions",
            "apiextensions.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all CustomResourceDefinitions");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("customresourcedefinitions", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;

    let mut crds = state
        .storage
        .list::<CustomResourceDefinition>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut crds, &params)?;

    let mut list = List::new(
        "CustomResourceDefinitionList",
        "apiextensions.k8s.io/v1",
        crds,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;

    fn create_test_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    deprecated: None,
                    deprecation_warning: None,
                    schema: None,
                    subresources: None,
                    additional_printer_columns: None,
                    selectable_fields: None,
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_validating_webhook_called_for_crd_creation() {
        use crate::admission_webhook::AdmissionWebhookManager;
        use rusternetes_common::admission::{
            GroupVersionKind, GroupVersionResource, Operation, UserInfo,
        };
        use rusternetes_common::resources::{
            FailurePolicy, OperationType, Rule, RuleWithOperations, SideEffectClass,
            ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
        };
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a ValidatingWebhookConfiguration targeting CRDs with FailurePolicy::Fail
        let webhook_config = ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new("deny-crd-creation"),
            webhooks: Some(vec![ValidatingWebhook {
                name: "deny-crd.example.com".to_string(),
                client_config: WebhookClientConfig {
                    url: Some("https://127.0.0.1:19443/deny-crd".to_string()),
                    service: None,
                    ca_bundle: None,
                },
                rules: vec![RuleWithOperations {
                    operations: vec![OperationType::Create],
                    rule: Rule {
                        api_groups: vec!["apiextensions.k8s.io".to_string()],
                        api_versions: vec!["v1".to_string()],
                        resources: vec!["customresourcedefinitions".to_string()],
                        scope: None,
                    },
                }],
                failure_policy: Some(FailurePolicy::Fail),
                match_policy: None,
                namespace_selector: None,
                object_selector: None,
                side_effects: SideEffectClass::None,
                timeout_seconds: Some(1),
                admission_review_versions: vec!["v1".to_string()],
                match_conditions: None,
            }]),
        };

        // Store the webhook configuration
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-crd-creation",
                &webhook_config,
            )
            .await
            .unwrap();

        // Build a CRD object and webhook parameters
        let crd = create_test_crd();
        let crd_value = serde_json::to_value(&crd).unwrap();

        let gvk = GroupVersionKind {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "customresourcedefinitions".to_string(),
        };
        let user_info = UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        };

        // Call run_validating_webhooks — the webhook URL is unreachable,
        // and FailurePolicy is Fail, so this should return an error,
        // proving the webhook infrastructure was consulted for CRDs.
        let result = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                None,
                &crd.metadata.name,
                Some(crd_value),
                None,
                &user_info,
            )
            .await;

        assert!(
            result.is_err(),
            "Expected webhook call to fail (proving webhook was consulted for CRD creation), but got: {:?}",
            result
        );
    }
}
