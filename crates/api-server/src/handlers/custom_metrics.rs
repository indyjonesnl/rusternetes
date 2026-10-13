use crate::handlers::metric_selectors::{
    decode_list_options, decode_metric_label_selector, metric_not_found, metric_selector_echo,
    unknown_error,
};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    label_selector::LabelSelector,
    resources::custom_metrics::{MetricSelector, MetricValue, MetricValueList, ObjectReference},
    Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// TypeMeta for the custom-metrics group. Flattened into the value/list structs
/// since #1916, so a response still names its type while a body that omits
/// apiVersion/kind still decodes.
fn metric_value_type_meta(kind: &str) -> rusternetes_common::types::TypeMeta {
    rusternetes_common::types::TypeMeta {
        api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
        kind: kind.to_string(),
    }
}

/// PromQL matchers for the metric label selector; an unrepresentable
/// requirement is an error (Rusternetes-only mapping, see
/// `prometheus_client::promql_matchers`).
fn matchers_for(metric_selector: &LabelSelector) -> Result<Vec<String>> {
    crate::prometheus_client::promql_matchers(metric_selector)
        .map_err(|e| unknown_error(e.to_string()))
}

/// The storage collection for a request `resource`, which `requestInfo`
/// carries as `plural[.group]` (reststorage.go:110-113 `ParseGroupResource`).
fn storage_resource(resource: &str) -> &str {
    resource.split('.').next().unwrap_or(resource)
}

/// The provider's object lookup: names of the `resource` objects in
/// `namespace` matching `selector` (`helpers.ListObjectNames`,
/// pkg/provider/helpers/helpers.go; `GetMetricBySelector` runs it first).
async fn selected_object_names(
    state: &ApiServerState,
    namespace: &str,
    resource: &str,
    selector: &LabelSelector,
) -> Result<Vec<String>> {
    let prefix = build_prefix(storage_resource(resource), Some(namespace));
    let objects: Vec<serde_json::Value> = state.storage.as_ref().list(&prefix).await?;
    let mut names: Vec<String> = objects
        .iter()
        .filter(|o| selector.matches_resource(o))
        .filter_map(|o| o["metadata"]["name"].as_str().map(str::to_string))
        .collect();
    names.sort();
    Ok(names)
}

fn metric_value(
    namespace: &str,
    resource: &str,
    name: String,
    metric_name: &str,
    value: String,
    selector: Option<MetricSelector>,
) -> MetricValue {
    MetricValue {
        type_meta: metric_value_type_meta("MetricValue"),
        described_object: ObjectReference {
            kind: capitalize(resource),
            namespace: Some(namespace.to_string()),
            name,
            api_version: Some("v1".to_string()),
        },
        metric_name: metric_name.to_string(),
        timestamp: Utc::now(),
        window: Some("60s".to_string()),
        value,
        selector,
    }
}

/// `GetMetricByName` (handleIndividualOp, reststorage.go:136-149).
///
/// Prometheus when configured. Otherwise the built-in series model of the
/// custom-metrics-apiserver test adapter (`test-adapter/provider/provider.go:
/// 215-235`): the object must exist (`NewMetricNotFoundForError`) and the
/// series, which carries no labels, must satisfy the metric label selector
/// (`NewMetricNotFoundForSelectorError`).
async fn get_metric_by_name(
    state: &ApiServerState,
    namespace: &str,
    resource: &str,
    name: &str,
    metric_name: &str,
    metric_selector: &LabelSelector,
) -> Result<MetricValue> {
    let value = if let Some(ref prometheus_client) = state.prometheus_client {
        let matchers = matchers_for(metric_selector)?;
        match prometheus_client
            .query_object_metric(metric_name, namespace, resource, name, &matchers)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to query Prometheus for metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                "0".to_string()
            }
        }
    } else {
        let key = build_key(storage_resource(resource), Some(namespace), name);
        if state
            .storage
            .as_ref()
            .get::<serde_json::Value>(&key)
            .await
            .is_err()
        {
            return Err(metric_not_found(format!(
                "the server could not find the metric {metric_name} for {resource} {name}"
            )));
        }
        if !metric_selector.matches(&HashMap::new()) {
            return Err(metric_not_found(format!(
                "the server could not find the metric {metric_name} for {resource} {name} with selector {metric_selector}"
            )));
        }
        "100".to_string()
    };
    let echo = metric_selector_echo(metric_selector)?;
    Ok(metric_value(
        namespace,
        resource,
        name.to_string(),
        metric_name,
        value,
        echo,
    ))
}

/// Get a custom metric value for a specific object
pub async fn get_custom_metric(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, resource_type, resource_name, metric_name)): Path<(
        String,
        String,
        String,
        String,
    )>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MetricValue>> {
    info!(
        "Getting custom metric {} for {}/{}/{}",
        metric_name, namespace, resource_type, resource_name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource_type)
        .with_api_group("custom.metrics.k8s.io")
        .with_namespace(&namespace)
        .with_name(&resource_name)
        .with_subresource(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // ListResource decode: a fieldSelector naming another object is a 400.
    decode_list_options(&params, Some(&resource_name))?;
    let metric_selector = decode_metric_label_selector(&params)?;

    Ok(Json(
        get_metric_by_name(
            &state,
            &namespace,
            &resource_type,
            &resource_name,
            &metric_name,
            &metric_selector,
        )
        .await?,
    ))
}

/// List custom metric values for multiple objects of the same type
pub async fn list_custom_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, resource_type, metric_name)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MetricValueList>> {
    info!(
        "Listing custom metric {} for {}/{}",
        metric_name, namespace, resource_type
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", &resource_type)
        .with_api_group("custom.metrics.k8s.io")
        .with_namespace(&namespace)
        .with_subresource(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    crate::handlers::validate_synthesized_list_options(&params)?;
    // get.go:84-126 decode, then reststorage.go List.
    let opts = decode_list_options(&params, None)?;
    let metric_selector = decode_metric_label_selector(&params)?;

    let items = if let Some(name) = opts.name.as_deref() {
        // `name != "*"`: handleIndividualOp; the object labelSelector is not
        // consulted.
        vec![
            get_metric_by_name(
                &state,
                &namespace,
                &resource_type,
                name,
                &metric_name,
                &metric_selector,
            )
            .await?,
        ]
    } else {
        // handleWildcardOp -> GetMetricBySelector(namespace, selector, ...).
        let objects =
            selected_object_names(&state, &namespace, &resource_type, &opts.object_selector)
                .await?;
        let mut series: Vec<(String, String)> = Vec::new();
        if let Some(ref prometheus_client) = state.prometheus_client {
            let matchers = matchers_for(&metric_selector)?;
            match prometheus_client
                .query_list_metric(&metric_name, &namespace, &resource_type, &matchers)
                .await
            {
                Ok(mut found) => {
                    for object in objects {
                        if let Some(value) = found.remove(&object) {
                            series.push((object, value));
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to query Prometheus for list metric {}: {}. Using fallback values.",
                        metric_name, e
                    );
                }
            }
        } else if metric_selector.matches(&HashMap::new()) {
            // Test-adapter series model: every selected object has a series
            // without labels (provider.go metricsFor/valueFor).
            series.extend(objects.into_iter().map(|o| (o, "100".to_string())));
        }
        if series.is_empty() {
            Vec::new()
        } else {
            let echo = metric_selector_echo(&metric_selector)?;
            series
                .into_iter()
                .map(|(name, value)| {
                    metric_value(
                        &namespace,
                        &resource_type,
                        name,
                        &metric_name,
                        value,
                        echo.clone(),
                    )
                })
                .collect()
        }
    };

    let metric_list = MetricValueList {
        type_meta: metric_value_type_meta("MetricValueList"),
        // `metav1.ListMeta`. The bespoke predecessor carried only `selfLink`,
        // which upstream stopped populating on every object in 1.20 (#1916).
        metadata: rusternetes_common::types::ListMeta::default(),
        items,
    };

    Ok(Json(metric_list))
}

/// Get a custom metric value for a namespace-scoped metric
pub async fn get_namespace_metric(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, metric_name)): Path<(String, String)>,
) -> Result<Json<MetricValue>> {
    debug!("Getting namespace metric {} for {}", metric_name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "namespaces")
        .with_api_group("custom.metrics.k8s.io")
        .with_name(&namespace)
        .with_subresource(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Query Prometheus for namespace metric (or use mock data if Prometheus not configured)
    let value = if let Some(ref prometheus_client) = state.prometheus_client {
        match prometheus_client
            .query_namespace_metric(&metric_name, &namespace)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to query Prometheus for namespace metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                "0".to_string()
            }
        }
    } else {
        // Fallback mock value when Prometheus is not configured
        "500".to_string()
    };

    let metric_value = MetricValue {
        type_meta: metric_value_type_meta("MetricValue"),
        described_object: ObjectReference {
            kind: "Namespace".to_string(),
            namespace: None,
            name: namespace,
            api_version: Some("v1".to_string()),
        },
        metric_name: metric_name.clone(),
        timestamp: Utc::now(),
        window: Some("60s".to_string()),
        value,
        selector: None,
    };

    Ok(Json(metric_value))
}

/// Get a custom metric value for a cluster-scoped metric
pub async fn get_cluster_metric(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((resource_type, resource_name, metric_name)): Path<(String, String, String)>,
) -> Result<Json<MetricValue>> {
    info!(
        "Getting cluster metric {} for {}/{}",
        metric_name, resource_type, resource_name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource_type)
        .with_api_group("custom.metrics.k8s.io")
        .with_name(&resource_name)
        .with_subresource(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Query Prometheus for cluster-scoped metric (or use mock data if Prometheus not configured)
    let value = if let Some(ref prometheus_client) = state.prometheus_client {
        match prometheus_client
            .query_cluster_metric(
                &metric_name,
                &resource_type,
                &resource_name,
                &[], // No additional matchers
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to query Prometheus for cluster metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                "0".to_string()
            }
        }
    } else {
        // Fallback mock value when Prometheus is not configured
        "200".to_string()
    };

    let metric_value = MetricValue {
        type_meta: metric_value_type_meta("MetricValue"),
        described_object: ObjectReference {
            kind: capitalize(&resource_type),
            namespace: None,
            name: resource_name,
            api_version: Some("v1".to_string()),
        },
        metric_name: metric_name.clone(),
        timestamp: Utc::now(),
        window: Some("60s".to_string()),
        value,
        selector: None,
    };

    Ok(Json(metric_value))
}

/// Helper function to capitalize the first letter of a string
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut result = first.to_uppercase().to_string();
            result.push_str(&chars.as_str().to_lowercase());
            result
        }
    }
}

#[cfg(test)]
#[cfg(feature = "integration-tests")] // Disable tests that require full setup
mod tests {
    use super::*;
    use crate::state::ApiServerState;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use rusternetes_common::storage::MemoryStorage;

    async fn create_test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let token_manager = Arc::new(TokenManager::new(b"test-key"));
        let bootstrap_token_manager = Arc::new(BootstrapTokenManager::new());
        let authorizer = Arc::new(AlwaysAllowAuthorizer);
        let metrics = Arc::new(MetricsRegistry::new());

        Arc::new(ApiServerState::new(
            storage,
            token_manager,
            bootstrap_token_manager,
            authorizer,
            metrics,
            true, // skip_auth for tests
            None, // ca_cert_pem
        ))
    }

    #[tokio::test]
    async fn test_get_custom_metric() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = get_custom_metric(
            State(state),
            Extension(auth_ctx),
            Path((
                "default".to_string(),
                "pods".to_string(),
                "test-pod".to_string(),
                "http_requests".to_string(),
            )),
        )
        .await;

        assert!(result.is_ok());
        let metric = result.unwrap().0;
        assert_eq!(metric.metric_name, "http_requests");
        assert_eq!(metric.described_object.kind, "Pods");
        assert_eq!(metric.described_object.name, "test-pod");
    }

    #[tokio::test]
    async fn test_list_custom_metrics() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = list_custom_metrics(
            State(state),
            Extension(auth_ctx),
            Path((
                "default".to_string(),
                "pods".to_string(),
                "http_requests".to_string(),
            )),
            Query(MetricQuery {
                label_selector: Some("app=nginx".to_string()),
            }),
        )
        .await;

        assert!(result.is_ok());
        let list = result.unwrap().0;
        assert_eq!(list.items.len(), 2);
        assert!(list.items[0].selector.is_some());
    }

    #[tokio::test]
    async fn test_get_namespace_metric() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = get_namespace_metric(
            State(state),
            Extension(auth_ctx),
            Path(("default".to_string(), "cpu_usage".to_string())),
        )
        .await;

        assert!(result.is_ok());
        let metric = result.unwrap().0;
        assert_eq!(metric.metric_name, "cpu_usage");
        assert_eq!(metric.described_object.kind, "Namespace");
        assert_eq!(metric.described_object.name, "default");
    }

    #[tokio::test]
    async fn test_capitalize() {
        assert_eq!(capitalize("pods"), "Pods");
        assert_eq!(capitalize("Pods"), "Pods");
        assert_eq!(capitalize(""), "");
        assert_eq!(capitalize("p"), "P");
    }
}
