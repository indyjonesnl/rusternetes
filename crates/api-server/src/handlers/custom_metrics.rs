use crate::handlers::metric_selectors::{
    decode_list_options, decode_metric_label_selector, metric_not_found, metric_selector_echo,
    unknown_error,
};
use crate::{middleware::AuthContext, state::ApiServerState};
use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    label_selector::LabelSelector,
    resources::custom_metrics::{
        MetricIdentifier, MetricSelector, MetricValue, MetricValueList, ObjectReference,
    },
    Result,
};
use rusternetes_discovery::deferred_restmapper::{
    DeferredRestMapper, DiscoveryClient, MemCacheClient, RESET_PERIOD,
};
use rusternetes_discovery::restmapper::{
    is_builtin_group, served_group_resources, ApiGroupResources, Gvr, MapperError,
};
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend};
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

/// `schema.GroupResource`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupResource {
    group: String,
    resource: String,
}

impl GroupResource {
    /// `schema.ParseGroupResource` (apimachinery/pkg/runtime/schema/
    /// group_version.go:87-92), applied to the request's `plural[.group]`
    /// resource (reststorage.go:110-113).
    fn parse(raw: &str) -> Self {
        match raw.split_once('.') {
            Some((resource, group)) => Self {
                group: group.to_string(),
                resource: resource.to_string(),
            },
            None => Self {
                group: String::new(),
                resource: raw.to_string(),
            },
        }
    }

    fn with_version_empty(&self) -> Gvr {
        Gvr::new(&self.group, "", &self.resource)
    }
}

/// `GroupResource.String()`: `resource`, or `resource.group`.
impl std::fmt::Display for GroupResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.group.is_empty() {
            f.write_str(&self.resource)
        } else {
            write!(f, "{}.{}", self.resource, self.group)
        }
    }
}

/// The provider's RESTMapper: `restmapper.NewDiscoveryRESTMapper` over the
/// served discovery (`pkg/dynamicmapper/mapper.go:65-78`
/// `RegenerateMappings`), here the built-in groups plus the groups of the
/// established CustomResourceDefinitions.
///
/// One mapper per process, kept in [`ApiServerState`], with the
/// invalidate-and-retry-on-miss and the periodic `Reset` of the
/// `DeferredDiscoveryRESTMapper` (see `rusternetes_discovery::
/// deferred_restmapper`); upstream regenerates it on an interval likewise
/// (`pkg/dynamicmapper/mapper.go:56-78`).
pub type ServedRestMapper = DeferredRestMapper<MemCacheClient<StorageDiscovery>>;

/// The discovery the apiserver serves: the built-in groups plus the groups
/// of the CustomResourceDefinitions in storage.
pub struct StorageDiscovery {
    storage: Arc<StorageBackend>,
}

#[async_trait]
impl DiscoveryClient for StorageDiscovery {
    async fn group_resources(&self) -> std::result::Result<Vec<ApiGroupResources>, MapperError> {
        let crds: Vec<serde_json::Value> = self
            .storage
            .as_ref()
            .list(&build_prefix("customresourcedefinitions", None))
            .await
            .map_err(|e| MapperError::Other(e.to_string()))?;
        Ok(served_group_resources(&crds))
    }
}

/// `NewDeferredDiscoveryRESTMapper(NewMemCacheClient(discovery))`.
pub fn new_served_rest_mapper(storage: Arc<StorageBackend>) -> Arc<ServedRestMapper> {
    Arc::new(DeferredRestMapper::new(Arc::new(MemCacheClient::new(
        Arc::new(StorageDiscovery { storage }),
    ))))
}

/// `go wait.Until(restMapper.Reset, 30*time.Second, ctx.Done())`
/// (controllermanager.go:516-519). The loop ends when the mapper is dropped,
/// the analogue of `ctx.Done()`.
pub fn spawn_periodic_reset(mapper: &Arc<ServedRestMapper>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(mapper);
    handle.spawn(async move {
        let mut tick = tokio::time::interval(RESET_PERIOD);
        tick.tick().await;
        loop {
            tick.tick().await;
            match weak.upgrade() {
                Some(m) => m.reset().await,
                None => return,
            }
        }
    });
}

/// `CustomMetricInfo.Normalized` (pkg/provider/interfaces.go:55-68): the
/// group-resource resolved to its plural, which must also have a singular.
/// The mapper's error is returned as-is (an APIStatus-less error: 500
/// `Unknown`, `ErrorToAPIStatus`).
async fn normalized(mapper: &ServedRestMapper, info: &GroupResource) -> Result<GroupResource> {
    let resolved = mapper
        .resource_for(&info.with_version_empty())
        .await
        .map_err(|e| unknown_error(e.to_string()))?;
    let normalized = GroupResource {
        group: resolved.group,
        resource: resolved.resource,
    };
    mapper
        .resource_singularizer(&normalized.resource)
        .await
        .map_err(|e| unknown_error(e.to_string()))?;
    Ok(normalized)
}

/// `helpers.ResourceFor` (pkg/provider/helpers/helpers.go:36-49): the first
/// of `ResourcesFor`, for listing the matching objects.
async fn resource_for(mapper: &ServedRestMapper, info: &GroupResource) -> Result<GroupResource> {
    let wrap = |e: String| {
        unknown_error(format!(
            "unable to find preferred version to list matching resource names: {e}"
        ))
    };
    let resources = mapper
        .resources_for(&info.with_version_empty())
        .await
        .map_err(|e| wrap(e.to_string()))?;
    let first = resources.into_iter().next().ok_or_else(|| {
        wrap(format!(
            "no fully versioned resources known for group-resource {info}"
        ))
    })?;
    Ok(GroupResource {
        group: first.group,
        resource: first.resource,
    })
}

/// `helpers.ReferenceFor` (helpers.go:55-69): the `DescribedObject` from
/// `mapper.KindFor`. `APIVersion` is `kind.Group + "/" + kind.Version`, so the
/// core group yields `/v1` (upstream behaviour, kept).
async fn reference_for(
    mapper: &ServedRestMapper,
    namespace: Option<&str>,
    name: &str,
    info: &GroupResource,
) -> Result<ObjectReference> {
    let kind = mapper
        .kind_for(&info.with_version_empty())
        .await
        .map_err(|e| unknown_error(e.to_string()))?;
    Ok(ObjectReference {
        kind: kind.kind,
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        api_version: Some(format!("{}/{}", kind.group, kind.version)),
    })
}

/// The storage collection holding a resolved group-resource: built-in
/// resources are keyed by their plural, custom resources by
/// `<group>_<plural>` (`handlers::custom_resource`).
fn storage_collection(gr: &GroupResource) -> String {
    if is_builtin_group(&gr.group) {
        gr.resource.clone()
    } else {
        format!("{}_{}", gr.group.replace('.', "_"), gr.resource)
    }
}

/// The provider's object lookup: names of the objects of the group-resource
/// in `namespace` (none for a root-scoped resource) matching `selector`
/// (`helpers.ListObjectNames`, pkg/provider/helpers/helpers.go:74-107;
/// `GetMetricBySelector` runs it first).
async fn selected_object_names(
    state: &ApiServerState,
    mapper: &ServedRestMapper,
    namespace: Option<&str>,
    info: &GroupResource,
    selector: &LabelSelector,
) -> Result<Vec<String>> {
    let res = resource_for(mapper, info).await?;
    let prefix = build_prefix(&storage_collection(&res), namespace);
    let objects: Vec<serde_json::Value> = state.storage.as_ref().list(&prefix).await?;
    let mut names: Vec<String> = objects
        .iter()
        .filter(|o| selector.matches_resource(o))
        .filter_map(|o| o["metadata"]["name"].as_str().map(str::to_string))
        .collect();
    names.sort();
    Ok(names)
}

/// `test-adapter/provider/provider.go:238-262` `metricFor`: the
/// `DescribedObject` reference, `Metric{Name, Selector}`, no `WindowSeconds`.
fn metric_value(
    described_object: ObjectReference,
    metric_name: &str,
    value: String,
    selector: Option<MetricSelector>,
) -> MetricValue {
    MetricValue {
        type_meta: metric_value_type_meta("MetricValue"),
        described_object,
        metric: MetricIdentifier {
            name: metric_name.to_string(),
            selector,
        },
        timestamp: Utc::now(),
        window_seconds: None,
        value,
    }
}

/// `GetMetricByName` (handleIndividualOp, reststorage.go:136-149).
///
/// Prometheus when configured. Otherwise the built-in series model of the
/// custom-metrics-apiserver test adapter (`test-adapter/provider/provider.go:
/// 215-235`): the group-resource is normalized first (`valueFor`), the object
/// must exist (`NewMetricNotFoundForError`) and the series, which carries no
/// labels, must satisfy the metric label selector
/// (`NewMetricNotFoundForSelectorError`); `metricFor` then builds the
/// `DescribedObject` with `ReferenceFor` from the request's group-resource.
/// `namespace` is `None` for a root-scoped resource (`Namespaced: namespace
/// != ""`, reststorage.go:140).
async fn get_metric_by_name(
    state: &ApiServerState,
    mapper: &ServedRestMapper,
    namespace: Option<&str>,
    info: &GroupResource,
    name: &str,
    metric_name: &str,
    metric_selector: &LabelSelector,
) -> Result<MetricValue> {
    let normalized = normalized(mapper, info).await?;
    let value = if let Some(ref prometheus_client) = state.prometheus_client {
        let matchers = matchers_for(metric_selector)?;
        let resource = normalized.resource.as_str();
        let queried = match namespace {
            Some(ns) => {
                prometheus_client
                    .query_object_metric(metric_name, ns, resource, name, &matchers)
                    .await
            }
            // The metric describing a namespace itself.
            None if resource == "namespaces" && normalized.group.is_empty() => {
                prometheus_client
                    .query_namespace_metric(metric_name, name, &matchers)
                    .await
            }
            None => {
                prometheus_client
                    .query_cluster_metric(metric_name, resource, name, &matchers)
                    .await
            }
        };
        match queried {
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
        let key = build_key(&storage_collection(&normalized), namespace, name);
        if state
            .storage
            .as_ref()
            .get::<serde_json::Value>(&key)
            .await
            .is_err()
        {
            return Err(metric_not_found(format!(
                "the server could not find the metric {metric_name} for {normalized} {name}"
            )));
        }
        if !metric_selector.matches(&HashMap::new()) {
            return Err(metric_not_found(format!(
                "the server could not find the metric {metric_name} for {normalized} {name} with selector {metric_selector}"
            )));
        }
        "100".to_string()
    };
    let reference = reference_for(mapper, namespace, name, info).await?;
    let echo = metric_selector_echo(metric_selector)?;
    Ok(metric_value(reference, metric_name, value, echo))
}

/// The one `REST.List` every custom.metrics route runs
/// (`pkg/apiserver/installer/cmhandlers.go:160-247` installs the root-scoped,
/// namespaced and `namespaces/{namespace}/metrics/{name}` routes on
/// `ListResourceWithOptions`, get.go:41-140). `route_name` is the object
/// named by the path; get.go:98-125 treats it as a `metadata.name` field
/// selector entry, and `name == "*"` is `handleWildcardOp` (reststorage.go:
/// 96-123).
async fn list_op(
    state: &ApiServerState,
    namespace: Option<&str>,
    resource: &str,
    route_name: Option<&str>,
    metric_name: &str,
    params: &HashMap<String, String>,
) -> Result<MetricValueList> {
    crate::handlers::validate_synthesized_list_options(params)?;
    // get.go:84-126 decode, then reststorage.go List.
    let opts = decode_list_options(params, route_name)?;
    let metric_selector = decode_metric_label_selector(params)?;
    // reststorage.go:110-113.
    let info = GroupResource::parse(resource);
    let mapper = state.custom_metrics_mapper.as_ref();

    let items = match opts.name.as_deref().filter(|n| *n != "*") {
        // `name != "*"`: handleIndividualOp; the object labelSelector is not
        // consulted, and the single value is wrapped in a list
        // (reststorage.go:146-148).
        Some(name) => vec![
            get_metric_by_name(
                state,
                mapper,
                namespace,
                &info,
                name,
                metric_name,
                &metric_selector,
            )
            .await?,
        ],
        // handleWildcardOp -> GetMetricBySelector(namespace, selector, ...).
        None => {
            let objects =
                selected_object_names(state, mapper, namespace, &info, &opts.object_selector)
                    .await?;
            // `metricsFor` runs `valueFor` (-> `Normalized`) per object and
            // returns anything but a NotFound; nothing to look up, nothing to
            // fail.
            let normalized = if objects.is_empty() {
                None
            } else {
                Some(normalized(mapper, &info).await?)
            };
            let resource = normalized
                .as_ref()
                .map_or(resource, |n| n.resource.as_str());
            let mut series: Vec<(String, String)> = Vec::new();
            if let Some(ref prometheus_client) = state.prometheus_client {
                let matchers = matchers_for(&metric_selector)?;
                // A root-scoped list has no namespace label to pin.
                match prometheus_client
                    .query_list_metric(metric_name, namespace.unwrap_or(""), resource, &matchers)
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
                // Test-adapter series model: every selected object has a
                // series without labels (provider.go metricsFor/valueFor).
                series.extend(objects.into_iter().map(|o| (o, "100".to_string())));
            }
            if series.is_empty() {
                Vec::new()
            } else {
                let echo = metric_selector_echo(&metric_selector)?;
                let mut out = Vec::with_capacity(series.len());
                for (name, value) in series {
                    // `metricFor` (provider.go:238-262).
                    let reference = reference_for(mapper, namespace, &name, &info).await?;
                    out.push(metric_value(reference, metric_name, value, echo.clone()));
                }
                out
            }
        }
    };

    Ok(MetricValueList {
        type_meta: metric_value_type_meta("MetricValueList"),
        // `metav1.ListMeta`. The bespoke predecessor carried only `selfLink`,
        // which upstream stopped populating on every object in 1.20 (#1916).
        metadata: rusternetes_common::types::ListMeta::default(),
        items,
    })
}

/// `GET .../namespaces/{namespace}/{resource}/{name}/{metric}`: a named
/// object (or `*`), answered as a `MetricValueList`.
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
) -> Result<Json<MetricValueList>> {
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

    Ok(crate::audit::json_response(
        list_op(
            &state,
            Some(&namespace),
            &resource_type,
            Some(&resource_name),
            &metric_name,
            &params,
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
    // `RequestInfo` of `.../namespaces/{ns}/{resource}/{metric}` (the
    // `/*/` wildcard-less form): resource, and the metric as the name. What
    // the `WithAuthorization` filter authorizes (requestinfo.go:211-221).
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource_type)
        .with_api_group("custom.metrics.k8s.io")
        .with_namespace(&namespace)
        .with_name(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    Ok(crate::audit::json_response(
        list_op(
            &state,
            Some(&namespace),
            &resource_type,
            None,
            &metric_name,
            &params,
        )
        .await?,
    ))
}

/// `GET .../namespaces/{namespace}/metrics/{metric}`: get.go:49-58 rewrites
/// it to resource `namespaces`, name = the namespace, no namespace scope, for
/// the provider; the request is authorized as the filter sees the path.
pub async fn get_namespace_metric(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, metric_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MetricValueList>> {
    debug!("Getting namespace metric {} for {}", metric_name, namespace);

    // Check authorization
    // The provider rewrites this to resource `namespaces` (get.go:49-58), but
    // authorization is the filter's, from `RequestInfo` of the path as sent:
    // resource `metrics`, name = the metric, in the namespace.
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "metrics")
        .with_api_group("custom.metrics.k8s.io")
        .with_namespace(&namespace)
        .with_name(&metric_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    Ok(crate::audit::json_response(
        list_op(
            &state,
            None,
            "namespaces",
            Some(&namespace),
            &metric_name,
            &params,
        )
        .await?,
    ))
}

/// `GET .../{resource}/{name}/{metric}`: a root-scoped object.
pub async fn get_cluster_metric(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((resource_type, resource_name, metric_name)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MetricValueList>> {
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

    Ok(crate::audit::json_response(
        list_op(
            &state,
            None,
            &resource_type,
            Some(&resource_name),
            &metric_name,
            &params,
        )
        .await?,
    ))
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
}
