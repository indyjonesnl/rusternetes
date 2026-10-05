//! Pod endpoints.
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the Pod strategies
//! ([`crate::registry::core::pod`]) — upstream's
//! `pkg/registry/core/pod/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. The admission the
//! pod handlers used to run inline is the in-tree chain of
//! [`crate::endpoints::handlers::Admission`]. Lists and watches are still
//! served here directly; `/eviction` keeps its own handler.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::pod;
use crate::registry::rest::{RequestContext, RestStorage};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    dump::decode_request_body,
    resources::{Binding, Node, Pod},
    validation::metav1::{validate_create_options, CreateOptions},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// The Pod `RequestScope`: `v1` `Pod` served as `pods` (or one of its
/// subresources), backed by `pod.NewStorage`'s stores.
fn scope(state: &ApiServerState, subresource: Option<&'static str>) -> RequestScope<Pod> {
    let store: Box<dyn RestStorage<Pod>> = match subresource {
        Some("ephemeralcontainers") => {
            Box::new(pod::new_ephemeral_containers_store(state.storage.clone()))
        }
        Some("resize") => Box::new(pod::new_resize_store(state.storage.clone())),
        Some("status") => Box::new(pod::new_status_store(state.storage.clone())),
        // `BindingREST` is not a `RestStorage<Pod>`; `create_binding` builds
        // its own. The scope only names the resource for authorization.
        _ => Box::new(pod::new_store(state.storage.clone())),
    };
    RequestScope {
        kind: GroupVersionKind {
            group: String::new(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: String::new(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        subresource,
        store,
        apply: Some(crate::ssa::apply_legacy::<Pod>),
        convert_to_internal: Some(pod::convert_to_internal),
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

/// Attach the opt-in so the response middleware emits a native protobuf
/// envelope when the client negotiates `application/vnd.kubernetes.protobuf`.
/// The native encoder (`crate::response::NativePodProtoEncoder`) writes real
/// protobuf derived from `crate::protobuf::PROTO_REGISTRY`, which is what
/// client-go's typed decoder needs (it never consults `Unknown.contentType`).
fn with_proto_opt_in(mut resp: Response) -> Response {
    resp.extensions_mut()
        .insert(crate::response::NativeProtoOptIn::pod());
    resp
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Creating pod in namespace {}", namespace);
    let resp = endpoints::create_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await?;
    Ok(with_proto_opt_in(resp))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    debug!("Getting pod: {}/{}", namespace, name);
    let resp = endpoints::get_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await?;
    Ok(with_proto_opt_in(resp))
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
        &scope(&state, None),
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
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

pub async fn delete_pod(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_resource(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

pub async fn deletecollection_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::delete_collection(
        &state,
        &scope(&state, None),
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await
}

/// GET `/ephemeralcontainers`: `EphemeralContainersREST.Get` is the store's.
pub async fn get_ephemeralcontainers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, Some("ephemeralcontainers")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

/// PUT `/ephemeralcontainers`: `EphemeralContainersREST.Update`.
pub async fn update_ephemeralcontainers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("ephemeralcontainers")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/ephemeralcontainers`: a patch into `EphemeralContainersREST.Update`.
pub async fn patch_ephemeralcontainers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("ephemeralcontainers")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

/// GET `/resize`: `ResizeREST.Get` is the store's.
pub async fn get_resize(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, Some("resize")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

/// PUT `/resize`: `ResizeREST.Update` (storage.go).
pub async fn update_resize(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("resize")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/resize`: a patch into `ResizeREST.Update`.
pub async fn patch_resize(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("resize")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

/// GET `/status`: `StatusREST.Get` is the store's (storage.go).
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
    endpoints::get_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
    )
    .await
}

/// PUT `/status`: `StatusREST.Update` (storage.go).
pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    endpoints::update_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await
}

/// PATCH `/status`: a patch into `StatusREST.Update`.
pub async fn patch_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    endpoints::patch_resource(
        &state,
        &scope(&state, Some("status")),
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await
}

/// POST `/binding`: `BindingREST.Create` as a named create
/// (`handlers.CreateNamedResource`, create.go, over a `Binding`).
pub async fn create_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Creating binding for pod {}/{}", namespace, name);
    let scope = scope(&state, Some("binding"));
    endpoints::authorize(
        &state,
        &auth_ctx.user,
        "create",
        &scope.resource,
        scope.subresource,
        Some(&namespace),
        Some(&name),
    )
    .await?;

    let options = CreateOptions {
        field_manager: params.get("fieldManager").cloned(),
        dry_run: endpoints::dry_run_param(&params),
        field_validation: params.get("fieldValidation").cloned(),
    };
    let errs = validate_create_options(&options);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }
    let dry_run = endpoints::is_dry_run(options.dry_run.as_deref());

    let mut binding: Binding = decode_request_body(&body)?;
    let ctx = RequestContext::new(Some(&namespace))
        .with_user(&auth_ctx.user)
        .with_name(&name);
    admit_binding_topology_labels(&state, &mut binding).await?;

    pod::BindingRest::new(state.storage.clone())
        .create(&ctx, &name, &binding, dry_run)
        .await?;
    Ok(endpoints::respond(
        axum::http::StatusCode::CREATED,
        &rusternetes_common::Status::success(),
        &ctx,
    ))
}

/// POST `/eviction`: `EvictionREST.Create` as a named create
/// (`handlers.CreateNamedResource`, create.go, over a `policy/v1` `Eviction`).
pub async fn create_eviction(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    use crate::endpoints::handlers::admission::{Admission, CreateValidation};
    use rusternetes_common::admission::Operation;
    use rusternetes_common::resources::Eviction;
    use rusternetes_common::Error;

    info!("Creating eviction for pod {}/{}", namespace, name);
    let scope = scope(&state, Some("eviction"));
    endpoints::authorize(
        &state,
        &auth_ctx.user,
        "create",
        &scope.resource,
        scope.subresource,
        Some(&namespace),
        Some(&name),
    )
    .await?;

    let options = CreateOptions {
        field_manager: params.get("fieldManager").cloned(),
        dry_run: endpoints::dry_run_param(&params),
        field_validation: params.get("fieldValidation").cloned(),
    };
    let errs = validate_create_options(&options);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }
    let dry_run = endpoints::is_dry_run(options.dry_run.as_deref());

    // `EvictionREST.AcceptsGroupVersion` (eviction.go:87-95): both policy/v1
    // and policy/v1beta1 bodies are acceptable.
    let value: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|_| decode_request_body::<Eviction>(&body).err().unwrap_or_else(|| {
            Error::BadRequest("the request body is not valid JSON".to_string())
        }))?;
    if let Some(api_version) = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
    {
        if api_version != "policy/v1" && api_version != "policy/v1beta1" {
            return Err(Error::BadRequest(format!(
                "the API version in the data ({api_version}) does not match the expected API version (policy/v1)"
            )));
        }
    }
    let mut eviction: Eviction = decode_request_body(&body)?;
    if eviction.type_meta.kind.is_empty() {
        eviction.type_meta.kind = "Eviction".to_string();
    }
    if eviction.type_meta.api_version.is_empty() {
        eviction.type_meta.api_version = "policy/v1".to_string();
    }

    let ctx = RequestContext::new(Some(&namespace))
        .with_user(&auth_ctx.user)
        .with_name(&name)
        .with_group_version("policy", "v1");
    crate::registry::rest::ensure_object_namespace_matches_request_namespace(
        Some(&namespace),
        &mut eviction.metadata,
    )?;

    let kind = GroupVersionKind {
        group: "policy".to_string(),
        version: "v1".to_string(),
        kind: "Eviction".to_string(),
    };
    let admission = Admission {
        state: &state,
        kind: &kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace: Some(&namespace),
        user: &auth_ctx.user,
        dry_run,
    };
    // Mutating admission, then the storage's create with validating
    // admission as its callback (create.go:202-209).
    let eviction = admission.admit(Operation::Create, eviction, None).await?;
    let validation = CreateValidation {
        admission: &admission,
        authorize_create: false,
    };
    let status = pod::new_eviction_rest(state.storage.clone())
        .create(&ctx, &name, eviction, Some(&validation), &options)
        .await?;

    // create.go:227-231: a result `Status` without a code is a 201; one with
    // a code (the 500 for a pod under several budgets) is written as it is.
    let code = status.code.unwrap_or(201);
    let mut status = status;
    status.code = Some(code);
    Ok(endpoints::respond(
        axum::http::StatusCode::from_u16(code)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
        &status,
        &ctx,
    ))
}

/// `podtopologylabels.Plugin.admitBinding`
/// (plugin/pkg/admission/podtopologylabels/admission.go): the node's
/// topology labels go onto the Binding, and `BindingREST` copies them to the
/// pod. Gated by `PodTopologyLabelsAdmission`; a node that is not there is
/// ignored, "to avoid risking breaking compatibility/behaviour".
async fn admit_binding_topology_labels(
    state: &ApiServerState,
    binding: &mut Binding,
) -> Result<()> {
    use rusternetes_common::feature_gates::{enabled, Feature};
    // The plugin's default `Config.Labels`.
    const TOPOLOGY_LABELS: [&str; 2] = [
        "topology.kubernetes.io/zone",
        "topology.kubernetes.io/region",
    ];
    if !enabled(Feature::PodTopologyLabelsAdmission) {
        return Ok(());
    }
    // other fields are not set by the default scheduler for the binding
    // target, so only check the Kind.
    if binding.target.kind.as_deref() != Some("Node") {
        return Ok(());
    }
    let node_key = build_key("nodes", None::<&str>, &binding.target.name);
    let node: Node = match state.storage.get(&node_key).await {
        Ok(node) => node,
        Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    let Some(node_labels) = node.metadata.labels.as_ref() else {
        return Ok(());
    };
    for key in TOPOLOGY_LABELS {
        if let Some(value) = node_labels.get(key) {
            binding
                .metadata
                .labels
                .get_or_insert_with(Default::default)
                .insert(key.to_string(), value.clone());
        }
    }
    Ok(())
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Starting watch for pods in namespace: {}", namespace);
        // Parse WatchParams from the query parameters
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
        return crate::handlers::watch::watch_namespaced::<Pod>(
            state,
            auth_ctx,
            namespace,
            "pods",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing pods in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("pods", Some(&namespace));
    let mut pods: Vec<Pod> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pods, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    pods.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Parse pagination parameters
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    // Use the current etcd revision as the list resourceVersion.
    // K8s returns the etcd revision at the time of the LIST, not the max item RV.
    // This ensures LIST+WATCH consistency AND that successive lists have different RVs.
    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &pods).await;

    // Apply pagination
    let paginated = match rusternetes_common::paginate(pods, pagination_params, &resource_version) {
        Ok(p) => p,
        Err(e) => {
            if e.message.contains("410 Gone") {
                let mut status = rusternetes_common::Status::failure(&e.message, "Expired", 410);
                if let Some(token) = e.fresh_continue_token {
                    status.metadata = Some(rusternetes_common::ListMeta {
                        resource_version: Some(resource_version),
                        continue_token: Some(token),
                        remaining_item_count: None,
                    });
                }
                return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
            }
            return Err(rusternetes_common::Error::InvalidResource(e.message));
        }
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table =
            crate::handlers::table::pods_table(paginated.items, Some(resource_version.clone()));
        return Ok(axum::Json(table).into_response());
    }

    // Wrap in proper List object with pagination metadata
    let mut list = List::new("PodList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);

    // Attach the PodList opt-in so the response middleware emits a native
    // protobuf envelope when the client negotiates
    // `application/vnd.kubernetes.protobuf`. See `build_pod_response` for
    // the encoder rationale.
    let mut resp = axum::Json(list).into_response();
    resp.extensions_mut()
        .insert(crate::response::NativeProtoOptIn::pod_list());
    Ok(resp)
}

/// List all pods across all namespaces
pub async fn list_all_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Watch request for all pods");
        // Parse WatchParams from the query parameters
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
        return crate::handlers::watch::watch_cluster_scoped::<Pod>(
            state,
            auth_ctx,
            "pods",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all pods");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("pods", None);
    let mut pods = state.storage.list::<Pod>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pods, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    pods.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Parse pagination parameters
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &pods).await;

    // Apply pagination
    let paginated = match rusternetes_common::paginate(pods, pagination_params, &resource_version) {
        Ok(p) => p,
        Err(e) => {
            if e.message.contains("410 Gone") {
                let mut status = rusternetes_common::Status::failure(&e.message, "Expired", 410);
                if let Some(token) = e.fresh_continue_token {
                    status.metadata = Some(rusternetes_common::ListMeta {
                        resource_version: Some(resource_version),
                        continue_token: Some(token),
                        remaining_item_count: None,
                    });
                }
                return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
            }
            return Err(rusternetes_common::Error::InvalidResource(e.message));
        }
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table =
            crate::handlers::table::pods_table(paginated.items, Some(resource_version.clone()));
        return Ok(axum::Json(table).into_response());
    }

    // Wrap in proper List object with pagination metadata
    let mut list = List::new("PodList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);

    Ok(axum::Json(list).into_response())
}
