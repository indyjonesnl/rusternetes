//! Event endpoints (core `v1` and `events.k8s.io/v1`, one storage).
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the Event strategy
//! ([`crate::registry::core::event`]) -- upstream's
//! `pkg/registry/core/event/storage` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go`. Lists and watches are
//! still served here directly.
//!
//! Both APIs reach one registry upstream. The stored object is the core Event;
//! an `events.k8s.io/v1` request is converted onto it on the way in and back on
//! the way out (`pkg/apis/events/v1/conversion.go:29-60`), and the scope carries
//! the served version so the strategy can pick the legacy or the strict
//! validation (`requestGroupVersion`, strategy.go:134-139).

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::core::event;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::{header, HeaderMap, Uri},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::dump::decode_request_body;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{Event, EventList, EventV1, EventV1List},
    Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// The API a request addressed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Served {
    Core,
    EventsV1,
}

fn served(uri: &Uri) -> Served {
    if uri.path().starts_with("/apis/events.k8s.io/") {
        Served::EventsV1
    } else {
        Served::Core
    }
}

/// The Event `RequestScope`: `v1` `Event` served as `events`, or
/// `events.k8s.io/v1` `Event` served as `events`, over the one `NewREST` store.
fn scope(state: &ApiServerState, uri: &Uri) -> (Served, RequestScope<Event>) {
    let served = served(uri);
    let group = match served {
        Served::Core => "",
        Served::EventsV1 => "events.k8s.io",
    };
    let scope = RequestScope {
        kind: GroupVersionKind {
            group: group.to_string(),
            version: "v1".to_string(),
            kind: "Event".to_string(),
        },
        resource: GroupVersionResource {
            group: group.to_string(),
            version: "v1".to_string(),
            resource: "events".to_string(),
        },
        subresource: None,
        store: Box::new(event::new_store(state.storage.clone())),
        apply: Some(match served {
            Served::Core => crate::ssa::apply_legacy::<Event>,
            Served::EventsV1 => event::apply_events_v1,
        }),
        convert_to_internal: Some(event::convert_to_internal),
        patch_conversion: match served {
            Served::Core => None,
            Served::EventsV1 => Some(event::EVENTS_V1_PATCH_CONVERSION),
        },
    };
    (served, scope)
}

/// A request body in the shape the scope decodes. An `events.k8s.io/v1` body is
/// converted onto the core object *before* validation, as the codec does
/// (`Convert_v1_Event_To_core_Event`): the strict validator reads
/// `involvedObject.namespace`, the message length and requires `source`,
/// `firstTimestamp`, `lastTimestamp` and `count` -- which arrive as
/// `deprecated*` on this version -- to be unset (#1914).
fn request_body(served: Served, params: &HashMap<String, String>, body: &Bytes) -> Result<Bytes> {
    if served == Served::Core {
        return Ok(body.clone());
    }
    let value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(e) => {
            return Err(decode_request_body::<EventV1>(body)
                .err()
                .unwrap_or_else(|| rusternetes_common::Error::BadRequest(e.to_string())))
        }
    };
    if let Some(api_version) = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
    {
        if api_version != "events.k8s.io/v1" {
            return Err(rusternetes_common::Error::BadRequest(format!(
                "the API version in the data ({api_version}) does not match the expected API version (events.k8s.io/v1)"
            )));
        }
    }
    let v1: EventV1 = decode_request_body(body)?;
    // Strictness judges the body in the versioned type, before conversion drops
    // any of its fields: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/
    // create.go:116-148 decodes into the versioned object first. Warn-mode
    // warnings are not carried through (#2111).
    crate::handlers::validation::validate_strict_fields(params, body, &v1)?;
    let mut core = serde_json::to_value(v1.into_core())
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    // The scope's decode checks the body's apiVersion against the served one;
    // `convert_to_internal` stamps the stored (core) version back on.
    core["apiVersion"] = serde_json::json!("events.k8s.io/v1");
    Ok(Bytes::from(serde_json::to_vec(&core).map_err(|e| {
        rusternetes_common::Error::Internal(e.to_string())
    })?))
}

/// A response in the served version's schema: the stored core Event, or a list
/// of them, converted the way upstream's codec does on the way out
/// (`Convert_core_Event_To_v1_Event`) (#1926).
async fn response(served: Served, response: Response) -> Result<Response> {
    if served == Served::Core || !response.status().is_success() {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let converted = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => match value.get("kind").and_then(|k| k.as_str()) {
            Some("Event") => Some(event::core_event_json_to_events_v1(&value)),
            Some("EventList") => {
                if let Some(items) = value.get_mut("items").and_then(|i| i.as_array_mut()) {
                    for item in items.iter_mut() {
                        *item = event::core_event_json_to_events_v1(item);
                    }
                }
                Some(value)
            }
            _ => None,
        },
        Err(_) => None,
    };
    let out = match converted.and_then(|v| serde_json::to_vec(&v).ok()) {
        Some(out) => out,
        None => bytes.to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    Ok(Response::from_parts(parts, Body::from(out)))
}

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
fn patch_content_type(headers: &HeaderMap) -> &str {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let body = request_body(served, &params, &body)?;
    let out = endpoints::create_resource(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await?;
    response(served, out).await
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let out = endpoints::get_resource(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
    )
    .await?;
    response(served, out).await
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let body = request_body(served, &params, &body)?;
    let out = endpoints::update_resource(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await?;
    response(served, out).await
}

/// PATCH. The patch is applied to the object in the version it was written
/// against and the result converted back before validation and storage, as
/// upstream does for every patch
/// (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go:323-338`,
/// `:449-462`); the Store then runs `ValidateUpdate`, so on
/// `events.k8s.io/v1` a patch of `note` is a 422 like the PUT (#1940).
pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let out = endpoints::patch_resource(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    response(served, out).await
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let out = endpoints::delete_resource(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &name,
        &params,
        &body,
    )
    .await?;
    response(served, out).await
}

pub async fn deletecollection_events(
    State(state): State<Arc<ApiServerState>>,
    OriginalUri(uri): OriginalUri,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let (served, scope) = scope(&state, &uri);
    let out = endpoints::delete_collection(
        &state,
        &scope,
        &auth_ctx.user,
        Some(&namespace),
        &params,
        &body,
    )
    .await?;
    response(served, out).await
}

/// List all events in a namespace
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
        return crate::handlers::watch::watch_namespaced::<Event>(
            state,
            auth_ctx,
            namespace,
            "events",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing events in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", Some(&namespace));
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    Ok(Json(EventList {
        api_version: "v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

/// List all events across all namespaces
pub async fn list_all(
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
        return crate::handlers::watch::watch_cluster_scoped::<Event>(
            state,
            auth_ctx,
            "events",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all events across all namespaces");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", None);
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    Ok(Json(EventList {
        api_version: "v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

// ---- events.k8s.io/v1 API group handlers ----
// These return apiVersion "events.k8s.io/v1" instead of "v1"
// and use api_group "events.k8s.io" for authorization.

/// List events via events.k8s.io/v1 (namespaced)
pub async fn list_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        // The stored object is the core Event; the client asked for
        // `events.k8s.io/v1`. Convert every streamed object the way upstream's
        // codec does on the way out (`Convert_core_Event_To_v1_Event`,
        // `pkg/apis/events/v1/conversion.go:46-60`) and stream the v1 wire
        // type, so a watch answers with the same schema as a GET (#1926).
        let converter: crate::handlers::watch::WatchObjectConverter =
            std::sync::Arc::new(|value: serde_json::Value| {
                Box::pin(async move { core_event_json_to_events_v1(value) })
            });
        return crate::handlers::watch::watch_namespaced_converted::<EventV1>(
            state,
            auth_ctx,
            namespace,
            "events",
            "events.k8s.io",
            watch_params,
            converter,
            Some(("Event".to_string(), "events.k8s.io/v1".to_string())),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", Some(&namespace));
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    // Answer in the events.k8s.io/v1 schema, not the stored core one with a
    // rewritten `apiVersion` (#1926).
    Ok(Json(EventV1List {
        api_version: "events.k8s.io/v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events.iter().map(Event::to_events_v1).collect(),
    })
    .into_response())
}

/// List all events via events.k8s.io/v1 (cluster-scoped)
pub async fn list_all_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_cluster_scoped::<Event>(
            state,
            auth_ctx,
            "events",
            "events.k8s.io",
            watch_params,
        )
        .await;
    }

    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "events").with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", None);
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    Ok(Json(EventV1List {
        api_version: "events.k8s.io/v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events.iter().map(Event::to_events_v1).collect(),
    })
    .into_response())
}

/// One stored core-Event JSON object in the `events.k8s.io/v1` shape, for the
/// watch path, which hands its converter raw JSON rather than a typed object.
fn core_event_json_to_events_v1(value: serde_json::Value) -> serde_json::Value {
    crate::registry::core::event::core_event_json_to_events_v1(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::EventType;
    use rusternetes_common::resources::ObjectReference;

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "PodStarted".to_string(),
            "Pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.metadata.name, "test-event".to_string());
        assert_eq!(event.metadata.namespace, Some("default".to_string()));
        assert_eq!(event.reason, "PodStarted");
        assert_eq!(event.count, 1);
    }

    #[test]
    fn test_event_field_selector_filtering() {
        // Simulate what list_all does: filter events using apply_selectors
        let mut events = vec![
            Event::new(
                "event-1".to_string(),
                "default".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("default".to_string()),
                    name: Some("pod-a".to_string()),
                    uid: Some("uid1".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Started".to_string(),
                "Pod started".to_string(),
                EventType::Normal,
            ),
            Event::new(
                "event-2".to_string(),
                "kube-system".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("kube-system".to_string()),
                    name: Some("pod-b".to_string()),
                    uid: Some("uid2".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Failed".to_string(),
                "Pod failed".to_string(),
                EventType::Warning,
            ),
        ];

        // Filter by involvedObject.name
        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "involvedObject.name=pod-a".to_string(),
        );
        crate::handlers::filtering::apply_selectors(&mut events, &params).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metadata.name, "event-1");
        assert_eq!(events[0].involved_object.name, Some("pod-a".to_string()));
    }
}
