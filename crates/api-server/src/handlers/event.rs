use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{Event, EventList, EventV1, EventV1List},
    Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

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

/// Get a specific event
pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Event>> {
    debug!("Getting event: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let event: Event = state.storage.get(&key).await?;

    Ok(Json(event))
}

/// Create a new event
pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut event): DumpingJson<Event>,
) -> Result<(StatusCode, Json<Event>)> {
    info!("Creating event in namespace: {}", namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "events")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace matches
    event.metadata.namespace = Some(namespace.clone());

    // Generate UID if not present
    event.metadata.ensure_uid();

    // Generate name if not present
    if event.metadata.name.is_empty() {
        let name = Event::generate_name(&event.involved_object, &event.reason);
        event.metadata.name = name;
    }

    // Ensure creation timestamp is set
    event.metadata.ensure_creation_timestamp();

    // Field validation (mirrors upstream ValidateEventCreate, core/v1 path —
    // legacy-only rules so internally-generated events are not over-tightened).
    {
        let errs = rusternetes_common::validation::events::validate_event_create(
            &event,
            rusternetes_common::validation::events::RequestVersion::CoreV1,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let key = build_key("events", Some(&namespace), &event.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not created)",
            namespace, event.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(event)));
    }

    let created = state.storage.create(&key, &event).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update an existing event
pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut event): DumpingJson<Event>,
) -> Result<Json<Event>> {
    info!("Updating event: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace and name match
    event.metadata.namespace = Some(namespace.clone());
    event.metadata.name = name.clone();

    let key = build_key("events", Some(&namespace), &name);

    // Preserve creation_timestamp from the existing resource — the client
    // may send a truncated version (Go's time.Time loses nanosecond precision
    // during JSON round-trip in some cases).
    let existing_event = state.storage.get::<Event>(&key).await.ok();
    if let Some(existing) = &existing_event {
        if existing.metadata.creation_timestamp.is_some() {
            event.metadata.creation_timestamp = existing.metadata.creation_timestamp;
        }
        // Also preserve UID — must not change on update
        if !existing.metadata.uid.is_empty() {
            event.metadata.uid = existing.metadata.uid.clone();
        }
        // Events allow unconditional updates (upstream Event.Strategy
        // AllowUnconditionalUpdate == true): if the client omits
        // resourceVersion, back-fill it from the stored object before
        // validation, mirroring registry/generic/registry/store.go.
        if event
            .metadata
            .resource_version
            .as_deref()
            .unwrap_or("")
            .is_empty()
        {
            event.metadata.resource_version = existing.metadata.resource_version.clone();
        }
    }

    // Field validation (mirrors upstream ValidateEventUpdate, core/v1 path).
    if let Some(existing) = &existing_event {
        let errs = rusternetes_common::validation::events::validate_event_update(
            &event,
            existing,
            rusternetes_common::validation::events::RequestVersion::CoreV1,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(event));
    }

    // Reinstate the server-owned metadata a PUT body may omit: uid,
    // creationTimestamp and a pending deletion. A locally built object —
    // what the dynamic client's Update() sends — carries none of them, and
    // storing the blanks orphans every child, because ownerReferences[].uid
    // then matches no live owner and the garbage collector deletes them
    // (#1605, #1793). Upstream applies this to every resource at once in
    // registry/rest/update.go::BeforeUpdate (lines 131-146).
    // The stored object is already in hand from the read above; re-reading it
    // here would cost a second round-trip on every PUT and widen the window
    // between read and write for no benefit.
    if let Some(stored) = existing_event.as_ref() {
        crate::handlers::lifecycle::inherit_server_owned_metadata(
            &mut event.metadata,
            &stored.metadata,
        );
    }
    // Event is one of the nine strategies whose `AllowCreateOnUpdate()` is
    // true (pkg/registry/core/event/strategy.go:74), so a PUT to a name that
    // does not exist creates it rather than answering NotFound (#1905).
    let updated = match state.storage.update(&key, &event).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &event).await?,
        Err(e) => return Err(e),
    };

    // Upstream ShouldDeleteDuringUpdate: an update that drains the last
    // finalizer off an object already pending deletion removes it as part of
    // that same request (store.go:565).
    crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
        &*state.storage,
        &key,
        &updated,
    )
    .await?;

    Ok(Json(updated))
}

/// Delete an event
pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Event>> {
    info!("Deleting event: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);

    // Get the event for finalizer handling
    let event: Event = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(event));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &event,
        &delete_opts,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(event))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Event = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
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

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, Event, "events", "");

pub async fn deletecollection_events(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection events in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "events")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Event collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all events in the namespace
    let prefix = build_prefix("events", Some(&namespace));
    let mut items = state.storage.list::<Event>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("events", Some(&namespace), &item.metadata.name);

        // Handle deletion with finalizers
        let deleted_immediately = match crate::handlers::finalizers::delete_collection_item(
            &state.storage,
            &key,
            &item,
            &delete_opts,
        )
        .await?
        {
            Some(deleted) => deleted,
            // Already gone — a concurrent deleter won the race; upstream
            // DeleteCollection ignores NotFound rather than failing the request.
            None => continue,
        };

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} events deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
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

/// Get a single event via events.k8s.io/v1
pub async fn get_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<EventV1>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let event: Event = state.storage.get(&key).await?;

    Ok(Json(event.to_events_v1()))
}

/// Create an event via events.k8s.io/v1
pub async fn create_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(body): DumpingJson<EventV1>,
) -> Result<(StatusCode, Json<EventV1>)> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Convert the request onto the core object BEFORE validation: the strict
    // validator reads `involvedObject.namespace`, `message`/`note` length, and
    // requires `source`, `firstTimestamp`, `lastTimestamp` and `count` — which
    // arrive as `deprecated*` on this version — to be unset (#1914).
    // Crucially we do NOT yet copy `reportingController` into `source`: strict
    // validation requires `source` to be unset, and the client legitimately
    // leaves it empty.
    let mut event = body.into_core();
    event.metadata.namespace = Some(namespace.clone());
    event.metadata.ensure_uid();

    if event.metadata.name.is_empty() {
        let name = Event::generate_name(&event.involved_object, &event.reason);
        event.metadata.name = name;
    }

    event.metadata.ensure_creation_timestamp();

    // Strict field validation (mirrors upstream ValidateEventCreate on the
    // events.k8s.io/v1 path). Run before the source-component back-fill so a
    // valid client event with an empty `source` still passes the
    // "source needs to be unset" rule.
    {
        let errs = rusternetes_common::validation::events::validate_event_create(
            &event,
            rusternetes_common::validation::events::RequestVersion::EventsV1,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Back-fill source.component from reportingController for field-selector
    // compatibility now that strict validation has passed.
    if event.source.component.is_empty() {
        if let Some(ref rc) = event.reporting_component {
            event.source.component = rc.clone();
        }
    }

    let key = build_key("events", Some(&namespace), &event.metadata.name);

    if is_dry_run {
        return Ok((StatusCode::CREATED, Json(event.to_events_v1())));
    }

    let created: Event = state.storage.create(&key, &event).await?;

    Ok((StatusCode::CREATED, Json(created.to_events_v1())))
}

/// Update an event via events.k8s.io/v1
pub async fn update_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(body): DumpingJson<EventV1>,
) -> Result<Json<EventV1>> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // The v1 body converts onto the core object unconditionally, as upstream's
    // `Convert_v1_Event_To_core_Event` does: this version has no `count`,
    // `message`, `source` or `involvedObject` field for the conversion to
    // clobber, and reads now answer in the same schema, so a
    // read-modify-write client round-trips the `deprecated*` names (#1926).
    let mut event = body.into_core();
    event.metadata.namespace = Some(namespace.clone());
    event.metadata.name = name.clone();

    let key = build_key("events", Some(&namespace), &name);

    // Preserve creation_timestamp and UID from existing resource — the client
    // may send a truncated version (Go's time.Time loses nanosecond precision
    // during JSON round-trip in some cases).
    let existing_event = state.storage.get::<Event>(&key).await.ok();
    if let Some(existing) = &existing_event {
        if existing.metadata.creation_timestamp.is_some() {
            event.metadata.creation_timestamp = existing.metadata.creation_timestamp;
        }
        // Also preserve UID — must not change on update
        if !existing.metadata.uid.is_empty() {
            event.metadata.uid = existing.metadata.uid.clone();
        }
        // Events allow unconditional updates (upstream Event.Strategy
        // AllowUnconditionalUpdate == true): if the client omits
        // resourceVersion, back-fill it from the stored object before
        // validation, mirroring registry/generic/registry/store.go.
        if event
            .metadata
            .resource_version
            .as_deref()
            .unwrap_or("")
            .is_empty()
        {
            event.metadata.resource_version = existing.metadata.resource_version.clone();
        }
    }

    // Back-fill `source.component` from `reportingController` to mirror how the
    // stored event was normalised at create time, so the strict update
    // immutability check on `source` compares like with like. An explicit
    // `deprecatedSource` already landed on `source` in the conversion above and
    // still wins.
    if event.source.component.is_empty() {
        if let Some(ref rc) = event.reporting_component {
            event.source.component = rc.clone();
        }
    }

    // Strict field validation (mirrors upstream ValidateEventUpdate on the
    // events.k8s.io/v1 path: immutability of most fields + microsecond-precision
    // eventTime tolerance).
    if let Some(existing) = &existing_event {
        let errs = rusternetes_common::validation::events::validate_event_update(
            &event,
            existing,
            rusternetes_common::validation::events::RequestVersion::EventsV1,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    if is_dry_run {
        return Ok(Json(event.to_events_v1()));
    }

    // Reinstate the server-owned metadata a PUT body may omit: uid,
    // creationTimestamp and a pending deletion. A locally built object —
    // what the dynamic client's Update() sends — carries none of them, and
    // storing the blanks orphans every child, because ownerReferences[].uid
    // then matches no live owner and the garbage collector deletes them
    // (#1605, #1793). Upstream applies this to every resource at once in
    // registry/rest/update.go::BeforeUpdate (lines 131-146).
    // The stored object is already in hand from the read above; re-reading it
    // here would cost a second round-trip on every PUT and widen the window
    // between read and write for no benefit.
    if let Some(stored) = existing_event.as_ref() {
        crate::handlers::lifecycle::inherit_server_owned_metadata(
            &mut event.metadata,
            &stored.metadata,
        );
    }
    // AllowCreateOnUpdate, as on the core endpoint above — both reach the same
    // registry upstream (pkg/registry/core/event/strategy.go:74).
    let updated: Event = match state.storage.update(&key, &event).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &event).await?,
        Err(e) => return Err(e),
    };

    // Upstream ShouldDeleteDuringUpdate: an update that drains the last
    // finalizer off an object already pending deletion removes it as part of
    // that same request (store.go:565).
    crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
        &*state.storage,
        &key,
        &updated,
    )
    .await?;

    Ok(Json(updated.to_events_v1()))
}

/// Delete an event via events.k8s.io/v1
pub async fn delete_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<EventV1>> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let event: Event = state.storage.get(&key).await?;

    if is_dry_run {
        return Ok(Json(event.to_events_v1()));
    }

    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &event,
        &delete_opts,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(event.to_events_v1()))
    } else {
        let updated: Event = state.storage.get(&key).await?;
        Ok(Json(updated.to_events_v1()))
    }
}

/// PATCH an event via events.k8s.io/v1.
///
/// The patch is applied to the object in the version it was written against —
/// this one — and the result converted back before validation and storage, as
/// upstream does for every patch
/// (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go:323-338,:449-462`).
/// Applied to the stored core object instead, a patch naming `note`,
/// `regarding` or a `deprecated*` field would land on a key the core schema does
/// not own and leave the real field alone (#1940).
///
/// The generic path then runs `validate_event_update` on the result, because
/// upstream's patch and PUT paths share one `Store.Update` and therefore one
/// `ValidateUpdate`: on this version everything but `series` and metadata is
/// immutable, so a patch of `note` is a 422, not a write.
pub async fn patch_events_v1(
    state: State<Arc<ApiServerState>>,
    auth_ctx: Extension<AuthContext>,
    path: Path<(String, String)>,
    query: Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<EventV1>> {
    let hooks = crate::handlers::generic_patch::PatchHooks::<Event> {
        conversion: Some(crate::handlers::generic_patch::RequestVersionConversion {
            to_request_version: core_event_json_to_events_v1_ref,
            from_request_version: events_v1_json_onto_core_event,
        }),
        validate_update: Some(validate_events_v1_update),
    };
    let Json(patched) =
        crate::handlers::generic_patch::patch_namespaced_resource_with_hooks::<Event>(
            state,
            auth_ctx,
            path,
            query,
            headers,
            body,
            "events",
            "events.k8s.io",
            hooks,
        )
        .await?;
    Ok(Json(patched.to_events_v1()))
}

/// `ValidateEventUpdate` on the events.k8s.io/v1 path, as a plain function so
/// the generic PATCH path can hold it (`PatchHooks::validate_update`). Same call
/// the PUT handler above makes.
fn validate_events_v1_update(
    new_event: &Event,
    old_event: &Event,
) -> rusternetes_common::validation::field::ErrorList {
    rusternetes_common::validation::events::validate_event_update(
        new_event,
        old_event,
        rusternetes_common::validation::events::RequestVersion::EventsV1,
    )
}

/// `to_request_version` for the pair above: the stored core JSON in the
/// `events.k8s.io/v1` shape.
fn core_event_json_to_events_v1_ref(value: &serde_json::Value) -> serde_json::Value {
    core_event_json_to_events_v1(value.clone())
}

/// `from_request_version` for the pair above: a patched `events.k8s.io/v1`
/// document back onto the stored core shape, via
/// `EventV1::into_core` — the same conversion the create and update handlers
/// use, so PATCH cannot drift from them.
fn events_v1_json_onto_core_event(
    stored: &serde_json::Value,
    patched: serde_json::Value,
) -> serde_json::Value {
    let Ok(v1) = serde_json::from_value::<EventV1>(patched.clone()) else {
        // Best-effort, like the watch converter: hand back a document that will
        // not decode as the v1 type so the caller's own decode produces the
        // error message rather than this conversion swallowing it.
        return patched;
    };
    let Ok(mut core) = serde_json::to_value(v1.into_core()) else {
        return patched;
    };
    // Carry over whatever `Event::extra` was holding — keys neither schema
    // names, kept only so a stored object survives a round trip unchanged. The
    // patch was written against the v1 shape, so it cannot have named them.
    if let Some(extra) = serde_json::from_value::<Event>(stored.clone())
        .ok()
        .and_then(|event| event.extra)
    {
        if let Some(obj) = core.as_object_mut() {
            for (key, value) in extra {
                obj.entry(key).or_insert(value);
            }
        }
    }
    core
}

/// Convert one stored core-Event JSON object into the `events.k8s.io/v1` shape.
///
/// Used by the watch path, which hands the converter raw JSON rather than a
/// typed object. Best-effort like every other watch converter: an object that
/// will not decode is passed through unchanged, exactly as before.
fn core_event_json_to_events_v1(value: serde_json::Value) -> serde_json::Value {
    match serde_json::from_value::<Event>(value.clone()) {
        Ok(event) => serde_json::to_value(event.to_events_v1()).unwrap_or(value),
        Err(_) => value,
    }
}
