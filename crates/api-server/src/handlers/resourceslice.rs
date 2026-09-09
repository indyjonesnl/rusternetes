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
    resources::ResourceSlice,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_resourceslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut slice): DumpingJson<ResourceSlice>,
) -> Result<(StatusCode, Json<ResourceSlice>)> {
    info!("Creating ResourceSlice: {}", slice.metadata.name);

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "resourceslices")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    slice.kind = "ResourceSlice".to_string();
    slice.api_version = "resource.k8s.io/v1".to_string();

    // Field validation (upstream resource ValidateResourceSlice): driver, pool,
    // exactly-one node selection, device set caps + unique names.
    {
        let errs = rusternetes_common::validation::resourceslice::validate_resource_slice(&slice);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Ensure metadata exists and set defaults
    let metadata = &mut slice.metadata;

    // Generate UID and timestamp if not present
    if metadata.uid.is_empty() {
        metadata.uid = uuid::Uuid::new_v4().to_string();
    }
    if metadata.creation_timestamp.is_none() {
        metadata.creation_timestamp = Some(chrono::Utc::now());
    }

    let name =
        crate::handlers::validation::require_optional_object_name(Some(metadata.name.as_str()))?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceSlice validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(slice)));
    }

    let key = build_key("resourceslices", None, name);
    let created = state.storage.create(&key, &slice).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_resourceslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ResourceSlice>> {
    debug!("Getting ResourceSlice: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "resourceslices")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceslices", None, &name);
    let mut slice: ResourceSlice = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set in the response
    slice.kind = "ResourceSlice".to_string();
    slice.api_version = "resource.k8s.io/v1".to_string();

    Ok(Json(slice))
}

pub async fn list_resourceslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Starting watch for resourceslices");
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
        return crate::handlers::watch::watch_cluster_scoped_json(
            state,
            auth_ctx,
            "resourceslices",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all ResourceSlices");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourceslices")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourceslices", None);
    let mut slices: Vec<ResourceSlice> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut slices, &params)?;

    let mut list = List::new("ResourceSliceList", "resource.k8s.io/v1", slices);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn update_resourceslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut slice): DumpingJson<ResourceSlice>,
) -> Result<Json<ResourceSlice>> {
    info!("Updating ResourceSlice: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "resourceslices")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    slice.kind = "ResourceSlice".to_string();
    slice.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata and set name
    {
        let metadata = &mut slice.metadata;
        metadata.name = name.clone();
    }

    let key = build_key("resourceslices", None, &name);

    // Update validation (upstream ValidateResourceSliceUpdate): re-run create
    // checks and enforce driver/pool.name/nodeName immutability.
    if let Ok(old) = state.storage.get::<ResourceSlice>(&key).await {
        let errs = rusternetes_common::validation::resourceslice::validate_resource_slice_update(
            &slice, &old,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceSlice validated successfully (not updated)");
        return Ok(Json(slice));
    }

    let updated = crate::handlers::lifecycle::update_inheriting_server_owned_metadata(
        &state.storage,
        &key,
        &mut slice,
    )
    .await?;

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

pub async fn delete_resourceslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ResourceSlice>> {
    info!("Deleting ResourceSlice: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "resourceslices")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceslices", None, &name);

    // Get the resource before deletion
    let resource: ResourceSlice = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceSlice validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Finalizers now apply: the shared helper stamps deletionTimestamp and
    // honours propagationPolicy instead of deleting outright (#1895).
    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &resource,
        &delete_opts,
    )
    .await?;

    if has_finalizers {
        let updated: ResourceSlice = state.storage.get(&key).await?;
        return Ok(Json(updated));
    }

    Ok(Json(resource))
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_resourceslice,
    ResourceSlice,
    "resourceslices",
    "resource.k8s.io"
);

pub async fn deletecollection_resourceslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection resourceslices with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "resourceslices")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceSlice collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all resourceslices
    let prefix = build_prefix("resourceslices", None);
    let mut items = state.storage.list::<ResourceSlice>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        // Extract name from metadata (handle Option)
        {
            let metadata = &item.metadata;
            {
                let name = &metadata.name;
                let key = build_key("resourceslices", None, name);

                // Same shared helper every other collection delete uses: it
                // stamps deletionTimestamp when finalizers remain, honours
                // propagationPolicy, and tolerates an item that vanished
                // between the list and the delete (#1895).
                let deleted_immediately = match crate::handlers::finalizers::delete_collection_item(
                    &state.storage,
                    &key,
                    &item,
                    &delete_opts,
                )
                .await?
                {
                    Some(deleted) => deleted,
                    // Already gone — a concurrent deleter won the race;
                    // upstream DeleteCollection ignores NotFound rather
                    // than failing the request.
                    None => continue,
                };
                if deleted_immediately {
                    deleted_count += 1;
                }
            }
        }
    }

    info!(
        "DeleteCollection completed: {} resourceslices deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
