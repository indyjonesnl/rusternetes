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
    resources::DeviceClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut dc): DumpingJson<DeviceClass>,
) -> Result<(StatusCode, Json<DeviceClass>)> {
    info!("Creating DeviceClass: {}", dc.metadata.name);

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "deviceclasses")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Set kind and apiVersion
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    // Field validation (upstream resource ValidateDeviceClass, structural
    // subset): selector/config caps + cel selector required.
    {
        let errs = rusternetes_common::validation::deviceclass::validate_device_class(&dc);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Ensure metadata exists and set defaults
    let metadata = &mut dc.metadata;

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
        info!("Dry-run: DeviceClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(dc)));
    }

    let key = build_key("deviceclasses", None, name);
    let created = state.storage.create(&key, &dc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<DeviceClass>> {
    debug!("Getting DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deviceclasses", None, &name);
    let mut dc: DeviceClass = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set in the response
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    Ok(Json(dc))
}

pub async fn list_deviceclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Intercept watch requests
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Starting watch for deviceclasses");
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
            "deviceclasses",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all DeviceClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "deviceclasses")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("deviceclasses", None);
    let mut dcs: Vec<DeviceClass> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut dcs, &params)?;

    let mut list = List::new("DeviceClassList", "resource.k8s.io/v1", dcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn update_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut dc): DumpingJson<DeviceClass>,
) -> Result<Json<DeviceClass>> {
    info!("Updating DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }
    // A PUT to an object that does not exist is a 404, not a create: upstream
    // consults the strategy's `AllowCreateOnUpdate()` in `Store.Update`
    // (registry/generic/registry/store.go:646-650) and only nine resources opt
    // in. The check sits ahead of every validator there, so it runs here before
    // validation too (#1905).
    crate::handlers::lifecycle::reject_create_on_update(
        &*state.storage,
        &build_key("deviceclasses", None, &name),
        "resource.k8s.io",
        "deviceclasses",
        &name,
    )
    .await?;

    // Ensure kind and apiVersion are set
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    // Field validation on update (upstream ValidateDeviceClassUpdate re-runs
    // ValidateDeviceClass on the new object — the spec is mutable).
    {
        let errs = rusternetes_common::validation::deviceclass::validate_device_class(&dc);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Ensure metadata and set name
    let metadata = &mut dc.metadata;
    metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass validated successfully (not updated)");
        return Ok(Json(dc));
    }

    let key = build_key("deviceclasses", None, &name);
    let updated = crate::handlers::lifecycle::update_inheriting_server_owned_metadata(
        &state.storage,
        &key,
        &mut dc,
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

pub async fn delete_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<DeviceClass>> {
    info!("Deleting DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deviceclasses", None, &name);

    // Get the resource before deletion
    let resource: DeviceClass = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass validated successfully (not deleted)");
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
        let updated: DeviceClass = state.storage.get(&key).await?;
        return Ok(Json(updated));
    }

    Ok(Json(resource))
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_deviceclass,
    DeviceClass,
    "deviceclasses",
    "resource.k8s.io"
);

pub async fn deletecollection_deviceclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection deviceclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "deviceclasses")
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
        info!("Dry-run: DeviceClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all deviceclasses
    let prefix = build_prefix("deviceclasses", None);
    let mut items = state.storage.list::<DeviceClass>(&prefix).await?;

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
                let key = build_key("deviceclasses", None, name);

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
        "DeleteCollection completed: {} deviceclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
