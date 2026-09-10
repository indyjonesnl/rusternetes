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
    resources::ResourceClaim,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_resourceclaim(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut claim): DumpingJson<ResourceClaim>,
) -> Result<(StatusCode, Json<ResourceClaim>)> {
    info!(
        "Creating ResourceClaim: {}/{}",
        namespace, claim.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "resourceclaims")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    claim.kind = "ResourceClaim".to_string();
    claim.api_version = "resource.k8s.io/v1".to_string();

    // Field validation (upstream resource ValidateResourceClaim, structural
    // subset): request count/uniqueness, exactly/firstAvailable coupling,
    // deviceClassName + allocationMode/count.
    {
        let errs = rusternetes_common::validation::resourceclaim::validate_resource_claim(&claim);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Ensure metadata exists and set defaults
    let metadata = &mut claim.metadata;
    metadata.namespace = Some(namespace.clone());

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
        info!("Dry-run: ResourceClaim validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(claim)));
    }

    let key = build_key("resourceclaims", Some(&namespace), name);
    let created = state.storage.create(&key, &claim).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_resourceclaim(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ResourceClaim>> {
    debug!("Getting ResourceClaim: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "resourceclaims")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceclaims", Some(&namespace), &name);
    let mut claim: ResourceClaim = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set in the response
    claim.kind = "ResourceClaim".to_string();
    claim.api_version = "resource.k8s.io/v1".to_string();

    Ok(Json(claim))
}

pub async fn list_resourceclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!(
            "Starting watch for resourceclaims in namespace: {}",
            namespace
        );
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
        return crate::handlers::watch::watch_namespaced_json(
            state,
            auth_ctx,
            namespace,
            "resourceclaims",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ResourceClaims in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourceclaims")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourceclaims", Some(&namespace));
    let mut claims: Vec<ResourceClaim> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut claims, &params)?;

    let mut list = List::new("ResourceClaimList", "resource.k8s.io/v1", claims);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::Json(list).into_response())
}

pub async fn list_all_resourceclaims(
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
        info!("Starting watch for all resourceclaims");
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
            "resourceclaims",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all ResourceClaims");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourceclaims")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourceclaims", None);
    let mut claims: Vec<ResourceClaim> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut claims, &params)?;

    let mut list = List::new("ResourceClaimList", "resource.k8s.io/v1", claims);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::Json(list).into_response())
}

pub async fn update_resourceclaim(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut claim): DumpingJson<ResourceClaim>,
) -> Result<Json<ResourceClaim>> {
    info!("Updating ResourceClaim: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "resourceclaims")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
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
        &build_key("resourceclaims", Some(&namespace), &name),
        "resource.k8s.io",
        "resourceclaims",
        &name,
    )
    .await?;

    // Ensure kind and apiVersion are set
    claim.kind = "ResourceClaim".to_string();
    claim.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata and set namespace/name
    {
        let metadata = &mut claim.metadata;
        metadata.namespace = Some(namespace.clone());
        metadata.name = name.clone();
    }

    let key = build_key("resourceclaims", Some(&namespace), &name);

    // Update validation (upstream ValidateResourceClaimUpdate): re-run create
    // checks and enforce spec immutability.
    if let Ok(old) = state.storage.get::<ResourceClaim>(&key).await {
        let errs = rusternetes_common::validation::resourceclaim::validate_resource_claim_update(
            &claim, &old,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceClaim validated successfully (not updated)");
        return Ok(Json(claim));
    }

    let updated = crate::handlers::lifecycle::update_inheriting_server_owned_metadata(
        &state.storage,
        &key,
        &mut claim,
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

pub async fn update_resourceclaim_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    DumpingJson(mut claim): DumpingJson<ResourceClaim>,
) -> Result<Json<ResourceClaim>> {
    info!("Updating ResourceClaim status: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "resourceclaims/status")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure metadata
    let metadata = &mut claim.metadata;
    metadata.namespace = Some(namespace.clone());
    metadata.name = name.clone();

    let key = build_key("resourceclaims", Some(&namespace), &name);

    // Upstream serves a subresource from the same `genericregistry.Store` as
    // its parent, with only the strategy swapped, so `Store.Update`'s
    // create-on-update gate applies to a status write exactly as it does to a
    // spec write (`registry/generic/registry/store.go:646-650`): the object has
    // to exist first, and the check runs before any validator (#1932).
    crate::handlers::lifecycle::reject_create_on_update(
        &*state.storage,
        &key,
        "resource.k8s.io",
        "resourceclaims",
        &name,
    )
    .await?;

    // Get existing claim to preserve spec
    let mut existing: ResourceClaim = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set
    existing.kind = "ResourceClaim".to_string();
    existing.api_version = "resource.k8s.io/v1".to_string();

    // Only update status
    existing.status = claim.status;

    let updated = state.storage.update(&key, &existing).await?;

    Ok(Json(updated))
}

pub async fn delete_resourceclaim(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ResourceClaim>> {
    info!("Deleting ResourceClaim: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "resourceclaims")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceclaims", Some(&namespace), &name);

    // Get the resource before deletion
    let resource: ResourceClaim = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceClaim validated successfully (not deleted)");
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
        let updated: ResourceClaim = state.storage.get(&key).await?;
        return Ok(Json(updated));
    }

    Ok(Json(resource))
}

// Use the macro to create a PATCH handler (namespace-scoped)
crate::patch_handler_namespaced!(
    patch_resourceclaim,
    ResourceClaim,
    "resourceclaims",
    "resource.k8s.io"
);

pub async fn deletecollection_resourceclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection resourceclaims in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "resourceclaims")
        .with_namespace(&namespace)
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
        info!("Dry-run: ResourceClaim collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all resourceclaims in the namespace
    let prefix = build_prefix("resourceclaims", Some(&namespace));
    let mut items = state.storage.list::<ResourceClaim>(&prefix).await?;

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
                let key = build_key("resourceclaims", Some(&namespace), name);

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
        "DeleteCollection completed: {} resourceclaims deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
