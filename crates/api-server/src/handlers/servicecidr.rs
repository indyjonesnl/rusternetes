use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ServiceCIDR,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut servicecidr): DumpingJson<ServiceCIDR>,
) -> Result<(StatusCode, Json<ServiceCIDR>)> {
    info!("Creating ServiceCIDR: {}", servicecidr.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &servicecidr.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate spec (upstream networking ValidateServiceCIDR): cidrs 1..=2,
    // each a valid CIDR, dual-stack one-per-family.
    let errs = rusternetes_common::validation::servicecidr::validate_service_cidr(&servicecidr);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "servicecidrs")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    servicecidr.metadata.ensure_uid();
    servicecidr.metadata.ensure_creation_timestamp();

    // Create writes no status. Upstream's registry strategy clears whatever the
    // client sent (`pkg/registry/networking/servicecidr/strategy.go:67-71`);
    // the `Ready` condition is the servicecidrs controller's to set
    // (`pkg/controller/servicecidrs/servicecidrs_controller.go:341-346`), which
    // is also the only component that can flip it to `False` with reason
    // `Terminating` while IPAddresses still reference the range. That
    // controller lives in `crates/controller-manager/src/controllers/servicecidr.rs`.
    servicecidr.status = None;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ServiceCIDR validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(servicecidr)));
    }

    // ServiceCIDR is cluster-scoped (no namespace)
    let key = build_key("servicecidrs", None, &servicecidr.metadata.name);
    let created = state.storage.create(&key, &servicecidr).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ServiceCIDR>> {
    debug!("Getting ServiceCIDR: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "servicecidrs")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("servicecidrs", None, &name);
    let servicecidr = state.storage.get(&key).await?;

    Ok(Json(servicecidr))
}

pub async fn update_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut servicecidr): DumpingJson<ServiceCIDR>,
) -> Result<Json<ServiceCIDR>> {
    info!("Updating ServiceCIDR: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "servicecidrs")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    servicecidr.metadata.name = name.clone();

    let key = build_key("servicecidrs", None, &name);

    // Immutability on update (upstream ValidateServiceCIDRUpdate): spec.cidrs is
    // immutable, except single→dual-stack expansion (append one CIDR).
    if let Ok(old) = state
        .storage
        .get::<rusternetes_common::resources::ServiceCIDR>(&key)
        .await
    {
        let errs = rusternetes_common::validation::servicecidr::validate_service_cidr_update(
            &servicecidr,
            &old,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ServiceCIDR validated successfully (not updated)");
        return Ok(Json(servicecidr));
    }

    // Reinstate the server-owned metadata a PUT body may omit: uid,
    // creationTimestamp and a pending deletion. A locally built object —
    // what the dynamic client's Update() sends — carries none of them, and
    // storing the blanks orphans every child, because ownerReferences[].uid
    // then matches no live owner and the garbage collector deletes them
    // (#1605, #1793). Upstream applies this to every resource at once in
    // registry/rest/update.go::BeforeUpdate (lines 131-146).
    if let Ok(stored) = state.storage.get::<ServiceCIDR>(&key).await {
        crate::handlers::lifecycle::inherit_server_owned_metadata(
            &mut servicecidr.metadata,
            &stored.metadata,
        );
    }
    let updated = state.storage.update(&key, &servicecidr).await?;

    Ok(Json(updated))
}

pub async fn delete_servicecidr(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ServiceCIDR>> {
    info!("Deleting ServiceCIDR: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "servicecidrs")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("servicecidrs", None, &name);

    // Get the resource for finalizer handling
    let servicecidr: ServiceCIDR = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ServiceCIDR validated successfully (not deleted)");
        return Ok(Json(servicecidr));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &servicecidr,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(servicecidr))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ServiceCIDR = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    debug!("Listing ServiceCIDRs");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ServiceCIDR>(
            state,
            auth_ctx,
            "servicecidrs",
            "networking.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "servicecidrs")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("servicecidrs", None);
    let servicecidrs = state.storage.list::<ServiceCIDR>(&prefix).await?;

    let mut list = List::new("ServiceCIDRList", "networking.k8s.io/v1", servicecidrs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_servicecidr,
    ServiceCIDR,
    "servicecidrs",
    "networking.k8s.io"
);

pub async fn deletecollection_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection servicecidrs with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "servicecidrs")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ServiceCIDR collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all servicecidrs
    let prefix = build_prefix("servicecidrs", None);
    let mut items = state.storage.list::<ServiceCIDR>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("servicecidrs", None, &item.metadata.name);

        // Handle deletion with finalizers
        let deleted_immediately =
            match crate::handlers::finalizers::delete_collection_item(&state.storage, &key, &item)
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
        "DeleteCollection completed: {} servicecidrs deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
