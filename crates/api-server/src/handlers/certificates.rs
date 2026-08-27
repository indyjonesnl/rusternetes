use crate::patch::{apply_patch, PatchType};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::CertificateSigningRequest,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut csr): DumpingJson<CertificateSigningRequest>,
) -> Result<(StatusCode, Json<CertificateSigningRequest>)> {
    info!("Creating CertificateSigningRequest: {}", csr.metadata.name);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // Full create-time ValidateObjectMeta (#1087). CertificateSigningRequest is
    // cluster-scoped and imposes no name-format constraint
    // (ValidateCertificateRequestName returns nil).
    crate::handlers::validation::validate_create_object_meta(
        &csr.metadata,
        None,
        crate::handlers::validation::NameKind::NoConstraint,
    )?;

    // Field validation (mirrors upstream ValidateCertificateSigningRequestCreate).
    {
        let errs =
            rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_create(&csr);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Enrich metadata with system fields
    csr.metadata.ensure_uid();
    csr.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(csr)));
    }

    let key = build_key("certificatesigningrequests", None, &csr.metadata.name);
    let created = state.storage.create(&key, &csr).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CertificateSigningRequest>> {
    debug!("Getting CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);
    let csr = state.storage.get(&key).await?;

    Ok(Json(csr))
}

pub async fn update_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Updating CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    csr.metadata.name = name.clone();
    csr.kind = "CertificateSigningRequest".to_string();
    csr.api_version = "certificates.k8s.io/v1".to_string();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not updated)");
        return Ok(Json(csr));
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Check resourceVersion for optimistic concurrency
    if let Ok(existing) = state.storage.get::<CertificateSigningRequest>(&key).await {
        crate::handlers::lifecycle::check_resource_version(
            existing.metadata.resource_version.as_deref(),
            csr.metadata.resource_version.as_deref(),
            &name,
        )?;
        // Preserve status if not provided
        if csr.status.is_none() {
            csr.status = existing.status.clone();
        }
        // Update validation (upstream ValidateCertificateSigningRequestUpdate):
        // re-validates spec/conditions/cert against the new object and forbids
        // removing existing Approved/Denied/Failed conditions, adding/modifying
        // Approved/Denied conditions, or mutating status.certificate via the
        // main resource endpoint.
        let errs = rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_update_main(&csr, &existing);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Reinstate the server-owned metadata a PUT body may omit: uid,
    // creationTimestamp and a pending deletion. A locally built object —
    // what the dynamic client's Update() sends — carries none of them, and
    // storing the blanks orphans every child, because ownerReferences[].uid
    // then matches no live owner and the garbage collector deletes them
    // (#1605, #1793). Upstream applies this to every resource at once in
    // registry/rest/update.go::BeforeUpdate (lines 131-146).
    if let Ok(stored) = state.storage.get::<CertificateSigningRequest>(&key).await {
        crate::handlers::lifecycle::inherit_server_owned_metadata(
            &mut csr.metadata,
            &stored.metadata,
        );
    }
    let result = match state.storage.update(&key, &csr).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &csr).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Deleting CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Get the resource for finalizer handling
    let resource: CertificateSigningRequest = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: CertificateSigningRequest = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_certificate_signing_requests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<CertificateSigningRequest>(
            state,
            auth_ctx,
            "certificatesigningrequests",
            "certificates.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing CertificateSigningRequests");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("certificatesigningrequests", None);
    let mut items = state
        .storage
        .list::<CertificateSigningRequest>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "CertificateSigningRequestList",
        "certificates.k8s.io/v1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Status subresource handlers
pub async fn get_certificate_signing_request_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CertificateSigningRequest>> {
    debug!("Getting CertificateSigningRequest status: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "certificatesigningrequests/status")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);
    let csr = state.storage.get(&key).await?;

    Ok(Json(csr))
}

pub async fn update_certificate_signing_request_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    DumpingJson(updated_csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    update_csr_status_inner(
        State(state),
        Extension(auth_ctx),
        Path(name),
        updated_csr,
        false,
    )
    .await
}

/// Shared body for the `/status` and `/approval` subresource updates. `approval`
/// selects which upstream update validator runs:
/// `ValidateCertificateSigningRequestApprovalUpdate` (allows setting
/// Approved/Denied conditions) vs `…StatusUpdate` (allows setting the cert).
async fn update_csr_status_inner(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    updated_csr: CertificateSigningRequest,
    approval: bool,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Updating CertificateSigningRequest status: {}", name);

    let subresource = if approval {
        "certificatesigningrequests/approval"
    } else {
        "certificatesigningrequests/status"
    };
    let attrs = RequestAttributes::new(auth_ctx.user, "update", subresource)
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Get existing CSR; keep the pre-update copy for the update-diff rules.
    let mut existing_csr: CertificateSigningRequest = state.storage.get(&key).await?;
    let old_csr = existing_csr.clone();

    // Update status and metadata (K8s allows metadata changes via status subresource)
    existing_csr.status = updated_csr.status;

    // Update validation against the stored object — upstream
    // ValidateCertificateSigningRequest{Status,Approval}Update. Runs the
    // create-style field checks (conditions valid type/status, Approved/Denied
    // mutual exclusion, certificate PEM parse) plus the diff rules: existing
    // Approved/Denied/Failed conditions may not be removed; the /status path
    // may set (but not modify an existing) certificate; only the /approval path
    // may add/modify Approved/Denied conditions.
    let errs = if approval {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_approval_update(&existing_csr, &old_csr)
    } else {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_status_update(&existing_csr, &old_csr)
    };
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Merge annotations from the patch into existing (don't replace entirely)
    if let Some(new_annotations) = updated_csr.metadata.annotations {
        let annotations = existing_csr
            .metadata
            .annotations
            .get_or_insert_with(Default::default);
        for (k, v) in new_annotations {
            annotations.insert(k, v);
        }
    }
    // Merge labels from the patch into existing
    if let Some(new_labels) = updated_csr.metadata.labels {
        let labels = existing_csr
            .metadata
            .labels
            .get_or_insert_with(Default::default);
        for (k, v) in new_labels {
            labels.insert(k, v);
        }
    }

    let result = state.storage.update(&key, &existing_csr).await?;

    Ok(Json(result))
}

// Approval subresource — same status-merge body, but runs the approval update
// validator (may set Approved/Denied conditions; may not set the certificate).
pub async fn approve_certificate_signing_request(
    state: State<Arc<ApiServerState>>,
    auth_ctx: Extension<AuthContext>,
    name: Path<String>,
    DumpingJson(csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    update_csr_status_inner(state, auth_ctx, name, csr, true).await
}

/// Shared body for the `/status` and `/approval` subresource PATCH endpoints.
///
/// Unlike the generic cluster PATCH handler (which replaces the object wholesale
/// via a full-object round-trip), this:
///   * applies the patch document as a genuine merge/JSON patch **onto the
///     stored object**, so a partial `status` patch (e.g. one that only sets
///     `status.certificate`) preserves conditions an earlier `/approval` write
///     added — a full-object replace would drop the Approved condition and trip
///     `ValidateCertificateSigningRequestStatusUpdate`'s "updates may not remove
///     a condition of type \"Approved\"" rule;
///   * never mutates `spec` (subresource writes may not change spec — it is
///     restored from the stored object); and
///   * runs the subresource-specific update validator (`…ApprovalUpdate` vs
///     `…StatusUpdate`) so only the `/approval` path may add Approved/Denied
///     conditions and only the `/status` path may set the certificate.
///
/// Mirrors upstream's subresource PATCH handling, which decodes the patch into
/// the versioned object, then routes through the subresource's update strategy.
async fn patch_csr_subresource_inner(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
    approval: bool,
) -> Result<Json<CertificateSigningRequest>> {
    let subresource = if approval {
        "certificatesigningrequests/approval"
    } else {
        "certificatesigningrequests/status"
    };
    info!("Patching CertificateSigningRequest {subresource}: {name}");

    let attrs = RequestAttributes::new(auth_ctx.user, "patch", subresource)
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // Resolve the patch type. `normalize_content_type_middleware` rewrites the
    // request content-type to `application/json` and stashes the original in
    // `x-original-content-type`, so consult that first.
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/merge-patch+json");
    let patch_type = PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    let key = build_key("certificatesigningrequests", None, &name);
    let old_csr: CertificateSigningRequest = state.storage.get(&key).await?;

    let current_json = serde_json::to_value(&old_csr)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let patch_json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid patch: {e}")))?;
    let patched_json = apply_patch(&current_json, &patch_json, patch_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    let mut new_csr: CertificateSigningRequest = serde_json::from_value(patched_json)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid result: {e}")))?;

    // Subresource writes never mutate spec or the object identity.
    new_csr.spec = old_csr.spec.clone();
    new_csr.metadata.name = name.clone();

    let errs = if approval {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_approval_update(&new_csr, &old_csr)
    } else {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_status_update(&new_csr, &old_csr)
    };
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    let result = state.storage.update(&key, &new_csr).await?;
    Ok(Json(result))
}

/// PATCH `…/certificatesigningrequests/{name}/status`.
pub async fn patch_certificate_signing_request_status(
    state: State<Arc<ApiServerState>>,
    auth_ctx: Extension<AuthContext>,
    name: Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<CertificateSigningRequest>> {
    patch_csr_subresource_inner(state, auth_ctx, name, headers, body, false).await
}

/// PATCH `…/certificatesigningrequests/{name}/approval`.
pub async fn patch_certificate_signing_request_approval(
    state: State<Arc<ApiServerState>>,
    auth_ctx: Extension<AuthContext>,
    name: Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<CertificateSigningRequest>> {
    patch_csr_subresource_inner(state, auth_ctx, name, headers, body, true).await
}

crate::patch_handler_cluster!(
    patch_certificate_signing_request,
    CertificateSigningRequest,
    "certificatesigningrequests",
    "certificates.k8s.io"
);

pub async fn deletecollection_certificatesigningrequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection certificatesigningrequests with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "certificatesigningrequests",
    )
    .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all certificatesigningrequests
    let prefix = build_prefix("certificatesigningrequests", None);
    let mut items = state
        .storage
        .list::<CertificateSigningRequest>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("certificatesigningrequests", None, &item.metadata.name);

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
        "DeleteCollection completed: {} certificatesigningrequests deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
