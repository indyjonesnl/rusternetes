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
    resources::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Compile every `matchConditions[i].expression`.
///
/// Upstream reaches this from inside `validateMatchCondition`
/// (`pkg/apis/admissionregistration/validation/validation.go:989` →
/// `validateMatchConditionsExpression`, `:1100`), which compiles against a
/// typed CEL environment and reports the failure as a `field.Invalid` on
/// `matchConditions[i].expression`. Rusternetes has no typed environment, so
/// this compiles with the plain `cel` crate and tolerates the errors that come
/// from the missing declarations rather than from the expression itself.
///
/// It is deliberately *not* part of `validate_match_conditions`: that function
/// lives in `rusternetes-common`, which does not depend on the CEL crate.
fn compile_match_conditions(
    conditions: &[rusternetes_common::resources::MatchCondition],
) -> Result<()> {
    for (i, condition) in conditions.iter().enumerate() {
        // Compile the expression — catch panics from the antlr4rust parser,
        // which panics on some invalid expressions instead of returning Err.
        let expr_clone = condition.expression.clone();
        let compile_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cel::Program::compile(&expr_clone)
        }));
        let program = match compile_result {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                // Allow "no such key"-class errors: the CEL crate type-checks at
                // compile time and rejects references to declarations we do not
                // supply (`object.metadata`), which are valid at admission time.
                if is_missing_declaration(&e.to_string()) {
                    continue;
                }
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "matchConditions[{i}].expression: compilation failed: {e}"
                )));
            }
            Err(_panic) => {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "matchConditions[{i}].expression: compilation failed: invalid CEL expression '{}'",
                    condition.expression
                )));
            }
        };

        // The CEL crate's parser accepts some expressions Kubernetes rejects.
        // Executing with an empty context catches the genuinely invalid ones.
        let test_ctx = cel::Context::default();
        let exec_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| program.execute(&test_ctx)));
        match exec_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                if !is_missing_declaration(&e.to_string()) {
                    return Err(rusternetes_common::Error::InvalidResource(format!(
                        "matchConditions[{i}].expression: compilation failed: {e}"
                    )));
                }
            }
            Err(_panic) => {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "matchConditions[{i}].expression: compilation failed: invalid CEL expression '{}'",
                    condition.expression
                )));
            }
        }
    }
    Ok(())
}

/// A CEL error that comes from a declaration this server does not supply,
/// rather than from the expression being malformed.
fn is_missing_declaration(err: &str) -> bool {
    let err = err.to_lowercase();
    err.contains("no such key")
        || err.contains("not found")
        || err.contains("undeclared")
        || err.contains("undefined")
        || err.contains("no matching overload")
}

// ===== ValidatingWebhookConfiguration Handlers =====

pub async fn create_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<ValidatingWebhookConfiguration>,
) -> Result<(StatusCode, Json<ValidatingWebhookConfiguration>)> {
    info!(
        "Creating ValidatingWebhookConfiguration: {}",
        config.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &config.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateValidatingWebhookConfiguration).
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_validating_webhook_configuration(&config);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Compile the matchConditions CEL expressions. The shape rules
    // (`Required`, qualified name, duplicates, the 64 cap) ran above in the
    // ported validator — upstream reaches the compile step from inside the same
    // `validateMatchCondition` (`validation.go:989` →
    // `validateMatchConditionsExpression`, `:1100`).
    for hook in config.webhooks.iter().flatten() {
        compile_match_conditions(hook.match_conditions.as_deref().unwrap_or_default())?;
    }
    // Enrich metadata with system fields
    config.metadata.ensure_uid();
    config.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(config)));
    }

    let key = build_key(
        "validatingwebhookconfigurations",
        None,
        &config.metadata.name,
    );
    let created = state.storage.create(&key, &config).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    debug!("Getting ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);
    let config = state.storage.get(&key).await?;

    Ok(Json(config))
}

pub async fn update_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<ValidatingWebhookConfiguration>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    info!("Updating ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
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
        &build_key("validatingwebhookconfigurations", None, &name),
        "admissionregistration.k8s.io",
        "validatingwebhookconfigurations",
        &name,
    )
    .await?;

    config.metadata.name = name.clone();

    // Field validation on update. Upstream
    // `ValidateValidatingWebhookConfigurationUpdate` (`validation.go:729-740`)
    // re-runs the config validator on the new object, with
    // `ignoreMatchConditions` set from the old one: a `matchConditions` list
    // that is byte-for-byte unchanged is left alone, so an object stored before
    // a rule existed stays updatable.
    let stored: ValidatingWebhookConfiguration = state
        .storage
        .get(&build_key("validatingwebhookconfigurations", None, &name))
        .await?;
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_validating_webhook_configuration_update(&config, &stored);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // The CEL compile step is part of the same gated check upstream, so it is
    // skipped for an unchanged list too.
    if !rusternetes_common::validation::webhookconfiguration::ignore_validating_webhook_match_conditions(&config, &stored) {
        for hook in config.webhooks.iter().flatten() {
            compile_match_conditions(hook.match_conditions.as_deref().unwrap_or_default())?;
        }
    }

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not updated)");
        return Ok(Json(config));
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);

    let result = crate::handlers::lifecycle::update_inheriting_server_owned_metadata(
        &*state.storage,
        &key,
        &mut config,
    )
    .await?;

    // Upstream ShouldDeleteDuringUpdate: an update that drains the last
    // finalizer off an object already pending deletion removes it as part of
    // that same request (store.go:565).
    crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
        &*state.storage,
        &key,
        &result,
    )
    .await?;
    Ok(Json(result))
}

pub async fn delete_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    info!("Deleting ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);

    // Get the resource for finalizer handling
    let resource: ValidatingWebhookConfiguration = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
        &delete_opts,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ValidatingWebhookConfiguration = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_validating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingWebhookConfiguration>(
            state,
            auth_ctx,
            "validatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingwebhookconfigurations", None);
    let mut configs = state
        .storage
        .list::<ValidatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "ValidatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_validating_webhook,
    ValidatingWebhookConfiguration,
    "validatingwebhookconfigurations",
    "admissionregistration.k8s.io"
);

// ===== MutatingWebhookConfiguration Handlers =====

pub async fn create_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<MutatingWebhookConfiguration>,
) -> Result<(StatusCode, Json<MutatingWebhookConfiguration>)> {
    info!(
        "Creating MutatingWebhookConfiguration: {}",
        config.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &config.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateMutatingWebhookConfiguration).
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_mutating_webhook_configuration(&config);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Compile the matchConditions CEL expressions. The shape rules
    // (`Required`, qualified name, duplicates, the 64 cap) ran above in the
    // ported validator — upstream reaches the compile step from inside the same
    // `validateMatchCondition` (`validation.go:989` →
    // `validateMatchConditionsExpression`, `:1100`).
    for hook in config.webhooks.iter().flatten() {
        compile_match_conditions(hook.match_conditions.as_deref().unwrap_or_default())?;
    }
    // Enrich metadata with system fields
    config.metadata.ensure_uid();
    config.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(config)));
    }

    let key = build_key("mutatingwebhookconfigurations", None, &config.metadata.name);
    let created = state.storage.create(&key, &config).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    debug!("Getting MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);
    let config = state.storage.get(&key).await?;

    Ok(Json(config))
}

pub async fn update_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<MutatingWebhookConfiguration>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    info!("Updating MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
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
        &build_key("mutatingwebhookconfigurations", None, &name),
        "admissionregistration.k8s.io",
        "mutatingwebhookconfigurations",
        &name,
    )
    .await?;

    config.metadata.name = name.clone();

    // Field validation on update. Upstream
    // `ValidateMutatingWebhookConfigurationUpdate` (`validation.go:742-753`)
    // re-runs the config validator on the new object, with
    // `ignoreMatchConditions` set from the old one.
    let stored: MutatingWebhookConfiguration = state
        .storage
        .get(&build_key("mutatingwebhookconfigurations", None, &name))
        .await?;
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_mutating_webhook_configuration_update(&config, &stored);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // The CEL compile step is part of the same gated check upstream, so it is
    // skipped for an unchanged list too.
    if !rusternetes_common::validation::webhookconfiguration::ignore_mutating_webhook_match_conditions(&config, &stored) {
        for hook in config.webhooks.iter().flatten() {
            compile_match_conditions(hook.match_conditions.as_deref().unwrap_or_default())?;
        }
    }

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not updated)");
        return Ok(Json(config));
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);

    let result = crate::handlers::lifecycle::update_inheriting_server_owned_metadata(
        &*state.storage,
        &key,
        &mut config,
    )
    .await?;

    // Upstream ShouldDeleteDuringUpdate: an update that drains the last
    // finalizer off an object already pending deletion removes it as part of
    // that same request (store.go:565).
    crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
        &*state.storage,
        &key,
        &result,
    )
    .await?;
    Ok(Json(result))
}

pub async fn delete_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    info!("Deleting MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);

    // Get the resource for finalizer handling
    let resource: MutatingWebhookConfiguration = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
        &delete_opts,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: MutatingWebhookConfiguration = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_mutating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<MutatingWebhookConfiguration>(
            state,
            auth_ctx,
            "mutatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing MutatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("mutatingwebhookconfigurations", None);
    let mut configs = state
        .storage
        .list::<MutatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "MutatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_mutating_webhook,
    MutatingWebhookConfiguration,
    "mutatingwebhookconfigurations",
    "admissionregistration.k8s.io"
);

pub async fn deletecollection_validatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection validatingwebhookconfigurations with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "validatingwebhookconfigurations",
    )
    .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all validatingwebhookconfigurations
    let prefix = build_prefix("validatingwebhookconfigurations", None);
    let mut items = state
        .storage
        .list::<ValidatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("validatingwebhookconfigurations", None, &item.metadata.name);

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
        "DeleteCollection completed: {} validatingwebhookconfigurations deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_mutatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection mutatingwebhookconfigurations with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "mutatingwebhookconfigurations",
    )
    .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all mutatingwebhookconfigurations
    let prefix = build_prefix("mutatingwebhookconfigurations", None);
    let mut items = state
        .storage
        .list::<MutatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("mutatingwebhookconfigurations", None, &item.metadata.name);

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
        "DeleteCollection completed: {} mutatingwebhookconfigurations deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
