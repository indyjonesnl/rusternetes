//! Port of `endpoints/handlers/delete.go` (`DeleteResource`, :57-197;
//! `DeleteCollection`, :199-340).

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::deletion::DeleteOptions;
use rusternetes_common::validation::metav1::validate_delete_options;
use rusternetes_common::{Error, List, Result, Status};

use super::admission::{Admission, DeleteValidation};
use super::rest::{authorize, dry_run_param, is_dry_run, respond, RequestScope};
use crate::registry::generic::Deleted;
use crate::registry::rest::{zero_delete_options, Object, RequestContext};
use crate::state::ApiServerState;

/// `DeleteOptions` from the request (delete.go:86-126): a non-empty body is
/// decoded as `DeleteOptions` and the query string ignored; an empty body
/// takes the options from the query. Then `ValidateDeleteOptions`.
pub fn decode_delete_options(
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<DeleteOptions> {
    let mut options = if !body.is_empty() {
        serde_json::from_slice::<DeleteOptions>(body)
            .map_err(|e| Error::BadRequest(e.to_string()))?
    } else {
        let bad_request = |e: &dyn std::fmt::Display| Error::BadRequest(e.to_string());
        DeleteOptions {
            grace_period_seconds: params
                .get("gracePeriodSeconds")
                .map(|v| v.parse::<i64>())
                .transpose()
                .map_err(|e| bad_request(&e))?,
            propagation_policy: params
                .get("propagationPolicy")
                .map(|v| serde_json::from_value(serde_json::Value::String(v.clone())))
                .transpose()
                .map_err(|e| bad_request(&e))?,
            orphan_dependents: params
                .get("orphanDependents")
                .map(|v| rusternetes_common::query::k8s_query_bool(v)),
            dry_run: dry_run_param(params),
            ..zero_delete_options()
        }
    };
    // The AllowUnsafeMalformedObjectDeletion gate is off, so the option is
    // dropped (delete.go:127-129).
    options.ignore_store_read_error_with_cluster_breaking_potential = None;

    let errs = validate_delete_options(&options);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }
    Ok(options)
}

/// DELETE of a named object.
#[allow(clippy::too_many_arguments)]
pub async fn delete_resource<T: Object>(
    state: &ApiServerState,
    scope: &RequestScope<T>,
    user: &UserInfo,
    namespace: Option<&str>,
    name: &str,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<Response> {
    authorize(
        state,
        user,
        "delete",
        &scope.resource,
        scope.subresource,
        namespace,
        Some(name),
    )
    .await?;

    let options = decode_delete_options(params, body)?;
    let ctx = RequestContext::new(namespace);
    let admission = Admission {
        state,
        kind: &scope.kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace,
        user,
        dry_run: is_dry_run(options.dry_run.as_deref()),
    };
    let validation = DeleteValidation {
        admission: &admission,
    };
    let orphan_dependents = options.orphan_dependents;
    let (result, deleted) = scope
        .store
        .delete(&ctx, name, Some(&validation), options)
        .await?;

    // delete.go:170-179: 202 only for a cascading delete that did not
    // complete.
    let status = if !deleted && orphan_dependents == Some(false) {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok(match result {
        Deleted::Object(obj) => respond(status, &obj, &ctx),
        Deleted::Status(details) => {
            let mut body = Status::success();
            body.details = Some(details);
            respond(status, &body, &ctx)
        }
    })
}

/// DELETE of a collection: every object the selectors match.
pub async fn delete_collection<T: Object>(
    state: &ApiServerState,
    scope: &RequestScope<T>,
    user: &UserInfo,
    namespace: Option<&str>,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<Response> {
    authorize(
        state,
        user,
        "deletecollection",
        &scope.resource,
        scope.subresource,
        namespace,
        None,
    )
    .await?;

    let options = decode_delete_options(params, body)?;
    let ctx = RequestContext::new(namespace);

    let admission = Admission {
        state,
        kind: &scope.kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace,
        user,
        dry_run: is_dry_run(options.dry_run.as_deref()),
    };
    let validation = DeleteValidation {
        admission: &admission,
    };
    let items = scope
        .store
        .delete_collection(&ctx, Some(&validation), &options, params)
        .await?;

    let list = List::new(
        format!("{}List", scope.kind.kind),
        scope.api_version(),
        items,
    );
    Ok(respond(StatusCode::OK, &list, &ctx))
}
