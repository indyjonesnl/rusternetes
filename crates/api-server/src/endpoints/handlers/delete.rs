//! Port of `endpoints/handlers/delete.go` (`DeleteResource`, :57-197;
//! `DeleteCollection`, :199-340).

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::deletion::DeleteOptions;
use rusternetes_common::validation::field::{Error as FieldError, Path};
use rusternetes_common::validation::metav1::validate_delete_options;
use rusternetes_common::{Error, List, Result, Status};

use super::admission::{Admission, DeleteValidation};
use super::rest::{authorize, dry_run_param, is_dry_run, respond, respond_object, RequestScope};
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
    let ctx =
        RequestContext::new(namespace).with_group_version(&scope.kind.group, &scope.kind.version);
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
        Deleted::Object(obj) => respond_object(scope, status, &obj, &ctx),
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

    // delete.go:242-247: `ValidateListOptions` precedes decoding the body;
    // a failure is `NewInvalid(meta.k8s.io ListOptions, "", errs)`.
    let list_errs = validate_list_options(params);
    if !list_errs.is_empty() {
        return Err(Error::new_invalid(
            "meta.k8s.io",
            "ListOptions",
            "",
            list_errs,
        ));
    }

    let options = decode_delete_options(params, body)?;
    let ctx =
        RequestContext::new(namespace).with_group_version(&scope.kind.group, &scope.kind.version);

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
    let deleted = scope
        .store
        .delete_collection(&ctx, Some(&validation), &options, params)
        .await?;

    let mut list = List::new(
        format!("{}List", scope.kind.kind),
        scope.api_version(),
        deleted.items,
    );
    // The response is the listed page (`listObj`, store.go:1366), so its
    // ListMeta carries `continue` and `remainingItemCount`.
    list.metadata.continue_token = deleted.continue_token;
    list.metadata.remaining_item_count = deleted.remaining_item_count;
    Ok(respond(StatusCode::OK, &list, &ctx))
}

/// `ValidateListOptions` (apimachinery
/// `pkg/apis/meta/internalversion/validation/validation.go:28-76`) over the
/// query parameters `ListOptions` decodes. The WatchList gate is on by
/// default in 1.35 (kube_features.go:503-509), so `isWatchListFeatureEnabled`
/// is true here, and `SetListOptionsDefaults` (defaults.go:25-38) is applied
/// first: a legacy watch (rv "" or "0") defaults to sendInitialEvents=true
/// with resourceVersionMatch=NotOlderThan.
pub fn validate_list_options(params: &HashMap<String, String>) -> Vec<FieldError> {
    let get = |k: &str| params.get(k).map(String::as_str).unwrap_or("");
    let flag = |v: &str| matches!(v, "true" | "1" | "True" | "TRUE" | "t" | "T");
    let watch = flag(get("watch"));
    let rv = get("resourceVersion");
    let mut matched = get("resourceVersionMatch");
    let mut send_initial = params.get("sendInitialEvents").map(|v| flag(v));
    let cont = get("continue");

    // SetListOptionsDefaults.
    if send_initial.is_none() && matched.is_empty() && watch && (rv.is_empty() || rv == "0") {
        send_initial = Some(true);
        matched = "NotOlderThan";
    }

    let rvm = || Path::new("resourceVersionMatch");
    let mut errs = Vec::new();
    if watch {
        // validateWatchOptions (validation.go:53-76).
        if send_initial.is_some() && matched != "NotOlderThan" {
            errs.push(FieldError::forbidden(
                &rvm(),
                "sendInitialEvents requires setting resourceVersionMatch to NotOlderThan",
            ));
        }
        if !matched.is_empty() {
            if send_initial.is_none() {
                errs.push(FieldError::forbidden(
                    &rvm(),
                    "resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided",
                ));
            }
            if matched != "NotOlderThan" {
                errs.push(FieldError::not_supported(
                    &rvm(),
                    matched.to_string(),
                    &["NotOlderThan"],
                ));
            }
            if !cont.is_empty() {
                errs.push(FieldError::forbidden(
                    &rvm(),
                    "resourceVersionMatch is forbidden when continue is provided",
                ));
            }
        }
        return errs;
    }
    if !matched.is_empty() {
        if rv.is_empty() {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch is forbidden unless resourceVersion is provided",
            ));
        }
        if !cont.is_empty() {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch is forbidden when continue is provided",
            ));
        }
        if matched != "Exact" && matched != "NotOlderThan" {
            errs.push(FieldError::not_supported(
                &rvm(),
                matched.to_string(),
                &["Exact", "NotOlderThan", ""],
            ));
        }
        if matched == "Exact" && rv == "0" {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\"",
            ));
        }
    }
    if send_initial.is_some() {
        errs.push(FieldError::forbidden(
            &Path::new("sendInitialEvents"),
            "sendInitialEvents is forbidden for list",
        ));
    }
    errs
}
