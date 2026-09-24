//! Port of `endpoints/handlers/update.go` (`UpdateResource`, :50-254).

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::validation::metav1::{validate_update_options, UpdateOptions};
use rusternetes_common::{Error, Result};

use super::admission::{Admission, CreateValidation, MutatingAdmission, UpdateValidation};
use super::rest::{
    authorize, check_name, decode, dry_run_param, is_dry_run, respond, RequestScope,
};
use crate::registry::generic;
use crate::registry::rest::{
    ensure_object_namespace_matches_request_namespace, expected_namespace_for_scope,
    DefaultUpdatedObjectInfo, Object, RequestContext, TransformFunc,
};
use crate::state::ApiServerState;

/// PUT to a named object: replace it with the object in `body`.
#[allow(clippy::too_many_arguments)]
pub async fn update_resource<T: Object>(
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
        "update",
        &scope.resource,
        scope.subresource,
        namespace,
        Some(name),
    )
    .await?;

    let options = UpdateOptions {
        field_manager: params.get("fieldManager").cloned(),
        dry_run: dry_run_param(params),
        field_validation: params.get("fieldValidation").cloned(),
    };
    let errs = validate_update_options(&options);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }
    let dry_run = is_dry_run(options.dry_run.as_deref());

    let (mut obj, decode_warnings) = decode(scope, params, body)?;
    let ctx = RequestContext::new(namespace);
    for warning in decode_warnings {
        ctx.add_warning(warning);
    }

    // update.go:141-154.
    ensure_object_namespace_matches_request_namespace(
        expected_namespace_for_scope(namespace, scope.namespace_scoped()),
        obj.metadata_mut(),
    )?;
    check_name(&obj, name, namespace)?;

    let admission = Admission {
        state,
        kind: &scope.kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace,
        user,
        dry_run,
    };

    // update.go:156-230: mutating admission is a transformer, so it sees the
    // live object on every retry; validating admission is the Store's
    // callback, and a create-on-update must also be allowed to `create`.
    let transformers: Vec<Box<dyn TransformFunc<T> + '_>> = vec![Box::new(MutatingAdmission {
        admission: &admission,
        scope,
    })];
    let obj_info = DefaultUpdatedObjectInfo::new(Some(obj), transformers);
    let create_validation = CreateValidation {
        admission: &admission,
        authorize_create: true,
    };
    let update_validation = UpdateValidation {
        admission: &admission,
    };
    let (out, created) = scope
        .store
        .update(
            &ctx,
            name,
            &obj_info,
            Some(&create_validation),
            Some(&update_validation),
            false,
            &generic::UpdateOptions { dry_run },
        )
        .await?;

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok(respond(status, &out, &ctx))
}
