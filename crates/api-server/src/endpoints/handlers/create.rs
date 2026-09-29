//! Port of `endpoints/handlers/create.go` (`createHandler`, :53-238).

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::admission::Operation;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::validation::metav1::{validate_create_options, CreateOptions};
use rusternetes_common::{Error, Result};

use super::admission::{Admission, CreateValidation};
use super::rest::{authorize, decode, dry_run_param, is_dry_run, respond, RequestScope};
use crate::registry::generic;
use crate::registry::rest::{
    ensure_object_namespace_matches_request_namespace, expected_namespace_for_scope,
    wipe_object_meta_system_fields, Object, RequestContext,
};
use crate::state::ApiServerState;

/// POST to a collection: create the object in `body`.
pub async fn create_resource<T: Object>(
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
        "create",
        &scope.resource,
        scope.subresource,
        namespace,
        None,
    )
    .await?;

    let options = CreateOptions {
        field_manager: params.get("fieldManager").cloned(),
        dry_run: dry_run_param(params),
        field_validation: params.get("fieldValidation").cloned(),
    };
    let errs = validate_create_options(&options);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }
    let dry_run = is_dry_run(options.dry_run.as_deref());

    let (mut obj, decode_warnings) = decode(scope, params, body)?;
    let ctx = RequestContext::new(namespace);
    for warning in decode_warnings {
        ctx.add_warning(warning);
    }

    // create.go:165-179.
    wipe_object_meta_system_fields(obj.metadata_mut());
    ensure_object_namespace_matches_request_namespace(
        expected_namespace_for_scope(namespace, scope.namespace_scoped()),
        obj.metadata_mut(),
    )?;

    let admission = Admission {
        state,
        kind: &scope.kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace,
        user,
        dry_run,
    };

    // Mutating admission, then the Store's create with validating admission
    // as its callback (create.go:183-209).
    let mut obj = admission.admit(Operation::Create, obj, None).await?;
    // The dispatcher decodes a webhook's patched object like a request body.
    scope.convert(&mut obj);
    let validation = CreateValidation {
        admission: &admission,
        authorize_create: false,
    };
    let out = scope
        .store
        .create(
            &ctx,
            obj,
            Some(&validation),
            &generic::CreateOptions { dry_run },
        )
        .await?;

    Ok(respond(StatusCode::CREATED, &out, &ctx))
}
