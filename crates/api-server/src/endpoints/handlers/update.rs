//! Port of `endpoints/handlers/update.go` (`UpdateResource`, :50-254).

use std::collections::HashMap;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::validation::metav1::{validate_update_options, UpdateOptions};
use rusternetes_common::{Error, Result};

use super::admission::{Admission, CreateValidation, MutatingAdmission, UpdateValidation};
use super::rest::{
    authorize, check_name, decode, dedup_owner_references_and_add_warning, dry_run_param,
    is_dry_run, respond_object, retry_without_managed_fields_if_too_large, RequestScope,
};
use crate::fieldmanager::manager_or_user_agent;
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
    let ctx = RequestContext::new(namespace)
        .with_group_version(&scope.kind.group, &scope.kind.version)
        .with_user(user)
        .with_name(name);
    for warning in decode_warnings {
        ctx.add_warning(warning);
    }

    // update.go:141-154.
    ensure_object_namespace_matches_request_namespace(
        expected_namespace_for_scope(namespace, scope.namespace_scoped()),
        obj.metadata_mut(),
    )?;
    check_name(&obj, name, namespace)?;

    // update.go:225-226: dedup owner references before the transformers; the
    // post-admission dedup (:185-189) is in `MutatingAdmission`.
    dedup_owner_references_and_add_warning(&mut obj, &ctx, false);

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
    let create_validation = CreateValidation {
        admission: &admission,
        authorize_create: true,
    };
    let update_validation = UpdateValidation {
        admission: &admission,
    };
    // update.go:226-240: a write refused for its size is retried once with
    // `shouldUpdateManagedFields = false` and the object's managedFields
    // cleared.
    let (out, created) = retry_without_managed_fields_if_too_large(|strip| {
        let mut attempt = obj.clone();
        if strip {
            attempt.metadata_mut().managed_fields = None;
        }
        let (ctx, admission, create_validation, update_validation) =
            (&ctx, &admission, &create_validation, &update_validation);
        let manager = manager_or_user_agent(options.field_manager.as_deref());
        async move {
            // update.go:160-167: the managedFields transformer comes first.
            let transformers: Vec<Box<dyn TransformFunc<T> + '_>> = vec![
                Box::new(UpdateManagedFields {
                    scope,
                    manager,
                    enabled: !strip,
                }),
                Box::new(MutatingAdmission { admission, scope }),
            ];
            let obj_info = DefaultUpdatedObjectInfo::new(Some(attempt), transformers);
            scope
                .store
                .update(
                    ctx,
                    name,
                    &obj_info,
                    Some(create_validation),
                    Some(update_validation),
                    false,
                    &generic::UpdateOptions { dry_run },
                )
                .await
        }
    })
    .await?;

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok(respond_object(scope, status, &out, &ctx))
}

/// The managedFields transformer of `UpdateResource` (update.go:162-167):
/// `scope.FieldManager.UpdateNoErrors(liveObj, newObj, managerOrUserAgent(...))`.
struct UpdateManagedFields<'a, T: Object> {
    scope: &'a RequestScope<T>,
    manager: String,
    /// `shouldUpdateManagedFields` (update.go:157).
    enabled: bool,
}

#[async_trait]
impl<T: Object> TransformFunc<T> for UpdateManagedFields<'_, T> {
    async fn transform(&self, _ctx: &RequestContext, new: Option<T>, old: Option<&T>) -> Result<T> {
        let new = new.ok_or_else(|| Error::Internal("no object to update".to_string()))?;
        if !self.enabled {
            return Ok(new);
        }
        // A create-on-update has a zero live object (no uid).
        let live = old.filter(|o| !o.metadata().uid.is_empty());
        Ok(self
            .scope
            .field_manager()
            .update_no_errors(live, new, &self.manager))
    }
}
