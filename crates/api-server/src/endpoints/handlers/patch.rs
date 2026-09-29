//! Port of `endpoints/handlers/patch.go` (`PatchResource`, :68-257, and the
//! patchers, :293-741).
//!
//! A PATCH is an update whose new object is built from the live one:
//! `patchResource` hands `Store.Update` a `DefaultUpdatedObjectInfo(nil,
//! applyPatch, applyAdmission)`, so the patch is re-applied to the current
//! object on every retry and PUT and PATCH share one validation path.

use std::collections::HashMap;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::validation::field::{Error as FieldError, Path};
use rusternetes_common::validation::metav1::{
    validate_patch_options, PatchOptions, APPLY_YAML_PATCH_TYPE,
};
use rusternetes_common::{Error, Result};

use super::admission::{Admission, CreateValidation, MutatingAdmission, UpdateValidation};
use super::rest::{
    authorize, check_name, dry_run_param, is_dry_run, respond, ApplyFn, RequestScope,
};
use crate::patch::{apply_patch, PatchType};
use crate::registry::generic;
use crate::registry::rest::{
    ensure_object_namespace_matches_request_namespace, expected_namespace_for_scope,
    DefaultUpdatedObjectInfo, Object, RequestContext, TransformFunc,
};
use crate::ssa::{decode_apply_body, ApplyOptions, ApplyOutcome};
use crate::state::ApiServerState;

const JSON_PATCH_TYPE: &str = "application/json-patch+json";
const MERGE_PATCH_TYPE: &str = "application/merge-patch+json";
const STRATEGIC_MERGE_PATCH_TYPE: &str = "application/strategic-merge-patch+json";

/// The patch types a resource endpoint accepts, in upstream's order
/// (endpoints/installer.go:895-900; `apply-patch+cbor` is behind the
/// `CBORServingAndStorage` gate, which is off).
const SUPPORTED_PATCH_TYPES: [&str; 4] = [
    JSON_PATCH_TYPE,
    MERGE_PATCH_TYPE,
    STRATEGIC_MERGE_PATCH_TYPE,
    APPLY_YAML_PATCH_TYPE,
];

/// PATCH to a named object. `content_type` is the request's `Content-Type`.
#[allow(clippy::too_many_arguments)]
pub async fn patch_resource<T: Object>(
    state: &ApiServerState,
    scope: &RequestScope<T>,
    user: &UserInfo,
    namespace: Option<&str>,
    name: &str,
    params: &HashMap<String, String>,
    content_type: &str,
    body: &[u8],
) -> Result<Response> {
    authorize(
        state,
        user,
        "patch",
        &scope.resource,
        scope.subresource,
        namespace,
        Some(name),
    )
    .await?;

    // patch.go:78-89: drop "; charset=...", then require a supported type.
    let content_type = match content_type.find(';') {
        Some(idx) if idx > 0 => &content_type[..idx],
        _ => content_type,
    };
    if !SUPPORTED_PATCH_TYPES.contains(&content_type) {
        // negotiation.NewUnsupportedMediaTypeError (negotiation/errors.go:84-90).
        return Err(Error::UnsupportedMediaType(format!(
            "the body of the request was in an unknown format - accepted media types include: {}",
            SUPPORTED_PATCH_TYPES.join(", ")
        )));
    }

    let options = PatchOptions {
        field_manager: params.get("fieldManager").cloned(),
        force: params
            .get("force")
            .map(|v| rusternetes_common::query::k8s_query_bool(v)),
        dry_run: dry_run_param(params),
        field_validation: params.get("fieldValidation").cloned(),
    };
    let errs = validate_patch_options(&options, content_type);
    if !errs.is_empty() {
        return Err(Error::Invalid(errs));
    }
    let dry_run = is_dry_run(options.dry_run.as_deref());

    // patchResource (patch.go:653-691): pick the mechanism. Apply may create.
    let mechanism = match content_type {
        JSON_PATCH_TYPE => Mechanism::Json(PatchType::JsonPatch),
        MERGE_PATCH_TYPE => Mechanism::Json(PatchType::JsonMergePatch),
        STRATEGIC_MERGE_PATCH_TYPE => Mechanism::Json(PatchType::StrategicMergePatch),
        _ => {
            let apply = scope.apply.ok_or_else(|| {
                Error::Internal(format!(
                    "{}: unimplemented patch type",
                    APPLY_YAML_PATCH_TYPE
                ))
            })?;
            let field_manager = options.field_manager.clone().unwrap_or_default();
            Mechanism::Apply {
                apply,
                options: ApplyOptions::new(field_manager)
                    .with_force(options.force.unwrap_or(false)),
            }
        }
    };
    // A subresource's REST passes `forceAllowCreate = false` to the Store
    // whatever the patch type: "subresources should never allow create on
    // update" (apps/deployment/storage/storage.go:156-160).
    let force_allow_create =
        matches!(mechanism, Mechanism::Apply { .. }) && scope.subresource.is_none();

    let ctx = RequestContext::new(namespace);
    let admission = Admission {
        state,
        kind: &scope.kind,
        resource: &scope.resource,
        subresource: scope.subresource,
        namespace,
        user,
        dry_run,
    };

    let patcher = Patcher {
        scope,
        mechanism,
        name,
        namespace,
        params,
        content_type,
        body,
    };
    let transformers: Vec<Box<dyn TransformFunc<T> + '_>> = vec![
        Box::new(patcher),
        Box::new(MutatingAdmission {
            admission: &admission,
            scope,
        }),
    ];
    let obj_info = DefaultUpdatedObjectInfo::new(None, transformers);
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
            force_allow_create,
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

/// `patchMechanism`: `jsonPatcher` / `smpPatcher` / `applyPatcher`.
enum Mechanism<T> {
    /// JSON patch, JSON merge patch and strategic merge patch.
    Json(PatchType),
    Apply {
        apply: ApplyFn<T>,
        options: ApplyOptions,
    },
}

/// `patcher.applyPatch` (patch.go:581-621) as a `TransformFunc`.
struct Patcher<'a, T: Object> {
    scope: &'a RequestScope<T>,
    mechanism: Mechanism<T>,
    name: &'a str,
    namespace: Option<&'a str>,
    params: &'a HashMap<String, String>,
    content_type: &'a str,
    body: &'a [u8],
}

impl<T: Object> Patcher<'_, T> {
    /// `jsonPatcher.applyPatchToCurrentObject` (patch.go:323-374) and
    /// `smpPatcher.applyPatchToCurrentObject` (:446-468).
    fn patch_current(
        &self,
        ctx: &RequestContext,
        patch_type: &PatchType,
        current: &T,
    ) -> Result<T> {
        let current_json =
            serde_json::to_value(current).map_err(|e| Error::Internal(e.to_string()))?;
        let patch: serde_json::Value = serde_json::from_slice(self.body)
            .map_err(|e| Error::BadRequest(format!("error decoding patch: {e}")))?;
        let patched = apply_patch(&current_json, &patch, patch_type.clone())
            .map_err(|e| Error::InvalidResource(e.to_string()))?;
        let patched_js = serde_json::to_string(&patched).unwrap_or_default();

        // Decode the result strictly or not, per fieldValidation. A strict
        // failure is `Invalid` on the `patch` field (patch.go:338-363).
        let invalid_patch = |msg: String| {
            Error::Invalid(vec![FieldError::invalid(
                &Path::new("patch"),
                patched_js.clone(),
                msg,
            )])
        };
        let obj: T = serde_json::from_value(patched).map_err(|e| invalid_patch(e.to_string()))?;
        match crate::handlers::validation::validate_strict_fields(
            self.params,
            patched_js.as_bytes(),
            &obj,
        ) {
            Ok(warnings) => {
                for warning in warnings {
                    ctx.add_warning(warning);
                }
            }
            Err(Error::BadRequest(msg)) => return Err(invalid_patch(msg)),
            Err(e) => return Err(e),
        }
        Ok(obj)
    }

    /// `applyPatcher.applyPatchToCurrentObject` / `createNewObject`
    /// (patch.go:500-543): `current` is `None` when the object does not exist.
    fn apply(&self, apply: ApplyFn<T>, options: &ApplyOptions, current: Option<&T>) -> Result<T> {
        let desired = decode_apply_body(self.content_type, self.body)
            .map_err(|e| Error::BadRequest(format!("error decoding YAML: {e}")))?;
        match apply(current, &desired, options)
            .map_err(|e| Error::InvalidResource(e.to_string()))?
        {
            ApplyOutcome::Applied { object, .. } => Ok(*object),
            ApplyOutcome::Conflicts(conflicts) => {
                let detail = conflicts
                    .iter()
                    .map(|c| {
                        format!(
                            ".{} is managed by {}",
                            c.path.replace('/', "."),
                            c.current_manager
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(Error::Conflict(format!(
                    "Apply failed with {} conflict{}: {}",
                    conflicts.len(),
                    if conflicts.len() == 1 { "" } else { "s" },
                    detail
                )))
            }
        }
    }
}

#[async_trait]
impl<T: Object> TransformFunc<T> for Patcher<'_, T> {
    async fn transform(&self, ctx: &RequestContext, _new: Option<T>, old: Option<&T>) -> Result<T> {
        let current = old.filter(|o| !o.metadata().uid.is_empty());
        let mut obj = match (&self.mechanism, current) {
            (Mechanism::Apply { apply, options }, current) => {
                self.apply(*apply, options, current)?
            }
            // jsonPatcher / smpPatcher.createNewObject: nothing to patch.
            (Mechanism::Json(_), None) => return Err(self.scope.store.not_found(self.name)),
            (Mechanism::Json(patch_type), Some(current)) => {
                self.patch_current(ctx, patch_type, current)?
            }
        };

        // The patched object is decoded, and so defaulted and converted, as a
        // request body is (patch.go:357-363, :792).
        self.scope.convert(&mut obj);

        let uid = &obj.metadata().uid;
        if !uid.is_empty() && current.is_none() {
            return Err(self.scope.store.conflict(
                self.name,
                format!(
                    "uid mismatch: the provided object specified uid {uid}, and no existing object was found"
                ),
            ));
        }
        ensure_object_namespace_matches_request_namespace(
            expected_namespace_for_scope(self.namespace, self.scope.namespace_scoped()),
            obj.metadata_mut(),
        )?;
        check_name(&obj, self.name, self.namespace)?;
        Ok(obj)
    }
}
