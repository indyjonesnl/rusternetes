//! MutatingAdmissionPolicy strategy and storage — port of
//! `pkg/registry/admissionregistration/mutatingadmissionpolicy/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.
//!
//! Unlike its validating sibling the policy has no `status` (the type has
//! none, `v1beta1/types.go:1202`), so there is no status strategy or
//! subresource, and `PrepareForCreate` only starts the generation.

use std::collections::BTreeSet;
use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
use rusternetes_common::resources::mutating_admission_policy::PatchType;
use rusternetes_common::resources::MutatingAdmissionPolicy;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::mutating_admission_policy::{
    find_mutating_policy_preexisting_expressions,
    ignore_mutating_admission_policy_match_conditions, set_defaults_mutating_admission_policy,
    validate_mutating_admission_policy, PreexistingExpressions,
};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use super::authz::{authorize_mutating_param_kind, ParamAuthz};
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy, ValidateFuture,
};

/// `SetObjectDefaults_MutatingAdmissionPolicy`
/// (`pkg/apis/admissionregistration/v1beta1/zz_generated.defaults.go`).
pub fn convert_to_internal(policy: &mut MutatingAdmissionPolicy) {
    set_defaults_mutating_admission_policy(policy);
}

fn spec_json(obj: &MutatingAdmissionPolicy) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `validateMutatingAdmissionPolicy` (validation.go:1366-1370): ObjectMeta with
/// `NameIsDNSSubdomain`, then the spec.
pub fn validate(obj: &MutatingAdmissionPolicy) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_mutating_admission_policy(obj));
    errs
}

/// One CEL compile, as `convertCELErrorToValidationError`
/// (validation.go:1073-1085) reports it: `field.Invalid` carrying the
/// expression, under `fld_path`.
///
/// `env_type` is `StoredExpressions` for an expression the old policy already
/// stores, else `NewExpressions` (the `envType` selection of `validateMatchConditionsExpression`,
/// `validateApplyConfiguration`, `validateJSONPatch`, `validateVariable`,
/// validation.go:999-1032, :1100-1112, :1455-1498).
fn compile_error(
    expression: &str,
    env: &MutationEnv,
    expected: ExpectedOutput,
    env_set: &EnvSet,
    env_type: EnvType,
    fld_path: &Path,
) -> Option<FieldError> {
    // The checker's declarations first, then the library versions of the
    // environment the expression compiles in (`CompileCELExpression` in
    // `envType`, compile.go:166-229).
    mutation_env_failure(expression, env, expected)
        .or_else(|| cel_versions::compile_failure(expression, env_set, env_type))
        .map(|detail| FieldError::invalid(fld_path, expression.to_string(), detail))
}

/// The CEL compile half of `validateMutatingAdmissionPolicySpec`
/// (validation.go:1372-1423): `matchConditions` unless `ignore_match_conditions`
/// (`validateMatchCondition`, :984-997), each `variables[i].expression`
/// (`validateVariable`, :999-1032) and each mutation's `applyConfiguration` /
/// `jsonPatch` expression (`validateApplyConfiguration`, `validateJSONPatch`,
/// :1458-1498). A blank expression is `Required`, already reported by the
/// shape rules, and is not compiled.
///
/// The environments are the three upstream builds, see
/// [`rusternetes_common::cel_env`] for what of the cel-go checker the `cel`
/// crate lets this port (declarations, `variables` fields, `Object` /
/// `JSONPatch` type names, the output type of the unambiguous shapes) and what
/// it does not (type inference). `environment.StoredExpressions`, which
/// `preexistingExpressions` selects, gates library versions
/// ([`rusternetes_common::cel_versions`]): see [`compile_error`].
fn compile_errors(
    obj: &MutatingAdmissionPolicy,
    ignore_match_conditions: bool,
    old: Option<&MutatingAdmissionPolicy>,
) -> ErrorList {
    compile_errors_in(
        obj,
        ignore_match_conditions,
        &old.map(find_mutating_policy_preexisting_expressions)
            .unwrap_or_default(),
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

/// [`compile_errors`] against explicit preexisting expressions and environment
/// set (`MustBaseEnvSet(DefaultCompatibilityVersion())`, validation.go:95).
pub fn compile_errors_in(
    obj: &MutatingAdmissionPolicy,
    ignore_match_conditions: bool,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    let spec_path = Path::new("spec");
    let has_params = spec.param_kind.is_some();
    if !ignore_match_conditions {
        // `validateMatchConditionsExpression` (:1100-1112): the stateless
        // compiler, so no `variables` and no patch types.
        let env = MutationEnv {
            has_params,
            has_authorizer: true,
            has_patch_types: false,
            variables: None,
        };
        for (i, c) in spec.match_conditions.iter().flatten().enumerate() {
            let expression = c.expression.trim();
            if !expression.is_empty() {
                let path = spec_path
                    .child("matchConditions")
                    .index(i)
                    .child("expression");
                let env_type = if preexisting.match_condition_expressions.contains(expression) {
                    EnvType::StoredExpressions
                } else {
                    EnvType::NewExpressions
                };
                errs.extend(compile_error(
                    expression,
                    &env,
                    ExpectedOutput::Bool,
                    env_set,
                    env_type,
                    &path,
                ));
            }
        }
    }
    // The composited compiler stores every variable (`CompileAndStoreVariable`,
    // composition.go:95-100, adds the field even when it fails to compile).
    let variables: BTreeSet<String> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| v.name.clone())
        .filter(|n| !n.trim().is_empty())
        .collect();
    let variable_env = MutationEnv {
        has_params,
        has_authorizer: true,
        has_patch_types: false,
        variables: Some(variables),
    };
    for (i, v) in spec.variables.iter().flatten().enumerate() {
        if !v.expression.trim().is_empty() {
            let path = spec_path.child("variables").index(i).child("expression");
            errs.extend(compile_error(
                &v.expression,
                &variable_env,
                ExpectedOutput::Any,
                env_set,
                // `findMutatingPolicyPreexistingExpressions` never fills
                // `validationExpressions` (validation.go:326-340), which is
                // the set `validateVariable` consults (:1012-1015), so a
                // variable always compiles as a new expression.
                EnvType::NewExpressions,
                &path,
            ));
        }
    }
    let mutation_env = MutationEnv {
        has_patch_types: true,
        ..variable_env
    };
    for (i, m) in spec.mutations.iter().flatten().enumerate() {
        let path = spec_path.child("mutations").index(i);
        // `validateMutation` (:1425-1456) compiles only the expression that
        // `patchType` selects.
        let (expression, child, expected) = match m.patch_type {
            Some(PatchType::JsonPatch) => (
                m.json_patch.as_ref().map(|p| &p.expression),
                "jsonPatch",
                ExpectedOutput::JsonPatchList,
            ),
            Some(PatchType::ApplyConfiguration) => (
                m.apply_configuration.as_ref().map(|a| &a.expression),
                "applyConfiguration",
                ExpectedOutput::Object,
            ),
            _ => (None, "", ExpectedOutput::Any),
        };
        if let Some(expression) = expression {
            let trimmed = expression.trim();
            if !trimmed.is_empty() {
                // Upstream replicated: `validateJSONPatch` consults
                // `applyConfigurationExpressions` (validation.go:1485), not
                // `jsonPatchExpressions`, so a stored jsonPatch is only
                // `StoredExpressions` when it equals a stored applyConfiguration.
                let env_type = if preexisting
                    .apply_configuration_expressions
                    .contains(expression)
                {
                    EnvType::StoredExpressions
                } else {
                    EnvType::NewExpressions
                };
                errs.extend(compile_error(
                    trimmed,
                    &mutation_env,
                    expected,
                    env_set,
                    env_type,
                    &path.child(child).child("expression"),
                ));
            }
        }
    }
    errs
}

/// `mutatingAdmissionPolicyStrategy` (strategy.go:34-39).
#[derive(Default)]
pub struct Strategy {
    /// `authorizer` / `resourceResolver`; `None` skips the authorization.
    pub authz: Option<ParamAuthz>,
}

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:52-54.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<MutatingAdmissionPolicy> for Strategy {
    /// `PrepareForCreate` (strategy.go:57-60): the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut MutatingAdmissionPolicy) {
        obj.metadata.generation = Some(1);
    }

    /// `Validate` (strategy.go:76-84) minus the `paramKind` authorization,
    /// which is `validate_async`.
    fn validate(&self, _ctx: &RequestContext, obj: &MutatingAdmissionPolicy) -> ErrorList {
        let mut errs = validate(obj);
        errs.extend(compile_errors(obj, false, None));
        errs
    }

    /// The `paramKind` authorization of `Validate`, run only when the rest of
    /// `Validate` found nothing (`len(errs) == 0`).
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a MutatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestCreateStrategy::validate(self, ctx, obj).is_empty() {
                return Vec::new();
            }
            authorize_mutating_param_kind(ctx, authz.authorizer.as_ref(), &authz.storage, obj, None)
                .await
        })
    }
}

impl RestUpdateStrategy<MutatingAdmissionPolicy> for Strategy {
    /// strategy.go:97-99.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:63-73): a spec change increments the
    /// generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicy,
        old: &MutatingAdmissionPolicy,
    ) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateMutatingAdmissionPolicyUpdate` (validation.go:1349-1354):
    /// unchanged `matchConditions` / `paramKind` are not compiled again
    /// (`ignoreMutatingAdmissionPolicyMatchConditions`, :630-638).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &MutatingAdmissionPolicy,
        old: &MutatingAdmissionPolicy,
    ) -> ErrorList {
        let mut errs = validate(obj);
        errs.extend(compile_errors(
            obj,
            ignore_mutating_admission_policy_match_conditions(obj, old),
            Some(old),
        ));
        errs
    }

    /// The `paramKind` authorization of `ValidateUpdate`.
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a MutatingAdmissionPolicy,
        old: &'a MutatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestUpdateStrategy::validate_update(self, ctx, obj, old).is_empty() {
                return Vec::new();
            }
            authorize_mutating_param_kind(
                ctx,
                authz.authorizer.as_ref(),
                &authz.storage,
                obj,
                Some(old),
            )
            .await
        })
    }

    /// strategy.go:120-122.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<MutatingAdmissionPolicy> for Strategy {}

/// `NewREST` (storage/storage.go:38-62). `Categories()` (`api-extensions`,
/// :65-68) is served by discovery.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<MutatingAdmissionPolicy, StorageBackend> {
    Store::new(
        storage.clone(),
        GroupResource::new("admissionregistration.k8s.io", "mutatingadmissionpolicies"),
        Arc::new(Strategy {
            authz: Some(ParamAuthz {
                authorizer,
                storage,
            }),
        }),
    )
    .with_decode_defaulter(convert_to_internal)
}
