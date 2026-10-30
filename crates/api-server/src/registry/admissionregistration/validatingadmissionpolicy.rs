//! ValidatingAdmissionPolicy strategy and storage — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicy/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
use rusternetes_common::cel_versions::{self, EnvSet, EnvType};

use rusternetes_common::resources::validating_admission_policy::ValidatingAdmissionPolicyStatus;
use rusternetes_common::resources::ValidatingAdmissionPolicy;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::validating_admission_policy::{
    find_validating_policy_preexisting_expressions,
    ignore_validating_admission_policy_match_conditions, set_defaults_validating_admission_policy,
    validate_validating_admission_policy, validate_validating_admission_policy_status_update,
    PreexistingExpressions, MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH,
};
use rusternetes_storage::StorageBackend;

use super::authz::{authorize_param_kind, ParamAuthz};
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
};

/// `SetObjectDefaults_ValidatingAdmissionPolicy`
/// (`pkg/apis/admissionregistration/v1/zz_generated.defaults.go`).
pub fn convert_to_internal(policy: &mut ValidatingAdmissionPolicy) {
    set_defaults_validating_admission_policy(policy);
}

/// A Go `ValidatingAdmissionPolicyStatus{}`: the status is not a pointer.
fn empty_status() -> ValidatingAdmissionPolicyStatus {
    ValidatingAdmissionPolicyStatus {
        observed_generation: None,
        type_checking: None,
        conditions: None,
    }
}

fn spec_json(obj: &ValidatingAdmissionPolicy) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// The CEL compile of `validateValidatingAdmissionPolicySpec`
/// (validation.go:803-828) that needs the composited compiler, in upstream's
/// order: `validateVariable` (:999-1032), then per validation
/// `validateValidation` (:1034-1061: `validateValidationExpression` and
/// `validateMessageExpression`) and `validateAuditAnnotation` (:1128-1166).
///
/// `getCompiler` (:773-779) builds the composited compiler only when the
/// policy has `variables` and the stateless one otherwise (`createCompiler`,
/// :1316-1327). `CompileAndStoreVariable` (composition.go:95-100) adds each
/// variable as a field of `variables` only after compiling it, so a variable
/// sees just the ones before it; validations and audit annotations (compiled
/// after the whole variables loop) see all of them. `HasParams` is
/// `paramKind != nil` (`opts.allowParamsInMatchConditions` for
/// `messageExpression`, set at :790-792); `HasAuthorizer` is true except for
/// `messageExpression` (:1124) (`compilePolicy`, validating/plugin.go:118-119).
/// Output types: `Variable` any (interface.go:74-76), `ValidationCondition`
/// bool (:46-48), `MessageExpressionCondition` string (message.go:34-36),
/// `AuditAnnotationCondition` string or null (:60-62); inference-dependent
/// checks are #3277. Each expression is also compiled in the CEL library
/// environment of its `envType` (`StoredExpressions` when the old policy's
/// `preexistingExpressions` hold it, else `NewExpressions`; `:1012`, `:1088`,
/// `:1114`, `:1143`), keyed by the untrimmed expression. Upstream replicated:
/// `validateVariable` keys on `validationExpressions` (:1012), there being no
/// set of variable expressions.
fn message_expression_errors(
    obj: &ValidatingAdmissionPolicy,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    #[allow(clippy::too_many_arguments)]
    fn push(
        errs: &mut ErrorList,
        path: Path,
        value: &str,
        compiled: &str,
        env: &MutationEnv,
        expected: ExpectedOutput,
        stored: &std::collections::HashSet<String>,
        env_set: &EnvSet,
    ) {
        if let Some(detail) = mutation_env_failure(compiled, env, expected).or_else(|| {
            cel_versions::compile_failure(compiled, env_set, env_type_of(stored, value))
        }) {
            errs.push(FieldError::invalid(&path, value.to_string(), detail));
        }
    }
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    let has_params = spec.param_kind.is_some();
    let has_variables = spec.variables.as_ref().is_some_and(|v| !v.is_empty());
    let all_names: std::collections::BTreeSet<String> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| v.name.clone())
        .filter(|n| !n.trim().is_empty())
        .collect();
    // validateVariable (:999-1032).
    let mut seen = std::collections::BTreeSet::new();
    for (i, v) in spec.variables.iter().flatten().enumerate() {
        if !v.expression.trim().is_empty() {
            let env = MutationEnv {
                has_params,
                has_authorizer: true,
                has_patch_types: false,
                variables: Some(seen.clone()),
            };
            push(
                &mut errs,
                Path::new("spec")
                    .child("variables")
                    .index(i)
                    .child("expression"),
                &v.expression,
                &v.expression,
                &env,
                ExpectedOutput::Any,
                &preexisting.validation_expressions,
                env_set,
            );
        }
        if !v.name.trim().is_empty() {
            seen.insert(v.name.clone());
        }
    }
    let env_for = |has_authorizer: bool| MutationEnv {
        has_params,
        has_authorizer,
        has_patch_types: false,
        variables: has_variables.then(|| all_names.clone()),
    };
    // validateValidation (:1034-1061).
    for (i, v) in spec.validations.iter().flatten().enumerate() {
        let base = Path::new("spec").child("validations").index(i);
        if !v.expression.trim().is_empty() {
            push(
                &mut errs,
                base.child("expression"),
                &v.expression,
                &v.expression,
                &env_for(true),
                ExpectedOutput::Bool,
                &preexisting.validation_expressions,
                env_set,
            );
        }
        if let Some(expression) = v.message_expression.as_deref() {
            if !expression.trim().is_empty() {
                push(
                    &mut errs,
                    base.child("messageExpression"),
                    expression,
                    expression,
                    &env_for(false),
                    ExpectedOutput::String,
                    &preexisting.validation_message_expressions,
                    env_set,
                );
            }
        }
    }
    // validateAuditAnnotation (:1128-1166): a blank or over-long expression is
    // reported by the structural validation instead.
    for (i, a) in spec.audit_annotations.iter().flatten().enumerate() {
        let trimmed = a.value_expression.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH {
            continue;
        }
        push(
            &mut errs,
            Path::new("spec")
                .child("auditAnnotations")
                .index(i)
                .child("valueExpression"),
            &a.value_expression,
            trimmed,
            &env_for(true),
            ExpectedOutput::StringOrNull,
            &preexisting.audit_annotation_values_expressions,
            env_set,
        );
    }
    errs
}

/// The `envType` selection every `validate*Expression` makes: the expression
/// is `StoredExpressions` when the old policy's `preexistingExpressions` hold
/// it, else `NewExpressions` (validation.go:1012-1015, :1088-1092, :1100-1104,
/// :1114-1118, :1143-1146).
fn env_type_of(stored: &std::collections::HashSet<String>, expression: &str) -> EnvType {
    if stored.contains(expression) {
        EnvType::StoredExpressions
    } else {
        EnvType::NewExpressions
    }
}

/// `validateValidatingAdmissionPolicy` (validation.go:762-770): ObjectMeta
/// with `NameIsDNSSubdomain`, then the spec.
pub fn validate(
    obj: &ValidatingAdmissionPolicy,
    old: Option<&ValidatingAdmissionPolicy>,
) -> ErrorList {
    let preexisting = old
        .map(find_validating_policy_preexisting_expressions)
        .unwrap_or_default();
    validate_in(
        obj,
        &preexisting,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

pub fn validate_in(
    obj: &ValidatingAdmissionPolicy,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_validating_admission_policy(obj));
    errs.extend(message_expression_errors(obj, preexisting, env_set));
    errs
}

/// The CEL compile of `spec.matchConditions` in
/// `validateValidatingAdmissionPolicySpec` (validation.go:800-802): each
/// non-blank expression goes through `validateMatchConditionsExpression`
/// (:1100-1112) and `convertCELErrorToValidationError` (:1073-1085) unless
/// `ignore_match_conditions` (`ignoreValidatingAdmissionPolicyMatchConditions`,
/// :619-628). The stateless compiler has `params` only with a `paramKind`
/// (`allowParamsInMatchConditions`, :788) and no `variables`; the output must
/// be `bool` (`MatchCondition.ReturnTypes`, matcher.go:49-51;
/// compile.go:189-203). Inference-dependent output types are #3277.
fn compile_errors(
    obj: &ValidatingAdmissionPolicy,
    ignore_match_conditions: bool,
    old: Option<&ValidatingAdmissionPolicy>,
) -> ErrorList {
    compile_errors_in(
        obj,
        ignore_match_conditions,
        &old.map(find_validating_policy_preexisting_expressions)
            .unwrap_or_default(),
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

pub fn compile_errors_in(
    obj: &ValidatingAdmissionPolicy,
    ignore_match_conditions: bool,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    if ignore_match_conditions {
        return errs;
    }
    let env = MutationEnv {
        has_params: spec.param_kind.is_some(),
        has_patch_types: false,
        has_authorizer: true,
        variables: None,
    };
    for (i, c) in spec.match_conditions.iter().flatten().enumerate() {
        let expression = c.expression.trim();
        if expression.is_empty() {
            continue;
        }
        // `validateMatchConditionsExpression` (:1100-1104), keyed by the trimmed
        // expression `validateMatchCondition` passes (:988-991).
        let env_type = env_type_of(&preexisting.match_condition_expressions, expression);
        if let Some(detail) = mutation_env_failure(expression, &env, ExpectedOutput::Bool)
            .or_else(|| cel_versions::compile_failure(expression, env_set, env_type))
        {
            let path = Path::new("spec")
                .child("matchConditions")
                .index(i)
                .child("expression");
            errs.push(FieldError::invalid(&path, expression.to_string(), detail));
        }
    }
    errs
}

/// `validatingAdmissionPolicyStrategy` (strategy.go:36-45).
#[derive(Default)]
pub struct Strategy {
    /// `authorizer` / `resourceResolver`; `None` skips the `paramKind` check.
    pub authz: Option<ParamAuthz>,
}

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ValidatingAdmissionPolicy> for Strategy {
    /// `PrepareForCreate` (strategy.go:56-60): clears the status and starts
    /// the generation at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ValidatingAdmissionPolicy) {
        obj.status = Some(empty_status());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ValidatingAdmissionPolicy) -> ErrorList {
        let mut errs = validate(obj, None);
        errs.extend(compile_errors(obj, false, None));
        errs
    }

    /// The `paramKind` authorization of `Validate` (strategy.go:83-92), run
    /// only when the rest of `Validate` found nothing (`len(errs) == 0`).
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestCreateStrategy::validate(self, ctx, obj).is_empty() {
                return Vec::new();
            }
            authorize_param_kind(ctx, authz.authorizer.as_ref(), &authz.storage, obj, None).await
        })
    }
}

impl RestUpdateStrategy<ValidatingAdmissionPolicy> for Strategy {
    /// `GetResetFields` (pkg/registry/admissionregistration/validatingadmissionpolicy/strategy.go:133): status.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with("admissionregistration.k8s.io/v1alpha1", &[&["status"]])
            .with("admissionregistration.k8s.io/v1beta1", &[&["status"]])
            .with("admissionregistration.k8s.io/v1", &[&["status"]])
    }

    /// strategy.go:91-93.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:63-78): the status is not writable
    /// here, and a spec change increments the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) {
        obj.status = old.status.clone();
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateValidatingAdmissionPolicyUpdate` (validation.go:1239-1244).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) -> ErrorList {
        let mut errs = validate(obj, Some(old));
        errs.extend(compile_errors(
            obj,
            ignore_validating_admission_policy_match_conditions(obj, old),
            Some(old),
        ));
        errs
    }

    /// The `paramKind` authorization of `ValidateUpdate` (strategy.go:109-119).
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicy,
        old: &'a ValidatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestUpdateStrategy::validate_update(self, ctx, obj, old).is_empty() {
                return Vec::new();
            }
            authorize_param_kind(
                ctx,
                authz.authorizer.as_ref(),
                &authz.storage,
                obj,
                Some(old),
            )
            .await
        })
    }

    /// strategy.go:111-113.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ValidatingAdmissionPolicy> for Strategy {}

/// `validatingAdmissionPolicyStatusStrategy` (strategy.go:147-180).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<ValidatingAdmissionPolicy> for StatusStrategy {
    /// `GetResetFields` (pkg/registry/admissionregistration/validatingadmissionpolicy/strategy.go:173): spec, metadata.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with(
                "admissionregistration.k8s.io/v1alpha1",
                &[&["spec"], &["metadata"]],
            )
            .with(
                "admissionregistration.k8s.io/v1beta1",
                &[&["spec"], &["metadata"]],
            )
            .with(
                "admissionregistration.k8s.io/v1",
                &[&["spec"], &["metadata"]],
            )
    }

    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Setting the spec is not allowed, setting the status is; the generation
    /// does not change (strategy.go:164-175).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `ValidateValidatingAdmissionPolicyStatusUpdate` (validation.go:1247).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ValidatingAdmissionPolicy,
        _old: &ValidatingAdmissionPolicy,
    ) -> ErrorList {
        validate_validating_admission_policy_status_update(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewREST` (storage/storage.go:49-80): the main store and the status store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> (
    Store<ValidatingAdmissionPolicy, StorageBackend>,
    Store<ValidatingAdmissionPolicy, StorageBackend>,
) {
    let store = Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "validatingadmissionpolicies",
        ),
        Arc::new(Strategy {
            authz: Some(ParamAuthz {
                authorizer,
                storage,
            }),
        }),
    )
    .with_decode_defaulter(convert_to_internal);
    // The status strategy has no authorization (strategy.go:147-180).
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
