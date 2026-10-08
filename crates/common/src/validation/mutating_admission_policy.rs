//! MutatingAdmissionPolicy / MutatingAdmissionPolicyBinding validation and
//! defaulting — port of upstream
//! `pkg/apis/admissionregistration/validation/validation.go` and
//! `pkg/apis/admissionregistration/v1beta1/{defaults,zz_generated.defaults}.go`
//! (release-1.35).
//!
//! Scope: the CEL-free structural half of `validateMutatingAdmissionPolicySpec`
//! (`:1372`), `validateMutation` (`:1425`),
//! `validateMutatingAdmissionPolicyBindingSpec` (`:1513`) and the
//! "DELETE is not mutable" operation check (`supportedMutatingOperations`,
//! `:521`). Compiling the `applyConfiguration` / `jsonPatch` / variable /
//! matchCondition expressions (`validateApplyConfiguration`, `validateJSONPatch`,
//! `:1458-1498`) needs the mutating CEL environment and is not modelled here;
//! only the `Required` rule on a blank expression that precedes the compile is.

use crate::resources::mutating_admission_policy::{
    MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingAdmissionPolicyBindingSpec,
    MutatingAdmissionPolicySpec, Mutation, PatchType, ReinvocationPolicyType,
};
use crate::resources::validating_admission_policy::{FailurePolicy, MatchResources};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::validating_admission_policy::{
    operation_str, set_defaults_match_resources, validate_match_conditions,
    validate_match_resources, validate_param_kind, validate_param_ref, validate_variable,
};

/// `supportedMutatingOperations.List()` (`validation.go:521-526`): sorted,
/// DELETE is absent because a mutation cannot apply to a delete.
const SUPPORTED_MUTATING_OPERATIONS: [&str; 4] = ["*", "CONNECT", "CREATE", "UPDATE"];

/// `SetObjectDefaults_MutatingAdmissionPolicy`
/// (`v1beta1/zz_generated.defaults.go:73-86`): `SetDefaults_MutatingAdmissionPolicySpec`
/// (`defaults.go:131-137`, `failurePolicy` = `Fail`) then
/// `SetDefaults_MatchResources` and `SetDefaults_Rule` over `matchConstraints`.
///
/// There is no `reinvocationPolicy` default on the policy, so it stays
/// `Required`.
pub fn set_defaults_mutating_admission_policy(policy: &mut MutatingAdmissionPolicy) {
    let Some(spec) = policy.spec.as_mut() else {
        return;
    };
    if spec.failure_policy.is_none() {
        spec.failure_policy = Some(FailurePolicy::Fail);
    }
    if let Some(match_constraints) = spec.match_constraints.as_mut() {
        set_defaults_match_resources(match_constraints);
    }
}

/// `SetObjectDefaults_MutatingAdmissionPolicyBinding`
/// (`zz_generated.defaults.go:88-100`).
pub fn set_defaults_mutating_admission_policy_binding(
    binding: &mut MutatingAdmissionPolicyBinding,
) {
    if let Some(match_resources) = binding
        .spec
        .as_mut()
        .and_then(|spec| spec.match_resources.as_mut())
    {
        set_defaults_match_resources(match_resources);
    }
}

/// `ValidateMutatingAdmissionPolicy` (`validation.go:1362`) minus ObjectMeta,
/// which the strategy validates separately.
pub fn validate_mutating_admission_policy(policy: &MutatingAdmissionPolicy) -> ErrorList {
    let spec = policy.spec.clone().unwrap_or_default();
    validate_mutating_admission_policy_spec(&spec, &Path::new("spec"))
}

/// `ValidateMutatingAdmissionPolicyBinding` (`validation.go:1502`) minus
/// ObjectMeta.
pub fn validate_mutating_admission_policy_binding(
    binding: &MutatingAdmissionPolicyBinding,
) -> ErrorList {
    let spec = binding.spec.clone().unwrap_or_default();
    validate_binding_spec(&spec, &Path::new("spec"))
}

/// `validateMutatingAdmissionPolicySpec` (`validation.go:1372-1423`).
fn validate_mutating_admission_policy_spec(
    spec: &MutatingAdmissionPolicySpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    match &spec.failure_policy {
        None => errs.push(Error::required(&fld_path.child("failurePolicy"), "")),
        Some(FailurePolicy::Unknown(v)) => errs.push(Error::not_supported(
            &fld_path.child("failurePolicy"),
            v.clone(),
            &["Fail", "Ignore"],
        )),
        Some(_) => {}
    }

    if let Some(param_kind) = &spec.param_kind {
        errs.extend(validate_param_kind(
            param_kind,
            &fld_path.child("paramKind"),
        ));
    }

    match &spec.match_constraints {
        None => errs.push(Error::required(&fld_path.child("matchConstraints"), "")),
        Some(match_constraints) => {
            let path = fld_path.child("matchConstraints");
            errs.extend(validate_match_resources(match_constraints, &path));
            // At least one resourceRule must be defined to provide type
            // information (`:1391-1393`).
            if match_constraints
                .resource_rules
                .as_ref()
                .is_none_or(Vec::is_empty)
            {
                errs.push(Error::required(&path.child("resourceRules"), ""));
            }
            errs.extend(validate_mutable_operations(
                match_constraints,
                &path.child("resourceRules").child("operations"),
            ));
        }
    }

    errs.extend(validate_match_conditions(
        spec.match_conditions.as_deref().unwrap_or_default(),
        &fld_path.child("matchConditions"),
    ));

    for (i, variable) in spec
        .variables
        .as_deref()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        errs.extend(validate_variable(
            variable,
            &fld_path.child("variables").index(i),
        ));
    }

    let mutations = spec.mutations.as_deref().unwrap_or_default();
    if mutations.is_empty() {
        errs.push(Error::required(
            &fld_path.child("mutations"),
            "mutations must contain at least one item",
        ));
    } else {
        for (i, mutation) in mutations.iter().enumerate() {
            errs.extend(validate_mutation(
                mutation,
                &fld_path.child("mutations").index(i),
            ));
        }
    }

    let policy_path = fld_path.child("reinvocationPolicy");
    match &spec.reinvocation_policy {
        None => errs.push(Error::required(&policy_path, "")),
        Some(ReinvocationPolicyType::Unknown(v)) => errs.push(Error::not_supported(
            &policy_path,
            v.clone(),
            &["Never", "IfNeeded"],
        )),
        Some(_) => {}
    }
    errs
}

/// "It is only possible to mutate create and update requests"
/// (`validation.go:1395-1403`, `:1526-1534`): every operation of every
/// `resourceRules` entry must be in `supportedMutatingOperations`. The error
/// path carries no index, as upstream's does not.
fn validate_mutable_operations(match_resources: &MatchResources, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for rule in match_resources
        .resource_rules
        .as_deref()
        .unwrap_or_default()
    {
        for op in rule.rule.operations.as_deref().unwrap_or_default() {
            let op = operation_str(op);
            if !SUPPORTED_MUTATING_OPERATIONS.contains(&op.as_str()) {
                errs.push(Error::not_supported(
                    fld_path,
                    op,
                    &SUPPORTED_MUTATING_OPERATIONS,
                ));
            }
        }
    }
    errs
}

/// `validateMutation` (`validation.go:1425-1456`): the `patchType` union.
fn validate_mutation(mutation: &Mutation, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let patch_type_path = fld_path.child("patchType");
    let apply_path = fld_path.child("applyConfiguration");
    let json_path = fld_path.child("jsonPatch");

    match &mutation.patch_type {
        None => errs.push(Error::required(&patch_type_path, "")),
        Some(PatchType::JsonPatch) => {
            match &mutation.json_patch {
                None => errs.push(Error::required(
                    &json_path,
                    "must be specified when patchType is JSONPatch",
                )),
                Some(patch) => errs.extend(validate_patch_expression(
                    &patch.expression,
                    &json_path.child("expression"),
                )),
            }
            if mutation.apply_configuration.is_some() {
                errs.push(Error::invalid(
                    &apply_path,
                    "{applyConfiguration}".to_string(),
                    "must not be specified when patchType is JSONPatch",
                ));
            }
        }
        Some(PatchType::ApplyConfiguration) => {
            match &mutation.apply_configuration {
                None => errs.push(Error::required(
                    &apply_path,
                    "must be specified when patchType is ApplyConfiguration",
                )),
                Some(config) => errs.extend(validate_patch_expression(
                    &config.expression,
                    &apply_path.child("expression"),
                )),
            }
            if mutation.json_patch.is_some() {
                errs.push(Error::invalid(
                    &json_path,
                    "{jsonPatch}".to_string(),
                    "must not be specified when patchType is ApplyConfiguration",
                ));
            }
        }
        Some(PatchType::Unknown(v)) => errs.push(Error::not_supported(
            &patch_type_path,
            v.clone(),
            &["ApplyConfiguration", "JSONPatch"],
        )),
    }
    errs
}

/// The part of `validateApplyConfiguration` / `validateJSONPatch`
/// (`:1458-1498`) before the CEL compile: a blank expression is `Required`.
fn validate_patch_expression(expression: &str, fld_path: &Path) -> ErrorList {
    if expression.trim().is_empty() {
        vec![Error::required(fld_path, "")]
    } else {
        Vec::new()
    }
}

/// `validateMutatingAdmissionPolicyBindingSpec` (`validation.go:1513-1537`).
fn validate_binding_spec(spec: &MutatingAdmissionPolicyBindingSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let policy_name = spec.policy_name.clone().unwrap_or_default();
    let policy_path = fld_path.child("policyName");
    if policy_name.is_empty() {
        errs.push(Error::required(&policy_path, ""));
    } else {
        for msg in is_dns1123_subdomain(&policy_name) {
            errs.push(Error::invalid(&policy_path, policy_name.clone(), msg));
        }
    }

    if let Some(param_ref) = &spec.param_ref {
        errs.extend(validate_param_ref(param_ref, &fld_path.child("paramRef")));
    }
    if let Some(match_resources) = &spec.match_resources {
        let path = fld_path.child("matchResources");
        errs.extend(validate_match_resources(match_resources, &path));
        errs.extend(validate_mutable_operations(
            match_resources,
            &path.child("resourceRules").child("operations"),
        ));
    }
    errs
}
