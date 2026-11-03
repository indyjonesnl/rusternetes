//! Shadow type check of the CEL expressions of a MutatingAdmissionPolicy
//! (`RUSTERNETES_CEL_CHECKER=shadow`, default off).
//!
//! The environment is the one the admission compiler builds, release-1.35:
//! `compilePolicy` (`plugin/policy/mutating/compilation.go:35-81`) compiles
//! `variables` with the plain options, `matchConditions` with the plain
//! options too and every patch expression with `HasPatchTypes`; the
//! environments are `createEnvForOpts` (`plugin/cel/compile.go:225-300`):
//! `object` / `oldObject` / `params` are `dyn` (there is no schema typing for
//! a MutatingAdmissionPolicy: unlike `validating/typechecking.go` there is
//! no `mutating` type checker), `request` and `namespaceObject` are the
//! hand-built types, `authorizer` and `authorizer.requestResource` the
//! authorizer types, and the patch types add `Object` / `JSONPatch`
//! (`mutation.DynamicTypeResolver`) and `jsonpatch.escapeKey`. After the
//! check the output type must be one of the accessor's `ReturnTypes()`
//! (`compile.go:186-203`): `bool` for a match condition,
//! `Object` for `ApplyConfigurationCondition` (`patch/smd.go:52`) and
//! `list(JSONPatch)` for `JSONPatchCondition` (`patch/json_patch.go:57`).
//!
//! The result is only compared with the untyped compile probe and logged
//! (`cel_checker_shadow`); nothing is rejected.

use crate::cel_env::ExpectedOutput;
use crate::cel_policy_types::{
    check_policy_expression_with, CheckOptions, DeclType, ParamsType, PolicyCheck,
    PolicyTypeContext,
};
use crate::cel_versions::{EnvSet, EnvType};
use rusternetes_cel_checker::DType;

/// One expression of a MutatingAdmissionPolicy and the environment it is
/// compiled in.
#[derive(Clone, Debug)]
pub struct MutationExpression<'a> {
    pub expression: &'a str,
    /// `HasParams`: `spec.paramKind != nil`.
    pub has_params: bool,
    /// `HasAuthorizer`.
    pub has_authorizer: bool,
    /// `HasPatchTypes`: the patch expressions.
    pub has_patch_types: bool,
    pub expected: ExpectedOutput,
    /// `spec.variables` as `(name, expression)`, in order: the composited
    /// `variables` object has the fields of the variables compiled before.
    pub variables: &'a [(String, String)],
    /// The environment has no `variables` (the stateless match condition
    /// compiler of the validation).
    pub no_variables: bool,
}

/// `ast.OutputType().IsExactType(returnType)` against `ReturnTypes()`.
fn output_matches(expected: ExpectedOutput, output: &DType) -> bool {
    match expected {
        ExpectedOutput::Any => true,
        ExpectedOutput::Bool => *output == DType::Bool,
        ExpectedOutput::String => *output == DType::Str,
        ExpectedOutput::StringOrNull => matches!(output, DType::Str | DType::Null),
        ExpectedOutput::Object => *output == DType::Struct("Object".into()),
        ExpectedOutput::JsonPatchList => {
            matches!(output, DType::List(e) if **e == DType::Struct("JSONPatch".into()))
        }
    }
}

fn expected_name(expected: ExpectedOutput) -> &'static str {
    match expected {
        ExpectedOutput::Any => "dyn",
        ExpectedOutput::Bool => "bool",
        ExpectedOutput::String => "string",
        ExpectedOutput::StringOrNull => "[string, null_type]",
        ExpectedOutput::Object => "Object",
        ExpectedOutput::JsonPatchList => "list(JSONPatch)",
    }
}

/// The checker's errors for `e`, with the output type check
/// (`CompileCELExpression`, `compile.go:166-203`): the messages of the
/// failed compile, or `must evaluate to ... but got ...`. `None` when the
/// expression does not parse (syntax errors are not this comparison's concern).
pub fn check_mutation_expression(
    set: &EnvSet,
    env_type: EnvType,
    e: &MutationExpression<'_>,
) -> Option<Vec<String>> {
    let ctx = PolicyTypeContext {
        // `cel.Variable(ObjectVarName, cel.DynType)` (`compile.go:254-255`).
        object: DeclType::simple(DType::Dyn),
        params: if e.has_params {
            ParamsType::Dyn
        } else {
            ParamsType::None
        },
        variables: e.variables.to_vec(),
    };
    let opts = CheckOptions {
        has_authorizer: e.has_authorizer,
        has_patch_types: e.has_patch_types,
        request_resource_authorizer: true,
        no_variables: e.no_variables,
    };
    let PolicyCheck { mut errors, output } =
        check_policy_expression_with(set, env_type, &ctx, &opts, e.expression)?;
    if errors.is_empty() {
        let output = output.unwrap_or(DType::Dyn);
        if !output_matches(e.expected, &output) {
            errors.push(format!(
                "must evaluate to {} but got {}",
                expected_name(e.expected),
                output.name()
            ));
        }
    }
    Some(errors)
}

/// A disagreement between the checker and the compile probe: `probe` is the
/// probe's failure detail when it rejects the expression, `checker` the
/// checker's errors (non-empty when it rejects). `None` when they agree.
pub fn mutation_divergence(
    set: &EnvSet,
    env_type: EnvType,
    e: &MutationExpression<'_>,
    probe: &Option<String>,
) -> Option<Vec<String>> {
    let checker = check_mutation_expression(set, env_type, e)?;
    (probe.is_some() != !checker.is_empty()).then_some(checker)
}

/// The hook of the MutatingAdmissionPolicy validation: in `shadow` mode logs
/// when the checker and the compile probe disagree about `field`. Never
/// affects validation.
pub fn observe_mutation(
    policy: &str,
    field: &str,
    set: &EnvSet,
    env_type: EnvType,
    e: &MutationExpression<'_>,
    probe: &Option<String>,
) {
    if crate::cel_checker_shadow::Mode::current() != crate::cel_checker_shadow::Mode::Shadow {
        return;
    }
    if let Some(checker) = mutation_divergence(set, env_type, e, probe) {
        tracing::warn!(
            target: "cel_checker_shadow",
            policy,
            field,
            expression = e.expression,
            probe = ?probe,
            checker = ?checker,
            "cel checker disagrees with the compile probe"
        );
    }
}
