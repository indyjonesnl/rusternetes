//! Shadow type check of a ValidatingAdmissionPolicy against the types of the
//! resources it matches (`K3RS_CEL_CHECKER=shadow`, default off).
//!
//! Ported from `plugin/policy/validating/typechecking.go` (release-1.35):
//! `TypeChecker.CreateContext`, `typesToCheck` (with `extractGroups`,
//! `extractVersions`, `extractResources`, `maxTypesToCheck`), `declType`,
//! `paramsGVK`, `CheckExpression`. The schemas come from the embedded upstream
//! OpenAPI v2 definitions through `DefinitionsSchemaResolver`
//! (`cel/openapi/resolver`); a GVK that is not there (custom resources) is
//! skipped, as upstream skips `ErrSchemaNotFound`.
//!
//! Upstream reports the results as `status.typeChecking` warnings. Here they
//! are only compared with the untyped compile probe and logged
//! (`cel_checker_shadow`); nothing is rejected or written.

use crate::restmapper::{
    new_discovery_rest_mapper, served_group_resources, Gvr, PriorityRestMapper,
};
use k3rs_common::cel_openapi_resolver::{DefinitionsSchemaResolver, Gvk, ResolveError};
use k3rs_common::cel_policy_types::{
    check_policy_expression_with, CheckOptions, DeclType, ParamsType, PolicyCheck,
    PolicyTypeContext,
};
use k3rs_common::cel_schema_types::schema_decl_type;
use k3rs_common::cel_versions::{self, EnvSet, EnvType};
use k3rs_common::resources::{
    ExpressionWarning, ValidatingAdmissionPolicy, ValidatingAdmissionPolicySpec,
};
use std::collections::BTreeSet;
use std::sync::OnceLock;

/// `maxTypesToCheck` (`typechecking.go:45`).
pub const MAX_TYPES_TO_CHECK: usize = 10;

/// Upstream v2 (swagger) definitions with their transitive `$ref` closure
/// (release-1.35 `api/openapi-spec/swagger.json`, vendored by
/// `scripts/sync-upstream-openapi.sh` under the api-server's `openapi/`).
pub fn upstream_v2_definitions() -> &'static serde_json::Map<String, serde_json::Value> {
    static DEFS: OnceLock<serde_json::Map<String, serde_json::Value>> = OnceLock::new();
    DEFS.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../../api-server/openapi/upstream/v1.35/v2.definitions.json"
        ))
        .unwrap_or_default()
    })
}

fn resolver() -> &'static DefinitionsSchemaResolver<'static> {
    static R: OnceLock<DefinitionsSchemaResolver<'static>> = OnceLock::new();
    R.get_or_init(|| DefinitionsSchemaResolver::new(upstream_v2_definitions()))
}

/// The RESTMapper over the built-in resources (custom resources have no
/// schema in the resolver, so they are skipped whichever way they map).
fn rest_mapper() -> &'static PriorityRestMapper {
    static M: OnceLock<PriorityRestMapper> = OnceLock::new();
    M.get_or_init(|| new_discovery_rest_mapper(&served_group_resources(&[])))
}

/// `extractGroups` / `extractVersions`: give up on a wildcard.
fn extract_exact(values: &[String]) -> Option<Vec<String>> {
    if values.iter().any(|v| v.contains('*')) {
        None
    } else {
        Some(values.to_vec())
    }
}

/// `extractResources`: skips wildcards and subresources.
fn extract_resources(values: &[String]) -> Vec<String> {
    values
        .iter()
        .filter(|r| !r.contains(['*', '/']))
        .cloned()
        .collect()
}

/// `typesToCheck`: the GVKs the policy's resource rules select, sorted by
/// group, version and kind.
pub fn types_to_check(spec: &ValidatingAdmissionPolicySpec) -> Vec<Gvk> {
    let rules = spec
        .match_constraints
        .as_ref()
        .and_then(|m| m.resource_rules.as_ref());
    let Some(rules) = rules.filter(|r| !r.is_empty()) else {
        return vec![];
    };
    let mut gvks = BTreeSet::new();
    for named in rules {
        let rule = &named.rule;
        let (Some(groups), Some(versions)) = (
            extract_exact(rule.api_groups.as_deref().unwrap_or_default()),
            extract_exact(rule.api_versions.as_deref().unwrap_or_default()),
        ) else {
            continue;
        };
        let resources = extract_resources(rule.resources.as_deref().unwrap_or_default());
        if groups.is_empty() || versions.is_empty() || resources.is_empty() {
            continue;
        }
        // Sorted, so the early return below is deterministic.
        let (mut groups, mut versions, mut resources) = (groups, versions, resources);
        groups.sort();
        versions.sort();
        resources.sort();
        let mut count = 0;
        for group in &groups {
            for version in &versions {
                for resource in &resources {
                    let Ok(kinds) = rest_mapper().kinds_for(&Gvr::new(group, version, resource))
                    else {
                        continue;
                    };
                    for k in kinds {
                        if k.kind.is_empty() {
                            continue;
                        }
                        gvks.insert(Gvk::new(&k.group, &k.version, &k.kind));
                        count += 1;
                        // Early return once the maximum is collected.
                        if count == MAX_TYPES_TO_CHECK {
                            return gvks.into_iter().collect();
                        }
                    }
                }
            }
        }
    }
    gvks.into_iter().collect()
}

/// `paramsGVK`: empty when there is no (parsable) `paramKind`.
fn params_gvk(spec: &ValidatingAdmissionPolicySpec) -> Option<Gvk> {
    let pk = spec.param_kind.as_ref()?;
    let api_version = pk.api_version.as_deref().unwrap_or_default();
    let (group, version) = match api_version.split_once('/') {
        Some((g, v)) if !v.contains('/') => (g, v),
        None => ("", api_version),
        _ => return None,
    };
    Some(Gvk::new(group, version, &pk.kind))
}

/// `declType(gvk)`: `SchemaDeclType(resolved, true)`, uniquely named.
fn decl_type(gvk: &Gvk) -> Result<DeclType, ResolveError> {
    let schema = resolver().resolve_schema(gvk)?;
    let decl = schema_decl_type(&schema, true)
        .ok_or_else(|| ResolveError::Internal(format!("{} is not exposed to CEL", gvk.kind)))?;
    Ok(PolicyTypeContext::named(&gvk.kind, &decl))
}

/// One expression of the policy and where it is.
struct Expr<'a> {
    field: String,
    expression: &'a str,
    has_authorizer: bool,
}

/// The expressions `CheckExpression` is applied to: validations and their
/// messageExpressions (`Check`), plus matchConditions, variables and audit
/// annotations, which are compiled in the same environment at admission.
/// `messageExpression` has no `authorizer` (`compilePolicy`).
fn expressions(spec: &ValidatingAdmissionPolicySpec) -> Vec<Expr<'_>> {
    let mut out = Vec::new();
    for (i, v) in spec.validations.iter().flatten().enumerate() {
        out.push(Expr {
            field: format!("spec.validations[{i}].expression"),
            expression: &v.expression,
            has_authorizer: true,
        });
        if let Some(me) = v.message_expression.as_deref().filter(|m| !m.is_empty()) {
            out.push(Expr {
                field: format!("spec.validations[{i}].messageExpression"),
                expression: me,
                has_authorizer: false,
            });
        }
    }
    for (i, m) in spec.match_conditions.iter().flatten().enumerate() {
        out.push(Expr {
            field: format!("spec.matchConditions[{i}].expression"),
            expression: &m.expression,
            has_authorizer: true,
        });
    }
    for (i, v) in spec.variables.iter().flatten().enumerate() {
        out.push(Expr {
            field: format!("spec.variables[{i}].expression"),
            expression: &v.expression,
            has_authorizer: true,
        });
    }
    for (i, a) in spec.audit_annotations.iter().flatten().enumerate() {
        out.push(Expr {
            field: format!("spec.auditAnnotations[{i}].valueExpression"),
            expression: &a.value_expression,
            has_authorizer: true,
        });
    }
    out.retain(|e| !e.expression.trim().is_empty());
    out
}

/// A disagreement between the typed check and the untyped probe.
#[derive(Debug, PartialEq, Eq)]
pub struct TypedDivergence {
    pub gvk: Gvk,
    pub field: String,
    /// The probe's failure detail, if it rejects the expression.
    pub probe: Option<String>,
    /// The typed checker's errors for this GVK.
    pub checker: Vec<String>,
    /// The checker's errors as `Errors.ToDisplayString` renders them, with
    /// `line:column` and the source snippet (empty when it accepted). Our log
    /// format: upstream's type checker reports only the messages.
    pub display: String,
}

/// Type checks every expression of `obj` for every matched GVK and returns
/// the disagreements with the probe. `[]` when the policy matches no resolvable
/// type (`CreateContext` skips them all).
pub fn typed_divergences(obj: &ValidatingAdmissionPolicy, set: &EnvSet) -> Vec<TypedDivergence> {
    let Some(spec) = &obj.spec else {
        return vec![];
    };
    let params = match params_gvk(spec) {
        None if spec.param_kind.is_none() => ParamsType::None,
        None => ParamsType::Dyn,
        Some(gvk) => match decl_type(&gvk) {
            Ok(t) => ParamsType::Typed(t),
            Err(_) => ParamsType::Dyn,
        },
    };
    let variables: Vec<(String, String)> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| (v.name.clone(), v.expression.clone()))
        .collect();
    let exprs = expressions(spec);
    let mut out = Vec::new();
    for gvk in types_to_check(spec) {
        // Anything but `ErrSchemaNotFound` is an internal error upstream logs;
        // both skip the type.
        let Ok(object) = decl_type(&gvk) else {
            continue;
        };
        let ctx = PolicyTypeContext {
            object,
            params: params.clone(),
            variables: variables.clone(),
        };
        for e in &exprs {
            // `StoredExpressions`: the type checker compiles in the stored env.
            let opts = CheckOptions {
                has_authorizer: e.has_authorizer,
                ..CheckOptions::default()
            };
            let Some(PolicyCheck {
                errors: checker,
                display,
                ..
            }) = check_policy_expression_with(
                set,
                EnvType::StoredExpressions,
                &ctx,
                &opts,
                e.expression,
            )
            else {
                continue;
            };
            let probe = cel_versions::probe_failure(e.expression, set, EnvType::StoredExpressions);
            if probe.is_some() != !checker.is_empty() {
                out.push(TypedDivergence {
                    gvk: gvk.clone(),
                    field: e.field.clone(),
                    probe,
                    checker,
                    display,
                });
            }
        }
    }
    out
}

/// `TypeChecker.Check` (`typechecking.go:107-139`): type checks the
/// `validations[*].expression` and `.messageExpression` of the policy against
/// every matched type and formats the result as the `ExpressionWarning`s that
/// go in `status.typeChecking`. Empty when nothing is wrong. The policy is
/// not mutated.
///
/// Unlike [`typed_divergences`] (the admission-side shadow run) this follows
/// the type checker exactly: only validations and their message expressions
/// are visited; `authorizer` is declared for every expression
/// (`HasAuthorizer: true`, `CheckExpression`) without
/// `authorizer.requestResource` (`buildEnvSet`); `params` is declared only
/// when the `paramKind` schema resolved (`CreateContext`).
///
/// Deviation: an expression that does not even parse is not reported (the
/// untyped checker port returns no verdict for it); API validation rejects
/// such a policy before it is stored.
pub fn check(policy: &ValidatingAdmissionPolicy, set: &EnvSet) -> Vec<ExpressionWarning> {
    let Some(spec) = &policy.spec else {
        return vec![];
    };
    // `CreateContext`: a type whose schema cannot be resolved is skipped.
    let objects: Vec<(Gvk, DeclType)> = types_to_check(spec)
        .into_iter()
        .filter_map(|gvk| decl_type(&gvk).ok().map(|t| (gvk, t)))
        .collect();
    // `paramsGVK` + `declType`: no resolvable params type leaves `params`
    // undeclared.
    let params = params_gvk(spec)
        .and_then(|gvk| decl_type(&gvk).ok())
        .map_or(ParamsType::None, ParamsType::Typed);
    let variables: Vec<(String, String)> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| (v.name.clone(), v.expression.clone()))
        .collect();
    let opts = CheckOptions {
        has_authorizer: true,
        request_resource_authorizer: false,
        ..CheckOptions::default()
    };

    // `CheckExpression` + `TypeCheckingResults.String()`: one
    // `"<gvk>: compilation failed: <issues>\n"` per failing type, joined by
    // `"\n"`.
    let results = |expression: &str| -> String {
        let mut messages = Vec::new();
        for (gvk, object) in &objects {
            let ctx = PolicyTypeContext {
                object: object.clone(),
                params: params.clone(),
                variables: variables.clone(),
            };
            let Some(PolicyCheck {
                errors, display, ..
            }) = check_policy_expression_with(
                set,
                EnvType::StoredExpressions,
                &ctx,
                &opts,
                expression,
            )
            else {
                continue;
            };
            if !errors.is_empty() {
                // `schema.GroupVersionKind.String()`.
                messages.push(format!(
                    "{}/{}, Kind={}: compilation failed: {}\n",
                    gvk.group, gvk.version, gvk.kind, display
                ));
            }
        }
        messages.join("\n")
    };

    let mut warnings = Vec::new();
    for (i, v) in spec.validations.iter().flatten().enumerate() {
        let w = results(&v.expression);
        if !w.is_empty() {
            warnings.push(ExpressionWarning {
                field_ref: format!("spec.validations[{i}].expression"),
                warning: w,
            });
        }
        // `MessageExpression` is optional.
        let Some(me) = v.message_expression.as_deref().filter(|m| !m.is_empty()) else {
            continue;
        };
        let w = results(me);
        if !w.is_empty() {
            warnings.push(ExpressionWarning {
                field_ref: format!("spec.validations[{i}].messageExpression"),
                warning: w,
            });
        }
    }
    warnings
}
