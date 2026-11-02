//! Shadow type check of a ValidatingAdmissionPolicy against the types of the
//! resources it matches (`RUSTERNETES_CEL_CHECKER=shadow`, default off).
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

use crate::handlers::openapi::upstream_v2_definitions;
use rusternetes_common::cel_checker_shadow::Mode;
use rusternetes_common::cel_openapi_resolver::{DefinitionsSchemaResolver, Gvk, ResolveError};
use rusternetes_common::cel_policy_types::{
    check_policy_expression, DeclType, ParamsType, PolicyTypeContext,
};
use rusternetes_common::cel_schema_types::schema_decl_type;
use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
use rusternetes_common::resources::{ValidatingAdmissionPolicy, ValidatingAdmissionPolicySpec};
use rusternetes_discovery::restmapper::{
    new_discovery_rest_mapper, served_group_resources, Gvr, PriorityRestMapper,
};
use std::collections::BTreeSet;
use std::sync::OnceLock;

/// `maxTypesToCheck` (`typechecking.go:45`).
pub const MAX_TYPES_TO_CHECK: usize = 10;

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
            let Some(checker) = check_policy_expression(
                set,
                EnvType::StoredExpressions,
                &ctx,
                e.has_authorizer,
                e.expression,
            ) else {
                continue;
            };
            let probe = cel_versions::probe_failure(e.expression, set, EnvType::StoredExpressions);
            if probe.is_some() != !checker.is_empty() {
                out.push(TypedDivergence {
                    gvk: gvk.clone(),
                    field: e.field.clone(),
                    probe,
                    checker,
                });
            }
        }
    }
    out
}

/// The hook called by the policy validation: in `shadow` mode logs the typed
/// check's disagreements with the probe. Never affects validation.
pub fn observe(obj: &ValidatingAdmissionPolicy, set: &EnvSet) {
    if Mode::current() != Mode::Shadow {
        return;
    }
    for d in typed_divergences(obj, set) {
        tracing::warn!(
            target: "cel_checker_shadow",
            policy = %obj.metadata.name,
            gvk = ?(&d.gvk.group, &d.gvk.version, &d.gvk.kind),
            field = %d.field,
            probe = ?d.probe,
            checker = ?d.checker,
            "typed cel checker disagrees with the compile probe"
        );
    }
}
