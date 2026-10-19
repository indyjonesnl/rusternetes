//! CEL (`x-kubernetes-validations`) enforcement for CustomResourceDefinitions
//! and the custom resources they govern.
//!
//! Upstream reference:
//! - `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel`
//! - `test/e2e/apimachinery/crd_validation_rules.go`
//!
//! Each entry in `spec.versions[].schema.openAPIV3Schema.x-kubernetes-validations`
//! is a `{rule, message, fieldPath?, reason?, optionalOldSelf?}` map. The rule
//! is a CEL expression evaluated with `self` bound to the JSON node the rule
//! is attached to, and (on UPDATE) `oldSelf` bound to the corresponding node
//! from the prior version of the object. A rule that evaluates to `false`
//! rejects the request with the rule's `message`.
//!
//! This module implements three concerns:
//!   1. CRD-time validation: parse + compile every rule when the CRD is
//!      created/updated so syntax errors, references to unknown properties,
//!      and runaway expression cost are rejected before they reach storage.
//!   2. CR-time validation: evaluate every rule against the incoming CR (and
//!      its prior version, for transition rules) on CREATE/UPDATE/PATCH.
//!   3. Cost limits: a coarse heuristic mirroring K8s' default 10M-token
//!      estimated cost budget — sufficient to reject obviously expensive
//!      rules at compile time and runaway evaluations at runtime.

use rusternetes_common::cel_versions::{compile_failure, EnvSet, EnvType};
use rusternetes_common::resources::{
    CustomResource, CustomResourceDefinition, CustomResourceDefinitionSpec, JSONSchemaProps,
    JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
};
use rusternetes_common::schema_validation::PathSeg;
use rusternetes_common::validation::field::{Error as FieldError, Path};
use rusternetes_common::{CELContext, CELEvaluator, Error, Result};
use std::collections::HashSet;

/// Default per-rule estimated cost budget (K8s default = 10M tokens).
const DEFAULT_RULE_COST_LIMIT: u64 = 10_000_000;

/// `supportedValidationReason` (apiextensions validation.go:66-71), in
/// `sets.String.List()` (sorted) order.
const SUPPORTED_VALIDATION_REASON: [&str; 4] = [
    "FieldValueDuplicate",
    "FieldValueForbidden",
    "FieldValueInvalid",
    "FieldValueRequired",
];

/// Default per-request runtime cost budget (K8s default = 100M tokens).
const DEFAULT_REQUEST_RUNTIME_COST_LIMIT: u64 = 100_000_000;

/// A single CEL validation rule extracted from `x-kubernetes-validations[]`.
#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub rule: String,
    pub message: String,
    /// Path inside the schema this rule was attached to, as a vector of
    /// property names (`["spec", "replicas"]`). Used to locate the JSON node
    /// the rule applies to on the incoming CR.
    pub path: Vec<String>,
    /// `optionalOldSelf` (KEP-4008).
    pub optional_old_self: bool,
}

/// Extract every `x-kubernetes-validations[]` entry from `schema`, recursing
/// into `properties`, `items`, and `additionalProperties`. The returned rules
/// carry the JSONPath segment at which they live so the evaluator can bind
/// `self` to the right sub-tree.
pub fn collect_rules(schema: &JSONSchemaProps) -> Vec<ValidationRule> {
    let mut out = Vec::new();
    collect_rules_recursive(schema, &mut Vec::new(), &mut out);
    out
}

fn collect_rules_recursive(
    schema: &JSONSchemaProps,
    path: &mut Vec<String>,
    out: &mut Vec<ValidationRule>,
) {
    if let Some(rules) = &schema.x_kubernetes_validations {
        for raw in rules {
            if let Some(obj) = raw.as_object() {
                let rule = obj
                    .get("rule")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if rule.is_empty() {
                    continue;
                }
                let message = obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("failed rule")
                    .to_string();
                out.push(ValidationRule {
                    rule,
                    message,
                    path: path.clone(),
                    optional_old_self: obj
                        .get("optionalOldSelf")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                });
            }
        }
    }

    if let Some(properties) = &schema.properties {
        for (name, child) in properties {
            path.push(name.clone());
            collect_rules_recursive(child, path, out);
            path.pop();
        }
    }
}

/// The first unknown property reference in one `x-kubernetes-validations`
/// rule of `schema`. A rule reference like `self.foo` is "known" if `foo` is
/// declared in the same schema's `properties` map.
///
/// This is a deliberately conservative heuristic (no schema-typed CEL
/// environment exists yet): it only flags obvious references such as
/// `self.<ident>` against a schema with a declared `properties` map. More
/// subtle references (chained accesses, dynamic indexing) are deferred to
/// runtime.
fn unknown_property_in_rule(schema: &JSONSchemaProps, rule: &str) -> Option<String> {
    let known = schema.properties.as_ref()?;
    let prop = first_self_reference(rule)?;
    (!known.contains_key(&prop)).then_some(prop)
}

/// Return the first `self.<ident>` access in `rule`, or `None` if there is
/// no plain `self.X` reference (rules may still legitimately use `self` as
/// a scalar — `self > 0` — and we leave those alone).
fn first_self_reference(rule: &str) -> Option<String> {
    let bytes = rule.as_bytes();
    let mut i = 0;
    while i + 4 < bytes.len() {
        if &bytes[i..i + 5] == b"self." {
            // Ensure it's not a longer identifier ending in "self." (e.g. "myself.")
            let prev_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            if prev_ok {
                let mut j = i + 5;
                let start = j;
                while j < bytes.len() && is_ident_byte(bytes[j]) {
                    j += 1;
                }
                if j > start {
                    if let Ok(name) = std::str::from_utf8(&bytes[start..j]) {
                        return Some(name.to_string());
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Return true if `rule` references the `oldSelf` identifier as a free
/// variable. K8s treats such rules as "transition rules" and skips them on
/// CREATE rather than evaluating against an undefined binding.
pub fn rule_references_old_self(rule: &str) -> bool {
    let bytes = rule.as_bytes();
    let needle = b"oldSelf";
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let prev_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let next_idx = i + needle.len();
            let next_ok = next_idx >= bytes.len() || !is_ident_byte(bytes[next_idx]);
            if prev_ok && next_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Estimate the cost of a CEL expression. K8s computes an "estimated cost" at
/// compile time as a (deliberately loose) upper bound on runtime tokens.
///
/// We approximate that with a cheap heuristic: each operator/identifier
/// contributes 1, each comprehension-style call (`all`, `exists`, `filter`,
/// `map`) multiplies by an assumed iteration count of 1024, and string
/// literals add their length / 8.  This is intentionally generous so that
/// the obviously-expensive expressions used in the upstream cost-limit test
/// (e.g. nested `all` over many list items) exceed the limit while the
/// trivial rules used in normal tests stay well under it.
pub fn estimate_cost(rule: &str) -> u64 {
    let mut cost: u64 = rule.len() as u64;
    let comprehensions = ["all(", "exists(", "filter(", "map("];
    let mut multiplier: u64 = 1;
    for token in comprehensions {
        let count = rule.matches(token).count() as u32;
        for _ in 0..count {
            multiplier = multiplier.saturating_mul(1024);
            if multiplier > DEFAULT_RULE_COST_LIMIT {
                return DEFAULT_RULE_COST_LIMIT.saturating_add(1);
            }
        }
    }
    cost = cost.saturating_mul(multiplier);
    cost
}

/// `preexistingExpressions` (apiextensions validation.go:153-156): the rules
/// and messageExpressions the stored CRD already carries.
#[derive(Debug, Default, Clone)]
pub struct PreexistingExpressions {
    pub rules: HashSet<String>,
    pub message_expressions: HashSet<String>,
}

impl PreexistingExpressions {
    /// `RuleEnv` (validation.go:158-163): a rule already stored on the CRD
    /// compiles in `StoredExpressions`, any other in `NewExpressions` at the
    /// compatibility version.
    pub fn rule_env(&self, expression: &str) -> EnvType {
        if self.rules.contains(expression) {
            EnvType::StoredExpressions
        } else {
            EnvType::NewExpressions
        }
    }

    /// `MessageExpressionEnv` (validation.go:165-170).
    pub fn message_expression_env(&self, expression: &str) -> EnvType {
        if self.message_expressions.contains(expression) {
            EnvType::StoredExpressions
        } else {
            EnvType::NewExpressions
        }
    }
}

/// `findPreexistingExpressions` (apiextensions validation.go:172-183): every
/// `x-kubernetes-validations[].rule` and non-empty `messageExpression`
/// anywhere in any version's schema (`SchemaHas`, validation.go:1659, walks
/// the whole schema and `findPreexistingExpressionsInSchema` never stops it,
/// :185-195). The deprecated top-level `spec.validation` is v1-only and not
/// modelled.
pub fn find_preexisting_expressions(spec: &CustomResourceDefinitionSpec) -> PreexistingExpressions {
    use std::cell::RefCell;
    let found = RefCell::new(PreexistingExpressions::default());
    for v in &spec.versions {
        if let Some(schema) = v.schema.as_ref().and_then(|s| s.open_apiv3_schema.as_ref()) {
            rusternetes_common::validation::crd::schema_has(schema, &|s| {
                for raw in s.x_kubernetes_validations.iter().flatten() {
                    if let Some(rule) = raw.get("rule").and_then(|r| r.as_str()) {
                        found.borrow_mut().rules.insert(rule.to_string());
                    }
                    if let Some(me) = raw
                        .get("messageExpression")
                        .and_then(|r| r.as_str())
                        .filter(|m| !m.is_empty())
                    {
                        found
                            .borrow_mut()
                            .message_expressions
                            .insert(me.to_string());
                    }
                }
                false
            });
        }
    }
    found.into_inner()
}

/// `getCostErrorMessage` (validation.go:1352-1366).
fn get_cost_error_message(cost_name: &str, expression_cost: u64, cost_limit: u64) -> String {
    let exceed_factor = expression_cost as f64 / cost_limit as f64;
    let factor = if exceed_factor > 100.0 {
        "more than 100x".to_string()
    } else if exceed_factor < 1.5 {
        format!("{exceed_factor:.6}x")
    } else {
        format!("{exceed_factor:.1}x")
    };
    format!("{cost_name} exceeds budget by factor of {factor} (try simplifying the rule, or adding maxItems, maxProperties, and maxLength where arrays, maps, and strings are declared)")
}

/// The CEL half of `ValidateCustomResourceDefinitionOpenAPISchema` for one
/// version's `openAPIV3Schema` at `fld_path`.
///
/// Not ported: the per-CRD `TotalCost` (`StaticEstimatedCRDCostLimit`,
/// validation.go:981-992, cel_validation.go:260-297) and the per-schema
/// cardinality multiplication (`getExpressionCost`, validation.go:1345): both
/// need a real cost estimator, and the coarse [`estimate_cost`] heuristic
/// would reject valid CRDs with them.
///
/// Rules are compiled against `env_set`; those already stored on the CRD
/// (`pre`) in the stored environment. Rust has no schema-typed CEL
/// environment or cost estimator yet, so compile errors are the
/// undeclared-reference / syntax checks of `cel_versions` and the CEL parser,
/// and costs come from the coarse [`estimate_cost`] heuristic.
pub fn validate_crd_schema_rules(
    schema: &JSONSchemaProps,
    fld_path: &Path,
    env_set: &EnvSet,
    pre: &PreexistingExpressions,
    suppress_per_expression_cost: bool,
) -> Vec<FieldError> {
    let mut errs = Vec::new();
    compile_schema_rules(
        schema,
        fld_path,
        env_set,
        pre,
        suppress_per_expression_cost,
        &mut errs,
    );
    errs
}

/// The recursion of `ValidateCustomResourceDefinitionOpenAPISchema`
/// (validation.go:1031-1322) restricted to the sub-schemas it passes a
/// non-nil `celContext`: `additionalProperties` (:1065), `properties`
/// (:1084), `items` (:1128) and tuple `items[i]` (:1132). `not`, `allOf`,
/// `oneOf`, `anyOf`, `definitions` and `dependencies` recurse with a nil
/// context (:1088-1141), so their rules are not compiled.
///
/// Returns whether a schema error occurred here or deeper: CEL compilation
/// is skipped for the node then (:1264-1268).
fn compile_schema_rules(
    schema: &JSONSchemaProps,
    fld_path: &Path,
    env_set: &EnvSet,
    pre: &PreexistingExpressions,
    suppress: bool,
    errs: &mut Vec<FieldError>,
) -> bool {
    let mut schema_errors = false;
    if let Some(JSONSchemaPropsOrBool::Schema(child)) = schema.additional_properties.as_deref() {
        schema_errors |= compile_schema_rules(
            child,
            &fld_path.child("additionalProperties"),
            env_set,
            pre,
            suppress,
            errs,
        );
    }
    if let Some(properties) = &schema.properties {
        let mut names: Vec<&String> = properties.keys().collect();
        names.sort();
        for name in names {
            schema_errors |= compile_schema_rules(
                &properties[name],
                &fld_path.child("properties").key(name.clone()),
                env_set,
                pre,
                suppress,
                errs,
            );
        }
    }
    match schema.items.as_deref() {
        Some(JSONSchemaPropsOrArray::Schema(i)) => {
            schema_errors |=
                compile_schema_rules(i, &fld_path.child("items"), env_set, pre, suppress, errs);
        }
        Some(JSONSchemaPropsOrArray::Schemas(list)) => {
            for (i, item) in list.iter().enumerate() {
                schema_errors |= compile_schema_rules(
                    item,
                    &fld_path.child("items").index(i),
                    env_set,
                    pre,
                    suppress,
                    errs,
                );
            }
        }
        None => {}
    }

    let Some(validations) = schema
        .x_kubernetes_validations
        .as_ref()
        .filter(|v| !v.is_empty())
    else {
        return schema_errors;
    };
    let rule_path = |i: usize| fld_path.child("x-kubernetes-validations").index(i);
    // validation.go:1229-1262.
    let str_of = |raw: &serde_json::Value, key: &str| -> String {
        raw.get(key)
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string()
    };
    let has_newlines = |s: &str| s.contains(['\n', '\r']);
    for (i, raw) in validations.iter().enumerate() {
        let rule = str_of(raw, "rule");
        let message = str_of(raw, "message");
        let message_expression = str_of(raw, "messageExpression");
        let field_path = str_of(raw, "fieldPath");
        let (trimmed_rule, trimmed_msg) = (rule.trim(), message.trim());
        let message_path = rule_path(i).child("message");
        if trimmed_rule.is_empty() {
            errs.push(FieldError::required(
                &rule_path(i).child("rule"),
                "rule is not specified",
            ));
            schema_errors = true;
        } else if !message.is_empty() && trimmed_msg.is_empty() {
            errs.push(FieldError::invalid(
                &message_path,
                message.clone(),
                "must be non-empty if specified",
            ));
            schema_errors = true;
        } else if has_newlines(trimmed_msg) {
            errs.push(FieldError::invalid(
                &message_path,
                message.clone(),
                "must not contain line breaks",
            ));
            schema_errors = true;
        } else if has_newlines(trimmed_rule) && trimmed_msg.is_empty() {
            errs.push(FieldError::required(
                &message_path,
                "message must be specified if rule contains line breaks",
            ));
            schema_errors = true;
        }
        if !message_expression.is_empty() && message_expression.trim().is_empty() {
            errs.push(FieldError::required(
                &rule_path(i).child("messageExpression"),
                "messageExpression must be non-empty if specified",
            ));
            schema_errors = true;
        }
        if let Some(reason) = raw.get("reason").and_then(|r| r.as_str()) {
            if !SUPPORTED_VALIDATION_REASON.contains(&reason) {
                errs.push(FieldError::not_supported(
                    &rule_path(i).child("reason"),
                    reason,
                    &SUPPORTED_VALIDATION_REASON,
                ));
                schema_errors = true;
            }
        }
        let fp_path = rule_path(i).child("fieldPath");
        if !field_path.is_empty() && field_path.trim().is_empty() {
            errs.push(FieldError::invalid(
                &fp_path,
                field_path.clone(),
                "must be non-empty if specified",
            ));
            schema_errors = true;
        }
        if has_newlines(&field_path) {
            errs.push(FieldError::invalid(
                &fp_path,
                field_path.clone(),
                "must not contain line breaks",
            ));
            schema_errors = true;
        }
        if !field_path.is_empty()
            && !rusternetes_common::validation::crd::path_valid(schema, &field_path)
        {
            errs.push(FieldError::invalid(
                &fp_path,
                field_path.clone(),
                "must be a valid path",
            ));
            schema_errors = true;
        }
    }
    if schema_errors {
        return true;
    }

    for (i, raw) in validations.iter().enumerate() {
        let rule = raw.get("rule").and_then(|r| r.as_str()).unwrap_or("");
        if rule.trim().is_empty() {
            // compileRule: an empty result, no error (compilation.go:236-241).
            continue;
        }
        let rule_field = rule_path(i).child("rule");
        // compileRule (compilation.go:242-246), then the unknown-property
        // heuristic standing in for the schema-typed checker.
        let failure = compile_failure(rule, env_set, pre.rule_env(rule))
            .or_else(|| {
                CELEvaluator::new()
                    .type_check(rule)
                    .err()
                    .map(|e| format!("compilation failed: {e}"))
            })
            .or_else(|| {
                unknown_property_in_rule(schema, rule).map(|prop| {
                    format!("x-kubernetes-validations rule refers to unknown property: {prop}")
                })
            });
        if let Some(detail) = failure {
            errs.push(FieldError::invalid(&rule_field, raw.clone(), detail));
            // compileRule returns before the messageExpression (:242-246).
            continue;
        }
        // validation.go:1280-1287.
        let cost = estimate_cost(rule);
        if !suppress && cost > DEFAULT_RULE_COST_LIMIT {
            errs.push(FieldError::forbidden(
                &rule_field,
                get_cost_error_message("estimated rule cost", cost, DEFAULT_RULE_COST_LIMIT),
            ));
        }
        // compileRule's messageExpression (compilation.go:253-287), and
        // validation.go:1295-1307.
        let me = raw
            .get("messageExpression")
            .and_then(|m| m.as_str())
            .unwrap_or("");
        if me.is_empty() {
            continue;
        }
        let me_field = rule_path(i).child("messageExpression");
        let me_failure = compile_failure(me, env_set, pre.message_expression_env(me))
            .map(|d| {
                format!(
                    "messageExpression compilation failed: {}",
                    d.strip_prefix("compilation failed: ").unwrap_or(&d)
                )
            })
            .or_else(|| {
                CELEvaluator::new()
                    .type_check(me)
                    .err()
                    .map(|e| format!("messageExpression compilation failed: {e}"))
            });
        if let Some(detail) = me_failure {
            errs.push(FieldError::invalid(&me_field, raw.clone(), detail));
            continue;
        }
        let me_cost = estimate_cost(me);
        if !suppress && me_cost > DEFAULT_RULE_COST_LIMIT {
            errs.push(FieldError::forbidden(
                &me_field,
                get_cost_error_message(
                    "estimated messageExpression cost",
                    me_cost,
                    DEFAULT_RULE_COST_LIMIT,
                ),
            ));
        }
    }
    false
}

/// Locate the JSON sub-tree the rule targets, given its `path` of property
/// names walked from the schema root.
fn node_at_path<'a>(root: &'a serde_json::Value, path: &[String]) -> Option<&'a serde_json::Value> {
    let mut cur = root;
    for seg in path {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Evaluate every collected rule against the incoming CR and (optionally) the
/// prior CR for transition-rule support. Returns the first rule that fails
/// (or errors at runtime) so callers can reject with the matching message.
///
/// `cr` is the candidate object (post-mutation); `old_cr` is the prior version
/// when this is an UPDATE/PATCH, or `None` on CREATE.
pub fn validate_cr_rules(
    rules: &[ValidationRule],
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    if rules.is_empty() {
        return Ok(());
    }

    let cr_json = serde_json::to_value(cr).map_err(|e| Error::Internal(e.to_string()))?;
    let old_json = old_cr
        .map(|c| serde_json::to_value(c).map_err(|e| Error::Internal(e.to_string())))
        .transpose()?;

    let mut evaluator = CELEvaluator::new();
    let mut total_cost: u64 = 0;

    for r in rules {
        // Transition rules (referencing `oldSelf`) only fire on UPDATE.
        // K8s skips them entirely on CREATE rather than evaluating against
        // an undefined `oldSelf` — see upstream
        // `apiextensions-apiserver/.../validation/validation.go`.
        let is_transition = rule_references_old_self(&r.rule);
        // Unless `optionalOldSelf` is set (validation.go:401-418: the rule
        // then runs with `oldSelf` an empty optional).
        if is_transition && old_json.is_none() && !r.optional_old_self {
            continue;
        }

        // K8s default is 100M for the whole request. Reject runaway rules
        // before they swamp the request.
        total_cost = total_cost.saturating_add(estimate_cost(&r.rule));
        if total_cost > DEFAULT_REQUEST_RUNTIME_COST_LIMIT {
            return Err(Error::InvalidResource(format!(
                "x-kubernetes-validations rule exceeded the runtime cost limit: {}",
                r.rule
            )));
        }

        let self_node = node_at_path(&cr_json, &r.path)
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let mut ctx = CELContext::new();
        ctx.add_json_variable("self", &self_node)
            .map_err(|e| Error::Internal(e.to_string()))?;

        let old_self = old_json.as_ref().and_then(|old| node_at_path(old, &r.path));
        if r.optional_old_self {
            ctx.add_optional_json_variable("oldSelf", old_self)
                .map_err(|e| Error::Internal(e.to_string()))?;
        } else if let Some(old_self) = old_self {
            ctx.add_json_variable("oldSelf", old_self)
                .map_err(|e| Error::Internal(e.to_string()))?;
        }

        // Also expose `object`/`oldObject` for tests that author rules against
        // the whole CR rather than the targeted sub-tree.
        ctx.add_json_variable("object", &cr_json)
            .map_err(|e| Error::Internal(e.to_string()))?;
        if let Some(old) = &old_json {
            ctx.add_json_variable("oldObject", old)
                .map_err(|e| Error::Internal(e.to_string()))?;
        }

        match evaluator.evaluate(&r.rule, &ctx) {
            Ok(true) => {}
            Ok(false) => {
                return Err(Error::InvalidResource(r.message.clone()));
            }
            Err(e) => {
                return Err(Error::InvalidResource(format!(
                    "failed to evaluate x-kubernetes-validations rule {:?}: {}",
                    r.rule, e
                )));
            }
        }
    }

    Ok(())
}

/// Convenience: walk every served version's schema and validate CR rules
/// against `cr`. Only the version the CR was POSTed against is consulted.
pub fn validate_cr_against_crd(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) else {
        return Ok(());
    };
    let Some(schema) = crd_version
        .schema
        .as_ref()
        .and_then(|s| s.open_apiv3_schema.as_ref())
    else {
        return Ok(());
    };
    let rules = collect_rules(schema);
    validate_cr_rules(&rules, cr, old_cr)
}

// ---------------------------------------------------------------------------
// Ratcheting-aware CR rule evaluation (KEP-4008)
// ---------------------------------------------------------------------------
//
// The simple `validate_cr_rules` above is keyed by property-name paths, which
// is fine for rules attached to scalar fields under `spec` but cannot describe
// the per-item evaluations the ratcheting tests demand (a rule attached to
// `list[].field` fails per-list-item, with `list[i]` carrying its own
// structural index). For ratcheting we need the *concrete* path of every
// failure so the caller can correlate against the prior object.

/// A single CEL rule failure surfaced from a CR validation walk.
#[derive(Debug, Clone)]
pub struct CrRuleFailure {
    /// Structural path to the node the rule was evaluated against.
    pub path: Vec<PathSeg>,
    /// Rendered failure message (the rule's `message`).
    pub message: String,
    /// Whether the rule references `oldSelf` (transition rule). Transition
    /// rules are NEVER ratcheted — surfaced here so callers can preserve them.
    pub is_transition: bool,
}

/// Walk `schema` and `cr_value` concurrently, evaluating every reachable
/// instance of every `x-kubernetes-validations` rule. Returns the list of
/// failures (each with a structural path) so the caller may filter them with
/// the ratcheting predicate.
///
/// `old_value` is only used to bind `oldSelf` for transition rules; pass
/// `None` on CREATE.
pub fn evaluate_cr_rules_with_paths(
    schema: &JSONSchemaProps,
    cr_value: &serde_json::Value,
    old_value: Option<&serde_json::Value>,
) -> Result<Vec<CrRuleFailure>> {
    let mut evaluator = CELEvaluator::new();
    let mut failures = Vec::new();
    let mut total_cost: u64 = 0;
    walk_and_eval(
        schema,
        cr_value,
        old_value,
        &mut Vec::new(),
        &mut evaluator,
        &mut total_cost,
        &mut failures,
    )?;
    Ok(failures)
}

#[allow(clippy::too_many_arguments)]
fn walk_and_eval(
    schema: &JSONSchemaProps,
    node: &serde_json::Value,
    old_node: Option<&serde_json::Value>,
    path: &mut Vec<PathSeg>,
    evaluator: &mut CELEvaluator,
    total_cost: &mut u64,
    failures: &mut Vec<CrRuleFailure>,
) -> Result<()> {
    // Evaluate every rule attached to this schema node against `node`.
    if let Some(rules) = &schema.x_kubernetes_validations {
        for raw in rules {
            let obj = match raw.as_object() {
                Some(o) => o,
                None => continue,
            };
            let rule_str = match obj.get("rule").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => continue,
            };
            let message = obj
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("failed rule")
                .to_string();

            let is_transition = rule_references_old_self(rule_str);
            // Skip transition rules on CREATE.
            // ...unless `optionalOldSelf` (validation.go:401-418).
            let optional_old_self = obj
                .get("optionalOldSelf")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if is_transition && old_node.is_none() && !optional_old_self {
                continue;
            }

            *total_cost = total_cost.saturating_add(estimate_cost(rule_str));
            if *total_cost > DEFAULT_REQUEST_RUNTIME_COST_LIMIT {
                return Err(Error::InvalidResource(format!(
                    "x-kubernetes-validations rule exceeded the runtime cost limit: {}",
                    rule_str
                )));
            }

            let mut ctx = CELContext::new();
            ctx.add_json_variable("self", node)
                .map_err(|e| Error::Internal(e.to_string()))?;
            if optional_old_self {
                ctx.add_optional_json_variable("oldSelf", old_node)
                    .map_err(|e| Error::Internal(e.to_string()))?;
            } else if let Some(o) = old_node {
                ctx.add_json_variable("oldSelf", o)
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }

            match evaluator.evaluate(rule_str, &ctx) {
                Ok(true) => {}
                Ok(false) => failures.push(CrRuleFailure {
                    path: path.clone(),
                    message,
                    is_transition,
                }),
                Err(_) => {
                    // Runtime errors are reported as failures (no ratcheting
                    // for malformed evaluation either). Render the rule body
                    // so the caller's diagnostics stay actionable.
                    failures.push(CrRuleFailure {
                        path: path.clone(),
                        message: format!(
                            "failed to evaluate x-kubernetes-validations rule {:?}",
                            rule_str
                        ),
                        is_transition,
                    });
                }
            }
        }
    }

    // Recurse into object properties.
    if let Some(map) = node.as_object() {
        if let Some(properties) = &schema.properties {
            for (key, prop_schema) in properties {
                if let Some(child) = map.get(key) {
                    let old_child = old_node
                        .and_then(|o| o.as_object())
                        .and_then(|m| m.get(key));
                    path.push(PathSeg::Key(key.clone()));
                    walk_and_eval(
                        prop_schema,
                        child,
                        old_child,
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
        if let Some(addl) = &schema.additional_properties {
            use rusternetes_common::resources::crd::JSONSchemaPropsOrBool;
            if let JSONSchemaPropsOrBool::Schema(addl_schema) = addl.as_ref() {
                let declared: std::collections::HashSet<&String> = schema
                    .properties
                    .as_ref()
                    .map(|p| p.keys().collect())
                    .unwrap_or_default();
                for (key, child) in map {
                    if declared.contains(key) {
                        continue;
                    }
                    let old_child = old_node
                        .and_then(|o| o.as_object())
                        .and_then(|m| m.get(key));
                    path.push(PathSeg::Key(key.clone()));
                    walk_and_eval(
                        addl_schema,
                        child,
                        old_child,
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
    }

    // Recurse into arrays.
    if let Some(arr) = node.as_array() {
        if let Some(items) = &schema.items {
            use rusternetes_common::resources::crd::JSONSchemaPropsOrArray;
            if let JSONSchemaPropsOrArray::Schema(item_schema) = items.as_ref() {
                for (i, child) in arr.iter().enumerate() {
                    let old_child = correlate_old_item(schema, old_node, child);
                    path.push(PathSeg::Index(i));
                    walk_and_eval(
                        item_schema,
                        child,
                        old_child.as_ref(),
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
    }

    Ok(())
}

/// For a list-type=map parent schema, look up the old item whose composite
/// key matches `new_item`. Returns `None` for any other shape.
fn correlate_old_item(
    array_schema: &JSONSchemaProps,
    old_node: Option<&serde_json::Value>,
    new_item: &serde_json::Value,
) -> Option<serde_json::Value> {
    if array_schema.x_kubernetes_list_type.as_deref() != Some("map") {
        return None;
    }
    let keys = array_schema.x_kubernetes_list_map_keys.as_ref()?;
    if keys.is_empty() {
        return None;
    }
    let new_obj = new_item.as_object()?;
    let mut new_ck = String::new();
    for k in keys {
        let v = new_obj.get(k)?;
        new_ck.push('\x00');
        match v {
            serde_json::Value::Bool(b) => new_ck.push_str(&format!("b:{b}")),
            serde_json::Value::Number(n) => new_ck.push_str(&format!("n:{n}")),
            serde_json::Value::String(s) => new_ck.push_str(&format!("s:{s}")),
            _ => return None,
        }
    }
    let old_arr = old_node?.as_array()?;
    for o in old_arr {
        let o_obj = match o.as_object() {
            Some(m) => m,
            None => continue,
        };
        let mut ck = String::new();
        let mut ok = true;
        for k in keys {
            match o_obj.get(k) {
                Some(serde_json::Value::Bool(b)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("b:{b}"))
                }
                Some(serde_json::Value::Number(n)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("n:{n}"))
                }
                Some(serde_json::Value::String(s)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("s:{s}"))
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && ck == new_ck {
            return Some(o.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::feature_gates::Version;
    use serde_json::json;

    fn schema_with_rule(rule: &str, message: &str) -> JSONSchemaProps {
        let raw = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "replicas": {"type": "integer"}
                    },
                    "x-kubernetes-validations": [
                        {"rule": rule, "message": message}
                    ]
                }
            }
        });
        serde_json::from_value(raw).expect("schema deserialises")
    }

    fn optional_schema(rule: &str, optional: Option<bool>) -> JSONSchemaProps {
        let mut v = json!({"rule": rule, "message": "failed rule"});
        if let Some(b) = optional {
            v["optionalOldSelf"] = json!(b);
        }
        serde_json::from_value(json!({
            "type": "object",
            "properties": {"foo": {"type": "string"}},
            "x-kubernetes-validations": [v]
        }))
        .expect("schema deserialises")
    }

    fn fails(
        schema: &JSONSchemaProps,
        new: serde_json::Value,
        old: Option<serde_json::Value>,
    ) -> bool {
        !evaluate_cr_rules_with_paths(schema, &new, old.as_ref())
            .unwrap()
            .is_empty()
    }

    /// apiextensions-apiserver `pkg/apiserver/schema/cel/validation_test.go`
    /// `TestOptionalOldSelf`: with `optionalOldSelf: true` the rule runs on
    /// create with `oldSelf` an empty optional.
    #[test]
    fn optional_old_self_evaluates_on_create_with_none() {
        let s = optional_schema("self.foo == 'not bar' || !oldSelf.hasValue()", Some(true));
        assert!(!fails(&s, json!({"foo": "bar"}), None));
        // rule requires an old value: on create the empty optional fails it.
        let s = optional_schema("oldSelf.hasValue() || self.foo == 'ok'", Some(true));
        assert!(fails(&s, json!({"foo": "bad"}), None));
        assert!(!fails(&s, json!({"foo": "ok"}), None));
    }

    #[test]
    fn optional_old_self_binds_old_value_on_update() {
        let s = optional_schema(
            "self.foo == 'valid' || (oldSelf.hasValue() && oldSelf.value().foo != 'valid')",
            Some(true),
        );
        // block: old was valid
        assert!(fails(
            &s,
            json!({"foo": "invalid"}),
            Some(json!({"foo": "valid"}))
        ));
        // ratchet: old also invalid
        assert!(!fails(
            &s,
            json!({"foo": "invalid again"}),
            Some(json!({"foo": "invalid"}))
        ));
        // `oldSelf.?foo.orValue(...)` optional field select
        let s = optional_schema(
            "self.foo != 'bar' || oldSelf.?foo.orValue('baz') == 'baz'",
            Some(true),
        );
        assert!(!fails(
            &s,
            json!({"foo": "bar"}),
            Some(json!({"foo": "baz"}))
        ));
        assert!(fails(
            &s,
            json!({"foo": "bar"}),
            Some(json!({"foo": "other"}))
        ));
    }

    /// Without the flag (or `false`) a transition rule is still skipped on
    /// create (validation.go: `compiled.UsesOldSelf && oldObj == nil`).
    #[test]
    fn transition_rule_without_optional_old_self_skipped_on_create() {
        for flag in [None, Some(false)] {
            let s = optional_schema("oldSelf.foo == self.foo", flag);
            assert!(!fails(&s, json!({"foo": "x"}), None));
        }
    }

    /// The flat (`collect_rules` / `validate_cr_rules`) path honours it too.
    #[test]
    fn collect_rules_carries_optional_old_self() {
        let s = optional_schema("!oldSelf.hasValue() || self.foo == 'x'", Some(true));
        assert!(collect_rules(&s)[0].optional_old_self);
        let s = optional_schema("oldSelf.foo == self.foo", None);
        assert!(!collect_rules(&s)[0].optional_old_self);
    }

    /// `preexistingExpressions.RuleEnv` (validation.go:158-163): a new rule is
    /// compiled in `NewExpressions` at `DefaultCompatibilityVersion()`, so a
    /// library introduced after the min-compatibility version is undeclared.
    #[test]
    fn new_rules_are_compiled_at_the_compatibility_version() {
        let schema = schema_with_rule("semver('1.2.3').major() > 0", "m");
        let at_1_32 = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        let root = Path::new("openAPIV3Schema");
        let pre = PreexistingExpressions::default();
        let errs = validate_crd_schema_rules(&schema, &root, &at_1_32, &pre, false);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0]
                .detail
                .contains("undeclared reference to 'semver' (in container '')"),
            "{errs:?}"
        );
        assert_eq!(
            errs[0].field,
            "openAPIV3Schema.properties[spec].x-kubernetes-validations[0].rule"
        );
        let at_1_33 = EnvSet::must_base_env_set(Version::major_minor(1, 33));
        assert!(validate_crd_schema_rules(&schema, &root, &at_1_33, &pre, false).is_empty());
    }

    /// A rule already stored on the CRD is compiled in `StoredExpressions`.
    #[test]
    fn preexisting_rules_are_compiled_in_the_stored_environment() {
        let rule = "semver('1.2.3').major() > 0";
        let schema = schema_with_rule(rule, "m");
        let at_1_32 = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        let pre = PreexistingExpressions {
            rules: [rule.to_string()].into(),
            ..Default::default()
        };
        let root = Path::new("openAPIV3Schema");
        assert!(validate_crd_schema_rules(&schema, &root, &at_1_32, &pre, false).is_empty());
    }

    #[test]
    fn collect_rules_finds_nested_validation() {
        let schema = schema_with_rule("self.replicas <= 100", "too many replicas");
        let rules = collect_rules(&schema);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].path, vec!["spec"]);
        assert_eq!(rules[0].message, "too many replicas");
    }

    #[test]
    fn first_self_reference_extracts_property() {
        assert_eq!(
            first_self_reference("self.foo > 0"),
            Some("foo".to_string())
        );
        assert_eq!(first_self_reference("self > 0"), None);
        assert_eq!(first_self_reference("myself.foo > 0"), None);
    }

    #[test]
    fn detect_unknown_property_flags_misspelling() {
        let schema = schema_with_rule("self.replicaz <= 100", "msg");
        let spec = &schema.properties.as_ref().unwrap()["spec"];
        let unknown = unknown_property_in_rule(spec, "self.replicaz <= 100");
        assert_eq!(unknown.as_deref(), Some("replicaz"));
        assert_eq!(unknown_property_in_rule(spec, "self.replicas <= 100"), None);
    }

    #[test]
    fn rule_references_old_self_finds_identifier() {
        assert!(rule_references_old_self("self.x >= oldSelf.x"));
        assert!(rule_references_old_self("oldSelf == self"));
        assert!(!rule_references_old_self("self.x > 0"));
        // Substring matches don't count.
        assert!(!rule_references_old_self("myoldSelfFoo > 0"));
    }

    #[test]
    fn estimate_cost_is_large_for_nested_comprehensions() {
        // Two nested all(...) calls => 1024*1024 multiplier
        let rule = "self.list.all(x, x.all(y, y > 0))";
        assert!(estimate_cost(rule) > 10_000_000);
    }
}
