//! Dedicated CEL (Common Expression Language) evaluators for the admission path.
//!
//! This module owns the **VAP** (`ValidatingAdmissionPolicy`) and admission
//! webhook CEL surfaces and offers a single, well-documented place to fix
//! parity issues against upstream Kubernetes.
//!
//! Upstream reference (Go):
//!   `staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/`
//!     - `compile.go`  — defines the activation variables every CEL surface sees
//!     - `filter.go`   — `Filter.ForInput` driver that evaluates expressions
//!     - `validator.go`— evaluates `validations[*].expression` &
//!                       `messageExpression`
//!   `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/matching/matcher.go`
//!     — short-circuits matchConditions in order
//!
//! The heavy-lifting CEL primitives (`CELEvaluator`, `CELContext`, JSON→CEL
//! value conversion) live in [`rusternetes_common::cel`]. This module exposes
//! purpose-built APIs that take a typed [`AdmissionRequest`] and return one of
//! a small set of outcomes per surface.
//!
//! # Surfaces in this module
//!
//! | Type                       | What it evaluates                                                |
//! |----------------------------|------------------------------------------------------------------|
//! | [`MatchConditionEvaluator`]| `Webhook.matchConditions[*]` & `VAP.spec.matchConditions[*]`     |
//! | [`ValidationEvaluator`]    | `VAP.spec.validations[*].expression` (+ optional messageExpression)|
//! | [`AuditAnnotationEvaluator`]| `VAP.spec.auditAnnotations[*].valueExpression`                  |
//!
//! # Activation variables
//!
//! Per the [VAP spec], the CEL expressions can reference:
//!
//! * `object`         - the new object being admitted (the request body)
//! * `oldObject`      - the prior object on UPDATE; `null` otherwise
//! * `request`        - the [`AdmissionRequest`] metadata (operation, kind, etc.)
//! * `params`         - the resource pointed to by the binding's `paramRef`, or `null`
//! * `variables`      - lazily-evaluated `spec.variables` map (callers supply)
//! * `namespaceObject`- the Namespace object (callers supply when available)
//!
//! `authorizer` is bound by [`bind_authorizer`] (activation.go:52-55) when the
//! caller has a request authorizer; request-options (`dryRun`, `options`) are **not** populated
//! by these evaluators; callers that need them should layer them on top of the
//! [`CELContext`] returned from [`build_context`] before invoking the evaluator.
//!
//! # Out of scope (tracked separately)
//!
//! * CRD `x-kubernetes-validations[*].rule` — that surface evaluates per-CR
//!   inside a different code path (CRD validation, not admission) and will be
//!   wired in a follow-up PR.
//!
//! [VAP spec]: https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/

use rusternetes_common::admission::AdmissionRequest;
use rusternetes_common::cel::{CELContext, CELEvaluator};
use rusternetes_common::cel_cost::{
    charge_budget, BudgetError, CostOptions, MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES,
    RUNTIME_CEL_COST_BUDGET, RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS,
};
use rusternetes_common::resources::MatchCondition;
use serde_json::Value;

/// Outcome of evaluating a policy's `matchConditions[]` against a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOutcome {
    /// Every match condition evaluated to `true` (or the list was empty). The
    /// policy applies and its validations should run.
    Matched,
    /// At least one match condition evaluated to `false`. The policy is silently
    /// skipped for this request — upstream calls this "not matched".
    NotMatched,
    /// A match condition expression failed to compile or returned a non-bool.
    /// Caller decides whether to honour the policy's `failurePolicy` here.
    Error(String),
}

/// Outcome of evaluating a single `VAP.spec.validations[*]` entry.
///
/// Upstream Go reference: `admission/plugin/cel/validator.go::Validate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationOutcome {
    /// The expression returned `true` — validation passed.
    Pass,
    /// The expression returned `false`. `message` is the resolved message:
    /// the result of `messageExpression` when present and the static `message`
    /// otherwise (mirrors upstream).
    Fail { message: String },
    /// The expression failed to compile or errored at runtime. Caller honours
    /// the policy's `failurePolicy` here.
    Error { message: String },
}

/// Outcome of evaluating a single `VAP.spec.auditAnnotations[*]` entry.
///
/// Upstream parity (`admission/plugin/cel/validator.go::auditAnnotation`):
///
/// * a value expression returning a string → emit the annotation
/// * a value expression returning `null`   → **skip** the annotation
/// * a runtime/compile error               → caller honours `failurePolicy`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditAnnotationOutcome {
    /// Emit `{key: value}` on the audit event. `key` is the annotation's
    /// declared `key`, prefixed by the policy name on the caller side.
    Emit { key: String, value: String },
    /// `valueExpression` returned `null` — upstream drops the annotation.
    Skip,
    /// `valueExpression` errored. Caller honours `failurePolicy`.
    Error { message: String },
}

/// Evaluator dedicated to webhook & VAP `matchConditions[*]`.
///
/// Reuses the program cache exposed by [`rusternetes_common::cel::CELEvaluator`]
/// so repeated admission of objects against the same policy avoids re-compiling
/// the CEL programs.
pub struct MatchConditionEvaluator {
    inner: CELEvaluator,
}

impl MatchConditionEvaluator {
    pub fn new() -> Self {
        Self {
            inner: CELEvaluator::new(),
        }
    }

    /// Evaluate every match condition in `conditions` against `request`/`params`.
    ///
    /// Conditions are evaluated **in order** and short-circuit on the first
    /// non-matching condition, mirroring upstream `matcher.go`.
    ///
    /// Returns:
    /// * [`MatchOutcome::Matched`]    — `conditions` is empty or all returned `true`
    /// * [`MatchOutcome::NotMatched`] — some condition returned `false`
    /// * [`MatchOutcome::Error`]      — some condition errored (compile or runtime)
    pub fn evaluate(
        &mut self,
        conditions: &[MatchCondition],
        request: &AdmissionRequest,
        params: Option<&Value>,
    ) -> MatchOutcome {
        if conditions.is_empty() {
            return MatchOutcome::Matched;
        }

        let context = match build_context(request, params) {
            Ok(ctx) => ctx,
            Err(e) => return MatchOutcome::Error(format!("failed to build CEL context: {}", e)),
        };

        self.evaluate_with_context(conditions, &context)
    }

    /// Same as [`Self::evaluate`] but with a pre-built [`CELContext`] — used by
    /// the VAP path that needs to layer `variables` / `namespaceObject` onto
    /// the base activation before evaluating.
    ///
    /// Ported from `webhook/matchconditions/matcher.go` `Match` (:60-143) over
    /// `cel/condition.go` `ForInput` (:90-111): EVERY condition is evaluated
    /// first (`condition.go:103-109`), each charged against
    /// `RuntimeCELCostBudgetMatchConditions` (`matcher.go:82-85`,
    /// `apis/cel/config.go:31`) via `activation.Evaluate`
    /// (`activation.go:132-189`); running out of budget returns the error
    /// instead of any evaluation (`return nil, -1, err`) and `Match` reports
    /// it as an error (:86-95). Only then are the results read in order: the
    /// first `false` skips the webhook (:111-119), otherwise any evaluation
    /// errors are reported as one aggregate error (:128-141).
    pub fn evaluate_with_context(
        &mut self,
        conditions: &[MatchCondition],
        context: &CELContext,
    ) -> MatchOutcome {
        // condition.go:103: `remainingBudget := runtimeCELCostBudget`.
        let mut remaining = RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS;
        let mut results: Vec<(&MatchCondition, Result<bool, String>)> = Vec::new();
        for cond in conditions {
            if cond.expression.trim().is_empty() {
                continue;
            }
            // Each expression runs under the per-call limit
            // (`environment/base.go:83`, `CostOptions::default`).
            let (value, cost) =
                self.inner
                    .evaluate_with_cost(&cond.expression, context, CostOptions::default());
            remaining = match charge_budget(None, cost, remaining) {
                Ok(r) => r,
                Err(e) => return MatchOutcome::Error(e.detail),
            };
            results.push((
                cond,
                match value {
                    Ok(cel::Value::Bool(b)) => Ok(b),
                    Ok(other) => Err(format!(
                        "matchCondition '{}' (expression: {}) failed: expression resulted in non-bool value: {other:?}",
                        cond.name, cond.expression
                    )),
                    Err(e) => Err(format!(
                        "matchCondition '{}' (expression: {}) failed: {}",
                        cond.name, cond.expression, e
                    )),
                },
            ));
        }

        let mut errors: Vec<String> = Vec::new();
        for (_, result) in results {
            match result {
                Err(e) => errors.push(e),
                // matcher.go:111-119: any false skips the webhook, always.
                Ok(false) => return MatchOutcome::NotMatched,
                Ok(true) => {}
            }
        }
        match errors.as_slice() {
            [] => MatchOutcome::Matched,
            // `utilerrors.NewAggregate(...).Error()`.
            [one] => MatchOutcome::Error(one.clone()),
            many => MatchOutcome::Error(format!("[{}]", many.join(", "))),
        }
    }
}

impl Default for MatchConditionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// Evaluator for `VAP.spec.validations[*]`.
///
/// One call to [`Self::evaluate_one`] returns whether the validation passed
/// and, when it failed, resolves the message via `messageExpression` (CEL) if
/// present, falling back to the static `message` field. This matches the
/// upstream `validator.go::Validate` path exactly.
pub struct ValidationEvaluator;

impl ValidationEvaluator {
    /// Evaluate a single `validations[*]` entry.
    ///
    /// * `expression` — required boolean CEL expression.
    /// * `message_expression` — optional CEL expression returning a string,
    ///   consulted only when `expression` returns false.
    /// * `static_message` — fallback message when `messageExpression` is
    ///   absent or errors.
    /// * `evaluator` — caller-owned `CELEvaluator` (shared so the program cache
    ///   spans the policy's validations, variables, and messageExpressions).
    /// * `context` — caller-built [`CELContext`] populated with
    ///   `object` / `oldObject` / `request` / `params` /
    ///   `variables` / `namespaceObject`.
    pub fn evaluate_one(
        expression: &str,
        message_expression: Option<&str>,
        static_message: Option<&str>,
        evaluator: &mut CELEvaluator,
        context: &CELContext,
    ) -> ValidationOutcome {
        Self::evaluate_one_with_message_context(
            expression,
            message_expression,
            static_message,
            evaluator,
            context,
            context,
        )
    }

    /// [`Self::evaluate_one`] with the `messageExpression` evaluated against
    /// its own activation: `compilePolicy` declares the messageExpressions
    /// with `HasAuthorizer: false` (plugin.go:119) and `Validate` binds them
    /// `OptionalVariableBindings{VersionedParams}` only (validator.go:105,
    /// 122), so `message_context` carries no `authorizer`.
    pub fn evaluate_one_with_message_context(
        expression: &str,
        message_expression: Option<&str>,
        static_message: Option<&str>,
        evaluator: &mut CELEvaluator,
        context: &CELContext,
        message_context: &CELContext,
    ) -> ValidationOutcome {
        let trimmed = expression.trim();
        if trimmed.is_empty() {
            // An empty expression cannot fail — upstream treats it as no-op (Pass).
            return ValidationOutcome::Pass;
        }

        match evaluator.evaluate(trimmed, context) {
            Ok(true) => ValidationOutcome::Pass,
            Ok(false) => {
                let message = resolve_message(
                    message_expression,
                    static_message,
                    evaluator,
                    message_context,
                );
                ValidationOutcome::Fail { message }
            }
            Err(e) => ValidationOutcome::Error {
                message: format!("validation expression '{}' failed: {}", trimmed, e),
            },
        }
    }
}

/// Evaluator for `VAP.spec.auditAnnotations[*]`.
///
/// Upstream returns `nil` to drop the annotation, otherwise produces a string.
/// Non-string / non-null results are rejected at admission registration time
/// in upstream — here we coerce them to their CEL `Debug` form to surface
/// misconfiguration rather than silently drop.
pub struct AuditAnnotationEvaluator;

impl AuditAnnotationEvaluator {
    /// Evaluate one `auditAnnotations[*]` entry.
    ///
    /// `key` is the annotation key (caller is expected to prefix with the
    /// policy name per upstream `validator.go`).
    pub fn evaluate_one(
        key: &str,
        value_expression: &str,
        evaluator: &mut CELEvaluator,
        context: &CELContext,
    ) -> AuditAnnotationOutcome {
        let trimmed = value_expression.trim();
        if trimmed.is_empty() {
            // No expression means nothing to emit — upstream treats as Skip.
            return AuditAnnotationOutcome::Skip;
        }

        match evaluator.evaluate_to_value(trimmed, context) {
            Ok(cel::objects::Value::Null) => AuditAnnotationOutcome::Skip,
            Ok(cel::objects::Value::String(s)) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: s.to_string(),
            },
            Ok(other) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: format!("{:?}", other),
            },
            Err(e) => AuditAnnotationOutcome::Error {
                message: format!(
                    "auditAnnotation '{}' valueExpression '{}' failed: {}",
                    key, trimmed, e
                ),
            },
        }
    }
}

/// What one `spec.validations[*]` entry carries into [`validate_budgeted`].
pub struct ValidationItem<'a> {
    pub expression: &'a str,
    pub message_expression: Option<&'a str>,
    pub message: Option<&'a str>,
}

/// The cost of each `spec.variables[*]` evaluation, charged lazily.
///
/// Upstream composes variables lazily (`cel/composition.go`
/// `variableAccessor.Callback`): a variable is evaluated, and its actual cost
/// reported (`reportCost`), the first time an expression resolves it, and
/// `evaluationActivation.Evaluate` (`activation.go:132-141`) charges the
/// accumulated cost (`GetAndResetCost`) against the filter's budget. Here
/// variables are evaluated eagerly, so the recorded cost is charged on the
/// first expression that references the variable (directly, or through
/// another variable's expression). Deviation: upstream's laziness also
/// follows `&&`/`||`/`?:` short-circuiting; this counts a textual reference.
#[derive(Default)]
pub struct VariableCosts {
    entries: std::collections::HashMap<String, (u64, Vec<String>)>,
    charged: std::collections::HashSet<String>,
}

fn referenced_variables(expression: &str) -> Vec<String> {
    let mut names = Vec::new();
    for (i, _) in expression.match_indices("variables.") {
        let rest = &expression[i + "variables.".len()..];
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        if end > 0 {
            names.push(rest[..end].to_string());
        }
    }
    names
}

impl VariableCosts {
    /// Record the actual cost of evaluating variable `name`.
    pub fn record(&mut self, name: &str, expression: &str, cost: u64) {
        self.entries
            .insert(name.to_string(), (cost, referenced_variables(expression)));
    }

    /// `compositionCtx.GetAndResetCost()` for `expression`: the cost of every
    /// not-yet-charged variable it reaches.
    pub fn composition_cost(&mut self, expression: &str) -> i64 {
        let mut total: i64 = 0;
        let mut pending = referenced_variables(expression);
        while let Some(name) = pending.pop() {
            if self.charged.contains(&name) {
                continue;
            }
            let Some((cost, deps)) = self.entries.get(&name) else {
                continue;
            };
            total = total.saturating_add(i64::try_from(*cost).unwrap_or(i64::MAX));
            pending.extend(deps.iter().cloned());
            self.charged.insert(name);
        }
        total
    }
}

/// `evaluationActivation.Evaluate` (`activation.go:132-189`) for one
/// expression: evaluate with cost tracking, then charge the composition and
/// actual cost against `remaining`. `Err` stops all further evaluation.
fn evaluate_charged(
    evaluator: &mut CELEvaluator,
    expression: &str,
    context: &CELContext,
    variables: &mut VariableCosts,
    remaining: i64,
) -> Result<(Result<cel::Value, String>, i64), BudgetError> {
    let (value, cost) = evaluator.evaluate_with_cost(expression, context, CostOptions::default());
    let remaining = charge_budget(
        Some(variables.composition_cost(expression)),
        cost,
        remaining,
    )?;
    Ok((
        value.map_err(|e| format!("expression '{expression}' resulted in error: {e}")),
        remaining,
    ))
}

/// Ported from `validating/validator.go` `Validate` (:73-218) over
/// `cel/condition.go` `ForInput` (:90-111): the validations share one
/// `RuntimeCELCostBudget` (`dispatcher.go:220`); the `messageExpression`s run
/// afterwards on what is left of it (:128-129), for every validation with one,
/// passing or not. Running out of budget in either stops evaluation:
///
/// * validations: `Err` is the one error decision replacing all others
///   (:108-119); the caller applies `failurePolicy` (`policyDecisionActionForError`).
/// * messageExpressions: every validation that did not itself error becomes
///   an error decision `failed messageExpression: <detail>` (:136-139).
///
/// A messageExpression that merely fails falls back to `message`, then to
/// `failed expression: <expression>` (:140-181).
pub fn validate_budgeted(
    items: &[ValidationItem<'_>],
    evaluator: &mut CELEvaluator,
    context: &CELContext,
    message_context: &CELContext,
    variables: &mut VariableCosts,
) -> Result<Vec<ValidationOutcome>, String> {
    let mut remaining = RUNTIME_CEL_COST_BUDGET;
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let (value, left) = evaluate_charged(
            evaluator,
            item.expression.trim(),
            context,
            variables,
            remaining,
        )
        .map_err(|e| e.detail)?;
        remaining = left;
        results.push(value);
    }

    // `messageFilter.ForInput` (:128): `err` is kept, not returned.
    let mut messages: Vec<Option<Result<cel::Value, String>>> = Vec::with_capacity(items.len());
    let mut message_budget_error: Option<String> = None;
    for item in items {
        let expr = item.message_expression.map(str::trim).unwrap_or("");
        if expr.is_empty() {
            messages.push(None);
            continue;
        }
        match evaluate_charged(evaluator, expr, message_context, variables, remaining) {
            Ok((value, left)) => {
                remaining = left;
                messages.push(Some(value));
            }
            Err(e) => {
                message_budget_error = Some(e.detail);
                break;
            }
        }
    }
    // `messageResults` is nil when `ForInput` returned an error.
    if message_budget_error.is_some() {
        messages.clear();
    }

    Ok(items
        .iter()
        .zip(results)
        .enumerate()
        .map(|(i, (item, result))| match result {
            Err(message) => ValidationOutcome::Error { message },
            Ok(_) if message_budget_error.is_some() => ValidationOutcome::Error {
                message: format!(
                    "failed messageExpression: {}",
                    message_budget_error.as_deref().unwrap_or_default()
                ),
            },
            Ok(cel::Value::Bool(true)) => ValidationOutcome::Pass,
            Ok(cel::Value::Bool(false)) => {
                let mut message = String::new();
                if let Some(Some(Ok(cel::Value::String(s)))) = messages.get(i) {
                    message = s.trim().to_string();
                    // validator.go:154-163: deny excessively long or multi-line.
                    if message.len() as i64 > MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES
                        || message.contains('\n')
                    {
                        message.clear();
                    }
                }
                if message.is_empty() {
                    message = item.message.unwrap_or("").trim().to_string();
                }
                if message.is_empty() {
                    message = format!("failed expression: {}", item.expression.trim());
                }
                ValidationOutcome::Fail { message }
            }
            Ok(other) => ValidationOutcome::Error {
                message: format!("CEL expression did not return a boolean: {other:?}"),
            },
        })
        .collect())
}

/// `auditAnnotationFilter.ForInput` (`validator.go:183-217`): the annotations
/// start from a fresh `RuntimeCELCostBudget` (:184 passes `runtimeCELCostBudget`,
/// not what the validations left). Running out of it is `Err`, the one error
/// decision that replaces the whole result (:185-195).
pub fn audit_annotations_budgeted(
    items: &[(&str, &str)],
    evaluator: &mut CELEvaluator,
    context: &CELContext,
    variables: &mut VariableCosts,
) -> Result<Vec<AuditAnnotationOutcome>, String> {
    let mut remaining = RUNTIME_CEL_COST_BUDGET;
    let mut out = Vec::with_capacity(items.len());
    for (key, expr) in items {
        let expr = expr.trim();
        let (value, left) = evaluate_charged(evaluator, expr, context, variables, remaining)
            .map_err(|e| e.detail)?;
        remaining = left;
        out.push(match value {
            Ok(cel::Value::Null) => AuditAnnotationOutcome::Skip,
            Ok(cel::Value::String(s)) if s.trim().is_empty() => AuditAnnotationOutcome::Skip,
            Ok(cel::Value::String(s)) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: s.trim().to_string(),
            },
            Ok(other) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: format!("{other:?}"),
            },
            Err(e) => AuditAnnotationOutcome::Error {
                message: format!("auditAnnotation '{key}' valueExpression '{expr}' failed: {e}"),
            },
        });
    }
    Ok(out)
}

/// The request authorizer a policy plugin evaluates with: the caching
/// authorizer the dispatcher hands every `Validate` (dispatcher.go:117, 221)
/// and the request's user (`versionedAttr.GetUserInfo()`, activation.go:53-54).
#[derive(Clone)]
pub struct VapAuthorizer {
    pub user: rusternetes_common::auth::UserInfo,
    pub authorizer: std::sync::Arc<dyn rusternetes_common::authz::Authorizer>,
    /// `versionedAttr.GetSubresource()`.
    pub subresource: String,
}

/// Bind `authorizer` and `authorizer.requestResource` (activation.go:52-55):
/// `NewAuthorizerVal(versionedAttr.GetUserInfo(), inputs.Authorizer)` and
/// `NewResourceAuthorizerVal(...)`, whose `Resource` is the request's own
/// resource (`GetResource().Group`, `.Resource`, `GetSubresource`,
/// `GetNamespace`, `GetName`).
pub fn bind_authorizer(
    context: &mut CELContext,
    authz: &VapAuthorizer,
    group: &str,
    resource: &str,
    namespace: &str,
    name: &str,
) {
    use rusternetes_common::cel_authz_library as lib;
    context.add_variable(
        lib::AUTHORIZER_VAR.to_string(),
        lib::new_authorizer_val(&authz.user, authz.authorizer.clone()),
    );
    context.add_variable(
        lib::REQUEST_RESOURCE_AUTHORIZER_VAR.to_string(),
        lib::new_resource_authorizer_val(
            &authz.user,
            authz.authorizer.clone(),
            group,
            resource,
            &authz.subresource,
            namespace,
            name,
        ),
    );
}

/// Build a CEL activation context with the standard VAP variables:
/// `object`, `oldObject`, `request`, and `params`.
///
/// Mirrors upstream `admission/plugin/cel/compile.go`'s activation. Callers
/// that need `variables` or `namespaceObject` should call
/// [`CELContext::add_variable`] / [`CELContext::add_json_variable`] on the
/// returned context. `authorizer` is layered on with [`bind_authorizer`] (no
/// request authorizer reaches this function).
pub fn build_context(
    request: &AdmissionRequest,
    params: Option<&Value>,
) -> Result<CELContext, anyhow::Error> {
    let mut context = CELContext::new();

    // object
    context.add_json_variable("object", &request.object)?;

    // oldObject — null on non-UPDATE
    let old = request.old_object.clone().unwrap_or(Value::Null);
    context.add_json_variable("oldObject", &old)?;

    // request — slim AdmissionRequest projection that mirrors upstream
    // `admission/plugin/cel/request.go` for the fields VAP / matchCondition
    // expressions actually use in the wild (operation/kind/namespace/name/userInfo).
    let op_str = operation_as_str(&request.operation);
    let request_val = serde_json::json!({
        "operation": op_str,
        "kind": {
            "group": request.group,
            "version": request.version,
            "kind": request.kind,
        },
        "namespace": request.namespace.clone().unwrap_or_default(),
        "name": request.name,
        "userInfo": {
            "username": request.user_info.username,
            "uid": request.user_info.uid,
            "groups": request.user_info.groups,
        },
    });
    context.add_json_variable("request", &request_val)?;

    // params — null when the binding has no paramRef or the param resource
    // wasn't found in storage. Matches upstream behaviour.
    let params_val = params.cloned().unwrap_or(Value::Null);
    context.add_json_variable("params", &params_val)?;

    Ok(context)
}

fn operation_as_str(op: &rusternetes_common::admission::Operation) -> &'static str {
    use rusternetes_common::admission::Operation;
    match op {
        Operation::Create => "CREATE",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
        Operation::Connect => "CONNECT",
    }
}

/// Resolve the rejection message for a failing validation, preferring
/// `messageExpression` (CEL → string) over the static `message`. Mirrors
/// upstream `validator.go::Validate` exactly: if the CEL message expression
/// errors or returns a non-string, fall back to the static message.
fn resolve_message(
    message_expression: Option<&str>,
    static_message: Option<&str>,
    evaluator: &mut CELEvaluator,
    context: &CELContext,
) -> String {
    if let Some(expr) = message_expression {
        let trimmed = expr.trim();
        if !trimmed.is_empty() {
            if let Ok(cel::objects::Value::String(s)) =
                evaluator.evaluate_to_value(trimmed, context)
            {
                return s.to_string();
            }
        }
    }
    static_message.unwrap_or("Validation failed").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::{Operation, UserInfo};

    fn req(namespace: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            operation: Operation::Create,
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: namespace.map(|s| s.to_string()),
            name: "test-cm".to_string(),
            object: serde_json::json!({
                "metadata": {
                    "name": "test-cm",
                    "namespace": namespace.unwrap_or(""),
                },
            }),
            old_object: None,
            user_info: UserInfo {
                username: "alice".to_string(),
                uid: "uid-1".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        }
    }

    #[test]
    fn empty_conditions_match() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        assert_eq!(e.evaluate(&[], &r, None), MatchOutcome::Matched);
    }

    #[test]
    fn skips_blank_expression() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        let cond = MatchCondition {
            name: "blank".to_string(),
            expression: "   ".to_string(),
        };
        assert_eq!(e.evaluate(&[cond], &r, None), MatchOutcome::Matched);
    }

    #[test]
    fn errors_surface_with_condition_name() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        let cond = MatchCondition {
            name: "bogus".to_string(),
            // not a boolean expression — should yield an Error outcome
            expression: "object.metadata.name".to_string(),
        };
        match e.evaluate(&[cond], &r, None) {
            MatchOutcome::Error(msg) => assert!(msg.contains("bogus"), "msg: {}", msg),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn request_kind_carries_group_and_version() {
        let mut e = MatchConditionEvaluator::new();
        let mut r = req(Some("ns"));
        r.group = "apps".to_string();
        r.version = "v1".to_string();
        r.kind = "Deployment".to_string();

        let cond = MatchCondition {
            name: "gvk-matches".to_string(),
            expression: "request.kind.group == 'apps' && request.kind.version == 'v1' \
                    && request.kind.kind == 'Deployment'"
                .to_string(),
        };
        assert_eq!(e.evaluate(&[cond], &r, None), MatchOutcome::Matched);
    }

    // ===== ValidationEvaluator =====

    #[test]
    fn validation_pass() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'test-cm'",
            None,
            None,
            &mut ev,
            &ctx,
        );
        assert_eq!(outcome, ValidationOutcome::Pass);
    }

    #[test]
    fn validation_fail_uses_static_message_when_no_message_expression() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            None,
            Some("name must be 'nope'"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "name must be 'nope'".to_string()
            }
        );
    }

    #[test]
    fn validation_fail_uses_message_expression_when_present() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            Some("'got name: ' + object.metadata.name"),
            Some("fallback"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "got name: test-cm".to_string()
            }
        );
    }

    #[test]
    fn validation_fail_falls_back_when_message_expression_errors() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            Some("this @@ is bogus"),
            Some("fallback used"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "fallback used".to_string()
            }
        );
    }

    #[test]
    fn validation_error_on_compile_failure() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "this is not valid CEL @@",
            None,
            None,
            &mut ev,
            &ctx,
        );
        match outcome {
            ValidationOutcome::Error { message } => {
                assert!(
                    message.contains("this is not valid CEL"),
                    "msg: {}",
                    message
                )
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn validation_empty_expression_is_pass() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        assert_eq!(
            ValidationEvaluator::evaluate_one("   ", None, None, &mut ev, &ctx),
            ValidationOutcome::Pass
        );
    }

    // ===== AuditAnnotationEvaluator =====

    #[test]
    fn audit_annotation_emits_string() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome =
            AuditAnnotationEvaluator::evaluate_one("name", "object.metadata.name", &mut ev, &ctx);
        assert_eq!(
            outcome,
            AuditAnnotationOutcome::Emit {
                key: "name".to_string(),
                value: "test-cm".to_string(),
            }
        );
    }

    #[test]
    fn audit_annotation_null_skips() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        // CEL expression returning null
        let outcome = AuditAnnotationEvaluator::evaluate_one("only", "null", &mut ev, &ctx);
        assert_eq!(outcome, AuditAnnotationOutcome::Skip);
    }

    #[test]
    fn audit_annotation_error_on_compile_failure() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome =
            AuditAnnotationEvaluator::evaluate_one("bad", "this @@ is bogus", &mut ev, &ctx);
        match outcome {
            AuditAnnotationOutcome::Error { message } => {
                assert!(message.contains("bad"), "msg: {}", message)
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn audit_annotation_empty_expression_skips() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        assert_eq!(
            AuditAnnotationEvaluator::evaluate_one("k", "   ", &mut ev, &ctx),
            AuditAnnotationOutcome::Skip
        );
    }
}
