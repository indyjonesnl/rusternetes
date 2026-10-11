//! `matchConditions` of an authorization webhook (#3161).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/authorization/cel/`
//! (`matcher.go` `CELMatcher.Eval`, `compile.go` `convertObjectToUnstructured`)
//! and `pkg/apis/apiserver/validation/validation.go` `compileMatchConditions`
//! :778-820. Selector attributes are not carried by `RequestAttributes`, so
//! `fieldSelector` / `labelSelector` are never populated.
//!
//! Deliberate deviation: upstream compiles against a typed CEL environment and
//! rejects non-bool expressions at compile time (`compile.go` :95-110);
//! Rusternetes' CEL has no typed environment (see
//! `rusternetes_common::cel::compile_failure`), so a non-bool result is
//! reported at evaluation time, which `Eval` already treats as an error.

use rusternetes_common::cel::{compile_failure, CELContext, CELEvaluator};
use std::collections::HashSet;
use std::sync::Mutex;

/// `apiserver.WebhookMatchCondition`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookMatchCondition {
    pub expression: String,
}

/// `authorizationcel.CELMatcher`.
pub struct CelMatcher {
    expressions: Vec<String>,
    evaluator: Mutex<CELEvaluator>,
    /// `CELMatcher.AuthorizerType` / `AuthorizerName`; metrics are reported
    /// only when set (matcher.go:35-42, :51-54).
    metrics_identity: Option<(String, String)>,
}

impl std::fmt::Debug for CelMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CelMatcher")
            .field("expressions", &self.expressions)
            .finish()
    }
}

impl CelMatcher {
    /// `compileMatchConditions` (validation.go:778-820); `Ok(None)` when no
    /// condition remains. Errors carry upstream's `field.ErrorType` wording.
    pub fn compile(conditions: &[WebhookMatchCondition]) -> Result<Option<Self>, Vec<String>> {
        let mut errs = Vec::new();
        if conditions.len() > 64 {
            errs.push(format!(
                "matchConditions: Too many: {}: must have at most 64 items",
                conditions.len()
            ));
            return Err(errs);
        }
        let mut seen = HashSet::new();
        let mut expressions = Vec::new();
        for (i, c) in conditions.iter().enumerate() {
            let path = format!("matchConditions[{i}].expression");
            if c.expression.trim().is_empty() {
                errs.push(format!("{path}: Required value"));
                continue;
            }
            if !seen.insert(c.expression.as_str()) {
                errs.push(format!("{path}: Duplicate value: {:?}", c.expression));
                continue;
            }
            if let Some(e) = compile_failure(&c.expression) {
                errs.push(format!("{path}: Invalid value: {:?}: {e}", c.expression));
                continue;
            }
            expressions.push(c.expression.clone());
        }
        if !errs.is_empty() {
            return Err(errs);
        }
        if expressions.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            expressions,
            evaluator: Mutex::new(CELEvaluator::new()),
            metrics_identity: None,
        }))
    }

    /// `cm.AuthorizerType = "Webhook"; cm.AuthorizerName = name; cm.Metrics = am`
    /// (webhook.go:123-126).
    pub fn with_metrics(mut self, authorizer_type: &str, authorizer_name: &str) -> Self {
        self.metrics_identity = Some((authorizer_type.to_string(), authorizer_name.to_string()));
        self
    }

    /// `CELMatcher.Eval` (matcher.go:46-93): `request` is the
    /// `SubjectAccessReviewSpec` bound as the CEL variable `request`. A FALSE
    /// returns at once (even after an earlier error); otherwise any error fails
    /// the evaluation; otherwise true.
    pub fn eval(&self, request: &serde_json::Value) -> Result<bool, String> {
        let start = std::time::Instant::now();
        let out = self.eval_inner(request);
        if let Some((t, n)) = &self.metrics_identity {
            // matcher.go:51-82: duration always, failure when any expression
            // errored (even if a later FALSE excluded), exclusion on FALSE.
            crate::authorization_webhook_metrics::record_match_condition_evaluation(
                t,
                n,
                start.elapsed(),
            );
            match &out {
                Ok(false) => {
                    crate::authorization_webhook_metrics::record_match_condition_exclusion(t, n)
                }
                Err(_) => {
                    crate::authorization_webhook_metrics::record_match_condition_failure(t, n)
                }
                Ok(true) => {}
            }
        }
        out
    }

    fn eval_inner(&self, request: &serde_json::Value) -> Result<bool, String> {
        let mut ctx = CELContext::new();
        ctx.add_json_variable("request", request)
            .map_err(|e| e.to_string())?;
        let mut errors = Vec::new();
        let mut ev = self.evaluator.lock().unwrap_or_else(|p| p.into_inner());
        for expr in &self.expressions {
            match ev.evaluate(expr, &ctx) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(e) => errors.push(format!(
                    "cel evaluation error: expression '{expr}' resulted in error: {e}"
                )),
            }
        }
        if errors.is_empty() {
            Ok(true)
        } else {
            Err(errors.join("; "))
        }
    }
}
