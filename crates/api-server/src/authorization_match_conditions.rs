//! `matchConditions` of an authorization webhook (#3161).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/authorization/cel/`
//! (`matcher.go` `CELMatcher.Eval`, `compile.go` `convertObjectToUnstructured`)
//! and `pkg/apis/apiserver/validation/validation.go` `compileMatchConditions`
//! :778-820. Selector attributes are not carried by `RequestAttributes`, so
//! `fieldSelector` / `labelSelector` are never populated.
//!
//! Upstream compiles against a typed CEL environment. Rusternetes' CEL has no
//! typed environment (see `rusternetes_common::cel::compile_failure`), so the
//! compile-time output-type check (`compile.go` :189-203, `ReturnTypes` is
//! `[bool]`) covers only the expression shapes whose type is known without
//! inference (`rusternetes_common::cel_env::output_type_failure`); any other
//! non-bool result is still reported at evaluation, which `Eval` treats as an
//! error.

use rusternetes_common::cel::{compile_failure, CELContext, CELEvaluator};
use rusternetes_common::cel_env::{output_type_failure, ExpectedOutput};
use rusternetes_common::validation::field::{BadValue, Error as FieldError, ErrorList, Path};
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
        let (matcher, errs) = Self::compile_at(conditions, &Path::default());
        if errs.is_empty() {
            Ok(matcher)
        } else {
            Err(errs.iter().map(|e| e.to_string()).collect())
        }
    }

    /// `compileMatchConditions` with its `fldPath` (validation.go:778-821):
    /// the matcher built from the conditions that compiled, plus every field
    /// error, exactly as upstream returns both. `ValidateWebhookConfiguration`
    /// passes the authorizer's path (:766).
    pub fn compile_at(
        conditions: &[WebhookMatchCondition],
        fld_path: &Path,
    ) -> (Option<Self>, ErrorList) {
        let mut errs = ErrorList::new();
        let match_conditions = fld_path.child("matchConditions");
        if conditions.len() > 64 {
            // `field.TooMany(fldPath.Child("matchConditions"), len, 64)`
            // (validation.go:785) carries the actual quantity as the bad value
            // (errors.go:294-302); `Error::too_many` has no such parameter.
            let mut e = FieldError::too_many(&match_conditions, 64);
            e.bad_value = BadValue::I64(conditions.len() as i64);
            errs.push(e);
            return (None, errs);
        }
        let mut seen = HashSet::new();
        let mut expressions = Vec::new();
        for (i, c) in conditions.iter().enumerate() {
            let path = match_conditions.index(i).child("expression");
            if c.expression.trim().is_empty() {
                errs.push(FieldError::required(&path, ""));
                continue;
            }
            if !seen.insert(c.expression.as_str()) {
                errs.push(FieldError::duplicate(&path, c.expression.as_str()));
                continue;
            }
            if let Some(e) = compile_failure(&c.expression)
                .or_else(|| output_type_failure(&c.expression, ExpectedOutput::Bool))
            {
                errs.push(FieldError::invalid(&path, c.expression.as_str(), e));
                continue;
            }
            expressions.push(c.expression.clone());
        }
        if expressions.is_empty() {
            return (None, errs);
        }
        (
            Some(Self {
                expressions,
                evaluator: Mutex::new(CELEvaluator::new()),
                metrics_identity: None,
            }),
            errs,
        )
    }

    /// `CELMatcher.UsesFieldSelector`.
    pub fn uses_field_selector(&self) -> bool {
        false
    }

    /// `CELMatcher.UsesLabelSelector`.
    pub fn uses_label_selector(&self) -> bool {
        false
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
