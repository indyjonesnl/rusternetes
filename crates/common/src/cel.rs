// CEL (Common Expression Language) evaluation engine
//
// This module provides CEL expression evaluation for ValidatingAdmissionPolicy

use crate::cel_struct::{self, CompiledExpression};
use anyhow::{anyhow, Result};
use cel::{
    objects::{Key, Map, OptionalValue},
    Value,
};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

/// CELEvaluator evaluates CEL expressions with given context
pub struct CELEvaluator {
    // Cache compiled programs for performance
    program_cache: HashMap<String, CompiledExpression>,
}

impl CELEvaluator {
    /// Create a new CEL evaluator
    pub fn new() -> Self {
        Self {
            program_cache: HashMap::new(),
        }
    }

    /// Compile (or fetch from the cache) `expression`. `Object{}` /
    /// `JSONPatch{}` initializers are rewritten to maps; see [`cel_struct`].
    fn compiled(&mut self, expression: &str) -> Result<&CompiledExpression> {
        if !self.program_cache.contains_key(expression) {
            let prog = cel_struct::compile(expression)
                .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
            self.program_cache.insert(expression.to_string(), prog);
        }
        Ok(&self.program_cache[expression])
    }

    /// Evaluate a CEL expression with the given context
    ///
    /// # Arguments
    /// * `expression` - The CEL expression to evaluate
    /// * `context` - Variables to make available in the CEL environment
    ///
    /// # Returns
    /// The result of the evaluation as a boolean, or an error
    pub fn evaluate(&mut self, expression: &str, context: &CELContext) -> Result<bool> {
        // Get or compile the program
        let program = self.compiled(expression)?;

        // Create CEL context
        let mut cel_context = cel_struct::new_context();

        // Add variables to context
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        // Execute the program
        let result = cel_struct::execute(program, &cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))?;

        // Convert result to boolean
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!(
                "CEL expression did not return a boolean: {:?}",
                result
            )),
        }
    }

    /// Evaluate a CEL expression that returns a string (for messages, audit annotations, etc.)
    pub fn evaluate_string(&mut self, expression: &str, context: &CELContext) -> Result<String> {
        // Get or compile the program
        let program = self.compiled(expression)?;

        // Create CEL context
        let mut cel_context = cel_struct::new_context();

        // Add variables to context
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        // Execute the program
        let result = cel_struct::execute(program, &cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))?;

        // Convert result to string
        match result {
            Value::String(s) => Ok(s.to_string()),
            Value::Int(i) => Ok(i.to_string()),
            Value::UInt(u) => Ok(u.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            _ => Ok(format!("{:?}", result)),
        }
    }

    /// Evaluate a CEL expression and return the raw Value (for VAP variables)
    pub fn evaluate_to_value(&mut self, expression: &str, context: &CELContext) -> Result<Value> {
        let program = self.compiled(expression)?;

        let mut cel_context = cel_struct::new_context();
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        cel_struct::execute(program, &cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))
    }

    /// Like [`Self::evaluate_to_value`] but keeps the `Object{}` type
    /// markers, for values that are bound into another evaluation.
    pub fn evaluate_to_value_raw(
        &mut self,
        expression: &str,
        context: &CELContext,
    ) -> Result<Value> {
        let program = self.compiled(expression)?;
        let mut cel_context = cel_struct::new_context();
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }
        cel_struct::execute_raw(program, &cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))
    }

    /// Evaluate with the cel-go runtime cost tracker (`cel.CostTracking`),
    /// returning the result and `EvalDetails.ActualCost()` (#3323). See
    /// [`crate::cel_cost`].
    pub fn evaluate_with_cost(
        &mut self,
        expression: &str,
        context: &CELContext,
        opts: crate::cel_cost::CostOptions,
    ) -> (
        std::result::Result<Value, crate::cel_cost::CostEvalError>,
        u64,
    ) {
        let (r, cost) = self.evaluate_with_cost_raw(expression, context, opts);
        (r.map(|v| cel_struct::strip_type_markers(&v)), cost)
    }

    /// [`Self::evaluate_with_cost`] keeping the `Object{}` type markers (for
    /// values bound into another evaluation, or checked by the caller).
    pub fn evaluate_with_cost_raw(
        &mut self,
        expression: &str,
        context: &CELContext,
        opts: crate::cel_cost::CostOptions,
    ) -> (
        std::result::Result<Value, crate::cel_cost::CostEvalError>,
        u64,
    ) {
        let program = match self.compiled(expression) {
            Ok(p) => p,
            Err(e) => {
                return (
                    Err(crate::cel_cost::CostEvalError::Exec(
                        cel::ExecutionError::function_error("compile", e.to_string()),
                    )),
                    0,
                )
            }
        };
        let mut cel_context = cel_struct::new_context();
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }
        crate::cel_cost::eval_with_cost(program.expression(), &cel_context, opts)
    }

    /// Evaluate a MAP patch expression and run upstream's
    /// `CheckTypeNamesMatchFieldPathNames` on the result; a mismatch is
    /// `unexpected type name ...` (the caller adds `type mismatch: `).
    pub fn evaluate_to_value_checked(
        &mut self,
        expression: &str,
        context: &CELContext,
        mode: cel_struct::TypeCheck,
    ) -> Result<Value> {
        let program = self.compiled(expression)?;
        let mut cel_context = cel_struct::new_context();
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }
        cel_struct::execute_checked(program, &cel_context, mode).map_err(|e| {
            if e.starts_with("unexpected type name") {
                anyhow!("type mismatch: {e}")
            } else {
                anyhow!("Failed to execute CEL expression '{}': {}", expression, e)
            }
        })
    }

    /// Type-check a CEL expression without executing it
    pub fn type_check(&mut self, expression: &str) -> Result<()> {
        cel_struct::compile(expression)
            .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
        Ok(())
    }
}

impl Default for CELEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// A CEL error that comes from a declaration this server does not supply,
/// rather than from the expression being malformed.
fn is_missing_declaration(err: &str) -> bool {
    let err = err.to_lowercase();
    err.contains("no such key")
        || err.contains("not found")
        || err.contains("undeclared")
        || err.contains("undefined")
        || err.contains("no matching overload")
}

/// Why `expression` does not compile, if it does not.
///
/// Upstream compiles against a typed CEL environment
/// (`validateMatchConditionsExpression`, validation.go:1100). Rusternetes has
/// no typed environment, so this compiles with the plain `cel` crate and
/// tolerates the errors that come from the missing declarations rather than
/// from the expression itself.
pub fn compile_failure(expression: &str) -> Option<String> {
    compile_failure_in(expression, true)
}

/// As [`compile_failure`], but only parses. The MutatingAdmissionPolicy
/// expressions use `Object{...}` / `JSONPatch{...}` struct initializers, which
/// the `cel` crate parses but panics on when executed ("Support structs!"),
/// so the execute step that catches webhook match-condition errors cannot be
/// applied to them.
pub fn parse_failure(expression: &str) -> Option<String> {
    compile_failure_in(expression, false)
}

fn compile_failure_in(expression: &str, execute: bool) -> Option<String> {
    // The antlr4rust parser panics on some invalid expressions instead of
    // returning `Err`.
    let source = expression.to_string();
    let program = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::Program::compile(&source)
    })) {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            // The CEL crate type-checks at compile time and rejects references
            // to declarations we do not supply (`object.metadata`), which are
            // valid at admission time.
            return (!is_missing_declaration(&e.to_string()))
                .then(|| format!("compilation failed: {e}"));
        }
        Err(_) => {
            return Some(format!(
                "compilation failed: invalid CEL expression '{expression}'"
            ))
        }
    };

    if !execute {
        return None;
    }

    // The CEL crate's parser accepts some expressions Kubernetes rejects.
    // Executing with an empty context catches the genuinely invalid ones.
    let ctx = cel::Context::default();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| program.execute(&ctx))) {
        Ok(Ok(_)) => None,
        Ok(Err(e)) => {
            (!is_missing_declaration(&e.to_string())).then(|| format!("compilation failed: {e}"))
        }
        Err(_) => Some(format!(
            "compilation failed: invalid CEL expression '{expression}'"
        )),
    }
}

/// CELContext holds variables available to CEL expressions
#[derive(Debug, Clone)]
pub struct CELContext {
    pub variables: HashMap<String, Value>,
}

impl CELContext {
    /// Create a new empty CEL context
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Create a CEL context for admission control
    ///
    /// Provides standard variables:
    /// - `object`: The object being created/updated/deleted
    /// - `oldObject`: The existing object (for UPDATE operations)
    /// - `request`: The admission request
    /// - `params`: Parameters from the binding
    pub fn for_admission(
        object: &serde_json::Value,
        old_object: Option<&serde_json::Value>,
        params: Option<&serde_json::Value>,
    ) -> Result<Self> {
        let mut context = Self::new();

        // Add object
        context.add_json_variable("object", object)?;

        // Add oldObject if present
        if let Some(old) = old_object {
            context.add_json_variable("oldObject", old)?;
        }

        // Add params if present
        if let Some(p) = params {
            context.add_json_variable("params", p)?;
        }

        // Add request metadata (simplified for now)
        let request = serde_json::json!({
            "operation": "CREATE", // This should be dynamic
            "userInfo": {},
            "namespace": "",
        });
        context.add_json_variable("request", &request)?;

        Ok(context)
    }

    /// Add a variable to the context
    pub fn add_variable(&mut self, name: String, value: Value) {
        self.variables.insert(name, value);
    }

    /// Bind `name` to a CEL optional: `optional.of(value)` when `value` is
    /// `Some`, else `optional.none()`. Port of
    /// `validationActivationWithOldSelf`'s `optionalOldSelfActivation`
    /// (`apiextensions-apiserver/pkg/apiserver/schema/cel/validation.go:776-793`,
    /// `types.OptionalNone` / `types.OptionalOf`).
    pub fn add_optional_json_variable(
        &mut self,
        name: &str,
        value: Option<&serde_json::Value>,
    ) -> Result<()> {
        let opt = match value {
            Some(v) => OptionalValue::of(json_to_cel_value(v)?),
            None => OptionalValue::none(),
        };
        self.variables
            .insert(name.to_string(), Value::Opaque(Arc::new(opt)));
        Ok(())
    }

    /// Add a JSON value as a variable (converts to CEL Value)
    pub fn add_json_variable(&mut self, name: &str, value: &serde_json::Value) -> Result<()> {
        let cel_value = json_to_cel_value(value)?;
        self.variables.insert(name.to_string(), cel_value);
        Ok(())
    }
}

impl Default for CELContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a serde_json::Value to a CEL Value
pub fn json_to_cel_value(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::UInt(u))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(anyhow!("Invalid number: {}", n))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(std::sync::Arc::new(s.clone()))),
        serde_json::Value::Array(arr) => {
            let cel_values: Result<Vec<Value>> = arr.iter().map(json_to_cel_value).collect();
            Ok(Value::List(Arc::new(cel_values?)))
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                let key = Key::String(Arc::new(k.clone()));
                map.insert(key, json_to_cel_value(v)?);
            }
            Ok(Value::Map(Map { map: Arc::new(map) }))
        }
    }
}
