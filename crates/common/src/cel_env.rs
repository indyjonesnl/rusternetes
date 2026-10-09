//! The declaration half of the typed CEL environment a MutatingAdmissionPolicy
//! is compiled against.
//!
//! Upstream compiles every MAP expression with cel-go's checker in the
//! environment built by `createEnvForOpts`
//! (`staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/compile.go:251-291`):
//! `object`, `oldObject`, `namespaceObject`, `request`, `params` (only when
//! `HasParams`), `authorizer`, the composited `variables`, and, for
//! `HasPatchTypes`, the `Object` / `Object.<field>` / `JSONPatch` types of
//! `mutation.DynamicTypeResolver` (`pkg/cel/mutation/typeresolver.go:32-42`).
//! `CompileCELExpression` (compile.go:166-229) then rejects an expression that
//! does not check, or whose output type is not one of
//! `ExpressionAccessor.ReturnTypes()` (`Object` for `applyConfiguration`,
//! `list(JSONPatch)` for `jsonPatch`: smd.go:52, json_patch.go:57).
//!
//! The `cel` crate has no checker and no type declarations: it parses to an
//! AST and resolves names only at execution. So the type *inference* of
//! cel-go cannot be ported. What the AST does support, soundly, is the part
//! of the checker that needs no inference, which is what this module does:
//!
//! * identifier resolution against the declared variables (`undeclared
//!   reference to 'x' (in container '')`), with macro-bound variables scoped
//!   as cel-go scopes comprehension variables;
//! * `variables.<name>` against the composited variables' fields
//!   (`undefined field 'name'`);
//! * struct-literal type names against the type resolver (`Object`,
//!   `Object.<path>`, `JSONPatch`), and the fixed fields of `JSONPatch`
//!   (`op`, `from`, `path`, `value`: `pkg/cel/mutation/jsonpatch.go:30-33`);
//! * the output-type check, only for the expression shapes whose type is
//!   known without inference (a scalar literal, a map literal, a comparison or
//!   logical operator).
//!
//! Not modelled: calls are not resolved (the Kubernetes function libraries are
//! not declared here), field types and overloads are not checked, and
//! `environment.StoredExpressions` versus `NewExpressions` only selects which
//! library versions are enabled, which the crate has no notion of. Positions
//! (`<input>:1:5`) are not reported because the crate drops its source info.

use std::collections::BTreeSet;

use cel::common::ast::{EntryExpr, Expr, IdedExpr, LiteralValue};

/// `plugincel.OptionalVariableDeclarations` plus the composited `variables`.
#[derive(Debug, Clone, Default)]
pub struct MutationEnv {
    /// `HasParams`: `spec.paramKind != nil`.
    pub has_params: bool,
    /// `HasPatchTypes`: the `Object` / `JSONPatch` type resolver.
    pub has_patch_types: bool,
    /// The fields of the composited `variables` object; `None` when the
    /// environment does not declare `variables` (the stateless compiler
    /// `validateMatchConditionsExpression` uses, validation.go:1108).
    pub variables: Option<BTreeSet<String>>,
}

/// `ExpressionAccessor.ReturnTypes()` of the compiled expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedOutput {
    /// No constraint (match conditions, variables).
    Any,
    /// `ApplyConfigurationCondition` (smd.go:52).
    Object,
    /// `JSONPatchCondition` (json_patch.go:57).
    JsonPatchList,
}

/// `variables` declared in every environment that composes them.
const VARIABLES: &str = "variables";

/// Names declared by `createEnvForOpts` apart from `params` / `variables`,
/// and the CEL type identifiers every environment declares.
const DECLARED: &[&str] = &[
    "object",
    "oldObject",
    "namespaceObject",
    "request",
    "authorizer",
    "int",
    "uint",
    "double",
    "bool",
    "string",
    "bytes",
    "list",
    "map",
    "null_type",
    "type",
    "dyn",
    "optional_type",
];

/// The fields of `JSONPatch` (`pkg/cel/mutation/jsonpatch.go:30-33`).
const JSON_PATCH_FIELDS: &[&str] = &["op", "from", "path", "value"];

/// Why `expression` does not check in `env`, as the `Detail` of the upstream
/// compilation error (`compilation failed: ...` / `must evaluate to ...`), or
/// `None`. A syntax error is reported first, as `env.Compile` does.
pub fn mutation_env_failure(
    expression: &str,
    env: &MutationEnv,
    expected: ExpectedOutput,
) -> Option<String> {
    let source = expression.to_string();
    let program = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::Program::compile(&source)
    })) {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => return Some(format!("compilation failed: {e}")),
        Err(_) => {
            return Some(format!(
                "compilation failed: invalid CEL expression '{expression}'"
            ))
        }
    };
    let root = program.expression();
    let mut checker = Checker {
        env,
        scope: Vec::new(),
    };
    if let Err(issue) = checker.check(root) {
        return Some(format!("compilation failed: ERROR: <input>: {issue}"));
    }
    output_mismatch(root, expected)
}

struct Checker<'a> {
    env: &'a MutationEnv,
    /// Comprehension variables in scope, innermost last.
    scope: Vec<String>,
}

impl Checker<'_> {
    fn declared(&self, name: &str) -> bool {
        self.scope.iter().any(|s| s == name)
            || DECLARED.contains(&name)
            || (name == "params" && self.env.has_params)
            || (name == VARIABLES && self.env.variables.is_some())
    }

    fn check(&mut self, e: &IdedExpr) -> Result<(), String> {
        match &e.expr {
            Expr::Unspecified | Expr::Literal(_) => Ok(()),
            Expr::Ident(name) => {
                if self.declared(name) {
                    Ok(())
                } else {
                    Err(format!(
                        "undeclared reference to '{name}' (in container '')"
                    ))
                }
            }
            Expr::Select(s) => {
                self.check(&s.operand)?;
                // `variables` is an object type whose fields are the
                // compiled variables (CompositionEnv.AddField, composition.go:126).
                if let (Expr::Ident(op), Some(fields)) = (&s.operand.expr, &self.env.variables) {
                    if op == VARIABLES
                        && !self.scope.iter().any(|v| v == VARIABLES)
                        && !fields.contains(&s.field)
                    {
                        return Err(format!("undefined field '{}'", s.field));
                    }
                }
                Ok(())
            }
            Expr::Call(c) => {
                if let Some(t) = &c.target {
                    self.check(t)?;
                }
                c.args.iter().try_for_each(|a| self.check(a))
            }
            Expr::List(l) => l.elements.iter().try_for_each(|a| self.check(a)),
            Expr::Map(m) => m.entries.iter().try_for_each(|en| self.check_entry(en)),
            Expr::Struct(s) => {
                let name = s.type_name.as_str();
                let is_object = name == "Object" || name.starts_with("Object.");
                let is_patch = name == "JSONPatch";
                // `DynamicTypeResolver.Resolve` (typeresolver.go:35-42), only
                // present when `HasPatchTypes`.
                if !self.env.has_patch_types || !(is_object || is_patch) {
                    return Err(format!(
                        "undeclared reference to '{name}' (in container '')"
                    ));
                }
                for entry in &s.entries {
                    if let EntryExpr::StructField(f) = &entry.expr {
                        if is_patch && !JSON_PATCH_FIELDS.contains(&f.field.as_str()) {
                            return Err(format!("undefined field '{}'", f.field));
                        }
                    }
                    self.check_entry(entry)?;
                }
                Ok(())
            }
            Expr::Comprehension(c) => {
                self.check(&c.iter_range)?;
                self.check(&c.accu_init)?;
                let depth = self.scope.len();
                self.scope.push(c.iter_var.clone());
                if let Some(v2) = &c.iter_var2 {
                    self.scope.push(v2.clone());
                }
                self.scope.push(c.accu_var.clone());
                let r = self
                    .check(&c.loop_cond)
                    .and_then(|_| self.check(&c.loop_step))
                    .and_then(|_| self.check(&c.result));
                self.scope.truncate(depth);
                r
            }
        }
    }

    fn check_entry(&mut self, en: &cel::common::ast::IdedEntryExpr) -> Result<(), String> {
        match &en.expr {
            EntryExpr::StructField(f) => self.check(&f.value),
            EntryExpr::MapEntry(m) => self.check(&m.key).and_then(|_| self.check(&m.value)),
        }
    }
}

/// `compile.go:190-203`, for the shapes whose type needs no inference.
fn output_mismatch(root: &IdedExpr, expected: ExpectedOutput) -> Option<String> {
    let want = match expected {
        ExpectedOutput::Any => return None,
        ExpectedOutput::Object => "Object",
        ExpectedOutput::JsonPatchList => "list(JSONPatch)",
    };
    let got = match &root.expr {
        Expr::Literal(l) => match l {
            LiteralValue::Boolean(_) => "bool",
            LiteralValue::Bytes(_) => "bytes",
            LiteralValue::Double(_) => "double",
            LiteralValue::Int(_) => "int",
            LiteralValue::Null => "null_type",
            LiteralValue::String(_) => "string",
            LiteralValue::UInt(_) => "uint",
        },
        Expr::Map(_) => "map(dyn, dyn)",
        Expr::Call(c) if is_bool_operator(&c.func_name) => "bool",
        _ => return None,
    };
    Some(format!("must evaluate to {want} but got {got}"))
}

fn is_bool_operator(name: &str) -> bool {
    matches!(
        name,
        "_<_" | "_<=_" | "_>_" | "_>=_" | "_==_" | "_!=_" | "_&&_" | "_||_" | "!_" | "@in"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> MutationEnv {
        MutationEnv {
            has_params: false,
            has_patch_types: true,
            variables: Some(["x".to_string()].into()),
        }
    }

    fn fail(expr: &str, env: &MutationEnv, out: ExpectedOutput) -> String {
        mutation_env_failure(expr, env, out).unwrap_or_default()
    }

    /// validation_test.go:4286.
    #[test]
    fn a_comparison_is_not_an_object() {
        let m = fail("1 < 2", &env(), ExpectedOutput::Object);
        assert_eq!(m, "must evaluate to Object but got bool");
    }

    /// validation_test.go:4442.
    #[test]
    fn variables_fields_are_the_declared_variables() {
        let m = fail("variables.x + variables.y", &env(), ExpectedOutput::Any);
        assert!(m.contains("undefined field 'y'"), "{m}");
        assert_eq!(fail("variables.x", &env(), ExpectedOutput::Any), "");
        assert_eq!(fail("has(variables.x)", &env(), ExpectedOutput::Any), "");
    }

    /// validation_test.go:5190.
    #[test]
    fn params_needs_a_param_kind() {
        let m = fail("params.foo == 'a'", &env(), ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'params'"), "{m}");
        let with = MutationEnv {
            has_params: true,
            ..env()
        };
        assert_eq!(fail("params.foo == 'a'", &with, ExpectedOutput::Any), "");
    }

    #[test]
    fn variables_is_undeclared_in_the_stateless_environment() {
        let stateless = MutationEnv::default();
        let m = fail("variables.x", &stateless, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'variables'"), "{m}");
    }

    #[test]
    fn patch_types_resolve_only_with_has_patch_types() {
        let no_patch = MutationEnv {
            has_patch_types: false,
            ..env()
        };
        let m = fail("Object{}", &no_patch, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'Object'"), "{m}");
        for ok in ["Object{}", "Object.spec{ replicas: 1 }", "Object.spec.a{}"] {
            assert_eq!(fail(ok, &env(), ExpectedOutput::Object), "", "{ok}");
        }
        let m = fail("Other{}", &env(), ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'Other'"), "{m}");
    }

    #[test]
    fn json_patch_has_fixed_fields() {
        let ok = "[JSONPatch{op: 'add', from: 'a', path: '/a', value: 1}]";
        assert_eq!(fail(ok, &env(), ExpectedOutput::JsonPatchList), "");
        let m = fail("[JSONPatch{bogus: 1}]", &env(), ExpectedOutput::Any);
        assert!(m.contains("undefined field 'bogus'"), "{m}");
    }

    #[test]
    fn comprehension_variables_are_scoped() {
        let e = env();
        assert_eq!(fail("[1].map(i, i + 1)", &e, ExpectedOutput::Any), "");
        assert_eq!(fail("[1].all(i, i > 0)", &e, ExpectedOutput::Any), "");
        assert_eq!(
            fail("object.a.filter(i, i > 0)", &e, ExpectedOutput::Any),
            ""
        );
        let m = fail("[1].map(i, i) + [i]", &e, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'i'"), "{m}");
    }

    #[test]
    fn output_check_skips_what_needs_inference() {
        let e = env();
        for expr in ["object.spec", "variables.x", "foo(1)", "a ? b : c"] {
            let src = expr
                .replace("foo(1)", "size(object.a)")
                .replace("a ? b : c", "true ? object : oldObject");
            assert_eq!(fail(&src, &e, ExpectedOutput::Object), "", "{expr}");
        }
        let m = fail("{'a': 1}", &e, ExpectedOutput::JsonPatchList);
        assert!(
            m.starts_with("must evaluate to list(JSONPatch) but got"),
            "{m}"
        );
    }

    #[test]
    fn syntax_errors_come_first() {
        assert!(fail("///", &env(), ExpectedOutput::Object).starts_with("compilation failed"));
    }
}
