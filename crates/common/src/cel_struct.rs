//! CEL struct initializers for MutatingAdmissionPolicy: `Object{}`,
//! `Object.spec{}` and `JSONPatch{}`.
//!
//! The `cel` 0.13 crate parses `Type{field: value}` into `Expr::Struct` but its
//! interpreter has `Expr::Struct(_) => todo!("Support structs!")`
//! (`cel-0.13.0/src/objects.rs:1433`) and no type registry, so executing one
//! panics. Upstream resolves the names through cel-go's type provider:
//!
//! * `DynamicTypeResolver.Resolve`
//!   (`staging/src/k8s.io/apiserver/pkg/cel/mutation/typeresolver.go:32-42`):
//!   `JSONPatch`, `Object`, and `Object.<path>` resolve; anything else is an
//!   "undeclared reference" compile error.
//! * `ObjectType.Val` / `ObjectVal.ConvertToNative`
//!   (`.../cel/mutation/dynamic/objects.go:56-60,95-111`): any field name, and
//!   the value is a `map[string]any`.
//! * `JSONPatchType.Val` (`.../cel/mutation/jsonpatch.go:51-74`): only
//!   `op`/`path`/`from` (strings) and `value` (dyn) are fields.
//! * `ObjectVal.CheckTypeNamesMatchFieldPathNames`
//!   (`dynamic/objects.go:180-215`): a nested `Object.x.y{}` must be named for
//!   its field path.
//!
//! Route (#2885): rewrite every `Expr::Struct` into the equivalent `Expr::Map`
//! before execution, so the stock interpreter evaluates the field values (and
//! any struct nested in a call argument, list, comprehension...), and keep
//! upstream's checks either statically (names, field set) or through two
//! guard functions registered on the evaluation [`Context`]. Patching the crate
//! was rejected: the interpreter would still need a type registry that its
//! `Context` does not have, and a `Context` function cannot intercept it
//! because `Expr::Struct` is not a call.
//!
//! Known divergences from cel-go, all benign: type-name path checking only
//! follows literal nesting (struct fields, list elements, map values and
//! ternary branches), not a struct reaching a field through a variable or
//! function; an unset `JSONPatch` `op`/`path`/`from` is an absent key rather
//! than `""`; `Object{}` is a CEL map, so its `type()` is `map`.

use cel::common::ast::{
    CallExpr, EntryExpr, Expr, IdedEntryExpr, IdedExpr, LiteralValue, MapEntryExpr, MapExpr,
};
use cel::common::types::CelString;
use cel::{Context, ExecutionError, Program, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};

const OBJECT_TYPE_NAME: &str = "Object";
const JSON_PATCH_TYPE_NAME: &str = "JSONPatch";
/// Guard on a JSONPatch `op`/`path`/`from` value (jsonpatch.go:51-66).
const JSON_PATCH_STRING_FN: &str = "__jsonPatchString";
/// Guard on an Object field value: map keys must be strings
/// (`convertField`, dynamic/objects.go:240-258).
const OBJECT_FIELD_FN: &str = "__objectField";

/// A compiled CEL expression whose struct initializers have been rewritten.
pub struct CompiledExpression(IdedExpr);

/// A default CEL [`Context`] with the struct-literal guard functions.
pub fn new_context() -> Context<'static> {
    let mut ctx = Context::default();
    ctx.add_function(
        JSON_PATCH_STRING_FN,
        |v: Value| -> Result<Value, ExecutionError> {
            match v {
                Value::String(_) => Ok(v),
                other => Err(ExecutionError::function_error(
                    JSON_PATCH_STRING_FN,
                    format!(
                        "unexpected type {} for JSONPatchType string field",
                        kind(&other)
                    ),
                )),
            }
        },
    );
    ctx.add_function(
        OBJECT_FIELD_FN,
        |v: Value| -> Result<Value, ExecutionError> {
            check_string_keys(&v)?;
            Ok(v)
        },
    );
    ctx
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::List(_) => "list",
        Value::Map(_) => "map",
        Value::Int(_) => "int",
        Value::UInt(_) => "uint",
        Value::Float(_) => "double",
        Value::Bool(_) => "bool",
        Value::Null => "null",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        _ => "value",
    }
}

fn check_string_keys(v: &Value) -> Result<(), ExecutionError> {
    match v {
        Value::List(l) => l.iter().try_for_each(check_string_keys),
        Value::Map(m) => m.map.iter().try_for_each(|(k, v)| {
            if !matches!(k, cel::objects::Key::String(_)) {
                return Err(ExecutionError::function_error(
                    OBJECT_FIELD_FN,
                    format!("map key {k:?} is not a string"),
                ));
            }
            check_string_keys(v)
        }),
        _ => Ok(()),
    }
}

/// Parse `source` and rewrite its struct initializers. Never panics.
pub fn compile(source: &str) -> Result<CompiledExpression, String> {
    // The antlr4rust parser panics on some invalid expressions.
    let program = catch_unwind(AssertUnwindSafe(|| Program::compile(source)))
        .map_err(|_| format!("invalid CEL expression '{source}'"))?
        .map_err(|e| e.to_string())?;
    let mut expr = program.expression().clone();
    rewrite(&mut expr, None)?;
    Ok(CompiledExpression(expr))
}

/// Evaluate with a context from [`new_context`]. Never panics.
pub fn execute(expr: &CompiledExpression, ctx: &Context) -> Result<Value, String> {
    match catch_unwind(AssertUnwindSafe(|| ctx.resolve(&expr.0))) {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("CEL evaluation failed unexpectedly".to_string()),
    }
}

fn str_lit(s: &str) -> IdedExpr {
    IdedExpr {
        id: 0,
        expr: Expr::Literal(LiteralValue::String(CelString::from(s))),
    }
}

fn guard(func: &str, arg: IdedExpr) -> IdedExpr {
    IdedExpr {
        id: arg.id,
        expr: Expr::Call(CallExpr {
            func_name: func.to_string(),
            target: None,
            args: vec![arg],
        }),
    }
}

fn rewrite_all(es: &mut [IdedExpr], expected: Option<&str>) -> Result<(), String> {
    es.iter_mut().try_for_each(|e| rewrite(e, expected))
}

/// `expected` is the type name a struct in this position must carry
/// (`Object.<field path>`), when the position is a field of an `Object`.
fn rewrite(e: &mut IdedExpr, expected: Option<&str>) -> Result<(), String> {
    match &mut e.expr {
        Expr::Unspecified => Err("unspecified CEL expression".to_string()),
        Expr::Ident(_) | Expr::Literal(_) => Ok(()),
        Expr::Select(s) => rewrite(&mut s.operand, None),
        Expr::Call(c) => {
            if let Some(t) = c.target.as_mut() {
                rewrite(t, None)?;
            }
            if c.func_name == cel::common::ast::operators::CONDITIONAL && c.args.len() == 3 {
                let (cond, branches) = c.args.split_at_mut(1);
                rewrite(&mut cond[0], None)?;
                return rewrite_all(branches, expected);
            }
            rewrite_all(&mut c.args, None)
        }
        Expr::List(l) => rewrite_all(&mut l.elements, expected),
        Expr::Map(m) => m
            .entries
            .iter_mut()
            .try_for_each(|entry| match &mut entry.expr {
                EntryExpr::MapEntry(me) => {
                    rewrite(&mut me.key, None)?;
                    rewrite(&mut me.value, expected)
                }
                // The interpreter panics ("WAT?") on a struct field inside a map.
                EntryExpr::StructField(_) => {
                    Err("unexpected struct field in map literal".to_string())
                }
            }),
        Expr::Comprehension(c) => {
            rewrite(&mut c.iter_range, None)?;
            rewrite(&mut c.accu_init, None)?;
            rewrite(&mut c.loop_cond, None)?;
            rewrite(&mut c.loop_step, None)?;
            rewrite(&mut c.result, None)
        }
        Expr::Struct(s) => {
            let name = s.type_name.trim_start_matches('.').to_string();
            let is_patch = name == JSON_PATCH_TYPE_NAME;
            let is_object = name == OBJECT_TYPE_NAME
                || name
                    .strip_prefix(OBJECT_TYPE_NAME)
                    .is_some_and(|rest| rest.starts_with('.') && rest.len() > 1);
            if !is_patch && !is_object {
                // typeresolver.go:32-42 resolves nothing else.
                return Err(format!("undeclared reference to '{name}'"));
            }
            if is_object {
                if let Some(exp) = expected {
                    if exp != name {
                        return Err(format!(
                            "unexpected type name \"{name}\", expected \"{exp}\", which matches field name path from root Object type"
                        ));
                    }
                }
            }
            let mut entries = Vec::with_capacity(s.entries.len());
            for entry in std::mem::take(&mut s.entries) {
                let IdedEntryExpr { id, expr } = entry;
                let EntryExpr::StructField(mut f) = expr else {
                    return Err("unexpected map entry in struct initializer".to_string());
                };
                if is_patch {
                    if !matches!(f.field.as_str(), "op" | "path" | "from" | "value") {
                        return Err(format!("unexpected JSONPatchType field: {}", f.field));
                    }
                    rewrite(&mut f.value, None)?;
                    if f.field != "value" && !f.optional {
                        f.value = guard(JSON_PATCH_STRING_FN, f.value);
                    }
                } else {
                    let child = format!("{name}.{}", f.field);
                    rewrite(&mut f.value, Some(&child))?;
                    if !f.optional {
                        f.value = guard(OBJECT_FIELD_FN, f.value);
                    }
                }
                entries.push(IdedEntryExpr {
                    id,
                    expr: EntryExpr::MapEntry(MapEntryExpr {
                        key: str_lit(&f.field),
                        value: f.value,
                        optional: f.optional,
                    }),
                });
            }
            e.expr = Expr::Map(MapExpr { entries });
            Ok(())
        }
    }
}
