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
//! `Object{}` values stay CEL maps (so field select, `in`, `size`, ... work)
//! but carry their type name under the reserved key `TYPE_KEY`, which makes
//! them unequal to a plain map and to an Object of another type, as
//! `ObjectVal.Equal` (`dynamic/objects.go:147-155`) does. The marker travels
//! with the value through variables and functions, so
//! `CheckTypeNamesMatchFieldPathNames` runs on the *evaluated* result
//! ([`execute_checked`]); [`execute`] strips the markers.
//!
//! `ObjectVal` reports its own type from `Type()` and is not a `Sizer`
//! (`dynamic/objects.go:158-160`), so the marker must be invisible to CEL:
//! `size` is overridden on the [`Context`] to not count it, `type` returns the
//! `Object.<path>` name, and every comprehension range is routed through
//! `OBJECT_ITERABLE_FN` so iteration yields only the real fields (#3157).
//!
//! Known divergences from cel-go, all benign: an unset `JSONPatch`
//! `op`/`path`/`from` is an absent key rather than `""` (the patcher reads an
//! absent key as `""`, as `jsonpatch.go` does); `type()` returns a string
//! (the cel 0.13 crate has no type values), the type name for an `Object{}`
//! and the CEL kind name (`map`, `int`, ...) otherwise.

use cel::common::ast::{
    CallExpr, EntryExpr, Expr, IdedEntryExpr, IdedExpr, LiteralValue, MapEntryExpr, MapExpr,
};
use cel::common::types::CelString;
use cel::objects::{Key, OptionalValue};
use cel::parser::Parser;
use cel::{Context, ExecutionError, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

const OBJECT_TYPE_NAME: &str = "Object";
/// Reserved map key carrying an `Object{}` value's type name.
const TYPE_KEY: &str = "\u{1}objectType";
const JSON_PATCH_TYPE_NAME: &str = "JSONPatch";
/// Guard on a JSONPatch `op`/`path`/`from` value (jsonpatch.go:51-66).
const JSON_PATCH_STRING_FN: &str = "__jsonPatchString";
/// Guard on an Object field value: map keys must be strings
/// (`convertField`, dynamic/objects.go:240-258).
const OBJECT_FIELD_FN: &str = "__objectField";
/// Guard on a comprehension range: drops the type marker of an Object so
/// iteration sees only its fields.
const OBJECT_ITERABLE_FN: &str = "__objectIterable";
/// `jsonpatch.escapeKey` (apiserver `pkg/cel/library/jsonpatch.go:57-62`).
/// The interpreter parses `jsonpatch.escapeKey(x)` as a member call on the
/// identifier `jsonpatch`; [`rewrite`] turns it into a global call of this
/// dotted name.
/// Guard replacing the `_?._` optional-select operator ([`opt_select`]).
const OPT_SELECT_FN: &str = "__optSelect";
const JSON_PATCH_ESCAPE_KEY_FN: &str = "jsonpatch.escapeKey";

/// A compiled CEL expression whose struct initializers have been rewritten.
pub struct CompiledExpression(IdedExpr);

impl CompiledExpression {
    /// The rewritten expression, for the costed evaluation of [`crate::cel_cost`].
    pub fn expression(&self) -> &IdedExpr {
        &self.0
    }
}

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
    // jsonpatch.go:76-80 `jsonPatchReplacer` ("/" -> "~1", "~" -> "~0"; the
    // replacer is a single pass, so "~" is not re-escaped after "/").
    ctx.add_function(
        JSON_PATCH_ESCAPE_KEY_FN,
        |v: Value| -> Result<Value, ExecutionError> {
            match v {
                Value::String(s) => {
                    let mut out = String::with_capacity(s.len());
                    for c in s.chars() {
                        match c {
                            '/' => out.push_str("~1"),
                            '~' => out.push_str("~0"),
                            c => out.push(c),
                        }
                    }
                    Ok(Value::String(std::sync::Arc::new(out)))
                }
                other => Err(ExecutionError::function_error(
                    JSON_PATCH_ESCAPE_KEY_FN,
                    format!("no such overload for {}", kind(&other)),
                )),
            }
        },
    );
    // `ObjectVal` is not a Sizer (dynamic/objects.go:158-160): the marker key
    // is not a field. Everything else is the stock `size`.
    ctx.add_function(
        "size",
        |ftx: &cel::FunctionContext, cel::extractors::This(this): cel::extractors::This<Value>| {
            cel::functions::size(ftx, cel::extractors::This(without_marker(&this)))
        },
    );
    // `ObjectVal.Type` (dynamic/objects.go:158-160).
    ctx.add_function("type", |v: Value| -> Result<Value, ExecutionError> {
        let name = type_name(&v).unwrap_or_else(|| kind(&v));
        Ok(Value::String(Arc::new(name.to_string())))
    });
    ctx.add_function(
        OBJECT_ITERABLE_FN,
        |v: Value| -> Result<Value, ExecutionError> { Ok(without_marker(&v)) },
    );
    ctx.add_function(
        OBJECT_FIELD_FN,
        |v: Value| -> Result<Value, ExecutionError> {
            check_string_keys(&v)?;
            Ok(v)
        },
    );
    ctx.add_function(OPT_SELECT_FN, opt_select);
    crate::cel_authz_library::register(&mut ctx);
    ctx
}

/// Guard standing in for the `_?._` operator. The `cel` 0.13 interpreter
/// (`objects.rs` `operators::OPT_SELECT`) errors with `No such key` for
/// `map.?missing` when the receiver is a plain map, instead of yielding
/// `optional.none()`. cel-go's `OptionalTypes()` library returns an optional
/// for every receiver, and the apiextensions `optionalOldSelf` tests rely on
/// it (`validation_test.go` `TestOptionalOldSelfCheckForNull`).
fn opt_select(operand: Value, field: Value) -> Result<Value, ExecutionError> {
    let Value::String(field) = field else {
        return Err(ExecutionError::function_error(
            "_?._",
            "field must be string",
        ));
    };
    fn lookup(v: &Value, field: &str) -> Option<Value> {
        match v {
            Value::Map(m) => m
                .map
                .get(&Key::String(std::sync::Arc::new(field.to_string())))
                .cloned(),
            _ => None,
        }
    }
    let wrap = |v: Option<Value>| {
        Value::Opaque(std::sync::Arc::new(match v {
            Some(v) => OptionalValue::of(v),
            None => OptionalValue::none(),
        }))
    };
    if let Ok(opt) = <&OptionalValue>::try_from(&operand) {
        return Ok(wrap(opt.value().and_then(|v| lookup(v, &field))));
    }
    match operand {
        Value::Map(_) => Ok(wrap(lookup(&operand, &field))),
        _ => Err(ExecutionError::NoSuchOverload),
    }
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

/// `v` without its own (top-level) `Object{}` marker key; nested Objects keep
/// theirs.
fn without_marker(v: &Value) -> Value {
    match v {
        Value::Map(m) if type_name(v).is_some() => {
            let map = m
                .map
                .iter()
                .filter(|(k, _)| !is_type_key(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Value::Map(cel::objects::Map { map: Arc::new(map) })
        }
        other => other.clone(),
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
    // `enable_optional_syntax`: cel-go `OptionalTypes()` is in the base env
    // (apiserver `pkg/cel/environment/base.go:99-101`), so `.?field` and
    // `[?index]` parse.
    let mut expr = catch_unwind(AssertUnwindSafe(|| {
        Parser::new().enable_optional_syntax(true).parse(source)
    }))
    .map_err(|_| format!("invalid CEL expression '{source}'"))?
    .map_err(|e| e.to_string())?;
    rewrite(&mut expr, None)?;
    Ok(CompiledExpression(expr))
}

/// Evaluate with a context from [`new_context`], keeping the `Object{}` type
/// markers (for values that are bound back into another evaluation). Never
/// panics.
pub fn execute_raw(expr: &CompiledExpression, ctx: &Context) -> Result<Value, String> {
    match catch_unwind(AssertUnwindSafe(|| ctx.resolve(&expr.0))) {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("CEL evaluation failed unexpectedly".to_string()),
    }
}

/// Evaluate with a context from [`new_context`]; the result is a plain CEL
/// value without type markers. Never panics.
pub fn execute(expr: &CompiledExpression, ctx: &Context) -> Result<Value, String> {
    execute_raw(expr, ctx).map(|v| strip_type_markers(&v))
}

/// What the evaluated result of a MAP patch expression is checked as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeCheck {
    /// An `ApplyConfiguration` result: the root `Object` (smd.go:99-102).
    Root,
    /// A `JSONPatch` list: each `value` that is an `Object` (json_patch.go:162-173).
    JsonPatchValues,
}

/// [`execute_raw`], then `CheckTypeNamesMatchFieldPathNames`
/// (`dynamic/objects.go:180-215`) as the MAP patchers apply it, then strip the
/// markers. A mismatch is `unexpected type name ...`; the caller adds the
/// `type mismatch: ` prefix.
pub fn execute_checked(
    expr: &CompiledExpression,
    ctx: &Context,
    mode: TypeCheck,
) -> Result<Value, String> {
    let v = execute_raw(expr, ctx)?;
    finish_checked(v, mode)
}

/// The `CheckTypeNamesMatchFieldPathNames` and marker-stripping half of
/// [`execute_checked`], for a value evaluated elsewhere (the costed path).
pub fn finish_checked(v: Value, mode: TypeCheck) -> Result<Value, String> {
    let mut errs = Vec::new();
    match mode {
        TypeCheck::Root => {
            if let Some(tn) = type_name(&v) {
                type_check(&v, tn, &mut errs);
            }
        }
        TypeCheck::JsonPatchValues => {
            if let Value::List(items) = &v {
                for item in items.iter() {
                    if let Value::Map(m) = item {
                        if let Some(val) = m.map.get(&Key::from("value")) {
                            if let Some(tn) = type_name(val) {
                                type_check(val, tn, &mut errs);
                            }
                        }
                    }
                }
            }
        }
    }
    if !errs.is_empty() {
        return Err(errs.join("\n"));
    }
    Ok(strip_type_markers(&v))
}

/// The `Object{}` type name of `v`, if it is one.
fn type_name(v: &Value) -> Option<&str> {
    match v {
        Value::Map(m) => match m.map.get(&Key::from(TYPE_KEY)) {
            Some(Value::String(s)) => Some(s.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn is_type_key(k: &Key) -> bool {
    matches!(k, Key::String(s) if s.as_str() == TYPE_KEY)
}

/// `typeCheck` (dynamic/objects.go:184-214): an Object's children under
/// `<field>` must be named `<path>.<field>`; intermediate maps and lists are
/// unnamed and keep the path.
fn type_check(v: &Value, path: &str, errs: &mut Vec<String>) {
    match v {
        Value::Map(m) => {
            if let Some(tn) = type_name(v) {
                if tn != path {
                    errs.push(format!(
                        "unexpected type name \"{tn}\", expected \"{path}\", which matches field name path from root Object type"
                    ));
                }
                let mut fields: Vec<_> = m.map.iter().filter(|(k, _)| !is_type_key(k)).collect();
                fields.sort_by_key(|(k, _)| format!("{k:?}"));
                for (k, f) in fields {
                    if let Key::String(k) = k {
                        type_check(f, &format!("{path}.{k}"), errs);
                    }
                }
            } else {
                m.map.values().for_each(|f| type_check(f, path, errs));
            }
        }
        Value::List(l) => l.iter().for_each(|f| type_check(f, path, errs)),
        _ => {}
    }
}

/// Remove the type marker from every Object in `v`.
pub fn strip_type_markers(v: &Value) -> Value {
    match v {
        Value::Map(m) => {
            let map = m
                .map
                .iter()
                .filter(|(k, _)| !is_type_key(k))
                .map(|(k, v)| (k.clone(), strip_type_markers(v)))
                .collect();
            Value::Map(cel::objects::Map { map: Arc::new(map) })
        }
        Value::List(l) => Value::List(Arc::new(l.iter().map(strip_type_markers).collect())),
        other => other.clone(),
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
        Expr::Select(s) => {
            // `authorizer.requestResource` is the qualified name of a
            // variable (compile.go:40, activation.go:99-103), not a field
            // select on `authorizer`.
            if !s.test
                && s.field == "requestResource"
                && matches!(&s.operand.expr, Expr::Ident(n) if n == crate::cel_authz_library::AUTHORIZER_VAR)
            {
                e.expr = Expr::Ident(
                    crate::cel_authz_library::REQUEST_RESOURCE_AUTHORIZER_VAR.to_string(),
                );
                return Ok(());
            }
            rewrite(&mut s.operand, None)
        }
        Expr::Call(c) => {
            if c.func_name == "escapeKey"
                && c.args.len() == 1
                && matches!(
                    c.target.as_deref().map(|t| &t.expr),
                    Some(Expr::Ident(n)) if n == "jsonpatch"
                )
            {
                c.target = None;
                c.func_name = JSON_PATCH_ESCAPE_KEY_FN.to_string();
            }
            if c.func_name == cel::common::ast::operators::OPT_SELECT
                && c.target.is_none()
                && c.args.len() == 2
            {
                c.func_name = OPT_SELECT_FN.to_string();
            }
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
            let range = std::mem::replace(&mut c.iter_range, str_lit(""));
            c.iter_range = guard(OBJECT_ITERABLE_FN, range);
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
            let mut entries = Vec::with_capacity(s.entries.len() + 1);
            if is_object {
                entries.push(IdedEntryExpr {
                    id: 0,
                    expr: EntryExpr::MapEntry(MapEntryExpr {
                        key: str_lit(TYPE_KEY),
                        value: str_lit(&name),
                        optional: false,
                    }),
                });
            }
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
