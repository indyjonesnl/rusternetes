//! CEL struct initializers for MutatingAdmissionPolicy: `Object{}`,
//! `Object.spec{}` and `JSONPatch{}`.
//!
//! The `cel` crate parses `Type{field: value}` into `Expr::Struct`, and (0.15)
//! constructs it from a type registered on the `Env` (`Env::add_type`, a
//! [`StructType`]). Upstream resolves the names through cel-go's type
//! provider:
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
//! Mechanism (#2885, re-based on registered struct types in #3440):
//! [`compile`] checks the initializers statically (names, the JSONPatch field
//! set, `Object.<path>` names in field position) and collects the struct type
//! names; [`new_context_for`] registers an [`ObjectStruct`] per name on the
//! evaluation `Env`, so the stock interpreter evaluates the field values (and
//! any struct nested in a call argument, list, comprehension...) and calls
//! `new_value`. `Object.<path>` names are open ended, so they are registered per
//! compiled expression. A struct is a native `CelStruct` value: field select
//! and `has()` read its fields, `==` is the same type name and equal fields
//! (an Object never equals a plain map or an Object of another type,
//! `ObjectVal.Equal`, `dynamic/objects.go:147-155`) and `type()` is its
//! `Object.<path>` name (`ObjectVal.Type`, `dynamic/objects.go:158-160`).
//! `CheckTypeNamesMatchFieldPathNames` runs on the *evaluated* result
//! ([`execute_checked`]); [`execute`] converts the structs to maps.
//!
//! The 0.15 `Expr::Struct` evaluator ignores the `?field:` optional flag, so
//! the value of an optional field goes through `OPTIONAL_FIELD_FN`, which
//! turns an absent optional into a reserved value that `new_value` drops.
//!
//! `ObjectVal` is not a `Sizer` upstream (`dynamic/objects.go:158-160`), so
//! `size(Object{})` is an error in cel-go. The policies accepted here before
//! structs were native take `size`, `in` and comprehensions over the fields
//! (#3157); to keep accept/reject unchanged those operands go through
//! `OBJECT_ITERABLE_FN`, the map of the struct's fields (a known divergence,
//! kept until it is decided separately).
//!
//! Known divergences from cel-go, all benign: an unset `JSONPatch`
//! `op`/`path`/`from` is an absent key rather than `""` (the patcher reads an
//! absent key as `""`, as `jsonpatch.go` does); the string/map-key checks of
//! `new_value` report the first bad field in alphabetical order.

use cel::common::ast::{CallExpr, EntryExpr, Expr, IdedExpr, LiteralValue};
use cel::common::types::{CelString, CelStruct, Type};
use cel::common::value::{CowVal, Val};
use cel::objects::{Key, OptionalValue};
use cel::parser::Parser;
use cel::{Context, Env, ExecutionError, StructType, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

const OBJECT_TYPE_NAME: &str = "Object";
const JSON_PATCH_TYPE_NAME: &str = "JSONPatch";
/// Error context of a JSONPatch `op`/`path`/`from` value (jsonpatch.go:51-66).
const JSON_PATCH_STRING_FN: &str = "__jsonPatchString";
/// Error context of an Object field value: map keys must be strings
/// (`convertField`, dynamic/objects.go:240-258).
const OBJECT_FIELD_FN: &str = "__objectField";
/// Guard on a comprehension range, a `size` argument and the right operand of
/// `in`: the field view of a struct (see the module docs).
const OBJECT_ITERABLE_FN: &str = "__objectIterable";
/// Guard on the value of an optional (`?field:`) struct field, whose flag the
/// 0.15 `Expr::Struct` evaluator does not honour: an absent optional becomes
/// [`OMIT_FIELD`], which `new_value` drops.
const OPTIONAL_FIELD_FN: &str = "__optionalField";
/// The reserved value of an absent optional struct field.
const OMIT_FIELD: &str = "\u{1}omitField";
/// Guard replacing the `_?._` optional-select operator ([`opt_select`]).
const OPT_SELECT_FN: &str = "__optSelect";
/// `jsonpatch.escapeKey` (apiserver `pkg/cel/library/jsonpatch.go:57-62`).
/// The interpreter parses `jsonpatch.escapeKey(x)` as a member call on the
/// identifier `jsonpatch`; [`rewrite`] turns it into a global call of this
/// dotted name.
const JSON_PATCH_ESCAPE_KEY_FN: &str = "jsonpatch.escapeKey";

/// A compiled CEL expression whose struct names have been collected.
pub struct CompiledExpression {
    expr: IdedExpr,
    /// The struct types the expression constructs: registered on the
    /// evaluation environment by [`new_context_for`].
    struct_types: BTreeSet<String>,
}

impl CompiledExpression {
    /// The checked expression, for the costed evaluation of [`crate::cel_cost`].
    pub fn expression(&self) -> &IdedExpr {
        &self.expr
    }
}

/// `Context::add_function` for a name that is known not to be a standard
/// library overload: cel 0.15 fails with `OverloadConflict` otherwise, which
/// is a programming error here.
pub(crate) fn add_function<T: 'static, F>(ctx: &mut Context<'_, '_>, name: &str, f: F)
where
    F: cel::extractors::IntoFunction<T> + 'static + Send + Sync,
{
    ctx.add_function(name, f)
        .unwrap_or_else(|e| panic!("cannot declare CEL function {name}: {e}"));
}

/// `ObjectType` (dynamic/objects.go:41-60): any field name is allowed and the
/// value is a struct of that type name; `JSONPatchType` (jsonpatch.go:51-74)
/// allows `op`/`path`/`from` (strings) and `value` (any).
struct ObjectStruct {
    ty: Type,
    json_patch: bool,
}

impl ObjectStruct {
    fn new(name: &str) -> Self {
        ObjectStruct {
            ty: Type::new_struct(name.to_string()),
            json_patch: name == JSON_PATCH_TYPE_NAME,
        }
    }
}

impl StructType for ObjectStruct {
    fn get_type(&self) -> &Type {
        &self.ty
    }

    fn new_value<'b, 'v>(
        &self,
        fields: BTreeMap<String, CowVal<'b, 'v>>,
    ) -> Result<Box<dyn Val + 'v>, ExecutionError> {
        let mut s = CelStruct::new(self.ty.name().to_string());
        for (name, value) in fields {
            let val: &dyn Val = value.as_ref();
            if val
                .downcast_ref::<CelString>()
                .is_some_and(|v| v.inner() == OMIT_FIELD)
            {
                continue;
            }
            if self.json_patch {
                match name.as_str() {
                    "op" | "path" | "from" => {
                        if val.downcast_ref::<CelString>().is_none() {
                            return Err(ExecutionError::function_error(
                                JSON_PATCH_STRING_FN,
                                format!(
                                    "unexpected type {} for JSONPatchType string field",
                                    val.get_type().name()
                                ),
                            ));
                        }
                    }
                    "value" => {}
                    other => {
                        return Err(ExecutionError::function_error(
                            JSON_PATCH_TYPE_NAME,
                            format!("unexpected JSONPatchType field: {other}"),
                        ))
                    }
                }
            } else {
                check_string_keys(&Value::try_from(val)?)?;
            }
            s.add_field_value(name, value);
        }
        Ok(Box::new(s))
    }
}

/// A default CEL [`Context`] with the guard functions and no struct types.
pub fn new_context() -> Context<'static, 'static> {
    context_with(&BTreeSet::new())
}

/// A [`Context`] for evaluating `expr`: [`new_context`] with the struct types
/// the expression constructs registered (`Object.<path>` names are open
/// ended, so they are registered per expression).
pub fn new_context_for(expr: &CompiledExpression) -> Context<'static, 'static> {
    context_with(&expr.struct_types)
}

fn context_with(struct_types: &BTreeSet<String>) -> Context<'static, 'static> {
    let mut env = Env::stdlib();
    for name in struct_types {
        // A name the registry rejects stays unregistered and the evaluation
        // reports it as an unknown struct.
        let _ = env.add_type(ObjectStruct::new(name));
    }
    let mut ctx = Context::with_env(Arc::new(env));
    // jsonpatch.go:76-80 `jsonPatchReplacer` ("/" -> "~1", "~" -> "~0"; the
    // replacer is a single pass, so "~" is not re-escaped after "/").
    add_function(
        &mut ctx,
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
    add_function(
        &mut ctx,
        OBJECT_ITERABLE_FN,
        |v: Value| -> Result<Value, ExecutionError> { Ok(struct_view(&v)) },
    );
    add_function(
        &mut ctx,
        OPTIONAL_FIELD_FN,
        |v: Value| -> Result<Value, ExecutionError> {
            match <&OptionalValue>::try_from(&v) {
                Ok(opt) => Ok(opt
                    .value()
                    .cloned()
                    .unwrap_or_else(|| Value::String(Arc::new(OMIT_FIELD.to_string())))),
                Err(_) => Ok(v),
            }
        },
    );
    add_function(&mut ctx, OPT_SELECT_FN, opt_select);
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
            Value::Struct(s) => s.field_value(field).and_then(|v| Value::try_from(v).ok()),
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
        Value::Map(_) | Value::Struct(_) => Ok(wrap(lookup(&operand, &field))),
        _ => Err(ExecutionError::no_such_overload("_?._", Vec::new())),
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
        Value::Struct(_) => "struct",
        _ => "value",
    }
}

/// The fields of a struct value, as values.
fn struct_fields(s: &CelStruct<'_>) -> Vec<(String, Value)> {
    s.field_values()
        .into_iter()
        .filter_map(|(k, v)| Value::try_from(&*v).ok().map(|v| (k, v)))
        .collect()
}

fn fields_to_map(fields: Vec<(String, Value)>) -> Value {
    let map = fields
        .into_iter()
        .map(|(k, v)| (Key::String(Arc::new(k)), v))
        .collect();
    Value::Map(cel::objects::Map { map: Arc::new(map) })
}

/// `v` as the map of its fields when it is a struct (one level; nested
/// structs stay). `ObjectVal` reports its own type and is not a `Sizer`
/// (`dynamic/objects.go:158-160`); the policies this server accepted before
/// the struct values were native take `size`, `in` and comprehensions over
/// the fields, so those operands are routed through this view.
fn struct_view(v: &Value) -> Value {
    match v {
        Value::Struct(s) => fields_to_map(struct_fields(s)),
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

/// Parse `source`, check its struct initializers and collect the struct types
/// it constructs. Never panics.
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
    let mut struct_types = BTreeSet::new();
    rewrite(&mut expr, None, &mut struct_types)?;
    Ok(CompiledExpression { expr, struct_types })
}

/// Evaluate with a context from [`new_context_for`], keeping the struct
/// values (for values that are bound back into another evaluation). Never
/// panics.
pub fn execute_raw(expr: &CompiledExpression, ctx: &Context) -> Result<Value, String> {
    match catch_unwind(AssertUnwindSafe(|| ctx.resolve(&expr.expr))) {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("CEL evaluation failed unexpectedly".to_string()),
    }
}

/// Evaluate with a context from [`new_context_for`]; the result is a plain CEL
/// value, structs converted to maps. Never panics.
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
/// (`dynamic/objects.go:180-215`) as the MAP patchers apply it, then convert
/// the structs to maps. A mismatch is `unexpected type name ...`; the caller
/// adds the `type mismatch: ` prefix.
pub fn execute_checked(
    expr: &CompiledExpression,
    ctx: &Context,
    mode: TypeCheck,
) -> Result<Value, String> {
    let v = execute_raw(expr, ctx)?;
    finish_checked(v, mode)
}

/// The `CheckTypeNamesMatchFieldPathNames` and struct-to-map half of
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
                    let value = match item {
                        Value::Map(m) => m.map.get(&Key::from("value")).cloned(),
                        Value::Struct(s) => {
                            s.field_value("value").and_then(|v| Value::try_from(v).ok())
                        }
                        _ => None,
                    };
                    if let Some(val) = value {
                        if let Some(tn) = type_name(&val) {
                            type_check(&val, tn, &mut errs);
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

/// The `Object{}` type name of `v`, if it is one (a `JSONPatch{}` is not an
/// `ObjectVal`).
fn type_name(v: &Value) -> Option<&str> {
    match v {
        Value::Struct(s) if s.name() != JSON_PATCH_TYPE_NAME => Some(s.name()),
        _ => None,
    }
}

/// `typeCheck` (dynamic/objects.go:184-214): an Object's children under
/// `<field>` must be named `<path>.<field>`; intermediate maps and lists are
/// unnamed and keep the path.
fn type_check(v: &Value, path: &str, errs: &mut Vec<String>) {
    match v {
        Value::Struct(s) => {
            let fields = struct_fields(s);
            if let Some(tn) = type_name(v) {
                if tn != path {
                    errs.push(format!(
                        "unexpected type name \"{tn}\", expected \"{path}\", which matches field name path from root Object type"
                    ));
                }
                for (k, f) in &fields {
                    type_check(f, &format!("{path}.{k}"), errs);
                }
            } else {
                fields.iter().for_each(|(_, f)| type_check(f, path, errs));
            }
        }
        Value::Map(m) => m.map.values().for_each(|f| type_check(f, path, errs)),
        Value::List(l) => l.iter().for_each(|f| type_check(f, path, errs)),
        _ => {}
    }
}

/// Convert every struct in `v` to a map of its fields.
pub fn strip_type_markers(v: &Value) -> Value {
    match v {
        Value::Struct(s) => fields_to_map(
            struct_fields(s)
                .into_iter()
                .map(|(k, v)| (k, strip_type_markers(&v)))
                .collect(),
        ),
        Value::Map(m) => {
            let map = m
                .map
                .iter()
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
        expr: Expr::Literal(LiteralValue::String(cel::common::types::CelString::from(
            s.to_string(),
        ))),
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

fn rewrite_all(
    es: &mut [IdedExpr],
    expected: Option<&str>,
    types: &mut BTreeSet<String>,
) -> Result<(), String> {
    es.iter_mut().try_for_each(|e| rewrite(e, expected, types))
}

/// Check the struct initializers of `e` and collect the struct type names
/// into `types`. `expected` is the type name a struct in this position must
/// carry (`Object.<field path>`), when the position is a field of an `Object`.
fn rewrite(
    e: &mut IdedExpr,
    expected: Option<&str>,
    types: &mut BTreeSet<String>,
) -> Result<(), String> {
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
            rewrite(&mut s.operand, None, types)
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
            // cel 0.15 refuses to override a standard-library overload with a
            // `Context` function, so the `ObjectVal` view of `size` and `in`
            // is routed here (see [`struct_view`]).
            if c.func_name == "size" {
                match (c.target.as_mut(), c.args.len()) {
                    (Some(t), 0) => {
                        let inner = std::mem::take(&mut **t);
                        **t = guard(OBJECT_ITERABLE_FN, inner);
                    }
                    (None, 1) => {
                        let inner = std::mem::take(&mut c.args[0]);
                        c.args[0] = guard(OBJECT_ITERABLE_FN, inner);
                    }
                    _ => {}
                }
            }
            if c.func_name == cel::common::ast::operators::IN
                && c.target.is_none()
                && c.args.len() == 2
                // a list literal is never a struct (and stays a constant
                // set for the cost evaluator, `maybeOptimizeSetMembership`)
                && !matches!(c.args[1].expr, Expr::List(_))
            {
                let inner = std::mem::take(&mut c.args[1]);
                c.args[1] = guard(OBJECT_ITERABLE_FN, inner);
            }
            if c.func_name == cel::common::ast::operators::OPT_SELECT
                && c.target.is_none()
                && c.args.len() == 2
            {
                c.func_name = OPT_SELECT_FN.to_string();
            }
            if let Some(t) = c.target.as_mut() {
                rewrite(t, None, types)?;
            }
            if c.func_name == cel::common::ast::operators::CONDITIONAL && c.args.len() == 3 {
                let (cond, branches) = c.args.split_at_mut(1);
                rewrite(&mut cond[0], None, types)?;
                return rewrite_all(branches, expected, types);
            }
            rewrite_all(&mut c.args, None, types)
        }
        Expr::List(l) => rewrite_all(&mut l.elements, expected, types),
        Expr::Map(m) => m
            .entries
            .iter_mut()
            .try_for_each(|entry| match &mut entry.expr {
                EntryExpr::MapEntry(me) => {
                    rewrite(&mut me.key, None, types)?;
                    rewrite(&mut me.value, expected, types)
                }
                // The interpreter panics ("WAT?") on a struct field inside a map.
                EntryExpr::StructField(_) => {
                    Err("unexpected struct field in map literal".to_string())
                }
            }),
        Expr::Comprehension(c) => {
            rewrite(&mut c.iter_range, None, types)?;
            let range = std::mem::replace(&mut c.iter_range, str_lit(""));
            c.iter_range = guard(OBJECT_ITERABLE_FN, range);
            rewrite(&mut c.accu_init, None, types)?;
            rewrite(&mut c.loop_cond, None, types)?;
            rewrite(&mut c.loop_step, None, types)?;
            rewrite(&mut c.result, None, types)
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
            s.type_name = name.clone();
            types.insert(name.clone());
            for entry in &mut s.entries {
                let EntryExpr::StructField(f) = &mut entry.expr else {
                    return Err("unexpected map entry in struct initializer".to_string());
                };
                if is_patch {
                    if !matches!(f.field.as_str(), "op" | "path" | "from" | "value") {
                        return Err(format!("unexpected JSONPatchType field: {}", f.field));
                    }
                    rewrite(&mut f.value, None, types)?;
                } else {
                    let child = format!("{name}.{}", f.field);
                    rewrite(&mut f.value, Some(&child), types)?;
                }
                if f.optional {
                    let value = std::mem::take(&mut f.value);
                    f.value = guard(OPTIONAL_FIELD_FN, value);
                }
            }
            Ok(())
        }
    }
}
