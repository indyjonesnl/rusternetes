//! `checker/checker.go`: type-checks a parsed expression against an [`Env`],
//! producing the type of every node (`typeMap`) and the resolved references
//! (`referenceMap`).
//!
//! Deviation: cel-go rewrites the AST in place (`e.SetKindCase`) so that
//! identifiers, struct type names and function names become their fully
//! qualified resolved names. The `cel` AST is borrowed here, so the resolved
//! name is recorded in [`Reference::name`] of that node instead.

use crate::env::{type_params, Env, FunctionDecl};
use crate::errors::{Errors, Locator};
use crate::types::DType;
use crate::unify::{is_assignable_in, is_assignable_list_in, most_general, substitute, Mapping};
use cel::common::ast::{EntryExpr, Expr, IdedExpr, LiteralValue};
use std::collections::HashMap;

/// `operators.OptSelect`.
const OPT_SELECT: &str = "_?._";

/// `ast.ReferenceInfo`: what an identifier or call node resolved to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Reference {
    /// The fully qualified identifier, struct type or function name.
    pub name: Option<String>,
    /// The matching overload ids of a function reference.
    pub overload_ids: Vec<String>,
    /// The ids `checker.Cost` switches on: the cel-go overload id where the
    /// declaration maps to one ([`crate::overload_ids`]), else the same as
    /// `overload_ids`.
    pub cost_overload_ids: Vec<String>,
}

/// The checked AST: `ast.CheckedAST` without the AST itself.
#[derive(Clone, Debug)]
pub struct CheckedAst {
    pub root: u64,
    pub type_map: HashMap<u64, DType>,
    pub ref_map: HashMap<u64, Reference>,
}

impl CheckedAst {
    /// `ast.AST.GetType(root)`: the type of the whole expression.
    pub fn output_type(&self) -> Option<&DType> {
        self.type_map.get(&self.root)
    }
}

/// `checker.Check`. The result may be unusable when `Errors` is not empty.
pub fn check(expr: &IdedExpr, env: &mut Env, locator: &dyn Locator) -> (CheckedAst, Errors) {
    let mut c = Checker {
        env,
        loc: locator,
        errors: Errors::default(),
        mappings: Mapping::new(),
        free_type_vars: 0,
        type_map: HashMap::new(),
        ref_map: HashMap::new(),
    };
    c.check(expr);
    // Substitute any type parameters either by their bound value or by DYN.
    let mappings = c.mappings.clone();
    for t in c.type_map.values_mut() {
        *t = substitute(&mappings, t, true);
    }
    (
        CheckedAst {
            root: expr.id,
            type_map: c.type_map,
            ref_map: c.ref_map,
        },
        c.errors,
    )
}

struct Checker<'a> {
    env: &'a mut Env,
    loc: &'a dyn Locator,
    errors: Errors,
    mappings: Mapping,
    free_type_vars: usize,
    type_map: HashMap<u64, DType>,
    ref_map: HashMap<u64, Reference>,
}

/// `containers.ToQualifiedName`.
fn to_qualified_name(e: &IdedExpr) -> Option<String> {
    match &e.expr {
        Expr::Ident(n) => Some(n.clone()),
        Expr::Select(s) if !s.test => {
            Some(format!("{}.{}", to_qualified_name(&s.operand)?, s.field))
        }
        _ => None,
    }
}

fn expr_kind(e: &Expr) -> &'static str {
    match e {
        Expr::Unspecified => "unspecified",
        Expr::Call(_) => "call",
        Expr::Comprehension(_) => "comprehension",
        Expr::Ident(_) => "ident",
        Expr::List(_) => "list",
        Expr::Literal(_) => "literal",
        Expr::Map(_) => "map",
        Expr::Select(_) => "select",
        Expr::Struct(_) => "struct",
    }
}

/// `isWellKnownType`.
fn is_well_known_type(t: &DType) -> bool {
    match t {
        DType::Any | DType::Timestamp | DType::Duration | DType::Dyn | DType::Null => true,
        DType::List(p) => **p == DType::Dyn,
        DType::Map(k, v) => **k == DType::Str && **v == DType::Dyn,
        _ => false,
    }
}

/// `getWellKnownTypeName`.
fn well_known_type_name(t: &DType) -> &'static str {
    match t {
        DType::Any => "google.protobuf.Any",
        DType::Bool => "google.protobuf.BoolValue",
        DType::Bytes => "google.protobuf.BytesValue",
        DType::Double => "google.protobuf.DoubleValue",
        DType::Duration => "google.protobuf.Duration",
        DType::Dyn => "google.protobuf.Value",
        DType::Int => "google.protobuf.Int64Value",
        DType::List(_) => "google.protobuf.ListValue",
        DType::Null => "google.protobuf.NullValue",
        DType::Map(..) => "google.protobuf.Struct",
        DType::Str => "google.protobuf.StringValue",
        DType::Timestamp => "google.protobuf.Timestamp",
        DType::Uint => "google.protobuf.UInt64Value",
        _ => "",
    }
}

impl Checker<'_> {
    fn check(&mut self, e: &IdedExpr) {
        match &e.expr {
            Expr::Literal(l) => {
                let t = match l {
                    LiteralValue::Boolean(_) => DType::Bool,
                    LiteralValue::Bytes(_) => DType::Bytes,
                    LiteralValue::Double(_) => DType::Double,
                    LiteralValue::Int(_) => DType::Int,
                    LiteralValue::Null => DType::Null,
                    LiteralValue::String(_) => DType::Str,
                    LiteralValue::UInt(_) => DType::Uint,
                };
                self.set_type(e.id, t);
            }
            Expr::Ident(_) => self.check_ident(e),
            Expr::Select(_) => self.check_select(e),
            Expr::Call(_) => self.check_call(e),
            Expr::List(_) => self.check_create_list(e),
            Expr::Map(_) => self.check_create_map(e),
            Expr::Struct(_) => self.check_create_struct(e),
            Expr::Comprehension(_) => self.check_comprehension(e),
            Expr::Unspecified => {
                self.errors
                    .unexpected_ast_type(e.id, self.loc, "unspecified", expr_kind(&e.expr))
            }
        }
    }

    fn check_ident(&mut self, e: &IdedExpr) {
        let Expr::Ident(name) = &e.expr else { return };
        if let Some(ident) = self.env.lookup_ident(name) {
            self.set_type(e.id, ident.ty.clone());
            self.set_reference(e.id, Reference::named(&ident.name));
            return;
        }
        self.set_type(e.id, DType::Error);
        self.errors
            .undeclared_reference(e.id, self.loc, self.env.container().name(), name);
    }

    fn check_select(&mut self, e: &IdedExpr) {
        let Expr::Select(sel) = &e.expr else { return };
        // Before traversing down the tree, try to interpret as qualified name.
        if let Some(qname) = to_qualified_name(e) {
            if let Some(ident) = self.env.lookup_ident(&qname) {
                self.set_type(e.id, ident.ty.clone());
                self.set_reference(e.id, Reference::named(&ident.name));
                return;
            }
        }
        let mut result = self.check_select_field(e.id, &sel.operand, &sel.field, false);
        if sel.test {
            result = DType::Bool;
        }
        let t = substitute(&self.mappings, &result, false);
        self.set_type(e.id, t);
    }

    fn check_opt_select(&mut self, e: &IdedExpr) {
        let Expr::Call(call) = &e.expr else { return };
        if call.args.len() != 2 || call.target.is_some() {
            let t = if call.target.is_some() {
                " member call with"
            } else {
                ""
            };
            self.errors.not_an_optional_field_selection_call(
                e.id,
                self.loc,
                &format!(
                    "incorrect signature.{t} argument count: {}",
                    call.args.len()
                ),
            );
            return;
        }
        let operand = &call.args[0];
        let field = &call.args[1];
        let Expr::Literal(LiteralValue::String(s)) = &field.expr else {
            self.errors
                .not_an_optional_field_selection(field.id, self.loc, expr_kind(&field.expr));
            return;
        };
        let name = s.inner().to_string();
        let result = self.check_select_field(e.id, operand, &name, true);
        let t = substitute(&self.mappings, &result, false);
        self.set_type(e.id, t);
        self.set_reference(
            e.id,
            Reference {
                name: None,
                overload_ids: vec!["select_optional_field".into()],
                cost_overload_ids: vec!["select_optional_field".into()],
            },
        );
    }

    fn check_select_field(
        &mut self,
        id: u64,
        operand: &IdedExpr,
        field: &str,
        optional: bool,
    ) -> DType {
        self.check(operand);
        let operand_type = substitute(&self.mappings, &self.get_type(operand.id), false);
        // If the target type is 'optional', unwrap it for the sake of this check.
        let (target, is_opt) = operand_type.maybe_unwrap_optional();
        let target = target.clone();
        let result = match &target {
            // Maps yield their value type as the selection result type.
            DType::Map(_, v) => (**v).clone(),
            // Objects yield their field type declaration, but only if the field is defined.
            DType::Struct(name) => self
                .lookup_field_type(id, name, field)
                .unwrap_or(DType::Error),
            // Set the operand type to DYN to prevent assignment to a potentially incorrect
            // type later; the substitution for the type param is updated.
            DType::Param(_) => {
                self.is_assignable(&DType::Dyn, &target);
                DType::Dyn
            }
            // Dynamic / error values are treated as DYN type.
            other => {
                if !other.is_dyn_or_error() {
                    self.errors
                        .type_does_not_support_field_selection(id, self.loc, other);
                }
                DType::Dyn
            }
        };
        if is_opt || optional {
            DType::optional(result)
        } else {
            result
        }
    }

    fn check_call(&mut self, e: &IdedExpr) {
        let Expr::Call(call) = &e.expr else { return };
        let fn_name = call.func_name.as_str();
        if fn_name == OPT_SELECT {
            self.check_opt_select(e);
            return;
        }
        for arg in &call.args {
            self.check(arg);
        }

        // Regular static call with simple name.
        let Some(target) = call.target.as_deref() else {
            let Some(f) = self.env.lookup_function(fn_name) else {
                self.errors.undeclared_reference(
                    e.id,
                    self.loc,
                    self.env.container().name(),
                    fn_name,
                );
                self.set_type(e.id, DType::Error);
                return;
            };
            self.resolve_overload_or_error(e.id, &f, None, &call.args);
            return;
        };

        // If a receiver 'target' is present, it may either be a receiver function, or a
        // namespaced function, but not both. Given a.b.c() either a.b.c is a function or c
        // is a function with target a.b.
        if let Some(prefix) = to_qualified_name(target) {
            if let Some(f) = self.env.lookup_function(&format!("{prefix}.{fn_name}")) {
                self.resolve_overload_or_error(e.id, &f, None, &call.args);
                return;
            }
        }

        // Regular instance call.
        self.check(target);
        match self.env.lookup_function(fn_name) {
            Some(f) => self.resolve_overload_or_error(e.id, &f, Some(target), &call.args),
            None => {
                self.set_type(e.id, DType::Error);
                self.errors.undeclared_reference(
                    e.id,
                    self.loc,
                    self.env.container().name(),
                    fn_name,
                );
            }
        }
    }

    fn resolve_overload_or_error(
        &mut self,
        id: u64,
        f: &FunctionDecl,
        target: Option<&IdedExpr>,
        args: &[IdedExpr],
    ) {
        match self.resolve_overload(id, f, target, args) {
            None => self.set_type(id, DType::Error),
            Some((t, r)) => {
                self.set_type(id, t);
                self.set_reference(id, r);
            }
        }
    }

    fn resolve_overload(
        &mut self,
        call_id: u64,
        f: &FunctionDecl,
        target: Option<&IdedExpr>,
        args: &[IdedExpr],
    ) -> Option<(DType, Reference)> {
        let mut arg_types: Vec<DType> = Vec::new();
        if let Some(t) = target {
            arg_types.push(self.get_type(t.id));
        }
        for a in args {
            arg_types.push(self.get_type(a.id));
        }

        let mut result: Option<DType> = None;
        let mut checked: Option<Reference> = None;
        for o in &f.overloads {
            if self.env.is_overload_disabled(&f.name, o) {
                continue;
            }
            // Ensure the call style for the overload matches.
            if target.is_none() == o.member {
                continue;
            }

            // Alternative type-checking behavior when the logical operators are compacted
            // into variadic AST representations.
            if f.name == "_&&_" || f.name == "_||_" {
                let r = Reference {
                    name: Some(f.name.clone()),
                    overload_ids: vec![o.id.clone()],
                    cost_overload_ids: vec![o.cost_id()],
                };
                let mut failed = false;
                for (i, at) in arg_types.iter().enumerate() {
                    if !self.is_assignable(at, &DType::Bool) {
                        self.errors
                            .type_mismatch(args[i].id, self.loc, &DType::Bool, at);
                        failed = true;
                    }
                }
                return if failed { None } else { Some((DType::Bool, r)) };
            }

            // Instantiate the overload's type with fresh type variables.
            let tps = type_params(o);
            let (cand_args, cand_result) = if tps.is_empty() {
                (o.args.clone(), o.result.clone())
            } else {
                let mut subs = Mapping::new();
                for p in tps {
                    let v = self.new_type_var();
                    subs.insert(p, v);
                }
                (
                    o.args.iter().map(|a| substitute(&subs, a, false)).collect(),
                    substitute(&subs, &o.result, false),
                )
            };

            if is_assignable_list_in(&mut self.mappings, &arg_types, &cand_args) {
                match &mut checked {
                    None => {
                        checked = Some(Reference {
                            name: Some(f.name.clone()),
                            overload_ids: vec![o.id.clone()],
                            cost_overload_ids: vec![o.cost_id()],
                        })
                    }
                    Some(r) => {
                        r.overload_ids.push(o.id.clone());
                        r.cost_overload_ids.push(o.cost_id());
                    }
                }
                // First matching overload determines the result type.
                let fn_result = substitute(&self.mappings, &cand_result, false);
                match &result {
                    None => result = Some(fn_result),
                    Some(prev) if !prev.is_dyn() && fn_result != *prev => result = Some(DType::Dyn),
                    Some(_) => {}
                }
            }
        }

        match result {
            Some(t) => Some((t, checked.unwrap_or_default())),
            None => {
                let shown: Vec<DType> = arg_types
                    .iter()
                    .map(|a| substitute(&self.mappings, a, true))
                    .collect();
                self.errors.no_matching_overload(
                    call_id,
                    self.loc,
                    &f.name,
                    &shown,
                    target.is_some(),
                );
                None
            }
        }
    }

    /// The type of an optional element/entry/field: unwrap `optional_type`,
    /// reporting a mismatch for a non-optional, non-dyn value.
    fn unwrap_optional_value(&mut self, e: &IdedExpr, t: DType) -> DType {
        let (inner, is_opt) = t.maybe_unwrap_optional();
        let inner = inner.clone();
        if !is_opt && !inner.is_dyn() {
            self.errors
                .type_mismatch(e.id, self.loc, &DType::optional(inner.clone()), &inner);
        }
        inner
    }

    fn check_create_list(&mut self, e: &IdedExpr) {
        let Expr::List(list) = &e.expr else { return };
        let mut elems: Option<DType> = None;
        for (i, el) in list.elements.iter().enumerate() {
            self.check(el);
            let mut t = self.get_type(el.id);
            if list.optional_indices.contains(&i) {
                t = self.unwrap_optional_value(el, t);
            }
            elems = Some(self.join_types(el.id, elems, t));
        }
        // If the list is empty, assign free type var to elem type.
        let elems = elems.unwrap_or_else(|| self.new_type_var());
        self.set_type(e.id, DType::List(Box::new(elems)));
    }

    fn check_create_map(&mut self, e: &IdedExpr) {
        let Expr::Map(map) = &e.expr else { return };
        let mut key_t: Option<DType> = None;
        let mut val_t: Option<DType> = None;
        for entry in &map.entries {
            let EntryExpr::MapEntry(me) = &entry.expr else {
                continue;
            };
            self.check(&me.key);
            let kt = self.get_type(me.key.id);
            key_t = Some(self.join_types(me.key.id, key_t, kt));

            self.check(&me.value);
            let mut vt = self.get_type(me.value.id);
            if me.optional {
                vt = self.unwrap_optional_value(&me.value, vt);
            }
            val_t = Some(self.join_types(me.value.id, val_t, vt));
        }
        // If the map is empty, assign free type variables to key and value type.
        let (k, v) = match (key_t, val_t) {
            (Some(k), Some(v)) => (k, v),
            _ => (self.new_type_var(), self.new_type_var()),
        };
        self.set_type(e.id, DType::Map(Box::new(k), Box::new(v)));
    }

    fn check_create_struct(&mut self, e: &IdedExpr) {
        let Expr::Struct(msg) = &e.expr else { return };
        let mut result = DType::Error;
        let Some(ident) = self.env.lookup_ident(&msg.type_name) else {
            self.errors.undeclared_reference(
                e.id,
                self.loc,
                self.env.container().name(),
                &msg.type_name,
            );
            self.set_type(e.id, DType::Error);
            return;
        };
        // The type name is fully qualified in the reference.
        let mut type_name = ident.name.clone();
        self.set_reference(e.id, Reference::named(&type_name));
        match &ident.ty {
            DType::Error => {}
            DType::Type(param) => {
                let param = param.as_deref().cloned().unwrap_or(DType::Dyn);
                if is_well_known_type(&param) {
                    type_name = well_known_type_name(&param).to_string();
                    result = param;
                } else if let DType::Struct(n) = &param {
                    type_name = n.clone();
                    result = param;
                } else {
                    self.errors
                        .not_a_message_type(e.id, self.loc, &param.name());
                }
            }
            other => self.errors.not_a_type(e.id, self.loc, &other.name()),
        }
        self.set_type(e.id, result);

        // Check the field initializers.
        for entry in &msg.entries {
            let EntryExpr::StructField(f) = &entry.expr else {
                continue;
            };
            self.check(&f.value);
            let field_t = self
                .lookup_field_type(entry.id, &type_name, &f.field)
                .unwrap_or(DType::Error);
            let mut val_t = self.get_type(f.value.id);
            if f.optional {
                val_t = self.unwrap_optional_value(&f.value, val_t);
            }
            if !self.is_assignable(&field_t, &val_t) {
                self.errors
                    .field_type_mismatch(entry.id, self.loc, &f.field, &field_t, &val_t);
            }
        }
    }

    fn check_comprehension(&mut self, e: &IdedExpr) {
        let Expr::Comprehension(comp) = &e.expr else {
            return;
        };
        self.check(&comp.iter_range);
        self.check(&comp.accu_init);
        let range_t = substitute(&self.mappings, &self.get_type(comp.iter_range.id), false);

        // A scope for the comprehension's accumulation variable.
        let accu_t = self.get_type(comp.accu_init.id);
        self.env.enter_scope();
        self.add_var(&comp.accu_var, accu_t.clone());

        let two = comp.iter_var2.is_some();
        let (var_t, var2_t) = match &range_t {
            DType::List(el) => {
                if two {
                    (DType::Int, Some((**el).clone()))
                } else {
                    ((**el).clone(), None)
                }
            }
            DType::Map(k, v) => ((**k).clone(), two.then(|| (**v).clone())),
            DType::Dyn | DType::Any | DType::Error | DType::Param(_) => {
                self.is_assignable(&DType::Dyn, &range_t);
                (DType::Dyn, two.then_some(DType::Dyn))
            }
            other => {
                self.errors
                    .not_a_comprehension_range(comp.iter_range.id, self.loc, other);
                (DType::Error, two.then_some(DType::Error))
            }
        };

        // A block scope for the loop.
        self.env.enter_scope();
        self.add_var(&comp.iter_var, var_t);
        if let (Some(n), Some(t)) = (&comp.iter_var2, var2_t) {
            self.add_var(n, t);
        }
        self.check(&comp.loop_cond);
        self.assert_type(&comp.loop_cond, &DType::Bool);
        self.check(&comp.loop_step);
        self.assert_type(&comp.loop_step, &accu_t);
        self.env.exit_scope();
        self.check(&comp.result);
        self.env.exit_scope();
        let t = substitute(&self.mappings, &self.get_type(comp.result.id), false);
        self.set_type(e.id, t);
    }

    fn add_var(&mut self, name: &str, t: DType) {
        // The returned error (overlapping identifier) is ignored, as in cel-go.
        self.env
            .add_ident(crate::env::VariableDecl::new(name, t))
            .ok();
    }

    /// Checks compatibility of joined types, and returns the most general
    /// common type.
    fn join_types(&mut self, id: u64, previous: Option<DType>, current: DType) -> DType {
        let Some(previous) = previous else {
            return current;
        };
        if self.is_assignable(&previous, &current) {
            return most_general(&previous, &current);
        }
        if !self.env.homogeneous_literals {
            return DType::Dyn;
        }
        self.errors.type_mismatch(id, self.loc, &previous, &current);
        DType::Error
    }

    fn new_type_var(&mut self) -> DType {
        let id = self.free_type_vars;
        self.free_type_vars += 1;
        DType::Param(format!("_var{id}"))
    }

    fn is_assignable(&mut self, t1: &DType, t2: &DType) -> bool {
        is_assignable_in(&mut self.mappings, t1, t2)
    }

    fn set_type(&mut self, id: u64, t: DType) {
        if let Some(old) = self.type_map.get(&id) {
            if *old != t {
                let old = old.clone();
                self.errors.incompatible_type(id, self.loc, &old, &t);
            }
            return;
        }
        self.type_map.insert(id, t);
    }

    fn get_type(&self, id: u64) -> DType {
        self.type_map.get(&id).cloned().unwrap_or(DType::Error)
    }

    fn set_reference(&mut self, id: u64, r: Reference) {
        if let Some(old) = self.ref_map.get(&id) {
            if *old != r {
                self.errors
                    .incompatible_type(id, self.loc, &DType::Error, &DType::Error);
            }
            return;
        }
        self.ref_map.insert(id, r);
    }

    fn assert_type(&mut self, e: &IdedExpr, t: &DType) {
        let actual = self.get_type(e.id);
        if !self.is_assignable(t, &actual) {
            self.errors.type_mismatch(e.id, self.loc, t, &actual);
        }
    }

    /// `checker.lookupFieldType`.
    fn lookup_field_type(&mut self, id: u64, struct_type: &str, field: &str) -> Option<DType> {
        if self.env.provider().find_struct_type(struct_type).is_none() {
            // This should not happen, anyway, report an error.
            self.errors
                .unexpected_failed_resolution(id, self.loc, struct_type);
            return None;
        }
        if let Some(t) = self
            .env
            .provider()
            .find_struct_field_type(struct_type, field)
        {
            return Some(t);
        }
        self.errors.undefined_field(id, self.loc, field);
        None
    }
}

impl Reference {
    fn named(name: &str) -> Self {
        Reference {
            name: Some(name.to_string()),
            overload_ids: vec![],
            cost_overload_ids: vec![],
        }
    }
}
