//! The typed CEL environment and worst-case cost estimate of a DRA device
//! selector (`CELDeviceSelector.expression`).
//!
//! Upstream compiles every selector with
//! `dracel.GetCompiler(..).CompileCELExpression`
//! (`staging/src/k8s.io/dynamic-resource-allocation/cel/compile.go:150-218`):
//! the expression is type-checked in an environment that declares one
//! variable, `device` (`compile.go:57-103`, `newCompiler`), its output type
//! must be `bool` or `any` (`compile.go:169-174`), and
//! `env.EstimateCost` bounds its worst-case cost, which
//! `validateCELSelector` compares to `CELSelectorExpressionMaxCost`
//! (`pkg/apis/resource/validation/validation.go:316-345`).
//!
//! The `cel` crate has a parser and an interpreter but no checker and no cost
//! estimator, so cel-go's checker cannot be called. This module ports the two
//! pieces over the crate's parsed AST:
//!
//! * a type pass for the part of the checker that needs no function
//!   declarations: the type of `device` and its fields, container element
//!   types, comparison / logical operators and macro iteration variables,
//!   reporting `_[_]` applied to a key of the wrong type, an undefined field of
//!   `device`, and an output type that is neither `bool` nor `any`. Anything it
//!   cannot type (a call to a library function) is `dyn`, which type-checks,
//!   so it never rejects what upstream accepts;
//! * `checker.Cost` (`vendor/github.com/google/cel-go/checker/cost.go`,
//!   `coster`) with the DRA `sizeEstimator` (`compile.go:417-475`).
//!
//! Not ported: the Kubernetes function libraries' `EstimateCallCost`
//! (`apiserver/pkg/cel/library/cost.go`), which prices e.g. `quantity()` and
//! `semver` calls above the base cost of 1. A selector can therefore be
//! estimated lower than upstream estimates it, never higher.
//!
//! Positions are reconstructed from the source text because the crate drops
//! its source info on a successful parse.

use std::collections::HashMap;

use cel::common::ast::{operators, EntryExpr, Expr, IdedExpr, LiteralValue};

/// `resourceapi.CELSelectorExpressionMaxCost` (`staging/src/k8s.io/api/resource/v1/types.go:1245`).
pub const CEL_SELECTOR_EXPRESSION_MAX_COST: u64 = 1_000_000;

/// `resourceapi.DriverNameMaxLength` (types.go:223).
const DRIVER_NAME_MAX_LENGTH: u64 = 63;
/// `resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice` (types.go:569).
const MAX_ATTRIBUTES_AND_CAPACITIES_PER_DEVICE: u64 = 32;
/// `resourceapi.DeviceMaxDomainLength` (types.go:596).
const DEVICE_MAX_DOMAIN_LENGTH: u64 = 63;
/// `resourceapi.DeviceMaxIDLength` (types.go:599).
const DEVICE_MAX_ID_LENGTH: u64 = 32;
/// `resourceapi.DeviceAttributeMaxValueLength` (types.go:638).
const DEVICE_ATTRIBUTE_MAX_VALUE_LENGTH: u64 = 64;
/// `apiservercel.QuantityDeclType` minimum size (`apiserver/pkg/cel/types.go:588`).
const QUANTITY_SIZE: u64 = 8;

/// The outcome of compiling a selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorCompilation {
    /// It compiles; `max_cost` is `CompilationResult.MaxCost`.
    Ok { max_cost: u64 },
    /// `Error{Type: ErrorTypeInvalid, Detail: ..}`.
    Invalid(String),
}

/// `CompileCELExpression` for a DRA selector. Callers parse first; an
/// expression the crate cannot parse is reported by them, so it is `Ok` here.
pub fn compile_selector(expression: &str) -> SelectorCompilation {
    let source = expression.to_string();
    let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::parser::Parser::new().parse(&source)
    }));
    let Ok(Ok(root)) = parsed else {
        return SelectorCompilation::Ok { max_cost: 0 };
    };

    let mut checker = Checker::new(expression);
    let mut scope: Vec<(String, Ty)> = Vec::new();
    let out = checker.infer(&root, &mut scope);
    if let Some(msg) = checker.error.take() {
        return SelectorCompilation::Invalid(format!("compilation failed: {msg}"));
    }
    if !matches!(out, Ty::Bool | Ty::Any | Ty::Dyn) && out.fully_known() {
        return SelectorCompilation::Invalid(format!(
            "must evaluate to bool or the unknown type, not {}",
            out.name()
        ));
    }

    let mut coster = Coster::new(&checker.types);
    let cost = coster.cost(&root);
    SelectorCompilation::Ok { max_cost: cost.max }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Ty {
    Bool,
    Int,
    Uint,
    Double,
    Str,
    Bytes,
    Null,
    /// `dyn`: unchecked.
    Dyn,
    /// `any`: the attribute value type.
    Any,
    /// `apiservercel.Quantity`: opaque, only used through library functions.
    Quantity,
    /// The `device` variable.
    Device,
    List(Box<Ty>),
    Map(Box<Ty>, Box<Ty>),
}

impl Ty {
    fn name(&self) -> String {
        match self {
            Ty::Bool => "bool".into(),
            Ty::Int => "int".into(),
            Ty::Uint => "uint".into(),
            Ty::Double => "double".into(),
            Ty::Str => "string".into(),
            Ty::Bytes => "bytes".into(),
            Ty::Null => "null_type".into(),
            Ty::Dyn => "dyn".into(),
            Ty::Any => "any".into(),
            Ty::Quantity => "apiserver.cel.Quantity".into(),
            Ty::Device => "device".into(),
            Ty::List(t) => format!("list({})", t.name()),
            Ty::Map(k, v) => format!("map({}, {})", k.name(), v.name()),
        }
    }

    /// Unchecked: anything is assignable to and from it.
    fn is_unchecked(&self) -> bool {
        matches!(self, Ty::Dyn | Ty::Any | Ty::Quantity)
    }

    /// No `dyn` anywhere in it, so its name is what upstream would print.
    fn fully_known(&self) -> bool {
        match self {
            Ty::Dyn => false,
            Ty::List(t) => t.fully_known(),
            Ty::Map(k, v) => k.fully_known() && v.fully_known(),
            _ => true,
        }
    }

    fn is_scalar(&self) -> bool {
        matches!(self, Ty::Bool | Ty::Int | Ty::Uint | Ty::Double)
    }
}

fn unify(types: impl Iterator<Item = Ty>) -> Ty {
    let mut out: Option<Ty> = None;
    for t in types {
        match &out {
            None => out = Some(t),
            Some(prev) if *prev == t => {}
            Some(_) => return Ty::Dyn,
        }
    }
    out.unwrap_or(Ty::Dyn)
}

fn device_field(name: &str) -> Option<Ty> {
    match name {
        "driver" => Some(Ty::Str),
        "allowMultipleAllocations" => Some(Ty::Bool),
        // `outerAttributesMapType` / `outerCapacityMapType` (compile.go:78-87).
        "attributes" => Some(Ty::Map(
            Box::new(Ty::Str),
            Box::new(Ty::Map(Box::new(Ty::Str), Box::new(Ty::Any))),
        )),
        "capacity" => Some(Ty::Map(
            Box::new(Ty::Str),
            Box::new(Ty::Map(Box::new(Ty::Str), Box::new(Ty::Quantity))),
        )),
        _ => None,
    }
}

/// Offsets (in characters) of the `[` of every index operator and of every
/// `.` of a selection or member call, in source order. A literal's `[`, a
/// float's `.` and anything inside a string or comment is not one.
fn operator_offsets(source: &str) -> (Vec<usize>, Vec<usize>) {
    let chars: Vec<char> = source.chars().collect();
    let mut brackets = Vec::new();
    let mut dots = Vec::new();
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    let mut i = 0;
    let mut prev: Option<char> = None; // previous significant char
    let mut prev_word = String::new();
    while i < chars.len() {
        let c = chars[i];
        // Comments.
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // Strings (raw prefix irrelevant to finding the end, except escapes).
        if c == '"' || c == '\'' {
            let raw = i > 0 && matches!(chars[i - 1], 'r' | 'R');
            let triple = chars.get(i + 1) == Some(&c) && chars.get(i + 2) == Some(&c);
            let n = if triple { 3 } else { 1 };
            i += n;
            while i < chars.len() {
                if chars[i] == '\\' && !raw {
                    i += 2;
                    continue;
                }
                if chars[i] == c
                    && (n == 1 || (chars.get(i + 1) == Some(&c) && chars.get(i + 2) == Some(&c)))
                {
                    i += n;
                    break;
                }
                i += 1;
            }
            prev = Some('"');
            prev_word.clear();
            continue;
        }
        match c {
            '[' => {
                let is_index = match prev {
                    Some(p) if ident(p) => prev_word != "in",
                    Some(')') | Some(']') | Some('}') | Some('"') => true,
                    _ => false,
                };
                if is_index {
                    brackets.push(i);
                }
            }
            '.' => {
                let next_digit = chars.get(i + 1).is_some_and(|n| n.is_ascii_digit());
                let prev_ident_like =
                    prev.is_some_and(|p| ident(p) || p == ')' || p == ']' || p == '"');
                let prev_digit_word = prev_word.chars().next().is_some_and(|f| f.is_ascii_digit());
                if next_digit && (!prev_ident_like || prev_digit_word) {
                    // a float literal
                } else {
                    dots.push(i);
                }
            }
            _ => {}
        }
        if !c.is_whitespace() {
            if ident(c) {
                if prev.is_some_and(ident) {
                    prev_word.push(c);
                } else {
                    prev_word = c.to_string();
                }
            } else {
                prev_word.clear();
            }
            prev = Some(c);
        }
        i += 1;
    }
    (brackets, dots)
}

struct Checker<'a> {
    source: &'a str,
    brackets: Vec<usize>,
    dots: Vec<usize>,
    next_bracket: usize,
    next_dot: usize,
    /// False while visiting the synthesized parts of a macro expansion, which
    /// hold none of the source's operators.
    counting: bool,
    error: Option<String>,
    types: HashMap<u64, Ty>,
}

impl<'a> Checker<'a> {
    fn new(source: &'a str) -> Self {
        let (brackets, dots) = operator_offsets(source);
        Self {
            source,
            brackets,
            dots,
            next_bracket: 0,
            next_dot: 0,
            counting: true,
            error: None,
            types: HashMap::new(),
        }
    }

    fn take_bracket(&mut self) -> Option<usize> {
        if !self.counting {
            return None;
        }
        let v = self.brackets.get(self.next_bracket).copied();
        self.next_bracket += 1;
        v
    }

    fn take_dot(&mut self) -> Option<usize> {
        if !self.counting {
            return None;
        }
        let v = self.dots.get(self.next_dot).copied();
        self.next_dot += 1;
        v
    }

    /// cel-go's `common.Errors` rendering: `ERROR: <input>:L:C: msg`, the
    /// source line and a caret under the column.
    fn report(&mut self, offset: Option<usize>, msg: String) {
        if self.error.is_some() {
            return;
        }
        let offset = offset.unwrap_or(0);
        let (mut line, mut col) = (1usize, 1usize);
        for (i, c) in self.source.chars().enumerate() {
            if i == offset {
                break;
            }
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        let text = self.source.lines().nth(line - 1).unwrap_or("");
        self.error = Some(format!(
            "ERROR: <input>:{line}:{col}: {msg}\n | {text}\n | {}^",
            ".".repeat(col - 1)
        ));
    }

    fn infer(&mut self, e: &IdedExpr, scope: &mut Vec<(String, Ty)>) -> Ty {
        let t = self.infer_inner(e, scope);
        self.types.insert(e.id, t.clone());
        t
    }

    fn infer_inner(&mut self, e: &IdedExpr, scope: &mut Vec<(String, Ty)>) -> Ty {
        match &e.expr {
            Expr::Literal(l) => match l {
                LiteralValue::Boolean(_) => Ty::Bool,
                LiteralValue::Int(_) => Ty::Int,
                LiteralValue::UInt(_) => Ty::Uint,
                LiteralValue::Double(_) => Ty::Double,
                LiteralValue::String(_) => Ty::Str,
                LiteralValue::Bytes(_) => Ty::Bytes,
                LiteralValue::Null => Ty::Null,
            },
            Expr::Ident(name) => {
                if let Some((_, t)) = scope.iter().rev().find(|(n, _)| n == name) {
                    t.clone()
                } else if name == "device" {
                    Ty::Device
                } else {
                    Ty::Dyn
                }
            }
            Expr::List(l) => {
                let tys: Vec<Ty> = l.elements.iter().map(|x| self.infer(x, scope)).collect();
                Ty::List(Box::new(unify(tys.into_iter())))
            }
            Expr::Map(m) => {
                let (mut ks, mut vs) = (Vec::new(), Vec::new());
                for ent in &m.entries {
                    if let EntryExpr::MapEntry(me) = &ent.expr {
                        ks.push(self.infer(&me.key, scope));
                        vs.push(self.infer(&me.value, scope));
                    }
                }
                Ty::Map(
                    Box::new(unify(ks.into_iter())),
                    Box::new(unify(vs.into_iter())),
                )
            }
            Expr::Struct(_) => Ty::Dyn,
            Expr::Select(s) => {
                let operand = self.infer(&s.operand, scope);
                let dot = self.take_dot();
                if s.test {
                    return Ty::Bool;
                }
                match operand {
                    Ty::Device => match device_field(&s.field) {
                        Some(t) => t,
                        None => {
                            self.report(dot, format!("undefined field '{}'", s.field));
                            Ty::Dyn
                        }
                    },
                    Ty::Map(_, v) => *v,
                    _ => Ty::Dyn,
                }
            }
            Expr::Call(c) => self.infer_call(c, scope),
            Expr::Comprehension(c) => {
                let range = self.infer(&c.iter_range, scope);
                let _dot = self.take_dot();
                let was = std::mem::replace(&mut self.counting, false);
                let accu = self.infer(&c.accu_init, scope);
                let accu = if let Ty::List(_) = accu {
                    Ty::List(Box::new(Ty::Dyn))
                } else {
                    accu
                };
                let elem = match range {
                    Ty::List(t) => *t,
                    Ty::Map(k, _) => *k,
                    _ => Ty::Dyn,
                };
                let pushed = scope.len();
                scope.push((c.accu_var.clone(), accu));
                scope.push((c.iter_var.clone(), elem));
                if let Some(v2) = &c.iter_var2 {
                    scope.push((v2.clone(), Ty::Dyn));
                }
                self.infer(&c.loop_cond, scope);
                self.counting = was;
                self.infer(&c.loop_step, scope);
                self.counting = false;
                let result = self.infer(&c.result, scope);
                self.counting = was;
                scope.truncate(pushed);
                result
            }
            Expr::Unspecified => Ty::Dyn,
        }
    }

    fn infer_call(&mut self, c: &cel::common::ast::CallExpr, scope: &mut Vec<(String, Ty)>) -> Ty {
        let name = c.func_name.as_str();
        if (name == operators::INDEX || name == operators::OPT_INDEX) && c.args.len() == 2 {
            let container = self.infer(&c.args[0], scope);
            let at = self.take_bracket();
            let key = self.infer(&c.args[1], scope);
            let (ok, result) = match &container {
                Ty::Map(k, v) => (
                    k.is_unchecked() || key.is_unchecked() || **k == key,
                    (**v).clone(),
                ),
                Ty::List(t) => (key.is_unchecked() || key == Ty::Int, (**t).clone()),
                t if t.is_unchecked() => (true, Ty::Dyn),
                _ => (false, Ty::Dyn),
            };
            if !ok {
                self.report(
                    at,
                    format!(
                        "found no matching overload for '{}' applied to '({}, {})'",
                        name,
                        container.name(),
                        key.name()
                    ),
                );
            }
            return result;
        }

        let target = c.target.as_ref().map(|t| self.infer(t, scope));
        if c.target.is_some() {
            self.take_dot();
        }
        let args: Vec<Ty> = c.args.iter().map(|a| self.infer(a, scope)).collect();

        match name {
            operators::LOGICAL_AND
            | operators::LOGICAL_OR
            | operators::LOGICAL_NOT
            | operators::NOT_STRICTLY_FALSE
            | operators::EQUALS
            | operators::NOT_EQUALS
            | operators::LESS
            | operators::LESS_EQUALS
            | operators::GREATER
            | operators::GREATER_EQUALS
            | operators::IN => Ty::Bool,
            operators::CONDITIONAL => unify(args.iter().skip(1).cloned()),
            operators::ADD
            | operators::SUBSTRACT
            | operators::MULTIPLY
            | operators::DIVIDE
            | operators::MODULO => match (args.first(), args.get(1)) {
                (Some(a), Some(b))
                    if a == b
                        && (a.is_scalar() || matches!(a, Ty::Str | Ty::Bytes | Ty::List(_))) =>
                {
                    a.clone()
                }
                _ => Ty::Dyn,
            },
            operators::NEGATE => args
                .first()
                .filter(|t| t.is_scalar())
                .cloned()
                .unwrap_or(Ty::Dyn),
            "startsWith" | "endsWith" | "contains" | "matches" if target == Some(Ty::Str) => {
                Ty::Bool
            }
            "size" => Ty::Int,
            "int" => Ty::Int,
            "uint" => Ty::Uint,
            "double" => Ty::Double,
            "string" => Ty::Str,
            "bool" => Ty::Bool,
            "bytes" => Ty::Bytes,
            "quantity" => Ty::Quantity,
            _ => Ty::Dyn,
        }
    }
}

// ---------------------------------------------------------------------------
// Cost: `checker.Cost` (cel-go checker/cost.go) with the DRA sizeEstimator.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct Size {
    min: u64,
    max: u64,
}

impl Size {
    const UNKNOWN: Size = Size {
        min: 0,
        max: u64::MAX,
    };

    fn fixed(n: u64) -> Self {
        Size { min: n, max: n }
    }

    fn add(self, o: Size) -> Size {
        Size {
            min: self.min.saturating_add(o.min),
            max: self.max.saturating_add(o.max),
        }
    }

    fn union(self, o: Size) -> Size {
        Size {
            min: self.min.min(o.min),
            max: self.max.max(o.max),
        }
    }

    fn times_cost(self, c: Cost) -> Cost {
        Cost {
            min: self.min.saturating_mul(c.min),
            max: self.max.saturating_mul(c.max),
        }
    }

    fn times_factor(self, f: f64) -> Cost {
        Cost {
            min: mul_factor(self.min, f),
            max: mul_factor(self.max, f),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Cost {
    min: u64,
    max: u64,
}

impl Cost {
    fn fixed(n: u64) -> Self {
        Cost { min: n, max: n }
    }

    fn add(self, o: Cost) -> Cost {
        Cost {
            min: self.min.saturating_add(o.min),
            max: self.max.saturating_add(o.max),
        }
    }

    fn mul(self, o: Cost) -> Cost {
        Cost {
            min: self.min.saturating_mul(o.min),
            max: self.max.saturating_mul(o.max),
        }
    }

    fn union(self, o: Cost) -> Cost {
        Cost {
            min: self.min.min(o.min),
            max: self.max.max(o.max),
        }
    }
}

/// `multiplyByCostFactor`: the product rounded up, saturating.
fn mul_factor(x: u64, f: f64) -> u64 {
    let v = (x as f64 * f).ceil();
    if v >= 18446744073709551616.0 {
        u64::MAX
    } else {
        v as u64
    }
}

/// `common.StringTraversalCostFactor`, `RegexStringLengthCostFactor`,
/// `ListCreateBaseCost`, `MapCreateBaseCost`, `StructCreateBaseCost`
/// (`vendor/github.com/google/cel-go/common/cost.go`).
const STRING_TRAVERSAL: f64 = 0.1;
const REGEX_STRING_LENGTH: f64 = 0.25;
const LIST_CREATE_BASE: u64 = 10;
const MAP_CREATE_BASE: u64 = 30;
const STRUCT_CREATE_BASE: u64 = 40;
const CONST_COST: u64 = 0;
const SELECT_AND_IDENT: u64 = 1;

/// `entrySizeEstimate`.
#[derive(Clone, Copy, Debug)]
struct Entry {
    list: bool,
    key: Size,
    val: Size,
}

impl Entry {
    fn union(self, o: Entry) -> Entry {
        Entry {
            list: self.list,
            key: self.key.union(o.key),
            val: self.val.union(o.val),
        }
    }
}

#[derive(Clone, Debug)]
struct LocalVar {
    path: Vec<String>,
    size: Option<Size>,
    entry: Option<Entry>,
}

/// The DRA declaration tree the `sizeEstimator` walks (`compile.go:66-87`).
#[derive(Clone, Copy, Debug)]
enum Decl {
    Device,
    Str(u64),
    Bool,
    /// `attributeType`.
    Attribute,
    Quantity,
    /// Which of the four maps, so `elem` can be resolved without allocating.
    OuterAttributes,
    InnerAttributes,
    OuterCapacity,
    InnerCapacity,
}

impl Decl {
    fn max_elements(self) -> u64 {
        match self {
            Decl::Device => 4,
            Decl::Str(n) => n,
            Decl::Bool => 1,
            Decl::Attribute => DEVICE_ATTRIBUTE_MAX_VALUE_LENGTH,
            Decl::Quantity => QUANTITY_SIZE,
            _ => MAX_ATTRIBUTES_AND_CAPACITIES_PER_DEVICE,
        }
    }

    fn elem(self) -> Option<Decl> {
        match self {
            Decl::OuterAttributes => Some(Decl::InnerAttributes),
            Decl::InnerAttributes => Some(Decl::Attribute),
            Decl::OuterCapacity => Some(Decl::InnerCapacity),
            Decl::InnerCapacity => Some(Decl::Quantity),
            _ => None,
        }
    }

    fn key(self) -> Option<Decl> {
        match self {
            Decl::OuterAttributes | Decl::OuterCapacity => {
                Some(Decl::Str(DEVICE_MAX_DOMAIN_LENGTH))
            }
            Decl::InnerAttributes | Decl::InnerCapacity => Some(Decl::Str(DEVICE_MAX_ID_LENGTH)),
            _ => None,
        }
    }
}

/// `sizeEstimator.EstimateSize` (compile.go:425-475).
fn estimate_size(path: &[String]) -> Option<Size> {
    if path.first().map(String::as_str) != Some("device") {
        return None;
    }
    let mut cur = Decl::Device;
    for name in &path[1..] {
        match name.as_str() {
            "@items" | "@values" => cur = cur.elem()?,
            "@keys" => cur = cur.key()?,
            field => match (cur, field) {
                (Decl::Device, "driver") => cur = Decl::Str(DRIVER_NAME_MAX_LENGTH),
                (Decl::Device, "allowMultipleAllocations") => cur = Decl::Bool,
                (Decl::Device, "attributes") => cur = Decl::OuterAttributes,
                (Decl::Device, "capacity") => cur = Decl::OuterCapacity,
                // An attribute map's elements all have the maximum size of
                // `attributeType`, whatever their name.
                (Decl::InnerAttributes, _) => cur = Decl::Attribute,
                _ => return None,
            },
        }
    }
    Some(Size {
        min: 0,
        max: cur.max_elements(),
    })
}

struct Coster<'a> {
    types: &'a HashMap<u64, Ty>,
    paths: HashMap<u64, Vec<String>>,
    sizes: HashMap<u64, Size>,
    entries: HashMap<u64, Entry>,
    locals: HashMap<String, Vec<LocalVar>>,
}

impl<'a> Coster<'a> {
    fn new(types: &'a HashMap<u64, Ty>) -> Self {
        Self {
            types,
            paths: HashMap::new(),
            sizes: HashMap::new(),
            entries: HashMap::new(),
            locals: HashMap::new(),
        }
    }

    fn ty(&self, e: &IdedExpr) -> Ty {
        self.types.get(&e.id).cloned().unwrap_or(Ty::Dyn)
    }

    fn path(&self, e: &IdedExpr) -> Vec<String> {
        self.paths.get(&e.id).cloned().unwrap_or_default()
    }

    fn peek(&self, name: &str) -> Option<&LocalVar> {
        self.locals.get(name).and_then(|v| v.last())
    }

    fn push(&mut self, name: &str, v: LocalVar) {
        self.locals.entry(name.to_string()).or_default().push(v);
    }

    fn pop(&mut self, name: &str) {
        if let Some(v) = self.locals.get_mut(name) {
            v.pop();
        }
    }

    fn set_size(&mut self, e: &IdedExpr, s: Option<Size>) {
        if let Some(s) = s {
            self.sizes.insert(e.id, s);
        }
    }

    fn set_entry(&mut self, e: &IdedExpr, s: Option<Entry>) {
        if let Some(s) = s {
            self.entries.insert(e.id, s);
        }
    }

    fn entry(&self, e: &IdedExpr) -> Option<Entry> {
        if let Some(s) = self.entries.get(&e.id) {
            return Some(*s);
        }
        if let Expr::Ident(n) = &e.expr {
            return self.peek(n).and_then(|v| v.entry);
        }
        None
    }

    /// `computeSize`.
    fn compute_size(&mut self, e: &IdedExpr) -> Option<Size> {
        if let Some(s) = self.sizes.get(&e.id) {
            return Some(*s);
        }
        let expr_size = match &e.expr {
            Expr::Literal(LiteralValue::String(s)) => Some(s.inner().chars().count() as u64),
            Expr::Literal(LiteralValue::Bytes(b)) => Some(b.inner().len() as u64),
            Expr::Literal(_) => Some(1),
            Expr::List(l) => Some(l.elements.len() as u64),
            Expr::Map(m) => Some(m.entries.len() as u64),
            _ => None,
        };
        if let Some(n) = expr_size {
            return Some(Size::fixed(n));
        }
        let path = self.path(e);
        if !path.is_empty() {
            if let Some(s) = estimate_size(&path) {
                self.sizes.insert(e.id, s);
                return Some(s);
            }
        }
        if self.ty(e).is_scalar() {
            return Some(Size::fixed(1));
        }
        if let Expr::Ident(n) = &e.expr {
            if let Some(v) = self.peek(n) {
                return v.size;
            }
        }
        None
    }

    fn size_or_unknown(&mut self, e: &IdedExpr) -> Size {
        self.compute_size(e).unwrap_or(Size::UNKNOWN)
    }

    fn copy_estimates(&mut self, dst: &IdedExpr, src: &IdedExpr) {
        let s = self.compute_size(src);
        self.set_size(dst, s);
        let en = self.entry(src);
        self.set_entry(dst, en);
    }

    fn cost(&mut self, e: &IdedExpr) -> Cost {
        match &e.expr {
            Expr::Literal(_) => Cost::fixed(CONST_COST),
            Expr::Ident(name) => {
                let path = self
                    .peek(name)
                    .map(|v| v.path.clone())
                    .unwrap_or_else(|| vec![name.clone()]);
                self.paths.insert(e.id, path);
                Cost::fixed(SELECT_AND_IDENT)
            }
            Expr::Select(s) => {
                if s.test {
                    // `PresenceTestHasCost(true)`: the default.
                    return Cost::fixed(SELECT_AND_IDENT).add(self.cost(&s.operand));
                }
                let mut sum = self.cost(&s.operand);
                if matches!(self.ty(&s.operand), Ty::Map(..) | Ty::Device) {
                    sum = sum.add(Cost::fixed(SELECT_AND_IDENT));
                }
                let mut p = self.path(&s.operand);
                p.push(s.field.clone());
                self.paths.insert(e.id, p);
                sum
            }
            Expr::Call(_) => self.cost_call(e),
            Expr::List(l) => {
                let mut sum = Cost::default();
                let mut item = Size {
                    min: u64::MAX,
                    max: 0,
                };
                if l.elements.is_empty() {
                    item.min = 0;
                }
                for x in &l.elements {
                    sum = sum.add(self.cost(x));
                    let s = self.size_or_unknown(x);
                    item = item.union(s);
                }
                self.set_entry(
                    e,
                    Some(Entry {
                        list: true,
                        key: Size::fixed(1),
                        val: item,
                    }),
                );
                sum.add(Cost::fixed(LIST_CREATE_BASE))
            }
            Expr::Map(m) => {
                let mut sum = Cost::default();
                let mut key = Size {
                    min: u64::MAX,
                    max: 0,
                };
                let mut val = key;
                if m.entries.is_empty() {
                    key.min = 0;
                    val.min = 0;
                }
                for ent in &m.entries {
                    if let EntryExpr::MapEntry(me) = &ent.expr {
                        sum = sum.add(self.cost(&me.key)).add(self.cost(&me.value));
                        let ks = self.size_or_unknown(&me.key);
                        key = key.union(ks);
                        let vs = self.size_or_unknown(&me.value);
                        val = val.union(vs);
                    }
                }
                self.set_entry(
                    e,
                    Some(Entry {
                        list: false,
                        key,
                        val,
                    }),
                );
                sum.add(Cost::fixed(MAP_CREATE_BASE))
            }
            Expr::Struct(s) => {
                let mut sum = Cost::default();
                for ent in &s.entries {
                    if let EntryExpr::StructField(f) = &ent.expr {
                        sum = sum.add(self.cost(&f.value));
                    }
                }
                sum.add(Cost::fixed(STRUCT_CREATE_BASE))
            }
            Expr::Comprehension(c) => {
                let is_bind = matches!(&c.iter_range.expr, Expr::List(l) if l.elements.is_empty())
                    && matches!(&c.loop_cond.expr, Expr::Literal(LiteralValue::Boolean(b)) if !*b.inner())
                    && c.accu_var != "@result";
                if is_bind {
                    return self.cost_bind(e, c);
                }
                self.cost_comprehension(e, c)
            }
            Expr::Unspecified => Cost::default(),
        }
    }

    fn push_local(&mut self, name: &str, init: &IdedExpr) {
        let path = self.path(init);
        let entry = self.entry(init);
        let size = self.compute_size(init);
        self.push(name, LocalVar { path, size, entry });
    }

    fn cost_bind(&mut self, e: &IdedExpr, c: &cel::common::ast::ComprehensionExpr) -> Cost {
        let mut sum = self.cost(&c.iter_range).add(self.cost(&c.accu_init));
        self.push_local(&c.accu_var, &c.accu_init);
        sum = sum.add(self.cost(&c.result));
        self.pop(&c.accu_var);
        self.copy_estimates(e, &c.result);
        sum
    }

    fn cost_comprehension(
        &mut self,
        e: &IdedExpr,
        c: &cel::common::ast::ComprehensionExpr,
    ) -> Cost {
        let mut sum = self.cost(&c.iter_range).add(self.cost(&c.accu_init));
        self.push_local(&c.accu_var, &c.accu_init);

        // `pushIterSingle` / `pushIterKey` + `pushIterValue`.
        let range_entry = self.entry(&c.iter_range);
        let is_list = match range_entry {
            Some(en) => en.list,
            None => matches!(self.ty(&c.iter_range), Ty::List(_)),
        };
        let range_path = self.path(&c.iter_range);
        let with = |sub: &str| {
            let mut p = range_path.clone();
            p.push(sub.to_string());
            p
        };
        if let Some(v2) = &c.iter_var2 {
            let key_sub = if is_list { "@indices" } else { "@keys" };
            let val_sub = if is_list { "@items" } else { "@values" };
            self.push(
                &c.iter_var,
                LocalVar {
                    path: with(key_sub),
                    size: range_entry.map(|en| en.key),
                    entry: None,
                },
            );
            self.push(
                v2,
                LocalVar {
                    path: with(val_sub),
                    size: range_entry.map(|en| en.val),
                    entry: None,
                },
            );
        } else {
            let (size, sub) = if is_list {
                (range_entry.map(|en| en.val), "@items")
            } else {
                (range_entry.map(|en| en.key), "@keys")
            };
            self.push(
                &c.iter_var,
                LocalVar {
                    path: with(sub),
                    size,
                    entry: None,
                },
            );
        }

        let loop_cost = self.cost(&c.loop_cond);
        let step_cost = self.cost(&c.loop_step);

        self.pop(&c.iter_var);
        if let Some(v2) = &c.iter_var2 {
            self.pop(v2);
        }
        sum = sum.add(self.cost(&c.result));
        self.pop(&c.accu_var);

        let range_cnt = self.size_or_unknown(&c.iter_range);
        sum = sum.add(range_cnt.times_cost(step_cost.add(loop_cost)));

        match &c.accu_init.expr {
            Expr::Literal(_) => {
                let s = self.compute_size(&c.accu_init);
                self.set_size(e, s);
            }
            Expr::List(_) | Expr::Map(_) => {
                self.set_size(e, Some(range_cnt));
                let step_entry = self.entry(&c.loop_step);
                self.set_entry(e, step_entry);
            }
            _ => {}
        }
        sum
    }

    fn cost_call(&mut self, e: &IdedExpr) -> Cost {
        let Expr::Call(call) = &e.expr else {
            return Cost::default();
        };
        let name = call.func_name.as_str();

        // `dyn` just disables type-checking: 1 plus the argument.
        if name == "dyn" && call.args.len() == 1 {
            let c = self.cost(&call.args[0]);
            self.copy_estimates(e, &call.args[0]);
            return Cost::fixed(1).add(c);
        }

        let arg_costs: Vec<Cost> = call.args.iter().map(|a| self.cost(a)).collect();
        let mut sum = Cost::default();
        if let Some(t) = &call.target {
            sum = sum.add(self.cost(t));
        }

        let (fn_cost, result_size) = self.function_cost(e, call, &arg_costs);
        let mut result_size = result_size;

        if (name == operators::INDEX || name == operators::OPT_INDEX) && !call.args.is_empty() {
            let en = self.entry(&call.args[0]);
            result_size = en.map(|en| en.val);
            let sub = if matches!(self.ty(&call.args[0]), Ty::List(_)) {
                "@items"
            } else {
                "@values"
            };
            let mut p = self.path(&call.args[0]);
            p.push(sub.to_string());
            self.paths.insert(e.id, p);
        }
        if result_size.is_none() {
            result_size = self.compute_size(e);
        }
        self.set_size(e, result_size);
        sum.add(fn_cost)
    }

    /// `coster.functionCost`: only the overloads that are priced by size are
    /// special-cased; everything else is `1 + the arguments`.
    fn function_cost(
        &mut self,
        e: &IdedExpr,
        call: &cel::common::ast::CallExpr,
        arg_costs: &[Cost],
    ) -> (Cost, Option<Size>) {
        let name = call.func_name.as_str();
        let arg_sum = arg_costs.iter().fold(Cost::default(), |a, c| a.add(*c));
        let args = &call.args;
        let arg_ty = |i: usize, s: &Self| args.get(i).map(|a| s.ty(a)).unwrap_or(Ty::Dyn);
        let target_ty = call.target.as_ref().map(|t| self.ty(t));
        let is_str_or_bytes = |t: &Ty| matches!(t, Ty::Str | Ty::Bytes);

        match name {
            operators::IN if args.len() == 2 && matches!(arg_ty(1, self), Ty::List(_)) => {
                let sz = self.size_or_unknown(&args[1]);
                return (sz.times_factor(1.0).add(arg_sum), None);
            }
            operators::LOGICAL_AND | operators::LOGICAL_OR if arg_costs.len() == 2 => {
                let (l, r) = (arg_costs[0], arg_costs[1]);
                return (
                    Cost {
                        min: l.min,
                        max: l.add(r).max,
                    },
                    None,
                );
            }
            operators::CONDITIONAL if args.len() == 3 => {
                let size = self
                    .size_or_unknown(&args[1])
                    .union(self.size_or_unknown(&args[2]));
                let e1 = self.entry(&args[1]);
                let e2 = self.entry(&args[2]);
                if let (Some(a), Some(b)) = (e1, e2) {
                    self.set_entry(e, Some(a.union(b)));
                }
                let cost = arg_costs[0].add(arg_costs[1].union(arg_costs[2]));
                return (cost, Some(size));
            }
            operators::ADD if args.len() == 2 => {
                let t = arg_ty(0, self);
                if matches!(t, Ty::Str | Ty::Bytes | Ty::List(_)) {
                    let lhs = self.size_or_unknown(&args[0]);
                    let rhs = self.size_or_unknown(&args[1]);
                    let result = lhs.add(rhs);
                    if let (Some(a), Some(b)) = (self.entry(&args[0]), self.entry(&args[1])) {
                        self.set_entry(e, Some(a.union(b)));
                    }
                    return if matches!(t, Ty::List(_)) {
                        (Cost::fixed(1).add(arg_sum), Some(result))
                    } else {
                        (
                            result.times_factor(STRING_TRAVERSAL).add(arg_sum),
                            Some(result),
                        )
                    };
                }
            }
            operators::EQUALS
            | operators::NOT_EQUALS
            | operators::LESS
            | operators::LESS_EQUALS
            | operators::GREATER
            | operators::GREATER_EQUALS
                if args.len() == 2 =>
            {
                let ordering = !matches!(name, operators::EQUALS | operators::NOT_EQUALS);
                if !ordering
                    || (is_str_or_bytes(&arg_ty(0, self)) && is_str_or_bytes(&arg_ty(1, self)))
                {
                    let l = self.size_or_unknown(&args[0]);
                    let r = self.size_or_unknown(&args[1]);
                    let smallest_max = l.max.min(r.max);
                    let min = if smallest_max > 0 { 1 } else { 0 };
                    return (
                        Cost {
                            min,
                            max: smallest_max,
                        }
                        .mul_factor_pub(STRING_TRAVERSAL)
                        .add(arg_sum),
                        None,
                    );
                }
            }
            "startsWith" | "endsWith" if args.len() == 1 && target_ty == Some(Ty::Str) => {
                let sz = self.size_or_unknown(&args[0]);
                return (sz.times_factor(STRING_TRAVERSAL).add(arg_sum), None);
            }
            "contains" if args.len() == 1 && target_ty == Some(Ty::Str) => {
                let t = call.target.as_ref().expect("member call");
                let str_cost = self.size_or_unknown(t).times_factor(STRING_TRAVERSAL);
                let sub_cost = self
                    .size_or_unknown(&args[0])
                    .times_factor(STRING_TRAVERSAL);
                return (str_cost.mul(sub_cost).add(arg_sum), None);
            }
            "matches" if args.len() == 1 && target_ty == Some(Ty::Str) => {
                let t = call.target.as_ref().expect("member call");
                let str_cost = self
                    .size_or_unknown(t)
                    .add(Size::fixed(1))
                    .times_factor(STRING_TRAVERSAL);
                let regex_cost = self
                    .size_or_unknown(&args[0])
                    .times_factor(REGEX_STRING_LENGTH);
                return (str_cost.mul(regex_cost).add(arg_sum), None);
            }
            _ => {}
        }
        (Cost::fixed(1).add(arg_sum), None)
    }
}

impl Cost {
    fn mul_factor_pub(self, f: f64) -> Cost {
        Cost {
            min: mul_factor(self.min, f),
            max: mul_factor(self.max, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid(x: &str) -> Option<String> {
        match compile_selector(x) {
            SelectorCompilation::Invalid(d) => Some(d),
            SelectorCompilation::Ok { .. } => None,
        }
    }

    fn cost(x: &str) -> u64 {
        match compile_selector(x) {
            SelectorCompilation::Ok { max_cost } => max_cost,
            SelectorCompilation::Invalid(d) => panic!("{x}: {d}"),
        }
    }

    #[test]
    fn index_key_type_mismatch_reports_position() {
        let d = invalid("device.attributes[true].someBoolean").unwrap();
        assert_eq!(
            d,
            "compilation failed: ERROR: <input>:1:18: found no matching overload for '_[_]' applied to '(map(string, map(string, any)), bool)'\n | device.attributes[true].someBoolean\n | .................^"
        );
    }

    #[test]
    fn position_skips_literals_strings_and_floats() {
        // The list literal's `[`, the string's `[` and the float's `.` are not
        // operators; the failing index is the second one.
        let d =
            invalid("1.5 > 0.5 && ['['].exists(x, x == '[') && device.capacity[1].a == 1").unwrap();
        assert!(d.contains("<input>:1:"), "{d}");
        assert!(
            d.contains("'(map(string, map(string, apiserver.cel.Quantity)), int)'"),
            "{d}"
        );
        assert!(d.ends_with("^"), "{d}");
    }

    #[test]
    fn undefined_device_field() {
        let d = invalid("device.nope == 1").unwrap();
        assert!(d.contains("<input>:1:7: undefined field 'nope'"), "{d}");
    }

    #[test]
    fn output_type_must_be_bool_or_any() {
        assert_eq!(
            invalid("device.driver").unwrap(),
            "must evaluate to bool or the unknown type, not string"
        );
        assert!(invalid("device.attributes['d'].x").is_none());
        assert!(invalid("size(device.driver) > 1").is_none());
    }

    #[test]
    fn list_index_needs_int() {
        assert!(invalid("['a'][0] == 'a'").is_none());
        assert!(invalid("['a']['x'] == 'a'")
            .unwrap()
            .contains("'(list(string), string)'"));
    }

    /// `CEL-cost` (validation_resourceclaim_test.go:610).
    #[test]
    fn nested_comprehensions_exceed_the_limit() {
        let x = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))";
        assert!(cost(x) > CEL_SELECTOR_EXPRESSION_MAX_COST);
    }

    #[test]
    fn ordinary_selectors_are_cheap() {
        for x in [
            "device.driver == 'dra.example.com'",
            "device.attributes['d'].model == 'a'",
            "device.capacity['d'].memory.compareTo(quantity('1Gi')) >= 0",
            "device.attributes['d'].x in ['a', 'b', 'c']",
            "device.attributes['d'].x.startsWith('a') && device.attributes['d'].y.contains('b')",
        ] {
            let c = cost(x);
            assert!(c > 0 && c < CEL_SELECTOR_EXPRESSION_MAX_COST, "{x}: {c}");
        }
    }

    /// `device.driver == "x"`: select(device)=1 + ident(device)=1 + const 0 +
    /// equals: min(63, 1)*0.1 rounded up = 1 -> 3.
    #[test]
    fn simple_equality_cost_matches_cel_go() {
        assert_eq!(cost("device.driver == 'x'"), 3);
    }

    #[test]
    fn comprehension_over_env_map_is_bounded_by_its_size() {
        // device.attributes has at most 32 domains.
        let c = cost("device.attributes.all(d, d == 'x')");
        assert!(c < CEL_SELECTOR_EXPRESSION_MAX_COST, "{c}");
    }
}
