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
//! * a type pass over the checker: the type of `device` and its fields,
//!   container element types, macro iteration variables, an undeclared
//!   reference or function, `_[_]` applied to a key of the wrong type, an
//!   undefined field of `device`, a call that no overload of
//!   `cel_dra_decls` accepts (`found no matching overload`), and an output
//!   type that is neither `bool` nor `any`. The result type of a call is
//!   still the pass's own (`dyn` when it cannot tell), which type-checks, so
//!   a call result never causes a rejection upstream would not make;
//! * `checker.Cost` (`vendor/github.com/google/cel-go/checker/cost.go`,
//!   `coster`) with the DRA `sizeEstimator` (`compile.go:417-475`) and the
//!   Kubernetes libraries' `CostEstimator.EstimateCallCost`
//!   (`apiserver/pkg/cel/library/cost.go`), see `estimate_call_cost`.
//!
//! Positions are reconstructed from the source text because the crate drops
//! its source info on a successful parse.

use std::collections::HashMap;

use cel::common::ast::{operators, EntryExpr, Expr, IdedExpr, LiteralValue};

use crate::cel_dra_decls::{self, DType};

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

/// `cel.bind(var, init, result)` (`ext/bindings.go` `celBind`; the parser
/// crate has no such macro, so the call form is recognised here): the
/// receiver must be the bare `cel` namespace identifier and the variable a
/// simple identifier.
fn bind_parts(c: &cel::common::ast::CallExpr) -> Option<(&str, &IdedExpr, &IdedExpr)> {
    if !is_bind_call(c) {
        return None;
    }
    match &c.args[0].expr {
        Expr::Ident(v) => Some((v.as_str(), &c.args[1], &c.args[2])),
        _ => None,
    }
}

/// The `cel.bind(_, _, _)` call shape (`celBind` is only reached for the bare
/// `cel` namespace receiver and three arguments).
fn is_bind_call(c: &cel::common::ast::CallExpr) -> bool {
    c.func_name == "bind"
        && c.args.len() == 3
        && matches!(c.target.as_deref().map(|t| &t.expr), Some(Expr::Ident(n)) if n == "cel")
}

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
    if let Some(msg) = checker.errors_display() {
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
    /// Another opaque/struct type of the Kubernetes CEL libraries
    /// (`semver`, `ip`, `cidr`, `url`, `format`), only used to price `==`
    /// (`CostEstimator.EstimateCallCost`, `_==_`).
    Lib(&'static str),
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
            Ty::Lib(n) => (*n).into(),
            Ty::Device => "device".into(),
            Ty::List(t) => format!("list({})", t.name()),
            Ty::Map(k, v) => format!("map({}, {})", k.name(), v.name()),
        }
    }

    /// Unchecked: anything is assignable to and from it.
    fn is_unchecked(&self) -> bool {
        matches!(self, Ty::Dyn | Ty::Any | Ty::Quantity | Ty::Lib(_))
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

    /// The declaration-side type (`cel_dra_decls`) of this type.
    fn to_dtype(&self) -> DType {
        match self {
            Ty::Bool => DType::Bool,
            Ty::Int => DType::Int,
            Ty::Uint => DType::Uint,
            Ty::Double => DType::Double,
            Ty::Str => DType::Str,
            Ty::Bytes => DType::Bytes,
            Ty::Null => DType::Null,
            Ty::Dyn => DType::Dyn,
            Ty::Any => DType::Any,
            Ty::Quantity => DType::Struct("apiserver.cel.Quantity".into()),
            Ty::Lib(n @ ("net.IP" | "net.CIDR")) => DType::Opaque((*n).into(), vec![]),
            Ty::Lib(n) => DType::Struct((*n).into()),
            Ty::Device => DType::Struct("kubernetes.DRADevice".into()),
            Ty::List(t) => DType::List(Box::new(t.to_dtype())),
            Ty::Map(k, v) => DType::Map(Box::new(k.to_dtype()), Box::new(v.to_dtype())),
        }
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
/// The identifiers the cel-go standard library declares besides variables
/// (`common/stdlib/standard.go:44-57`, `decls.TypeVariable`), plus the root
/// `google` of `google.protobuf.Duration` / `Timestamp`, which the checker
/// resolves as a qualified name (`checker.go:123-145`).
fn is_declared_ident(name: &str) -> bool {
    matches!(
        name,
        "bool"
            | "bytes"
            | "double"
            | "int"
            | "list"
            | "map"
            | "null_type"
            | "string"
            | "type"
            | "uint"
            | "google"
    )
}

/// The character offset of the first occurrence of `name` as an identifier
/// token: outside string literals, not a selected field (preceded by `.`) and
/// not a called function (followed by `(`).
fn ident_offset(source: &str, name: &str) -> Option<usize> {
    let chars: Vec<char> = source.chars().collect();
    let word = |c: char| c.is_alphanumeric() || c == '_';
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' || c == '"' {
            i += 1;
            while i < chars.len() && chars[i] != c {
                i += if chars[i] == '\\' { 2 } else { 1 };
            }
            i += 1;
        } else if word(c) {
            let start = i;
            while i < chars.len() && word(chars[i]) {
                i += 1;
            }
            let tok: String = chars[start..i].iter().collect();
            let prev = chars[..start].iter().rev().find(|c| !c.is_whitespace());
            let next = chars[i..].iter().find(|c| !c.is_whitespace());
            if tok == name && prev != Some(&'.') && next != Some(&'(') {
                return Some(start);
            }
        } else {
            i += 1;
        }
    }
    None
}

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

/// The character offset of the expression ID of the root of the CEL
/// expression that starts at `start`: the position cel-go reports an error
/// "at" an expression (`MacroExprFactory.NewError(expr.ID(), ..)`).
///
/// Ported from the `parser.go` visitors (cel-go `parser/parser.go:469-760`,
/// grammar `parser/gen/CEL.g4:24-100`). An expression's ID is the token the
/// visitor passes to `helper.id`: the `?` of a conditional, the operator of a
/// `||`/`&&`/relation/calc, the first `!`/`-` of an odd-length unary chain,
/// the `.` of a select, the `(` of a call (global or member), the `[` of an
/// index, the `[`/`{` of a list/map/message literal, and the token start of an
/// identifier or literal (a literal's sign included). `||`/`&&` chains are
/// balanced (`logicManager.balancedTree`, helper.go:305-322): the root is
/// operator `len(ops)/2`. Parentheses add no node. `None` when the text is not
/// a well-formed expression, so the caller keeps its fallback.
fn cel_root_id_offset(source: &str, start: usize) -> Option<usize> {
    #[derive(Clone, PartialEq)]
    enum Tok {
        Word(String),
        Num,
        Str,
        Punct(&'static str),
    }
    const PUNCTS: [&str; 24] = [
        "<=", ">=", "==", "!=", "&&", "||", "(", ")", "[", "]", "{", "}", ".", ",", ":", "?", "!",
        "-", "+", "*", "/", "%", "<", ">",
    ];
    let chars: Vec<char> = source.chars().collect();
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    let mut toks: Vec<(Tok, usize)> = Vec::new();
    let mut i = start;
    while i < chars.len() {
        let c = chars[i];
        let after_operand = matches!(
            toks.last(),
            Some((
                Tok::Word(_) | Tok::Num | Tok::Str | Tok::Punct(")" | "]" | "}"),
                _
            ))
        );
        let next_is_digit = chars.get(i + 1).is_some_and(|d| d.is_ascii_digit());
        // A string prefix (`r`, `b`, `rb`, `br`, any case) right before a quote.
        let prefix_len = chars[i..]
            .iter()
            .take_while(|ch| matches!(ch, 'r' | 'R' | 'b' | 'B'))
            .count();
        let is_string = c == '"'
            || c == '\''
            || (prefix_len > 0
                && prefix_len <= 2
                && matches!(chars.get(i + prefix_len), Some('"' | '\'')));
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c.is_ascii_digit() || (c == '.' && next_is_digit && !after_operand) {
            let at = i;
            while i < chars.len() {
                let ch = chars[i];
                let exp_sign = matches!(ch, '+' | '-')
                    && matches!(chars[i - 1], 'e' | 'E')
                    && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit());
                let frac_dot = ch == '.' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit());
                if ident(ch) || exp_sign || frac_dot {
                    i += 1;
                } else {
                    break;
                }
            }
            toks.push((Tok::Num, at));
        } else if is_string {
            let at = i;
            let raw = chars[i..i + prefix_len]
                .iter()
                .any(|ch| matches!(ch, 'r' | 'R'));
            i += prefix_len;
            let q = chars[i];
            let n = if chars.get(i + 1) == Some(&q) && chars.get(i + 2) == Some(&q) {
                3
            } else {
                1
            };
            i += n;
            loop {
                if i >= chars.len() {
                    return None;
                }
                if chars[i] == '\\' && !raw {
                    i += 2;
                } else if chars[i] == q
                    && (n == 1 || (chars.get(i + 1) == Some(&q) && chars.get(i + 2) == Some(&q)))
                {
                    i += n;
                    break;
                } else {
                    i += 1;
                }
            }
            toks.push((Tok::Str, at));
        } else if ident(c) {
            let at = i;
            while i < chars.len() && ident(chars[i]) {
                i += 1;
            }
            toks.push((Tok::Word(chars[at..i].iter().collect()), at));
        } else {
            let p = PUNCTS.iter().find(|p| {
                p.chars()
                    .enumerate()
                    .all(|(k, pc)| chars.get(i + k) == Some(&pc))
            })?;
            toks.push((Tok::Punct(p), i));
            i += p.len();
        }
    }

    struct P {
        toks: Vec<(Tok, usize)>,
        at: usize,
    }
    impl P {
        fn punct(&self, p: &str) -> bool {
            matches!(self.toks.get(self.at), Some((Tok::Punct(q), _)) if *q == p)
        }
        fn pos(&self) -> Option<usize> {
            self.toks.get(self.at).map(|t| t.1)
        }
        fn eat(&mut self, p: &str) -> Option<usize> {
            if !self.punct(p) {
                return None;
            }
            let pos = self.pos();
            self.at += 1;
            pos
        }
        /// `exprList` / `mapInitializerList`: comma separated items up to
        /// (not including) `close`.
        fn list(&mut self, close: &str, pairs: bool) -> Option<()> {
            while !self.punct(close) {
                self.eat("?");
                self.expr()?;
                if pairs {
                    self.eat(":")?;
                    self.expr()?;
                }
                if self.eat(",").is_none() {
                    break;
                }
            }
            Some(())
        }
        fn expr(&mut self) -> Option<usize> {
            let e = self.chain(true)?;
            if let Some(q) = self.eat("?") {
                self.chain(true)?;
                self.eat(":")?;
                self.expr()?;
                return Some(q);
            }
            Some(e)
        }
        /// `conditionalOr` (`or`) / `conditionalAnd`: a balanced chain.
        fn chain(&mut self, or: bool) -> Option<usize> {
            let (op, mut ops) = (if or { "||" } else { "&&" }, Vec::new());
            let first = self.operand(or)?;
            while let Some(p) = self.eat(op) {
                ops.push(p);
                self.operand(or)?;
            }
            Some(if ops.is_empty() {
                first
            } else {
                ops[ops.len() / 2]
            })
        }
        fn operand(&mut self, or: bool) -> Option<usize> {
            if or {
                self.chain(false)
            } else {
                self.relation()
            }
        }
        fn rel_op(&self) -> bool {
            match self.toks.get(self.at) {
                Some((Tok::Punct(p), _)) => ["<", "<=", ">=", ">", "==", "!="].contains(p),
                Some((Tok::Word(w), _)) => w == "in",
                _ => false,
            }
        }
        /// Left-associative levels: the root is the last operator.
        fn relation(&mut self) -> Option<usize> {
            let mut root = self.add()?;
            while self.rel_op() {
                root = self.pos()?;
                self.at += 1;
                self.add()?;
            }
            Some(root)
        }
        fn add(&mut self) -> Option<usize> {
            let mut root = self.mul()?;
            while self.punct("+") || self.punct("-") {
                root = self.pos()?;
                self.at += 1;
                self.mul()?;
            }
            Some(root)
        }
        fn mul(&mut self) -> Option<usize> {
            let mut root = self.unary()?;
            while self.punct("*") || self.punct("/") || self.punct("%") {
                root = self.pos()?;
                self.at += 1;
                self.unary()?;
            }
            Some(root)
        }
        fn unary(&mut self) -> Option<usize> {
            let first = self.pos()?;
            for op in ["!", "-"] {
                // A `-` directly before a number is the literal's sign.
                let signed = op == "-" && matches!(self.toks.get(self.at + 1), Some((Tok::Num, _)));
                if self.punct(op) && !signed {
                    let mut n = 0;
                    while self.eat(op).is_some() {
                        n += 1;
                    }
                    let inner = self.member()?;
                    return Some(if n % 2 == 0 { inner } else { first });
                }
            }
            self.member()
        }
        fn member(&mut self) -> Option<usize> {
            let mut root = self.primary()?;
            loop {
                if let Some(dot) = self.eat(".") {
                    self.eat("?");
                    if !matches!(self.toks.get(self.at), Some((Tok::Word(_), _))) {
                        return None;
                    }
                    self.at += 1;
                    root = match self.eat("(") {
                        Some(open) => {
                            self.list(")", false)?;
                            self.eat(")")?;
                            open
                        }
                        None => dot,
                    };
                } else if let Some(open) = self.eat("[") {
                    self.eat("?");
                    self.expr()?;
                    self.eat("]")?;
                    root = open;
                } else {
                    return Some(root);
                }
            }
        }
        fn primary(&mut self) -> Option<usize> {
            if self.eat("(").is_some() {
                let inner = self.expr()?;
                self.eat(")")?;
                return Some(inner);
            }
            if let Some(open) = self.eat("[") {
                self.list("]", false)?;
                self.eat("]")?;
                return Some(open);
            }
            if let Some(open) = self.eat("{") {
                self.list("}", true)?;
                self.eat("}")?;
                return Some(open);
            }
            self.eat(".");
            match self.toks.get(self.at).cloned() {
                Some((Tok::Num | Tok::Str, pos)) => {
                    self.at += 1;
                    Some(pos)
                }
                Some((Tok::Punct("-"), pos)) => {
                    // the sign of a numeric literal
                    self.at += 1;
                    if matches!(self.toks.get(self.at), Some((Tok::Num, _))) {
                        self.at += 1;
                        Some(pos)
                    } else {
                        None
                    }
                }
                Some((Tok::Word(_), pos)) => {
                    // `a.b.C{..}` (CreateMessage): the ID is the `{`.
                    let mut j = self.at + 1;
                    while matches!(self.toks.get(j), Some((Tok::Punct("."), _)))
                        && matches!(self.toks.get(j + 1), Some((Tok::Word(_), _)))
                    {
                        j += 2;
                    }
                    if let Some((Tok::Punct("{"), open)) = self.toks.get(j).cloned() {
                        self.at = j + 1;
                        while !self.punct("}") {
                            self.eat("?");
                            self.at += 1; // field name
                            self.eat(":")?;
                            self.expr()?;
                            if self.eat(",").is_none() {
                                break;
                            }
                        }
                        self.eat("}")?;
                        return Some(open);
                    }
                    self.at += 1;
                    if let Some(open) = self.eat("(") {
                        self.list(")", false)?;
                        self.eat(")")?;
                        return Some(open);
                    }
                    Some(pos)
                }
                _ => None,
            }
        }
    }
    P { toks, at: 0 }.expr()
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
    /// `(line, column, rendered)` of every reported error, `common.Errors`.
    errors: Vec<(usize, usize, String)>,
    types: HashMap<u64, Ty>,
    /// Token offsets of every expression ID, see [`source_info_for`].
    source_info: Option<std::sync::Arc<cel::common::ast::SourceInfo>>,
}

impl<'a> Checker<'a> {
    fn new(source: &'a str) -> Self {
        let (brackets, dots) = operator_offsets(source);
        Self {
            source_info: source_info_for(source),
            source,
            brackets,
            dots,
            next_bracket: 0,
            next_dot: 0,
            counting: true,
            errors: Vec::new(),
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
        self.errors.push((
            line,
            col,
            format!(
                "ERROR: <input>:{line}:{col}: {msg}\n | {text}\n | {}^",
                ".".repeat(col - 1)
            ),
        ));
    }

    /// `common.Errors.ToDisplayString` (common/errors.go:84-112): the errors
    /// stably sorted by position, newline delimited.
    fn errors_display(&mut self) -> Option<String> {
        if self.errors.is_empty() {
            return None;
        }
        self.errors.sort_by_key(|(line, col, _)| (*line, *col));
        Some(
            self.errors
                .iter()
                .map(|(_, _, text)| text.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        )
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
                    // checker.go:116 `checkIdent`: an identifier that is no
                    // variable of the environment, no comprehension / bind
                    // variable and no standard type identifier.
                    if !is_declared_ident(name) {
                        let at = ident_offset(self.source, name);
                        self.report(
                            at,
                            format!("undeclared reference to '{name}' (in container '')"),
                        );
                    }
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
            Expr::Call(c) => match bind_parts(c) {
                Some((var, init, result)) => {
                    // `.bind` consumes one `.` of the source, as any receiver call does.
                    self.take_dot();
                    let init_ty = self.infer(init, scope);
                    let pushed = scope.len();
                    scope.push((var.to_string(), init_ty));
                    let out = self.infer(result, scope);
                    scope.truncate(pushed);
                    out
                }
                None if is_bind_call(c) => {
                    // ext/bindings.go:172-179 celBind: a non-identifier
                    // variable is a macro error at `varIdent.ID()`, the
                    // argument's expression ID (`cel_root_id_offset`); the
                    // argument start when its text cannot be re-read.
                    let dot = self.take_dot();
                    let at = dot.and_then(|d| {
                        let rest: Vec<(usize, char)> =
                            self.source.chars().enumerate().skip(d).collect();
                        let open = rest.iter().position(|(_, ch)| *ch == '(')?;
                        let arg_start = rest[open + 1..]
                            .iter()
                            .find(|(_, ch)| !ch.is_whitespace())
                            .map(|(i, _)| *i)?;
                        Some(cel_root_id_offset(self.source, arg_start).unwrap_or(arg_start))
                    });
                    self.report(
                        at,
                        "cel.bind() variable names must be simple identifiers".into(),
                    );
                    Ty::Dyn
                }
                None => self.infer_call(e.id, c, scope),
            },
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

    fn infer_call(
        &mut self,
        id: u64,
        c: &cel::common::ast::CallExpr,
        scope: &mut Vec<(String, Ty)>,
    ) -> Ty {
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

        // checker.go:249-262: a receiver that is a qualified name may be the
        // namespace of a declared function (`sets.contains`, `strings.quote`),
        // in which case the call is a plain call of that function and the
        // receiver is not an expression to check. Only a bare identifier can
        // be one here (no declared namespace is dotted).
        let namespaced = match c.target.as_ref().map(|t| &t.expr) {
            Some(Expr::Ident(n))
                if !scope.iter().any(|(s, _)| s == n)
                    && n != "device"
                    && cel_dra_decls::lookup(&format!("{n}.{name}")).is_some() =>
            {
                Some(format!("{n}.{name}"))
            }
            _ => None,
        };
        let target = match (&c.target, &namespaced) {
            (Some(t), None) => Some(self.infer(t, scope)),
            _ => None,
        };
        if c.target.is_some() {
            self.take_dot();
        }
        let args: Vec<Ty> = c.args.iter().map(|a| self.infer(a, scope)).collect();
        self.check_call(id, c, namespaced.as_deref(), target.as_ref(), &args);

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
            "semver" => Ty::Lib("kubernetes.Semver"),
            "ip" => Ty::Lib("net.IP"),
            "cidr" => Ty::Lib("net.CIDR"),
            "url" => Ty::Lib("kubernetes.URL"),
            "format.named" => Ty::Lib("kubernetes.NamedFormat"),
            // Kubernetes strings library results, so the cost estimator can
            // see a list's element type (`listElementNode`).
            "split" | "findAll"
                if target
                    .as_ref()
                    .is_some_and(|t| *t == Ty::Str || t.is_unchecked()) =>
            {
                Ty::List(Box::new(Ty::Str))
            }
            "find" | "lowerAscii" | "upperAscii" | "trim" | "substring" | "replace"
                if target
                    .as_ref()
                    .is_some_and(|t| *t == Ty::Str || t.is_unchecked()) =>
            {
                Ty::Str
            }
            _ => Ty::Dyn,
        }
    }
}

impl Checker<'_> {
    /// The character offset of the token of expression `id`
    /// (`common.Source.OffsetLocation(sourceInfo.GetStartOffset(id))`).
    fn id_offset(&self, id: u64) -> Option<usize> {
        let (start, _) = self.source_info.as_ref()?.offset_for(id)?;
        // The parser counts bytes; every other offset here counts characters.
        let bytes = self.source.as_bytes();
        let start = (start as usize).min(bytes.len());
        Some(String::from_utf8_lossy(&bytes[..start]).chars().count())
    }

    /// `checker.checkCall` (checker.go:217-290) past the argument traversal:
    /// the function must be declared and one of its overloads must accept the
    /// argument types. `qualified` is the resolved name of a namespaced
    /// function, in which case there is no receiver.
    fn check_call(
        &mut self,
        id: u64,
        c: &cel::common::ast::CallExpr,
        qualified: Option<&str>,
        target: Option<&Ty>,
        args: &[Ty],
    ) {
        let name = qualified.unwrap_or(c.func_name.as_str());
        if cel_dra_decls::is_macro(name)
            || (name.starts_with('@')
                && name != operators::IN
                && name != operators::NOT_STRICTLY_FALSE)
        {
            return;
        }
        let at = self.id_offset(id);
        let Some(decls) = cel_dra_decls::lookup(name) else {
            // checker.go:228-232 / 288-290.
            self.report(
                at,
                format!("undeclared reference to '{name}' (in container '')"),
            );
            return;
        };
        let mut arg_types: Vec<DType> = Vec::new();
        if let Some(t) = target {
            arg_types.push(t.to_dtype());
        }
        arg_types.extend(args.iter().map(Ty::to_dtype));

        // checker.go:309-322: `&&` / `||` check each argument against bool
        // (the AST form is variadic upstream; binary here).
        if name == operators::LOGICAL_AND || name == operators::LOGICAL_OR {
            for (i, a) in arg_types.iter().enumerate() {
                if !cel_dra_decls::is_assignable(a, &DType::Bool) {
                    let arg_at = c.args.get(i).and_then(|x| self.id_offset(x.id));
                    self.report(
                        arg_at,
                        format!("expected type 'bool' but found '{}'", a.name()),
                    );
                    return;
                }
            }
            return;
        }

        let member = target.is_some();
        if let cel_dra_decls::Resolution::NoMatch(shown) =
            cel_dra_decls::resolve(decls, member, &arg_types)
        {
            self.report(
                at,
                format!(
                    "found no matching overload for '{}' applied to '{}'",
                    name,
                    cel_dra_decls::format_signature(&shown, member)
                ),
            );
        }
    }
}

/// The parser keeps the offset of every expression ID in its `SourceInfo`
/// but drops it when the parse succeeds, so the expression is parsed once
/// more with a trailing `)` that makes the parse fail: the identifiers the
/// parser hands out are the same, and the error carries the `SourceInfo`.
fn source_info_for(source: &str) -> Option<std::sync::Arc<cel::common::ast::SourceInfo>> {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::parser::Parser::new().parse(&format!("{source}\n)"))
    }));
    match r {
        Ok(Err(e)) => e.errors.first()?.source_info.clone(),
        _ => None,
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

    /// `SizeEstimate.Multiply`.
    fn mul(self, o: Size) -> Size {
        Size {
            min: self.min.saturating_mul(o.min),
            max: self.max.saturating_mul(o.max),
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

/// `apiservercel.MaxFormatSize` / `MaxNameFormatRegexSize`
/// (`staging/src/k8s.io/apiserver/pkg/cel/limits.go:52-53`).
const MAX_FORMAT_SIZE: u64 = 64;
const MAX_NAME_FORMAT_REGEX_SIZE: f64 = 128.0;

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
            Expr::Call(c) => match bind_parts(c) {
                Some((var, init, result)) => self.cost_bind(e, var, init, result),
                None => self.cost_call(e),
            },
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
                    return self.cost_bind(e, &c.accu_var, &c.accu_init, &c.result);
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

    /// `coster.costBind` (`checker/cost.go:710`). The range of the expanded
    /// bind comprehension is an empty list literal (`ext/bindings.go:182-190`),
    /// whose creation cost is retained ("Binds are lazily initialized").
    fn cost_bind(&mut self, e: &IdedExpr, var: &str, init: &IdedExpr, result: &IdedExpr) -> Cost {
        let mut sum = Cost::fixed(LIST_CREATE_BASE).add(self.cost(init));
        self.push_local(var, init);
        sum = sum.add(self.cost(result));
        self.pop(var);
        self.copy_estimates(e, result);
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

    /// `CostEstimator.listElementNode` + `sizeEstimate`: the size of a list's
    /// elements, by `<path>/@items` when the list has a path.
    fn element_size(&self, list: &IdedExpr) -> Size {
        let path = self.path(list);
        if path.is_empty() {
            return Size::UNKNOWN;
        }
        let mut p = path;
        p.push("@items".to_string());
        estimate_size(&p).unwrap_or(Size::UNKNOWN)
    }

    /// `CostEstimator.EstimateCallCost`
    /// (`staging/src/k8s.io/apiserver/pkg/cel/library/cost.go:112-410`): the
    /// cost of the call itself, without its arguments, and the result size.
    /// `None` when upstream returns nil. The authorizer and
    /// `fieldSelector`/`labelSelector` cases are not ported: the DRA
    /// environment does not declare those libraries, so such a selector never
    /// reaches the cost estimate upstream.
    fn estimate_call_cost(
        &mut self,
        call: &cel::common::ast::CallExpr,
    ) -> Option<(Cost, Option<Size>)> {
        let name = call.func_name.as_str();
        let args = &call.args;
        let target = call.target.as_deref();
        let one = Some((Cost::fixed(1), None));
        match name {
            "isSorted" | "sum" | "max" | "min" | "indexOf" | "lastIndexOf" => {
                let t = target?;
                if let Ty::List(el) = self.ty(t) {
                    // Charge 1 for comparing each element; strings and bytes
                    // add the cost of traversing them.
                    let mut el_cost = Cost::fixed(1);
                    if matches!(*el, Ty::Str | Ty::Bytes) {
                        el_cost = el_cost.add(self.element_size(t).times_factor(STRING_TRAVERSAL));
                    }
                    Some((self.size_or_unknown(t).times_cost(el_cost), None))
                } else {
                    // The target is a string (`indexOf` / `lastIndexOf`).
                    Some((self.size_or_unknown(t).times_factor(STRING_TRAVERSAL), None))
                }
            }
            "url" | "jsonpatch.escapeKey" if args.len() == 1 => {
                let sz = self.size_or_unknown(&args[0]);
                Some((sz.times_factor(STRING_TRAVERSAL), Some(sz)))
            }
            "lowerAscii" | "upperAscii" | "substring" | "trim" => {
                let sz = self.size_or_unknown(target?);
                Some((sz.times_factor(STRING_TRAVERSAL), Some(sz)))
            }
            "replace" if args.len() >= 2 => {
                let sz = self.size_or_unknown(target?);
                let to_replace = self.size_or_unknown(&args[0]);
                let replace_with = self.size_or_unknown(&args[1]);
                let mut count = Size::fixed(0);
                let mut retained = Size::fixed(0);
                // The longest replacement.
                if to_replace.min == 0 {
                    // Replacing "" surrounds every character with the replacement.
                    count.max = sz.max.saturating_add(1);
                    retained.max = sz.max;
                } else if replace_with.max <= to_replace.min {
                    count.max = 0;
                    retained.max = sz.max;
                } else {
                    count.max = (sz.max as f64 / to_replace.min as f64).ceil() as u64;
                }
                // The shortest replacement.
                if to_replace.max == 0 {
                    count.min = sz.min.saturating_add(1);
                    retained.min = sz.min;
                } else if to_replace.max <= replace_with.min {
                    count.min = 0;
                    retained.min = sz.min;
                } else {
                    count.min = (sz.min as f64 / to_replace.max as f64).ceil() as u64;
                }
                let size = count.mul(replace_with).add(retained);
                // The traversal plus the construction of the result.
                Some((sz.times_factor(2.0 * STRING_TRAVERSAL), Some(size)))
            }
            "split" => {
                let sz = self.size_or_unknown(target?);
                let mut max = sz.max;
                if args.len() > 1 {
                    if let Expr::Literal(LiteralValue::Int(i)) = &args[1].expr {
                        max = *i.inner() as u64;
                    }
                }
                Some((
                    sz.times_factor(2.0 * STRING_TRAVERSAL),
                    Some(Size { min: 0, max }),
                ))
            }
            "join" => {
                let t = target?;
                let list = self.size_or_unknown(t);
                let mut sz = Size::fixed(0);
                if matches!(self.ty(t), Ty::List(_)) {
                    sz = list.mul(self.element_size(t));
                }
                if let Some(sep) = args.first() {
                    let sep = self.size_or_unknown(sep);
                    let seps = Size {
                        min: list.min.saturating_sub(1),
                        max: list.max.saturating_sub(1),
                    };
                    sz = sz.add(sep.mul(seps));
                }
                Some((sz.times_factor(STRING_TRAVERSAL), Some(sz)))
            }
            "find" | "findAll" if !args.is_empty() => {
                let sz = self.size_or_unknown(target?);
                // One is added to the string length so the product with an
                // empty string is not 0.
                let str_cost = sz.add(Size::fixed(1)).times_factor(STRING_TRAVERSAL);
                let regex_cost = self
                    .size_or_unknown(&args[0])
                    .times_factor(REGEX_STRING_LENGTH);
                Some((
                    str_cost.mul(regex_cost),
                    Some(Size {
                        min: 0,
                        max: sz.max,
                    }),
                ))
            }
            "cidr" | "isIP" | "isCIDR" if !args.is_empty() => {
                let sz = self.size_or_unknown(&args[0]);
                Some((sz.times_factor(STRING_TRAVERSAL), None))
            }
            // `cidr_ip` is the member form: a field access.
            "ip" if target.is_some() => one,
            "ip" if !args.is_empty() => {
                let sz = self.size_or_unknown(&args[0]);
                Some((sz.times_factor(STRING_TRAVERSAL), None))
            }
            "ip.isCanonical" if !args.is_empty() => {
                let sz = self.size_or_unknown(&args[0]);
                Some((sz.times_factor(2.0 * STRING_TRAVERSAL), None))
            }
            "masked"
            | "prefixLength"
            | "family"
            | "isUnspecified"
            | "isLoopback"
            | "isLinkLocalMulticast"
            | "isLinkLocalUnicast"
            | "isGlobalUnicast" => one,
            "containsIP" | "containsCIDR" if !args.is_empty() => {
                // Comparing two byte lists of 4 or 16 bytes each.
                let sz = Size { min: 4, max: 16 };
                let mut c = sz.add(sz).times_factor(STRING_TRAVERSAL);
                if name == "containsCIDR" {
                    // Mask the base CIDR and compare the CIDR bits.
                    c = c.add(sz.times_factor(STRING_TRAVERSAL)).add(Cost::fixed(1));
                }
                if self.ty(&args[0]) == Ty::Str {
                    // The string must be parsed into the right type.
                    c = c.add(
                        self.size_or_unknown(&args[0])
                            .times_factor(STRING_TRAVERSAL),
                    );
                }
                Some((c, None))
            }
            "quantity" | "isQuantity" | "semver" | "isSemver" if !args.is_empty() => {
                let sz = self.size_or_unknown(&args[0]);
                Some((sz.times_factor(STRING_TRAVERSAL), None))
            }
            "validate" if !args.is_empty() => {
                let sz = self.size_or_unknown(&args[0]);
                Some((
                    sz.times_factor(STRING_TRAVERSAL)
                        .mul_factor_pub(MAX_NAME_FORMAT_REGEX_SIZE * REGEX_STRING_LENGTH),
                    None,
                ))
            }
            "format.named" => one,
            "sign" | "asInteger" | "isInteger" | "asApproximateFloat" | "isGreaterThan"
            | "isLessThan" | "compareTo" | "add" | "sub" | "major" | "minor" | "patch" => one,
            "getScheme" | "getHostname" | "getHost" | "getPort" | "getEscapedPath" | "getQuery" => {
                one
            }
            operators::EQUALS if args.len() == 2 => {
                let (lt, rt) = (self.ty(&args[0]), self.ty(&args[1]));
                if lt != rt {
                    return None;
                }
                match lt {
                    // O(1) equality checks.
                    Ty::Quantity => one,
                    Ty::Lib("net.IP" | "net.CIDR" | "kubernetes.Semver") => one,
                    Ty::Lib("kubernetes.NamedFormat") => Some((
                        Cost {
                            min: 1,
                            max: MAX_FORMAT_SIZE,
                        }
                        .mul_factor_pub(STRING_TRAVERSAL),
                        None,
                    )),
                    Ty::Lib("kubernetes.URL") => {
                        // Upstream reads the computed size of `rhs` for both
                        // sides (cost.go:373-374).
                        let size = self.compute_size(&args[1]).unwrap_or(Size::fixed(1));
                        Some((
                            Cost {
                                min: 1,
                                max: size.max,
                            }
                            .mul_factor_pub(STRING_TRAVERSAL),
                            None,
                        ))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
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
        // A member call on `any` (an attribute value) resolves to the string
        // overload, as cel-go's checker does for a dyn target.
        let target_ty = call.target.as_ref().map(|t| self.ty(t)).map(|t| {
            if t.is_unchecked() {
                Ty::Str
            } else {
                t
            }
        });
        let is_str_or_bytes = |t: &Ty| matches!(t, Ty::Str | Ty::Bytes);

        // `c.estimator.EstimateCallCost` is consulted before the built-in
        // overload costs (cel-go `checker/cost.go:742-745`).
        if let Some((cost, size)) = self.estimate_call_cost(call) {
            return (cost.add(arg_sum), size);
        }

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
