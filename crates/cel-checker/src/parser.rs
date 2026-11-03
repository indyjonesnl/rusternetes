//! The parser the checker runs on: the `cel` parser with the macros of the
//! libraries the Kubernetes environment enables, as cel-go expands them while
//! parsing (`parser/macro.go`, and the `cel.Macros(..)` options of the
//! libraries).
//!
//! * the standard macros (`has`, `all`, `exists`, `exists_one`, `existsOne`,
//!   `map`, `filter`), which the `cel` crate expands on its own;
//! * `sortBy`, from `ext.Lists(ext.ListsVersion(3))` (`ext/lists.go`
//!   `sortByMacro`), which the `cel` crate ships as [`cel::extensions::lists`]
//!   (introduced at 1.34, `environment/base.go`);
//! * the two-variable comprehensions of `ext.TwoVarComprehensions()`
//!   (`ext/comprehensions.go`: `all`, `exists`, `existsOne` / `exists_one`
//!   with two variables, `transformList`, `transformMap`, `transformMapEntry`,
//!   introduced at 1.32), which the `cel` crate (0.15) does not expand: they
//!   are written here with its public [`Macro`] API, the expansions of
//!   `comprehensions.go` one for one.
//!
//! A macro of a library the environment does not enable is not expanded, so
//! the call is checked as the plain member call it then is
//! (`undeclared reference to 'exists'`), like cel-go.

use crate::environment::{EnvSet, EnvType};
use cel::common::ast::{
    operators, CallExpr, ComprehensionExpr, Expr, IdedExpr, ListExpr, LiteralValue, MapExpr,
};
use cel::parser::{Macro, MacroExprHelper, ParseError, Parser};
use cel::Env;
use std::sync::{Arc, OnceLock};

/// `parser.AccumulatorName`-style accumulator the standard macros use in
/// the `cel` crate (`parser/macros.rs`).
const ACCU: &str = "@result";

/// `cel.@mapInsert` (`ext/comprehensions.go` `mapInsert`).
pub const MAP_INSERT: &str = "cel.@mapInsert";

type Target = Option<IdedExpr>;
type Args = Vec<IdedExpr>;
type Expansion = Result<Option<IdedExpr>, ParseError>;

impl EnvSet {
    /// A parser expanding the macros the environment of `env_type` has.
    pub fn parser(&self, env_type: EnvType) -> Parser {
        parser(
            self.sort_by_macro(env_type),
            self.two_var_comprehensions(env_type),
        )
    }
}

/// A parser with `sortBy` (`lists`) and the two-variable comprehension
/// macros (`two_var`), optional syntax enabled as the K8s environment has it.
pub fn parser(lists: bool, two_var: bool) -> Parser {
    static ENVS: [OnceLock<Arc<Env>>; 4] = [
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
    ];
    let env = ENVS[usize::from(lists) * 2 + usize::from(two_var)].get_or_init(|| {
        let mut env = Env::stdlib();
        if lists {
            env.add_extension(cel::extensions::lists)
                .expect("lists extension");
        }
        if two_var {
            for m in two_var_macros() {
                env.add_macro(m).expect("two-variable macro");
            }
        }
        Arc::new(env)
    });
    env.parser().enable_optional_syntax(true)
}

fn two_var_macros() -> Vec<Macro> {
    let mut macros = vec![
        Macro::receiver("all", 3, quantifier(Quantifier::All)),
        Macro::receiver("exists", 3, quantifier(Quantifier::Exists)),
        Macro::receiver("existsOne", 3, quantifier(Quantifier::ExistsOne)),
        Macro::receiver("exists_one", 3, quantifier(Quantifier::ExistsOne)),
    ];
    for n in [3, 4] {
        macros.push(Macro::receiver("transformList", n, transform(Kind::List)));
        macros.push(Macro::receiver("transformMap", n, transform(Kind::Map)));
        macros.push(Macro::receiver(
            "transformMapEntry",
            n,
            transform(Kind::MapEntry),
        ));
    }
    macros
}

fn call(h: &mut MacroExprHelper<'_>, f: &str, args: Vec<IdedExpr>) -> IdedExpr {
    h.next_expr(Expr::Call(CallExpr {
        func_name: f.to_string(),
        target: None,
        args,
    }))
}

fn accu(h: &mut MacroExprHelper<'_>) -> IdedExpr {
    h.next_expr(Expr::Ident(ACCU.to_string()))
}

fn literal(h: &mut MacroExprHelper<'_>, l: LiteralValue) -> IdedExpr {
    h.next_expr(Expr::Literal(l))
}

/// `extractIterVars` (`comprehensions.go`).
fn iter_vars(
    h: &mut MacroExprHelper<'_>,
    a0: &IdedExpr,
    a1: &IdedExpr,
) -> Result<(String, String), ParseError> {
    let one = |h: &MacroExprHelper<'_>, e: &IdedExpr| match &e.expr {
        Expr::Ident(n) => Ok(n.clone()),
        _ => Err(h.new_error(e.id, "argument must be a simple name")),
    };
    let v1 = one(h, a0)?;
    let v2 = one(h, a1)?;
    if v1 == v2 {
        return Err(h.new_error(a1.id, format!("duplicate variable name: {v1}")));
    }
    // `mef.AccuIdentName()` and `parser.AccumulatorName`.
    let is_accu = |v: &str| v == ACCU || v == "__result__";
    if is_accu(&v1) {
        return Err(h.new_error(a0.id, "iteration variable overwrites accumulator variable"));
    }
    if is_accu(&v2) {
        return Err(h.new_error(a1.id, "iteration variable overwrites accumulator variable"));
    }
    Ok((v1, v2))
}

#[derive(Clone, Copy)]
enum Quantifier {
    All,
    Exists,
    ExistsOne,
}

/// `quantifierAll`, `quantifierExists`, `quantifierExistsOne`.
fn quantifier(
    q: Quantifier,
) -> impl Fn(&mut MacroExprHelper<'_>, &mut Target, &mut Args) -> Expansion {
    move |h, target, args| {
        let (v1, v2) = iter_vars(h, &args[0], &args[1])?;
        let pred = args.pop().expect("three arguments");
        let (init, cond, step, result) = match q {
            Quantifier::All => {
                let init = literal(h, LiteralValue::Boolean(true.into()));
                let a = accu(h);
                let cond = call(h, operators::NOT_STRICTLY_FALSE, vec![a]);
                let a = accu(h);
                let step = call(h, operators::LOGICAL_AND, vec![a, pred]);
                (init, cond, step, accu(h))
            }
            Quantifier::Exists => {
                let init = literal(h, LiteralValue::Boolean(false.into()));
                let a = accu(h);
                let not = call(h, operators::LOGICAL_NOT, vec![a]);
                let cond = call(h, operators::NOT_STRICTLY_FALSE, vec![not]);
                let a = accu(h);
                let step = call(h, operators::LOGICAL_OR, vec![a, pred]);
                (init, cond, step, accu(h))
            }
            Quantifier::ExistsOne => {
                let init = literal(h, LiteralValue::Int(0.into()));
                let cond = literal(h, LiteralValue::Boolean(true.into()));
                let a = accu(h);
                let one = literal(h, LiteralValue::Int(1.into()));
                let plus = call(h, operators::ADD, vec![a, one]);
                let a = accu(h);
                let step = call(h, operators::CONDITIONAL, vec![pred, plus, a]);
                let a = accu(h);
                let one = literal(h, LiteralValue::Int(1.into()));
                let result = call(h, operators::EQUALS, vec![a, one]);
                (init, cond, step, result)
            }
        };
        Ok(Some(comprehension(
            h,
            target.take().expect("receiver macro"),
            v1,
            v2,
            init,
            cond,
            step,
            result,
        )))
    }
}

#[derive(Clone, Copy)]
enum Kind {
    List,
    Map,
    MapEntry,
}

/// `transformList`, `transformMap`, `transformMapEntry`.
fn transform(k: Kind) -> impl Fn(&mut MacroExprHelper<'_>, &mut Target, &mut Args) -> Expansion {
    move |h, target, args| {
        let (v1, _v2) = iter_vars(h, &args[0], &args[1])?;
        let v2 = match &args[1].expr {
            Expr::Ident(n) => n.clone(),
            _ => unreachable!("checked by iter_vars"),
        };
        let body = args.pop().expect("transform");
        let filter = (args.len() == 3).then(|| args.pop().expect("filter"));
        let (init, step) = match k {
            Kind::List => {
                let init = h.next_expr(Expr::List(ListExpr::new(Vec::new())));
                let a = accu(h);
                let one = h.next_expr(Expr::List(ListExpr::new(vec![body])));
                (init, call(h, operators::ADD, vec![a, one]))
            }
            Kind::Map => {
                let init = h.next_expr(Expr::Map(MapExpr { entries: vec![] }));
                let a = accu(h);
                let key = h.next_expr(Expr::Ident(v1.clone()));
                (init, call(h, MAP_INSERT, vec![a, key, body]))
            }
            Kind::MapEntry => {
                let init = h.next_expr(Expr::Map(MapExpr { entries: vec![] }));
                let a = accu(h);
                (init, call(h, MAP_INSERT, vec![a, body]))
            }
        };
        let step = match filter {
            Some(f) => {
                let a = accu(h);
                call(h, operators::CONDITIONAL, vec![f, step, a])
            }
            None => step,
        };
        let cond = literal(h, LiteralValue::Boolean(true.into()));
        let result = accu(h);
        Ok(Some(comprehension(
            h,
            target.take().expect("receiver macro"),
            v1,
            v2,
            init,
            cond,
            step,
            result,
        )))
    }
}

/// `NewComprehensionTwoVar`.
#[allow(clippy::too_many_arguments)]
fn comprehension(
    h: &mut MacroExprHelper<'_>,
    range: IdedExpr,
    v1: String,
    v2: String,
    init: IdedExpr,
    cond: IdedExpr,
    step: IdedExpr,
    result: IdedExpr,
) -> IdedExpr {
    h.next_expr(Expr::Comprehension(Box::new(ComprehensionExpr {
        iter_range: range,
        iter_var: v1,
        iter_var2: Some(v2),
        accu_var: ACCU.to_string(),
        accu_init: init,
        loop_cond: cond,
        loop_step: step,
        result,
    })))
}
