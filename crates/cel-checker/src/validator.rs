//! `cel/validator.go` `homogeneousAggregateLiteralValidator`: the AST
//! validator `cel.HomogeneousAggregateLiterals()` installs
//! (`cel/options.go`), which the Kubernetes environment has from the base
//! options (`environment/base.go:63-69`).
//!
//! It is not part of the type check: `Env.Check` (`cel/env.go:344-385`) runs
//! the validators only on a type-check result without errors, over the final
//! type map (type parameters already substituted, so an unbound `_var0` is
//! `dyn` in its messages), and reports through `Issues.ReportErrorAtID`. The
//! checker itself joins mixed element types to `dyn` (`joinTypes` with
//! `dynElementType`).

use crate::checker::CheckedAst;
use crate::errors::{Errors, Locator};
use crate::types::DType;
use cel::common::ast::{EntryExpr, Expr, IdedExpr};

/// `ValidatorConfig` for this validator: the functions whose arguments may
/// hold mixed literals (`HomogeneousAggregateLiteralExemptFunctions`, which
/// `ext.Strings` adds `format` to from `StringsVersion(1)`).
#[derive(Clone, Debug, Default)]
pub struct HomogeneousConfig {
    pub exempt_functions: Vec<String>,
}

/// `Type.IsEquivalentType`: the same kind, name and parameters, ignoring the
/// names of type parameters.
fn equivalent(a: &DType, b: &DType) -> bool {
    match (a, b) {
        (DType::Param(_), DType::Param(_)) => true,
        (DType::Type(x), DType::Type(y)) => match (x, y) {
            (Some(x), Some(y)) => equivalent(x, y),
            (None, None) => true,
            _ => false,
        },
        (DType::List(x), DType::List(y)) => equivalent(x, y),
        (DType::Map(k1, v1), DType::Map(k2, v2)) => equivalent(k1, k2) && equivalent(v1, v2),
        (DType::Opaque(n1, p1), DType::Opaque(n2, p2)) => {
            n1 == n2 && p1.len() == p2.len() && p1.iter().zip(p2).all(|(x, y)| equivalent(x, y))
        }
        _ => a == b,
    }
}

/// Runs the validator over `expr`, reporting into `errors`.
pub fn validate_homogeneous_literals(
    expr: &IdedExpr,
    checked: &CheckedAst,
    config: &HomogeneousConfig,
    loc: &dyn Locator,
    errors: &mut Errors,
) {
    let mut lists = Vec::new();
    let mut maps = Vec::new();
    collect(expr, config, false, &mut lists, &mut maps);
    let ty = |id: u64| checked.type_map.get(&id).cloned().unwrap_or(DType::Error);
    let report = |errors: &mut Errors, id: u64, expected: &DType, actual: &DType| {
        errors.type_mismatch(id, loc, expected, actual);
    };
    // `ast.MatchDescendants(root, KindMatcher(ListKind))`, then maps.
    for l in lists {
        let Expr::List(list) = &l.expr else { continue };
        let mut elem: Option<DType> = None;
        for (i, e) in list.elements.iter().enumerate() {
            let mut et = ty(e.id);
            if list.optional_indices.contains(&i) {
                // `et.Parameters()[0]`.
                et = match &et {
                    DType::Opaque(_, ps) if !ps.is_empty() => ps[0].clone(),
                    other => other.clone(),
                };
            }
            match &elem {
                None => elem = Some(et),
                Some(first) => {
                    if !equivalent(first, &et) {
                        report(errors, e.id, first, &et);
                        break;
                    }
                }
            }
        }
    }
    for m in maps {
        let Expr::Map(map) = &m.expr else { continue };
        let mut first: Option<(DType, DType)> = None;
        for entry in &map.entries {
            let EntryExpr::MapEntry(me) = &entry.expr else {
                continue;
            };
            let kt = ty(me.key.id);
            let mut vt = ty(me.value.id);
            if me.optional {
                vt = match &vt {
                    DType::Opaque(_, ps) if !ps.is_empty() => ps[0].clone(),
                    other => other.clone(),
                };
            }
            match &first {
                None => first = Some((kt, vt)),
                Some((fk, fv)) => {
                    if !equivalent(fk, &kt) {
                        report(errors, me.key.id, fk, &kt);
                    }
                    if !equivalent(fv, &vt) {
                        report(errors, me.value.id, fv, &vt);
                    }
                }
            }
        }
    }
}

/// The list and map literals under `e`, in pre-order, leaving out those with
/// an exempt function among their ancestors (`inExemptFunction`).
fn collect<'a>(
    e: &'a IdedExpr,
    config: &HomogeneousConfig,
    exempt: bool,
    lists: &mut Vec<&'a IdedExpr>,
    maps: &mut Vec<&'a IdedExpr>,
) {
    match &e.expr {
        Expr::List(l) => {
            if !exempt {
                lists.push(e);
            }
            for c in &l.elements {
                collect(c, config, exempt, lists, maps);
            }
        }
        Expr::Map(m) => {
            if !exempt {
                maps.push(e);
            }
            for entry in &m.entries {
                if let EntryExpr::MapEntry(me) = &entry.expr {
                    collect(&me.key, config, exempt, lists, maps);
                    collect(&me.value, config, exempt, lists, maps);
                }
            }
        }
        Expr::Call(c) => {
            let exempt = exempt || config.exempt_functions.contains(&c.func_name);
            if let Some(t) = &c.target {
                collect(t, config, exempt, lists, maps);
            }
            for a in &c.args {
                collect(a, config, exempt, lists, maps);
            }
        }
        Expr::Select(s) => collect(&s.operand, config, exempt, lists, maps),
        Expr::Struct(s) => {
            for entry in &s.entries {
                if let EntryExpr::StructField(f) = &entry.expr {
                    collect(&f.value, config, exempt, lists, maps);
                }
            }
        }
        Expr::Comprehension(c) => {
            for part in [
                &c.iter_range,
                &c.accu_init,
                &c.loop_cond,
                &c.loop_step,
                &c.result,
            ] {
                collect(part, config, exempt, lists, maps);
            }
        }
        _ => {}
    }
}
