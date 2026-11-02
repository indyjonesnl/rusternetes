//! `checker/types.go` and `checker/mapping.go`: assignability with
//! type-parameter substitution, and `checker.resolveOverload`
//! (`checker/checker.go:261-348`).

use crate::types::{DType, Overload};
use std::collections::HashMap;

/// `checker.mapping`: type-parameter name to its substitution.
pub type Mapping = HashMap<String, DType>;

fn find(m: &Mapping, t: &DType) -> Option<DType> {
    match t {
        DType::Param(n) => m.get(n).cloned(),
        _ => None,
    }
}

/// `types.Type.IsAssignableType` (`defaultIsAssignableType`): `t` accepts
/// `from` structurally.
fn is_assignable_type(t: &DType, from: &DType) -> bool {
    if t == from || t.is_dyn() || matches!(t, DType::Param(_)) {
        return true;
    }
    if !t.same_kind(from) || t.kind_name() != from.kind_name() {
        return false;
    }
    let (tp, fp) = (t.params(), from.params());
    tp.len() == fp.len()
        && tp
            .iter()
            .zip(fp.iter())
            .all(|(a, b)| is_assignable_type(a, b))
}

/// `isLegacyNullable` or assignable from `null`.
fn assignable_null(t: &DType) -> bool {
    matches!(
        t,
        DType::Opaque(..)
            | DType::Struct(_)
            | DType::Any
            | DType::Dyn
            | DType::Duration
            | DType::Timestamp
            | DType::Null
            | DType::Param(_)
    )
}

/// `isEqualOrLessSpecific`.
fn equal_or_less_specific(t1: &DType, t2: &DType) -> bool {
    if t1.is_dyn() || matches!(t1, DType::Param(_)) {
        return true;
    }
    if t2.is_dyn() || matches!(t2, DType::Param(_)) {
        return false;
    }
    if !t1.same_kind(t2) {
        return false;
    }
    match t1 {
        DType::Opaque(n1, p1) => {
            let DType::Opaque(n2, p2) = t2 else {
                return false;
            };
            n1 == n2
                && p1.len() == p2.len()
                && p1
                    .iter()
                    .zip(p2.iter())
                    .all(|(a, b)| equal_or_less_specific(a, b))
        }
        DType::List(a) => {
            let DType::List(b) = t2 else { return false };
            equal_or_less_specific(a, b)
        }
        DType::Map(k1, v1) => {
            let DType::Map(k2, v2) = t2 else {
                return false;
            };
            equal_or_less_specific(k1, k2) && equal_or_less_specific(v1, v2)
        }
        DType::Type(_) => true,
        _ => t1 == t2,
    }
}

/// `mostGeneral`.
pub fn most_general(t1: &DType, t2: &DType) -> DType {
    if equal_or_less_specific(t1, t2) {
        t1.clone()
    } else {
        t2.clone()
    }
}

/// `notReferencedIn` (the occurs check).
fn not_referenced_in(m: &Mapping, t: &DType, within: &DType) -> bool {
    if t == within {
        return false;
    }
    match within {
        DType::Param(_) => match find(m, within) {
            None => true,
            Some(sub) => not_referenced_in(m, t, &sub),
        },
        DType::Opaque(..) | DType::List(_) | DType::Map(..) | DType::Type(_) => {
            within.params().iter().all(|p| not_referenced_in(m, t, p))
        }
        _ => true,
    }
}

/// `isValidTypeSubstitution`: (valid, t2 already has a substitution).
fn valid_substitution(m: &mut Mapping, t1: &DType, t2: &DType) -> (bool, bool) {
    if t1.same_kind(t2) && t1 == t2 {
        return (true, true);
    }
    let name = match t2 {
        DType::Param(n) => n.clone(),
        _ => return (false, false),
    };
    if let Some(sub) = find(m, t2) {
        if t1.same_kind(&sub) && *t1 == sub {
            return (true, true);
        }
        if assignable(m, t1, &sub) {
            let new = most_general(t1, &sub);
            if not_referenced_in(m, t2, &new) {
                m.insert(name, new);
            }
            return (true, true);
        }
        return (false, true);
    }
    if not_referenced_in(m, t2, t1) {
        m.insert(name, t1.clone());
        return (true, false);
    }
    (false, false)
}

/// `internalIsAssignable`: is `t1` assignable to `t2`.
fn assignable(m: &mut Mapping, t1: &DType, t2: &DType) -> bool {
    if matches!(t2, DType::Param(_)) {
        let (valid, has_sub) = valid_substitution(m, t1, t2);
        if valid {
            return true;
        }
        if !valid && has_sub {
            return false;
        }
    }
    if matches!(t1, DType::Param(_)) {
        let (valid, _) = valid_substitution(m, t2, t1);
        return valid;
    }
    if t1.is_dyn_or_error() || t2.is_dyn_or_error() {
        return true;
    }
    if matches!(t1, DType::Null) {
        return assignable_null(t2);
    }
    if matches!(t2, DType::Null) {
        return assignable_null(t1);
    }
    match t1 {
        DType::Bool
        | DType::Bytes
        | DType::Double
        | DType::Int
        | DType::Str
        | DType::Uint
        | DType::Duration
        | DType::Timestamp
        | DType::Struct(_) => is_assignable_type(t2, t1),
        DType::Type(_) => matches!(t2, DType::Type(_)),
        DType::Opaque(..) | DType::List(_) | DType::Map(..) => {
            t1.same_kind(t2) && t1.kind_name() == t2.kind_name() && {
                let (p1, p2) = (t1.params(), t2.params());
                p1.len() == p2.len() && p1.iter().zip(p2.iter()).all(|(a, b)| assignable(m, a, b))
            }
        }
        _ => false,
    }
}

/// `checker.isAssignable` on a checker's mapping: on success the mapping
/// holds the new substitutions, on failure it is left unchanged.
pub fn is_assignable_in(m: &mut Mapping, t1: &DType, t2: &DType) -> bool {
    let mut copy = m.clone();
    if assignable(&mut copy, t1, t2) {
        *m = copy;
        true
    } else {
        false
    }
}

/// `checker.isAssignableList` (see [`is_assignable_in`]).
pub fn is_assignable_list_in(m: &mut Mapping, l1: &[DType], l2: &[DType]) -> bool {
    assignable_list(m, l1, l2)
}

/// `internalIsAssignableList` on a copy of the mapping, committing it only on
/// success (`isAssignableList`).
fn assignable_list(m: &mut Mapping, l1: &[DType], l2: &[DType]) -> bool {
    if l1.len() != l2.len() {
        return false;
    }
    let mut copy = m.clone();
    if l1
        .iter()
        .zip(l2.iter())
        .all(|(a, b)| assignable(&mut copy, a, b))
    {
        *m = copy;
        true
    } else {
        false
    }
}

/// `checker.substitute` with `typeParamToDyn`.
pub fn substitute(m: &Mapping, t: &DType, param_to_dyn: bool) -> DType {
    if let Some(sub) = find(m, t) {
        return substitute(m, &sub, param_to_dyn);
    }
    match t {
        DType::Param(_) if param_to_dyn => DType::Dyn,
        DType::Opaque(n, ps) => DType::Opaque(
            n.clone(),
            ps.iter().map(|p| substitute(m, p, param_to_dyn)).collect(),
        ),
        DType::List(e) => DType::List(Box::new(substitute(m, e, param_to_dyn))),
        DType::Map(k, v) => DType::Map(
            Box::new(substitute(m, k, param_to_dyn)),
            Box::new(substitute(m, v, param_to_dyn)),
        ),
        DType::Type(Some(p)) => DType::Type(Some(Box::new(substitute(m, p, param_to_dyn)))),
        other => other.clone(),
    }
}

/// `checker.isAssignable(argType, target)` outside a call: used for the
/// logical operators' `typeMismatch`.
pub fn is_assignable(t1: &DType, t2: &DType) -> bool {
    assignable(&mut Mapping::new(), t1, t2)
}

/// The outcome of `resolveOverload`.
pub enum Resolution {
    /// The result type of the first matching overload.
    #[allow(dead_code)] // the pass keeps its own call result types, see `cel_dra`
    Resolved(DType),
    /// No overload accepted the arguments: the substituted argument types for
    /// `noMatchingOverload` (the receiver first for a member call).
    NoMatch(Vec<DType>),
}

/// `checker.resolveOverload` for the ordinary (non-logical) functions.
/// `arg_types` holds the receiver first when `member`.
pub fn resolve(decls: &[Overload], member: bool, arg_types: &[DType]) -> Resolution {
    let mut result: Option<DType> = None;
    let mut mapping = Mapping::new();
    for o in decls {
        if o.member != member {
            continue;
        }
        // Each overload gets its own fresh type variables; the parameters of
        // different overloads therefore never interact, so a per-call mapping
        // reproduces `substitutions.add(typePar, c.newTypeVar())`.
        let mut local = Mapping::new();
        if !assignable_list(&mut local, arg_types, &o.args) {
            continue;
        }
        let r = substitute(&local, &o.result, false);
        match &result {
            None => result = Some(r),
            Some(prev) if !prev.is_dyn() && *prev != r => result = Some(DType::Dyn),
            Some(_) => {}
        }
        mapping.extend(local);
    }
    match result {
        Some(r) => Resolution::Resolved(r),
        None => Resolution::NoMatch(
            arg_types
                .iter()
                .map(|a| substitute(&mapping, a, true))
                .collect(),
        ),
    }
}

/// `checker/format.go` `formatFunctionDeclType(nil, args, isInstance)`.
pub fn format_signature(args: &[DType], is_instance: bool) -> String {
    let (target, rest) = if is_instance {
        (Some(&args[0]), &args[1..])
    } else {
        (None, args)
    };
    let mut out = String::new();
    if let Some(t) = target {
        out.push_str(&t.name());
        out.push('.');
    }
    out.push('(');
    out.push_str(&rest.iter().map(DType::name).collect::<Vec<_>>().join(", "));
    out.push(')');
    out
}
