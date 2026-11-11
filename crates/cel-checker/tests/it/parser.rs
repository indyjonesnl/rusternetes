//! The macros the checker's parser expands (`parser.rs`): `sortBy` of
//! `ext.Lists` (introduced at 1.34) and the two-variable comprehensions of
//! `ext.TwoVarComprehensions()` (1.32, `ext/comprehensions.go`), gated like
//! the libraries that bring them (`environment/base.go`). The type checks of
//! the expansions are compared with cel-go in `goldens.rs`.

use cel::common::ast::Expr;
use k3rs_cel_checker::{EnvSet, EnvType, Features, Version};

fn set(minor: u32) -> EnvSet {
    EnvSet::must_base_env_set(Version::major_minor(1, minor), &Features::default())
}

fn expr(minor: u32, t: EnvType, src: &str) -> Expr {
    set(minor)
        .parser(t)
        .parse(src)
        .unwrap_or_else(|e| panic!("{src}: {e}"))
        .expr
}

#[test]
fn two_variable_macros_expand_from_1_32() {
    for src in [
        "[1].all(i, v, v > i)",
        "[1].exists(i, v, v > i)",
        "[1].existsOne(i, v, v > i)",
        "[1].exists_one(i, v, v > i)",
        "[1].transformList(i, v, v)",
        "[1].transformList(i, v, true, v)",
        "[1].transformMap(i, v, v)",
        "[1].transformMap(i, v, true, v)",
        "[1].transformMapEntry(i, v, {v: i})",
        "[1].transformMapEntry(i, v, true, {v: i})",
    ] {
        match expr(32, EnvType::NewExpressions, src) {
            Expr::Comprehension(c) => {
                assert_eq!(c.iter_var, "i", "{src}");
                assert_eq!(c.iter_var2.as_deref(), Some("v"), "{src}");
            }
            other => panic!("{src}: {other:?}"),
        }
        // Stored expressions have every library, whatever the version.
        assert!(
            matches!(
                expr(31, EnvType::StoredExpressions, src),
                Expr::Comprehension(_)
            ),
            "{src}"
        );
        // Before 1.32 the call is the plain member call it is written as.
        assert!(
            matches!(expr(31, EnvType::NewExpressions, src), Expr::Call(_)),
            "{src}"
        );
    }
}

#[test]
fn one_variable_macros_are_always_expanded() {
    assert!(matches!(
        expr(31, EnvType::NewExpressions, "[1].exists(v, v > 0)"),
        Expr::Comprehension(_)
    ));
}

/// `ext.Lists(ext.ListsVersion(3))` is introduced at 1.34.
#[test]
fn sort_by_expands_from_1_34() {
    let src = "[2, 1].sortBy(x, x)";
    assert!(matches!(
        expr(34, EnvType::NewExpressions, src),
        Expr::Comprehension(_)
    ));
    assert!(matches!(
        expr(33, EnvType::StoredExpressions, src),
        Expr::Comprehension(_)
    ));
    assert!(matches!(
        expr(33, EnvType::NewExpressions, src),
        Expr::Call(_)
    ));
}

/// `extractIterVars` (`comprehensions.go`).
#[test]
fn iteration_variables_are_validated() {
    let err = |src: &str| {
        set(35)
            .parser(EnvType::NewExpressions)
            .parse(src)
            .unwrap_err()
            .errors[0]
            .msg
            .clone()
    };
    assert_eq!(err("[1].exists(i, i, true)"), "duplicate variable name: i");
    assert_eq!(
        err("[1].exists(1, v, true)"),
        "argument must be a simple name"
    );
    // `@result` cannot be written in a source, so the accumulator check has
    // no test here.
}
