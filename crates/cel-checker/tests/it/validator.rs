//! `cel/validator.go` `homogeneousAggregateLiteralValidator`, which the
//! Kubernetes environment installs through `cel.HomogeneousAggregateLiterals()`
//! (`environment/base.go:63-69`): run after a type check without errors, over
//! the final types, with `format` exempted by `ext.Strings`
//! (`ext/formatting.go` `stringFormatValidator.Configure`). The comparison
//! with cel-go is in `goldens.rs`.

use k3rs_cel_checker::{
    check, Container, EnvSet, EnvType, Features, MapProvider, SourceLocator, Version,
};
use std::sync::Arc;

fn errors(minor: u32, src: &str) -> Vec<String> {
    let set = EnvSet::must_base_env_set(Version::major_minor(1, minor), &Features::default());
    let (expr, info) = set
        .parser(EnvType::NewExpressions)
        .parse_with_source_info(src)
        .unwrap();
    let mut env = set.env(
        EnvType::NewExpressions,
        Container::default(),
        Arc::new(MapProvider::new()),
    );
    let locator = SourceLocator::new(&info, src);
    let (_, errs) = check(&expr, &mut env, &locator);
    errs.errors().iter().map(|e| e.message.clone()).collect()
}

#[test]
fn mixed_literals_are_reported() {
    assert_eq!(
        errors(35, "[1, 'a']"),
        ["expected type 'int' but found 'string'"]
    );
    assert_eq!(
        errors(35, "{1: 'a', 'b': 3}"),
        [
            "expected type 'int' but found 'string'",
            "expected type 'string' but found 'int'"
        ]
    );
    assert!(errors(35, "[1, 2]").is_empty());
    assert!(errors(35, "[1, dyn('a')]").len() == 1);
}

/// The validator runs only on a result without type errors, so the mixed
/// list is not reported next to an undeclared reference.
#[test]
fn only_a_clean_check_is_validated() {
    assert_eq!(
        errors(35, "[1, 'a', undeclared]"),
        ["undeclared reference to 'undeclared' (in container '')"]
    );
}

/// The validator sees the final types: an unbound type parameter is `dyn`.
#[test]
fn messages_use_the_substituted_types() {
    assert_eq!(
        errors(35, "{}.map(c, [c, type(c)])"),
        ["expected type 'dyn' but found 'type(dyn)'"]
    );
}

/// `format` is exempt from `StringsVersion(1)`, which the environment has
/// from 1.29.
#[test]
fn format_arguments_are_exempt() {
    assert!(errors(35, "'%s %d'.format(['a', 1])").is_empty());
}
