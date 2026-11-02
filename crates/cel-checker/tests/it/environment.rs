//! Version gating of the base environment (`environment/base.go` `baseOpts`,
//! `environment.go` `filterAndBuildOpts`). The upstream tests this follows:
//! `environment_test.go` (`TestBaseEnvironment`) and the library
//! compatibility test (`library_compatibility_test.go`).

use cel::parser::Parser;
use rusternetes_cel_checker::{
    check, Container, DType, EnvSet, EnvType, Features, MapProvider, NoLocations, VariableDecl,
    Version,
};
use std::sync::Arc;

fn errors(set: &EnvSet, t: EnvType, src: &str) -> Vec<String> {
    let expr = Parser::new()
        .enable_optional_syntax(true)
        .parse(src)
        .unwrap_or_else(|e| panic!("{src}: {e}"));
    let mut env = set.env(t, Container::default(), Arc::new(MapProvider::new()));
    let (_, errs) = check(&expr, &mut env, &NoLocations);
    errs.errors().iter().map(|e| e.message.clone()).collect()
}

fn set(minor: u32) -> EnvSet {
    EnvSet::must_base_env_set(Version::major_minor(1, minor), &Features::default())
}

fn undeclared(f: &str) -> String {
    format!("undeclared reference to '{f}' (in container '')")
}

#[test]
fn libraries_arrive_at_their_introduced_version() {
    // (expression, function that is undeclared before `minor`, minor)
    let cases = [
        ("url('http://a').getHost() == 'a'", "url", 0),
        ("'a1'.find('[0-9]') == '1'", "find", 0),
        ("[1, 2].isSorted()", "isSorted", 0),
        ("isQuantity('1Gi')", "isQuantity", 28),
        ("sets.contains([1], [1])", "sets", 29),
        ("isIP('1.2.3.4')", "isIP", 30),
        ("isCIDR('1.2.3.0/24')", "isCIDR", 30),
        (
            "format.dns1123Label().validate('a').hasValue()",
            "format",
            31,
        ),
        ("isSemver('1.2.3')", "isSemver", 33),
        ("[1, 2, 3].slice(0, 1) == [1]", "slice", 34),
    ];
    for (src, missing, minor) in cases {
        if minor > 0 {
            let errs = errors(&set(minor - 1), EnvType::NewExpressions, src);
            assert!(
                errs.contains(&undeclared(missing)),
                "{src} at 1.{}: {errs:?}",
                minor - 1
            );
            // StoredExpressions always has every library.
            let errs = errors(&set(minor - 1), EnvType::StoredExpressions, src);
            assert!(errs.is_empty(), "{src} stored: {errs:?}");
        }
        let errs = errors(&set(minor), EnvType::NewExpressions, src);
        assert!(errs.is_empty(), "{src} at 1.{minor}: {errs:?}");
    }
}

#[test]
fn strings_library_is_replaced_not_added() {
    // 1.28: ext.Strings(0): charAt and join exist (join at every version,
    // ext/strings.go:498-534), strings.quote does not.
    let s = set(28);
    assert!(errors(&s, EnvType::NewExpressions, "'abc'.charAt(1) == 'b'").is_empty());
    assert!(!errors(&s, EnvType::NewExpressions, "strings.quote('a')").is_empty());
    assert!(errors(&s, EnvType::NewExpressions, "['a'].join()").is_empty());
    // 1.29: ext.Strings(2).
    let s = set(29);
    assert!(errors(&s, EnvType::NewExpressions, "'abc'.charAt(1) == 'b'").is_empty());
    assert!(errors(&s, EnvType::NewExpressions, "['a', 'b'].join(',') == 'a,b'").is_empty());
    assert!(errors(&s, EnvType::NewExpressions, "strings.quote('a')").is_empty());
}

#[test]
fn numeric_cross_comparison_and_optionals_from_1_28() {
    let src = "1 < 1.0";
    assert!(!errors(&set(27), EnvType::NewExpressions, src).is_empty());
    assert!(errors(&set(28), EnvType::NewExpressions, src).is_empty());
    // The optional library (`optional.of`) and `optional_type`.
    let opt = "optional.of(1).hasValue()";
    assert!(!errors(&set(27), EnvType::NewExpressions, opt).is_empty());
    assert!(errors(&set(28), EnvType::NewExpressions, opt).is_empty());
}

#[test]
fn aggregate_literals_are_homogeneous_in_every_version() {
    for minor in [20, 27, 35] {
        assert_eq!(
            errors(&set(minor), EnvType::NewExpressions, "[1, 'a']"),
            ["expected type 'int' but found 'string'"]
        );
    }
}

#[test]
fn feature_gated_library_follows_the_gate_for_new_expressions_only() {
    let src = "authorizer.group('').resource('pods').fieldSelector('a=b').check('get').allowed()";
    let on = Features {
        authorize_with_selectors: true,
    };
    let v = Version::major_minor(1, 30);
    let gated_on = EnvSet::must_base_env_set(v, &on);
    let gated_off = EnvSet::must_base_env_set(v, &Features::default());
    let run = |s: &EnvSet, t: EnvType| {
        let expr = Parser::new().parse(src).unwrap();
        let mut env = s.env(t, Container::default(), Arc::new(MapProvider::new()));
        env.add_ident(VariableDecl::new(
            "authorizer",
            DType::Struct("kubernetes.authorization.Authorizer".into()),
        ))
        .unwrap();
        let (_, errs) = check(&expr, &mut env, &NoLocations);
        errs.errors()
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    };
    // An enabled gate allows the library whatever the compatibility version
    // (`allowedByFeatureGate`), a disabled one hides it from NewExpressions.
    assert!(run(&gated_on, EnvType::NewExpressions).is_empty());
    assert!(!run(&gated_off, EnvType::NewExpressions).is_empty());
    // StoredExpressions ignores gates.
    assert!(run(&gated_off, EnvType::StoredExpressions).is_empty());
}

#[test]
fn two_variable_comprehensions_from_1_32() {
    assert!(!set(31).two_var_comprehensions(EnvType::NewExpressions));
    assert!(set(32).two_var_comprehensions(EnvType::NewExpressions));
    assert!(set(31).two_var_comprehensions(EnvType::StoredExpressions));
}
