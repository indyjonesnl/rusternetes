//! `K3RS_CEL_CHECKER=shadow`: the checker port compared with the
//! compile probe (`cel_versions::compile_failure`), without affecting it.

use k3rs_common::cel_checker_shadow::{check_expression, divergence, Mode};
use k3rs_common::cel_versions::{compile_failure, EnvSet, EnvType};
use k3rs_common::feature_gates::Version;

fn set(minor: u32) -> EnvSet {
    EnvSet::must_base_env_set(Version::major_minor(1, minor))
}

#[test]
fn mode_defaults_to_off() {
    assert_eq!(Mode::parse(None), Mode::Off);
    assert_eq!(Mode::parse(Some("")), Mode::Off);
    assert_eq!(Mode::parse(Some("off")), Mode::Off);
    assert_eq!(Mode::parse(Some("enforce")), Mode::Off);
    assert_eq!(Mode::parse(Some("shadow")), Mode::Shadow);
    assert_eq!(Mode::parse(Some(" Shadow ")), Mode::Shadow);
}

#[test]
fn checker_verdict_follows_the_environment() {
    let new = EnvType::NewExpressions;
    assert_eq!(
        check_expression("object.a + 1 > 2", &set(35), new, None),
        Some(vec![])
    );
    // isIP arrives in 1.30, and stored expressions always have it.
    let src = "isIP('1.2.3.4')";
    assert!(!check_expression(src, &set(29), new, None)
        .unwrap()
        .is_empty());
    assert_eq!(
        check_expression(src, &set(29), EnvType::StoredExpressions, None),
        Some(vec![])
    );
    // Syntax errors are not the comparison's concern.
    assert_eq!(check_expression("1 +", &set(35), new, None), None);
}

#[test]
fn probe_and_checker_agree_on_library_gating() {
    for (src, minor) in [
        ("isIP('1.2.3.4')", 29),
        ("isIP('1.2.3.4')", 30),
        ("['a'].join(',')", 28),
        ("sets.contains([1], [1])", 28),
        ("object.metadata.name.startsWith('a')", 35),
        ("request.userInfo.username == 'x'", 27),
    ] {
        for env in [EnvType::NewExpressions, EnvType::StoredExpressions] {
            let s = set(minor);
            let probe = compile_failure(src, &s, env);
            assert_eq!(
                divergence(src, &s, env, &probe, None),
                None,
                "{src} at 1.{minor}"
            );
        }
    }
}

#[test]
fn a_disagreement_is_reported() {
    let s = set(35);
    // The probe says fine; the checker rejects the overload.
    let d = divergence("1 + 'a'", &s, EnvType::NewExpressions, &None, None).unwrap();
    assert_eq!(d.probe, None);
    assert_eq!(d.checker.len(), 1);
}

#[test]
fn the_probe_result_is_unchanged_by_the_hook() {
    // Whatever the flag, `compile_failure` returns the probe's own answer.
    let s = set(29);
    assert!(compile_failure("isIP('1.2.3.4')", &s, EnvType::NewExpressions).is_some());
    assert!(compile_failure("1 + 'a'", &s, EnvType::NewExpressions).is_none());
}
