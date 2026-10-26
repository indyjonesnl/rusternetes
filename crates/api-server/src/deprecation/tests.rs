//! Ports of `endpoints/deprecation/deprecation_test.go` (TestMajorMinor,
//! TestIsDeprecated, TestRemovedRelease, TestWarningMessage) plus registry
//! lookups against the generated release-1.35 data.

use super::*;

fn fake(
    deprecated: (u32, u32),
    removed: Option<(u32, u32)>,
    replacement: Option<(&'static str, &'static str, &'static str)>,
) -> Lifecycle {
    Lifecycle {
        group: "mygroup",
        version: "v1",
        kind: "MyKind",
        deprecated,
        removed,
        replacement,
    }
}

#[test]
fn test_major_minor() {
    // (name, major, minor, expect, want err)
    #[allow(clippy::type_complexity)]
    let cases: &[(&str, &str, &str, (i64, i64), bool)] = &[
        ("empty", "", "", (0, 0), true),
        ("non-numeric major", "A", "0", (0, 0), true),
        ("non-numeric minor", "1", "A", (0, 0), true),
        ("valid", "1", "2", (1, 2), false),
        ("valid zero", "0", "0", (0, 0), false),
        ("valid zero decimal", "01", "02", (1, 2), false),
        ("valid with extra minor", "1", "2+", (1, 2), false),
        ("valid with extra minor", "1", "2.3", (1, 2), false),
    ];
    for (name, major, minor, want, want_err) in cases {
        match major_minor(major, minor) {
            Ok(got) => {
                assert!(!want_err, "{name}: expected error");
                assert_eq!(got, *want, "{name}");
            }
            Err(e) => assert!(want_err, "{name}: unexpected error {e}"),
        }
    }
}

#[test]
fn test_is_deprecated() {
    let obj = |ma, mi| Some(fake((ma, mi), None, None));
    let cases: &[(&str, Option<Lifecycle>, i64, i64, bool)] = &[
        ("no interface", None, 0, 0, false),
        ("interface, zero-value", obj(0, 0), 0, 0, false),
        (
            "interface, non-zero-value, no current value",
            obj(10, 20),
            0,
            0,
            true,
        ),
        (
            "interface, non-zero-value matching major, minor",
            obj(10, 20),
            10,
            20,
            true,
        ),
        (
            "interface, non-zero-value after major, after minor",
            obj(10, 20),
            9,
            19,
            false,
        ),
        (
            "interface, non-zero-value after major, before minor",
            obj(10, 20),
            9,
            21,
            false,
        ),
        (
            "interface, non-zero-value before major, after minor",
            obj(10, 20),
            11,
            19,
            true,
        ),
        (
            "interface, non-zero-value before major, before minor",
            obj(10, 20),
            11,
            21,
            true,
        ),
    ];
    for (name, o, cm, cn, want) in cases {
        assert_eq!(is_deprecated(o.as_ref(), *cm, *cn), *want, "{name}");
    }
}

#[test]
fn test_removed_release() {
    let removed = |ma, mi| Some(fake((1, 1), Some((ma, mi)), None));
    let cases: &[(&str, Option<Lifecycle>, &str)] = &[
        ("no interface", None, ""),
        ("interface, zero-value", removed(0, 0), ""),
        ("interface, non-zero major", removed(1, 0), "1.0"),
        ("interface, non-zero minor", removed(0, 1), "0.1"),
        ("interface, non-zero", removed(1, 2), "1.2"),
    ];
    for (name, o, want) in cases {
        assert_eq!(removed_release(o.as_ref()), *want, "{name}");
    }
}

#[test]
fn test_warning_message() {
    let anon = Lifecycle {
        group: "",
        version: "",
        kind: "",
        deprecated: (0, 0),
        removed: None,
        replacement: None,
    };
    assert_eq!(warning_message(None), "");
    assert_eq!(warning_message(Some(&anon)), "", "deprecated, zero-value");
    assert_eq!(
        warning_message(Some(&fake((1, 2), None, None))),
        "mygroup/v1 MyKind is deprecated in v1.2+"
    );
    assert_eq!(
        warning_message(Some(&fake((1, 2), Some((0, 0)), None))),
        "mygroup/v1 MyKind is deprecated in v1.2+",
        "removed interface, zero-value removal version"
    );
    assert_eq!(
        warning_message(Some(&fake((1, 2), Some((3, 4)), None))),
        "mygroup/v1 MyKind is deprecated in v1.2+, unavailable in v3.4+"
    );
    assert_eq!(
        warning_message(Some(&fake((1, 2), Some((3, 4)), Some(("", "", ""))))),
        "mygroup/v1 MyKind is deprecated in v1.2+, unavailable in v3.4+",
        "replaced interface, zero-value replacement"
    );
    assert_eq!(
        warning_message(Some(&fake(
            (1, 2),
            Some((3, 4)),
            Some(("anothergroup", "v2", "AnotherKind"))
        ))),
        "mygroup/v1 MyKind is deprecated in v1.2+, unavailable in v3.4+; use anothergroup/v2 AnotherKind"
    );
}

/// `apps/v1beta1 Deployment` (apps/v1beta1/zz_generated.prerelease-lifecycle.go:
/// deprecated 1.8, replacement apps/v1 Deployment, removed 1.16).
#[test]
fn lookup_deployment_v1beta1() {
    let l = lookup("apps", "v1beta1", "deployments", "").expect("registered");
    assert_eq!(l.kind, "Deployment");
    assert_eq!(l.deprecated, (1, 8));
    assert_eq!(l.removed, Some((1, 16)));
    // Removed in 1.16 < 1.35: upstream never installs the route (#3755).
    assert_eq!(
        deprecation_for("apps", "v1beta1", "deployments", ""),
        (false, String::new())
    );
    assert_eq!(
        warning_message(Some(l)),
        "apps/v1beta1 Deployment is deprecated in v1.8+, unavailable in v1.16+; use apps/v1 Deployment"
    );
    // status serves the parent kind; scale serves Scale.
    assert!(!deprecation_for("apps", "v1beta1", "deployments", "status").0);
    assert_eq!(
        lookup("apps", "v1beta1", "deployments", "scale")
            .unwrap()
            .kind,
        "Scale"
    );
}

#[test]
fn lookup_pluralization_follows_unsafe_guess_kind_to_resource() {
    // "y" -> "ies", "s" -> "es".
    assert!(lookup("networking.k8s.io", "v1beta1", "ingresses", "").is_some());
    assert!(lookup("policy", "v1beta1", "poddisruptionbudgets", "").is_some());
    assert!(lookup("rbac.authorization.k8s.io", "v1beta1", "clusterroles", "").is_some());
    assert!(lookup("storage.k8s.io", "v1beta1", "storageclasses", "").is_some());
}

#[test]
fn stable_apis_are_not_deprecated() {
    for (g, v, r) in [
        ("", "v1", "pods"),
        ("apps", "v1", "deployments"),
        ("batch", "v1", "cronjobs"),
        ("networking.k8s.io", "v1", "ingresses"),
    ] {
        assert_eq!(
            deprecation_for(g, v, r, ""),
            (false, String::new()),
            "{g}/{v} {r}"
        );
    }
    // List kinds are never a route's resource.
    assert!(lookup("apps", "v1beta1", "deploymentlists", "").is_none());
}

#[test]
fn table_is_sorted_and_complete() {
    assert!(table::TABLE.len() > 100, "{}", table::TABLE.len());
    // Every removed release is not before the deprecation release.
    for l in table::TABLE {
        if let Some(r) = l.removed {
            assert!(r >= l.deprecated, "{} {}", l.version, l.kind);
        }
    }
}

/// `ShouldServeForVersion` (deleted_kinds.go:150-172), GA build: removed after
/// current is served; equal or before is not.
#[test]
fn should_serve_for_version_ga_build() {
    assert!(should_serve_for_version((1, 36)));
    assert!(should_serve_for_version((2, 0)));
    assert!(!should_serve_for_version((1, 35)));
    assert!(!should_serve_for_version((1, 16)));
    assert!(!should_serve_for_version((0, 99)));
}

fn ev(
    current: (u32, u32),
    is_alpha: bool,
    is_alpha_zero: bool,
    strict: bool,
    one_more: bool,
) -> ResourceExpirationEvaluator {
    ResourceExpirationEvaluator {
        current_version: current,
        is_alpha,
        is_alpha_zero,
        strict_removed_handling_in_alpha: strict,
        serve_removed_apis_one_more_release: one_more,
    }
}

/// `Test_resourceExpirationEvaluator_isNotRemoved`
/// (`server/deleted_kinds_test.go:179-261`), every case.
#[test]
fn resource_expiration_evaluator_is_not_removed() {
    let cases: [(&str, ResourceExpirationEvaluator, (u32, u32), bool); 8] = [
        (
            "removed-in-curr",
            ev((1, 20), false, false, false, false),
            (1, 20),
            false,
        ),
        (
            "removed-in-curr-but-deferred",
            ev((1, 20), false, false, false, true),
            (1, 20),
            true,
        ),
        (
            "removed-in-curr-but-alpha",
            ev((1, 20), true, false, false, false),
            (1, 20),
            true,
        ),
        (
            "removed-in-curr-but-alpha-but-strict",
            ev((1, 20), true, false, true, false),
            (1, 20),
            false,
        ),
        (
            "removed-in-curr-but-alpha-but-strict-and-alpha-zero",
            ev((1, 20), true, true, true, false),
            (1, 20),
            true,
        ),
        (
            "removed-in-prev-deferral-does-not-help",
            ev((1, 21), false, false, false, true),
            (1, 20),
            false,
        ),
        (
            "removed-in-prev-major",
            ev((2, 20), false, false, false, true),
            (1, 20),
            false,
        ),
        (
            "removed-in-future",
            ev((1, 20), false, false, false, false),
            (1, 21),
            true,
        ),
    ];
    for (name, e, removed, want) in cases {
        assert_eq!(e.should_serve_for_version(removed), want, "{name}");
    }
}

/// `Test_newResourceExpirationEvaluator` (`deleted_kinds_test.go:36-77`).
#[test]
fn new_resource_expiration_evaluator_prerelease() {
    let n = |pre: &str| ResourceExpirationEvaluator::new((1, 20), pre, None, None).unwrap();
    assert_eq!(
        n("beta.0.62+a5d22854a2ac21"),
        ev((1, 20), false, false, false, false)
    );
    assert_eq!(
        n("alpha.0.62+a5d22854a2ac21"),
        ev((1, 20), true, true, false, false)
    );
    assert_eq!(
        n("alpha.1.62+a5d22854a2ac21"),
        ev((1, 20), true, false, false, false)
    );
    assert_eq!(n(""), ev((1, 20), false, false, false, false));
}

/// The two env switches (`deleted_kinds.go:100-118`), parsed with `strconv.ParseBool`.
#[test]
fn resource_expiration_evaluator_env_switches() {
    let e = ResourceExpirationEvaluator::new((1, 20), "alpha.1", Some("true"), Some("0")).unwrap();
    assert_eq!(e, ev((1, 20), true, false, true, false));
    let e = ResourceExpirationEvaluator::new((1, 20), "", Some("F"), Some("T")).unwrap();
    assert_eq!(e, ev((1, 20), false, false, false, true));
    assert!(ResourceExpirationEvaluator::new((1, 20), "", Some("yes"), None).is_err());
    assert!(ResourceExpirationEvaluator::new((1, 20), "", None, Some("")).is_err());
}

/// A still-served deprecated kind keeps its deprecation:
/// admissionregistration.k8s.io/v1alpha1 MutatingAdmissionPolicy, deprecated
/// 1.35, removed 1.38.
#[test]
fn served_deprecated_kind_is_deprecated() {
    assert_eq!(
        deprecation_for(
            "admissionregistration.k8s.io",
            "v1alpha1",
            "mutatingadmissionpolicies",
            ""
        ),
        (true, "1.38".to_string())
    );
}

/// Removed APIs (`APILifecycleRemoved` <= current) have no installed route
/// upstream, so no deprecation signal is derived for them (#3755).
#[test]
fn removed_kinds_carry_no_deprecation() {
    for (g, v, r) in [
        ("extensions", "v1beta1", "ingresses"),
        ("apps", "v1beta2", "deployments"),
        ("batch", "v1beta1", "cronjobs"),
    ] {
        assert_eq!(
            deprecation_for(g, v, r, ""),
            (false, String::new()),
            "{g}/{v} {r}"
        );
    }
}
