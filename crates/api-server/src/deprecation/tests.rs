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

fn ev(cur: (u32, u32)) -> ResourceExpirationEvaluator {
    ResourceExpirationEvaluator {
        current_version: cur,
        is_alpha: false,
        is_alpha_zero: false,
        strict_removed_handling_in_alpha: false,
        serve_removed_apis_one_more_release: false,
    }
}

/// `Test_resourceExpirationEvaluator_isNotRemoved` (deleted_kinds_test.go:179-290);
/// a never-removed kind has no `APILifecycleRemoved` and is always served.
#[test]
fn should_serve_for_version_table() {
    let mut deferred = ev((1, 20));
    deferred.serve_removed_apis_one_more_release = true;
    let mut alpha = ev((1, 20));
    alpha.is_alpha = true;
    let mut alpha_strict = alpha.clone();
    alpha_strict.strict_removed_handling_in_alpha = true;
    let mut alpha_strict_zero = alpha_strict.clone();
    alpha_strict_zero.is_alpha_zero = true;
    let mut prev = ev((1, 21));
    prev.serve_removed_apis_one_more_release = true;
    let mut prev_major = ev((2, 20));
    prev_major.serve_removed_apis_one_more_release = true;
    let cases = [
        ("removed-in-curr", ev((1, 20)), (1, 20), false),
        ("removed-in-curr-but-deferred", deferred, (1, 20), true),
        ("removed-in-curr-but-alpha", alpha, (1, 20), true),
        (
            "removed-in-curr-but-alpha-but-strict",
            alpha_strict,
            (1, 20),
            false,
        ),
        ("strict-and-alpha-zero", alpha_strict_zero, (1, 20), true),
        (
            "removed-in-prev-deferral-does-not-help",
            prev,
            (1, 20),
            false,
        ),
        ("removed-in-prev-major", prev_major, (1, 20), false),
        ("removed-in-future", ev((1, 20)), (1, 21), true),
    ];
    for (name, e, removed, want) in cases {
        assert_eq!(e.should_serve_for_version(removed), want, "{name}");
    }
}

/// `Test_newResourceExpirationEvaluator` (deleted_kinds_test.go:36-90).
#[test]
fn new_evaluator_parses_version() {
    let none = |_: &str| None;
    let mk = |v: &str| ResourceExpirationEvaluator::new(v, none).unwrap();
    assert_eq!(mk("v1.20.0-beta.0.62+a5d22854a2ac21"), ev((1, 20)));
    let mut a0 = ev((1, 20));
    a0.is_alpha = true;
    a0.is_alpha_zero = true;
    assert_eq!(mk("v1.20.0-alpha.0.62+a5d22854a2ac21"), a0);
    let mut a1 = ev((1, 20));
    a1.is_alpha = true;
    assert_eq!(mk("v1.20.0-alpha.1.62+a5d22854a2ac21"), a1);
    assert_eq!(mk("v1.20.1"), ev((1, 20)));
    assert_eq!(mk("1.20.1"), ev((1, 20)));
}

/// `NewResourceExpirationEvaluator` env handling (deleted_kinds.go:98-115).
#[test]
fn new_evaluator_reads_env_switches() {
    let env = |k: &str| match k {
        "KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE" => Some("true".to_string()),
        "KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA" => Some("1".to_string()),
        _ => None,
    };
    let e = ResourceExpirationEvaluator::new("v1.35.0", env).unwrap();
    assert!(e.serve_removed_apis_one_more_release && e.strict_removed_handling_in_alpha);
    assert!(e.should_serve_for_version((1, 35)));
    let bad = |_: &str| Some("maybe".to_string());
    assert!(ResourceExpirationEvaluator::new("v1.35.0", bad).is_err());
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
