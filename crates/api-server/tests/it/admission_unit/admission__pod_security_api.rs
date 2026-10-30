//! Unit tests moved from `src/admission/pod_security_api.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::{self, Path};
    pub use std::collections::HashMap;
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::pod_security_api::*;

    const LATEST: Version = Version::LATEST;

    fn lv(l: Level, v: Version) -> LevelVersion {
        LevelVersion::new(l, v)
    }

    /// `TestParseVersion`.
    #[test]
    fn parse_version_cases() {
        for (s, want) in [
            ("latest", LATEST),
            ("v1.0", Version::major_minor(1, 0)),
            ("v1.1", Version::major_minor(1, 1)),
            ("v1.20", Version::major_minor(1, 20)),
            ("v1.10000", Version::major_minor(1, 10000)),
        ] {
            let (v, r) = parse_version(s);
            assert!(r.is_ok(), "{s}");
            assert_eq!(v, want, "{s}");
        }
        for s in ["foo", "", "v2.0", "v1", "1.1", "v1.01", "v1.", "v1.-1"] {
            let (v, r) = parse_version(s);
            assert!(r.is_err(), "{s:?}");
            assert_eq!(v, LATEST, "error falls back to latest");
        }
    }

    #[test]
    fn version_strings_and_older() {
        assert_eq!(Version::major_minor(1, 22).to_string(), "v1.22");
        assert_eq!(LATEST.to_string(), "latest");
        assert!(Version::major_minor(1, 20).older(&Version::major_minor(1, 21)));
        assert!(Version::major_minor(1, 20).older(&LATEST));
        assert!(!LATEST.older(&Version::major_minor(9, 9)));
    }

    /// `TestLevelVersionEquals`.
    #[test]
    fn level_version_equivalence() {
        let levels = [Level::Privileged, Level::Baseline, Level::Restricted];
        let vs = [
            LATEST,
            Version::major_minor(1, 18),
            Version::major_minor(1, 30),
        ];
        let others = [Version::major_minor(1, 16), Version::major_minor(1, 13)];
        for l in levels {
            for v in vs {
                assert!(lv(l, v).equivalent(&lv(l, v)));
            }
        }
        for l1 in levels {
            for l2 in levels {
                if l1 != l2 {
                    assert!(!lv(l1, LATEST).equivalent(&lv(l2, LATEST)));
                }
            }
        }
        for l in [Level::Baseline, Level::Restricted] {
            for v1 in vs {
                for v2 in others {
                    assert!(!lv(l, v1).equivalent(&lv(l, v2)));
                }
            }
        }
        for v1 in vs {
            for v2 in others {
                assert!(lv(Level::Privileged, v1).equivalent(&lv(Level::Privileged, v2)));
            }
        }
    }

    /// `TestPolicyEquals`.
    #[test]
    fn policy_equivalence() {
        let privileged = Policy::PRIVILEGED;
        assert!(privileged.fully_privileged());
        let mut privileged2 = privileged;
        privileged2.enforce.version = Version::major_minor(1, 20);
        assert!(privileged2.fully_privileged());
        let mut baseline = privileged;
        baseline.audit.level = Level::Baseline;
        assert!(!baseline.fully_privileged());
        assert!(privileged.equivalent(&privileged2));
        assert!(baseline.equivalent(&baseline));
        assert!(!privileged.equivalent(&baseline));
    }

    fn make_labels(kvs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut m = HashMap::from([("other-label".to_string(), "foo-bar".to_string())]);
        for (k, v) in kvs {
            m.insert(format!("{LABEL_PREFIX}{k}"), v.to_string());
        }
        m
    }

    type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, Policy, bool);

    /// `TestPolicyToEvaluate`.
    #[test]
    fn policy_to_evaluate_cases() {
        use Level::*;
        let p = lv(Privileged, LATEST);
        let v = Version::major_minor;
        let pol = |enforce, warn, audit| Policy {
            enforce,
            warn,
            audit,
        };
        let cases: Vec<Case> = vec![
            (
                "simple enforce",
                vec![("enforce", "baseline")],
                pol(lv(Baseline, LATEST), lv(Baseline, LATEST), p),
                false,
            ),
            (
                "simple warn",
                vec![("warn", "restricted")],
                pol(p, lv(Restricted, LATEST), p),
                false,
            ),
            (
                "simple audit",
                vec![("audit", "baseline")],
                pol(p, p, lv(Baseline, LATEST)),
                false,
            ),
            (
                "enforce & warn",
                vec![("enforce", "baseline"), ("warn", "restricted")],
                pol(lv(Baseline, LATEST), lv(Restricted, LATEST), p),
                false,
            ),
            (
                "enforce version",
                vec![("enforce", "baseline"), ("enforce-version", "v1.22")],
                pol(lv(Baseline, v(1, 22)), lv(Baseline, v(1, 22)), p),
                false,
            ),
            (
                "enforce version & warn-version",
                vec![
                    ("enforce", "baseline"),
                    ("enforce-version", "v1.22"),
                    ("warn-version", "latest"),
                ],
                pol(lv(Baseline, v(1, 22)), lv(Baseline, LATEST), p),
                false,
            ),
            (
                "enforce & warn-version",
                vec![("enforce", "baseline"), ("warn-version", "v1.23")],
                pol(lv(Baseline, LATEST), lv(Baseline, v(1, 23)), p),
                false,
            ),
            (
                "fully specd",
                vec![
                    ("enforce", "baseline"),
                    ("enforce-version", "v1.20"),
                    ("warn", "restricted"),
                    ("warn-version", "v1.21"),
                    ("audit", "restricted"),
                    ("audit-version", "v1.22"),
                ],
                pol(
                    lv(Baseline, v(1, 20)),
                    lv(Restricted, v(1, 21)),
                    lv(Restricted, v(1, 22)),
                ),
                false,
            ),
            (
                "enforce no warn",
                vec![("enforce", "baseline"), ("warn", "privileged")],
                pol(lv(Baseline, LATEST), p, p),
                false,
            ),
            (
                "enforce warn error",
                vec![("enforce", "baseline"), ("warn", "foo")],
                pol(lv(Baseline, LATEST), p, p),
                true,
            ),
            (
                "enforce error",
                vec![("enforce", "foo")],
                pol(lv(Restricted, LATEST), p, p),
                true,
            ),
        ];
        for (desc, kvs, expect, expect_err) in cases {
            let (actual, errs) = policy_to_evaluate(Some(&make_labels(&kvs)), Policy::PRIVILEGED);
            assert_eq!(errs.is_empty(), !expect_err, "{desc}: {errs:?}");
            assert_eq!(actual, expect, "{desc}");
        }
    }

    #[test]
    fn policy_to_evaluate_no_labels_is_defaults() {
        let (p, errs) = policy_to_evaluate(None, Policy::PRIVILEGED);
        assert_eq!(p, Policy::PRIVILEGED);
        assert!(errs.is_empty());
        assert_eq!(
            policy_to_evaluate(Some(&HashMap::new()), Policy::PRIVILEGED).0,
            Policy::PRIVILEGED
        );
    }

    #[test]
    fn policy_to_evaluate_error_text() {
        let (_, errs) = policy_to_evaluate(
            Some(&make_labels(&[("enforce", "foo")])),
            Policy::PRIVILEGED,
        );
        assert_eq!(
            errs[0].to_string(),
            r#"metadata.labels[pod-security.kubernetes.io/enforce]: Invalid value: "foo": must be one of privileged, baseline, restricted"#
        );
    }
}
