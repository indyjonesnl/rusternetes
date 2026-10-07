//! The Pod Security Admission `api` package: levels, versions and the
//! namespace-label -> policy resolution.
//!
//! Port of `staging/src/k8s.io/pod-security-admission/api/{constants,helpers}.go`
//! (release-1.35); the tests port `api/helpers_test.go`.

// A faithful port of the `api` package: `Older`, `Equivalent` and friends are
// ported ahead of their callers (versioned checks, namespace validation).
#![allow(dead_code)]

use rusternetes_common::validation::field::{self, Path};
use std::collections::HashMap;
use std::fmt;

/// `api.Level` (constants.go:19-25).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    Privileged,
    Baseline,
    Restricted,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Level::Privileged => "privileged",
            Level::Baseline => "baseline",
            Level::Restricted => "restricted",
        })
    }
}

/// `api.Version` (helpers.go:30-34).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub latest: bool,
}

impl Version {
    pub const LATEST: Version = Version {
        major: 0,
        minor: 0,
        latest: true,
    };

    /// `api.MajorMinorVersion`.
    pub fn major_minor(major: u32, minor: u32) -> Self {
        Self {
            major,
            minor,
            latest: false,
        }
    }

    /// `Version.Older` (helpers.go:43-53): latest is always newer.
    pub fn older(&self, other: &Version) -> bool {
        if self.latest {
            return false;
        }
        if other.latest {
            return true;
        }
        if self.major != other.major {
            return self.major < other.major;
        }
        self.minor < other.minor
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.latest {
            f.write_str("latest")
        } else {
            write!(f, "v{}.{}", self.major, self.minor)
        }
    }
}

/// `ParseLevel` (helpers.go:100-107): on error the level is `restricted`.
pub fn parse_level(level: &str) -> (Level, Result<(), String>) {
    match level {
        "privileged" => (Level::Privileged, Ok(())),
        "baseline" => (Level::Baseline, Ok(())),
        "restricted" => (Level::Restricted, Ok(())),
        _ => (
            Level::Restricted,
            Err("must be one of privileged, baseline, restricted".to_string()),
        ),
    }
}

/// `ParseVersion` (helpers.go:124-137), `^v1\.([0-9]|[1-9][0-9]*)$`: on
/// error the version is `latest`.
pub fn parse_version(version: &str) -> (Version, Result<(), String>) {
    let bad = || {
        (
            Version::LATEST,
            Err(r#"must be "latest" or "v1.x""#.to_string()),
        )
    };
    if version == "latest" {
        return (Version::LATEST, Ok(()));
    }
    let Some(minor) = version.strip_prefix("v1.") else {
        return bad();
    };
    let valid = minor == "0"
        || (!minor.is_empty()
            && !minor.starts_with('0')
            && minor.bytes().all(|b| b.is_ascii_digit()));
    match (valid, minor.parse::<u32>()) {
        (true, Ok(m)) => (Version::major_minor(1, m), Ok(())),
        _ => bad(),
    }
}

/// `api.LevelVersion` (helpers.go:139-142).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LevelVersion {
    pub level: Level,
    pub version: Version,
}

impl LevelVersion {
    pub const fn new(level: Level, version: Version) -> Self {
        Self { level, version }
    }

    /// `LevelVersion.Equivalent` (helpers.go:150-153).
    pub fn equivalent(&self, other: &LevelVersion) -> bool {
        (self.level == Level::Privileged && other.level == Level::Privileged) || self == other
    }
}

impl fmt::Display for LevelVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.level, self.version)
    }
}

/// `api.Policy` (helpers.go:155-159).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub enforce: LevelVersion,
    pub audit: LevelVersion,
    pub warn: LevelVersion,
}

impl Policy {
    /// The plugin's default policy, privileged:latest in all three modes
    /// (admission/api/v1/defaults.go `SetDefaults_PodSecurityDefaults`).
    pub const PRIVILEGED: Policy = {
        let lv = LevelVersion::new(Level::Privileged, Version::LATEST);
        Policy {
            enforce: lv,
            audit: lv,
            warn: lv,
        }
    };

    /// `Policy.Equivalent` (helpers.go:170-172).
    pub fn equivalent(&self, other: &Policy) -> bool {
        self.enforce.equivalent(&other.enforce)
            && self.audit.equivalent(&other.audit)
            && self.warn.equivalent(&other.warn)
    }

    /// `Policy.FullyPrivileged` (helpers.go:175-179).
    pub fn fully_privileged(&self) -> bool {
        self.enforce.level == Level::Privileged
            && self.audit.level == Level::Privileged
            && self.warn.level == Level::Privileged
    }
}

pub const LABEL_PREFIX: &str = "pod-security.kubernetes.io/";
pub const ENFORCE_LEVEL_LABEL: &str = "pod-security.kubernetes.io/enforce";
pub const ENFORCE_VERSION_LABEL: &str = "pod-security.kubernetes.io/enforce-version";
pub const AUDIT_LEVEL_LABEL: &str = "pod-security.kubernetes.io/audit";
pub const AUDIT_VERSION_LABEL: &str = "pod-security.kubernetes.io/audit-version";
pub const WARN_LEVEL_LABEL: &str = "pod-security.kubernetes.io/warn";
pub const WARN_VERSION_LABEL: &str = "pod-security.kubernetes.io/warn-version";

/// `CompareLevels` (helpers.go:240-258): -1 / 0 / 1 by strictness.
pub fn compare_levels(a: Level, b: Level) -> i32 {
    match a.cmp(&b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// `PolicyToEvaluate` (helpers.go:182-238): the namespace's policy,
/// falling back to `defaults` for an unspecified label. A valid policy is
/// always returned, even with errors; an unparseable enforce level is
/// `restricted` (fail closed), an unparseable audit/warn level `privileged`
/// (fail open), an unparseable version `latest`.
pub fn policy_to_evaluate(
    labels: Option<&HashMap<String, String>>,
    defaults: Policy,
) -> (Policy, field::ErrorList) {
    let mut p = defaults;
    let mut errs = field::ErrorList::new();
    let Some(labels) = labels.filter(|l| !l.is_empty()) else {
        return (p, errs);
    };
    let mut push = |label: &str, value: &str, r: Result<(), String>| {
        if let Err(e) = r {
            errs.push(field::Error::invalid(
                &Path::new("metadata").child("labels").key(label),
                value.to_string(),
                e,
            ));
        }
    };

    let (mut has_enforce_level, mut has_warn_level, mut has_warn_version) = (false, false, false);
    if let Some(v) = labels.get(ENFORCE_LEVEL_LABEL) {
        let (l, r) = parse_level(v);
        p.enforce.level = l;
        has_enforce_level = r.is_ok(); // Don't default warn in case of error
        push(ENFORCE_LEVEL_LABEL, v, r);
    }
    if let Some(v) = labels.get(ENFORCE_VERSION_LABEL) {
        let (ver, r) = parse_version(v);
        p.enforce.version = ver;
        push(ENFORCE_VERSION_LABEL, v, r);
    }
    if let Some(v) = labels.get(AUDIT_LEVEL_LABEL) {
        let (l, r) = parse_level(v);
        p.audit.level = if r.is_err() { Level::Privileged } else { l }; // Fail open for audit.
        push(AUDIT_LEVEL_LABEL, v, r);
    }
    if let Some(v) = labels.get(AUDIT_VERSION_LABEL) {
        let (ver, r) = parse_version(v);
        p.audit.version = ver;
        push(AUDIT_VERSION_LABEL, v, r);
    }
    if let Some(v) = labels.get(WARN_LEVEL_LABEL) {
        has_warn_level = true;
        let (l, r) = parse_level(v);
        p.warn.level = if r.is_err() { Level::Privileged } else { l }; // Fail open for warn.
        push(WARN_LEVEL_LABEL, v, r);
    }
    if let Some(v) = labels.get(WARN_VERSION_LABEL) {
        has_warn_version = true;
        let (ver, r) = parse_version(v);
        p.warn.version = ver;
        push(WARN_VERSION_LABEL, v, r);
    }

    // Default warn to the enforce level when explicitly set to a more restrictive level.
    if !has_warn_level && has_enforce_level && compare_levels(p.enforce.level, p.warn.level) > 0 {
        p.warn.level = p.enforce.level;
        if !has_warn_version {
            p.warn.version = p.enforce.version;
        }
    }
    (p, errs)
}

#[cfg(test)]
mod tests {
    use super::*;

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
