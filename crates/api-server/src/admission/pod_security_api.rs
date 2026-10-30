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
