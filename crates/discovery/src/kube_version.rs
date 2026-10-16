//! Kube-aware version ordering.
//!
//! Ported from `staging/src/k8s.io/apimachinery/pkg/version/helpers.go`
//! (`versionType` :27-34, `kubeVersionRegex` :36, `parseKubeVersion` :38-62,
//! `CompareKubeAwareVersionStrings` :64-88) and
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/
//! customresource_discovery_controller.go` `sortGroupDiscoveryByKubeAwareVersion`
//! (:284-288).

use std::cmp::Ordering;

/// `versionType`: the bigger, the higher the priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum VersionType {
    Alpha,
    Beta,
    Ga,
}

/// `parseKubeVersion`: `^v([\d]+)(?:(alpha|beta)([\d]+))?$`, as
/// `(major, type, minor)`; the minor of a GA version is 0.
fn parse_kube_version(v: &str) -> Option<(i64, VersionType, i64)> {
    let rest = v.strip_prefix('v')?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let major_end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    let (major, tail) = rest.split_at(major_end);
    if !digits(major) {
        return None;
    }
    let major = major.parse().ok()?;
    if tail.is_empty() {
        return Some((major, VersionType::Ga, 0));
    }
    let (vtype, minor) = match (tail.strip_prefix("alpha"), tail.strip_prefix("beta")) {
        (Some(m), _) => (VersionType::Alpha, m),
        (_, Some(m)) => (VersionType::Beta, m),
        _ => return None,
    };
    if !digits(minor) {
        return None;
    }
    Some((major, vtype, minor.parse().ok()?))
}

/// `CompareKubeAwareVersionStrings`: negative, zero or positive as `v1` sorts
/// below, equal to or above `v2` (GA > beta > alpha, then major, then minor;
/// kube-like versions beat the rest, which compare in reverse alphabetical
/// order, exactly as upstream's `strings.Compare(v2, v1)`).
pub fn compare_kube_aware_version_strings(v1: &str, v2: &str) -> i64 {
    if v1 == v2 {
        return 0;
    }
    match (parse_kube_version(v1), parse_kube_version(v2)) {
        (None, None) => match v2.cmp(v1) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some((major1, type1, minor1)), Some((major2, type2, minor2))) => {
            if type1 != type2 {
                return type1 as i64 - type2 as i64;
            }
            if major1 != major2 {
                return major1 - major2;
            }
            minor1 - minor2
        }
    }
}

/// `sortGroupDiscoveryByKubeAwareVersion`: highest version first, so the first
/// item is the group's preferred version. `sort.Slice` is not stable but the
/// comparison is total over distinct names, so the result is the same.
pub fn sort_by_kube_aware_version_desc<T>(items: &mut [T], version: impl Fn(&T) -> &str) {
    items.sort_by(|a, b| compare_kube_aware_version_strings(version(b), version(a)).cmp(&0));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// apimachinery/pkg/version/helpers_test.go:25-55
    /// `TestCompareKubeAwareVersionStrings`.
    #[test]
    fn compare_kube_aware_version_strings_orders_like_upstream() {
        for (v1, v2, expected_greater) in [
            ("v1", "v2", false),
            ("v2", "v1", true),
            ("v10", "v2", true),
            ("v1", "v2alpha1", true),
            ("v1", "v2beta1", true),
            ("v1alpha2", "v1alpha1", true),
            ("v1beta1", "v2alpha3", true),
            ("v1alpha10", "v1alpha2", true),
            ("v1beta10", "v1beta2", true),
            ("foo", "v1beta2", false),
            ("bar", "foo", true),
            // Non kube-like versions are sorted alphabetically
            ("version1", "version2", true),
            ("version1", "version10", true),
        ] {
            assert_eq!(
                compare_kube_aware_version_strings(v1, v2) > 0,
                expected_greater,
                "{v1} vs {v2}"
            );
        }
        assert_eq!(compare_kube_aware_version_strings("v1", "v1"), 0);
    }

    /// helpers_test.go:57-123 `Test_parseKubeVersion`.
    #[test]
    fn parse_kube_version_matches_upstream_cases() {
        use VersionType::*;
        assert_eq!(parse_kube_version("v1.1"), None);
        assert_eq!(parse_kube_version("v1alpha1.1"), None);
        assert_eq!(parse_kube_version("v1alpha1"), Some((1, Alpha, 1)));
        assert_eq!(parse_kube_version("v2beta10"), Some((2, Beta, 10)));
        assert_eq!(parse_kube_version("v3"), Some((3, Ga, 0)));
    }

    /// customresource_discovery_controller.go:284-288
    /// `sortGroupDiscoveryByKubeAwareVersion`: highest first, so the first
    /// item is the group's preferred version.
    #[test]
    fn sort_kube_aware_puts_the_preferred_version_first() {
        let mut v = vec!["v1beta1", "v2alpha1", "v1", "v10", "foo", "v2"];
        sort_by_kube_aware_version_desc(&mut v, |s| s);
        assert_eq!(v, vec!["v10", "v2", "v1", "v1beta1", "v2alpha1", "foo"]);
    }
}
