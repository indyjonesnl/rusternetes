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
