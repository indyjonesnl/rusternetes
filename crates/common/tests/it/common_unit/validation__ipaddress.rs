//! Unit tests moved from `src/validation/ipaddress.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::ipaddress::{IPAddress, ParentReference};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_subdomain;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_ip, name_is_path_segment, validate_immutable_field, validate_object_meta,
        validate_object_meta_update,
    };
}

mod cidr_warning_tests {
    use super::__parent::*;
    use rusternetes_common::validation::ipaddress::*;

    /// `TestGetWarningsForCIDR` (apimachinery/pkg/util/validation/ip_test.go:530-605),
    /// less the leading-zero IPv4 cases the Rust parser rejects outright.
    #[test]
    fn warnings_match_upstream() {
        let path = Path::new("spec").child("loadBalancerSourceRanges").index(0);
        let cases: [(&str, &[&str]); 6] = [
            ("192.12.2.0/24", &[]),
            ("2001:db8::/64", &[]),
            (
                "192.12.2.0/024",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: non-standard CIDR value "192.12.2.0/024" will be considered invalid in a future Kubernetes release: use "192.12.2.0/24""#,
                ],
            ),
            (
                "::ffff:192.12.2.0/120",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: non-standard CIDR value "::ffff:192.12.2.0/120" will be considered invalid in a future Kubernetes release: use "192.12.2.0/24""#,
                ],
            ),
            (
                "192.12.2.8/24",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: CIDR value "192.12.2.8/24" is ambiguous in this context (should be "192.12.2.0/24" or "192.12.2.8/32"?)"#,
                ],
            ),
            (
                "2001:db8:0:0::/64",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: IPv6 CIDR value "2001:db8:0:0::/64" should be in RFC 5952 canonical format ("2001:db8::/64")"#,
                ],
            ),
        ];
        for (cidr, want) in cases {
            assert_eq!(get_warnings_for_cidr(&path, cidr), want, "{cidr}");
        }
    }
}
