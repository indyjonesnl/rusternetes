//! Unit tests moved from `src/validation/metav1.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::deletion::DeleteOptions;
    pub use rusternetes_common::types::{
        Condition, DeletionPropagation, LabelSelector, LabelSelectorRequirement, ManagedFieldsEntry,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use std::collections::HashMap;
}

mod tests {

    /// `TestGetWarningsForIP` (util/validation/ip_test.go:263-305).
    #[test]
    fn get_warnings_for_ip_matches_upstream() {
        let cluster = "spec.clusterIPs[0]";
        assert!(
            rusternetes_common::validation::metav1::get_warnings_for_ip(cluster, "192.12.2.2")
                .is_empty()
        );
        assert!(rusternetes_common::validation::metav1::get_warnings_for_ip(
            cluster,
            "2001:db8::2"
        )
        .is_empty());
        assert_eq!(
            rusternetes_common::validation::metav1::get_warnings_for_ip(cluster, "192.012.2.2"),
            vec![
                r#"spec.clusterIPs[0]: non-standard IP address "192.012.2.2" will be considered invalid in a future Kubernetes release: use "192.12.2.2""#
            ]
        );
        assert_eq!(
            rusternetes_common::validation::metav1::get_warnings_for_ip(
                cluster,
                "::ffff:192.12.2.2"
            ),
            vec![
                r#"spec.clusterIPs[0]: non-standard IP address "::ffff:192.12.2.2" will be considered invalid in a future Kubernetes release: use "192.12.2.2""#
            ]
        );
        assert_eq!(
            rusternetes_common::validation::metav1::get_warnings_for_ip(
                "spec.loadBalancerIP",
                "2001:db8:0:0::2"
            ),
            vec![
                r#"spec.loadBalancerIP: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")"#
            ]
        );
    }
    use super::__parent::*;
    use rusternetes_common::validation::metav1::*;

    /// Ported from upstream `TestIsRelaxedEnvVarName`
    /// (`staging/src/k8s.io/apimachinery/pkg/util/validation/validation_test.go:588-613`).
    #[test]
    fn relaxed_env_var_name() {
        let good = [
            "-", ":", "_", "+a", ">a", "<a", "a.", "a..", "*a", "%a", "?a", "a:a", "a_a", "aAz",
            "~a", "|a", "a0a", "a9", "/a", "a ", "#a", "0a", "0 a", "'a", "(a", "@a",
        ];
        for v in good {
            assert!(
                is_relaxed_env_var_name(v).is_empty(),
                "expected valid: {v:?}"
            );
        }
        let bad = [
            "".to_string(),
            "=".to_string(),
            "a=".to_string(),
            "1=a".to_string(),
            "a=b".to_string(),
            "#%=&&".to_string(),
            format!("{}abc", char::from(1u8)),
            format!("{}abc", char::from_u32(130).unwrap()),
            "Ç ç".to_string(),
            "Ä ä".to_string(),
            "Ñ ñ".to_string(),
            "Ø ø".to_string(),
        ];
        for v in &bad {
            assert!(
                !is_relaxed_env_var_name(v).is_empty(),
                "expected invalid: {v:?}"
            );
        }
    }
}
