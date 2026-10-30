//! Unit tests moved from `src/validation/ingress.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::resources::ingress::{
        HTTPIngressPath, Ingress, IngressBackend, IngressLoadBalancerStatus, IngressRule,
        IngressSpec,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1035_label, is_dns1123_label, is_dns1123_subdomain,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, name_is_path_segment, validate_object_meta,
        validate_object_meta_update,
    };
    pub use std::net::{IpAddr, Ipv4Addr};
}

mod tls_tests {
    use super::__parent::*;
    use rusternetes_common::validation::ingress::*;

    fn tls_errs(json: serde_json::Value) -> Vec<String> {
        let spec: IngressSpec = serde_json::from_value(json).unwrap();
        validate_ingress_tls(&spec, &Path::new("spec").child("tls"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn base(tls: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "defaultBackend": {"service": {"name": "s", "port": {"number": 80}}},
            "tls": tls
        })
    }

    #[test]
    fn valid_tls_passes() {
        assert!(tls_errs(base(serde_json::json!([
            {"hosts": ["example.com", "*.example.com"], "secretName": "my-tls"}
        ])))
        .is_empty());
    }

    #[test]
    fn invalid_host_rejected() {
        let e = tls_errs(base(serde_json::json!([{"hosts": ["NotADNSName"]}])));
        assert!(e.iter().any(|m| m.contains("hosts")), "{e:?}");
    }

    #[test]
    fn invalid_wildcard_host_rejected() {
        // bare "*" is not a valid wildcard subdomain (needs "*.")
        let e = tls_errs(base(serde_json::json!([{"hosts": ["*"]}])));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn invalid_secret_name_rejected() {
        let e = tls_errs(base(
            serde_json::json!([{"hosts": ["example.com"], "secretName": "Bad_Name"}]),
        ));
        assert!(e.iter().any(|m| m.contains("secretName")), "{e:?}");
    }
}

mod spec_tests {
    use super::__parent::*;
    use rusternetes_common::validation::ingress::*;

    fn spec_errs(json: serde_json::Value) -> Vec<String> {
        let spec: IngressSpec = serde_json::from_value(json).unwrap();
        validate_ingress_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// A spec with a single rule whose HTTP path uses the given pathType/path.
    fn rule_with_path(path_type: &str, path: &str) -> serde_json::Value {
        serde_json::json!({
            "rules": [{
                "http": {
                    "paths": [{
                        "path": path,
                        "pathType": path_type,
                        "backend": {"service": {"name": "s", "port": {"number": 80}}}
                    }]
                }
            }]
        })
    }

    #[test]
    fn valid_prefix_path_passes() {
        assert!(
            spec_errs(rule_with_path("Prefix", "/foo/bar")).is_empty(),
            "{:?}",
            spec_errs(rule_with_path("Prefix", "/foo/bar"))
        );
    }

    #[test]
    fn double_slash_sequence_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo//bar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '//'")),
            "{e:?}"
        );
    }

    #[test]
    fn dot_dot_sequence_rejected() {
        let e = spec_errs(rule_with_path("Exact", "/foo/../bar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '/../'")),
            "{e:?}"
        );
    }

    #[test]
    fn percent_encoded_slash_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo%2Fbar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '%2F'")),
            "{e:?}"
        );
        let e2 = spec_errs(rule_with_path("Prefix", "/foo%2fbar"));
        assert!(
            e2.iter().any(|m| m.contains("must not contain '%2f'")),
            "{e2:?}"
        );
    }

    #[test]
    fn dot_dot_suffix_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo/.."));
        assert!(
            e.iter().any(|m| m.contains("cannot end with '/..'")),
            "{e:?}"
        );
    }

    #[test]
    fn dot_suffix_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo/."));
        assert!(
            e.iter().any(|m| m.contains("cannot end with '/.'")),
            "{e:?}"
        );
    }

    #[test]
    fn implementation_specific_skips_sequence_checks() {
        // ImplementationSpecific only checks the absolute-path prefix, not the
        // invalid-sequence/suffix rules.
        assert!(
            spec_errs(rule_with_path("ImplementationSpecific", "/foo//bar")).is_empty(),
            "{:?}",
            spec_errs(rule_with_path("ImplementationSpecific", "/foo//bar"))
        );
    }

    fn rule_with_host(host: &str) -> serde_json::Value {
        serde_json::json!({
            "rules": [{
                "host": host,
                "http": {
                    "paths": [{
                        "path": "/",
                        "pathType": "Prefix",
                        "backend": {"service": {"name": "s", "port": {"number": 80}}}
                    }]
                }
            }]
        })
    }

    #[test]
    fn wildcard_host_accepted() {
        assert!(
            spec_errs(rule_with_host("*.example.com")).is_empty(),
            "{:?}",
            spec_errs(rule_with_host("*.example.com"))
        );
    }

    #[test]
    fn bad_wildcard_host_rejected() {
        // bare "*" is not a valid wildcard subdomain (needs "*.")
        let e = spec_errs(rule_with_host("*"));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn wildcard_not_at_start_rejected() {
        // "foo.*.example.com" does not match the wildcard format.
        let e = spec_errs(rule_with_host("foo.*.example.com"));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn plain_host_still_validated() {
        let e = spec_errs(rule_with_host("Not_A_DNS_Name"));
        assert!(e.iter().any(|m| m.contains("host")), "{e:?}");
    }

    fn spec_with_class(class_name: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "ingressClassName": class_name,
            "defaultBackend": {"service": {"name": "s", "port": {"number": 80}}}
        })
    }

    #[test]
    fn valid_ingress_class_name_passes() {
        assert!(
            spec_errs(spec_with_class(serde_json::json!("nginx"))).is_empty(),
            "{:?}",
            spec_errs(spec_with_class(serde_json::json!("nginx")))
        );
    }

    #[test]
    fn invalid_ingress_class_name_rejected() {
        let e = spec_errs(spec_with_class(serde_json::json!("Bad_Class")));
        assert!(e.iter().any(|m| m.contains("ingressClassName")), "{e:?}");
    }
}
