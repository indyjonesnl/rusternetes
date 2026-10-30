//! Unit tests moved from `src/validation/endpointslice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::endpointslice::{EndpointPort, EndpointSlice};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
    pub use std::net::IpAddr;
    pub use std::str::FromStr;
}

mod size_cap_tests {
    use super::__parent::*;
    use rusternetes_common::validation::endpointslice::*;

    fn slice_with(endpoints: usize, addrs_per: usize, ports: usize) -> EndpointSlice {
        let eps: Vec<_> = (0..endpoints)
            .map(|_| serde_json::json!({"addresses": vec!["10.0.0.1"; addrs_per.max(1)]}))
            .collect();
        let ps: Vec<_> = (0..ports)
            .map(|_| serde_json::json!({"port": 80}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "es"},
            "addressType": "IPv4",
            "endpoints": eps,
            "ports": ps
        }))
        .unwrap()
    }

    #[test]
    fn within_caps_passes() {
        let s = slice_with(2, 1, 2);
        assert!(validate_endpoint_slice(&s)
            .iter()
            .all(|e| !e.to_string().contains("at most")));
    }

    #[test]
    fn too_many_endpoints_rejected() {
        let s = slice_with(MAX_ENDPOINTS + 1, 1, 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field == "endpoints" && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }

    #[test]
    fn too_many_addresses_rejected() {
        let s = slice_with(1, MAX_ADDRESSES + 1, 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field.contains("addresses") && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }

    #[test]
    fn too_many_ports_rejected() {
        let s = slice_with(1, 1, MAX_PORTS + 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field == "ports" && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }
}

mod port_tests {
    use super::__parent::*;
    use rusternetes_common::validation::endpointslice::*;
    use rusternetes_common::validation::field::ErrorType;

    fn es(ports: serde_json::Value) -> EndpointSlice {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {"name": "es", "namespace": "default"},
            "addressType": "IPv4",
            "endpoints": [{"addresses": ["10.0.0.1"]}],
            "ports": ports
        }))
        .unwrap()
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    // Upstream: protocol nil → Required (discovery validation.go:193-194). A
    // *missing* protocol is defaulted to TCP by serde, so the empty case is
    // exercised with an explicit "" (the String unset sentinel).
    #[test]
    fn empty_protocol_is_required() {
        let errs = validate_endpoint_slice(&es(
            serde_json::json!([{"name": "a", "port": 80, "protocol": ""}]),
        ));
        assert!(
            has(&errs, "ports[0].protocol", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn tcp_protocol_passes() {
        let errs = validate_endpoint_slice(&es(
            serde_json::json!([{"name": "a", "port": 80, "protocol": "TCP"}]),
        ));
        assert!(
            !errs.iter().any(|e| e.field == "ports[0].protocol"),
            "{errs:?}"
        );
    }

    // Upstream: port name is a DNS-1123 label, not the 15-char IANA_SVC_NAME
    // rule (discovery validation.go:183-185).
    #[test]
    fn long_dns_label_port_name_passes() {
        let errs = validate_endpoint_slice(&es(serde_json::json!([{
            "name": "tcp-prometheus-servicemonitor", "port": 80, "protocol": "TCP"
        }])));
        assert!(!errs.iter().any(|e| e.field == "ports[0].name"), "{errs:?}");
    }

    #[test]
    fn uppercase_port_name_rejected_as_invalid_label() {
        let errs = validate_endpoint_slice(&es(
            serde_json::json!([{"name": "HTTP", "port": 80, "protocol": "TCP"}]),
        ));
        assert!(has(&errs, "ports[0].name", ErrorType::Invalid), "{errs:?}");
    }

    // Upstream: duplicate port names collide on the second occurrence
    // (discovery validation.go:187-191).
    #[test]
    fn duplicate_named_ports_rejected() {
        let errs = validate_endpoint_slice(&es(serde_json::json!([
            {"name": "http", "port": 80, "protocol": "TCP"},
            {"name": "http", "port": 81, "protocol": "TCP"}
        ])));
        assert!(
            has(&errs, "ports[1].name", ErrorType::Duplicate),
            "{errs:?}"
        );
    }

    // Upstream dereferences the (defaulted-empty) name unconditionally, so two
    // unnamed ports also collide.
    #[test]
    fn duplicate_unnamed_ports_rejected() {
        let errs = validate_endpoint_slice(&es(serde_json::json!([
            {"port": 80, "protocol": "TCP"},
            {"port": 81, "protocol": "TCP"}
        ])));
        assert!(
            has(&errs, "ports[1].name", ErrorType::Duplicate),
            "{errs:?}"
        );
    }

    #[test]
    fn distinct_named_ports_pass() {
        let errs = validate_endpoint_slice(&es(serde_json::json!([
            {"name": "http", "port": 80, "protocol": "TCP"},
            {"name": "https", "port": 443, "protocol": "TCP"}
        ])));
        assert!(
            !errs.iter().any(|e| e.error_type == ErrorType::Duplicate),
            "{errs:?}"
        );
    }
}
