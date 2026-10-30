//! Unit tests moved from `src/validation/endpoints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::endpoints::{EndpointAddress, EndpointPort, Endpoints};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use std::net::IpAddr;
    pub use std::str::FromStr;
}

mod port_tests {
    use super::__parent::*;
    use rusternetes_common::validation::endpoints::*;
    use rusternetes_common::validation::field::ErrorType;

    fn ep(subsets: serde_json::Value) -> Endpoints {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "ep", "namespace": "default"},
            "subsets": subsets
        }))
        .unwrap()
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    // Upstream validateEndpointPort: protocol empty → Required (validation.go:8346).
    // A *missing* protocol is defaulted to TCP by serde, so the empty case is
    // exercised with an explicit "" (the String unset sentinel).
    #[test]
    fn empty_protocol_is_required() {
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 80, "protocol": ""}]
        }])));
        assert!(
            has(&errs, "subsets[0].ports[0].protocol", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn tcp_protocol_passes() {
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            !errs
                .iter()
                .any(|e| e.field == "subsets[0].ports[0].protocol"),
            "{errs:?}"
        );
    }

    // Upstream validates EndpointPort.Name as a DNS-1123 label (≤63 chars),
    // NOT the 15-char IANA_SVC_NAME rule (validation.go:8341).
    #[test]
    fn long_dns_label_port_name_passes() {
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"name": "tcp-prometheus-servicemonitor", "port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            !errs.iter().any(|e| e.field == "subsets[0].ports[0].name"),
            "{errs:?}"
        );
    }

    #[test]
    fn uppercase_port_name_rejected_as_invalid_label() {
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"name": "HTTP", "port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            has(&errs, "subsets[0].ports[0].name", ErrorType::Invalid),
            "{errs:?}"
        );
    }

    /// validation.go:8233-8243: an update that leaves invalid subsets as they
    /// were is accepted; one that changes them is validated.
    #[test]
    fn unchanged_invalid_subsets_pass_an_update() {
        let mut old = ep(serde_json::json!([{"addresses": [{"ip": "127.0.0.1"}]}]));
        old.metadata.resource_version = Some("1".into());
        assert!(!validate_endpoints_create(&old).is_empty());

        let mut labelled = old.clone();
        labelled.metadata.labels = Some([("l".to_string(), "v".to_string())].into());
        let errs = validate_endpoints_update(&labelled, &old);
        assert!(errs.is_empty(), "{errs:?}");

        let mut changed = old.clone();
        changed.subsets[0].addresses.as_mut().unwrap()[0].ip = "127.0.0.2".into();
        assert!(has(
            &validate_endpoints_update(&changed, &old),
            "subsets[0].addresses[0].ip",
            ErrorType::Invalid
        ));
    }

    #[test]
    fn create_validates_object_meta() {
        let mut e = ep(serde_json::json!([]));
        e.metadata.name = "Bad_Name".into();
        assert!(validate_endpoints_create(&e)
            .iter()
            .any(|e| e.field == "metadata.name"));
    }

    /// validation.go:8291-8299.
    #[test]
    fn hostname_and_node_name_are_validated() {
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1", "hostname": "Bad.Host", "nodeName": "Bad_Node"}]
        }])));
        assert!(has(
            &errs,
            "subsets[0].addresses[0].hostname",
            ErrorType::Invalid
        ));
        assert!(has(
            &errs,
            "subsets[0].addresses[0].nodeName",
            ErrorType::Invalid
        ));
        let errs = validate_endpoints_create(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1", "hostname": "web-0", "nodeName": "node-1.example"}]
        }])));
        assert!(errs.is_empty(), "{errs:?}");
    }
}
