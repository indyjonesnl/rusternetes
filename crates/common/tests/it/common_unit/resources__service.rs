//! Unit tests moved from `src/resources/service.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::resources::serde_helpers::empty_string_as_none;
    pub use rusternetes_common::types::{Condition, ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::service::*;

    #[test]
    fn test_service_port_with_int_target_port() {
        let port = ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(IntOrString::Int(8080)),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        };

        let json = serde_json::to_string(&port).unwrap();
        assert!(json.contains("\"targetPort\":8080"));

        let deserialized: ServicePort = serde_json::from_str(&json).unwrap();
        match deserialized.target_port {
            Some(IntOrString::Int(p)) => assert_eq!(p, 8080),
            _ => panic!("Expected IntOrString::Int"),
        }
    }

    #[test]
    fn test_service_port_with_string_target_port() {
        let port = ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(IntOrString::String("http-server".to_string())),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        };

        let json = serde_json::to_string(&port).unwrap();
        assert!(json.contains("\"targetPort\":\"http-server\""));

        let deserialized: ServicePort = serde_json::from_str(&json).unwrap();
        match deserialized.target_port {
            Some(IntOrString::String(ref s)) => assert_eq!(s, "http-server"),
            _ => panic!("Expected IntOrString::String"),
        }
    }

    #[test]
    fn test_service_port_deserialization_from_kubernetes() {
        // Kubernetes sends targetPort as either number or string
        let json_int = r#"{"port": 80, "targetPort": 8080, "protocol": "TCP"}"#;
        let port: ServicePort = serde_json::from_str(json_int).unwrap();
        assert!(matches!(port.target_port, Some(IntOrString::Int(8080))));

        let json_str = r#"{"port": 80, "targetPort": "http", "protocol": "TCP"}"#;
        let port: ServicePort = serde_json::from_str(json_str).unwrap();
        match port.target_port {
            Some(IntOrString::String(ref s)) => assert_eq!(s, "http"),
            _ => panic!("Expected IntOrString::String for named port"),
        }
    }

    #[test]
    fn test_service_with_session_affinity_config() {
        let svc = Service::new(
            "test-svc",
            ServiceSpec {
                selector: Some(HashMap::new()),
                ports: vec![],
                service_type: None,
                cluster_ip: None,
                external_ips: None,
                session_affinity: Some("ClientIP".to_string()),
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: Some(SessionAffinityConfig {
                    client_ip: Some(ClientIPConfig {
                        timeout_seconds: Some(10800),
                    }),
                }),
                traffic_distribution: None,
            },
        );

        let json = serde_json::to_string(&svc).unwrap();
        assert!(json.contains("\"timeoutSeconds\":10800"));
    }

    #[test]
    fn test_service_spec_default() {
        // ServiceSpec should have a Default impl
        let spec = ServiceSpec::default();
        assert!(spec.selector.is_none());
        assert!(spec.ports.is_empty());
        assert!(spec.service_type.is_none());
        assert!(spec.cluster_ip.is_none());
    }

    #[test]
    fn test_service_deserialize_without_selector() {
        // A Service JSON without selector should deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"},
            "spec": {"ports": []}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
    }

    #[test]
    fn test_service_deserialize_without_spec() {
        // A Service JSON without spec should deserialize (uses Default)
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
        assert!(svc.spec.ports.is_empty());
    }

    #[test]
    fn test_service_selector_empty_serialization() {
        // Empty selector should be serialized as {} (K8s clients require the field present)
        let spec = ServiceSpec {
            selector: Some(HashMap::new()),
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"selector\":{}"));
    }

    #[test]
    fn test_service_selector_some_serialization() {
        // When selector has values, it should be included
        let mut sel = HashMap::new();
        sel.insert("app".to_string(), "web".to_string());
        let spec = ServiceSpec {
            selector: Some(sel),
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"selector\""));
        assert!(json.contains("\"app\":\"web\""));
    }

    #[test]
    fn test_service_selector_null_deserialization() {
        // A Service with "selector": null should deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"},
            "spec": {"selector": null, "ports": []}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
    }

    /// Regression: upstream e2e (and client-go in general) serialize unset
    /// enum-typed Option<_> fields as the empty string instead of omitting
    /// them. Without empty_string_as_none, strict serde rejects "" with
    /// `unknown variant ''` and the api-server returns 422 before any
    /// handler runs, breaking `[sig-node] Pods should contain environment
    /// variables for services` and every other test that builds a Service
    /// via framework helpers.
    #[test]
    fn service_spec_accepts_empty_enum_strings() {
        let json = r#"{
            "type": "",
            "ipFamilyPolicy": "",
            "internalTrafficPolicy": "",
            "externalTrafficPolicy": ""
        }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert!(spec.service_type.is_none());
        // Pointer fields upstream (*IPFamilyPolicy, *ServiceInternalTrafficPolicy):
        // a non-nil "" is kept and validation rejects it (#2531).
        assert_eq!(
            spec.ip_family_policy,
            Some(IPFamilyPolicy::Unknown(String::new()))
        );
        assert_eq!(
            spec.internal_traffic_policy,
            Some(ServiceInternalTrafficPolicy::Unknown(String::new()))
        );
        assert!(spec.external_traffic_policy.is_none());
    }

    #[test]
    fn service_spec_still_accepts_named_enum_variants() {
        let json = r#"{
            "type": "ClusterIP",
            "ipFamilyPolicy": "SingleStack",
            "internalTrafficPolicy": "Cluster",
            "externalTrafficPolicy": "Local"
        }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.service_type, Some(ServiceType::ClusterIP));
        assert_eq!(spec.ip_family_policy, Some(IPFamilyPolicy::SingleStack));
        assert_eq!(
            spec.internal_traffic_policy,
            Some(ServiceInternalTrafficPolicy::Cluster)
        );
        assert_eq!(
            spec.external_traffic_policy,
            Some(ServiceExternalTrafficPolicy::Local)
        );
    }

    #[test]
    fn service_spec_keeps_unknown_enum_strings_for_validation() {
        // Upstream's fields are plain strings, so decode succeeds and
        // validation answers 422 Unsupported value (#2469).
        let json = r#"{ "externalTrafficPolicy": "NotARealPolicy" }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert_eq!(
            spec.external_traffic_policy,
            Some(ServiceExternalTrafficPolicy::Unknown(
                "NotARealPolicy".to_string()
            ))
        );
    }

    /// The e2e payload that triggered the bug, captured verbatim from the
    /// `[sig-node] Pods should contain environment variables for services`
    /// failure on node-1.
    #[test]
    fn service_e2e_envvars_payload_deserializes() {
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "fooservice"},
            "spec": {
                "selector": {"name": "server-envvars"},
                "ports": [{"port": 8765, "targetPort": 8080}],
                "type": "",
                "internalTrafficPolicy": "",
                "externalTrafficPolicy": "",
                "ipFamilyPolicy": ""
            }
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.service_type.is_none());
        assert!(svc.spec.external_traffic_policy.is_none());
        // `*IPFamilyPolicy` / `*ServiceInternalTrafficPolicy` upstream: Go
        // clients omit a nil pointer (generated.pb.go:13193,13245 skip nil),
        // so a literal "" is a real value that validation rejects (#2531).
        assert_eq!(
            svc.spec.internal_traffic_policy,
            Some(ServiceInternalTrafficPolicy::Unknown(String::new()))
        );
        assert_eq!(
            svc.spec.ip_family_policy,
            Some(IPFamilyPolicy::Unknown(String::new()))
        );
    }
}
