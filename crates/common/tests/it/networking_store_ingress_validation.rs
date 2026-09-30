//! Kubernetes release-1.35 networking validation regressions.
//! Upstream pkg/apis/networking/validation/validation_test.go:611,989,1240,2220.

use rusternetes_common::resources::ingress::Ingress;
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::ingress::validate_ingress;
use serde_json::{json, Value};

fn base() -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
        "metadata": {"name": "ingress", "namespace": "default", "resourceVersion": "1"},
        "spec": {"defaultBackend": {"service": {"name": "backend", "port": {"number": 80}}}}
    })
}

fn decode(value: Value) -> Ingress {
    serde_json::from_value(value).unwrap()
}

#[test]
fn ingress_create_metadata_and_absent_spec_follow_upstream() {
    for (pointer, value, field) in [
        ("/metadata/name", json!("Invalid_Name"), "metadata.name"),
        ("/metadata/namespace", json!(""), "metadata.namespace"),
        ("/spec", Value::Null, "spec"),
    ] {
        let mut value_under_test = base();
        *value_under_test.pointer_mut(pointer).unwrap() = value;
        let errors = validate_ingress(&decode(value_under_test));
        assert!(
            errors.iter().any(|error| error.field == field),
            "{pointer}: {errors:?}"
        );
    }
}

#[test]
fn ingress_create_class_annotation_must_match_field() {
    let mut value = base();
    value["metadata"]["annotations"] = json!({"kubernetes.io/ingress.class": "other"});
    value["spec"]["ingressClassName"] = json!("nginx");
    let errors = validate_ingress(&decode(value));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].field, "annotations.kubernetes.io/ingress.class");
    assert_eq!(
        errors[0].detail,
        "must match `ingressClassName` when both are specified"
    );
}

#[test]
fn ingress_backend_error_types_and_paths_follow_upstream() {
    for (backend, kind, field, detail) in [
        (
            json!({}),
            ErrorType::Invalid,
            "spec.defaultBackend",
            "resource or service backend is required",
        ),
        (
            json!({"service":{"name":"backend"}}),
            ErrorType::Required,
            "spec.defaultBackend",
            "port name or number is required",
        ),
        (
            json!({"service":{"name":"backend","port":{"number":0}}}),
            ErrorType::Required,
            "spec.defaultBackend",
            "port name or number is required",
        ),
        (
            json!({"service":{"name":"backend","port":{"name":"http","number":80}}}),
            ErrorType::Invalid,
            "spec.defaultBackend",
            "cannot set both port name & port number",
        ),
    ] {
        let mut value = base();
        value["spec"]["defaultBackend"] = backend;
        let errors = validate_ingress(&decode(value));
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].error_type, kind);
        assert_eq!(errors[0].field, field);
        assert_eq!(errors[0].detail, detail);
    }
}

#[test]
fn ingress_backend_names_and_references_follow_upstream() {
    for (backend, field) in [
        (
            json!({"service":{"name":"1backend","port":{"number":80}}}),
            "spec.defaultBackend.service.name",
        ),
        (
            json!({"resource":{"apiGroup":"","kind":"Bucket","name":"bucket"}}),
            "spec.defaultBackend.resource.apiGroup",
        ),
        (
            json!({"resource":{"kind":"bad/kind","name":"bucket"}}),
            "spec.defaultBackend.resource.kind",
        ),
        (
            json!({"resource":{"kind":"Bucket","name":".."}}),
            "spec.defaultBackend.resource.name",
        ),
        (
            json!({"service":{"name":"backend","port":{"name":"a--b"}}}),
            "spec.defaultBackend.service.port.name",
        ),
    ] {
        let mut value = base();
        value["spec"]["defaultBackend"] = backend;
        let errors = validate_ingress(&decode(value));
        assert!(
            errors.iter().any(|error| error.field == field),
            "{field}: {errors:?}"
        );
    }
}

#[test]
fn ingress_missing_path_type_returns_before_validating_backend() {
    let mut value = base();
    value["spec"] = json!({"rules":[{"http":{"paths":[{}]}}]});
    let errors = validate_ingress(&decode(value));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].error_type, ErrorType::Required);
    assert_eq!(errors[0].field, "spec.rules[0].http.paths[0].pathType");
}

#[test]
fn ingress_rule_host_rejects_sloppy_ip_addresses() {
    let mut value = base();
    value["spec"]["rules"] = json!([{"host":"001.002.003.004"}]);
    let errors = validate_ingress(&decode(value));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].field, "spec.rules[0].host");
    assert_eq!(errors[0].detail, "must be a DNS name, not an IP address");
}

#[test]
fn ingress_valid_edge_cases_remain_accepted() {
    for spec in [
        json!({"rules":[{}]}),
        json!({"defaultBackend":{"resource":{"kind":"Bucket","name":"UPPER_name"}}}),
        json!({"defaultBackend":{"service":{"name":"backend","port":{"name":"http","number":0}}}}),
        json!({"defaultBackend":{"service":{"name":"backend","port":{"number":80}}},"tls":[{"secretName":""}]}),
        json!({"rules":[{"http":{"paths":[{"pathType":"ImplementationSpecific","path":"","backend":{"service":{"name":"backend","port":{"number":80}}}}]}}]}),
    ] {
        let mut value = base();
        value["spec"] = spec;
        let errors = validate_ingress(&decode(value));
        assert!(errors.is_empty(), "{errors:?}");
    }
}
