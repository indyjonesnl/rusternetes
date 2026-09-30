//! Kubernetes release-1.35 networking validation regressions.
//! Upstream pkg/apis/networking/validation/validation_test.go:611,989,1240,2220.

use rusternetes_common::resources::ingress::Ingress;
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::ingress::{
    validate_ingress, validate_ingress_status_update, validate_ingress_update,
};
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

// Upstream validation_test.go:1240 TestValidateIngressUpdate, including the
// compatibility cases at :1552-1639. Invalidity in one old entry opens the
// corresponding gate for all new entries, not merely the unchanged value.
#[test]
fn ingress_update_secret_compatibility_does_not_skip_tls_hosts() {
    for (old_secret, new_secret, host, expected) in [
        (
            "valid",
            "bad secret",
            "example.com",
            Some("spec.tls[0].secretName"),
        ),
        ("old bad", "new bad", "example.com", None),
        (
            "old bad",
            "new bad",
            "Bad_Host",
            Some("spec.tls[0].hosts[0]"),
        ),
    ] {
        let mut old = base();
        old["spec"]["tls"] = json!([{"secretName":old_secret}]);
        let mut new = base();
        new["spec"]["tls"] = json!([{"secretName":new_secret,"hosts":[host]}]);
        let errors = validate_ingress_update(&decode(new), &decode(old));
        assert_eq!(errors.len(), usize::from(expected.is_some()), "{errors:?}");
        if let Some(field) = expected {
            assert_eq!(errors[0].field, field);
        }
    }
}

fn rule(host: &str, path: &str) -> Value {
    json!({"host":host,"http":{"paths":[{"pathType":"Exact","path":path,"backend":{"service":{"name":"backend","port":{"number":80}}}}]}})
}

#[test]
fn ingress_update_wildcard_compatibility_is_scoped_to_rule_values() {
    for (old_host, old_path, new_host, new_path, expected) in [
        ("*.old.com", "bad", "*.new.com", "also-bad", None),
        (
            "*.old.com",
            "/valid",
            "*.new.com",
            "bad",
            Some("spec.rules[0].http.paths[0].path"),
        ),
        (
            "plain.com",
            "bad",
            "*.new.com",
            "bad",
            Some("spec.rules[0].http.paths[0].path"),
        ),
        (
            "*.old.com",
            "bad",
            "plain.com",
            "bad",
            Some("spec.rules[0].http.paths[0].path"),
        ),
        (
            "*.old.com",
            "bad",
            "bad.*.com",
            "bad",
            Some("spec.rules[0].host"),
        ),
    ] {
        let mut old = base();
        old["spec"]["rules"] = json!([rule(old_host, old_path)]);
        let mut new = base();
        new["spec"]["rules"] = json!([rule(new_host, new_path)]);
        let errors = validate_ingress_update(&decode(new), &decode(old));
        assert_eq!(errors.len(), usize::from(expected.is_some()), "{errors:?}");
        if let Some(field) = expected {
            assert_eq!(errors[0].field, field);
        }
    }
}

#[test]
fn ingress_update_relaxes_service_names_only_for_old_dns1123_names() {
    for (old_name, expected_valid) in [("1old", true), ("old", false), ("bad_name", false)] {
        for use_rule in [false, true] {
            let mut old = base();
            if use_rule {
                old["spec"]["rules"] = json!([rule("example.com", "/")]);
                old["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"] =
                    json!(old_name);
            } else {
                old["spec"]["defaultBackend"]["service"]["name"] = json!(old_name);
            }
            let mut new = base();
            new["spec"]["defaultBackend"]["service"]["name"] = json!("2new");
            let errors = validate_ingress_update(&decode(new), &decode(old));
            assert_eq!(
                errors.is_empty(),
                expected_valid,
                "{old_name}, rule={use_rule}: {errors:?}"
            );
        }
    }
}

#[test]
fn ingress_update_does_not_enforce_create_only_class_annotation_equality() {
    let old = decode(base());
    let mut new = base();
    new["spec"]["ingressClassName"] = json!("nginx");
    new["metadata"]["annotations"] = json!({"kubernetes.io/ingress.class":"other"});
    assert!(validate_ingress_update(&decode(new), &old).is_empty());
}

#[test]
fn ingress_update_and_status_update_enforce_metadata_immutability() {
    let old = decode(base());
    for (field, replacement) in [
        ("name", json!("other")),
        ("namespace", json!("other")),
        ("resourceVersion", json!("")),
    ] {
        let mut new = base();
        new["metadata"][field] = replacement;
        let new = decode(new);
        for errors in [
            validate_ingress_update(&new, &old),
            validate_ingress_status_update(&new, &old),
        ] {
            assert!(
                errors
                    .iter()
                    .any(|e| e.field == format!("metadata.{field}")),
                "{errors:?}"
            );
        }
    }
}

// Upstream validation_test.go:2220 TestValidateIngressStatusUpdate and
// validation.go:389-416. StrictIPCIDRValidation defaults false in 1.35.
#[test]
fn ingress_status_legacy_ips_and_unvalidated_ports_follow_upstream() {
    for ip in [
        "001.002.003.004",
        "00000001.2.3.4",
        "::ffff:1.2.3.4",
        "::ffff:001.002.003.004",
        "FE80:0:0:0:0:0:0:abc",
        "",
    ] {
        let mut new = base();
        new["spec"] = Value::Null; // Status validation never validates spec.
        new["status"] = json!({"loadBalancer":{"ingress":[{"ip":ip,"ports":[{"port":-1,"protocol":"invalid","error":"anything"}]}]}});
        let errors = validate_ingress_status_update(&decode(new), &decode(base()));
        assert!(errors.is_empty(), "{ip}: {errors:?}");
    }
}

#[test]
fn ingress_status_bad_ips_are_grandfathered_by_exact_old_value() {
    for (old_ip, new_ip, expected_valid) in [
        ("bad", "bad", true),
        ("bad", "different", false),
        ("", "bad", false),
    ] {
        let mut old = base();
        old["status"] = json!({"loadBalancer":{"ingress":[{"ip":old_ip}]}});
        let mut new = base();
        new["status"] = json!({"loadBalancer":{"ingress":[{"ip":new_ip}]}});
        let errors = validate_ingress_status_update(&decode(new), &decode(old));
        assert_eq!(errors.is_empty(), expected_valid, "{errors:?}");
        if !expected_valid {
            assert_eq!(errors[0].field, "status.loadBalancer.ingress[0].ip");
            assert_eq!(errors[0].origin, "format=ip-sloppy");
        }
    }
}

#[test]
fn ingress_status_hostnames_are_dns_names_and_never_grandfathered() {
    for (hostname, valid) in [
        ("example.com", true),
        ("", true),
        ("Bad_Host", false),
        ("127.0.0.1", false),
        ("001.002.003.004", false),
        ("*.example.com", false),
    ] {
        let mut value = base();
        value["status"] = json!({"loadBalancer":{"ingress":[{"hostname":hostname}]}});
        let value = decode(value);
        let errors = validate_ingress_status_update(&value, &value);
        assert_eq!(errors.is_empty(), valid, "{hostname}: {errors:?}");
        assert!(errors
            .iter()
            .all(|e| e.field == "status.loadBalancer.ingress[0].hostname"));
    }
}

#[test]
fn ingress_explicit_empty_path_type_is_unsupported() {
    let mut value = base();
    value["spec"]["rules"] = json!([{"http":{"paths":[{"pathType":"","backend":{"service":{"name":"backend","port":{"number":80}}}}]}}]);
    let errors = validate_ingress(&decode(value));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].error_type, ErrorType::NotSupported);
    assert_eq!(
        errors[0].detail,
        "supported values: \"Exact\", \"ImplementationSpecific\", \"Prefix\""
    );
}
