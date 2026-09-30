//! Regression tables ported from Kubernetes release-1.35
//! pkg/apis/networking/validation/validation_test.go:71,401,1899,2030 and
//! staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go:96,321.

use rusternetes_common::resources::{ingressclass::IngressClass, networking::NetworkPolicy};
use rusternetes_common::validation::{
    ingressclass::{validate_ingress_class, validate_ingress_class_update},
    networkpolicy::validate_network_policy,
};
use serde_json::{json, Value};

fn policy(spec: Value) -> NetworkPolicy {
    serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": {"name": "policy", "namespace": "default", "resourceVersion": "1"},
        "spec": spec
    }))
    .unwrap()
}

fn class(controller: &str, parameters: Value) -> IngressClass {
    serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
        "metadata": {"name": "class", "resourceVersion": "1"},
        "spec": {"controller": controller, "parameters": parameters}
    }))
    .unwrap()
}

#[test]
fn network_policy_metadata_is_validated() {
    for (name, namespace, field) in [
        ("bad*name", "default", "metadata.name"),
        ("policy", "bad_namespace", "metadata.namespace"),
        ("policy", "", "metadata.namespace"),
    ] {
        let mut object = policy(json!({"podSelector": {}}));
        object.metadata.name = name.into();
        object.metadata.namespace = Some(namespace.into());
        let errors = validate_network_policy(&object);
        assert!(
            errors.iter().any(|e| e.field == field),
            "{name}/{namespace}: {errors:?}"
        );
    }
}

#[test]
fn network_policy_port_table() {
    for (port, valid) in [
        (json!({}), true),
        (json!({"port": 1}), true),
        (json!({"port": 65535}), true),
        (json!({"port": "http-alt"}), true),
        (json!({"port": "http--alt"}), false),
        (json!({"port": "123"}), false),
        (json!({"port": "HTTP"}), false),
        (json!({"port": 0}), false),
        (json!({"port": 65536}), false),
        (json!({"protocol": "SCTP", "port": 80, "endPort": 90}), true),
        (json!({"protocol": "ICMP"}), false),
        (json!({"protocol": ""}), false),
        (json!({"port": 80, "endPort": 79}), false),
        (json!({"port": "http", "endPort": 90}), false),
        (json!({"endPort": 90}), false),
    ] {
        let errors = validate_network_policy(&policy(
            json!({"podSelector": {}, "ingress": [{"ports": [port]}]}),
        ));
        assert_eq!(errors.is_empty(), valid, "{port}: {errors:?}");
    }
}

#[test]
fn network_policy_legacy_cidr_and_peer_table() {
    // StrictIPCIDRValidation defaults false in pkg/features/kube_features.go:1844.
    for (peer, valid) in [
        (
            json!({"ipBlock": {"cidr": "010.000.000.001/08", "except": ["010.001.000.000/16"]}}),
            true,
        ),
        (json!({"ipBlock": {"cidr": "10.0.0.1/8"}}), true),
        (json!({"ipBlock": {"cidr": "::ffff:192.168.0.0/112"}}), true),
        (
            json!({"ipBlock": {"cidr": "10.0.0.0/8", "except": ["10.0.0.0/8"]}}),
            false,
        ),
        (
            json!({"ipBlock": {"cidr": "10.0.0.0/8", "except": ["11.0.0.0/16"]}}),
            false,
        ),
        (
            json!({"ipBlock": {"cidr": "2001:db8::/32", "except": ["2001:db8:1::/48"]}}),
            true,
        ),
        (
            json!({"ipBlock": {"cidr": "10.0.0.0/8", "except": ["2001:db8::/48"]}}),
            false,
        ),
        (json!({"ipBlock": {"cidr": ""}}), false),
        (json!({"ipBlock": {"cidr": "10.0.0.0/33"}}), false),
        (json!({"podSelector": {}, "namespaceSelector": {}}), true),
        (
            json!({"podSelector": {}, "ipBlock": {"cidr": "10.0.0.0/8"}}),
            false,
        ),
        (json!({}), false),
    ] {
        let errors = validate_network_policy(&policy(
            json!({"podSelector": {}, "egress": [{"to": [peer]}]}),
        ));
        assert_eq!(errors.is_empty(), valid, "{peer}: {errors:?}");
    }
}

#[test]
fn network_policy_types_table() {
    for (types, valid) in [
        (json!([]), true),
        (json!(["Ingress"]), true),
        (json!(["Ingress", "Egress"]), true),
        (json!(["Ingress", "Ingress"]), true),
        (json!(["Ingress", "Egress", "Ingress"]), false),
        (json!(["ingress"]), false),
    ] {
        let errors =
            validate_network_policy(&policy(json!({"podSelector": {}, "policyTypes": types})));
        assert_eq!(errors.is_empty(), valid, "{types}: {errors:?}");
    }
}

#[test]
fn ingress_class_controller_path_table() {
    for (controller, valid) in [
        ("foo.co/bar", true),
        ("foo.co/a/b", true),
        ("foo.co/a\\b", true),
        ("foo.co/a%20!$&'()*+,;=:~_-.", true),
        ("foo.co/a?b", false),
        ("foo.co/a#b", false),
        ("foo.co/a b", false),
        ("foo.co/é", false),
        ("Foo.co/bar", false),
        ("foo.co/", false),
        ("foo.co", false),
        ("", false),
    ] {
        let errors = validate_ingress_class(&class(controller, Value::Null));
        assert_eq!(errors.is_empty(), valid, "{controller}: {errors:?}");
    }
    for (length, valid) in [(243, true), (244, false)] {
        let errors = validate_ingress_class(&class(
            &format!("foo.co/{}", "a".repeat(length)),
            Value::Null,
        ));
        assert_eq!(errors.is_empty(), valid, "{errors:?}");
    }
}

#[test]
fn ingress_class_metadata_and_update_table() {
    let old = class("foo.co/bar", Value::Null);
    let mut invalid_name = old.clone();
    invalid_name.metadata.name = "bad*name".into();
    assert!(validate_ingress_class(&invalid_name)
        .iter()
        .any(|e| e.field == "metadata.name"));
    let mut namespaced = old.clone();
    namespaced.metadata.namespace = Some("default".into());
    assert!(validate_ingress_class(&namespaced)
        .iter()
        .any(|e| e.field == "metadata.namespace"));
    for (name, controller, version, valid) in [
        ("class", "foo.co/bar", "2", true),
        ("renamed", "foo.co/bar", "2", false),
        ("class", "foo.co/new", "2", false),
        ("class", "foo.co/bar", "", false),
    ] {
        let mut new = class(controller, Value::Null);
        new.metadata.name = name.into();
        new.metadata.resource_version = Some(version.into());
        let errors = validate_ingress_class_update(&new, &old);
        assert_eq!(
            errors.is_empty(),
            valid,
            "{name}/{controller}/{version}: {errors:?}"
        );
    }
}

#[test]
fn ingress_class_parameters_table() {
    for (parameters, valid) in [
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Cluster"}),
            true,
        ),
        (json!({"kind": "ConfigMap", "name": "params"}), false),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Namespace", "namespace": "ns"}),
            true,
        ),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Namespace"}),
            false,
        ),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Namespace", "namespace": "bad_ns"}),
            false,
        ),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Cluster", "namespace": ""}),
            false,
        ),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Other"}),
            false,
        ),
        (
            json!({"kind": "ConfigMap", "name": "params", "scope": "Cluster", "apiGroup": ""}),
            false,
        ),
        (
            json!({"kind": "bad/kind", "name": "params", "scope": "Cluster"}),
            false,
        ),
        (
            json!({"kind": "ConfigMap", "name": "..", "scope": "Cluster"}),
            false,
        ),
    ] {
        let errors = validate_ingress_class(&class("foo.co/bar", parameters.clone()));
        assert_eq!(errors.is_empty(), valid, "{parameters}: {errors:?}");
    }
}
