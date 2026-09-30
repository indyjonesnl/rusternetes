//! Regression tables ported from Kubernetes release-1.35
//! pkg/apis/networking/validation/validation_test.go:71,401,1899,2030 and
//! staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go:96,321.

use rusternetes_common::resources::{ingressclass::IngressClass, networking::NetworkPolicy};
use rusternetes_common::validation::{
    ingressclass::{validate_ingress_class, validate_ingress_class_update},
    networkpolicy::{validate_network_policy, validate_network_policy_update},
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

#[test]
fn network_policy_update_selector_compatibility() {
    // validation.go:195: only invalid old spec.podSelector enables compatibility,
    // which then applies to expression values in ALL selectors, never their keys.
    let invalid_selector =
        json!({"matchExpressions": [{"key": "app", "operator": "In", "values": ["bad$value"]}]});
    let valid_old = policy(json!({"podSelector": {}}));
    let invalid_old = policy(json!({"podSelector": invalid_selector}));
    let invalid_new = policy(json!({
        "podSelector": invalid_selector,
        "ingress": [{"from": [{"namespaceSelector": invalid_selector}]}]
    }));
    assert!(validate_network_policy_update(&invalid_new, &invalid_old).is_empty());
    assert!(!validate_network_policy_update(&invalid_new, &valid_old).is_empty());
    assert!(!validate_network_policy(&invalid_new).is_empty());

    let old_peer_only = policy(
        json!({"podSelector": {}, "ingress": [{"from": [{"podSelector": invalid_selector}]}]}),
    );
    assert!(!validate_network_policy_update(&old_peer_only, &old_peer_only).is_empty());
    let bad_key = policy(
        json!({"podSelector": {"matchExpressions": [{"key": "bad$key", "operator": "In", "values": ["bad$value"]}]}}),
    );
    assert!(!validate_network_policy_update(&bad_key, &invalid_old).is_empty());
    let bad_match_label = policy(json!({"podSelector": {"matchLabels": {"app": "bad$value"}}}));
    assert!(!validate_network_policy_update(&bad_match_label, &invalid_old).is_empty());
}

#[test]
fn network_policy_update_metadata_and_combined_passes() {
    let old = policy(json!({"podSelector": {}}));
    let changed_spec = policy(json!({"podSelector": {"matchLabels": {"app": "changed"}}}));
    assert!(validate_network_policy_update(&changed_spec, &old).is_empty());
    for field in ["name", "namespace", "resourceVersion"] {
        let mut new = old.clone();
        match field {
            "name" => new.metadata.name = "renamed".into(),
            "namespace" => new.metadata.namespace = Some("another".into()),
            _ => new.metadata.resource_version = None,
        }
        assert!(validate_network_policy_update(&new, &old)
            .iter()
            .any(|e| e.field == format!("metadata.{field}")));
    }
    let invalid_port = policy(json!({"podSelector": {}, "ingress": [{"ports": [{"port": 0}]}]}));
    let errors = validate_network_policy_update(&invalid_port, &old);
    assert_eq!(
        errors
            .iter()
            .filter(|e| e.field == "spec.ingress[0].ports[0].port")
            .count(),
        2
    );

    // strategy.go:86-90 runs create validation before update's old-CIDR allowlist.
    let invalid_cidr = policy(
        json!({"podSelector": {}, "ingress": [{"from": [{"ipBlock": {"cidr": "unparseable"}}]}]}),
    );
    let errors = validate_network_policy_update(&invalid_cidr, &invalid_cidr);
    assert_eq!(
        errors
            .iter()
            .filter(|e| e.field == "spec.ingress[0].from[0].ipBlock.cidr")
            .count(),
        1
    );
}

#[test]
fn network_policy_mapped_cidr_mask_semantics() {
    // ValidateIPBlock compares original Mask.Size values after net.IPNet.Contains
    // normalizes mapped IPv6 addresses. IPv4 /8 and mapped /104 have equal address
    // ranges but different mask sizes, so this legacy combination is accepted.
    for (cidr, except, valid) in [
        ("10.0.0.0/8", "::ffff:10.0.0.0/104", true),
        ("::ffff:10.0.0.0/104", "10.1.0.0/16", false),
        ("::ffff:010.0.0.0/104", "::ffff:10.1.0.0/112", true),
        ("10.0.0.0/+8", "10.1.0.0/16", false),
    ] {
        let object = policy(
            json!({"podSelector": {}, "ingress": [{"from": [{"ipBlock": {"cidr": cidr, "except": [except]}}]}]}),
        );
        let errors = validate_network_policy(&object);
        assert_eq!(
            errors.is_empty(),
            valid,
            "{cidr} except {except}: {errors:?}"
        );
    }
}

#[test]
fn network_policy_port_error_details_match_upstream() {
    for (name, expected) in [
        ("http--alt", vec!["must not contain consecutive hyphens"]),
        ("123", vec!["must contain at least one letter (a-z)"]),
        (
            "-HTTP-",
            vec![
                "must contain only alpha-numeric characters (a-z, 0-9), and hyphens (-)",
                "must contain at least one letter (a-z)",
                "must not begin or end with a hyphen",
            ],
        ),
    ] {
        let object = policy(json!({"podSelector": {}, "ingress": [{"ports": [{"port": name}]}]}));
        let errors = validate_network_policy(&object);
        assert_eq!(
            errors.iter().map(|e| e.detail.as_str()).collect::<Vec<_>>(),
            expected,
            "{name}: {errors:?}"
        );
    }
}
