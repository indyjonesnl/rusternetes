//! Service validation, ported from upstream's table tests in
//! `pkg/apis/core/validation/validation_test.go` (release-1.35):
//! `TestValidateServiceCreate` (:16422), `TestValidateServiceUpdate` (:19185)
//! and `TestValidateLoadBalancerStatus` (:27076).
//!
//! Each case starts from `makeValidService` (:16039), applies the same tweak
//! and asserts the same number of errors. The feature gates are those of a
//! 1.35 api-server: `StrictIPCIDRValidation` and `RelaxedServiceNameValidation`
//! are alpha and off, `PreferSameTrafficDistribution` is GA. So the
//! `legacyIPs: true` create cases are ported and their strict-validation twins
//! are not, the "without feature gate" traffic-distribution cases (which
//! emulate 1.34) are not, and the update cases that only fail under strict IP
//! validation (the runner sets it, :20468) are not. Cases whose invalid value
//! the Rust types cannot hold (an unknown enum value, a port above 65535) are
//! left out too.

use rusternetes_common::resources::service::Service;
use rusternetes_common::validation::field::Path;
use rusternetes_common::validation::service::{
    validate_load_balancer_status, validate_service_create, validate_service_update,
};
use serde_json::{json, Value};

/// `makeValidService` (validation_test.go:16039-16057).
fn make_valid_service() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "valid",
            "namespace": "valid",
            "labels": {},
            "annotations": {},
            "resourceVersion": "1",
        },
        "spec": {
            "selector": {"key": "val"},
            "sessionAffinity": "None",
            "type": "ClusterIP",
            "ports": [{"name": "p", "protocol": "TCP", "port": 8675, "targetPort": 8675}],
            "internalTrafficPolicy": "Cluster",
        },
    })
}

fn decode(v: Value) -> Service {
    serde_json::from_value(v).expect("service decodes")
}

fn remove(v: &mut Value, key: &str) {
    v.as_object_mut().unwrap().remove(key);
}

fn push_port(s: &mut Value, port: Value) {
    s["spec"]["ports"].as_array_mut().unwrap().push(port);
}

fn port(name: &str, port: u16, protocol: &str, target: u16) -> Value {
    json!({"name": name, "port": port, "protocol": protocol, "targetPort": target})
}

fn node_port(name: &str, port: u16, protocol: &str, node_port: u16, target: u16) -> Value {
    json!({"name": name, "port": port, "protocol": protocol, "nodePort": node_port, "targetPort": target})
}

/// The type, policy and allocation every valid LoadBalancer case sets.
fn lb(s: &mut Value) {
    s["spec"]["type"] = json!("LoadBalancer");
    s["spec"]["externalTrafficPolicy"] = json!("Cluster");
    s["spec"]["allocateLoadBalancerNodePorts"] = json!(true);
}

fn node_port_type(s: &mut Value) {
    s["spec"]["type"] = json!("NodePort");
    s["spec"]["externalTrafficPolicy"] = json!("Cluster");
}

fn cluster_ips(s: &mut Value, primary: &str, ips: &[&str]) {
    s["spec"]["clusterIP"] = json!(primary);
    s["spec"]["clusterIPs"] = json!(ips);
}

fn families(s: &mut Value, policy: Option<&str>, fams: &[&str]) {
    if let Some(p) = policy {
        s["spec"]["ipFamilyPolicy"] = json!(p);
    }
    s["spec"]["ipFamilies"] = json!(fams);
}

const LB_SOURCE_RANGES: &str = "service.beta.kubernetes.io/load-balancer-source-ranges";

type CreateCase = (&'static str, fn(&mut Value), usize);

/// `TestValidateServiceCreate` (validation_test.go:16422-17756).
#[test]
fn validate_service_create_cases() {
    let cases: Vec<CreateCase> = vec![
        ("default", |_| {}, 0),
        ("missing namespace", |s| s["metadata"]["namespace"] = json!(""), 1),
        ("invalid namespace", |s| s["metadata"]["namespace"] = json!("-123"), 1),
        ("missing name", |s| s["metadata"]["name"] = json!(""), 1),
        ("invalid name", |s| s["metadata"]["name"] = json!("-123"), 1),
        ("too long name", |s| s["metadata"]["name"] = json!("a".repeat(64)), 1),
        ("invalid generateName", |s| s["metadata"]["generateName"] = json!("-123"), 1),
        (
            "too long generateName",
            |s| s["metadata"]["generateName"] = json!("a".repeat(64)),
            1,
        ),
        (
            "invalid label",
            |s| s["metadata"]["labels"]["NoUppercaseOrSpecialCharsLike=Equals"] = json!("bar"),
            1,
        ),
        (
            "invalid annotation",
            |s| s["metadata"]["annotations"]["NoSpecialCharsLike=Equals"] = json!("bar"),
            1,
        ),
        ("nil selector", |s| remove(&mut s["spec"], "selector"), 0),
        (
            "invalid selector",
            |s| s["spec"]["selector"]["NoSpecialCharsLike=Equals"] = json!("bar"),
            1,
        ),
        ("missing session affinity", |s| s["spec"]["sessionAffinity"] = json!(""), 1),
        ("missing type", |s| remove(&mut s["spec"], "type"), 1),
        ("missing ports", |s| remove(&mut s["spec"], "ports"), 1),
        (
            "missing ports but headless",
            |s| {
                remove(&mut s["spec"], "ports");
                cluster_ips(s, "None", &["None"]);
            },
            0,
        ),
        ("empty port[0] name", |s| s["spec"]["ports"][0]["name"] = json!(""), 0),
        (
            "empty port[1] name",
            |s| push_port(s, port("", 12345, "TCP", 12345)),
            1,
        ),
        (
            "empty multi-port port[0] name",
            |s| {
                s["spec"]["ports"][0]["name"] = json!("");
                push_port(s, port("p", 12345, "TCP", 12345));
            },
            1,
        ),
        ("invalid port name", |s| s["spec"]["ports"][0]["name"] = json!("INVALID"), 1),
        ("missing protocol", |s| s["spec"]["ports"][0]["protocol"] = json!(""), 1),
        ("invalid protocol", |s| s["spec"]["ports"][0]["protocol"] = json!("INVALID"), 1),
        ("invalid cluster ip", |s| cluster_ips(s, "invalid", &["invalid"]), 1),
        (
            "valid legacy cluster ip with legacy validation",
            |s| cluster_ips(s, "001.002.003.004", &["001.002.003.004"]),
            0,
        ),
        ("missing port", |s| s["spec"]["ports"][0]["port"] = json!(0), 1),
        (
            "invalid TargetPort int",
            |s| s["spec"]["ports"][0]["targetPort"] = json!(65536),
            1,
        ),
        (
            "valid port headless",
            |s| {
                s["spec"]["ports"][0]["port"] = json!(11722);
                s["spec"]["ports"][0]["targetPort"] = json!(11722);
                cluster_ips(s, "None", &["None"]);
            },
            0,
        ),
        (
            "invalid port headless 1",
            |s| {
                s["spec"]["ports"][0]["port"] = json!(11722);
                s["spec"]["ports"][0]["targetPort"] = json!(11721);
                cluster_ips(s, "None", &["None"]);
            },
            0,
        ),
        (
            "invalid port headless 2",
            |s| {
                s["spec"]["ports"][0]["port"] = json!(11722);
                s["spec"]["ports"][0]["targetPort"] = json!("target");
                cluster_ips(s, "None", &["None"]);
            },
            0,
        ),
        (
            "invalid externalIPs localhost",
            |s| {
                s["spec"]["externalTrafficPolicy"] = json!("Cluster");
                s["spec"]["externalIPs"] = json!(["127.0.0.1"]);
            },
            1,
        ),
        (
            "invalid externalIPs unspecified",
            |s| {
                s["spec"]["externalTrafficPolicy"] = json!("Cluster");
                s["spec"]["externalIPs"] = json!(["0.0.0.0"]);
            },
            1,
        ),
        (
            "invalid externalIPs host",
            |s| {
                s["spec"]["externalTrafficPolicy"] = json!("Cluster");
                s["spec"]["externalIPs"] = json!(["myhost.mydomain"]);
            },
            1,
        ),
        (
            "valid legacy externalIPs with legacy validation",
            |s| {
                s["spec"]["externalTrafficPolicy"] = json!("Cluster");
                s["spec"]["externalIPs"] = json!(["001.002.003.004"]);
            },
            0,
        ),
        (
            "valid externalIPs",
            |s| {
                s["spec"]["externalTrafficPolicy"] = json!("Cluster");
                s["spec"]["externalIPs"] = json!(["1.2.3.4"]);
            },
            0,
        ),
        (
            "dup port name",
            |s| push_port(s, port("p", 12345, "TCP", 12345)),
            1,
        ),
        (
            "valid load balancer protocol UDP 1",
            |s| {
                lb(s);
                s["spec"]["ports"][0]["protocol"] = json!("UDP");
            },
            0,
        ),
        (
            "valid load balancer protocol UDP 2",
            |s| {
                lb(s);
                s["spec"]["ports"][0] = port("q", 12345, "UDP", 12345);
            },
            0,
        ),
        (
            "load balancer with mix protocol",
            |s| {
                lb(s);
                push_port(s, port("q", 12345, "UDP", 12345));
            },
            0,
        ),
        ("valid 1", |_| {}, 0),
        (
            "valid 2",
            |s| {
                s["spec"]["ports"][0]["protocol"] = json!("UDP");
                s["spec"]["ports"][0]["targetPort"] = json!(12345);
            },
            0,
        ),
        ("valid 3", |s| s["spec"]["ports"][0]["targetPort"] = json!("http"), 0),
        ("valid cluster ip - none ", |s| cluster_ips(s, "None", &["None"]), 0),
        (
            "valid cluster ip - empty",
            |s| {
                remove(&mut s["spec"], "clusterIPs");
                s["spec"]["ports"][0]["targetPort"] = json!("http");
            },
            0,
        ),
        ("valid type - clusterIP", |s| s["spec"]["type"] = json!("ClusterIP"), 0),
        ("valid type - loadbalancer", lb, 0),
        (
            "valid type - loadbalancer with allocateLoadBalancerNodePorts=false",
            |s| {
                lb(s);
                s["spec"]["allocateLoadBalancerNodePorts"] = json!(false);
            },
            0,
        ),
        (
            "invalid type - missing AllocateLoadBalancerNodePorts for loadbalancer type",
            |s| {
                lb(s);
                remove(&mut s["spec"], "allocateLoadBalancerNodePorts");
            },
            1,
        ),
        (
            "valid type loadbalancer 2 ports",
            |s| {
                lb(s);
                push_port(s, port("q", 12345, "TCP", 12345));
            },
            0,
        ),
        (
            "duplicate nodeports",
            |s| {
                node_port_type(s);
                push_port(s, node_port("q", 1, "TCP", 1, 1));
                push_port(s, node_port("r", 2, "TCP", 1, 2));
            },
            1,
        ),
        (
            "duplicate nodeports (different protocols)",
            |s| {
                node_port_type(s);
                push_port(s, node_port("q", 1, "TCP", 1, 1));
                push_port(s, node_port("r", 2, "UDP", 1, 2));
                push_port(s, node_port("s", 3, "SCTP", 1, 3));
            },
            0,
        ),
        (
            "invalid duplicate ports (with same protocol)",
            |s| {
                push_port(s, port("q", 12345, "TCP", 8080));
                push_port(s, port("r", 12345, "TCP", 80));
            },
            1,
        ),
        (
            "valid duplicate ports (with different protocols)",
            |s| {
                push_port(s, port("q", 12345, "TCP", 8080));
                push_port(s, port("r", 12345, "UDP", 80));
                push_port(s, port("s", 12345, "SCTP", 8088));
            },
            0,
        ),
        ("valid type - nodeport", node_port_type, 0),
        (
            "valid type loadbalancer with NodePort",
            |s| {
                lb(s);
                push_port(s, node_port("q", 12345, "TCP", 12345, 12345));
            },
            0,
        ),
        (
            "valid type=NodePort service with NodePort",
            |s| {
                node_port_type(s);
                push_port(s, node_port("q", 12345, "TCP", 12345, 12345));
            },
            0,
        ),
        (
            "valid type=NodePort service without NodePort",
            |s| {
                node_port_type(s);
                push_port(s, port("q", 12345, "TCP", 12345));
            },
            0,
        ),
        (
            "valid cluster service without NodePort",
            |s| push_port(s, port("q", 12345, "TCP", 12345)),
            0,
        ),
        (
            "invalid cluster service with NodePort",
            |s| push_port(s, node_port("q", 12345, "TCP", 12345, 12345)),
            1,
        ),
        (
            "invalid public service with duplicate NodePort",
            |s| {
                node_port_type(s);
                push_port(s, node_port("p1", 1, "TCP", 1, 1));
                push_port(s, node_port("p2", 2, "TCP", 1, 2));
            },
            1,
        ),
        (
            "valid port type=LoadBalancer",
            |s| {
                lb(s);
                push_port(s, port("kubelet", 10250, "TCP", 12345));
            },
            0,
        ),
        (
            "valid LoadBalancer source range annotation",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("1.2.3.0/24,  5.6.0.0/16");
            },
            0,
        ),
        (
            "valid empty LoadBalancer source range annotation",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("");
            },
            0,
        ),
        (
            "valid whitespace-only LoadBalancer source range annotation",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("  ");
            },
            0,
        ),
        (
            "invalid LoadBalancer source range annotation (hostname)",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("foo.bar");
            },
            1,
        ),
        (
            "invalid LoadBalancer source range annotation (invalid CIDR)",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("1.2.3.4/33");
            },
            1,
        ),
        (
            "invalid LoadBalancer source range annotation for non LoadBalancer type service",
            |s| s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("1.2.3.0/24"),
            1,
        ),
        (
            "invalid empty-but-set LoadBalancer source range annotation for non LoadBalancer type service",
            |s| s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!(""),
            1,
        ),
        (
            "valid legacy LoadBalancer source range with legacy validation",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["001.002.003.000/24"]);
            },
            0,
        ),
        (
            "valid LoadBalancer source range",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.0/24", "5.6.0.0/16"]);
            },
            0,
        ),
        (
            "valid LoadBalancer source range with whitespace",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.0/24  ", " 5.6.0.0/16"]);
            },
            0,
        ),
        (
            "invalid empty LoadBalancer source range",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["   "]);
            },
            1,
        ),
        (
            "invalid LoadBalancer source range (hostname)",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["foo.bar"]);
            },
            1,
        ),
        (
            "invalid LoadBalancer source range (invalid CIDR)",
            |s| {
                lb(s);
                s["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.4/33"]);
            },
            1,
        ),
        (
            "invalid source range for non LoadBalancer type service",
            |s| s["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.0/24", "5.6.0.0/16"]),
            1,
        ),
        (
            "invalid source range annotation ignored with valid source range field",
            |s| {
                lb(s);
                s["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("foo.bar");
                s["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.0/24", "5.6.0.0/16"]);
            },
            0,
        ),
        (
            "valid ExternalName",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                s["spec"]["externalName"] = json!("foo.bar.example.com");
            },
            0,
        ),
        (
            "valid ExternalName (trailing dot)",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                s["spec"]["externalName"] = json!("foo.bar.example.com.");
            },
            0,
        ),
        (
            "invalid ExternalName clusterIP (valid IP)",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                cluster_ips(s, "1.2.3.4", &["1.2.3.4"]);
                s["spec"]["externalName"] = json!("foo.bar.example.com");
            },
            1,
        ),
        (
            "invalid ExternalName clusterIP (None)",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                cluster_ips(s, "None", &["None"]);
                s["spec"]["externalName"] = json!("foo.bar.example.com");
            },
            1,
        ),
        (
            "invalid ExternalName (not a DNS name)",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                s["spec"]["externalName"] = json!("-123");
            },
            1,
        ),
        (
            "LoadBalancer type cannot have None ClusterIP",
            |s| {
                cluster_ips(s, "None", &["None"]);
                lb(s);
            },
            1,
        ),
        (
            "invalid node port with clusterIP None",
            |s| {
                node_port_type(s);
                push_port(s, node_port("q", 1, "TCP", 1, 1));
                cluster_ips(s, "None", &["None"]);
            },
            1,
        ),
        (
            "nil internalTraffic field when feature gate is on",
            |s| remove(&mut s["spec"], "internalTrafficPolicy"),
            1,
        ),
        (
            "internalTrafficPolicy field nil when type is ExternalName",
            |s| {
                remove(&mut s["spec"], "internalTrafficPolicy");
                s["spec"]["type"] = json!("ExternalName");
                s["spec"]["externalName"] = json!("foo.bar.com");
            },
            0,
        ),
        (
            "internalTrafficPolicy field is set when type is ExternalName",
            |s| {
                s["spec"]["type"] = json!("ExternalName");
                s["spec"]["externalName"] = json!("foo.bar.com");
            },
            0,
        ),
        (
            "internalTrafficPolicy field set to Local",
            |s| s["spec"]["internalTrafficPolicy"] = json!("Local"),
            0,
        ),
        (
            "negative healthCheckNodePort field",
            |s| {
                lb(s);
                s["spec"]["externalTrafficPolicy"] = json!("Local");
                s["spec"]["healthCheckNodePort"] = json!(-1);
            },
            1,
        ),
        (
            "valid healthCheckNodePort field",
            |s| {
                lb(s);
                s["spec"]["externalTrafficPolicy"] = json!("Local");
                s["spec"]["healthCheckNodePort"] = json!(31100);
            },
            0,
        ),
        (
            "invalid timeoutSeconds field",
            |s| {
                s["spec"]["sessionAffinity"] = json!("ClientIP");
                s["spec"]["sessionAffinityConfig"] = json!({"clientIP": {"timeoutSeconds": -1}});
            },
            1,
        ),
        (
            "sessionAffinityConfig can't be set when session affinity is None",
            |s| {
                lb(s);
                s["spec"]["sessionAffinity"] = json!("None");
                s["spec"]["sessionAffinityConfig"] = json!({"clientIP": {"timeoutSeconds": 90}});
            },
            1,
        ),
        (
            "IPFamilyPolicy(singleStack) is set for two families",
            |s| families(s, Some("SingleStack"), &["IPv4", "IPv6"]),
            0,
        ),
        (
            "valid, IPFamilyPolicy(preferDualStack) is set for two families",
            |s| families(s, Some("PreferDualStack"), &["IPv4", "IPv6"]),
            0,
        ),
        (
            "invalid, service with 2+ ipFamilies",
            |s| families(s, Some("RequireDualStack"), &["IPv4", "IPv6", "IPv4"]),
            1,
        ),
        (
            "invalid, service with same ip families",
            |s| families(s, Some("RequireDualStack"), &["IPv6", "IPv6"]),
            1,
        ),
        ("valid, nil service ipFamilies", |s| remove(&mut s["spec"], "ipFamilies"), 0),
        ("valid, service with valid ipFamilies (v4)", |s| families(s, None, &["IPv4"]), 0),
        ("valid, service with valid ipFamilies (v6)", |s| families(s, None, &["IPv6"]), 0),
        (
            "valid, service with valid ipFamilies(v4,v6)",
            |s| families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]),
            0,
        ),
        (
            "valid, service with valid ipFamilies(v6,v4)",
            |s| families(s, Some("RequireDualStack"), &["IPv6", "IPv4"]),
            0,
        ),
        (
            "valid, service preferred dual stack with single family",
            |s| families(s, Some("PreferDualStack"), &["IPv6"]),
            0,
        ),
        ("invalid, garbage single ip", |s| cluster_ips(s, "garbage-ip", &["garbage-ip"]), 1),
        (
            "invalid, garbage ips",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "garbage-ip", &["garbage-ip", "garbage-second-ip"]);
            },
            2,
        ),
        (
            "invalid, garbage first ip",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "garbage-ip", &["garbage-ip", "2001::1"]);
            },
            1,
        ),
        (
            "invalid, garbage second ip",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "2001::1", &["2001::1", "garbage-ip"]);
            },
            1,
        ),
        (
            "invalid, NONE + IP",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "None", &["None", "2001::1"]);
            },
            1,
        ),
        (
            "invalid, IP + NONE",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "2001::1", &["2001::1", "None"]);
            },
            1,
        ),
        (
            "invalid, EMPTY STRING + IP",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "", &["", "2001::1"]);
            },
            2,
        ),
        (
            "invalid, IP + EMPTY STRING",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "2001::1", &["2001::1", ""]);
            },
            1,
        ),
        (
            "invalid, same ip family (v6)",
            |s| {
                cluster_ips(s, "2001::1", &["2001::1", "2001::4"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            2,
        ),
        (
            "invalid, same ip family (v4)",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "10.0.0.10"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            2,
        ),
        (
            "invalid, more than two ips",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "2001::1", "10.0.0.10"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            1,
        ),
        (
            " multi ip, dualstack not set (request for downgrade)",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "2001::1"]);
                families(s, Some("SingleStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "valid, headless-no-selector + multi family + gate off",
            |s| {
                cluster_ips(s, "None", &["None"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                remove(&mut s["spec"], "selector");
            },
            0,
        ),
        (
            "valid, multi ip, single ipfamilies preferDualStack",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "2001::1"]);
                families(s, Some("PreferDualStack"), &["IPv4"]);
            },
            0,
        ),
        (
            "valid, multi ip, single ipfamilies (must match when provided) + requireDualStack",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "2001::1"]);
                families(s, Some("RequireDualStack"), &["IPv4"]);
            },
            0,
        ),
        (
            "invalid, families don't match (v4=>v6)",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1"]);
                families(s, None, &["IPv6"]);
            },
            1,
        ),
        (
            "invalid, families don't match (v6=>v4)",
            |s| {
                cluster_ips(s, "2001::1", &["2001::1"]);
                families(s, None, &["IPv4"]);
            },
            1,
        ),
        (
            "valid, single ip",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("SingleStack");
                cluster_ips(s, "10.0.0.1", &["10.0.0.1"]);
            },
            0,
        ),
        (
            "valid, single family",
            |s| families(s, Some("SingleStack"), &["IPv6"]),
            0,
        ),
        (
            "valid, single ip + single family",
            |s| {
                cluster_ips(s, "2001::1", &["2001::1"]);
                families(s, Some("SingleStack"), &["IPv6"]);
            },
            0,
        ),
        (
            "valid, single ip + single family (dual stack requested)",
            |s| {
                cluster_ips(s, "2001::1", &["2001::1"]);
                families(s, Some("PreferDualStack"), &["IPv6"]);
            },
            0,
        ),
        (
            "valid, single ip, multi ipfamilies",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "valid, multi ips, multi ipfamilies (4,6)",
            |s| {
                cluster_ips(s, "10.0.0.1", &["10.0.0.1", "2001::1"]);
                families(s, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "valid, ips, multi ipfamilies (6,4)",
            |s| {
                cluster_ips(s, "2001::1", &["2001::1", "10.0.0.1"]);
                families(s, Some("RequireDualStack"), &["IPv6", "IPv4"]);
            },
            0,
        ),
        (
            "valid, multi ips (6,4)",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "2001::1", &["2001::1", "10.0.0.1"]);
            },
            0,
        ),
        (
            "valid, dual stack",
            |s| s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack"),
            0,
        ),
        (
            "valid appProtocol",
            |s| {
                s["spec"]["ports"] = json!([{"port": 12345, "targetPort": 12345,
                                              "protocol": "TCP", "appProtocol": "HTTP"}]);
            },
            0,
        ),
        (
            "valid custom appProtocol",
            |s| {
                s["spec"]["ports"] = json!([{"port": 12345, "targetPort": 12345,
                                              "protocol": "TCP",
                                              "appProtocol": "example.com/protocol"}]);
            },
            0,
        ),
        (
            "invalid appProtocol",
            |s| {
                s["spec"]["ports"] = json!([{"port": 12345, "targetPort": 12345,
                    "protocol": "TCP",
                    "appProtocol": "example.com/protocol_with{invalid}[characters]"}]);
            },
            1,
        ),
        (
            "invalid cluster ip != clusterIP in multi ip service",
            |s| {
                s["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
                cluster_ips(s, "10.0.0.10", &["10.0.0.1", "2001::1"]);
            },
            1,
        ),
        (
            "invalid cluster ip != clusterIP in single ip service",
            |s| cluster_ips(s, "10.0.0.10", &["10.0.0.1"]),
            1,
        ),
        (
            "Use AllocateLoadBalancerNodePorts when type is not LoadBalancer",
            |s| s["spec"]["allocateLoadBalancerNodePorts"] = json!(true),
            1,
        ),
        (
            "valid LoadBalancerClass when type is LoadBalancer",
            |s| {
                lb(s);
                s["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class");
            },
            0,
        ),
        (
            "invalid LoadBalancerClass",
            |s| {
                lb(s);
                s["spec"]["loadBalancerClass"] = json!("Bad/LoadBalancerClass");
            },
            1,
        ),
        (
            "invalid: set LoadBalancerClass when type is not LoadBalancer",
            |s| s["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class"),
            1,
        ),
        (
            "topology annotations are mismatched",
            |s| {
                s["metadata"]["annotations"]["service.kubernetes.io/topology-aware-hints"] =
                    json!("original");
                s["metadata"]["annotations"]["service.kubernetes.io/topology-mode"] =
                    json!("different");
            },
            1,
        ),
        (
            "valid: trafficDistribution field set to PreferClose",
            |s| s["spec"]["trafficDistribution"] = json!("PreferClose"),
            0,
        ),
        (
            "valid: trafficDistribution field set to PreferSameZone with feature gate",
            |s| s["spec"]["trafficDistribution"] = json!("PreferSameZone"),
            0,
        ),
        (
            "valid: trafficDistribution field set to PreferSameNode with feature gate",
            |s| s["spec"]["trafficDistribution"] = json!("PreferSameNode"),
            0,
        ),
        (
            "invalid: trafficDistribution field set to Random",
            |s| s["spec"]["trafficDistribution"] = json!("Random"),
            1,
        ),
        (
            "invalid: service name begins with a digit feature gate disabled",
            |s| s["metadata"]["name"] = json!("1-test-service"),
            1,
        ),
    ];

    let mut failures = Vec::new();
    for (name, tweak, num_errs) in cases {
        let mut v = make_valid_service();
        tweak(&mut v);
        let errs = validate_service_create(&decode(v));
        if errs.len() != num_errs {
            failures.push(format!(
                "{name:?}: expected {num_errs} errors, got {}: {:?}",
                errs.len(),
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

type UpdateCase = (&'static str, fn(&mut Value, &mut Value), usize);

/// `TestValidateServiceUpdate` (validation_test.go:19185-20478).
#[test]
fn validate_service_update_cases() {
    fn set_ip(s: &mut Value, ip: &str) {
        cluster_ips(s, ip, &[ip]);
    }
    fn clear_ip(s: &mut Value) {
        s["spec"]["clusterIP"] = json!("");
        remove(&mut s["spec"], "clusterIPs");
    }
    fn lb_old(s: &mut Value) {
        s["spec"]["type"] = json!("LoadBalancer");
        s["spec"]["allocateLoadBalancerNodePorts"] = json!(true);
    }

    let cases: Vec<UpdateCase> = vec![
        ("no change", |_, _| {}, 0),
        ("change name", |_, n| n["metadata"]["name"] = json!("valid2"), 1),
        ("change namespace", |_, n| n["metadata"]["namespace"] = json!("valid2"), 1),
        (
            "change label valid",
            |_, n| n["metadata"]["labels"]["key"] = json!("other-value"),
            0,
        ),
        (
            "change cluster IP",
            |o, n| {
                set_ip(o, "1.2.3.4");
                set_ip(n, "8.6.7.5");
            },
            1,
        ),
        (
            "remove cluster IP",
            |o, n| {
                set_ip(o, "1.2.3.4");
                clear_ip(n);
            },
            1,
        ),
        (
            "change affinity",
            |_, n| {
                n["spec"]["sessionAffinity"] = json!("ClientIP");
                n["spec"]["sessionAffinityConfig"] = json!({"clientIP": {"timeoutSeconds": 90}});
            },
            0,
        ),
        ("remove affinity", |_, n| n["spec"]["sessionAffinity"] = json!(""), 1),
        ("change type", |_, n| lb(n), 0),
        ("remove type", |_, n| remove(&mut n["spec"], "type"), 1),
        ("change type -> nodeport", |_, n| node_port_type(n), 0),
        (
            "add loadBalancerSourceRanges",
            |o, n| {
                lb_old(o);
                lb(n);
                n["spec"]["loadBalancerSourceRanges"] = json!(["10.0.0.0/8"]);
            },
            0,
        ),
        (
            "LoadBalancer type cannot have None ClusterIP",
            |_, n| {
                set_ip(n, "None");
                lb(n);
            },
            1,
        ),
        (
            "`None` ClusterIP can NOT be changed",
            |o, n| {
                set_ip(o, "None");
                set_ip(n, "1.2.3.4");
            },
            1,
        ),
        (
            "`None` ClusterIP can NOT be removed",
            |o, n| {
                set_ip(o, "None");
                clear_ip(n);
            },
            1,
        ),
        (
            "ClusterIP can NOT be changed to None",
            |o, n| {
                set_ip(o, "1.2.3.4");
                set_ip(n, "None");
            },
            1,
        ),
        (
            "Service with ClusterIP type can change its empty ClusterIP",
            |o, n| {
                clear_ip(o);
                set_ip(n, "1.2.3.5");
            },
            0,
        ),
        (
            "Service with ClusterIP type cannot change its set ClusterIP when changing type to NodePort",
            |o, n| {
                node_port_type(n);
                set_ip(o, "1.2.3.4");
                set_ip(n, "1.2.3.5");
            },
            1,
        ),
        (
            "Service with ClusterIP type can change its empty ClusterIP when changing type to LoadBalancer",
            |o, n| {
                lb(n);
                clear_ip(o);
                set_ip(n, "1.2.3.5");
            },
            0,
        ),
        (
            "Service with LoadBalancer type can change its AllocateLoadBalancerNodePorts from true to false",
            |o, n| {
                lb_old(o);
                lb(n);
                n["spec"]["allocateLoadBalancerNodePorts"] = json!(false);
            },
            0,
        ),
        (
            "Service with LoadBalancer type cannot change its set ClusterIP when changing type to NodePort",
            |o, n| {
                lb_old(o);
                node_port_type(n);
                set_ip(o, "1.2.3.4");
                set_ip(n, "1.2.3.5");
            },
            1,
        ),
        (
            "Service with ExternalName type can change its set ClusterIP when changing type to ClusterIP",
            |o, n| {
                o["spec"]["type"] = json!("ExternalName");
                set_ip(o, "1.2.3.4");
                set_ip(n, "1.2.3.5");
            },
            0,
        ),
        (
            "invalid node port with clusterIP None",
            |o, n| {
                o["spec"]["type"] = json!("NodePort");
                node_port_type(n);
                push_port(o, node_port("q", 1, "TCP", 1, 1));
                push_port(n, node_port("q", 1, "TCP", 1, 1));
                clear_ip(o);
                set_ip(n, "None");
            },
            1,
        ),
        (
            "convert from ExternalName",
            |o, _| o["spec"]["type"] = json!("ExternalName"),
            0,
        ),
        (
            "invalid: convert to ExternalName",
            |o, n| {
                set_ip(o, "10.0.0.10");
                families(o, Some("SingleStack"), &["IPv4"]);
                n["spec"]["type"] = json!("ExternalName");
                n["spec"]["externalName"] = json!("foo");
                set_ip(n, "10.0.0.10");
                families(n, Some("SingleStack"), &["IPv4"]);
            },
            3,
        ),
        (
            "valid: convert to ExternalName",
            |o, n| {
                set_ip(o, "10.0.0.10");
                families(o, Some("SingleStack"), &["IPv4"]);
                n["spec"]["type"] = json!("ExternalName");
                n["spec"]["externalName"] = json!("foo");
            },
            0,
        ),
        (
            "same ServiceIPFamily, change IPFamilyPolicy singleStack => requireDualStack",
            |o, n| {
                families(o, Some("SingleStack"), &["IPv4"]);
                families(n, Some("RequireDualStack"), &["IPv4"]);
            },
            0,
        ),
        (
            "add a new ServiceIPFamily",
            |o, n| {
                families(o, Some("RequireDualStack"), &["IPv4"]);
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "change primary ServiceIPFamily",
            |o, n| {
                set_ip(o, "1.2.3.4");
                families(o, None, &["IPv4"]);
                set_ip(n, "1.2.3.4");
                families(n, None, &["IPv6"]);
            },
            2,
        ),
        (
            "valid: upgrade to dual stack, with specific secondary ip",
            |o, n| {
                set_ip(o, "1.2.3.4");
                families(o, Some("SingleStack"), &["IPv4"]);
                cluster_ips(n, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "valid: downgrade from dual to single",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                set_ip(n, "1.2.3.4");
                families(n, Some("SingleStack"), &["IPv4"]);
            },
            0,
        ),
        (
            "valid: change families for a headless service",
            |o, n| {
                set_ip(o, "None");
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                set_ip(n, "None");
                families(n, Some("RequireDualStack"), &["IPv6", "IPv4"]);
            },
            0,
        ),
        (
            "invalid flip families",
            |o, n| {
                cluster_ips(o, "1.2.3.40", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                cluster_ips(n, "2001::1", &["2001::1", "1.2.3.5"]);
                families(n, Some("RequireDualStack"), &["IPv6", "IPv4"]);
            },
            4,
        ),
        (
            "invalid change first ip, in dualstack service",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                cluster_ips(n, "1.2.3.5", &["1.2.3.5", "2001::1"]);
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            1,
        ),
        (
            "invalid, change second ip in dualstack service",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                cluster_ips(n, "1.2.3.4", &["1.2.3.4", "2002::1"]);
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            1,
        ),
        (
            "downgrade keeping the families",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                set_ip(n, "1.2.3.4");
                families(n, Some("SingleStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "invalid, downgrade without changing to singleStack",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                set_ip(n, "1.2.3.4");
                families(n, Some("RequireDualStack"), &["IPv4"]);
            },
            2,
        ),
        (
            "invalid, downgrade and change primary ip",
            |o, n| {
                cluster_ips(o, "1.2.3.4", &["1.2.3.4", "2001::1"]);
                families(o, Some("RequireDualStack"), &["IPv4", "IPv6"]);
                set_ip(n, "1.2.3.5");
                families(n, Some("SingleStack"), &["IPv4"]);
            },
            1,
        ),
        (
            "invalid: upgrade to dual stack and change primary",
            |o, n| {
                set_ip(o, "1.2.3.4");
                families(o, Some("SingleStack"), &["IPv4"]);
                set_ip(n, "1.2.3.5");
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            1,
        ),
        (
            "update to invalid app protocol",
            |o, n| {
                o["spec"]["ports"] = json!([{"name": "a", "port": 443, "targetPort": 3000, "protocol": "TCP"}]);
                n["spec"]["ports"] = json!([{"name": "a", "port": 443, "targetPort": 3000,
                                              "protocol": "TCP", "appProtocol": "~https"}]);
            },
            1,
        ),
        (
            "invalid: change LoadBalancerClass when update service",
            |o, n| {
                lb_old(o);
                o["spec"]["loadBalancerClass"] = json!("test.com/test-old");
                lb(n);
                n["spec"]["loadBalancerClass"] = json!("test.com/test-new");
            },
            1,
        ),
        (
            "invalid: unset LoadBalancerClass when update service",
            |o, n| {
                lb_old(o);
                o["spec"]["loadBalancerClass"] = json!("test.com/test-old");
                lb(n);
            },
            1,
        ),
        (
            "invalid: set LoadBalancerClass when update service",
            |o, n| {
                lb_old(o);
                lb(n);
                n["spec"]["loadBalancerClass"] = json!("test.com/test-new");
            },
            1,
        ),
        (
            "update to LoadBalancer type of service with valid LoadBalancerClass",
            |_, n| {
                lb(n);
                n["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class");
            },
            0,
        ),
        (
            "invalid: set invalid LoadBalancerClass when update service to LoadBalancer",
            |_, n| {
                lb(n);
                n["spec"]["loadBalancerClass"] = json!("Bad/LoadBalancerclass");
            },
            2,
        ),
        (
            "invalid: set LoadBalancerClass when update service to non LoadBalancer type of service (ClusterIP)",
            |_, n| n["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class"),
            2,
        ),
        (
            "invalid: set LoadBalancerClass when update service to non LoadBalancer type of service (ExternalName)",
            |o, n| {
                o["spec"]["type"] = json!("ExternalName");
                n["spec"]["type"] = json!("ExternalName");
                n["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class");
            },
            3,
        ),
        (
            "invalid: set LoadBalancerClass when update from LoadBalancer service to non LoadBalancer type of service",
            |o, n| {
                lb_old(o);
                o["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class");
                node_port_type(n);
                n["spec"]["loadBalancerClass"] = json!("test.com/test-load-balancer-class");
            },
            2,
        ),
        (
            "update internalTrafficPolicy from Cluster to Local",
            |_, n| n["spec"]["internalTrafficPolicy"] = json!("Local"),
            0,
        ),
        (
            "topology annotations are mismatched",
            |_, n| {
                n["metadata"]["annotations"]["service.kubernetes.io/topology-aware-hints"] =
                    json!("original");
                n["metadata"]["annotations"]["service.kubernetes.io/topology-mode"] =
                    json!("different");
            },
            1,
        ),
        (
            "pre-existing invalid clusterIP ignored when adding clusterIPs",
            |o, n| {
                set_ip(o, "1.2.3.04");
                families(o, Some("SingleStack"), &["IPv4"]);
                cluster_ips(n, "1.2.3.04", &["1.2.3.04", "2001:db8::4"]);
                families(n, Some("RequireDualStack"), &["IPv4", "IPv6"]);
            },
            0,
        ),
        (
            "pre-existing invalid externalIP ignored when adding externalIPs",
            |o, n| {
                o["spec"]["externalTrafficPolicy"] = json!("Cluster");
                o["spec"]["externalIPs"] = json!(["1.2.3.04"]);
                n["spec"]["externalTrafficPolicy"] = json!("Cluster");
                n["spec"]["externalIPs"] = json!(["5.6.7.8", "1.2.3.04"]);
            },
            0,
        ),
        (
            "pre-existing invalid loadBalancerSourceRanges ignored when adding source ranges",
            |o, n| {
                lb_old(o);
                o["spec"]["loadBalancerSourceRanges"] = json!(["010.0.0.0/8"]);
                lb(n);
                n["spec"]["loadBalancerSourceRanges"] = json!(["1.2.3.0/24", "010.0.0.0/8"]);
            },
            0,
        ),
        (
            "can fix invalid source ranges annotation",
            |o, n| {
                lb_old(o);
                o["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("010.0.0.0/8");
                lb(n);
                n["metadata"]["annotations"][LB_SOURCE_RANGES] = json!("10.0.0.0/8");
            },
            0,
        ),
    ];

    let mut failures = Vec::new();
    for (name, tweak, num_errs) in cases {
        let (mut o, mut n) = (make_valid_service(), make_valid_service());
        tweak(&mut o, &mut n);
        let errs = validate_service_update(&decode(n), &decode(o));
        if errs.len() != num_errs {
            failures.push(format!(
                "{name:?}: expected {num_errs} errors, got {}: {:?}",
                errs.len(),
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// `TestValidateLoadBalancerStatus` (validation_test.go:27076-27231): the
/// status against a LoadBalancer spec unless the case says otherwise.
#[test]
fn validate_load_balancer_status_cases() {
    let cases: Vec<(&str, &str, Value, Value, usize)> = vec![
        (
            "type is not LB",
            "ClusterIP",
            json!([]),
            json!([{"ip": "1.2.3.4"}]),
            1,
        ),
        (
            "valid vip ipMode",
            "LoadBalancer",
            json!([]),
            json!([{"ip": "1.2.3.4", "ipMode": "VIP"}]),
            0,
        ),
        (
            "valid proxy ipMode",
            "LoadBalancer",
            json!([]),
            json!([{"ip": "1.2.3.4", "ipMode": "Proxy"}]),
            0,
        ),
        (
            "invalid ipMode",
            "LoadBalancer",
            json!([]),
            json!([{"ip": "1.2.3.4", "ipMode": "dummy"}]),
            1,
        ),
        (
            "missing ipMode",
            "LoadBalancer",
            json!([]),
            json!([{"ip": "1.2.3.4"}]),
            1,
        ),
        (
            "missing ip with ipMode present",
            "LoadBalancer",
            json!([]),
            json!([{"ipMode": "Proxy"}]),
            1,
        ),
        (
            "legacy IP with legacy validation",
            "LoadBalancer",
            json!([]),
            json!([{"ip": "001.002.003.004", "ipMode": "VIP"}]),
            0,
        ),
        (
            "invalid ingress IP ignored when adding IP",
            "LoadBalancer",
            json!([{"ip": "1.2.3.04", "ipMode": "VIP"}]),
            json!([{"ip": "1.2.3.04", "ipMode": "VIP"}, {"ip": "5.6.7.8", "ipMode": "VIP"}]),
            0,
        ),
        (
            "invalid ingress IP can be fixed",
            "LoadBalancer",
            json!([{"ip": "1.2.3.04", "ipMode": "VIP"}]),
            json!([{"ip": "1.2.3.4", "ipMode": "VIP"}]),
            0,
        ),
    ];

    let mut failures = Vec::new();
    for (name, svc_type, old_ingress, ingress, num_errs) in cases {
        let mut v = make_valid_service();
        v["spec"]["type"] = json!(svc_type);
        let svc = decode(v);
        let old = serde_json::from_value(json!({"ingress": old_ingress})).unwrap();
        let status = serde_json::from_value(json!({"ingress": ingress})).unwrap();
        let errs =
            validate_load_balancer_status(Some(&status), Some(&old), &Path::new("status"), &svc);
        if errs.len() != num_errs {
            failures.push(format!(
                "{name:?}: expected {num_errs} errors, got {}: {:?}",
                errs.len(),
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// `IPFamilyPolicy` and `InternalTrafficPolicy` are `*T` in
/// `staging/src/k8s.io/api/core/v1/types.go:6121,6153` (release-1.35), so a
/// non-nil `""` is distinct from nil and upstream rejects it:
/// `validation.go:9011-9015` (`ipFamilyPolicy`) and `:6886-6888`
/// (`internalTrafficPolicy`) both answer `field.NotSupported`. `type` (:5998)
/// and `externalTrafficPolicy` (:6056) are plain strings that
/// `SetDefaults_Service` (defaults.go:121-123,135-139) defaults from `""`, so
/// they keep decoding `""` as unset.
#[test]
fn pointer_enum_empty_and_unknown_values_are_not_supported() {
    let families = r#""SingleStack", "PreferDualStack", "RequireDualStack""#;
    let traffic = r#""Cluster", "Local""#;
    for (field, value, valid) in [
        ("ipFamilyPolicy", "", families),
        ("ipFamilyPolicy", "Bogus", families),
        ("internalTrafficPolicy", "", traffic),
        ("internalTrafficPolicy", "Bogus", traffic),
    ] {
        let mut v = make_valid_service();
        v["spec"][field] = json!(value);
        let svc = decode(v);
        let errs: Vec<String> = validate_service_create(&svc)
            .iter()
            .map(|e| e.to_string())
            .collect();
        let want =
            format!("spec.{field}: Unsupported value: \"{value}\": supported values: {valid}");
        assert!(
            errs.contains(&want),
            "{field}={value:?}: want {want:?}, got {errs:?}"
        );
    }
}

#[test]
fn plain_string_enums_still_decode_empty_as_unset() {
    let mut v = make_valid_service();
    v["spec"]["type"] = json!("");
    v["spec"]["externalTrafficPolicy"] = json!("");
    let svc = decode(v);
    assert!(svc.spec.service_type.is_none());
    assert!(svc.spec.external_traffic_policy.is_none());
}
