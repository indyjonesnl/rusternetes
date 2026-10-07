//! Three smaller tables of pkg/registry/core/service/storage/storage_test.go
//! that sit beside `TestCreateInitIPFields` / `TestUpdateIPsFrom*Stack`:
//!
//! - `TestCreateInvalidClusterIPInputs` (:5977): the validation message each
//!   malformed `ipFamilyPolicy` / `ipFamilies` / `clusterIPs` create returns;
//! - `TestCreateDeleteReuse` (:6108): a deleted Service's IPs and NodePorts
//!   are free again;
//! - `TestUpdatePatchAllocatedValues` (:6889): an update that omits the
//!   allocated clusterIPs / nodePorts / healthCheckNodePort keeps them, and
//!   one that changes or steals them is rejected.
//!
//! Deviation: upstream's allocator ranges are `10.0.0.0/16` and `2000::/108`
//! (storage_test.go:70-81); `10.0.0.1` / `2000::1` / `10.0.0.2` are remapped
//! into `10.96.0.0/12` / `2001:db8:1::/112`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";

/// Upstream's `field.NotSupported` text. Our closed enums reject an unknown
/// `ipFamilyPolicy` / `ipFamilies` value at decode (400) before validation
/// can say this (#2469), so for those cases only the rejection is asserted.
const UNSUPPORTED: &str = "Unsupported value";

fn remap(ip: &str) -> &str {
    match ip {
        "10.0.0.1" => "10.96.0.10",
        "10.0.0.2" => "10.96.0.11",
        "2000::1" => "2001:db8:1::10",
        other => other,
    }
}

fn cluster(families: &[&str]) -> TestApiServer {
    let range = families
        .iter()
        .map(|f| match *f {
            "IPv4" => "10.96.0.0/12",
            _ => "2001:db8:1::/112",
        })
        .collect::<Vec<_>>()
        .join(",");
    TestApiServer::builder()
        .service_cluster_ip_range(&range)
        .build()
}

/// `svctest.MakeService(name, SetTypeClusterIP|NodePort|LoadBalancer, ...)`
/// (pkg/api/service/testing/make.go:34): one port 93 -> 76, selector k=v.
fn make(name: &str, type_: &str) -> Value {
    let mut spec = json!({
        "type": type_,
        "sessionAffinity": "None",
        "selector": {"k": "v"},
        "ports": [{"port": 93, "targetPort": 76, "protocol": "TCP"}],
    });
    if type_ != "ClusterIP" {
        spec["externalTrafficPolicy"] = json!("Cluster");
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    })
}

fn set_cluster_ips(svc: &mut Value, ips: &[&str]) {
    let ips: Vec<&str> = ips.iter().map(|ip| remap(ip)).collect();
    svc["spec"]["clusterIP"] = json!(ips[0]);
    svc["spec"]["clusterIPs"] = json!(ips);
}

#[tokio::test]
async fn create_invalid_cluster_ip_inputs_report_upstream_errors() {
    let v4 = ["IPv4"];
    let v46 = ["IPv4", "IPv6"];
    // (name, cluster families, ipFamilyPolicy, ipFamilies, clusterIPs, expect)
    #[allow(clippy::type_complexity)]
    let cases: Vec<(
        &str,
        &[&str],
        Option<&str>,
        Option<Vec<&str>>,
        Option<Vec<&str>>,
        Vec<&str>,
    )> = vec![
        (
            "bad_ipFamilyPolicy",
            &v4,
            Some("garbage"),
            None,
            None,
            vec![UNSUPPORTED],
        ),
        (
            "requiredual_ipFamilyPolicy_on_singlestack",
            &v4,
            Some("RequireDualStack"),
            None,
            None,
            vec!["cluster is not configured for dual-stack"],
        ),
        (
            "bad_ipFamilies_0_value",
            &v4,
            None,
            Some(vec!["garbage"]),
            None,
            vec![UNSUPPORTED],
        ),
        (
            "bad_ipFamilies_1_value",
            &v4,
            None,
            Some(vec!["IPv4", "garbage"]),
            None,
            vec![UNSUPPORTED],
        ),
        (
            "bad_ipFamilies_2_value",
            &v46,
            None,
            Some(vec!["IPv4", "IPv6", "garbage"]),
            None,
            vec![UNSUPPORTED],
        ),
        (
            "wrong_ipFamily",
            &v4,
            None,
            Some(vec!["IPv6"]),
            None,
            vec!["not configured on this cluster"],
        ),
        (
            "too_many_ipFamilies_on_singlestack",
            &v4,
            None,
            Some(vec!["IPv4", "IPv6"]),
            None,
            vec!["when multiple IP families are specified"],
        ),
        (
            "dup_ipFamily_singlestack",
            &v4,
            None,
            Some(vec!["IPv4", "IPv4"]),
            None,
            vec!["Duplicate value"],
        ),
        (
            "dup_ipFamily_dualstack",
            &v46,
            None,
            Some(vec!["IPv4", "IPv6", "IPv6"]),
            None,
            vec!["Duplicate value"],
        ),
        (
            "bad_IP",
            &v4,
            None,
            None,
            Some(vec!["garbage"]),
            vec!["must be a valid IP"],
        ),
        (
            "IP_wrong_family",
            &v4,
            None,
            None,
            Some(vec!["2000::1"]),
            vec!["not configured on this cluster"],
        ),
        (
            "IP_doesnt_match_family",
            &v4,
            None,
            Some(vec!["IPv4"]),
            Some(vec!["2000::1"]),
            vec!["expected an IPv4 value as indicated"],
        ),
        (
            "too_many_IPs_singlestack",
            &v4,
            None,
            None,
            Some(vec!["10.0.0.1", "10.0.0.2"]),
            vec!["no more than one IP for each IP family"],
        ),
        (
            "too_many_IPs_dualstack",
            &v46,
            None,
            None,
            Some(vec!["10.0.0.1", "2000::1", "10.0.0.2"]),
            vec!["only hold up to 2 values"],
        ),
        (
            "dup_IPs",
            &v4,
            None,
            None,
            Some(vec!["10.0.0.1", "10.0.0.1"]),
            vec!["no more than one IP for each IP family"],
        ),
        (
            "empty_IP",
            &v4,
            None,
            None,
            Some(vec![""]),
            vec!["must be empty when", "must be a valid IP"],
        ),
        (
            "None_IP_1",
            &v4,
            None,
            None,
            Some(vec!["10.0.0.1", "None"]),
            vec!["must be a valid IP"],
        ),
    ];
    let mut failures = Vec::new();
    for (name, families, policy, fams, ips, expect) in cases {
        let api = cluster(families);
        let mut svc = make("foo", "ClusterIP");
        if let Some(p) = policy {
            svc["spec"]["ipFamilyPolicy"] = json!(p);
        }
        if let Some(f) = fams {
            svc["spec"]["ipFamilies"] = json!(f);
        }
        if let Some(ips) = ips {
            set_cluster_ips(&mut svc, &ips);
        }
        let (status, body) = api.post(SVCS, &svc).await;
        if status.is_success() {
            failures.push(format!("{name}: unexpected success"));
            continue;
        }
        let text = body.to_string();
        for want in expect {
            // #2469: the decode-time 400 stands in for the 422 NotSupported.
            if want == UNSUPPORTED && status == StatusCode::BAD_REQUEST {
                continue;
            }
            if !text.contains(want) {
                failures.push(format!("{name}: want {want:?} in {text}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn create_delete_reuse_frees_cluster_ips_and_node_ports() {
    // (name, ipFamilyPolicy, ipFamilies)
    let cases: [(&str, Option<&str>, Vec<&str>); 3] = [
        ("v4", None, vec!["IPv4"]),
        ("v6", None, vec!["IPv6"]),
        ("v4v6", Some("PreferDualStack"), vec!["IPv4", "IPv6"]),
    ];
    for (name, policy, fams) in cases {
        let api = cluster(&["IPv4", "IPv6"]);
        let mut svc = make("foo", "NodePort");
        svc["spec"]["ipFamilies"] = json!(fams);
        if let Some(p) = policy {
            svc["spec"]["ipFamilyPolicy"] = json!(p);
        }
        let (s, created) = api.post(SVCS, &svc).await;
        assert_eq!(s, StatusCode::CREATED, "{name}: {created}");
        let (s, b) = api.delete(&format!("{SVCS}/foo")).await;
        assert_eq!(s, StatusCode::OK, "{name}: delete {b}");

        // "Force the same IPs and ports" (:6176-6181).
        let mut svc2 = svc.clone();
        svc2["metadata"]["name"] = json!("foo2");
        svc2["spec"]["clusterIP"] = created["spec"]["clusterIP"].clone();
        svc2["spec"]["clusterIPs"] = created["spec"]["clusterIPs"].clone();
        svc2["spec"]["ports"] = created["spec"]["ports"].clone();
        let (s, b) = api.post(SVCS, &svc2).await;
        assert_eq!(s, StatusCode::CREATED, "{name}: reuse after delete: {b}");
        assert_eq!(
            b["spec"]["clusterIPs"], created["spec"]["clusterIPs"],
            "{name}"
        );
        assert_eq!(
            b["spec"]["ports"][0]["nodePort"], created["spec"]["ports"][0]["nodePort"],
            "{name}"
        );
    }
}

fn lb(ports: &[(&str, i64)], node_ports: &[i64], hcnp: Option<i64>, ips: &[&str]) -> Value {
    let mut svc = make("foo", "LoadBalancer");
    svc["spec"]["externalTrafficPolicy"] = json!("Local");
    svc["spec"]["allocateLoadBalancerNodePorts"] = json!(true);
    if !ports.is_empty() {
        let list: Vec<Value> = ports
            .iter()
            .enumerate()
            .map(|(i, (n, p))| {
                let mut port = json!({"name": n, "port": p, "targetPort": p, "protocol": "TCP"});
                if let Some(np) = node_ports.get(i).filter(|np| **np != 0) {
                    port["nodePort"] = json!(np);
                }
                port
            })
            .collect();
        svc["spec"]["ports"] = json!(list);
    } else if let Some(np) = node_ports.first() {
        svc["spec"]["ports"][0]["nodePort"] = json!(np);
    }
    if let Some(h) = hcnp {
        svc["spec"]["healthCheckNodePort"] = json!(h);
    }
    if !ips.is_empty() {
        set_cluster_ips(&mut svc, ips);
    }
    svc
}

#[tokio::test]
async fn update_patch_allocated_values() {
    const PQ: [(&str, i64); 2] = [("p", 867), ("q", 5309)];
    const QP: [(&str, i64); 2] = [("q", 5309), ("p", 867)];
    struct Case {
        name: &'static str,
        create: Value,
        update: Value,
        // None = expect rejection.
        want: Option<Want>,
    }
    struct Want {
        ips: Vec<&'static str>,
        node_ports: Vec<i64>,
        hcnp: i64,
    }
    let cases = vec![
        Case {
            name: "single-ip_single-port",
            create: lb(&[], &[30093], Some(30118), &["10.0.0.1"]),
            update: lb(&[], &[], None, &[]),
            want: Some(Want {
                ips: vec!["10.0.0.1"],
                node_ports: vec![30093],
                hcnp: 30118,
            }),
        },
        Case {
            name: "multi-ip_multi-port",
            create: {
                let mut s = lb(&PQ, &[30093, 30076], Some(30118), &["10.0.0.1", "2000::1"]);
                s["spec"]["ipFamilyPolicy"] = json!("PreferDualStack");
                s
            },
            update: lb(&PQ, &[], None, &[]),
            want: Some(Want {
                ips: vec!["10.0.0.1", "2000::1"],
                node_ports: vec![30093, 30076],
                hcnp: 30118,
            }),
        },
        Case {
            name: "multi-ip_partial",
            create: {
                let mut s = lb(&PQ, &[30093, 30076], Some(30118), &["10.0.0.1", "2000::1"]);
                s["spec"]["ipFamilyPolicy"] = json!("PreferDualStack");
                s
            },
            update: lb(&[], &[], None, &["10.0.0.1"]),
            want: None,
        },
        Case {
            name: "multi-port_partial",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&PQ, &[30093, 0], None, &[]),
            want: Some(Want {
                ips: vec![],
                node_ports: vec![30093, 30076],
                hcnp: 30118,
            }),
        },
        Case {
            name: "swap-ports",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&QP, &[], None, &[]),
            want: Some(Want {
                ips: vec![],
                node_ports: vec![30076, 30093],
                hcnp: 30118,
            }),
        },
        Case {
            name: "partial-swap-ports",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&PQ, &[30076, 0], Some(30118), &[]),
            // nodePort[1] is "anything but 30076" (proveNodePort(1, -30076)).
            want: Some(Want {
                ips: vec![],
                node_ports: vec![30076, -30076],
                hcnp: 30118,
            }),
        },
        Case {
            name: "swap-port-with-hcnp",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&PQ, &[30076, 30118], None, &[]),
            want: None,
        },
        Case {
            name: "partial-swap-port-with-hcnp",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&PQ, &[30118, 0], None, &[]),
            want: None,
        },
        Case {
            name: "update-hcnp",
            create: lb(&PQ, &[30093, 30076], Some(30118), &[]),
            update: lb(&PQ, &[30093, 30076], Some(30111), &[]),
            want: None,
        },
    ];
    let mut failures = Vec::new();
    for c in cases {
        let api = cluster(&["IPv4", "IPv6"]);
        let (s, b) = api.post(SVCS, &c.create).await;
        if s != StatusCode::CREATED {
            failures.push(format!("{}: create {s}: {b}", c.name));
            continue;
        }
        let (s, b) = api.put(&format!("{SVCS}/foo"), &c.update).await;
        match c.want {
            None => {
                if s.is_success() {
                    failures.push(format!("{}: update unexpectedly succeeded: {b}", c.name));
                }
            }
            Some(w) => {
                if !s.is_success() {
                    failures.push(format!("{}: update {s}: {b}", c.name));
                    continue;
                }
                for (i, ip) in w.ips.iter().enumerate() {
                    if b["spec"]["clusterIPs"][i] != remap(ip) {
                        failures.push(format!(
                            "{}: clusterIPs[{i}] want {} got {}",
                            c.name,
                            remap(ip),
                            b["spec"]["clusterIPs"][i]
                        ));
                    }
                }
                for (i, np) in w.node_ports.iter().enumerate() {
                    let got = b["spec"]["ports"][i]["nodePort"].as_i64().unwrap_or(0);
                    if (*np > 0 && got != *np) || (*np < 0 && got == -*np) {
                        failures.push(format!(
                            "{}: ports[{i}].nodePort want {np} got {got}",
                            c.name
                        ));
                    }
                }
                let got = b["spec"]["healthCheckNodePort"].as_i64().unwrap_or(0);
                if got != w.hcnp {
                    failures.push(format!(
                        "{}: healthCheckNodePort want {} got {got}",
                        c.name, w.hcnp
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
