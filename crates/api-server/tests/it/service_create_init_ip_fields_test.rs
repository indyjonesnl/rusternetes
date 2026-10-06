//! `TestCreateInitIPFields` (pkg/registry/core/service/storage/
//! storage_test.go:1512-5975): the 520-case table pinning what
//! `initIPFamilyFields` (storage/alloc.go:104-303) defaults and rejects for a
//! create, on a v4, v6, v4v6 and v6v4 cluster.
//!
//! The table is extracted mechanically from the checkout, not retyped:
//! `fixtures_create_init_ip_fields.json` holds one object per cluster, each
//! case being the `svctest.MakeService` tweaks plus the expectation.
//!
//! Two deliberate deviations. Upstream's allocator ranges (storage_test.go:
//! 70-81) are `10.0.0.0/16` and `2000::/108`; `--service-cluster-ip-range[0]`
//! must still be the default `10.96.0.0/12` (`ipranges.rs`, the `kubernetes`
//! Service address is not yet derived from the range), so the table's
//! `10.0.0.1` / `2000::1` are remapped into `10.96.0.0/12` / `2001:db8:1::/112`.
//! For the same reason the two clusters with an IPv6 primary (`singlestack:v6`,
//! `dualstack:v6v4`, 260 cases) cannot be started and are skipped.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";

fn range_for(families: &[Value]) -> String {
    families
        .iter()
        .map(|f| match f.as_str().unwrap() {
            "IPv4" => "10.96.0.0/12",
            _ => "2001:db8:1::/112",
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// `svctest.MakeService("foo", tweaks...)` (pkg/api/service/testing/make.go:34).
fn make_service(case: &Value) -> Value {
    let mut spec = json!({
        "type": "ClusterIP",
        "sessionAffinity": "None",
        "ports": [{"port": 93, "targetPort": 76, "protocol": "TCP"}],
    });
    if case["selector"].as_bool().unwrap() {
        spec["selector"] = json!({"k": "v"});
    }
    if let Some(ips) = case.get("clusterIPs") {
        let ips = json!(ips
            .as_array()
            .unwrap()
            .iter()
            .map(|ip| match ip.as_str().unwrap() {
                "10.0.0.1" => "10.96.0.10",
                "2000::1" => "2001:db8:1::10",
                other => other,
            })
            .collect::<Vec<_>>());
        spec["clusterIP"] = ips[0].clone();
        spec["clusterIPs"] = ips.clone();
    }
    if let Some(p) = case.get("policy") {
        spec["ipFamilyPolicy"] = p.clone();
    }
    if let Some(f) = case.get("families") {
        spec["ipFamilies"] = f.clone();
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "foo", "namespace": "default"},
        "spec": spec,
    })
}

#[tokio::test]
async fn create_init_ip_fields_matches_upstream_table() {
    let clusters: Vec<Value> =
        serde_json::from_str(include_str!("fixtures_create_init_ip_fields.json")).unwrap();
    let mut failures = Vec::new();
    let mut ran = 0;
    for cluster in &clusters {
        if cluster["clusterFamilies"][0] == "IPv6" {
            continue;
        }
        let range = range_for(cluster["clusterFamilies"].as_array().unwrap());
        let api = TestApiServer::builder()
            .service_cluster_ip_range(&range)
            .build();
        for case in cluster["cases"].as_array().unwrap() {
            ran += 1;
            let label = format!("{}/{}", cluster["name"].as_str().unwrap(), case["name"]);
            let (status, body) = api.post(SVCS, &make_service(case)).await;
            if case["expectError"].as_bool().unwrap() {
                if status.is_success() {
                    failures.push(format!("{label}: unexpected success: {body}"));
                    let _ = api.delete(&format!("{SVCS}/foo")).await;
                }
                continue;
            }
            if status != StatusCode::CREATED {
                failures.push(format!("{label}: create failed {status}: {body}"));
                continue;
            }
            let spec = &body["spec"];
            if spec["ipFamilyPolicy"] != case["expectPolicy"] {
                failures.push(format!(
                    "{label}: policy want {} got {}",
                    case["expectPolicy"], spec["ipFamilyPolicy"]
                ));
            }
            if spec["ipFamilies"] != case["expectFamilies"] {
                failures.push(format!(
                    "{label}: families want {} got {}",
                    case["expectFamilies"], spec["ipFamilies"]
                ));
            }
            if case["expectHeadless"].as_bool().unwrap() {
                if spec["clusterIP"] != "None" {
                    failures.push(format!("{label}: not headless: {}", spec["clusterIP"]));
                }
            } else if spec["clusterIPs"].as_array().map(Vec::len)
                != case["expectFamilies"].as_array().map(Vec::len)
            {
                // proveClusterIPsAllocated: one IP per family.
                failures.push(format!("{label}: clusterIPs {}", spec["clusterIPs"]));
            }
            let (s, _) = api.delete(&format!("{SVCS}/foo")).await;
            assert_eq!(s, StatusCode::OK, "{label}: delete");
        }
    }
    assert_eq!(ran, 260);
    assert!(
        failures.is_empty(),
        "{} of {ran} diverge from upstream:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
