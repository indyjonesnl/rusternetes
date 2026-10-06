//! An IPv6-primary `--service-cluster-ip-range` (`fd00::/112`, or
//! `fd00::/112,10.96.0.0/12`): the first range names the cluster's default
//! IP family (`REST.primaryIPFamily`, pkg/registry/core/service/storage/
//! storage.go:115-124), so a Service with no family preference lands on it.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";

fn svc(name: &str, extra: Value) -> Value {
    let mut spec = json!({"selector": {"app": name}, "ports": [{"port": 80, "protocol": "TCP"}]});
    for (k, v) in extra.as_object().unwrap() {
        spec[k] = v.clone();
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec
    })
}

#[tokio::test]
async fn single_stack_ipv6_cluster_allocates_from_the_v6_range() {
    let api = TestApiServer::builder()
        .service_cluster_ip_range("2001:db8:1::/112")
        .build();
    let (s, c) = api.post(SVCS, &svc("a", json!({}))).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6"]), "{c}");
    assert_eq!(c["spec"]["ipFamilyPolicy"], "SingleStack");
    assert!(
        c["spec"]["clusterIP"]
            .as_str()
            .unwrap()
            .starts_with("2001:db8:1::"),
        "{c}"
    );
}

#[tokio::test]
async fn ipv6_primary_dual_stack_orders_families_primary_first() {
    let api = TestApiServer::builder()
        .service_cluster_ip_range("2001:db8:1::/112,10.96.0.0/12")
        .build();
    let (s, c) = api.post(SVCS, &svc("d", json!({}))).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6"]), "{c}");

    let (s, c) = api
        .post(
            SVCS,
            &svc("r", json!({"ipFamilyPolicy": "RequireDualStack"})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6", "IPv4"]), "{c}");
    let ips = c["spec"]["clusterIPs"].as_array().unwrap();
    assert!(ips[0].as_str().unwrap().starts_with("2001:db8:1::"), "{c}");
    assert!(ips[1].as_str().unwrap().starts_with("10."), "{c}");
    assert_eq!(c["spec"]["clusterIP"], ips[0]);
}

/// A selectorless headless Service reads back as dual-stack with the
/// primary family first (`defaultOnReadIPFamilies`, storage.go:296-312).
#[tokio::test]
async fn headless_selectorless_reads_back_primary_first() {
    let api = TestApiServer::builder()
        .service_cluster_ip_range("2001:db8:1::/112,10.96.0.0/12")
        .build();
    let mut body = svc("h", json!({"clusterIP": "None"}));
    body["spec"].as_object_mut().unwrap().remove("selector");
    let (s, c) = api.post(SVCS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6", "IPv4"]), "{c}");
}
