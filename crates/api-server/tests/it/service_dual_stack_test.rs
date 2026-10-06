//! Dual-stack ClusterIP allocation: `--service-cluster-ip-range` carrying a
//! second family gives the api-server a second allocator
//! (`serviceIPAllocatorsByFamily`, pkg/registry/core/rest/storage_core.go:
//! 329-490), and `allocClusterIPs` / `updateClusterIPs` (pkg/registry/core/
//! service/storage/alloc.go:340-451, 699-752) allocate and release per family.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";
const IPS: &str = "/apis/networking.k8s.io/v1/ipaddresses";
const DUAL: &str = "10.96.0.0/12,2001:db8:1::/112";

fn dual() -> TestApiServer {
    TestApiServer::builder()
        .service_cluster_ip_range(DUAL)
        .build()
}

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
async fn require_dual_stack_allocates_one_ip_per_family() {
    let api = dual();
    let (s, c) = api
        .post(
            SVCS,
            &svc("a", json!({"ipFamilyPolicy": "RequireDualStack"})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv4", "IPv6"]));
    let ips = c["spec"]["clusterIPs"].as_array().unwrap().clone();
    assert_eq!(ips.len(), 2, "{c}");
    assert!(ips[0].as_str().unwrap().starts_with("10."), "{c}");
    assert!(ips[1].as_str().unwrap().starts_with("2001:db8:1::"), "{c}");
    assert_eq!(c["spec"]["clusterIP"], ips[0]);

    for ip in &ips {
        let (s, _) = api.get(&format!("{IPS}/{}", ip.as_str().unwrap())).await;
        assert_eq!(s, StatusCode::OK);
    }
    let (s, _) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK);
    for ip in &ips {
        let (s, _) = api.get(&format!("{IPS}/{}", ip.as_str().unwrap())).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "both families are released");
    }
}

#[tokio::test]
async fn prefer_dual_stack_gets_both_and_default_is_single_stack() {
    let api = dual();
    let (s, c) = api
        .post(
            SVCS,
            &svc("p", json!({"ipFamilyPolicy": "PreferDualStack"})),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["clusterIPs"].as_array().unwrap().len(), 2, "{c}");

    let (s, c) = api.post(SVCS, &svc("d", json!({}))).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilyPolicy"], "SingleStack");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv4"]));
    assert_eq!(c["spec"]["clusterIPs"].as_array().unwrap().len(), 1, "{c}");
}

#[tokio::test]
async fn an_ipv6_service_is_served_on_a_dual_stack_cluster() {
    let api = dual();
    let (s, c) = api
        .post(SVCS, &svc("v6", json!({"ipFamilies": ["IPv6"]})))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6"]));
    assert!(
        c["spec"]["clusterIP"]
            .as_str()
            .unwrap()
            .starts_with("2001:db8:1::"),
        "{c}"
    );

    let (s, c) = api
        .post(SVCS, &svc("v6b", json!({"clusterIP": "2001:db8:1::77"})))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    assert_eq!(c["spec"]["ipFamilies"], json!(["IPv6"]));
}

#[tokio::test]
async fn a_single_stack_cluster_still_refuses_dual_stack() {
    let api = TestApiServer::new();
    let (s, c) = api
        .post(
            SVCS,
            &svc("a", json!({"ipFamilyPolicy": "RequireDualStack"})),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{c}");
    assert!(
        c["message"]
            .as_str()
            .unwrap()
            .contains("this cluster is not configured for dual-stack services"),
        "{c}"
    );
}

#[tokio::test]
async fn upgrade_allocates_the_secondary_and_downgrade_releases_it() {
    let api = dual();
    let (s, c) = api.post(SVCS, &svc("u", json!({}))).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");

    // CASE C: SingleStack -> RequireDualStack.
    let mut up = c.clone();
    up["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
    let (s, up) = api.put(&format!("{SVCS}/u"), &up).await;
    assert_eq!(s, StatusCode::OK, "{up}");
    assert_eq!(up["spec"]["ipFamilies"], json!(["IPv4", "IPv6"]));
    let ips = up["spec"]["clusterIPs"].as_array().unwrap().clone();
    assert_eq!(ips.len(), 2, "{up}");
    assert_eq!(ips[0], c["spec"]["clusterIP"], "the primary is kept");
    let secondary = ips[1].as_str().unwrap().to_string();
    let (s, _) = api.get(&format!("{IPS}/{secondary}")).await;
    assert_eq!(s, StatusCode::OK);

    // CASE D: back to SingleStack drops the secondary.
    let mut down = up.clone();
    down["spec"]["ipFamilyPolicy"] = json!("SingleStack");
    down["spec"]["ipFamilies"] = json!(["IPv4"]);
    down["spec"]["clusterIPs"] = json!([ips[0]]);
    let (s, down) = api.put(&format!("{SVCS}/u"), &down).await;
    assert_eq!(s, StatusCode::OK, "{down}");
    assert_eq!(down["spec"]["clusterIPs"], json!([ips[0]]));
    let (s, _) = api.get(&format!("{IPS}/{secondary}")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "the secondary is released");
}

#[tokio::test]
async fn a_failed_dual_stack_create_releases_both_ips() {
    let api = dual();
    let np = |name: &str| {
        let mut s = svc(
            name,
            json!({"type": "NodePort", "ipFamilyPolicy": "RequireDualStack",
                   "clusterIP": "10.96.0.60", "clusterIPs": ["10.96.0.60", "2001:db8:1::60"]}),
        );
        s["spec"]["ports"][0]["nodePort"] = json!(30701);
        s
    };
    let (s, c) = api.post(SVCS, &np("a")).await;
    assert_eq!(s, StatusCode::CREATED, "{c}");
    let (s, _) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK);
    let mut holder = svc("holder", json!({"type": "NodePort"}));
    holder["spec"]["ports"][0]["nodePort"] = json!(30701);
    let (s, _) = api.post(SVCS, &holder).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, c) = api.post(SVCS, &np("b")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{c}");
    for ip in ["10.96.0.60", "2001:db8:1::60"] {
        let (s, _) = api.get(&format!("{IPS}/{ip}")).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{ip} was rolled back");
    }
}
