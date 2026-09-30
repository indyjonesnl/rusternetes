//! NodePorts are claimed through the shared allocator (upstream
//! pkg/registry/core/service/storage/alloc.go): a requested port that
//! another Service holds is a 422 on `spec.ports[i].nodePort`, and deleting
//! the holder frees it. The old handler picked ports from the clock and
//! checked nothing, so two Services could share one.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";

fn node_port_svc(name: &str, node_port: Option<u16>) -> Value {
    let mut port = json!({"port": 80, "protocol": "TCP"});
    if let Some(np) = node_port {
        port["nodePort"] = json!(np);
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"type": "NodePort", "selector": {"app": name}, "ports": [port]}
    })
}

#[tokio::test]
async fn a_requested_node_port_in_use_is_rejected_until_released() {
    let api = TestApiServer::new();

    let (s, _) = api.post(SVCS, &node_port_svc("a", Some(30555))).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api.post(SVCS, &node_port_svc("b", Some(30555))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("spec.ports[0].nodePort")
            && msg.contains("provided port is already allocated"),
        "{body}"
    );

    let (s, _) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = api.post(SVCS, &node_port_svc("b", Some(30555))).await;
    assert_eq!(s, StatusCode::CREATED, "the port was freed: {body}");
}

#[tokio::test]
async fn allocated_node_ports_are_unique() {
    let api = TestApiServer::new();
    let mut seen = std::collections::HashSet::new();
    for i in 0..50 {
        let (s, body) = api.post(SVCS, &node_port_svc(&format!("s{i}"), None)).await;
        assert_eq!(s, StatusCode::CREATED, "{body}");
        let np = body["spec"]["ports"][0]["nodePort"]
            .as_u64()
            .expect("allocated");
        assert!((30000..=32767).contains(&np), "{np}");
        assert!(seen.insert(np), "nodePort {np} handed out twice");
    }
}

#[tokio::test]
async fn an_update_to_a_port_in_use_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SVCS, &node_port_svc("a", Some(30600))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, created) = api.post(SVCS, &node_port_svc("b", Some(30601))).await;
    assert_eq!(s, StatusCode::CREATED);

    let mut update = created.clone();
    update["spec"]["ports"][0]["nodePort"] = json!(30600);
    let (s, body) = api.put(&format!("{SVCS}/b"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // Switching b to ClusterIP frees 30601 for someone else.
    let mut to_cip = created;
    to_cip["spec"]["type"] = json!("ClusterIP");
    to_cip["spec"]["ports"][0]
        .as_object_mut()
        .unwrap()
        .remove("nodePort");
    let (s, body) = api.put(&format!("{SVCS}/b"), &to_cip).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, body) = api.post(SVCS, &node_port_svc("c", Some(30601))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}
