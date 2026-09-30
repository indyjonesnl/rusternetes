//! Services on the generic Store (#2077): the allocation hooks of upstream's
//! Service REST (pkg/registry/core/service/storage/storage.go) run inside
//! `Store.Create` / `Store.Update` / `Store.Delete`, so every write path —
//! PATCH and finalizer-drained deletes included — claims and frees
//! ClusterIPs and node ports with the object.

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

fn cluster_ip_svc(name: &str, ip: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"clusterIP": ip, "selector": {"app": name}, "ports": [{"port": 80}]}
    })
}

/// A PATCH to a node port another Service holds is rejected before the
/// write: `beginUpdate` runs inside `Store.Update` (store.go:742-750), so the
/// stored object keeps its port.
#[tokio::test]
async fn a_patch_to_a_port_in_use_is_rejected_and_not_stored() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SVCS, &node_port_svc("a", Some(30610))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = api.post(SVCS, &node_port_svc("b", Some(30611))).await;
    assert_eq!(s, StatusCode::CREATED);

    let patch = json!({"spec": {"ports": [{"port": 80, "protocol": "TCP", "nodePort": 30610}]}});
    let (s, body) = api.patch(&format!("{SVCS}/b"), &patch).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let (_, stored) = api.get(&format!("{SVCS}/b")).await;
    assert_eq!(stored["spec"]["ports"][0]["nodePort"], 30611, "{stored}");
}

/// A delete that completes when the last finalizer is removed runs
/// `afterDelete` (storage.go:332-358) and frees the node port at once.
#[tokio::test]
async fn a_finalizer_drained_delete_releases_the_node_port() {
    let api = TestApiServer::new();
    let mut svc = node_port_svc("a", Some(30620));
    svc["metadata"]["finalizers"] = json!(["example.com/hold"]);
    let (s, body) = api.post(SVCS, &svc).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let (s, body) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert!(body["metadata"]["deletionTimestamp"].is_string(), "{body}");

    let (s, body) = api.post(SVCS, &node_port_svc("b", Some(30620))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "still held: {body}");

    let (s, body) = api
        .patch(
            &format!("{SVCS}/a"),
            &json!({"metadata": {"finalizers": null}}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, body) = api.post(SVCS, &node_port_svc("b", Some(30620))).await;
    assert_eq!(s, StatusCode::CREATED, "freed with the Service: {body}");
}

/// `afterDelete` deletes the Endpoints of the same name (storage.go:347-352).
#[tokio::test]
async fn a_delete_removes_the_endpoints_of_the_same_name() {
    let api = TestApiServer::new();
    let (s, body) = api
        .post(
            "/api/v1/namespaces/default/endpoints",
            &json!({"apiVersion": "v1", "kind": "Endpoints",
                    "metadata": {"name": "e", "namespace": "default"}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, _) = api.post(SVCS, &cluster_ip_svc("e", "")).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api.delete(&format!("{SVCS}/e")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get("/api/v1/namespaces/default/endpoints/e").await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// `ReturnDeletedObject: true` (storage.go:92): a delete answers with the
/// Service, not a Status.
#[tokio::test]
async fn a_delete_returns_the_service() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SVCS, &cluster_ip_svc("d", "")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{SVCS}/d")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "Service", "{body}");
    assert_eq!(body["metadata"]["name"], "d", "{body}");
}

/// Resubmitting the create body, which names no ClusterIP or node port,
/// keeps what was allocated (`patchAllocatedValues`, storage.go:589-666).
#[tokio::test]
async fn resubmitting_the_create_body_keeps_allocated_values() {
    let api = TestApiServer::new();
    let body = node_port_svc("r", None);
    let (s, created) = api.post(SVCS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let (s, updated) = api.put(&format!("{SVCS}/r"), &body).await;
    assert_eq!(s, StatusCode::OK, "{updated}");
    assert_eq!(updated["spec"]["clusterIP"], created["spec"]["clusterIP"]);
    assert_eq!(
        updated["spec"]["ports"][0]["nodePort"],
        created["spec"]["ports"][0]["nodePort"]
    );
}

/// Turning a Service into ExternalName while resubmitting its ClusterIP:
/// `dropTypeDependentFields` (strategy.go) clears the IP fields, and the
/// update frees the address (`txnUpdateClusterIPs` case B).
#[tokio::test]
async fn converting_to_external_name_frees_the_cluster_ip() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SVCS, &cluster_ip_svc("x", "10.96.200.10")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["type"] = json!("ExternalName");
    update["spec"]["externalName"] = json!("foo.example.com");
    let (s, updated) = api.put(&format!("{SVCS}/x"), &update).await;
    assert_eq!(s, StatusCode::OK, "{updated}");
    assert!(updated["spec"].get("clusterIP").is_none(), "{updated}");
    assert!(updated["spec"].get("ipFamilies").is_none(), "{updated}");

    let (s, body) = api.post(SVCS, &cluster_ip_svc("y", "10.96.200.10")).await;
    assert_eq!(s, StatusCode::CREATED, "freed: {body}");
}

/// A status write keeps the spec (`serviceStatusStrategy.PrepareForUpdate`)
/// and defaults an ingress ipMode to VIP on decode (`SetDefaults_Service`).
#[tokio::test]
async fn a_status_update_keeps_the_spec() {
    let api = TestApiServer::new();
    let mut svc = cluster_ip_svc("lb", "");
    svc["spec"]["type"] = json!("LoadBalancer");
    let (s, created) = api.post(SVCS, &svc).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["spec"]["allocateLoadBalancerNodePorts"], true);
    assert_eq!(created["spec"]["externalTrafficPolicy"], "Cluster");

    let mut update = created.clone();
    update["spec"]["sessionAffinity"] = json!("ClientIP");
    update["status"] = json!({"loadBalancer": {"ingress": [{"ip": "1.2.3.4"}]}});
    let (s, body) = api.put(&format!("{SVCS}/lb/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["sessionAffinity"], "None", "{body}");
    assert_eq!(
        body["status"]["loadBalancer"]["ingress"][0],
        json!({"ip": "1.2.3.4", "ipMode": "VIP"})
    );
}
