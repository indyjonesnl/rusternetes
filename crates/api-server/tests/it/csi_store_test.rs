//! CSINode and CSIStorageCapacity on the generic Store (#1990): the strategy
//! rules of `pkg/registry/storage/{csinode,csistoragecapacity}/strategy.go`
//! run inside `Store.Create` / `Store.Update` on every write path.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NODES: &str = "/apis/storage.k8s.io/v1/csinodes";
const CAPS: &str = "/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities";

fn csinode(name: &str, node_id: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
        "metadata": {"name": name},
        "spec": {"drivers": [{"name": "csi.example.com", "nodeID": node_id,
                              "topologyKeys": ["topology.kubernetes.io/zone"]}]}
    })
}

fn capacity(name: &str, topology: Value) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
        "metadata": {"name": name, "namespace": "default"},
        "storageClassName": "fast", "capacity": "10Gi",
        "nodeTopology": topology
    })
}

/// `AllowLongNodeID: true` (csinode/strategy.go:57-59): 256 characters are
/// accepted, 257 are not.
#[tokio::test]
async fn a_node_id_may_be_256_characters() {
    let api = TestApiServer::new();
    let (s, body) = api.post(NODES, &csinode("n1", &"a".repeat(256))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api.post(NODES, &csinode("n2", &"a".repeat(257))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("must be 256 characters or less"),
        "{body}"
    );
}

/// `MutableCSINodeAllocatableCount` is on in 1.35: `allocatable` changes,
/// anything else in an existing driver does not (validation.go:313-339).
#[tokio::test]
async fn only_allocatable_may_change_on_an_existing_driver() {
    let api = TestApiServer::new();
    let (s, created) = api.post(NODES, &csinode("n1", "node-1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["drivers"][0]["allocatable"] = json!({"count": 10});
    let (s, body) = api.put(&format!("{NODES}/n1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["drivers"][0]["allocatable"]["count"], 10);

    let mut update = body.clone();
    update["spec"]["drivers"][0]["nodeID"] = json!("node-2");
    let (s, body) = api.put(&format!("{NODES}/n1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("spec.drivers: Invalid value"),
        "{body}"
    );
}

/// `AllowUnconditionalUpdate() == false` (store.go:727-733).
#[tokio::test]
async fn a_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(NODES, &csinode("n1", "node-1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .put(&format!("{NODES}/n1"), &csinode("n1", "node-1"))
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("must be specified for an update"),
        "{body}"
    );
}

/// `AllowCreateOnUpdate() == false`: a PUT to a missing CSINode is a 404.
#[tokio::test]
async fn a_put_to_a_missing_csinode_is_not_a_create() {
    let api = TestApiServer::new();
    let (s, body) = api
        .put(&format!("{NODES}/nope"), &csinode("nope", "n"))
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `ReturnDeletedObject: true` (csinode/storage/storage.go:51).
#[tokio::test]
async fn deleting_a_csinode_returns_it() {
    let api = TestApiServer::new();
    let (s, _) = api.post(NODES, &csinode("n1", "node-1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{NODES}/n1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "CSINode", "{body}");
}

/// The CSIStorageCapacity store sets no `ReturnDeletedObject`, so a delete
/// answers with a Status.
#[tokio::test]
async fn deleting_a_capacity_returns_a_status() {
    let api = TestApiServer::new();
    let (s, body) = api.post(CAPS, &capacity("c1", json!(null))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api.delete(&format!("{CAPS}/c1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "Status", "{body}");
}

/// `ValidateStorageCapacityName` is `NameIsDNSSubdomain`.
#[tokio::test]
async fn a_capacity_name_must_be_a_dns_subdomain() {
    let api = TestApiServer::new();
    let (s, body) = api.post(CAPS, &capacity("Bad_Name", json!(null))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("metadata.name"),
        "{body}"
    );
}

/// `ValidateCSIStorageCapacityUpdate`: `nodeTopology` is immutable, and the
/// bad value is the submitted selector (validation.go:614-616).
#[tokio::test]
async fn node_topology_is_immutable_and_reported_with_its_value() {
    let api = TestApiServer::new();
    let (s, created) = api
        .post(CAPS, &capacity("c1", json!({"matchLabels": {"zone": "a"}})))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["nodeTopology"] = json!({"matchLabels": {"zone": "b"}});
    let (s, body) = api.put(&format!("{CAPS}/c1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("nodeTopology: Invalid value"), "{msg}");
    assert!(msg.contains("zone"), "value, not a placeholder: {msg}");
    assert!(msg.contains("field is immutable"), "{msg}");
}

/// A capacity's `capacity` may change.
#[tokio::test]
async fn a_capacity_value_may_change() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CAPS, &capacity("c1", json!(null))).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["capacity"] = json!("20Gi");
    let (s, body) = api.put(&format!("{CAPS}/c1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["capacity"], "20Gi");
}
