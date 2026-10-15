//! Mirrors of the five object-count `ResourceQuota` conformance cases that had
//! no mirror at all (#1770).
//!
//! Upstream (`release-1.35`): `test/e2e/apimachinery/resource_quota.go`
//!
//! | case | line |
//! |---|---|
//! | should create a ResourceQuota and capture the life of a service. | :112 |
//! | should create a ResourceQuota and capture the life of a secret. | :172 |
//! | should create a ResourceQuota and capture the life of a configMap. | :339 |
//! | should create a ResourceQuota and capture the life of a replication controller. | :406 |
//! | should create a ResourceQuota and capture the life of a replica set. | :462 |
//!
//! Every case uses `newTestResourceQuota` (:2152-2176) for the quota, so the
//! mirror builds the same full `hard` list rather than only the key under
//! test. Upstream's `waitForResourceQuota` (:2495) polls the quota until
//! `status.used` matches; the mirror instead drives one
//! `ResourceQuotaController::reconcile_one` pass and then reads the quota
//! over HTTP, the same substitution the existing pod-lifecycle mirror makes
//! in `conformance_apimachinery_namespaces_quota_limits.rs`.
//! `haveUsedResources` compares only the keys in the expected list (other
//! keys are ignored), and compares with `Quantity.Equal`, so the mirror
//! compares the listed keys as quantities too.

use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const QUOTA: &str = "test-quota";

/// `newTestResourceQuota` (resource_quota.go:2152-2176), with
/// `classGold = "gold"` (:74) and `extendedResourceName = "example.com/dongle"`
/// (:76). `extra` overrides entries, as the secret and configMap cases do.
fn new_test_resource_quota(extra: &[(&str, String)]) -> Value {
    let mut hard = serde_json::Map::new();
    for (k, v) in [
        ("pods", "5"),
        ("services", "10"),
        ("services.nodeports", "1"),
        ("services.loadbalancers", "1"),
        ("replicationcontrollers", "10"),
        ("resourcequotas", "1"),
        ("cpu", "1"),
        ("memory", "500Mi"),
        ("configmaps", "10"),
        ("secrets", "10"),
        ("persistentvolumeclaims", "10"),
        ("requests.storage", "10Gi"),
        ("ephemeral-storage", "50Gi"),
        (
            "gold.storageclass.storage.k8s.io/persistentvolumeclaims",
            "10",
        ),
        ("gold.storageclass.storage.k8s.io/requests.storage", "10Gi"),
        ("count/replicasets.apps", "5"),
        ("requests.example.com/dongle", "3"),
    ] {
        hard.insert(k.to_string(), json!(v));
    }
    for (k, v) in extra {
        hard.insert((*k).to_string(), json!(v));
    }
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": QUOTA },
        "spec": { "hard": Value::Object(hard) }
    })
}

fn setup() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

async fn send(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some("application/json"), body).await;
    (status.as_u16(), value)
}

/// `countResourceQuota` (resource_quota.go): the number of quotas already in
/// the namespace before the test's own is created.
async fn count(api: &TestApiServer, ns: &str, resource: &str) -> usize {
    let (s, list) = send(
        api,
        "GET",
        &format!("/api/v1/namespaces/{ns}/{resource}"),
        None,
    )
    .await;
    assert_eq!(s, 200, "list {resource}: {list}");
    list["items"].as_array().map_or(0, |a| a.len())
}

async fn create_quota(api: &TestApiServer, ns: &str, quota: &Value) {
    let (s, body) = send(
        api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/resourcequotas"),
        Some(quota),
    )
    .await;
    assert_eq!(s, 201, "create quota: {body}");
}

/// Parse a canonical quantity string to milli-units so `"0"` and `"0m"`, or
/// `"1"` and `"1000m"`, compare equal (upstream compares with
/// `Quantity.Equal`). Only the plain-integer and `m` forms appear in these
/// object-count cases.
fn milli(q: &str) -> i64 {
    match q.strip_suffix('m') {
        Some(n) => n.parse().unwrap_or_else(|_| panic!("quantity {q}")),
        None => q.parse::<i64>().unwrap_or_else(|_| panic!("quantity {q}")) * 1000,
    }
}

/// One reconcile, then `haveUsedResources` (resource_quota.go:2513-2535):
/// every expected key must be present in `status.used` with an equal value.
async fn wait_for_resource_quota(
    api: &TestApiServer,
    mem: &Arc<MemoryStorage>,
    ns: &str,
    used: &[(&str, String)],
) {
    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, QUOTA)
        .await
        .expect("reconcile quota");
    let (s, quota) = send(
        api,
        "GET",
        &format!("/api/v1/namespaces/{ns}/resourcequotas/{QUOTA}"),
        None,
    )
    .await;
    assert_eq!(s, 200, "get quota: {quota}");
    for (key, want) in used {
        let got = quota["status"]["used"][*key]
            .as_str()
            .unwrap_or_else(|| panic!("status.used[{key}] missing: {quota}"));
        assert_eq!(
            milli(got),
            milli(want),
            "status.used[{key}] = {got}, want {want}: {quota}"
        );
    }
}

/// `newTestServiceForQuota` (resource_quota.go:2418-2438).
fn service(name: &str, ty: &str, allocate_lb_node_ports: Option<bool>) -> Value {
    let mut spec = json!({
        "type": ty,
        "ports": [{ "port": 80, "targetPort": 80 }]
    });
    if let Some(a) = allocate_lb_node_ports {
        spec["allocateLoadBalancerNodePorts"] = json!(a);
    }
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name },
        "spec": spec
    })
}

/// resource_quota.go:112 — capture the life of a service.
#[tokio::test]
async fn resource_quota_captures_the_life_of_a_service() {
    let (api, mem) = setup();
    let ns = "rq-service";
    let c = count(&api, ns, "resourcequotas").await;

    create_quota(&api, ns, &new_test_resource_quota(&[])).await;
    wait_for_resource_quota(&api, &mem, ns, &[("resourcequotas", (c + 1).to_string())]).await;

    // "Creating a Service" + "Creating a NodePort Service" (:131-139).
    let svc_uri = format!("/api/v1/namespaces/{ns}/services");
    let (s, b) = send(
        &api,
        "POST",
        &svc_uri,
        Some(&service("test-service", "ClusterIP", None)),
    )
    .await;
    assert_eq!(s, 201, "create ClusterIP service: {b}");
    let (s, b) = send(
        &api,
        "POST",
        &svc_uri,
        Some(&service("test-service-np", "NodePort", None)),
    )
    .await;
    assert_eq!(s, 201, "create NodePort service: {b}");

    // "Not allowing a LoadBalancer Service with NodePort to be created that
    // exceeds remaining quota" (:141-144): services.nodeports is 1/1 used.
    let (s, b) = send(
        &api,
        "POST",
        &svc_uri,
        Some(&service("test-service-lb", "LoadBalancer", Some(true))),
    )
    .await;
    assert!(
        !(200..300).contains(&s),
        "LoadBalancer service exceeding services.nodeports must be rejected, got {s}: {b}"
    );

    // "Ensuring resource quota status captures service creation" (:146-152).
    let mut used = vec![
        ("resourcequotas", (c + 1).to_string()),
        ("services", "2".to_string()),
        ("services.nodeports", "1".to_string()),
    ];
    wait_for_resource_quota(&api, &mem, ns, &used).await;

    // "Deleting Services" (:154-158).
    for name in ["test-service", "test-service-np"] {
        let (s, b) = send(&api, "DELETE", &format!("{svc_uri}/{name}"), None).await;
        assert!((200..300).contains(&s), "delete {name}: {s} {b}");
    }

    // "Ensuring resource quota status released usage" (:160-164).
    used[1].1 = "0".to_string();
    used[2].1 = "0".to_string();
    wait_for_resource_quota(&api, &mem, ns, &used).await;
}

/// resource_quota.go:172 — capture the life of a secret.
#[tokio::test]
async fn resource_quota_captures_the_life_of_a_secret() {
    let (api, mem) = setup();
    let ns = "rq-secret";
    // "Discovering how many secrets are in namespace by default" (:173-190).
    let found = count(&api, ns, "secrets").await;
    let default_secrets = found.to_string();
    let hard_secrets = (found + 1).to_string();
    let c = count(&api, ns, "resourcequotas").await;

    create_quota(
        &api,
        ns,
        &new_test_resource_quota(&[("secrets", hard_secrets.clone())]),
    )
    .await;
    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("secrets", default_secrets.clone()),
        ],
    )
    .await;

    // "Creating a Secret" — `newTestSecretForQuota` (:2451-2462).
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "test-secret" },
        "data": {
            "data-1": "dmFsdWUtMQo=",
            "data-2": "dmFsdWUtMgo=",
            "data-3": "dmFsdWUtMwo="
        }
    });
    let uri = format!("/api/v1/namespaces/{ns}/secrets");
    let (s, b) = send(&api, "POST", &uri, Some(&secret)).await;
    assert_eq!(s, 201, "create secret: {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("secrets", hard_secrets)]).await;

    let (s, b) = send(&api, "DELETE", &format!("{uri}/test-secret"), None).await;
    assert!((200..300).contains(&s), "delete secret: {s} {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("secrets", default_secrets)]).await;
}

/// resource_quota.go:339 — capture the life of a configMap.
#[tokio::test]
async fn resource_quota_captures_the_life_of_a_configmap() {
    let (api, mem) = setup();
    let ns = "rq-configmap";
    let found = count(&api, ns, "configmaps").await;
    let default_cms = found.to_string();
    let hard_cms = (found + 1).to_string();
    let c = count(&api, ns, "resourcequotas").await;

    create_quota(
        &api,
        ns,
        &new_test_resource_quota(&[("configmaps", hard_cms.clone())]),
    )
    .await;
    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("configmaps", default_cms.clone()),
        ],
    )
    .await;

    // `newTestConfigMapForQuota` (:2440-2449).
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "test-configmap" },
        "data": { "a": "b" }
    });
    let uri = format!("/api/v1/namespaces/{ns}/configmaps");
    let (s, b) = send(&api, "POST", &uri, Some(&cm)).await;
    assert_eq!(s, 201, "create configmap: {b}");

    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("configmaps", hard_cms),
        ],
    )
    .await;

    let (s, b) = send(&api, "DELETE", &format!("{uri}/test-configmap"), None).await;
    assert!((200..300).contains(&s), "delete configmap: {s} {b}");

    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("configmaps", default_cms),
        ],
    )
    .await;
}

/// resource_quota.go:406 — capture the life of a replication controller.
#[tokio::test]
async fn resource_quota_captures_the_life_of_a_replication_controller() {
    let (api, mem) = setup();
    let ns = "rq-rc";
    let c = count(&api, ns, "resourcequotas").await;

    create_quota(&api, ns, &new_test_resource_quota(&[])).await;
    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("replicationcontrollers", "0".to_string()),
        ],
    )
    .await;

    // `newTestReplicationControllerForQuota("test-rc", "nginx", 0)`
    // (:2362-2388).
    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": { "name": "test-rc" },
        "spec": {
            "replicas": 0,
            "selector": { "name": "test-rc" },
            "template": {
                "metadata": { "labels": { "name": "test-rc" } },
                "spec": { "containers": [{ "name": "test-rc", "image": "nginx" }] }
            }
        }
    });
    let uri = format!("/api/v1/namespaces/{ns}/replicationcontrollers");
    let (s, b) = send(&api, "POST", &uri, Some(&rc)).await;
    assert_eq!(s, 201, "create rc: {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("replicationcontrollers", "1".into())]).await;

    // Deleted with PropagationPolicy=Background (:426-436).
    let opts = json!({
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background"
    });
    let (s, b) = send(&api, "DELETE", &format!("{uri}/test-rc"), Some(&opts)).await;
    assert!((200..300).contains(&s), "delete rc: {s} {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("replicationcontrollers", "0".into())]).await;
}

/// resource_quota.go:462 — capture the life of a replica set. Exercises the
/// generic `count/<resource>.<group>` object-count key.
#[tokio::test]
async fn resource_quota_captures_the_life_of_a_replica_set() {
    let (api, mem) = setup();
    let ns = "rq-rs";
    let c = count(&api, ns, "resourcequotas").await;

    create_quota(&api, ns, &new_test_resource_quota(&[])).await;
    wait_for_resource_quota(
        &api,
        &mem,
        ns,
        &[
            ("resourcequotas", (c + 1).to_string()),
            ("count/replicasets.apps", "0".to_string()),
        ],
    )
    .await;

    // `newTestReplicaSetForQuota("test-rs", "nginx", 0)` (:2390-2416).
    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": { "name": "test-rs" },
        "spec": {
            "replicas": 0,
            "selector": { "matchLabels": { "name": "test-rs" } },
            "template": {
                "metadata": { "labels": { "name": "test-rs" } },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{ "name": "test-rs", "image": "nginx" }]
                }
            }
        }
    });
    let uri = format!("/apis/apps/v1/namespaces/{ns}/replicasets");
    let (s, b) = send(&api, "POST", &uri, Some(&rs)).await;
    assert_eq!(s, 201, "create rs: {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("count/replicasets.apps", "1".into())]).await;

    let (s, b) = send(&api, "DELETE", &format!("{uri}/test-rs"), None).await;
    assert!((200..300).contains(&s), "delete rs: {s} {b}");

    wait_for_resource_quota(&api, &mem, ns, &[("count/replicasets.apps", "0".into())]).await;
}
