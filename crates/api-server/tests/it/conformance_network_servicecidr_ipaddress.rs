//! Mirror of the two `[sig-network] ServiceCIDR and IPAddress API`
//! conformance cases (#1786), Kubernetes release-1.35:
//!
//! * `test/e2e/network/service_cidrs.go:166`
//!   "should support ServiceCIDR API operations"
//! * `test/e2e/network/service_cidrs.go:212`
//!   "should support IPAddress API operations"
//!
//! Both case descriptions (service_cidrs.go:155-163, :202-209) state the
//! discovery documents and the verb set as requirements, so those are
//! asserted first, as #1785 established for the Ingress mirrors. The verb
//! sequence then follows each case body step by step.
//!
//! The ServiceCIDR case starts from the `kubernetes` ServiceCIDR, which
//! upstream's `kubernetes-service-cidr-controller` creates
//! (`pkg/controlplane/controller/defaultservicecidr`). The in-process harness
//! runs no post-start hooks, so the test runs one `sync` of our port of that
//! controller ([`DefaultServiceCIDRController`]) the way the api-server does
//! at startup.
//!
//! Beyond upstream: the IPAddress `deletecollection` step adds a bystander
//! (not labelled `type=link-local`) that must survive, so a handler that
//! ignored the label selector would fail here (#1786, the #1781 lesson).
//! Everything else is exactly the upstream sequence.

use std::sync::Arc;

use futures::StreamExt;
use rusternetes_api_server::bootstrap::{DefaultServiceCIDRController, DEFAULT_SERVICE_CIDR_NAME};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GV: &str = "/apis/networking.k8s.io/v1";

/// The eight verbs both case descriptions require of the main resource
/// (service_cidrs.go:161, :208).
const COLLECTION_VERBS: &[&str] = &[
    "create",
    "get",
    "list",
    "watch",
    "update",
    "patch",
    "delete",
    "deletecollection",
];

/// The three discovery requirements of service_cidrs.go:158-160 / :205-207,
/// returning the `/apis/networking.k8s.io/v1` resource list.
async fn networking_v1_discovery(server: &TestApiServer) -> Vec<Value> {
    let (status, groups) = server.get("/apis").await;
    assert_eq!(status.as_u16(), 200);
    let group = groups["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .find(|g| g["name"] == "networking.k8s.io")
        .unwrap_or_else(|| panic!("networking.k8s.io missing from /apis: {groups}"))
        .clone();
    assert!(
        group["versions"]
            .as_array()
            .expect("versions")
            .iter()
            .any(|v| v["version"] == "v1"),
        "networking.k8s.io/v1 missing from /apis: {group}"
    );

    let (status, group_doc) = server.get("/apis/networking.k8s.io").await;
    assert_eq!(status.as_u16(), 200);
    assert!(
        group_doc["versions"]
            .as_array()
            .expect("versions")
            .iter()
            .any(|v| v["version"] == "v1"),
        "v1 missing from /apis/networking.k8s.io: {group_doc}"
    );

    let (status, resources) = server.get(GV).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(resources["groupVersion"], "networking.k8s.io/v1");
    resources["resources"]
        .as_array()
        .expect("resources")
        .clone()
}

fn verbs_of(resources: &[Value], name: &str) -> Vec<String> {
    resources
        .iter()
        .find(|r| r["name"] == name)
        .unwrap_or_else(|| panic!("{name} missing from {GV}"))["verbs"]
        .as_array()
        .expect("verbs")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

fn assert_verbs(resources: &[Value], name: &str, required: &[&str]) {
    let verbs = verbs_of(resources, name);
    for v in required {
        assert!(
            verbs.iter().any(|x| x == v),
            "{name} must advertise {v}, got {verbs:?}"
        );
    }
}

/// `framework.ConformanceIt("should support ... ")` opens a watch and only
/// checks that the call succeeds; the stream never ends, so read the status
/// line and drop the body.
async fn watch_opens(server: &TestApiServer, uri: &str) {
    let resp = server.respond("GET", uri, None, None).await;
    assert_eq!(resp.status().as_u16(), 200, "watch {uri} must open");
    // The stream is live: dropping it is the client closing the watch.
    let mut body = resp.into_body().into_data_stream();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(50), body.next()).await;
}

fn rv(obj: &Value) -> u64 {
    obj["metadata"]["resourceVersion"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no numeric resourceVersion: {obj}"))
}

/// [sig-network] ServiceCIDR and IPAddress API should support ServiceCIDR API
/// operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service_cidrs.go:166
#[tokio::test]
async fn servicecidr_api_operations() {
    let server = TestApiServer::new();

    // Description (service_cidrs.go:158-162): discovery and the verb sets.
    let resources = networking_v1_discovery(&server).await;
    assert_verbs(&resources, "servicecidrs", COLLECTION_VERBS);
    assert_verbs(&resources, "servicecidrs/status", &["update", "patch"]);

    // The default ServiceCIDR the api-server's controller maintains.
    DefaultServiceCIDRController::new(
        Arc::clone(&server.storage),
        vec!["10.96.0.0/12".to_string()],
    )
    .sync()
    .await
    .expect("default ServiceCIDR sync");

    // getting
    let (status, default_cidr) = server
        .get(&format!("{GV}/servicecidrs/{DEFAULT_SERVICE_CIDR_NAME}"))
        .await;
    assert_eq!(
        status.as_u16(),
        200,
        "get default ServiceCIDR: {default_cidr}"
    );

    // getting /status: the GVK is networking.k8s.io/v1 ServiceCIDR and the
    // UID is the object's.
    let (status, gotten_status) = server
        .get(&format!(
            "{GV}/servicecidrs/{DEFAULT_SERVICE_CIDR_NAME}/status"
        ))
        .await;
    assert_eq!(status.as_u16(), 200, "get status: {gotten_status}");
    assert_eq!(gotten_status["apiVersion"], "networking.k8s.io/v1");
    assert_eq!(gotten_status["kind"], "ServiceCIDR");
    assert_eq!(
        gotten_status["metadata"]["uid"], default_cidr["metadata"]["uid"],
        "status UID must be the object's"
    );

    // listing: at least the default one.
    let (status, list) = server.get(&format!("{GV}/servicecidrs")).await;
    assert_eq!(status.as_u16(), 200);
    assert!(
        !list["items"].as_array().expect("items").is_empty(),
        "expected at least the default ServiceCIDR: {list}"
    );

    // watching
    watch_opens(&server, &format!("{GV}/servicecidrs?watch=true")).await;
}

fn ip_address(name: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IPAddress",
        "metadata": {"name": name, "labels": {"type": kind}},
        "spec": {"parentRef": {
            "group": "mygroup",
            "resource": "myresources",
            "namespace": "foo",
            "name": "bar",
        }},
    })
}

/// [sig-network] ServiceCIDR and IPAddress API should support IPAddress API
/// operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service_cidrs.go:212
#[tokio::test]
async fn ipaddress_api_operations() {
    let server = TestApiServer::new();

    // Description (service_cidrs.go:205-208).
    let resources = networking_v1_discovery(&server).await;
    assert_verbs(&resources, "ipaddresses", COLLECTION_VERBS);

    let ip = ip_address("255.255.255.255", "broadcast");
    let ipv6 = ip_address("fe80::42:1dff:fe84:f9e2", "link-local");
    // Not upstream: must survive the label-selected deletecollection.
    let bystander = ip_address("192.0.2.10", "bystander");

    // creating
    let (status, created) = server.post(&format!("{GV}/ipaddresses"), &ip).await;
    assert_eq!(status.as_u16(), 201, "create IPv4 IPAddress: {created}");
    let created_rv = rv(&created);
    let (status, body) = server.post(&format!("{GV}/ipaddresses"), &ipv6).await;
    assert_eq!(status.as_u16(), 201, "create IPv6 IPAddress: {body}");
    let (status, body) = server.post(&format!("{GV}/ipaddresses"), &bystander).await;
    assert_eq!(status.as_u16(), 201, "create bystander: {body}");

    // patching
    let (status, patched) = server
        .patch(
            &format!("{GV}/ipaddresses/255.255.255.255"),
            &json!({"metadata": {"annotations": {"patched": "true"}}}),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "patch: {patched}");
    assert_eq!(patched["metadata"]["annotations"]["patched"], "true");
    assert!(
        rv(&patched) > created_rv,
        "patched object should have a larger resource version"
    );

    // updating
    let (status, mut to_update) = server
        .get(&format!("{GV}/ipaddresses/255.255.255.255"))
        .await;
    assert_eq!(status.as_u16(), 200);
    to_update["metadata"]["annotations"]["updated"] = json!("true");
    let (status, updated) = server
        .put(&format!("{GV}/ipaddresses/255.255.255.255"), &to_update)
        .await;
    assert_eq!(status.as_u16(), 200, "update: {updated}");
    assert_eq!(updated["metadata"]["annotations"]["updated"], "true");

    // getting
    let (status, _) = server
        .get(&format!("{GV}/ipaddresses/255.255.255.255"))
        .await;
    assert_eq!(status.as_u16(), 200);

    // listing by label: exactly one.
    let (status, list) = server
        .get(&format!("{GV}/ipaddresses?labelSelector=type%3Dbroadcast"))
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        list["items"].as_array().expect("items").len(),
        1,
        "expected one IPAddress: {list}"
    );

    // watching
    watch_opens(
        &server,
        &format!("{GV}/ipaddresses?watch=true&labelSelector=type%3Dbroadcast"),
    )
    .await;

    // deleting
    let (status, body) = server
        .delete(&format!("{GV}/ipaddresses/255.255.255.255"))
        .await;
    assert_eq!(status.as_u16(), 200, "delete: {body}");

    // deleting a collection
    let (status, body) = server
        .delete(&format!("{GV}/ipaddresses?labelSelector=type%3Dlink-local"))
        .await;
    assert_eq!(status.as_u16(), 200, "deletecollection: {body}");
    let (status, list) = server
        .get(&format!("{GV}/ipaddresses?labelSelector=type%3Dlink-local"))
        .await;
    assert_eq!(status.as_u16(), 200);
    assert!(
        list["items"].as_array().expect("items").is_empty(),
        "expected 0 IPAddresses: {list}"
    );

    // Not upstream: the selector bounded the collection delete.
    let (status, survivor) = server.get(&format!("{GV}/ipaddresses/192.0.2.10")).await;
    assert_eq!(status.as_u16(), 200, "bystander must survive: {survivor}");
}
