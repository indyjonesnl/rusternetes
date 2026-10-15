//! Mirror of the three API-operations `[sig-network] Services` conformance
//! cases (#3741, the rest of #1786), Kubernetes release-1.35:
//!
//! * `test/e2e/network/service.go:3220`
//!   "should find a service from listing all namespaces"
//! * `test/e2e/network/service.go:3246`
//!   "should complete a service status lifecycle"
//! * `test/e2e/network/service.go:3472`
//!   "should delete a collection of services"
//!
//! Each follows its case body step by step through the HTTP API. The
//! storage-level fragments in controller-manager's
//! `tests/it/conformance_network_services.rs` and the LoadBalancer controller
//! test cover controller behaviour; these cover the API verbs the cases
//! actually issue.
//!
//! The `default/kubernetes` Service the first case looks for is created by
//! upstream's kubernetesservice controller
//! (`pkg/controlplane/controller/kubernetesservice/controller.go:169`
//! `UpdateKubernetesService` -> `:212` `CreateOrUpdateMasterServiceIfNeeded`,
//! started from `pkg/controlplane/instance.go:349`). The in-process harness
//! runs no post-start hooks, so the test runs our port of it,
//! [`reconcile_kubernetes_service`], the way the api-server does at startup.

use std::time::Duration;

use futures::StreamExt;
use rusternetes_api_server::bootstrap::reconcile_kubernetes_service;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn rv_str(obj: &Value) -> String {
    obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("no resourceVersion: {obj}"))
        .to_string()
}

/// `watchtools.Until(ctx, rv, w, cond)`: open a watch on `uri` and return the
/// first event for which `cond` holds; panic after 5s, as upstream's context
/// deadline fails the case.
async fn watch_until(
    server: &TestApiServer,
    uri: &str,
    cond: impl Fn(&str, &Value) -> bool,
) -> Value {
    let resp = server.respond("GET", uri, None, None).await;
    assert_eq!(resp.status().as_u16(), 200, "watch {uri} must open");
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut seen = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                let Ok(ev) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let ty = ev["type"].as_str().unwrap_or_default().to_string();
                if cond(&ty, &ev["object"]) {
                    return Some(ev);
                }
                seen.push(ty);
            }
        }
        None
    })
    .await;
    match found {
        Ok(Some(ev)) => ev,
        _ => panic!("watch {uri}: no matching event; saw {seen:?}"),
    }
}

/// [sig-network] Services should find a service from listing all namespaces
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:3220
#[tokio::test]
async fn find_service_from_listing_all_namespaces() {
    let server = TestApiServer::new();
    reconcile_kubernetes_service(
        server.storage.as_ref(),
        6443,
        "10.96.0.1".parse().expect("ip"),
    )
    .await
    .expect("kubernetes Service");

    // fetching services (service.go:3221-3222)
    let (status, svcs) = server.get("/api/v1/services").await;
    assert_eq!(status.as_u16(), 200, "list all namespaces: {svcs}");

    // service.go:3224-3235
    assert!(
        svcs["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|s| s["metadata"]["name"] == "kubernetes"
                && s["metadata"]["namespace"] == "default"),
        "could not find service 'kubernetes' in service list in all namespaces: {svcs}"
    );
}

/// [sig-network] Services should complete a service status lifecycle
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:3246
#[tokio::test]
async fn service_status_lifecycle() {
    let server = TestApiServer::new();
    let ns = "services-status-e2e";
    let svc_uri = format!("/api/v1/namespaces/{ns}/services");
    let name = "test-service-e2e01";
    let sel = "test-service-static%3Dtrue";
    let named = |obj: &Value| obj["metadata"]["name"] == name && obj["metadata"]["namespace"] == ns;

    // service.go:3263-3264: the cluster-wide list every watch starts from.
    let (status, svc_list) = server
        .get(&format!("/api/v1/services?labelSelector={sel}"))
        .await;
    assert_eq!(status.as_u16(), 200);
    let watch_uri = format!(
        "{svc_uri}?watch=true&resourceVersion={}&labelSelector={sel}",
        rv_str(&svc_list)
    );

    // creating a Service (service.go:3266-3284)
    let test_service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": {"test-service-static": "true"}},
        "spec": {
            "type": "LoadBalancer",
            "ports": [{"name": "http", "protocol": "TCP", "port": 80, "targetPort": 80}],
            "loadBalancerClass": "example.com/internal-vip",
        },
    });
    let (status, body) = server.post(&svc_uri, &test_service).await;
    assert_eq!(status.as_u16(), 201, "create Service: {body}");

    // watching for the Service to be added (service.go:3286-3304)
    watch_until(&server, &watch_uri, |_, obj| {
        named(obj) && obj["metadata"]["labels"]["test-service-static"] == "true"
    })
    .await;

    // Getting /status (service.go:3307-3316)
    let (status, svc_status) = server.get(&format!("{svc_uri}/{name}/status")).await;
    assert_eq!(status.as_u16(), 200, "get /status: {svc_status}");
    assert_eq!(svc_status["kind"], "Service");
    assert_eq!(svc_status["metadata"]["name"], name);

    // patching the ServiceStatus (service.go:3318-3326): a merge patch to
    // /status carrying an annotation and the LoadBalancer ingress.
    let (status, body) = server
        .patch(
            &format!("{svc_uri}/{name}/status"),
            &json!({
                "metadata": {"annotations": {"patchedstatus": "true"}},
                "status": {"loadBalancer": {"ingress": [{"ip": "203.0.113.1"}]}},
            }),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "patch /status: {body}");

    // watching for the Service to be patched (service.go:3329-3348)
    let ev = watch_until(&server, &watch_uri, |_, obj| {
        named(obj) && obj["metadata"]["annotations"]["patchedstatus"] == "true"
    })
    .await;
    assert_eq!(
        ev["object"]["status"]["loadBalancer"]["ingress"][0]["ip"], "203.0.113.1",
        "status patch must land: {ev}"
    );

    // updating the ServiceStatus (service.go:3351-3368): append a condition
    // and UpdateStatus. A zero metav1.Time marshals as null.
    let (status, mut to_update) = server.get(&format!("{svc_uri}/{name}")).await;
    assert_eq!(status.as_u16(), 200);
    let condition = json!({
        "type": "StatusUpdate",
        "status": "True",
        "lastTransitionTime": null,
        "reason": "E2E",
        "message": "Set from e2e test",
    });
    match to_update["status"]["conditions"].as_array_mut() {
        Some(c) => c.push(condition),
        None => to_update["status"]["conditions"] = json!([condition]),
    }
    let (status, updated) = server
        .put(&format!("{svc_uri}/{name}/status"), &to_update)
        .await;
    assert_eq!(status.as_u16(), 200, "UpdateStatus: {updated}");

    // watching for the Service to be updated (service.go:3371-3397)
    watch_until(&server, &watch_uri, |_, obj| {
        named(obj)
            && obj["metadata"]["annotations"]["patchedstatus"] == "true"
            && obj["status"]["conditions"].as_array().is_some_and(|c| {
                c.iter().any(|c| {
                    c["type"] == "StatusUpdate"
                        && c["reason"] == "E2E"
                        && c["message"] == "Set from e2e test"
                })
            })
    })
    .await;

    // patching the service (service.go:3400-3410): a strategic merge patch of
    // the json.Marshal of a Service carrying only labels.
    let (status, body) = server
        .send(
            "PATCH",
            &format!("{svc_uri}/{name}"),
            Some("application/strategic-merge-patch+json"),
            Some(&json!({
                "metadata": {"labels": {"test-service": "patched"}},
                "spec": {},
                "status": {"loadBalancer": {}},
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "patch Service: {body}");

    // watching for the Service to be patched (service.go:3412-3430)
    watch_until(&server, &watch_uri, |_, obj| {
        named(obj) && obj["metadata"]["labels"]["test-service"] == "patched"
    })
    .await;

    // deleting the service (service.go:3433-3435)
    let (status, body) = server.delete(&format!("{svc_uri}/{name}")).await;
    assert_eq!(status.as_u16(), 200, "delete Service: {body}");

    // watching for the Service to be deleted (service.go:3437-3459)
    watch_until(&server, &watch_uri, |ty, obj| {
        ty == "DELETED" && named(obj) && obj["metadata"]["labels"]["test-service-static"] == "true"
    })
    .await;
}

/// [sig-network] Services should delete a collection of services
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:3472
///
/// Upstream's third Service (`e2e-test-service=keep`) is already the
/// bystander the label-selected delete must spare.
#[tokio::test]
async fn delete_collection_of_services() {
    let server = TestApiServer::new();
    let ns = "services-dc-e2e";
    let svc_uri = format!("/api/v1/namespaces/{ns}/services");

    // creating a collection of services (service.go:3482-3529)
    for (name, label, port) in [
        ("e2e-svc-a-aaaaa", "delete", 8001),
        ("e2e-svc-b-bbbbb", "delete", 8002),
        ("e2e-svc-c-ccccc", "keep", 8003),
    ] {
        let svc = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "labels": {"e2e-test-service": label}},
            "spec": {
                "type": "ClusterIP",
                "ports": [{"name": "http", "protocol": "TCP", "port": port, "targetPort": port}],
            },
        });
        let (status, body) = server.post(&svc_uri, &svc).await;
        assert_eq!(status.as_u16(), 201, "create {name}: {body}");
    }

    // service.go:3531-3533
    let (status, list) = server.get(&svc_uri).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        list["items"].as_array().expect("items").len(),
        3,
        "Required count of services out of sync: {list}"
    );

    // deleting service collection (service.go:3535-3537)
    let (status, body) = server
        .delete(&format!(
            "{svc_uri}?labelSelector=e2e-test-service%3Ddelete"
        ))
        .await;
    assert_eq!(status.as_u16(), 200, "deletecollection: {body}");

    // service.go:3539-3541
    let (status, list) = server.get(&svc_uri).await;
    assert_eq!(status.as_u16(), 200);
    let items = list["items"].as_array().expect("items");
    assert_eq!(
        items.len(),
        1,
        "Required count of services out of sync: {list}"
    );
    assert_eq!(items[0]["metadata"]["name"], "e2e-svc-c-ccccc");
}
