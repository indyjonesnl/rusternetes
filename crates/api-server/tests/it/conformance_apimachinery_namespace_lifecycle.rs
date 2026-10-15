//! Mirrors of the four `Namespaces` conformance cases that had no mirror at
//! all (#1770).
//!
//! Upstream (`release-1.35`): `test/e2e/apimachinery/namespace.go`
//!
//! | case | line |
//! |---|---|
//! | should ensure that all pods are removed when a namespace is deleted | :247 |
//! | should ensure that all services are removed when a namespace is deleted | :256 |
//! | should apply changes to a namespace status | :309 |
//! | should apply an update to a Namespace | :376 |
//!
//! The two deletion cases are controller-owned: the namespace only disappears
//! once the namespace controller has deleted its content and drained the
//! `kubernetes` finalizer. The mirror drives the real `NamespaceController`
//! (and the `ServiceAccountController`, for upstream's
//! `WaitForDefaultServiceAccountInNamespace`) over `ApiStorage` against a
//! served `TestApiServer`, so every controller write goes through the HTTP
//! surface the way it does in a cluster — the same shape as
//! `namespace_controller_api_finalize_test.rs`. Upstream's `wait.Poll` loops
//! become a bounded number of reconcile passes.

use std::sync::Arc;

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controllers::namespace::NamespaceController;
use rusternetes_controller_manager::controllers::serviceaccount::ServiceAccountController;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// A `TestApiServer` served on a loopback port, plus `ApiStorage` pointed at
/// it for the controllers.
struct Served {
    api: TestApiServer,
    storage: Arc<ApiStorage>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve() -> Served {
    // NamespaceLifecycle must be observable: a namespace that has been
    // deleted must not be silently re-created by the harness.
    let api = TestApiServer::builder().strict_namespaces().build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api-server");
    let address = listener.local_addr().expect("test api-server address");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });
    let client = Arc::new(
        ApiClient::new(&format!("http://{address}"), true, None).expect("build api-server client"),
    );
    Served {
        api,
        storage: Arc::new(ApiStorage::new(client)),
        server,
    }
}

async fn send(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some("application/json"), body).await;
    (status.as_u16(), value)
}

/// `f.CreateNamespace` (test/e2e/framework/framework.go): the framework
/// labels every namespace it creates with `e2e-framework` / `e2e-run`.
async fn create_namespace(api: &TestApiServer, name: &str) -> Value {
    let (s, ns) = send(
        api,
        "POST",
        "/api/v1/namespaces",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "labels": { "e2e-framework": "namespaces", "e2e-run": "mirror" }
            }
        })),
    )
    .await;
    assert_eq!(s, 201, "create namespace {name}: {ns}");
    ns
}

/// `framework.WaitForDefaultServiceAccountInNamespace`
/// (test/e2e/framework/util.go:310): the `default` ServiceAccount is
/// provisioned by the serviceaccounts controller.
async fn wait_for_default_service_account(served: &Served, ns: &str) {
    ServiceAccountController::new(served.storage.clone())
        .reconcile_all()
        .await
        .expect("serviceaccount reconcile");
    let (s, sa) = send(
        &served.api,
        "GET",
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/default"),
        None,
    )
    .await;
    assert_eq!(s, 200, "default ServiceAccount in {ns}: {sa}");
}

/// "Waiting for the namespace to be removed." — upstream polls `Get` until
/// NotFound. Each poll here is one namespace-controller pass; `between` runs
/// after each pass that leaves the namespace in place (the kubelet's half of
/// a graceful pod delete, for the pod case).
async fn wait_for_namespace_removed<F, Fut>(served: &Served, ns: &str, mut between: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let controller = NamespaceController::new(served.storage.clone());
    for _ in 0..10 {
        controller
            .reconcile_all()
            .await
            .expect("namespace reconcile");
        let (s, _) = send(
            &served.api,
            "GET",
            &format!("/api/v1/namespaces/{ns}"),
            None,
        )
        .await;
        if s == 404 {
            return;
        }
        between().await;
    }
    let (_, body) = send(
        &served.api,
        "GET",
        &format!("/api/v1/namespaces/{ns}"),
        None,
    )
    .await;
    panic!("namespace {ns} was never removed: {body}");
}

/// namespace.go:247 — should ensure that all pods are removed when a
/// namespace is deleted (`ensurePodsAreRemovedWhenNamespaceIsDeleted`,
/// :96-149).
#[tokio::test]
async fn namespace_deletion_removes_all_pods() {
    let served = serve().await;
    let api = &served.api;
    let ns = "nsdeletetest";

    // "Creating a test namespace" (:97-100).
    create_namespace(api, ns).await;
    // "Waiting for a default service account to be provisioned" (:102-104).
    wait_for_default_service_account(&served, ns).await;

    // "Creating a pod in the namespace" (:106-122).
    let pods = format!("/api/v1/namespaces/{ns}/pods");
    let (s, pod) = send(
        api,
        "POST",
        &pods,
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "test-pod" },
            "spec": {
                "containers": [{ "name": "nginx", "image": "registry.k8s.io/pause:3.10.1" }]
            }
        })),
    )
    .await;
    assert_eq!(s, 201, "create pod: {pod}");
    // The api-server defaults terminationGracePeriodSeconds, which upstream
    // dereferences for its wait budget (:132).
    assert_eq!(
        pod["spec"]["terminationGracePeriodSeconds"],
        json!(30),
        "terminationGracePeriodSeconds must be defaulted: {pod}"
    );

    // "Waiting for the pod to have running status" (:124-125). No kubelet
    // runs in-process, so play the scheduler (bind) and the kubelet (status
    // Running) through the API they use.
    let (s, b) = send(
        api,
        "POST",
        &format!("{pods}/test-pod/binding"),
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Binding",
            "metadata": { "name": "test-pod" },
            "target": { "apiVersion": "v1", "kind": "Node", "name": "node-1" }
        })),
    )
    .await;
    assert_eq!(s, 201, "bind pod: {b}");
    let (s, mut running) = send(api, "GET", &format!("{pods}/test-pod"), None).await;
    assert_eq!(s, 200, "get pod: {running}");
    running["status"]["phase"] = json!("Running");
    let (s, b) = send(
        api,
        "PUT",
        &format!("{pods}/test-pod/status"),
        Some(&running),
    )
    .await;
    assert_eq!(s, 200, "set pod Running: {b}");
    assert_eq!(b["status"]["phase"], "Running", "pod phase: {b}");

    // "Deleting the namespace" (:127-129).
    let (s, b) = send(api, "DELETE", &format!("/api/v1/namespaces/{ns}"), None).await;
    assert!((200..300).contains(&s), "delete namespace {ns}: {s} {b}");

    // "Waiting for the namespace to be removed." (:131-140). The namespace
    // controller's delete of a bound pod is graceful; in a cluster the
    // kubelet then kills the containers and issues the final
    // gracePeriodSeconds=0 delete with a UID precondition
    // (pkg/kubelet/status/status_manager.go:1091-1098, `canBeDeleted` →
    // `Delete(... GracePeriodSeconds: 0, Preconditions: UID)`). Play that
    // part once the pod is marked for deletion.
    let pod_uri = format!("{pods}/test-pod");
    wait_for_namespace_removed(&served, ns, || {
        let pod_uri = pod_uri.clone();
        async move {
            let (s, p) = send(api, "GET", &pod_uri, None).await;
            if s == 200 && !p["metadata"]["deletionTimestamp"].is_null() {
                let mut opts = json!({
                    "apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 0
                });
                if let Some(uid) = p["metadata"]["uid"].as_str() {
                    opts["preconditions"] = json!({ "uid": uid });
                }
                let (s, b) = send(api, "DELETE", &pod_uri, Some(&opts)).await;
                assert!(s == 200 || s == 404, "kubelet final pod delete: {s} {b}");
            }
        }
    })
    .await;

    // "Recreating the namespace" (:142-144).
    create_namespace(api, ns).await;

    // "Verifying there are no pods in the namespace" (:146-148).
    let (s, b) = send(api, "GET", &pod_uri, None).await;
    assert_eq!(s, 404, "pod must not survive namespace deletion: {b}");
}

/// namespace.go:256 — should ensure that all services are removed when a
/// namespace is deleted (`ensureServicesAreRemovedWhenNamespaceIsDeleted`,
/// :151-206).
#[tokio::test]
async fn namespace_deletion_removes_all_services() {
    let served = serve().await;
    let api = &served.api;
    let ns = "nsdeletetest";

    // "Creating a test namespace" (:154-157).
    create_namespace(api, ns).await;
    // "Waiting for a default service account to be provisioned" (:159-161).
    wait_for_default_service_account(&served, ns).await;

    // "Creating a service in the namespace" (:163-182).
    let svc_uri = format!("/api/v1/namespaces/{ns}/services/test-service");
    let (s, b) = send(
        api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/services"),
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": "test-service" },
            "spec": {
                "selector": { "foo": "bar", "baz": "blah" },
                "ports": [{ "port": 80, "targetPort": 80 }]
            }
        })),
    )
    .await;
    assert_eq!(s, 201, "create service: {b}");

    // "Deleting the namespace" (:184-186).
    let (s, b) = send(api, "DELETE", &format!("/api/v1/namespaces/{ns}"), None).await;
    assert!((200..300).contains(&s), "delete namespace {ns}: {s} {b}");

    // "Waiting for the namespace to be removed." (:188-197).
    wait_for_namespace_removed(&served, ns, || async {}).await;

    // "Recreating the namespace" (:199-201).
    create_namespace(api, ns).await;

    // "Verifying there is no service in the namespace" (:203-205).
    let (s, b) = send(api, "GET", &svc_uri, None).await;
    assert_eq!(s, 404, "service must not survive namespace deletion: {b}");
}

/// namespace.go:309 — should apply changes to a namespace status.
#[tokio::test]
async fn namespace_status_patch_and_update_apply() {
    let api = TestApiServer::new();
    let ns = "namespaces-status";
    create_namespace(&api, ns).await;
    let status_uri = format!("/api/v1/namespaces/{ns}/status");

    // "Read namespace status" (:315-322): phase MUST be Active.
    let (s, current) = send(&api, "GET", &status_uri, None).await;
    assert_eq!(s, 200, "get namespace status: {current}");
    assert_eq!(
        current["status"]["phase"], "Active",
        "The phase returned was {}",
        current["status"]["phase"]
    );

    // "Patch namespace status" (:324-342): a merge patch carrying both an
    // annotation and a condition.
    let patch = json!({
        "metadata": { "annotations": { "e2e-patched-ns-status": ns } },
        "status": { "conditions": [{
            "type": "StatusPatch",
            "status": "True",
            "reason": "E2E",
            "message": "Patched by an e2e test"
        }]}
    });
    let (s, patched) = api
        .send(
            "PATCH",
            &status_uri,
            Some("application/merge-patch+json"),
            Some(&patch),
        )
        .await;
    assert_eq!(s.as_u16(), 200, "patch namespace status: {patched}");
    assert_eq!(
        patched["metadata"]["annotations"]["e2e-patched-ns-status"], ns,
        "patched object should have the applied annotation: {patched}"
    );
    let conds = patched["status"]["conditions"]
        .as_array()
        .unwrap_or_else(|| panic!("patched status has no conditions: {patched}"));
    let last = conds.last().expect("at least one condition");
    assert_eq!(last["reason"], "E2E", "The Reason returned was {last}");
    assert_eq!(
        last["message"], "Patched by an e2e test",
        "The Message returned was {last}"
    );

    // "Update namespace status" (:344-367): read /status, append a
    // condition, PUT /status.
    let (s, mut to_update) = send(&api, "GET", &status_uri, None).await;
    assert_eq!(s, 200, "get namespace status: {to_update}");
    let mut conditions = to_update["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    conditions.push(json!({
        "type": "StatusUpdate",
        "status": "True",
        "reason": "E2E",
        "message": "Updated by an e2e test"
    }));
    to_update["status"]["conditions"] = Value::Array(conditions);
    let (s, updated) = send(&api, "PUT", &status_uri, Some(&to_update)).await;
    assert_eq!(s, 200, "update namespace status: {updated}");
    let conds = updated["status"]["conditions"]
        .as_array()
        .unwrap_or_else(|| panic!("updated status has no conditions: {updated}"));
    let last = conds.last().expect("at least one condition");
    assert_eq!(
        last["type"], "StatusUpdate",
        "updated object should have the approved condition, got {conds:?}"
    );
    assert_eq!(
        last["message"], "Updated by an e2e test",
        "The Message returned was {last}"
    );
}

/// namespace.go:376 — should apply an update to a Namespace.
#[tokio::test]
async fn namespace_update_applies_label() {
    let api = TestApiServer::new();
    let ns = "namespaces-update";
    create_namespace(&api, ns).await;
    let uri = format!("/api/v1/namespaces/{ns}");

    // "Updating Namespace" (:382-391): GET, set labels[ns] = "updated", PUT.
    let (s, mut namespace) = send(&api, "GET", &uri, None).await;
    assert_eq!(s, 200, "get namespace: {namespace}");
    namespace["metadata"]["labels"][ns] = json!("updated");
    let (s, updated) = send(&api, "PUT", &uri, Some(&namespace)).await;
    assert_eq!(s, 200, "update namespace: {updated}");
    assert_eq!(
        updated["metadata"]["labels"][ns], "updated",
        "Failed to update namespace {ns}. Current Labels: {}",
        updated["metadata"]["labels"]
    );

    // The returned object is what upstream asserts; a re-read confirms the
    // update was persisted, not only echoed.
    let (s, reread) = send(&api, "GET", &uri, None).await;
    assert_eq!(s, 200, "re-get namespace: {reread}");
    assert_eq!(reread["metadata"]["labels"][ns], "updated", "{reread}");
}
