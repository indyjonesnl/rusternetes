//! Mirrors of `[sig-apps]` conformance cases (#3768), slice 2.
//!
//! Upstream (`release-1.35`):
//! - `test/e2e/apps/daemon_set.go:856` "should list and delete a collection of
//!   DaemonSets" and `:895` "should verify changes to a daemon set status".
//! - `test/e2e/apps/deployment.go:492` "should validate Deployment Status
//!   endpoints".
//! - `test/e2e/apps/statefulset.go:1118` "should validate Statefulset Status
//!   endpoints".
//! - `test/e2e/apps/job.go:1066` "should apply changes to a job status" and
//!   `:1160` "should manage the lifecycle of a job".
//! - `test/e2e/apps/cronjob.go:368` "should support CronJob API operations".
//! - `test/e2e/common/node/pods.go:853` "should delete a collection of pods"
//!   and `:1091` "should patch a pod status".
//!
//! The mirror drives the api-server's router in the order the upstream body
//! issues its client calls. Where upstream waits for a controller or the
//! kubelet (`checkRunningOnAllNodes`, `WaitForDeploymentComplete`,
//! `WaitForRunningAndReady`, `WaitForJobComplete`, ...), the mirror performs
//! that component's write itself through the endpoint the component uses and
//! says so at the step. `watchtools.Until` loops are mirrored by opening the
//! label-selected watch at the list's resourceVersion (as `w`/`ListWatch` do)
//! and reading events until the same predicate holds. Random `utilrand`
//! suffixes are fixed.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    ct: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some(ct), body).await;
    (status.as_u16(), value)
}

async fn get(api: &TestApiServer, uri: &str) -> (u16, Value) {
    send(api, "GET", uri, "application/json", None).await
}

/// `apimachineryutils.HaveValidResourceVersion`
/// (test/e2e/common/apimachinery/resourceversion_matchers.go:39-54).
fn resource_version(obj: &Value) -> u128 {
    let rv = obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("object has no resourceVersion: {obj}"));
    rv.parse::<u128>()
        .unwrap_or_else(|_| panic!("resourceVersion {rv:?} is not a valid integer"))
}

fn items(list: &Value) -> &Vec<Value> {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
}

/// Open `GET <uri>?watch=true&resourceVersion=<rv>&labelSelector=<sel>`.
async fn open_watch(
    api: &TestApiServer,
    collection: &str,
    rv: &str,
    selector: &str,
) -> axum::response::Response {
    let resp = api
        .respond(
            "GET",
            &format!("{collection}?watch=true&resourceVersion={rv}&labelSelector={selector}"),
            None,
            None,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "watch {collection}");
    resp
}

/// `watchtools.Until`: read watch events until `pred(type, object)` is true.
async fn watch_until(
    what: &str,
    watch: &mut std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Send>,
    >,
    buf: &mut String,
    pred: impl Fn(&str, &Value) -> bool,
) {
    let found = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(chunk) = watch.next().await {
            let bytes = chunk.unwrap_or_else(|e| panic!("{what}: watch errored: {e}"));
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if line.trim().is_empty() {
                    continue;
                }
                let evt: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("{what}: bad watch line {line:?}: {e}"));
                if pred(evt["type"].as_str().unwrap_or(""), &evt["object"]) {
                    return true;
                }
            }
        }
        false
    })
    .await;
    assert!(matches!(found, Ok(true)), "{what}: event not observed");
}

fn into_stream(
    resp: axum::response::Response,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Send>> {
    Box::pin(resp.into_body().into_data_stream())
}

fn has_condition(obj: &Value, f: impl Fn(&Value) -> bool) -> bool {
    obj["status"]["conditions"]
        .as_array()
        .map(|c| c.iter().any(f))
        .unwrap_or(false)
}

fn is_status_update(c: &Value) -> bool {
    c["type"] == "StatusUpdate" && c["reason"] == "E2E" && c["message"] == "Set from e2e test"
}

fn list_rv(list: &Value) -> String {
    list["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or("0")
        .to_string()
}

/// The shared tail of the DaemonSet/Deployment/StatefulSet status cases:
/// "Getting /status", "updating ... Status" (Get + UpdateStatus with a
/// StatusUpdate condition appended), watch for it, "patching ... Status" (merge
/// patch on /status with StatusPatched), watch for it.
async fn status_endpoints_flow(
    api: &TestApiServer,
    kind_label: &str,
    one: &str,
    collection: &str,
    selector: &str,
    rv: &str,
    expected_uid: &str,
) {
    let watch = open_watch(api, collection, rv, selector).await;
    let mut stream = into_stream(watch);
    let mut buf = String::new();

    // "Getting /status": the dynamic client Get with subresource "status".
    let (s, status_obj) = get(api, &format!("{one}/status")).await;
    assert_eq!(s, 200, "{kind_label} get /status: {status_obj}");
    assert_eq!(status_obj["metadata"]["uid"], expected_uid, "{status_obj}");

    // "updating ... Status": Get, append condition, UpdateStatus.
    let (s, mut to_update) = get(api, one).await;
    assert_eq!(s, 200, "{kind_label} get: {to_update}");
    let mut conds = to_update["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    conds.push(json!({
        "type": "StatusUpdate", "status": "True",
        "reason": "E2E", "message": "Set from e2e test",
    }));
    to_update["status"]["conditions"] = json!(conds);
    let (s, updated) = send(
        api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&to_update),
    )
    .await;
    assert_eq!(s, 200, "{kind_label} UpdateStatus: {updated}");
    assert!(
        has_condition(&updated, is_status_update),
        "updated status lost the condition: {updated}"
    );

    // "watching for the ... status to be updated".
    watch_until(
        &format!("{kind_label} status updated"),
        &mut stream,
        &mut buf,
        |_, o| has_condition(o, is_status_update),
    )
    .await;

    // "patching the ... Status": merge patch on /status.
    let (s, patched) = send(
        api,
        "PATCH",
        &format!("{one}/status"),
        "application/merge-patch+json",
        Some(&json!({ "status": { "conditions": [
            { "type": "StatusPatched", "status": "True" }
        ]}})),
    )
    .await;
    assert_eq!(s, 200, "{kind_label} patch /status: {patched}");

    // "watching for the ... status to be patched".
    watch_until(
        &format!("{kind_label} status patched"),
        &mut stream,
        &mut buf,
        |_, o| has_condition(o, |c| c["type"] == "StatusPatched"),
    )
    .await;
}

fn new_daemonset(name: &str, labels: &Value) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": { "name": name, "namespace": NS, "labels": labels },
        "spec": {
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "app",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                        "ports": [{ "containerPort": 9376 }],
                    }],
                },
            },
        },
    })
}

/// The DaemonSet controller's status write after daemon pods are running on
/// every node (`checkRunningOnAllNodes` + `CheckDaemonStatus`), through
/// /status, as daemon_controller.go updateDaemonSetStatus does.
async fn daemonset_controller_status(api: &TestApiServer, one: &str, created: &Value) {
    let mut ds = created.clone();
    ds["status"] = json!({
        "currentNumberScheduled": 1, "numberMisscheduled": 0,
        "desiredNumberScheduled": 1, "numberReady": 1,
        "observedGeneration": 1, "updatedNumberScheduled": 1, "numberAvailable": 1,
    });
    let (s, body) = send(
        api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&ds),
    )
    .await;
    assert_eq!(s, 200, "daemonset controller status write: {body}");
}

/// daemon_set.go:856 "should list and delete a collection of DaemonSets".
#[tokio::test]
async fn conformance_daemonset_list_delete_collection() {
    let api = TestApiServer::new();
    let base = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let one = format!("{base}/daemon-set");
    let labels = json!({ "daemonset-name": "daemon-set" });
    let sel = "daemonset-name%3Ddaemon-set";

    // :862-864 Creating simple DaemonSet.
    let (s, created) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_daemonset("daemon-set", &labels)),
    )
    .await;
    assert_eq!(s, 201, "create daemonset: {created}");

    // :866-870 daemon pods launch on every node / CheckDaemonStatus: the
    // controller's status write.
    daemonset_controller_status(&api, &one, &created).await;

    // :872-875 listing all DaemonSets (all namespaces) -> exactly 1.
    let (s, list) = get(
        &api,
        &format!("/apis/apps/v1/daemonsets?labelSelector={sel}"),
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list).len(), 1, "filtered list wasn't found: {list}");

    // :877-879 DeleteCollection with GracePeriodSeconds=1 and the selector.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{base}?labelSelector={sel}"),
        "application/json",
        Some(&json!({ "gracePeriodSeconds": 1 })),
    )
    .await;
    assert_eq!(s, 200, "failed to delete DaemonSets: {body}");

    // :881-884 the filtered list MUST be empty.
    let (s, list) = get(
        &api,
        &format!("/apis/apps/v1/daemonsets?labelSelector={sel}"),
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert!(
        items(&list).is_empty(),
        "filtered list should have no daemonset: {list}"
    );
}

/// daemon_set.go:895 "should verify changes to a daemon set status".
#[tokio::test]
async fn conformance_daemonset_status() {
    let api = TestApiServer::new();
    let base = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let one = format!("{base}/daemon-set");
    let labels = json!({ "daemonset-name": "daemon-set" });
    let sel = "daemonset-name%3Ddaemon-set";

    // :911-912 list across namespaces (its resourceVersion seeds the watch).
    let (s, list) = get(
        &api,
        &format!("/apis/apps/v1/daemonsets?labelSelector={sel}"),
    )
    .await;
    assert_eq!(s, 200, "{list}");

    // :914-916 create.
    let (s, created) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_daemonset("daemon-set", &labels)),
    )
    .await;
    assert_eq!(s, 201, "create daemonset: {created}");

    // :918-922 daemon pods running: the controller's status write.
    daemonset_controller_status(&api, &one, &created).await;

    // :924-1029 /status get, UpdateStatus, patch, with their watches.
    let uid = created["metadata"]["uid"].as_str().unwrap().to_string();
    status_endpoints_flow(&api, "daemonset", &one, &base, sel, &list_rv(&list), &uid).await;
}

fn new_deployment(name: &str, labels: &Value) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": name, "namespace": NS, "labels": labels },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": labels },
            "strategy": { "type": "RollingUpdate" },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    }],
                },
            },
        },
    })
}

/// deployment.go:492 "should validate Deployment Status endpoints".
#[tokio::test]
async fn conformance_deployment_status_endpoints() {
    let api = TestApiServer::new();
    let base = format!("/apis/apps/v1/namespaces/{NS}/deployments");
    let one = format!("{base}/test-deployment");
    let labels = json!({ "name": "agnhost", "e2e": "testing" });
    let sel = "e2e%3Dtesting";

    // :501-505 list across namespaces.
    let (s, list) = get(
        &api,
        &format!("/apis/apps/v1/deployments?labelSelector={sel}"),
    )
    .await;
    assert_eq!(s, 200, "{list}");

    // :507-515 create.
    let (s, created) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_deployment("test-deployment", &labels)),
    )
    .await;
    assert_eq!(s, 201, "create deployment: {created}");

    // :517-522 WaitForDeploymentRevisionAndImage / WaitForDeploymentComplete:
    // the deployment controller's /status write (sync.go syncRolloutStatus).
    let mut with_status = created.clone();
    with_status["status"] = json!({
        "observedGeneration": 1, "replicas": 1, "updatedReplicas": 1,
        "readyReplicas": 1, "availableReplicas": 1,
        "conditions": [{
            "type": "Available", "status": "True",
            "reason": "MinimumReplicasAvailable",
            "message": "Deployment has minimum availability.",
        }],
    });
    let (s, body) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&with_status),
    )
    .await;
    assert_eq!(s, 200, "controller status write: {body}");

    // :524-525 Get, then :527-1.. /status flow with watches.
    let uid = created["metadata"]["uid"].as_str().unwrap().to_string();
    status_endpoints_flow(&api, "deployment", &one, &base, sel, &list_rv(&list), &uid).await;
}

/// statefulset.go:1118 "should validate Statefulset Status endpoints".
#[tokio::test]
async fn conformance_statefulset_status_endpoints() {
    let api = TestApiServer::new();
    let base = format!("/apis/apps/v1/namespaces/{NS}/statefulsets");
    let one = format!("{base}/ss");
    let labels = json!({ "foo": "bar", "baz": "blah" });
    let sel = "e2e%3Dtesting";

    // :1127-1128 list across namespaces.
    let (s, list) = get(
        &api,
        &format!("/apis/apps/v1/statefulsets?labelSelector={sel}"),
    )
    .await;
    assert_eq!(s, 200, "{list}");

    // :1130-1133 NewStatefulSet(ssName, ns, headlessSvcName, 1, ...) +
    // setHTTPProbe; create.
    let ss = json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": { "name": "ss", "namespace": NS, "labels": labels },
        "spec": {
            "replicas": 1, "serviceName": "test",
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": { "labels": labels },
                "spec": { "containers": [{
                    "name": "webserver",
                    "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                    "readinessProbe": { "httpGet": { "path": "/index.html", "port": 80 } },
                }]},
            },
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": { "type": "RollingUpdate" },
        },
    });
    let (s, created) = send(&api, "POST", &base, "application/json", Some(&ss)).await;
    assert_eq!(s, 201, "create statefulset: {created}");

    // :1135-1136 WaitForRunningAndReady / waitForStatus: the StatefulSet
    // controller's /status write.
    let mut with_status = created.clone();
    with_status["status"] = json!({ "replicas": 1, "readyReplicas": 1, "availableReplicas": 1 });
    let (s, body) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&with_status),
    )
    .await;
    assert_eq!(s, 200, "controller status write: {body}");

    // :1138-1141 patch the label e2e=testing (strategic merge).
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { "e2e": "testing" } } })),
    )
    .await;
    assert_eq!(s, 200, "patch label: {patched}");

    // :1143-1.. /status flow with watches.
    let uid = created["metadata"]["uid"].as_str().unwrap().to_string();
    status_endpoints_flow(&api, "statefulset", &one, &base, sel, &list_rv(&list), &uid).await;
}

fn new_job(name: &str, labels: Option<&Value>, suspend: Option<bool>) -> Value {
    let mut job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": { "name": name, "namespace": NS },
        "spec": {
            "parallelism": 2, "completions": 4, "backoffLimit": 6,
            "template": { "spec": {
                "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 0,
                "containers": [{
                    "name": "c", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                    "command": ["/bin/sh", "-c", "exit 0"],
                }],
            }},
        },
    });
    if let Some(l) = labels {
        job["metadata"]["labels"] = l.clone();
    }
    if let Some(s) = suspend {
        job["spec"]["suspend"] = json!(s);
    }
    job
}

/// job.go:1066 "should apply changes to a job status".
#[tokio::test]
async fn conformance_job_status() {
    let api = TestApiServer::new();
    let base = format!("/apis/batch/v1/namespaces/{NS}/jobs");
    let one = format!("{base}/suspend-false-to-true");

    // :1074-1078 Creating a job (parallelism 2, completions 4, backoff 6).
    let (s, job) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_job("suspend-false-to-true", None, None)),
    )
    .await;
    assert_eq!(s, 201, "create job: {job}");

    // :1080-1082 WaitForJobPodsRunning: kubelet/controller driven, no REST
    // write on the Job itself to mirror.

    // :1084-1113 patching /status: merge patch with annotation + custom
    // condition carrying an RFC3339 lastTransitionTime.
    let now1 = "2026-10-15T12:00:00Z";
    let (s, patched) = send(
        &api,
        "PATCH",
        &format!("{one}/status"),
        "application/merge-patch+json",
        Some(&json!({
            "metadata": { "annotations": { "patchedstatus": "true" } },
            "status": { "conditions": [{
                "type": "CustomConditionType", "status": "True",
                "lastTransitionTime": now1,
            }]},
        })),
    )
    .await;
    assert_eq!(s, 200, "patch /status: {patched}");
    let cond = patched["status"]["conditions"]
        .as_array()
        .and_then(|c| c.iter().find(|c| c["type"] == "CustomConditionType"))
        .unwrap_or_else(|| panic!("patched object lacks the condition: {patched}"));
    assert_eq!(cond["lastTransitionTime"], now1, "{patched}");
    assert_eq!(patched["metadata"]["annotations"]["patchedstatus"], "true");

    // :1115-1141 updating /status: Get, change lastTransitionTime, UpdateStatus.
    let now2 = "2026-10-15T12:00:01Z";
    let (s, mut to_update) = get(&api, &one).await;
    assert_eq!(s, 200, "{to_update}");
    let conds = to_update["status"]["conditions"]
        .as_array_mut()
        .unwrap_or_else(|| panic!("patched object does not have conditions"));
    let c = conds
        .iter_mut()
        .find(|c| c["type"] == "CustomConditionType")
        .expect("patched object does not have the required condition");
    c["lastTransitionTime"] = json!(now2);
    let (s, updated) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&to_update),
    )
    .await;
    assert_eq!(s, 200, "UpdateStatus: {updated}");
    let cond = updated["status"]["conditions"]
        .as_array()
        .and_then(|c| c.iter().find(|c| c["type"] == "CustomConditionType"))
        .unwrap_or_else(|| panic!("updated object lacks the condition: {updated}"));
    assert_eq!(cond["lastTransitionTime"], now2, "{updated}");

    // :1143-1150 get /status: UID MUST match.
    let (s, status) = get(&api, &format!("{one}/status")).await;
    assert_eq!(s, 200, "{status}");
    assert_eq!(
        status["metadata"]["uid"], job["metadata"]["uid"],
        "{status}"
    );
}

/// job.go:1160 "should manage the lifecycle of a job".
#[tokio::test]
async fn conformance_job_lifecycle() {
    let api = TestApiServer::new();
    let base = format!("/apis/batch/v1/namespaces/{NS}/jobs");
    let name = "e2e-abcde";
    let one = format!("{base}/{name}");
    let labels = json!({ "e2e-job-label": name });
    let sel = format!("e2e-job-label%3D{name}");

    // :1170-1177 list (resourceVersion seeds the watch).
    let (s, list) = get(&api, &format!("{base}?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{list}");
    let watch = open_watch(&api, &base, &list_rv(&list), &sel).await;
    let mut stream = into_stream(watch);
    let mut buf = String::new();

    // :1183-1189 Creating a suspended job; valid resourceVersion.
    let (s, job) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_job(name, Some(&labels), Some(true))),
    )
    .await;
    assert_eq!(s, 201, "create job: {job}");
    let created_rv = resource_version(&job);

    // :1191-1196 Patching the Job (strategic merge label); the patched object
    // MUST have a larger resourceVersion.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { name: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "patch job: {patched}");
    assert!(resource_version(&patched) > created_rv, "{patched}");

    // :1198-1213 Watching for Job to be patched: MODIFIED with the label.
    watch_until("job patched", &mut stream, &mut buf, |t, o| {
        t == "MODIFIED" && o["metadata"]["labels"][name] == "patched"
    })
    .await;
    assert_eq!(patched["metadata"]["labels"][name], "patched");

    // :1215-1233 Updating the job: Get, suspend=false, annotation, Update.
    let (s, mut to_update) = get(&api, &one).await;
    assert_eq!(s, 200, "{to_update}");
    to_update["spec"]["suspend"] = json!(false);
    to_update["metadata"]["annotations"] = json!({ "updated": "true" });
    let (s, updated) = send(&api, "PUT", &one, "application/json", Some(&to_update)).await;
    assert_eq!(s, 200, "update job: {updated}");

    // :1235-1251 Watching for Job to be updated: MODIFIED with the annotation.
    watch_until("job updated", &mut stream, &mut buf, |t, o| {
        t == "MODIFIED" && o["metadata"]["annotations"]["updated"] == "true"
    })
    .await;
    assert_eq!(updated["metadata"]["annotations"]["updated"], "true");

    // :1253-1258 Listing all Jobs with LabelSelector (all namespaces) -> 1.
    let (s, jobs) = get(&api, &format!("/apis/batch/v1/jobs?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{jobs}");
    assert_eq!(items(&jobs).len(), 1, "Failed to find job: {jobs}");

    // :1260-1262 WaitForJobComplete: the job controller's /status write
    // (job_controller.go syncJob -> updateStatusHandler).
    let (_, mut cur) = get(&api, &one).await;
    cur["status"] = json!({
        "startTime": "2026-10-15T12:00:00Z",
        "completionTime": "2026-10-15T12:00:05Z",
        "succeeded": 4,
        "conditions": [
            { "type": "SuccessCriteriaMet", "status": "True",
              "reason": "CompletionsReached",
              "lastTransitionTime": "2026-10-15T12:00:05Z" },
            { "type": "Complete", "status": "True",
              "reason": "CompletionsReached",
              "lastTransitionTime": "2026-10-15T12:00:05Z" },
        ],
    });
    let (s, body) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&cur),
    )
    .await;
    assert_eq!(s, 200, "job controller status write: {body}");

    // :1264-1267 Delete a job collection with a labelselector, Background.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{base}?labelSelector={sel}"),
        "application/json",
        Some(&json!({ "propagationPolicy": "Background" })),
    )
    .await;
    assert_eq!(s, 200, "failed to delete job: {body}");

    // :1269-1283 Watching for Job to be deleted: DELETED with the label.
    watch_until("job deleted", &mut stream, &mut buf, |t, o| {
        t == "DELETED" && o["metadata"]["labels"]["e2e-job-label"] == name
    })
    .await;

    // :1285-1288 Relist jobs to confirm deletion.
    let (s, jobs) = get(&api, &format!("/apis/batch/v1/jobs?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{jobs}");
    assert!(items(&jobs).is_empty(), "Found job: {jobs}");
}

/// cronjob.go:368 "should support CronJob API operations".
#[tokio::test]
async fn conformance_cronjob_api_operations() {
    let api = TestApiServer::new();
    let base = format!("/apis/batch/v1/namespaces/{NS}/cronjobs");
    let all = "/apis/batch/v1/cronjobs";
    let one = format!("{base}/test-api");
    let unique = "e2e-tests-cronjob-abcde";
    let sel = format!("special-label%3D{unique}");

    // :369-376 newTestCronJob("test-api", "* */1 * * ?", AllowConcurrent,
    // successCommand, &1, &0) with the special label.
    let template = |name: &str| {
        json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": { "name": name, "namespace": NS,
                          "labels": { "special-label": unique } },
            "spec": {
                "schedule": "* */1 * * ?",
                "concurrencyPolicy": "Allow",
                "successfulJobsHistoryLimit": 1,
                "failedJobsHistoryLimit": 0,
                "jobTemplate": { "spec": { "template": { "spec": {
                    "restartPolicy": "OnFailure",
                    "terminationGracePeriodSeconds": 1,
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                        "command": ["echo", "SUCCESS"],
                    }],
                }}}},
            },
        })
    };

    // :381-384 creating; valid resourceVersion.
    let (s, created) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&template("test-api")),
    )
    .await;
    assert_eq!(s, 201, "create cronjob: {created}");
    let created_rv = resource_version(&created);

    // :386-390 getting; UID matches.
    let (s, gotten) = get(&api, &one).await;
    assert_eq!(s, 200, "{gotten}");
    assert_eq!(gotten["metadata"]["uid"], created["metadata"]["uid"]);

    // :392-396 listing -> 1.
    let (s, cjs) = get(&api, &format!("{base}?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{cjs}");
    assert_eq!(items(&cjs).len(), 1, "filtered list should have 1 item");

    // :398-402 watching at the list's resourceVersion.
    let watch = open_watch(&api, &base, &list_rv(&cjs), &sel).await;
    let mut stream = into_stream(watch);
    let mut buf = String::new();

    // :404-416 cluster-wide listing -> 1, and cluster-wide watching opens.
    let (s, cluster) = get(&api, &format!("{all}?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{cluster}");
    assert_eq!(items(&cluster).len(), 1, "filtered list should have 1 item");
    let _cluster_watch = open_watch(&api, all, &list_rv(&cjs), &sel).await;

    // :418-424 patching (merge patch annotation); larger resourceVersion.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/merge-patch+json",
        Some(&json!({ "metadata": { "annotations": { "patched": "true" } } })),
    )
    .await;
    assert_eq!(s, 200, "patch: {patched}");
    assert_eq!(patched["metadata"]["annotations"]["patched"], "true");
    assert!(resource_version(&patched) > created_rv, "{patched}");

    // :426-438 updating: Get, annotations["updated"]="true", Update.
    let (s, mut to_update) = get(&api, &one).await;
    assert_eq!(s, 200, "{to_update}");
    to_update["metadata"]["annotations"]["updated"] = json!("true");
    let (s, updated) = send(&api, "PUT", &one, "application/json", Some(&to_update)).await;
    assert_eq!(s, 200, "update: {updated}");
    assert_eq!(updated["metadata"]["annotations"]["updated"], "true");

    // :440-465 watch events with the annotations: every event MUST be
    // MODIFIED and decode as a CronJob.
    watch_until("cronjob annotations", &mut stream, &mut buf, |t, o| {
        assert_eq!(t, "MODIFIED", "{o}");
        assert_eq!(o["kind"], "CronJob", "{o}");
        o["metadata"]["annotations"]["patched"] == "true"
    })
    .await;

    // :467-487 patching /status: annotation + lastScheduleTime.
    let now1 = "2026-10-15T12:00:00Z";
    let (s, patched_status) = send(
        &api,
        "PATCH",
        &format!("{one}/status"),
        "application/merge-patch+json",
        Some(&json!({
            "metadata": { "annotations": { "patchedstatus": "true" } },
            "status": { "lastScheduleTime": now1 },
        })),
    )
    .await;
    assert_eq!(s, 200, "patch /status: {patched_status}");
    assert_eq!(patched_status["status"]["lastScheduleTime"], now1);
    assert_eq!(
        patched_status["metadata"]["annotations"]["patchedstatus"],
        "true"
    );

    // :489-509 updating /status: Get, lastScheduleTime=now2, UpdateStatus.
    let now2 = "2026-10-15T12:00:01Z";
    let (s, mut to_update) = get(&api, &one).await;
    assert_eq!(s, 200, "{to_update}");
    to_update["status"]["lastScheduleTime"] = json!(now2);
    let (s, updated_status) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&to_update),
    )
    .await;
    assert_eq!(s, 200, "UpdateStatus: {updated_status}");
    assert_eq!(updated_status["status"]["lastScheduleTime"], now2);

    // :511-519 get /status: UID matches.
    let (s, status) = get(&api, &format!("{one}/status")).await;
    assert_eq!(s, 200, "{status}");
    assert_eq!(status["metadata"]["uid"], created["metadata"]["uid"]);

    // :521-526 expectFinalizer.
    let expect_finalizer = |cj: &Value, msg: &str| {
        assert!(
            !cj["metadata"]["deletionTimestamp"].is_null(),
            "expected deletionTimestamp on step {msg:?}: {cj}"
        );
        assert!(
            cj["metadata"]["finalizers"]
                .as_array()
                .is_some_and(|f| !f.is_empty()),
            "expected finalizers on step {msg:?}: {cj}"
        );
    };

    // :528-541 deleting "for-removal": either 404 or finalizer behaviour.
    let (s, _) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&template("for-removal")),
    )
    .await;
    assert_eq!(s, 201);
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{base}/for-removal"),
        "application/json",
        Some(&json!({})),
    )
    .await;
    assert_eq!(s, 200, "delete: {body}");
    let (s, cj) = get(&api, &format!("{base}/for-removal")).await;
    match s {
        200 => expect_finalizer(&cj, "deleting cronjob"),
        404 => {}
        other => panic!("expected 404, got {other}: {cj}"),
    }

    // :543-554 deleting a collection; <= 2 left, each with finalizers.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{base}?labelSelector={sel}"),
        "application/json",
        Some(&json!({})),
    )
    .await;
    assert_eq!(s, 200, "delete collection: {body}");
    let (s, cjs) = get(&api, &format!("{base}?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{cjs}");
    assert!(
        items(&cjs).len() <= 2,
        "filtered list length should be <= 2: {cjs}"
    );
    for cj in items(&cjs) {
        expect_finalizer(cj, "deleting cronjob collection");
    }
}

fn new_pod(name: &str, labels: &Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": { "name": name, "namespace": NS, "labels": labels },
        "spec": {
            "terminationGracePeriodSeconds": 1,
            "restartPolicy": "Never",
            "containers": [{
                "name": "token-test",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "securityContext": {
                    "allowPrivilegeEscalation": false,
                    "capabilities": { "drop": ["ALL"] },
                    "runAsNonRoot": true,
                    "runAsUser": 1000,
                    "seccompProfile": { "type": "RuntimeDefault" },
                },
            }],
        },
    })
}

/// pods.go:853 "should delete a collection of pods".
#[tokio::test]
async fn conformance_pods_delete_collection() {
    let api = TestApiServer::new();
    let base = format!("/api/v1/namespaces/{NS}/pods");
    let sel = "type%3DTesting";

    // :857-876 Create set of pods test-pod-1..3 labelled type=Testing.
    for n in ["test-pod-1", "test-pod-2", "test-pod-3"] {
        let (s, pod) = send(
            &api,
            "POST",
            &base,
            "application/json",
            Some(&new_pod(n, &json!({ "type": "Testing" }))),
        )
        .await;
        assert_eq!(s, 201, "create {n}: {pod}");
    }
    // :878-882 WaitForPodsRunningReady: kubelet-driven, no REST write that
    // DeleteCollection depends on.

    // :884-887 DeleteCollection with GracePeriodSeconds=1 and the selector.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{base}?labelSelector={sel}"),
        "application/json",
        Some(&json!({ "gracePeriodSeconds": 1 })),
    )
    .await;
    assert_eq!(s, 200, "failed to delete collection of pods: {body}");

    // :889-892 wait for all pods to be deleted. Unscheduled pods go away
    // at once; scheduled ones carry deletionTimestamp until the kubelet's
    // grace-0 delete. These are unscheduled, so none MUST remain.
    let (s, list) = get(&api, &format!("{base}?labelSelector={sel}")).await;
    assert_eq!(s, 200, "{list}");
    assert!(items(&list).is_empty(), "found a pod(s): {list}");
}

/// pods.go:1091 "should patch a pod status".
#[tokio::test]
async fn conformance_pod_patch_status() {
    let api = TestApiServer::new();
    let base = format!("/api/v1/namespaces/{NS}/pods");
    let name = "pod-abcde";

    // :1092-1109 Create a pod; :1110 WaitForPodRunningInNamespace is the
    // kubelet's work.
    let (s, pod) = send(
        &api,
        "POST",
        &base,
        "application/json",
        Some(&new_pod(name, &json!({ "e2e": name }))),
    )
    .await;
    assert_eq!(s, 201, "create pod: {pod}");

    // :1112-1128 patching /status: merge patch, annotation + message/reason.
    let (s, p) = send(
        &api,
        "PATCH",
        &format!("{base}/{name}/status"),
        "application/merge-patch+json",
        Some(&json!({
            "metadata": { "annotations": { "patchedstatus": "true" } },
            "status": { "message": "Patched by e2e test", "reason": "E2E" },
        })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch pod: {p}");
    assert_eq!(p["status"]["message"], "Patched by e2e test", "{p}");
    assert_eq!(p["status"]["reason"], "E2E", "{p}");
}
