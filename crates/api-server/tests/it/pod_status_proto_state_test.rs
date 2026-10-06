//! A Pod's `containerStatuses[].state` must survive the protobuf encoder.
//! kubelet (client-go, protobuf) GETs the pod and diffs its status against
//! the new one; a `state` lost on the wire makes the diff omit `waiting:null`.

use rusternetes_api_server::protobuf::PROTO_REGISTRY;
use serde_json::{json, Value};

fn roundtrip(pod: &Value) -> Value {
    let bytes = PROTO_REGISTRY.encode_message("Pod", pod).expect("encode");
    PROTO_REGISTRY
        .decode_message("Pod", &bytes)
        .expect("decode")
}

#[test]
fn waiting_state_with_null_message_survives_protobuf() {
    let pod = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
        "status": {"phase": "Pending", "containerStatuses": [{
            "name": "c", "ready": false, "restartCount": 0,
            "image": "busybox", "imageID": "",
            "state": {"waiting": {"reason": "ContainerCreating", "message": null}}
        }]}
    });
    let out = roundtrip(&pod);
    assert_eq!(
        out["status"]["containerStatuses"][0]["state"]["waiting"]["reason"], "ContainerCreating",
        "{out}"
    );
}

#[test]
fn waiting_state_without_message_survives_protobuf() {
    let pod = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
        "status": {"phase": "Pending", "containerStatuses": [{
            "name": "c", "ready": false, "restartCount": 0,
            "image": "busybox", "imageID": "",
            "state": {"waiting": {"reason": "ContainerCreating"}}
        }]}
    });
    let out = roundtrip(&pod);
    assert_eq!(
        out["status"]["containerStatuses"][0]["state"]["waiting"]["reason"], "ContainerCreating",
        "{out}"
    );
}

// ---------------------------------------------------------------------------
// kubelet status-manager flow over the real router (protobuf GET + SMP PATCH).
// ---------------------------------------------------------------------------

use axum::http::StatusCode;
use rusternetes_common::protobuf::{Unknown, PROTOBUF_MAGIC};
use rusternetes_test_support::harness::TestApiServer;

const POD: &str = "/api/v1/namespaces/default/pods/p";

fn container_status(state: Value) -> Value {
    json!({
        "allocatedResources": {"cpu": "100m", "memory": "50Mi"},
        "image": "busybox", "imageID": "", "lastState": {}, "name": "c",
        "ready": false, "restartCount": 0, "started": false,
        "resources": {"limits": {"cpu": "100m"}, "requests": {"cpu": "100m"}},
        "state": state,
        "volumeMounts": [{"mountPath": "/x", "name": "v", "recursiveReadOnly": "Disabled"}],
    })
}

async fn smp_status(api: &TestApiServer, patch: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{POD}/status"),
        Some("application/strategic-merge-patch+json"),
        Some(patch),
    )
    .await
}

async fn get_pod_over_protobuf(api: &TestApiServer) -> Value {
    use prost::Message;
    let (s, _, body, _) = api
        .send_with_headers(
            "GET",
            POD,
            &[("accept", "application/vnd.kubernetes.protobuf")],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let unknown = Unknown::decode(&body[PROTOBUF_MAGIC.len()..]).expect("Unknown");
    PROTO_REGISTRY.decode_message("Pod", &unknown.raw).unwrap()
}

/// The kubelet's second status patch (waiting -> running) carries
/// `"waiting": null` only if its GET showed the first patch's containerStatus.
/// Pins: the first patch's state is visible on a protobuf GET, and the
/// kubelet-shaped second patch applies.
#[tokio::test]
async fn kubelet_status_patches_waiting_then_running_apply() {
    let api = TestApiServer::new();
    let mut p = json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
        "spec": {"nodeName": "n", "containers": [{"name": "c", "image": "busybox"}]}});
    p["spec"]["hostNetwork"] = json!(true);
    let (s, b) = api.post("/api/v1/namespaces/default/pods", &p).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");

    let first = json!({"metadata": {"uid": b["metadata"]["uid"]}, "status": {
        "$setElementOrder/conditions": [{"type": "Initialized"}, {"type": "Ready"}],
        "conditions": [
            {"lastTransitionTime": "2026-10-06T16:20:20Z", "status": "True", "type": "Initialized"},
            {"lastTransitionTime": "2026-10-06T16:20:20Z", "message": "containers with unready status: [c]",
             "reason": "ContainersNotReady", "status": "False", "type": "Ready"}],
        "containerStatuses": [container_status(json!({"waiting": {"reason": "ContainerCreating"}}))],
        "hostIP": "172.18.0.2", "phase": "Pending", "startTime": "2026-10-06T16:20:20Z"}});
    let (s, b) = smp_status(&api, &first).await;
    assert_eq!(s, StatusCode::OK, "{b}");

    let read = get_pod_over_protobuf(&api).await;
    assert_eq!(
        read["status"]["containerStatuses"][0]["state"]["waiting"]["reason"], "ContainerCreating",
        "kubelet's GET lost the container status: {read}"
    );

    let second = json!({"metadata": {"uid": b["metadata"]["uid"]}, "status": {
        "$setElementOrder/conditions": [{"type": "Initialized"}, {"type": "Ready"}],
        "conditions": [{"lastTransitionTime": "2026-10-06T16:20:23Z", "message": null,
                        "reason": null, "status": "True", "type": "Ready"}],
        "containerStatuses": [{"name": "c", "ready": true, "started": true,
            "state": {"running": {"startedAt": "2026-10-06T16:20:23Z"}, "waiting": null}}],
        "phase": "Running"}});
    let (s, b) = smp_status(&api, &second).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(b["status"]["containerStatuses"][0]["state"]["waiting"].is_null());
}
