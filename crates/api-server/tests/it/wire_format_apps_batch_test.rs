//! apps/v1 and batch/v1 wire-format gates (tracking issue #3057).
//!
//! * protobuf negotiation: upstream's `transformResponseObject`
//!   (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/response.go) picks the
//!   serializer from the request's `Accept` for EVERY resource, not per kind, so
//!   `Accept: application/vnd.kubernetes.protobuf` on a GET of any apps or
//!   batch workload must be answered with the `k8s\0` envelope.
//! * OpenAPI: kube-openapi serves one v3 document per group-version
//!   (staging/src/k8s.io/kube-openapi/pkg/handler3/handler.go) whose
//!   `components.schemas` carry the generated definitions of every kind in it.

use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

fn tmpl() -> Value {
    json!({"metadata":{"labels":{"a":"b"}},"spec":{"containers":[{"name":"c","image":"busybox"}]}})
}

fn object(kind: &str, group: &str) -> Value {
    let spec = match kind {
        "Job" => json!({"template":{"metadata":{"labels":{"a":"b"}},
            "spec":{"restartPolicy":"Never","containers":[{"name":"c","image":"busybox"}]}}}),
        _ => json!({"selector":{"matchLabels":{"a":"b"}}, "template": tmpl()}),
    };
    json!({"apiVersion": format!("{group}/v1"), "kind": kind,
           "metadata":{"name":"wfab","namespace":"wfab-ns"}, "spec": spec})
}

#[tokio::test]
async fn apps_batch_workloads_answer_protobuf_accept_with_protobuf() {
    let registry = ProtoRegistry::new();
    for (kind, group, plural) in [
        ("Deployment", "apps", "deployments"),
        ("ReplicaSet", "apps", "replicasets"),
        ("StatefulSet", "apps", "statefulsets"),
        ("DaemonSet", "apps", "daemonsets"),
        ("Job", "batch", "jobs"),
    ] {
        let api = TestApiServer::new();
        let coll = format!("/apis/{group}/v1/namespaces/wfab-ns/{plural}");
        let (st, resp) = api.post(&coll, &object(kind, group)).await;
        assert!(st.is_success(), "{kind}: JSON create -> {st}: {resp}");

        let (st, headers, bytes, _) = api
            .send_with_headers(
                "GET",
                &format!("{coll}/wfab"),
                &[("accept", PROTO_CT)],
                None,
            )
            .await;
        assert!(st.is_success(), "{kind}: GET -> {st}");
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("protobuf") && bytes.starts_with(b"k8s\0"),
            "{kind}: Accept protobuf answered with content-type '{ct}'"
        );
        let decoded = registry
            .decode_k8s_resource(&bytes)
            .unwrap_or_else(|| panic!("{kind}: protobuf body not decodable"));
        let decoded: Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(decoded["metadata"]["name"], "wfab", "{kind}");
        assert_eq!(
            decoded["spec"]["template"]["spec"]["containers"][0]["name"], "c",
            "{kind}"
        );
    }
}

async fn v3_schemas(api: &TestApiServer, gv: &str) -> serde_json::Map<String, Value> {
    let (st, doc) = api.get(&format!("/openapi/v3/{gv}")).await;
    assert_eq!(st, 200, "{gv}");
    doc["components"]["schemas"]
        .as_object()
        .cloned()
        .unwrap_or_default()
}

fn has_gvk(
    schemas: &serde_json::Map<String, Value>,
    group: &str,
    version: &str,
    kind: &str,
) -> bool {
    schemas.values().any(|s| {
        s["x-kubernetes-group-version-kind"]
            .as_array()
            .is_some_and(|a| {
                a.iter()
                    .any(|e| e["group"] == group && e["version"] == version && e["kind"] == kind)
            })
    })
}

#[tokio::test]
async fn openapi_v3_apps_and_batch_documents_define_their_kinds() {
    let api = TestApiServer::new();
    let apps = v3_schemas(&api, "apis/apps/v1").await;
    for kind in [
        "DaemonSet",
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "ControllerRevision",
    ] {
        assert!(
            has_gvk(&apps, "apps", "v1", kind),
            "apps/v1 v3 lacks {kind}"
        );
    }
    // The scale subresource is autoscaling/v1 Scale upstream.
    assert!(apps.contains_key("io.k8s.api.autoscaling.v1.Scale"));
    let batch = v3_schemas(&api, "apis/batch/v1").await;
    for kind in ["CronJob", "Job"] {
        assert!(
            has_gvk(&batch, "batch", "v1", kind),
            "batch/v1 v3 lacks {kind}"
        );
    }
}

#[tokio::test]
async fn openapi_v2_defines_controller_revision_and_scale() {
    let api = TestApiServer::new();
    let (st, doc) = api.get("/openapi/v2").await;
    assert_eq!(st, 200);
    let defs = &doc["definitions"];
    assert!(defs["io.k8s.api.apps.v1.ControllerRevision"].is_object());
    assert!(defs["io.k8s.api.autoscaling.v1.Scale"].is_object());
}
