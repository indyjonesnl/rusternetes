//! `Accept: application/vnd.kubernetes.protobuf` on the webhook-configuration
//! endpoints answers with a protobuf envelope (upstream's
//! `transformResponseObject` serializer choice), not JSON. Refs #3057.

use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

async fn assert_proto_get_and_list(resource: &str, kind: &str) {
    let api = TestApiServer::new();
    let base = format!("/apis/admissionregistration.k8s.io/v1/{resource}");
    let obj = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": kind,
        "metadata": {"name": "wh-proto"},
        "webhooks": [],
    });
    let (st, body) = api.post(&base, &obj).await;
    assert!(st.is_success(), "create failed: {st} {body}");

    for (url, want_kind) in [
        (format!("{base}/wh-proto"), kind.to_string()),
        (base.clone(), format!("{kind}List")),
    ] {
        let (st, headers, bytes, _) = api
            .send_with_headers("GET", &url, &[("accept", PROTO_CT)], None)
            .await;
        assert!(st.is_success(), "GET {url} -> {st}");
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("protobuf"), "GET {url}: content-type {ct}");
        assert!(bytes.starts_with(b"k8s\0"), "GET {url}: not an envelope");
        let json: Value = serde_json::from_slice(
            &ProtoRegistry::new()
                .decode_k8s_resource(&bytes)
                .expect("registry decodes the envelope"),
        )
        .unwrap();
        assert_eq!(json["kind"], want_kind, "GET {url}");
    }
}

#[tokio::test]
async fn mutating_webhook_configuration_serves_protobuf() {
    assert_proto_get_and_list(
        "mutatingwebhookconfigurations",
        "MutatingWebhookConfiguration",
    )
    .await;
}

#[tokio::test]
async fn validating_webhook_configuration_serves_protobuf() {
    assert_proto_get_and_list(
        "validatingwebhookconfigurations",
        "ValidatingWebhookConfiguration",
    )
    .await;
}
