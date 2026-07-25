//! #1667: the request-decode cascade (`decode_k8s_protobuf_request_body`) must
//! decode a real DaemonSet-controller ControllerRevision POST into the CR
//! wrapper — not hoist the inner DaemonSet from `data.raw` to the top level.
//! (decode_k8s_resource alone is correct; the bug is the cascade preferring an
//! extract that digs into the nested k8s\0-Unknown inside data.raw.)
use rusternetes_middleware::decode_k8s_protobuf_request_body;

#[test]
fn controllerrevision_request_decodes_with_data() {
    let body = include_bytes!("fixtures/controllerrevision_daemonset_post.bin");
    let json = decode_k8s_protobuf_request_body(body);
    let v: serde_json::Value = serde_json::from_slice(&json).expect("decoded JSON");
    assert_eq!(
        v.pointer("/kind").and_then(|k| k.as_str()),
        Some("ControllerRevision"),
        "must decode the ControllerRevision, not the inner DaemonSet (got kind={:?})",
        v.pointer("/kind")
    );
    assert!(v.pointer("/data").is_some(), "data must be present");
}
