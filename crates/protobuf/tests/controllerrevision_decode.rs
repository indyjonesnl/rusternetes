//! #1667: decoding a real DaemonSet-controller ControllerRevision POST must
//! yield the ControllerRevision wrapper (`metadata`, `data`, `revision`), NOT a
//! bare copy of the DaemonSet stored inside `data`.
//!
//! `controllerrevision_daemonset_post.bin` is the exact `application/vnd.
//! kubernetes.protobuf` body the vanilla controller-manager POSTs to
//! `/apis/apps/v1/namespaces/kube-system/controllerrevisions` for the kube-proxy
//! DaemonSet, captured off the wire. Bug: the decode surfaced `data`'s content
//! (the DaemonSet) as the top-level object, so `data` was absent and the
//! apiserver rejected it with "data: Required value" — the DaemonSet controller
//! then never created kindnet/kube-proxy pods (no CNI) on a swapped api-server.
use rusternetes_protobuf::PROTO_REGISTRY;

#[test]
fn controllerrevision_post_decodes_with_data_wrapper() {
    let body = include_bytes!("fixtures/controllerrevision_daemonset_post.bin");
    let json = PROTO_REGISTRY
        .decode_k8s_resource(body)
        .expect("ControllerRevision POST body must decode");
    let v: serde_json::Value = serde_json::from_slice(&json).expect("decoded bytes are JSON");

    assert_eq!(
        v.pointer("/kind").and_then(|k| k.as_str()),
        Some("ControllerRevision"),
        "top-level kind must stay ControllerRevision, not the inner DaemonSet"
    );
    assert!(
        v.pointer("/revision").is_some(),
        "revision must be preserved"
    );
    let data = v
        .pointer("/data")
        .expect("data (the DaemonSet snapshot) must be present, not hoisted to top-level");
    assert!(
        data.is_object(),
        "data must be a JSON object (the serialized DaemonSet)"
    );
}
