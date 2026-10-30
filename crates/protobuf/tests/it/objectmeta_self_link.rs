//! `ObjectMeta.selfLink` is field 4 in `meta/v1/generated.proto`. It is
//! deprecated and never populated by the server, but upstream's golden
//! fixtures (`staging/src/k8s.io/api/testdata/HEAD/*.pb`) carry it, so the
//! codec must keep it across a decode/encode cycle.
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::json;

#[test]
fn object_meta_self_link_roundtrips() {
    let meta = json!({"name": "n", "selfLink": "selfLinkValue", "uid": "u"});
    let bytes = PROTO_REGISTRY
        .encode_message("ObjectMeta", &meta)
        .expect("ObjectMeta registered");
    // field 4, wire type 2, "selfLinkValue"
    let want = [&[0x22u8, 13][..], b"selfLinkValue"].concat();
    assert!(
        bytes.windows(want.len()).any(|w| w == want.as_slice()),
        "selfLink must be written at field 4: {bytes:?}"
    );
    let back = PROTO_REGISTRY.decode_message("ObjectMeta", &bytes).unwrap();
    assert_eq!(back["selfLink"], "selfLinkValue");
}
