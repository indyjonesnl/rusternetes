//! Wire-level test for authorization.k8s.io/v1 `ResourceAttributes`
//! `fieldSelector` (8) and `labelSelector` (9), hand-encoded with the UPSTREAM
//! tags from `staging/src/k8s.io/api/authorization/v1/generated.proto`:
//!
//! ```text
//! message FieldSelectorAttributes { optional string rawSelector = 1;
//!   repeated ...FieldSelectorRequirement requirements = 2; }
//! message LabelSelectorAttributes { optional string rawSelector = 1;
//!   repeated ...LabelSelectorRequirement requirements = 2; }
//! ResourceAttributes: optional FieldSelectorAttributes fieldSelector = 8;
//!                     optional LabelSelectorAttributes labelSelector = 9;
//! ```
use rusternetes_protobuf::ProtoRegistry;

fn len_field(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![tag, payload.len() as u8];
    v.extend_from_slice(payload);
    v
}

/// requirement { key=1 "k", operator=2 "In", values=3 "v" }
fn requirement() -> Vec<u8> {
    let mut r = len_field(0x0A, b"k");
    r.extend(len_field(0x12, b"In"));
    r.extend(len_field(0x1A, b"v"));
    r
}

fn selector_attrs() -> Vec<u8> {
    let mut s = len_field(0x0A, b"raw"); // rawSelector = 1
    s.extend(len_field(0x12, &requirement())); // requirements = 2
    s
}

#[test]
fn resource_attributes_decodes_field_and_label_selector() {
    let mut ra = len_field(0x0A, b"ns"); // namespace = 1
    ra.extend(len_field(0x42, &selector_attrs())); // fieldSelector = 8
    ra.extend(len_field(0x4A, &selector_attrs())); // labelSelector = 9

    let reg = ProtoRegistry::new();
    let d = reg
        .decode_message("ResourceAttributes", &ra)
        .expect("decode");
    for sel in ["fieldSelector", "labelSelector"] {
        assert_eq!(
            d.pointer(&format!("/{sel}/rawSelector")),
            Some(&serde_json::json!("raw")),
            "{sel}: {d}"
        );
        assert_eq!(
            d.pointer(&format!("/{sel}/requirements/0/operator")),
            Some(&serde_json::json!("In")),
            "{sel}: {d}"
        );
        assert_eq!(
            d.pointer(&format!("/{sel}/requirements/0/values/0")),
            Some(&serde_json::json!("v")),
            "{sel}: {d}"
        );
    }
    // And the roundtrip re-emits the upstream bytes. Go's marshaller also
    // writes the plain `verb`..`name` strings (fields 2-7, `omitempty` in JSON)
    // when empty (authorization/v1 generated.pb.go
    // `ResourceAttributes.MarshalToSizedBuffer`), which the hand-built input
    // above leaves out.
    let re = reg
        .encode_message("ResourceAttributes", &d)
        .expect("encode");
    let mut want = ra[..4].to_vec(); // namespace
    for tag in [0x12, 0x1a, 0x22, 0x2a, 0x32, 0x3a] {
        want.extend_from_slice(&[tag, 0]);
    }
    want.extend_from_slice(&ra[4..]);
    assert_eq!(re, want);
    assert_eq!(reg.decode_message("ResourceAttributes", &re).unwrap(), d);
}
