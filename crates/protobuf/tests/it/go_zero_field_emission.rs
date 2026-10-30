//! Wire exactness for types whose Go marshaller writes zero-valued fields.
//! Ported from the generated `MarshalToSizedBuffer` code in release-1.35:
//! * `metav1.Timestamp` (`apimachinery/pkg/apis/meta/v1/generated.pb.go`) writes
//!   `seconds` and `nanos` unconditionally; `Time.ProtoTime` (`time_proto.go`)
//!   always sends `nanos = 0`, `MicroTime.ProtoMicroTime` truncates to micros.
//! * `IntOrString` (`apimachinery/pkg/util/intstr/generated.pb.go`) writes
//!   `type`, `intVal`, `strVal` unconditionally.
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::json;

#[test]
fn time_emits_explicit_zero_nanos() {
    let meta = json!({"creationTimestamp": "2007-01-01T01:01:01Z"});
    let bytes = PROTO_REGISTRY.encode_message("ObjectMeta", &meta).unwrap();
    // field 8 (0x42), len 8: seconds(0x08 + 5-byte varint) -- then nanos 0x10 0x00
    // (preceded by the plain `omitempty` strings/ints Go writes empty: name,
    // generateName, namespace, selfLink, uid, resourceVersion, generation).
    assert!(bytes.windows(2).any(|w| w == [0x42, 0x08]), "{bytes:?}");
    assert_eq!(&bytes[bytes.len() - 2..], &[0x10, 0x00], "{bytes:?}");
}

#[test]
fn time_drops_subsecond_precision() {
    let meta = json!({"creationTimestamp": "2007-01-01T01:01:01.123456789Z"});
    let bytes = PROTO_REGISTRY.encode_message("ObjectMeta", &meta).unwrap();
    assert_eq!(&bytes[bytes.len() - 2..], &[0x10, 0x00], "{bytes:?}");
}

#[test]
fn int_or_string_emits_all_three_fields() {
    let sp = json!({"targetPort": "http"});
    let s = PROTO_REGISTRY.encode_message("ServicePort", &sp).unwrap();
    // targetPort = field 4 (0x22): type=1, intVal=0, strVal="http"
    let want = [&[0x22u8, 10, 0x08, 1, 0x10, 0, 0x1a, 4][..], b"http"].concat();
    assert!(s.windows(want.len()).any(|w| w == want.as_slice()), "{s:?}");

    let sp = json!({"targetPort": 8080});
    let i = PROTO_REGISTRY.encode_message("ServicePort", &sp).unwrap();
    // type=0, intVal=8080 (0x90 0x3f), strVal=""
    let want = [0x22u8, 7, 0x08, 0, 0x10, 0x90, 0x3f, 0x1a, 0];
    assert!(i.windows(want.len()).any(|w| w == want), "{i:?}");
}
