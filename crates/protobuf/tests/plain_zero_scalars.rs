//! #2449: client-go's generated protobuf marshaller writes every non-pointer
//! scalar, zero included (`dAtA[i] = 0` for `Stdin bool`), where JSON drops it
//! (`json:"stdin,omitempty"`). Our `Option<bool>` models keep `Some(false)` as
//! different from `None`, so an unchanged object compared different from its
//! JSON-created twin (#2382: the ephemeral-container update 422).
//!
//! A plain (non-pointer) `omitempty` field decodes to the JSON form, i.e.
//! absent; a pointer field (`*bool`) keeps an explicit `false`, which Go's
//! marshaller only writes when the pointer is non-nil.
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::Value;

fn decode(msg: &str, data: &[u8]) -> Value {
    PROTO_REGISTRY
        .decode_message(msg, data)
        .unwrap_or_else(|| panic!("{msg} is registered"))
}

#[test]
fn container_plain_bools_written_false_decode_absent() {
    // name(1)="c", stdin(16)=0, stdinOnce(17)=0, tty(18)=0
    let wire = [
        0x0a, 0x01, b'c', 0x80, 0x01, 0x00, 0x88, 0x01, 0x00, 0x90, 0x01, 0x00,
    ];
    for msg in ["Container", "EphemeralContainerCommon"] {
        let v = decode(msg, &wire);
        assert_eq!(v, serde_json::json!({"name": "c"}), "{msg}");
    }
}

#[test]
fn a_true_plain_bool_is_kept() {
    let v = decode("Container", &[0x80, 0x01, 0x01]);
    assert_eq!(v["stdin"], Value::Bool(true));
}

#[test]
fn pod_spec_host_flags_written_false_decode_absent() {
    // hostNetwork(11)=0, hostPID(12)=0
    let v = decode("PodSpec", &[0x58, 0x00, 0x60, 0x00]);
    assert_eq!(v, serde_json::json!({}));
}

#[test]
fn pod_spec_pointer_bool_keeps_explicit_false() {
    // automountServiceAccountToken(21) is *bool: an explicit false is a value.
    let v = decode("PodSpec", &[0xa8, 0x01, 0x00]);
    assert_eq!(v["automountServiceAccountToken"], Value::Bool(false));
}

#[test]
fn probe_plain_ints_written_zero_decode_absent() {
    // initialDelaySeconds(2)=0, timeoutSeconds(3)=0, periodSeconds(4)=0,
    // successThreshold(5)=0, failureThreshold(6)=0
    let v = decode(
        "Probe",
        &[0x10, 0x00, 0x18, 0x00, 0x20, 0x00, 0x28, 0x00, 0x30, 0x00],
    );
    assert_eq!(v, serde_json::json!({}));
}

#[test]
fn volume_mount_and_port_plain_scalars() {
    // VolumeMount.readOnly(2)=0
    assert_eq!(decode("VolumeMount", &[0x10, 0x00]), serde_json::json!({}));
    // ContainerPort.hostPort(2)=0
    assert_eq!(
        decode("ContainerPort", &[0x10, 0x00]),
        serde_json::json!({})
    );
}

/// #2929: Go's generated `ObjectMeta.MarshalToSizedBuffer` writes `name`,
/// `generateName`, `namespace`, `uid`, `resourceVersion` and `generation`
/// unconditionally (k8s.io/apimachinery/pkg/apis/meta/v1/generated.pb.go), but
/// their JSON tags are `omitempty`, so the decoder must emit the JSON form:
/// nothing.
#[test]
fn object_meta_go_zero_scalars_decode_absent() {
    let v = decode(
        "ObjectMeta",
        &[
            0x0a, 0x00, // name(1)=""
            0x12, 0x00, // generateName(2)=""
            0x1a, 0x00, // namespace(3)=""
            0x2a, 0x00, // uid(5)=""
            0x32, 0x00, // resourceVersion(6)=""
            0x38, 0x00, // generation(7)=0
        ],
    );
    let obj = v.as_object().unwrap();
    for k in [
        "name",
        "generateName",
        "namespace",
        "uid",
        "resourceVersion",
        "generation",
    ] {
        assert!(!obj.contains_key(k), "{k} must be absent: {v}");
    }
}

#[test]
fn object_meta_non_zero_scalars_are_kept() {
    // name(1)="a", generation(7)=3
    let v = decode("ObjectMeta", &[0x0a, 0x01, b'a', 0x38, 0x03]);
    assert_eq!(v["name"], "a");
    assert_eq!(v["generation"], 3);
}
