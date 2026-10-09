//! #2952: client-go's generated marshaller writes a non-nil `*string` even
//! when it is empty (`generated.pb.go` PersistentVolumeClaimSpec.MarshalToSizedBuffer:
//! `if m.StorageClassName != nil { ...; dAtA[i] = 0x2a }`), so `0x2a 0x00` means
//! `storageClassName: ""` ("no class, do not default"), which is different from
//! the field being absent. A plain `string` is never written when empty
//! (omitempty), so only pointer strings can carry an explicit empty value.
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::Value;

fn decode(msg: &str, data: &[u8]) -> Value {
    PROTO_REGISTRY
        .decode_message(msg, data)
        .unwrap_or_else(|| panic!("{msg} is registered"))
}

#[test]
fn pvc_spec_explicit_empty_storage_class_name_is_kept() {
    // storageClassName(5) = "" (*string, non-nil)
    let v = decode("PersistentVolumeClaimSpec", &[0x2a, 0x00]);
    assert_eq!(
        v.get("storageClassName"),
        Some(&Value::String(String::new())),
        "explicit empty *string must survive decode; got {v}"
    );
}

#[test]
fn pvc_spec_absent_storage_class_name_stays_absent() {
    // volumeName(3) = "pv" only
    let v = decode("PersistentVolumeClaimSpec", &[0x1a, 0x02, b'p', b'v']);
    assert!(v.get("storageClassName").is_none(), "got {v}");
}

#[test]
fn pvc_spec_other_pointer_strings_keep_explicit_empty() {
    // volumeMode(6) *PersistentVolumeMode, volumeAttributesClassName(9) *string
    for (field_tag, json_name) in [(0x32u8, "volumeMode"), (0x4a, "volumeAttributesClassName")] {
        let v = decode("PersistentVolumeClaimSpec", &[field_tag, 0x00]);
        assert_eq!(
            v.get(json_name),
            Some(&Value::String(String::new())),
            "{json_name}: got {v}"
        );
    }
}

#[test]
fn plain_empty_string_stays_absent() {
    // ObjectMeta.generateName(2) is a plain string: Go never writes it empty,
    // and if it is written empty the JSON twin omits it.
    let v = decode("ObjectMeta", &[0x12, 0x00]);
    assert!(v.get("generateName").is_none(), "got {v}");
}
