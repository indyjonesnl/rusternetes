//! The served List kinds of certificates/v1, flowcontrol/v1, node/v1 and
//! resource/v1 must be registered with the upstream field numbers
//! (metadata = 1, items = 2, per each group's generated.proto in
//! crates/api-server/proto/upstream/v1.35/k8s.io/api/). Refs #3057.

use rusternetes_protobuf::{FieldType, ProtoRegistry};

/// (list message, item message as the registry resolves it).
const LISTS: &[(&str, &str)] = &[
    // certificates/v1/generated.proto:102
    ("CertificateSigningRequestList", "CertificateSigningRequest"),
    // flowcontrol/v1/generated.proto:114, :340
    ("FlowSchemaList", "FlowSchema"),
    (
        "PriorityLevelConfigurationList",
        "PriorityLevelConfiguration",
    ),
    // node/v1/generated.proto:79
    ("RuntimeClassList", "RuntimeClass"),
    // resource/v1/generated.proto:668, :1409, :1505, :1601. The DRA kinds
    // are registered under group-qualified keys (see register_resource_v1).
    ("DeviceClassList", "resource.k8s.io/v1.DeviceClass"),
    ("ResourceClaimList", "resource.k8s.io/v1.ResourceClaim"),
    (
        "ResourceClaimTemplateList",
        "resource.k8s.io/v1.ResourceClaimTemplate",
    ),
    ("ResourceSliceList", "resource.k8s.io/v1.ResourceSlice"),
    // resource/v1alpha3/generated.proto:152 (only v1alpha3 serves it in 1.35)
    ("DeviceTaintRuleList", "DeviceTaintRule"),
];

#[test]
fn served_list_kinds_registered_with_upstream_numbers() {
    let r = ProtoRegistry::new();
    for (list, item) in LISTS {
        let (_, s) = r
            .iter_schemas()
            .find(|(k, _)| k == list)
            .unwrap_or_else(|| panic!("{list} not registered"));
        match s.fields.get(&1) {
            Some((n, FieldType::Message(m))) => {
                assert_eq!((n.as_str(), m.as_str()), ("metadata", "ListMeta"), "{list}")
            }
            other => panic!("{list} #1: {other:?}"),
        }
        match s.fields.get(&2) {
            Some((n, FieldType::Repeated(b))) => {
                assert_eq!(n, "items", "{list}");
                assert!(
                    matches!(&**b, FieldType::Message(m) if m == item),
                    "{list} #2 item: {b:?}"
                );
            }
            other => panic!("{list} #2: {other:?}"),
        }
        assert!(
            r.iter_schemas().any(|(k, _)| k == *item),
            "{list}: item schema {item} is not registered"
        );
    }
}

/// resource/v1alpha3/generated.proto:135-:240: DeviceTaintRule{metadata=1,
/// spec=2,status=3}, Spec{deviceSelector=1,taint=2}, Selector{driver=2,
/// pool=3,device=4}, Status{conditions=1}. Upstream's
/// pkg/registry/resource/devicetaintrule/storage/storage_test.go round-trips
/// through the storage codec; this pins the wire shape of that round trip.
#[test]
fn device_taint_rule_protobuf_roundtrip() {
    let r = ProtoRegistry::new();
    let rule = serde_json::json!({
        "metadata": {"name": "r1"},
        "spec": {
            "deviceSelector": {"driver": "d.example.com", "pool": "p", "device": "dev"},
            "taint": {"key": "k", "value": "v", "effect": "NoExecute"}
        },
        "status": {"conditions": [{
            "type": "EvictionInProgress", "status": "False",
            "reason": "NoEvictions", "message": "m"
        }]}
    });
    let bytes = r.encode_message("DeviceTaintRule", &rule).unwrap();
    let back = r.decode_message("DeviceTaintRule", &bytes).unwrap();
    assert_eq!(
        back["spec"]["deviceSelector"],
        rule["spec"]["deviceSelector"]
    );
    assert_eq!(back["spec"]["taint"]["effect"], "NoExecute");
    assert_eq!(back["spec"]["taint"]["key"], "k");
    assert_eq!(
        back["status"]["conditions"][0]["type"],
        "EvictionInProgress"
    );
    let list = serde_json::json!({"metadata": {}, "items": [rule]});
    let bytes = r.encode_message("DeviceTaintRuleList", &list).unwrap();
    let back = r.decode_message("DeviceTaintRuleList", &bytes).unwrap();
    assert_eq!(back["items"][0]["spec"]["taint"]["value"], "v");
}
