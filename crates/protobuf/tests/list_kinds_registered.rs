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
