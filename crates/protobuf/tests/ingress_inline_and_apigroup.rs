//! networking/v1 JSON shapes the registry must honour:
//! * `IngressRule` embeds `IngressRuleValue` with `json:",inline"`
//!   (`staging/src/k8s.io/api/networking/v1/types.go`), so `http` sits on the
//!   rule itself, not under an `ingressRuleValue` key.
//! * `IngressClassParametersReference.APIGroup` has the JSON tag `apiGroup`;
//!   `aPIGroup` is only the proto field name.
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::json;

#[test]
fn ingress_rule_http_is_inline() {
    let rule = json!({
        "host": "h",
        "http": {"paths": [{"path": "/p", "pathType": "Prefix",
                            "backend": {"service": {"name": "s", "port": {"number": 80}}}}]}
    });
    let bytes = PROTO_REGISTRY.encode_message("IngressRule", &rule).unwrap();
    let back = PROTO_REGISTRY
        .decode_message("IngressRule", &bytes)
        .unwrap();
    assert_eq!(back["http"]["paths"][0]["path"], "/p", "{back}");
    assert!(back.get("ingressRuleValue").is_none(), "{back}");
}

#[test]
fn ingress_class_parameters_api_group_uses_json_name() {
    let p = json!({"apiGroup": "g", "kind": "k", "name": "n"});
    let bytes = PROTO_REGISTRY
        .encode_message("IngressClassParametersReference", &p)
        .unwrap();
    let back = PROTO_REGISTRY
        .decode_message("IngressClassParametersReference", &bytes)
        .unwrap();
    assert_eq!(back["apiGroup"], "g", "{back}");
    assert!(back.get("aPIGroup").is_none(), "{back}");
}
