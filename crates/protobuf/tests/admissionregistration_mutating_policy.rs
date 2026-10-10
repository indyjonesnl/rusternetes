//! admissionregistration.k8s.io/{v1alpha1,v1beta1} MutatingAdmissionPolicy
//! family must be in the registry with the upstream field numbers
//! (crates/api-server/proto/upstream/v1.35/k8s.io/api/admissionregistration/
//! v1beta1/generated.proto). Refs #3057.

use rusternetes_protobuf::{FieldType, ProtoRegistry};
use serde_json::json;

fn field(reg: &ProtoRegistry, msg: &str, num: u32) -> (String, FieldType) {
    let (_, s) = reg
        .iter_schemas()
        .find(|(k, _)| *k == msg)
        .unwrap_or_else(|| panic!("{msg} not registered"));
    s.fields
        .get(&num)
        .unwrap_or_else(|| panic!("{msg} has no field #{num}"))
        .clone()
}

#[test]
fn mutating_admission_policy_messages_registered_with_upstream_numbers() {
    let r = ProtoRegistry::new();
    assert_eq!(field(&r, "MutatingAdmissionPolicy", 2).0, "spec");
    assert_eq!(field(&r, "MutatingAdmissionPolicyBinding", 2).0, "spec");
    assert_eq!(field(&r, "MutatingAdmissionPolicyList", 2).0, "items");
    assert_eq!(
        field(&r, "MutatingAdmissionPolicyBindingList", 2).0,
        "items"
    );
    assert_eq!(field(&r, "MutatingAdmissionPolicySpec", 4).0, "mutations");
    assert_eq!(
        field(&r, "MutatingAdmissionPolicySpec", 7).0,
        "reinvocationPolicy"
    );
    assert_eq!(
        field(&r, "MutatingAdmissionPolicyBindingSpec", 3).0,
        "matchResources"
    );
    assert_eq!(field(&r, "Mutation", 2).0, "patchType");
    assert_eq!(field(&r, "Mutation", 3).0, "applyConfiguration");
    assert_eq!(field(&r, "Mutation", 4).0, "jsonPatch");
    assert_eq!(field(&r, "ApplyConfiguration", 1).0, "expression");
    assert_eq!(field(&r, "JSONPatch", 1).0, "expression");
}

#[test]
fn mutating_admission_policy_round_trips() {
    let r = ProtoRegistry::new();
    let obj = json!({"metadata":{"name":"p"},"spec":{
        "failurePolicy":"Fail","reinvocationPolicy":"Never",
        "mutations":[{"patchType":"ApplyConfiguration","applyConfiguration":{"expression":"x"}},
                     {"patchType":"JSONPatch","jsonPatch":{"expression":"y"}}]}});
    let raw = r
        .encode_message("MutatingAdmissionPolicy", &obj)
        .expect("encodes");
    let back = r
        .decode_message("MutatingAdmissionPolicy", &raw)
        .expect("decodes");
    assert_eq!(back["spec"]["mutations"][1]["jsonPatch"]["expression"], "y");
    assert_eq!(
        back["spec"]["mutations"][0]["applyConfiguration"]["expression"],
        "x"
    );
    assert_eq!(back["spec"]["reinvocationPolicy"], "Never");
}
