//! Unknown values of a policy's string-typed enum fields decode and validation
//! reports `Unsupported value` (422), as upstream's plain `string` types do
//! (#2475). Upstream: validation_test.go:2050 (failurePolicy), :2159 (reason),
//! :2275 (matchPolicy), :3924 (validationActions), :3976
//! (parameterNotFoundAction); checks at validation.go:785, :1058, :894, :931,
//! :1231, :547.

use rusternetes_common::resources::validating_admission_policy::{
    ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding,
};
use rusternetes_common::validation::validating_admission_policy::{
    validate_validating_admission_policy, validate_validating_admission_policy_binding,
};
use serde_json::{json, Value};

fn policy() -> Value {
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "p.example.com"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "matchPolicy": "Equivalent",
                "namespaceSelector": {},
                "objectSelector": {},
                "resourceRules": [{
                    "operations": ["CREATE"],
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "resources": ["pods"],
                    "scope": "*"
                }]
            },
            "validations": [{"expression": "true"}]
        }
    })
}

fn binding() -> Value {
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "b.example.com"},
        "spec": {
            "policyName": "p.example.com",
            "validationActions": ["Deny"],
            "paramRef": {"name": "x", "parameterNotFoundAction": "Deny"}
        }
    })
}

fn policy_errs(v: Value) -> Vec<String> {
    let p: ValidatingAdmissionPolicy = serde_json::from_value(v).expect("must decode");
    validate_validating_admission_policy(&p)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn binding_errs(v: Value) -> Vec<String> {
    let b: ValidatingAdmissionPolicyBinding = serde_json::from_value(v).expect("must decode");
    validate_validating_admission_policy_binding(&b)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

#[test]
fn valid_policy_and_binding_have_no_errors() {
    assert_eq!(policy_errs(policy()), Vec::<String>::new());
    assert_eq!(binding_errs(binding()), Vec::<String>::new());
}

#[test]
fn unknown_policy_enum_values_report_unsupported_value() {
    type Tweak = fn(&mut Value);
    let cases: Vec<(Tweak, &str)> = vec![
        (
            |v| v["spec"]["failurePolicy"] = json!("other"),
            "spec.failurePolicy: Unsupported value: \"other\": supported values: \"Fail\", \"Ignore\"",
        ),
        (
            |v| v["spec"]["matchConstraints"]["matchPolicy"] = json!("other"),
            "spec.matchConstraints.matchPolicy: Unsupported value: \"other\": supported values: \"Equivalent\", \"Exact\"",
        ),
        (
            |v| v["spec"]["validations"][0]["reason"] = json!("other"),
            "spec.validations[0].reason: Unsupported value: \"other\": supported values: \"Forbidden\", \"Invalid\", \"RequestEntityTooLarge\"",
        ),
        (
            |v| v["spec"]["matchConstraints"]["resourceRules"][0]["operations"] = json!(["PATCH"]),
            "spec.matchConstraints.resourceRules[0].operations[0]: Unsupported value: \"PATCH\": supported values: \"*\", \"CONNECT\", \"CREATE\", \"DELETE\", \"UPDATE\"",
        ),
    ];
    for (tweak, want) in cases {
        let mut v = policy();
        tweak(&mut v);
        let errs = policy_errs(v);
        assert!(
            errs.iter().any(|e| e.contains(want)),
            "want {want:?} in {errs:?}"
        );
    }
}

/// `supportedValidationPolicyReason` admits only three reasons, so a real
/// `StatusReason` outside them is still unsupported (validation.go:1058).
#[test]
fn reason_outside_supported_set_is_unsupported_and_supported_ones_pass() {
    let mut v = policy();
    v["spec"]["validations"][0]["reason"] = json!("NotFound");
    let errs = policy_errs(v);
    assert!(
        errs.iter()
            .any(|e| e.contains("reason: Unsupported value: \"NotFound\"")),
        "{errs:?}"
    );
    for ok in ["Forbidden", "Invalid", "RequestEntityTooLarge"] {
        let mut v = policy();
        v["spec"]["validations"][0]["reason"] = json!(ok);
        assert_eq!(policy_errs(v), Vec::<String>::new(), "{ok}");
    }
}

#[test]
fn unknown_binding_enum_values_report_unsupported_value() {
    let mut v = binding();
    v["spec"]["validationActions"] = json!(["illegal"]);
    let errs = binding_errs(v);
    let want = "spec.validationActions[0]: Unsupported value: \"illegal\": supported values: \"Audit\", \"Deny\", \"Warn\"";
    assert!(errs.iter().any(|e| e.contains(want)), "{errs:?}");

    let mut v = binding();
    v["spec"]["paramRef"]["parameterNotFoundAction"] = json!("invalid");
    let errs = binding_errs(v);
    let want = "spec.paramRef.parameterNotFoundAction: Unsupported value: \"invalid\": supported values: \"Deny\", \"Allow\"";
    assert!(errs.iter().any(|e| e.contains(want)), "{errs:?}");
}

#[test]
fn unknown_values_round_trip() {
    let mut v = policy();
    v["spec"]["failurePolicy"] = json!("other");
    v["spec"]["validations"][0]["reason"] = json!("weird");
    let p: ValidatingAdmissionPolicy = serde_json::from_value(v).unwrap();
    let back = serde_json::to_value(&p).unwrap();
    assert_eq!(back["spec"]["failurePolicy"], "other");
    assert_eq!(back["spec"]["validations"][0]["reason"], "weird");
}
