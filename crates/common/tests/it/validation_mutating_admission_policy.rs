//! MutatingAdmissionPolicy / MutatingAdmissionPolicyBinding validation (#2730).
//! Upstream: `pkg/apis/admissionregistration/validation/validation_test.go`
//! `TestValidateMutatingAdmissionPolicy` (:4229) and
//! `TestValidateMutatingAdmissionPolicyBinding` (:5556); checks at
//! `validation.go:1372-1537`.

use rusternetes_common::resources::mutating_admission_policy::{
    MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
};
use rusternetes_common::validation::mutating_admission_policy::{
    set_defaults_mutating_admission_policy, set_defaults_mutating_admission_policy_binding,
    validate_mutating_admission_policy, validate_mutating_admission_policy_binding,
};
use serde_json::{json, Value};

fn policy() -> Value {
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1beta1",
        "kind": "MutatingAdmissionPolicy",
        "metadata": {"name": "p.example.com"},
        "spec": {
            "failurePolicy": "Fail",
            "reinvocationPolicy": "Never",
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
            "mutations": [{
                "patchType": "ApplyConfiguration",
                "applyConfiguration": {"expression": "Object{}"}
            }]
        }
    })
}

fn binding() -> Value {
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1beta1",
        "kind": "MutatingAdmissionPolicyBinding",
        "metadata": {"name": "b.example.com"},
        "spec": {
            "policyName": "p.example.com",
            "paramRef": {"name": "x", "parameterNotFoundAction": "Deny"}
        }
    })
}

fn policy_errs(v: Value) -> Vec<String> {
    let p: MutatingAdmissionPolicy = serde_json::from_value(v).expect("must decode");
    validate_mutating_admission_policy(&p)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn binding_errs(v: Value) -> Vec<String> {
    let b: MutatingAdmissionPolicyBinding = serde_json::from_value(v).expect("must decode");
    validate_mutating_admission_policy_binding(&b)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

fn assert_has(errs: &[String], want: &str) {
    assert!(
        errs.iter().any(|e| e.contains(want)),
        "want {want:?} in {errs:#?}"
    );
}

#[test]
fn valid_policy_and_binding_have_no_errors() {
    assert_eq!(policy_errs(policy()), Vec::<String>::new());
    assert_eq!(binding_errs(binding()), Vec::<String>::new());
}

#[test]
fn empty_spec_reports_every_required_field() {
    // validation_test.go:4523-4531
    let errs = policy_errs(json!({"metadata": {"name": "p"}, "spec": {}}));
    assert_has(&errs, "spec.failurePolicy: Required value");
    assert_has(&errs, "spec.matchConstraints: Required value");
    assert_has(
        &errs,
        "spec.mutations: Required value: mutations must contain at least one item",
    );
    assert_has(&errs, "spec.reinvocationPolicy: Required value");
}

#[test]
fn policy_enum_values_report_unsupported_value() {
    // :4244-4267 (failurePolicy), :4288-4309 (patchType), validation.go:1420.
    let mut v = policy();
    v["spec"]["failurePolicy"] = json!("other");
    assert_has(
        &policy_errs(v),
        r#"spec.failurePolicy: Unsupported value: "other": supported values: "Fail", "Ignore""#,
    );

    let mut v = policy();
    v["spec"]["mutations"][0]["patchType"] = json!("other");
    assert_has(
        &policy_errs(v),
        r#"spec.mutations[0].patchType: Unsupported value: "other": supported values: "ApplyConfiguration", "JSONPatch""#,
    );

    let mut v = policy();
    v["spec"]["reinvocationPolicy"] = json!("other");
    assert_has(
        &policy_errs(v),
        r#"spec.reinvocationPolicy: Unsupported value: "other": supported values: "Never", "IfNeeded""#,
    );
}

#[test]
fn patch_type_is_required() {
    // :5049-5074
    let mut v = policy();
    v["spec"]["mutations"] = json!([{"applyConfiguration": {"expression": "Object{}"}}]);
    assert_has(
        &policy_errs(v),
        "spec.mutations[0].patchType: Required value",
    );
}

#[test]
fn patch_type_union_members_are_exclusive() {
    // :4311-4353 and :4355-4388
    let mut v = policy();
    v["spec"]["mutations"] = json!([{
        "patchType": "JSONPatch",
        "applyConfiguration": {"expression": "Object{}"}
    }]);
    let errs = policy_errs(v);
    assert_has(
        &errs,
        r#"spec.mutations[0].applyConfiguration: Invalid value: "{applyConfiguration}": must not be specified when patchType is JSONPatch"#,
    );
    assert_has(
        &errs,
        "spec.mutations[0].jsonPatch: Required value: must be specified when patchType is JSONPatch",
    );

    let mut v = policy();
    v["spec"]["mutations"] = json!([{
        "patchType": "ApplyConfiguration",
        "jsonPatch": {"expression": "[]"}
    }]);
    let errs = policy_errs(v);
    assert_has(
        &errs,
        r#"spec.mutations[0].jsonPatch: Invalid value: "{jsonPatch}": must not be specified when patchType is ApplyConfiguration"#,
    );
    assert_has(
        &errs,
        "spec.mutations[0].applyConfiguration: Required value: must be specified when patchType is ApplyConfiguration",
    );
}

#[test]
fn blank_patch_expression_is_required() {
    // :4371-4388, :4590-4605
    let mut v = policy();
    v["spec"]["mutations"] = json!([{
        "patchType": "JSONPatch",
        "jsonPatch": {"expression": "  "}
    }]);
    assert_has(
        &policy_errs(v),
        "spec.mutations[0].jsonPatch.expression: Required value",
    );

    let mut v = policy();
    v["spec"]["mutations"][0]["applyConfiguration"] = json!({"expression": ""});
    assert_has(
        &policy_errs(v),
        "spec.mutations[0].applyConfiguration.expression: Required value",
    );
}

#[test]
fn match_constraints_need_a_resource_rule() {
    // :4549-4564
    let mut v = policy();
    v["spec"]["matchConstraints"]["resourceRules"] = json!([]);
    assert_has(
        &policy_errs(v),
        "spec.matchConstraints.resourceRules: Required value",
    );
}

#[test]
fn delete_and_other_operations_are_not_mutable() {
    // :4741-4823; supportedMutatingOperations has no DELETE (validation.go:521).
    for op in ["DELETE", "PATCH", ""] {
        let mut v = policy();
        v["spec"]["matchConstraints"]["resourceRules"][0]["operations"] = json!([op]);
        assert_has(
            &policy_errs(v),
            &format!(
                r#"spec.matchConstraints.resourceRules.operations: Unsupported value: "{op}": supported values: "*", "CONNECT", "CREATE", "UPDATE""#
            ),
        );
    }
    let mut v = policy();
    v["spec"]["matchConstraints"]["resourceRules"][0]["operations"] =
        json!(["CREATE", "UPDATE", "CONNECT"]);
    assert_eq!(policy_errs(v), Vec::<String>::new());
}

#[test]
fn param_kind_and_match_conditions_use_the_shared_rules() {
    // :4464-4481, :5076-5093
    let mut v = policy();
    v["spec"]["paramKind"] = json!({"apiVersion": "v1"});
    assert_has(&policy_errs(v), "spec.paramKind.kind: Required value");

    let mut v = policy();
    v["spec"]["matchConditions"] = json!([{"expression": "true"}]);
    assert_has(
        &policy_errs(v),
        "spec.matchConditions[0].name: Required value",
    );
}

#[test]
fn defaulting_fills_failure_policy_and_match_resources_but_not_reinvocation() {
    // v1beta1/defaults.go:39-52, :131-137; zz_generated.defaults.go:73-86
    let mut v = policy();
    v["spec"]["failurePolicy"] = Value::Null;
    v["spec"]["reinvocationPolicy"] = Value::Null;
    v["spec"]["matchConstraints"] = json!({
        "resourceRules": [{
            "operations": ["CREATE"], "apiGroups": [""], "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    });
    let mut p: MutatingAdmissionPolicy = serde_json::from_value(v).unwrap();
    set_defaults_mutating_admission_policy(&mut p);
    let errs: Vec<String> = validate_mutating_admission_policy(&p)
        .iter()
        .map(|e| e.to_string())
        .collect();
    assert_eq!(
        errs,
        vec!["spec.reinvocationPolicy: Required value".to_string()]
    );
    let rule = &p
        .spec
        .unwrap()
        .match_constraints
        .unwrap()
        .resource_rules
        .unwrap()[0];
    assert_eq!(rule.rule.scope.as_deref(), Some("*"));
}

#[test]
fn binding_requires_a_valid_policy_name() {
    // validation.go:1514-1520
    let mut v = binding();
    v["spec"]["policyName"] = json!("");
    assert_has(&binding_errs(v), "spec.policyName: Required value");
    let mut v = binding();
    v["spec"]["policyName"] = json!("!!!");
    assert_has(&binding_errs(v), r#"spec.policyName: Invalid value: "!!!""#);
}

#[test]
fn binding_param_ref_uses_the_shared_rules() {
    // validation.go:1521 -> validateParamRef
    let mut v = binding();
    v["spec"]["paramRef"] = json!({"name": "x"});
    assert_has(
        &binding_errs(v),
        "spec.paramRef.parameterNotFoundAction: Required value",
    );
}

#[test]
fn binding_match_resources_exclude_delete() {
    // validation.go:1523-1534
    let mut v = binding();
    v["spec"]["matchResources"] = json!({
        "matchPolicy": "Equivalent",
        "namespaceSelector": {},
        "objectSelector": {},
        "resourceRules": [{
            "operations": ["DELETE"], "apiGroups": [""], "apiVersions": ["v1"],
            "resources": ["pods"], "scope": "*"
        }]
    });
    assert_has(
        &binding_errs(v),
        r#"spec.matchResources.resourceRules.operations: Unsupported value: "DELETE""#,
    );
}

#[test]
fn binding_defaulting_fills_match_resources() {
    let mut b: MutatingAdmissionPolicyBinding = serde_json::from_value({
        let mut v = binding();
        v["spec"]["matchResources"] = json!({});
        v
    })
    .unwrap();
    set_defaults_mutating_admission_policy_binding(&mut b);
    assert_eq!(binding_errs_of(&b), Vec::<String>::new());
}

fn binding_errs_of(b: &MutatingAdmissionPolicyBinding) -> Vec<String> {
    validate_mutating_admission_policy_binding(b)
        .iter()
        .map(|e| e.to_string())
        .collect()
}

#[test]
fn types_round_trip_with_upstream_field_names() {
    let p: MutatingAdmissionPolicy = serde_json::from_value(policy()).unwrap();
    let out = serde_json::to_value(&p).unwrap();
    assert_eq!(out["spec"]["reinvocationPolicy"], "Never");
    assert_eq!(
        out["spec"]["mutations"][0]["patchType"],
        "ApplyConfiguration"
    );
    let mut v = policy();
    v["spec"]["mutations"] = json!([{"patchType": "JSONPatch", "jsonPatch": {"expression": "[]"}}]);
    let p: MutatingAdmissionPolicy = serde_json::from_value(v).unwrap();
    let out = serde_json::to_value(&p).unwrap();
    assert_eq!(out["spec"]["mutations"][0]["patchType"], "JSONPatch");
}
