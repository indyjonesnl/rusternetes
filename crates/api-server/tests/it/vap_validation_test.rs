//! ValidatingAdmissionPolicy and its binding are validated.
//!
//! `handlers/validating_admission_policy.rs` performed no field validation at
//! all — no `Required`, no enum check, no duplicate detection (#1949). A policy
//! that matched nothing, validated nothing and named no param kind was a 201,
//! and so was a binding with no `policyName`.
//!
//! Upstream validates the whole spec in
//! `pkg/apis/admissionregistration/validation/validation.go`:
//! `validateValidatingAdmissionPolicySpec` (`:772`), `validateParamKind`
//! (`:832`), `validateMatchResources` (`:886`), `validateValidationActions`
//! (`:926`), `validateMatchConditions` (`:963`), `validateVariable` (`:999`),
//! `validateValidation` (`:1034`), `validateAuditAnnotation` (`:1128`),
//! `validateValidatingAdmissionPolicyBindingSpec` (`:1181`) and
//! `validateParamRef` (`:1198`).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const POLICIES: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";
const BINDINGS: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings";

/// A `matchConstraints` that satisfies `validateMatchResources`.
fn match_constraints() -> Value {
    json!({
        "resourceRules": [{
            "apiGroups": ["apps"],
            "apiVersions": ["v1"],
            "operations": ["CREATE"],
            "resources": ["deployments"]
        }]
    })
}

/// A valid policy spec with `patch` merged over it.
fn spec_with(patch: Value) -> Value {
    let mut spec = json!({
        "matchConstraints": match_constraints(),
        "validations": [{ "expression": "object.spec.replicas <= 5" }]
    });
    for (k, v) in patch.as_object().unwrap() {
        spec[k] = v.clone();
    }
    spec
}

fn policy_cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a spec with no matchConstraints",
            json!({ "validations": [{ "expression": "true" }] }),
            "spec.matchConstraints: Required value",
        ),
        (
            "matchConstraints with no resourceRules",
            spec_with(json!({ "matchConstraints": {} })),
            "spec.matchConstraints.resourceRules: Required value",
        ),
        (
            "a resource rule with no operations",
            spec_with(json!({ "matchConstraints": { "resourceRules": [{
                "apiGroups": ["apps"], "apiVersions": ["v1"], "resources": ["deployments"]
            }] } })),
            "spec.matchConstraints.resourceRules[0].operations: Required value",
        ),
        (
            "neither validations nor auditAnnotations",
            json!({ "matchConstraints": match_constraints() }),
            "spec.validations: Required value: validations or auditAnnotations must contain at least one item",
        ),
        (
            "a validation with no expression",
            spec_with(json!({ "validations": [{ "message": "nope" }] })),
            "spec.validations[0].expression: Required value: expression is not specified",
        ),
        (
            "a validation whose message has a line break",
            spec_with(json!({ "validations": [{
                "expression": "true", "message": "first\nsecond"
            }] })),
            "spec.validations[0].message: Invalid value: \"first\\nsecond\": must not contain line breaks",
        ),
        (
            "a validation whose messageExpression is only whitespace",
            spec_with(json!({ "validations": [{
                "expression": "true", "messageExpression": "   "
            }] })),
            "spec.validations[0].messageExpression: Invalid value: \"   \": must be non-empty if specified",
        ),
        (
            "a variable with no name",
            spec_with(json!({ "variables": [{ "expression": "object.spec" }] })),
            "spec.variables[0].name: Required value: name is not specified",
        ),
        (
            "a variable whose name is not a CEL identifier",
            spec_with(json!({ "variables": [{
                "name": "not-an-identifier", "expression": "object.spec"
            }] })),
            "spec.variables[0].name: Invalid value: \"not-an-identifier\": must be a valid CEL identifier",
        ),
        (
            "a variable with no expression",
            spec_with(json!({ "variables": [{ "name": "replicas" }] })),
            "spec.variables[0].expression: Required value: expression is not specified",
        ),
        (
            "an auditAnnotation with no valueExpression",
            spec_with(json!({ "auditAnnotations": [{ "key": "checked" }] })),
            "spec.auditAnnotations[0].valueExpression: Required value: valueExpression is not specified",
        ),
        (
            "two auditAnnotations with the same key",
            spec_with(json!({ "auditAnnotations": [
                { "key": "checked", "valueExpression": "'a'" },
                { "key": "checked", "valueExpression": "'b'" }
            ] })),
            "spec.auditAnnotations[1].key: Duplicate value",
        ),
        (
            "a paramKind with no kind",
            spec_with(json!({ "paramKind": { "apiVersion": "v1" } })),
            "spec.paramKind.kind: Required value",
        ),
        (
            "a paramKind with no apiVersion",
            spec_with(json!({ "paramKind": { "kind": "ConfigMap" } })),
            "spec.paramKind.apiVersion: Required value",
        ),
        (
            "a matchCondition with no name",
            spec_with(json!({ "matchConditions": [{ "expression": "true" }] })),
            "spec.matchConditions[0].name: Required value",
        ),
        (
            "two matchConditions with the same name",
            spec_with(json!({ "matchConditions": [
                { "name": "same", "expression": "true" },
                { "name": "same", "expression": "false" }
            ] })),
            "spec.matchConditions[1].name: Duplicate value",
        ),
    ]
}

fn binding_cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a binding with no policyName",
            json!({ "validationActions": ["Deny"] }),
            "spec.policyName: Required value",
        ),
        (
            "a binding with no validationActions",
            json!({ "policyName": "policy-1" }),
            "spec.validationActions: Required value: at least one validation action is required",
        ),
        (
            "a binding asking for both Deny and Warn",
            json!({ "policyName": "policy-1", "validationActions": ["Deny", "Warn"] }),
            "must not contain both Deny and Warn",
        ),
        (
            "a binding repeating an action",
            json!({ "policyName": "policy-1", "validationActions": ["Audit", "Audit"] }),
            "spec.validationActions[1]: Duplicate value",
        ),
        (
            "a paramRef with neither name nor selector",
            json!({
                "policyName": "policy-1", "validationActions": ["Deny"],
                "paramRef": { "parameterNotFoundAction": "Deny" }
            }),
            "spec.paramRef: Required value: one of name or selector must be specified",
        ),
        (
            "a paramRef with both name and selector",
            json!({
                "policyName": "policy-1", "validationActions": ["Deny"],
                "paramRef": {
                    "name": "params", "selector": { "matchLabels": { "a": "b" } },
                    "parameterNotFoundAction": "Deny"
                }
            }),
            "spec.paramRef.name: Forbidden: name and selector are mutually exclusive",
        ),
        (
            "a paramRef with no parameterNotFoundAction",
            json!({
                "policyName": "policy-1", "validationActions": ["Deny"],
                "paramRef": { "name": "params" }
            }),
            "spec.paramRef.parameterNotFoundAction: Required value",
        ),
    ]
}

#[tokio::test]
async fn every_bad_policy_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in policy_cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                POLICIES,
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicy",
                    "metadata": { "name": format!("policy-{i}") },
                    "spec": spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

#[tokio::test]
async fn every_bad_binding_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in binding_cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                BINDINGS,
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicyBinding",
                    "metadata": { "name": format!("binding-{i}") },
                    "spec": spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side, plus the defaulting the required fields depend on:
/// `SetDefaults_ValidatingAdmissionPolicySpec` and `SetDefaults_MatchResources`
/// (`pkg/apis/admissionregistration/v1/defaults.go:97-119`) fill
/// `failurePolicy`, `matchPolicy` and both selectors, which is exactly why
/// `validateValidatingAdmissionPolicySpec` can require them.
#[tokio::test]
async fn a_valid_policy_is_written_with_its_defaults_filled() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            POLICIES,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": { "name": "policy-ok" },
                "spec": spec_with(json!({
                    "paramKind": { "apiVersion": "v1", "kind": "ConfigMap" },
                    "variables": [{ "name": "replicas", "expression": "object.spec.replicas" }],
                    "matchConditions": [{ "name": "not-kube-system", "expression": "true" }],
                    "auditAnnotations": [{ "key": "checked", "valueExpression": "'yes'" }]
                })),
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a valid policy must be written: {status} {body}"
    );
    assert_eq!(body["spec"]["failurePolicy"], "Fail");
    assert_eq!(
        body["spec"]["matchConstraints"]["matchPolicy"],
        "Equivalent"
    );
    assert!(body["spec"]["matchConstraints"]["namespaceSelector"].is_object());
    assert!(body["spec"]["matchConstraints"]["objectSelector"].is_object());
}

#[tokio::test]
async fn a_valid_binding_is_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            BINDINGS,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicyBinding",
                "metadata": { "name": "binding-ok" },
                "spec": {
                    "policyName": "policy-ok",
                    "validationActions": ["Deny"],
                    "paramRef": { "name": "params", "parameterNotFoundAction": "Deny" },
                    "matchResources": { "resourceRules": [{
                        "apiGroups": ["apps"], "apiVersions": ["v1"],
                        "operations": ["CREATE"], "resources": ["deployments"]
                    }] }
                },
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a valid binding must be written: {status} {body}"
    );
}

/// `/status` is served by the generic cluster-status handler, which dispatches
/// per resource type. Upstream runs
/// `ValidateValidatingAdmissionPolicyStatusUpdate` (`validation.go:1247`) there
/// — `typeChecking.expressionWarnings[*].warning`/`.fieldRef` are both
/// `Required`, and the conditions go through `metav1validation.ValidateConditions`.
#[tokio::test]
async fn a_bad_policy_status_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            POLICIES,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": { "name": "policy-status" },
                "spec": spec_with(json!({})),
            })),
        )
        .await;
    assert!(status.is_success(), "seed policy: {status} {body}");

    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "an expression warning with no warning text",
            json!({ "typeChecking": { "expressionWarnings": [{ "fieldRef": "spec.validations[0].expression" }] } }),
            "status.typeChecking.expressionWarnings[0].warning: Required value",
        ),
        (
            "an expression warning with no fieldRef",
            json!({ "typeChecking": { "expressionWarnings": [{ "warning": "no such field" }] } }),
            "status.typeChecking.expressionWarnings[0].fieldRef: Required value",
        ),
        (
            "an expression warning whose fieldRef is whitespace",
            json!({ "typeChecking": { "expressionWarnings": [{ "fieldRef": "  ", "warning": "w" }] } }),
            "status.typeChecking.expressionWarnings[0].fieldRef: Required value",
        ),
        (
            "a condition with an unsupported status",
            json!({ "conditions": [{
                "type": "TypeChecked", "status": "Maybe", "reason": "Checked",
                "lastTransitionTime": "2026-01-01T00:00:00Z"
            }] }),
            "status.conditions[0].status: Unsupported value",
        ),
    ];

    for (label, status_body, expected) in cases {
        let (code, body) = api
            .send(
                "PUT",
                &format!("{POLICIES}/policy-status/status"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicy",
                    "metadata": { "name": "policy-status" },
                    "status": status_body,
                })),
            )
            .await;
        assert_eq!(
            code.as_u16(),
            422,
            "{label} must be 422 Invalid, got {code}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side of the status path: an `expressionWarnings` entry carrying
/// both fields, and no `typeChecking` at all (upstream's `validateTypeChecking`
/// returns nil for a nil pointer, `validation.go:1263-1266`).
#[tokio::test]
async fn a_valid_policy_status_is_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            POLICIES,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": { "name": "policy-status-ok" },
                "spec": spec_with(json!({})),
            })),
        )
        .await;
    assert!(status.is_success(), "seed policy: {status} {body}");

    for status_body in [
        json!({ "observedGeneration": 1 }),
        json!({ "typeChecking": { "expressionWarnings": [{
            "fieldRef": "spec.validations[0].expression",
            "warning": "apps/v1 Deployment: undefined field 'replica'"
        }] } }),
    ] {
        let (code, body) = api
            .send(
                "PUT",
                &format!("{POLICIES}/policy-status-ok/status"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicy",
                    "metadata": { "name": "policy-status-ok" },
                    "status": status_body,
                })),
            )
            .await;
        assert!(
            code.is_success(),
            "a valid status must be written: {code} {body}"
        );
    }
}
