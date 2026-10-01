//! admissionregistration.k8s.io/v1 on the generic Store (#1990): the strategy
//! rules of `pkg/registry/admissionregistration/*/strategy.go` and the v1
//! defaults of `pkg/apis/admissionregistration/v1/defaults.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const VAP: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";
const VAPB: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings";
const VWC: &str = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations";
const MWC: &str = "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations";

fn policy(name: &str) -> Value {
    json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
           "metadata": {"name": name},
           "spec": {
               "matchConstraints": {"resourceRules": [{
                   "apiGroups": ["apps"], "apiVersions": ["v1"],
                   "operations": ["CREATE"], "resources": ["deployments"]}]},
               "validations": [{"expression": "object.spec.replicas <= 5"}]}})
}

fn binding(name: &str, policy: &str) -> Value {
    json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
           "metadata": {"name": name},
           "spec": {"policyName": policy, "validationActions": ["Deny"],
                    "matchResources": {"resourceRules": [{
                        "apiGroups": ["apps"], "apiVersions": ["v1"],
                        "operations": ["CREATE"], "resources": ["deployments"]}]}}})
}

fn hook(name: &str) -> Value {
    json!({"name": name,
           "clientConfig": {"service": {"namespace": "ns", "name": "svc", "path": "/v"}},
           "rules": [{"operations": ["CREATE"], "apiGroups": [""], "apiVersions": ["v1"],
                      "resources": ["configmaps"]}],
           "sideEffects": "None", "admissionReviewVersions": ["v1"]})
}

fn config(kind: &str, name: &str, hooks: Vec<Value>) -> Value {
    json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": kind,
           "metadata": {"name": name}, "webhooks": hooks})
}

/// `PrepareForCreate`: generation 1, empty status. `SetObjectDefaults_
/// ValidatingAdmissionPolicy`: failurePolicy, matchPolicy, selectors and the
/// rule scope.
#[tokio::test]
async fn a_policy_is_defaulted_and_gets_generation_one() {
    let api = TestApiServer::new();
    let (s, body) = api.post(VAP, &policy("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    assert_eq!(body["spec"]["failurePolicy"], "Fail", "{body}");
    let mc = &body["spec"]["matchConstraints"];
    assert_eq!(mc["matchPolicy"], "Equivalent", "{body}");
    assert_eq!(mc["namespaceSelector"], json!({}), "{body}");
    assert_eq!(mc["objectSelector"], json!({}), "{body}");
    assert_eq!(mc["resourceRules"][0]["scope"], "*", "{body}");
}

/// `PrepareForCreate` clears the status.
#[tokio::test]
async fn a_policy_create_clears_the_status() {
    let api = TestApiServer::new();
    let mut p = policy("p1");
    p["status"] = json!({"observedGeneration": 7});
    let (s, body) = api.post(VAP, &p).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert!(body["status"].get("observedGeneration").is_none(), "{body}");
}

/// `PrepareForUpdate`: a spec change bumps the generation, a metadata change
/// does not, and a PUT cannot write the status.
#[tokio::test]
async fn a_spec_change_bumps_the_policy_generation() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAP, &policy("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    labelled["status"] = json!({"observedGeneration": 9});
    let (s, body) = api.put(&format!("{VAP}/p1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    assert!(body["status"].get("observedGeneration").is_none(), "{body}");

    let mut changed = body.clone();
    changed["spec"]["validations"][0]["expression"] = json!("object.spec.replicas <= 9");
    let (s, body) = api.put(&format!("{VAP}/p1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
}

/// `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_admission_object_is_not_found() {
    let api = TestApiServer::new();
    for (uri, body) in [
        (format!("{VAP}/missing"), policy("missing")),
        (
            format!("{VAPB}/missing"),
            binding("missing", "some-policy"),
        ),
        (
            format!("{VWC}/missing"),
            config("ValidatingWebhookConfiguration", "missing", vec![]),
        ),
        (
            format!("{MWC}/missing"),
            config("MutatingWebhookConfiguration", "missing", vec![]),
        ),
    ] {
        let (s, out) = api.put(&uri, &body).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{uri}: {out}");
    }
}

/// `AllowUnconditionalUpdate() == false`: a PUT needs a resourceVersion.
#[tokio::test]
async fn a_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAP, &policy("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    let (s, body) = api.put(&format!("{VAP}/p1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("must be specified for an update"),
        "{body}"
    );
}

/// `ValidateObjectMeta(..., NameIsDNSSubdomain, ...)` runs in every validator.
#[tokio::test]
async fn a_non_dns_subdomain_name_is_rejected() {
    let api = TestApiServer::new();
    for (uri, body) in [
        (VAP, policy("Bad_Name")),
        (VAPB, binding("Bad_Name", "p")),
        (
            VWC,
            config("ValidatingWebhookConfiguration", "Bad_Name", vec![]),
        ),
        (
            MWC,
            config("MutatingWebhookConfiguration", "Bad_Name", vec![]),
        ),
    ] {
        let (s, out) = api.post(uri, &body).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {out}");
        assert!(
            out["message"].as_str().unwrap().contains("metadata.name"),
            "{uri}: {out}"
        );
    }
}

/// `validatingAdmissionPolicyStatusStrategy`: the status is written, the spec
/// is kept, and an invalid condition is rejected.
#[tokio::test]
async fn policy_status_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAP, &policy("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["validations"][0]["expression"] = json!("false");
    update["status"] = json!({"observedGeneration": 1});
    let (s, body) = api.put(&format!("{VAP}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["observedGeneration"], 1, "{body}");
    assert_eq!(
        body["spec"]["validations"][0]["expression"], "object.spec.replicas <= 5",
        "{body}"
    );
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut bad = body.clone();
    bad["status"] = json!({"conditions": [{"status": "True"}]});
    let (s, body) = api.put(&format!("{VAP}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// A PATCH of `/status` goes through the status strategy.
#[tokio::test]
async fn policy_status_patch_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAP, &policy("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let patch = json!({"spec": {"failurePolicy": "Ignore"},
                       "status": {"observedGeneration": 1}});
    let (s, body) = api.patch(&format!("{VAP}/p1/status"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["observedGeneration"], 1, "{body}");
    assert_eq!(body["spec"]["failurePolicy"], "Fail", "{body}");
}

/// Binding: generation 1, `matchResources` defaulted; a spec change bumps
/// the generation.
#[tokio::test]
async fn a_binding_is_defaulted_and_its_generation_follows_the_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAPB, &binding("b1", "p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");
    let mr = &created["spec"]["matchResources"];
    assert_eq!(mr["matchPolicy"], "Equivalent", "{created}");
    assert_eq!(mr["resourceRules"][0]["scope"], "*", "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{VAPB}/b1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut changed = body.clone();
    changed["spec"]["validationActions"] = json!(["Warn"]);
    let (s, body) = api.put(&format!("{VAPB}/b1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
}

/// `SetDefaults_ValidatingWebhook`, `SetDefaults_Rule`,
/// `SetDefaults_ServiceReference`; `PrepareForCreate` sets generation 1.
#[tokio::test]
async fn a_validating_webhook_is_defaulted() {
    let api = TestApiServer::new();
    let (s, body) = api
        .post(
            VWC,
            &config(
                "ValidatingWebhookConfiguration",
                "v1",
                vec![hook("a.example.com")],
            ),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    let w = &body["webhooks"][0];
    assert_eq!(w["failurePolicy"], "Fail", "{body}");
    assert_eq!(w["matchPolicy"], "Equivalent", "{body}");
    assert_eq!(w["namespaceSelector"], json!({}), "{body}");
    assert_eq!(w["objectSelector"], json!({}), "{body}");
    assert_eq!(w["timeoutSeconds"], 10, "{body}");
    assert_eq!(w["rules"][0]["scope"], "*", "{body}");
    assert_eq!(w["clientConfig"]["service"]["port"], 443, "{body}");
}

/// `SetDefaults_MutatingWebhook` additionally defaults `reinvocationPolicy`.
#[tokio::test]
async fn a_mutating_webhook_is_defaulted() {
    let api = TestApiServer::new();
    let (s, body) = api
        .post(
            MWC,
            &config(
                "MutatingWebhookConfiguration",
                "m1",
                vec![hook("a.example.com")],
            ),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    let w = &body["webhooks"][0];
    assert_eq!(w["reinvocationPolicy"], "Never", "{body}");
    assert_eq!(w["failurePolicy"], "Fail", "{body}");
    assert_eq!(w["timeoutSeconds"], 10, "{body}");
    assert_eq!(w["clientConfig"]["service"]["port"], 443, "{body}");
}

/// Explicit values survive defaulting.
#[tokio::test]
async fn explicit_webhook_values_are_kept() {
    let api = TestApiServer::new();
    let mut h = hook("a.example.com");
    h["failurePolicy"] = json!("Ignore");
    h["timeoutSeconds"] = json!(3);
    h["clientConfig"]["service"]["port"] = json!(8443);
    let (s, body) = api
        .post(MWC, &config("MutatingWebhookConfiguration", "m1", vec![h]))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let w = &body["webhooks"][0];
    assert_eq!(w["failurePolicy"], "Ignore", "{body}");
    assert_eq!(w["timeoutSeconds"], 3, "{body}");
    assert_eq!(w["clientConfig"]["service"]["port"], 8443, "{body}");
}

/// The strategies bump the generation when `webhooks` changes, not on a
/// metadata change (strategy.go `PrepareForUpdate`).
#[tokio::test]
async fn a_webhooks_change_bumps_the_configuration_generation() {
    let api = TestApiServer::new();
    for (uri, kind) in [
        (VWC, "ValidatingWebhookConfiguration"),
        (MWC, "MutatingWebhookConfiguration"),
    ] {
        let (s, created) = api
            .post(uri, &config(kind, "c1", vec![hook("a.example.com")]))
            .await;
        assert_eq!(s, StatusCode::CREATED, "{created}");

        let mut labelled = created.clone();
        labelled["metadata"]["labels"] = json!({"a": "b"});
        let (s, body) = api.put(&format!("{uri}/c1"), &labelled).await;
        assert_eq!(s, StatusCode::OK, "{body}");
        assert_eq!(body["metadata"]["generation"], 1, "{body}");

        let mut changed = body.clone();
        changed["webhooks"][0]["timeoutSeconds"] = json!(5);
        let (s, body) = api.put(&format!("{uri}/c1"), &changed).await;
        assert_eq!(s, StatusCode::OK, "{body}");
        assert_eq!(body["metadata"]["generation"], 2, "{body}");
    }
}

/// An unparsable `matchConditions` expression is a field error on
/// `webhooks[0].matchConditions[0].expression` (validateMatchCondition,
/// validation.go:989 -> validateMatchConditionsExpression).
#[tokio::test]
async fn a_match_condition_compile_failure_is_a_field_error() {
    let api = TestApiServer::new();
    let mut h = hook("a.example.com");
    h["matchConditions"] = json!([{"name": "bad", "expression": "this is ((not cel"}]);
    let (s, body) = api
        .post(VWC, &config("ValidatingWebhookConfiguration", "v1", vec![h]))
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let field = body["details"]["causes"][0]["field"]
        .as_str()
        .unwrap_or_default();
    assert_eq!(field, "webhooks[0].matchConditions[0].expression", "{body}");
}
