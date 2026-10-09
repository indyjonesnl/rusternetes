//! `admissionregistration.k8s.io/v1beta1` ValidatingAdmissionPolicy and
//! Binding (#2847).
//!
//! Upstream `v1beta1Storage`
//! (pkg/registry/admissionregistration/rest/storage_apiserver.go:155-185)
//! installs `validatingadmissionpolicies` (+`/status`) and
//! `validatingadmissionpolicybindings` whenever the version is enabled, over
//! the same etcd storage as v1 (storage version v1), so an object written at
//! one version reads at the other with that version's apiVersion.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const B: &str = "/apis/admissionregistration.k8s.io/v1beta1";
const V1: &str = "/apis/admissionregistration.k8s.io/v1";
const GV_BETA: &str = "admissionregistration.k8s.io/v1beta1";
const GV_V1: &str = "admissionregistration.k8s.io/v1";

fn policy(gv: &str, name: &str) -> Value {
    json!({"apiVersion": gv, "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": name},
        "spec": {
            "matchConstraints": {"resourceRules": [{
                "apiGroups": ["apps"], "apiVersions": ["v1"],
                "operations": ["CREATE"], "resources": ["deployments"]}]},
            "validations": [{"expression": "object.spec.replicas <= 5"}]}})
}

fn binding(gv: &str, name: &str, policy: &str) -> Value {
    json!({"apiVersion": gv, "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": name},
        "spec": {"policyName": policy, "validationActions": ["Deny"]}})
}

#[tokio::test]
#[serial_test::serial]
async fn v1beta1_policy_round_trips_between_versions() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = TestApiServer::new();

    let (s, created) = api
        .post(
            &format!("{B}/validatingadmissionpolicies"),
            &policy(GV_BETA, "p"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["apiVersion"], GV_BETA);
    assert_eq!(created["metadata"]["generation"], 1);

    let (s, got) = api.get(&format!("{B}/validatingadmissionpolicies/p")).await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["apiVersion"], GV_BETA);
    assert_eq!(got["kind"], "ValidatingAdmissionPolicy");
    // SetDefaults_ValidatingAdmissionPolicySpec.
    assert_eq!(got["spec"]["failurePolicy"], "Fail");

    let (s, got) = api
        .get(&format!("{V1}/validatingadmissionpolicies/p"))
        .await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["apiVersion"], GV_V1);

    let (s, list) = api.get(&format!("{B}/validatingadmissionpolicies")).await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["apiVersion"], GV_BETA);
    assert_eq!(list["kind"], "ValidatingAdmissionPolicyList");
    assert_eq!(list["items"][0]["apiVersion"], GV_BETA);

    let (_, list) = api.get(&format!("{V1}/validatingadmissionpolicies")).await;
    assert_eq!(list["items"][0]["apiVersion"], GV_V1);

    let (s, st) = api
        .get(&format!("{B}/validatingadmissionpolicies/p/status"))
        .await;
    assert_eq!(s, StatusCode::OK, "{st}");
    assert_eq!(st["apiVersion"], GV_BETA);

    // A v1 body is refused at the v1beta1 endpoint.
    let (s, _) = api
        .post(
            &format!("{B}/validatingadmissionpolicies"),
            &policy(GV_V1, "q"),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    let (s, _) = api
        .delete(&format!("{B}/validatingadmissionpolicies/p"))
        .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
#[serial_test::serial]
async fn v1beta1_binding_round_trips_between_versions() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = TestApiServer::new();
    api.post(
        &format!("{V1}/validatingadmissionpolicies"),
        &policy(GV_V1, "p"),
    )
    .await;

    let (s, created) = api
        .post(
            &format!("{B}/validatingadmissionpolicybindings"),
            &binding(GV_BETA, "b", "p"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["apiVersion"], GV_BETA);

    let (s, got) = api
        .get(&format!("{V1}/validatingadmissionpolicybindings/b"))
        .await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["apiVersion"], GV_V1);

    let (_, list) = api
        .get(&format!("{B}/validatingadmissionpolicybindings"))
        .await;
    assert_eq!(list["apiVersion"], GV_BETA);
    assert_eq!(list["items"][0]["apiVersion"], GV_BETA);
}

#[tokio::test]
#[serial_test::serial]
async fn v1beta1_validating_resources_are_404_while_the_gate_is_off() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, false);
    let api = TestApiServer::new();
    for r in [
        "validatingadmissionpolicies",
        "validatingadmissionpolicybindings",
    ] {
        let (s, _) = api.get(&format!("{B}/{r}")).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "{r}");
    }
    let (s, _) = api
        .post(
            &format!("{B}/validatingadmissionpolicies"),
            &policy(GV_BETA, "p"),
        )
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}
