//! A body that omits a field these modules used to require decodes, and the
//! validator answers.
//!
//! The #1939 slice for `rbac.rs`, `flowcontrol.rs`, `ipaddress.rs`,
//! `endpointslice.rs`, `node.rs`, `networking.rs` and `service.rs`. Every field
//! involved is required *upstream by validation*, never by the decoder, so the
//! answer must be a 422 `Status` with a field path — not serde's 400
//! BadRequest, which carries neither.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn assert_not_a_decode_failure(status: axum::http::StatusCode, body: &Value, whose: &str) {
    assert_ne!(
        status.as_u16(),
        400,
        "{whose} was rejected by the decoder, before any validation: {body}"
    );
}

/// `rules[].verbs` is required by `ValidatePolicyRule`
/// (`pkg/apis/rbac/validation/validation.go`: "verbs must contain at least one
/// value"), not by the decoder.
#[tokio::test]
async fn a_role_rule_without_verbs_is_a_422_naming_verbs() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": { "name": "no-verbs", "namespace": "default" },
                "rules": [{ "apiGroups": [""], "resources": ["pods"] }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a rule with no verbs");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("verbs"),
        "the rejection names the field: {answer}"
    );
}

/// A RoleBinding subject with no `kind`: upstream answers `NotSupported` on
/// `subjects[0].kind` (`validation.go`, `supportedSubjectKinds`).
#[tokio::test]
async fn a_role_binding_subject_without_a_kind_is_a_422() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": { "name": "no-kind", "namespace": "default" },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "some-role",
                },
                "subjects": [{ "name": "alice" }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a subject with no kind");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("kind"),
        "the rejection names the field: {answer}"
    );
}

/// A FlowSchema subject with no `kind`. `ValidateFlowSchemaSubject`'s `default`
/// arm answers `NotSupported` (`pkg/apis/flowcontrol/validation/validation.go:185-187`);
/// the kind is what decides which of `user`/`group`/`serviceAccount` is read,
/// so it cannot be guessed.
#[tokio::test]
async fn a_flow_schema_subject_without_a_kind_is_a_422() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
                "kind": "FlowSchema",
                "metadata": { "name": "no-kind" },
                "spec": {
                    "matchingPrecedence": 1000,
                    "priorityLevelConfiguration": { "name": "global-default" },
                    "rules": [{
                        "subjects": [{ "user": { "name": "alice" } }],
                        "resourceRules": [{
                            "verbs": ["get"],
                            "apiGroups": [""],
                            "resources": ["pods"],
                        }],
                    }],
                },
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a flow schema subject with no kind");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("kind"),
        "the rejection names the field: {answer}"
    );
}

/// An IPAddress with no `parentRef`. Upstream's field is a pointer
/// (`staging/src/k8s.io/api/networking/v1/types.go:669`) and an absent one is
/// `field.Required(spec.parentRef)`
/// (`pkg/apis/networking/validation/validation.go:772`) — so the answer names
/// `parentRef` itself, not the fields inside it.
#[tokio::test]
async fn an_ip_address_without_a_parent_ref_is_a_422_naming_parent_ref() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/networking.k8s.io/v1/ipaddresses",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "IPAddress",
                "metadata": { "name": "10.96.0.10" },
                "spec": {},
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "an IPAddress with an empty spec");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("parentRef"),
        "the rejection names `parentRef`, not the fields inside it: {answer}"
    );
}

/// An EndpointSlice endpoint with no `addresses`: upstream requires at least
/// one (`pkg/apis/discovery/validation/validation.go`).
#[tokio::test]
async fn an_endpoint_without_addresses_is_a_422() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": { "name": "no-addrs", "namespace": "default" },
                "addressType": "IPv4",
                "endpoints": [{ "conditions": { "ready": true } }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "an endpoint with no addresses");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("addresses"),
        "the rejection names the field: {answer}"
    );
}

/// A Service port with no `port`: 0 is outside the valid range, so the
/// validator answers, where the decoder used to.
#[tokio::test]
async fn a_service_port_without_a_number_is_a_422() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/services",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": { "name": "no-port", "namespace": "default" },
                "spec": {
                    "selector": { "app": "x" },
                    "ports": [{ "name": "http", "protocol": "TCP" }],
                },
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a service port with no number");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    assert!(
        answer.to_string().contains("port"),
        "the rejection names the field: {answer}"
    );
}

/// A NetworkPolicy with no `podSelector`. Here the *write* is the right answer:
/// upstream's field is a value `metav1.LabelSelector`, so an absent one is the
/// empty selector, which selects every pod in the namespace
/// (`staging/src/k8s.io/api/networking/v1/types.go`, `PodSelector`). Defaulting
/// it reproduces that; rejecting it at decode time did not.
#[tokio::test]
async fn a_network_policy_without_a_pod_selector_selects_every_pod() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": { "name": "all-pods", "namespace": "default" },
                "spec": { "policyTypes": ["Ingress"] },
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &created, "a NetworkPolicy with no podSelector");
    assert!(status.is_success(), "{status} {created}");
}
