//! The eight remaining core resource modules decode when a field is absent.
//!
//! #1939: Go has no required JSON fields. An absent key decodes to the zero
//! value and *validation* answers `422 Invalid` with a field path; a bare
//! non-`Option` Rust field instead answers serde's `400 BadRequest`, with no
//! `Status`, no `reason` and no `details.causes`.
//!
//! `certificates.rs`, `componentstatus.rs`, `config_and_secret.rs`,
//! `controllerrevision.rs`, `coordination.rs`, `namespace.rs`,
//! `runtimeclass.rs` and `servicecidr.rs` already model every wire field as
//! either `Option` or `#[serde(default)]`, and the validators upstream requires
//! are already ported. This file is the behavioural half of that claim: for
//! each of them, a body that omits the field upstream requires must come back
//! as a `422` naming the field, never as a `400`.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// `(label, path, body, expected substring)`.
fn cases() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    vec![
        (
            "a CSR with no spec at all",
            "/apis/certificates.k8s.io/v1/certificatesigningrequests",
            json!({
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": { "name": "csr-no-spec" },
            }),
            "spec.usages",
        ),
        (
            "a CSR with no usages",
            "/apis/certificates.k8s.io/v1/certificatesigningrequests",
            json!({
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": { "name": "csr-no-usages" },
                "spec": { "signerName": "example.com/signer" },
            }),
            "spec.usages: Required value",
        ),
        (
            "a ControllerRevision with no data",
            "/apis/apps/v1/namespaces/default/controllerrevisions",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ControllerRevision",
                "metadata": { "name": "rev-no-data" },
                "revision": 1,
            }),
            "data",
        ),
        (
            "a ServiceCIDR with no spec at all",
            "/apis/networking.k8s.io/v1/servicecidrs",
            json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "ServiceCIDR",
                "metadata": { "name": "cidr-no-spec" },
            }),
            "cidrs",
        ),
        (
            "a RuntimeClass with no handler",
            "/apis/node.k8s.io/v1/runtimeclasses",
            json!({
                "apiVersion": "node.k8s.io/v1",
                "kind": "RuntimeClass",
                "metadata": { "name": "rc-no-handler" },
            }),
            "handler",
        ),
    ]
}

#[tokio::test]
async fn every_core_module_answers_422_not_400_when_a_field_is_absent() {
    let api = TestApiServer::new();

    for (label, path, body, expected) in cases() {
        let (status, response) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be 422 Invalid, got {status}: {response}"
        );
        let message = response["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must name `{expected}`, got: {message}"
        );
    }
}

/// The accept side: the modules with no upstream-required spec field — a Lease,
/// a Namespace, a ConfigMap and a Secret — take a body that carries metadata
/// only. Upstream's `ValidateLease` and `ValidateNamespace` are ObjectMeta-only
/// (`pkg/apis/coordination/validation/validation.go:29`,
/// `pkg/apis/core/validation/validation.go:4909`), so an empty spec is a 201,
/// not a 400 and not a 422.
#[tokio::test]
async fn a_metadata_only_body_is_accepted_where_upstream_requires_nothing() {
    let api = TestApiServer::new();

    let accepts: Vec<(&str, &str, Value)> = vec![
        (
            "a Lease with no spec",
            "/apis/coordination.k8s.io/v1/namespaces/default/leases",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": { "name": "lease-bare" },
            }),
        ),
        (
            "a Namespace with no spec",
            "/api/v1/namespaces",
            json!({ "apiVersion": "v1", "kind": "Namespace", "metadata": { "name": "ns-bare" } }),
        ),
        (
            "a ConfigMap with no data",
            "/api/v1/namespaces/default/configmaps",
            json!({ "apiVersion": "v1", "kind": "ConfigMap", "metadata": { "name": "cm-bare" } }),
        ),
        (
            "a Secret with no data and no type",
            "/api/v1/namespaces/default/secrets",
            json!({ "apiVersion": "v1", "kind": "Secret", "metadata": { "name": "secret-bare" } }),
        ),
    ];

    for (label, path, body) in accepts {
        let (status, response) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;
        assert!(
            status.is_success(),
            "{label} must be accepted, got {status}: {response}"
        );
    }
}
