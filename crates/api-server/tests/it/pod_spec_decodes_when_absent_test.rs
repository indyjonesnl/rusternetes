//! Every pod-level field of a Pod decodes when the body omits it — and the
//! answer comes from a validator, with a field path, not from serde.
//!
//! Companion to `pod_container_decodes_when_absent_test.rs`, which covers the
//! container half. Go has no required JSON fields, so an omitted `os.name`,
//! scheduling-gate `name`, readiness-gate `conditionType`, resource-claim
//! `name` or `workloadRef` field decodes to the zero value and validation
//! answers. Rusternetes modelled all of them as bare non-`Option`, so the
//! answer was serde's 400 BadRequest — no `reason`, no `details.causes`, no
//! field path (#1939).
//!
//! Making them reachable exposed five validators upstream runs from
//! `validatePodSpec` and Rusternetes had no equivalent of at all —
//! `validateOS`, `validatePodResourceClaims`, `validateReadinessGates`,
//! `validateSchedulingGates` and `validateWorkloadReference` — each ported in
//! `crates/common/src/validation/pod.rs` with its citation. Without them these
//! bodies would have been silently *accepted*.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn containers() -> Value {
    json!([{ "name": "c", "image": "nginx" }])
}

/// `(label, pod spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "volume with no name",
            json!({ "containers": containers(), "volumes": [{ "emptyDir": {} }] }),
            "spec.volumes[0].name: Required value",
        ),
        (
            "os with no name",
            json!({ "containers": containers(), "os": {} }),
            "spec.os.name: Required value",
        ),
        (
            "os with an unsupported name",
            json!({ "containers": containers(), "os": { "name": "plan9" } }),
            "spec.os: Unsupported value: \"plan9\": supported values: \"linux\", \"windows\"",
        ),
        (
            "schedulingGate with no name",
            json!({ "containers": containers(), "schedulingGates": [{}] }),
            "spec.schedulingGates[0]: Invalid value: \"\": name part must be non-empty",
        ),
        (
            "duplicate schedulingGates",
            json!({ "containers": containers(), "schedulingGates": [{ "name": "a" }, { "name": "a" }] }),
            "spec.schedulingGates[1]: Duplicate value: \"a\"",
        ),
        (
            "readinessGate with no conditionType",
            json!({ "containers": containers(), "readinessGates": [{}] }),
            "spec.readinessGates[0].conditionType: Invalid value: \"\": name part must be non-empty",
        ),
        (
            "resourceClaim with no name",
            json!({ "containers": containers(), "resourceClaims": [{ "resourceClaimName": "rc" }] }),
            "spec.resourceClaims[0].name: Required value",
        ),
        (
            "resourceClaim naming both a claim and a template",
            json!({ "containers": containers(), "resourceClaims": [{ "name": "r", "resourceClaimName": "a", "resourceClaimTemplateName": "b" }] }),
            "at most one of `resourceClaimName` or `resourceClaimTemplateName` may be specified",
        ),
        (
            "workloadRef with no name",
            json!({ "containers": containers(), "workloadRef": { "podGroup": "g" } }),
            "spec.workloadRef.name: Invalid value: \"\"",
        ),
        (
            "workloadRef with no podGroup",
            json!({ "containers": containers(), "workloadRef": { "name": "w" } }),
            "spec.workloadRef.podGroup: Invalid value: \"\"",
        ),
        (
            "dnsConfig option with no name",
            json!({ "containers": containers(), "dnsPolicy": "None", "dnsConfig": { "nameservers": ["1.1.1.1"], "options": [{ "value": "v" }] } }),
            "spec.dnsConfig.options[0]: Required value",
        ),
        (
            "sysctl with no name",
            json!({ "containers": containers(), "securityContext": { "sysctls": [{ "value": "1" }] } }),
            "spec.securityContext.sysctls[0].name: Required value",
        ),
        (
            "topologySpreadConstraint with no maxSkew",
            json!({ "containers": containers(), "topologySpreadConstraints": [{ "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule", "labelSelector": { "matchLabels": { "a": "b" } } }] }),
            "spec.topologySpreadConstraints[0].maxSkew: Invalid value: 0: must be greater than 0",
        ),
        (
            "topologySpreadConstraint with no topologyKey",
            json!({ "containers": containers(), "topologySpreadConstraints": [{ "maxSkew": 1, "whenUnsatisfiable": "DoNotSchedule" }] }),
            "spec.topologySpreadConstraints[0].topologyKey: Required value",
        ),
        (
            "topologySpreadConstraint with no whenUnsatisfiable",
            json!({ "containers": containers(), "topologySpreadConstraints": [{ "maxSkew": 1, "topologyKey": "z" }] }),
            "spec.topologySpreadConstraints[0].whenUnsatisfiable: Unsupported value: \"\"",
        ),
    ]
}

#[tokio::test]
async fn every_absent_pod_level_field_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("pod-absent-{i}"), "namespace": "default" },
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

/// The other side of the audit: two of the fields defaulted here carry **no**
/// obligation, because upstream requires nothing of them either. A `sysctl`
/// with a name but no value passes `validateSysctls`
/// (`pkg/apis/core/validation/validation.go:5445-5465` checks only the name),
/// and an `imagePullSecrets` entry with no name passes
/// `validateImagePullSecrets` (`:4248-4258` only forbids setting a field other
/// than `name`). Both must therefore be written, not rejected — a 422 here
/// would be a divergence, not extra safety.
#[tokio::test]
async fn fields_upstream_leaves_optional_are_written_not_rejected() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "pod-optional-zeroes", "namespace": "default" },
                "spec": {
                    "containers": containers(),
                    "imagePullSecrets": [{}],
                    "securityContext": { "sysctls": [{ "name": "kernel.shm_rmid_forced" }] },
                },
            })),
        )
        .await;

    assert!(
        status.is_success(),
        "upstream accepts a valueless sysctl and a nameless pull secret: {status} {body}"
    );
}
