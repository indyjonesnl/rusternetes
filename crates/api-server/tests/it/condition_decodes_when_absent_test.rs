//! A status condition decodes when the body omits one of its fields.
//!
//! Go has no required JSON fields. A condition arrives through
//! `decoder.Decode(body, &defaultGVK, obj)` like everything else, so an absent
//! `status`, `reason` or `message` decodes to `""` and the object reaches
//! validation — which either rejects it with a `Status` naming the field
//! (`metav1validation.ValidateCondition`,
//! `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation.go:315-350`,
//! called for every `[]metav1.Condition` list) or accepts it, for the older
//! typed conditions upstream never validates.
//!
//! A bare non-`Option` Rust field answers 400 BadRequest instead, from serde,
//! before any validator: no `reason`, no `details.causes`, no field path. This
//! is the nested half of #1937's rule — that sweep sends
//! `{"apiVersion","kind","metadata"}` only, so it cannot see a field inside a
//! status (#1939).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// Whatever the answer is, it must not be the decoder's: a 400 here means the
/// body never reached a validator.
fn assert_not_a_decode_failure(status: axum::http::StatusCode, body: &Value, whose: &str) {
    assert_ne!(
        status.as_u16(),
        400,
        "{whose} was rejected by the decoder, before any validation: {body}"
    );
}

/// Deployment conditions are the old typed shape (`DeploymentCondition`), and
/// upstream validates nothing about them — `ValidateDeploymentStatusUpdate`
/// (`pkg/apis/apps/validation/validation.go`) checks the replica counters only.
/// So a condition with no `status` is *accepted* upstream, and the only wrong
/// answer is the decoder's.
///
/// The write goes to the main endpoint, not `/status`: that is where the whole
/// object is decoded into the typed struct, and where a client sending back an
/// object it read (or built) with a partial condition is stopped.
#[tokio::test]
async fn a_deployment_condition_without_a_status_is_not_a_decode_failure() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/deployments",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": { "name": "cond-dep", "namespace": "default" },
                "spec": {
                    "replicas": 1,
                    "selector": { "matchLabels": { "app": "c" } },
                    "template": {
                        "metadata": { "labels": { "app": "c" } },
                        "spec": { "containers": [{ "name": "c", "image": "nginx" }] },
                    },
                },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");

    let mut put = created.clone();
    put["status"] = json!({
        "conditions": [{ "type": "Progressing" }],
    });
    let (status, answer) = api
        .send(
            "PUT",
            "/apis/apps/v1/namespaces/default/deployments/cond-dep",
            Some("application/json"),
            Some(&put),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a Deployment condition with no `status`");
    assert!(
        status.is_success(),
        "upstream validates nothing about a DeploymentCondition, so this is a \
         write: {status} {answer}"
    );
}

/// PodDisruptionBudget conditions *are* `[]metav1.Condition` upstream, and
/// `ValidatePodDisruptionBudgetStatusUpdate` runs `ValidateConditions` over them
/// (`pkg/apis/policy/validation/validation.go:82`). So a condition with no
/// `status` is a 422 naming `status` — the validator's answer, with a field
/// path a client can act on, not the decoder's 400.
#[tokio::test]
async fn a_pdb_condition_without_a_status_is_a_422_naming_the_field() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "policy/v1",
                "kind": "PodDisruptionBudget",
                "metadata": { "name": "cond-pdb", "namespace": "default" },
                "spec": {
                    "minAvailable": 1,
                    "selector": { "matchLabels": { "app": "c" } },
                },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");

    let mut put = created.clone();
    put["status"] = json!({
        "currentHealthy": 1,
        "desiredHealthy": 1,
        "disruptionsAllowed": 0,
        "expectedPods": 1,
        "observedGeneration": 1,
        "conditions": [{
            "type": "DisruptionAllowed",
            "reason": "SufficientPods",
            "lastTransitionTime": "2026-09-10T10:00:00Z",
        }],
    });
    let (status, answer) = api
        .send(
            "PUT",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets/cond-pdb/status",
            Some("application/json"),
            Some(&put),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a PDB condition with no `status`");
    assert_eq!(
        status.as_u16(),
        422,
        "`ValidateConditions` rejects an empty status: {status} {answer}"
    );
    assert!(
        answer.to_string().contains("status"),
        "the rejection names the field: {answer}"
    );
}

/// `ServiceCIDRCondition` is `metav1.Condition` too, where `message` is
/// optional (`ValidateCondition` bounds its length and nothing else,
/// `validation.go:346-348`). Requiring it at decode time made a perfectly good
/// status update a 400.
#[tokio::test]
async fn a_service_cidr_condition_without_a_message_decodes() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            "/apis/networking.k8s.io/v1/servicecidrs",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "ServiceCIDR",
                "metadata": { "name": "cond-cidr" },
                "spec": { "cidrs": ["10.96.0.0/16"] },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");

    let mut put = created.clone();
    put["status"] = json!({
        "conditions": [{
            "type": "Ready",
            "status": "True",
            "reason": "Allocated",
            "lastTransitionTime": "2026-09-10T10:00:00Z",
        }],
    });
    let (status, answer) = api
        .send(
            "PUT",
            "/apis/networking.k8s.io/v1/servicecidrs/cond-cidr",
            Some("application/json"),
            Some(&put),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a ServiceCIDR condition with no `message`");
    assert!(status.is_success(), "{status} {answer}");
    assert_eq!(
        answer["status"]["conditions"][0]["type"],
        json!("Ready"),
        "{answer}"
    );
}
