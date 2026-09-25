//! Every autoscaling wire field decodes when it is absent.
//!
//! #1939: Go has no required JSON fields. An absent key decodes to the zero
//! value and *validation* answers `422 Invalid` with a field path; a bare
//! non-`Option` Rust field answers serde's `400 BadRequest` instead, with no
//! `Status`, no `reason` and no `details.causes`.
//!
//! `autoscaling.rs` carried 41 such fields. Every one of the HPA ones is
//! already covered by the port of
//! `pkg/apis/autoscaling/validation/validation.go` in
//! `crates/common/src/validation/hpa.rs`, so defaulting them moves the answer
//! from serde to that validator rather than turning it into a silent accept —
//! this file is the proof, one row per rejection upstream makes.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const HPAS: &str = "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers";

fn hpa(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": { "name": name },
        "spec": spec,
    })
}

fn scale_target_ref() -> Value {
    json!({ "apiVersion": "apps/v1", "kind": "Deployment", "name": "web" })
}

/// `(label, spec, expected substring)`. Each spec omits exactly the field the
/// row is about; the answer must be the 422 upstream gives, not a 400.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "no maxReplicas",
            json!({ "scaleTargetRef": scale_target_ref() }),
            "spec.maxReplicas: Invalid value: 0: must be greater than 0",
        ),
        (
            "no scaleTargetRef at all",
            json!({ "maxReplicas": 3 }),
            "spec.scaleTargetRef.kind: Required value",
        ),
        (
            "a scaleTargetRef with no name",
            json!({ "maxReplicas": 3, "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment" } }),
            "spec.scaleTargetRef.name: Required value",
        ),
        (
            "a metric with no type",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "resource": { "name": "cpu", "target": { "type": "Utilization", "averageUtilization": 50 } } }]
            }),
            "spec.metrics[0].type: Required value",
        ),
        (
            "a Resource metric with no name",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "type": "Resource", "resource": { "target": { "type": "Utilization", "averageUtilization": 50 } } }]
            }),
            "spec.metrics[0].resource.name",
        ),
        (
            "a Resource metric whose target has no type",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "type": "Resource", "resource": { "name": "cpu", "target": {} } }]
            }),
            "spec.metrics[0].resource.target.type: Required value",
        ),
        (
            "a Pods metric whose identifier has no name",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "type": "Pods", "pods": { "metric": {}, "target": { "type": "AverageValue", "averageValue": "10" } } }]
            }),
            "spec.metrics[0].pods.metric.name: Required value",
        ),
        (
            "an Object metric whose describedObject is absent",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "type": "Object", "object": {
                    "metric": { "name": "requests" },
                    "target": { "type": "Value", "value": "10" }
                } }]
            }),
            "spec.metrics[0].object.describedObject.kind: Required value",
        ),
        (
            "a ContainerResource metric with no container",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "metrics": [{ "type": "ContainerResource", "containerResource": {
                    "name": "cpu",
                    "target": { "type": "Utilization", "averageUtilization": 50 }
                } }]
            }),
            "spec.metrics[0].containerResource.container",
        ),
        (
            "a scaling policy with no type, value or periodSeconds",
            json!({
                "maxReplicas": 3,
                "scaleTargetRef": scale_target_ref(),
                "behavior": { "scaleUp": { "policies": [{}] } }
            }),
            "spec.behavior.scaleUp.policies[0].value: Invalid value: 0: must be greater than zero",
        ),
    ]
}

#[tokio::test]
async fn every_absent_hpa_field_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (label, spec, expected) in cases() {
        let (status, body) = api
            .send(
                "POST",
                HPAS,
                Some("application/json"),
                Some(&hpa("hpa-bad", spec)),
            )
            .await;
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be 422 Invalid, got {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side. An HPA status with no counters at all is upstream's zero
/// value, which `ValidateHorizontalPodAutoscalerStatusUpdate` accepts
/// (`currentReplicas`/`desiredReplicas` only have to be `>= 0`), so it must not
/// become a 400 here either.
#[tokio::test]
async fn a_minimal_hpa_and_an_empty_status_are_accepted() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            HPAS,
            Some("application/json"),
            Some(&hpa(
                "hpa-ok",
                json!({ "maxReplicas": 3, "scaleTargetRef": scale_target_ref() }),
            )),
        )
        .await;
    assert!(
        status.is_success(),
        "a minimal HPA must be written: {status} {body}"
    );

    let (code, body) = api
        .send(
            "PUT",
            &format!("{HPAS}/hpa-ok/status"),
            Some("application/json"),
            Some(&json!({
                "apiVersion": "autoscaling/v2",
                "kind": "HorizontalPodAutoscaler",
                "metadata": { "name": "hpa-ok" },
                "status": {},
            })),
        )
        .await;
    assert!(
        code.is_success(),
        "an empty status must be written: {code} {body}"
    );
}

/// The VerticalPodAutoscaler types are read only by
/// `controller-manager/src/controllers/vpa.rs`, via
/// `list::<VerticalPodAutoscaler>`. Before this slice a stored object missing
/// any one field failed to decode and the controller skipped the whole item,
/// so the contract to pin is the decode itself.
#[test]
fn a_vertical_pod_autoscaler_decodes_from_a_minimal_object() {
    use rusternetes_common::resources::autoscaling::VerticalPodAutoscaler;

    let vpa: VerticalPodAutoscaler = serde_json::from_value(json!({
        "apiVersion": "autoscaling.k8s.io/v1",
        "kind": "VerticalPodAutoscaler",
        "metadata": { "name": "vpa-1", "namespace": "default" },
        "spec": {},
    }))
    .expect("a VPA with an empty spec must decode");
    assert_eq!(vpa.metadata.name, "vpa-1");

    let with_recs: VerticalPodAutoscaler = serde_json::from_value(json!({
        "apiVersion": "autoscaling.k8s.io/v1",
        "kind": "VerticalPodAutoscaler",
        "metadata": { "name": "vpa-2", "namespace": "default" },
        "spec": { "recommenders": [{}] },
        "status": { "recommendation": { "containerRecommendations": [{}] } },
    }))
    .expect("a VPA whose nested objects omit every field must decode");
    assert_eq!(with_recs.metadata.name, "vpa-2");
}
