//! A workload decodes when the body omits its pod template — and answers 422.
//!
//! Go has no required JSON fields: a Deployment whose `spec.template.spec` is
//! `{}` decodes to a zero `PodSpec` and reaches validation, which answers
//! `spec.template.spec.containers: Required value` from
//! `ValidatePodTemplateSpec`
//! (`pkg/apis/core/validation/validation.go:7066-7073`). Every workload spec
//! validator calls it — `ValidateStatefulSetSpec`
//! (`pkg/apis/apps/validation/validation.go:214`), `ValidateDaemonSetSpec`
//! (`:454`), `ValidatePodTemplateSpecForReplicaSet` (`:656`),
//! `ValidateJobSpec` (`pkg/apis/batch/validation/validation.go:276`) — so the
//! answer is the same shape for all of them.
//!
//! Rusternetes answered serde's 400 BadRequest instead: `missing field
//! `containers``, with no `reason`, no `details.causes`, no field path. Fixing
//! that meant two changes in one commit, because they are only correct
//! together — the workload validators had to start calling
//! `validate_pod_template_spec` (they never did; the standalone `PodTemplate`
//! handler was its only caller), and only then could the template fields gain
//! `#[serde(default)]`. Defaulting alone would have turned the 400 into a
//! silent accept (#1939).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// A 400 means the body never reached a validator; the field path and the
/// `Required value` cause are what a client acts on.
fn assert_422_naming(status: axum::http::StatusCode, body: &Value, field: &str, whose: &str) {
    assert_ne!(
        status.as_u16(),
        400,
        "{whose} was rejected by the decoder, before any validation: {body}"
    );
    assert_eq!(
        status.as_u16(),
        422,
        "{whose} must be Invalid, not {status}: {body}"
    );
    let text = body.to_string();
    assert!(text.contains(field), "{whose} must name `{field}`: {body}");
}

#[tokio::test]
async fn a_deployment_with_an_empty_template_spec_is_a_422_naming_containers() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/deployments",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": { "name": "empty-template", "namespace": "default" },
                "spec": {
                    "replicas": 1,
                    "selector": { "matchLabels": { "app": "c" } },
                    "template": { "metadata": { "labels": { "app": "c" } }, "spec": {} },
                },
            })),
        )
        .await;
    assert_422_naming(
        status,
        &answer,
        "spec.template.spec.containers",
        "a Deployment with an empty template spec",
    );
}

/// The template itself absent, not just its `spec`. Upstream decodes it to a
/// zero `PodTemplateSpec` and `ValidateDeploymentSpec` still reaches
/// `ValidatePodTemplateSpec` on it, so the answer stays a 422.
#[tokio::test]
async fn a_deployment_with_no_template_at_all_is_a_422_not_a_400() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/deployments",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": { "name": "no-template", "namespace": "default" },
                "spec": {
                    "replicas": 1,
                    "selector": { "matchLabels": { "app": "c" } },
                },
            })),
        )
        .await;
    assert_422_naming(
        status,
        &answer,
        "spec.template",
        "a Deployment with no template",
    );
}

#[tokio::test]
async fn a_statefulset_with_an_empty_template_spec_is_a_422_naming_containers() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/statefulsets",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": { "name": "empty-template", "namespace": "default" },
                "spec": {
                    "replicas": 1,
                    "serviceName": "svc",
                    "selector": { "matchLabels": { "app": "c" } },
                    "template": { "metadata": { "labels": { "app": "c" } }, "spec": {} },
                },
            })),
        )
        .await;
    assert_422_naming(
        status,
        &answer,
        "spec.template.spec.containers",
        "a StatefulSet with an empty template spec",
    );
}

#[tokio::test]
async fn a_daemonset_with_an_empty_template_spec_is_a_422_naming_containers() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/daemonsets",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": { "name": "empty-template", "namespace": "default" },
                "spec": {
                    "selector": { "matchLabels": { "app": "c" } },
                    "template": { "metadata": { "labels": { "app": "c" } }, "spec": {} },
                },
            })),
        )
        .await;
    assert_422_naming(
        status,
        &answer,
        "spec.template.spec.containers",
        "a DaemonSet with an empty template spec",
    );
}

#[tokio::test]
async fn a_job_with_an_empty_template_spec_is_a_422_naming_containers() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/batch/v1/namespaces/default/jobs",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": { "name": "empty-template", "namespace": "default" },
                "spec": {
                    "template": { "spec": { "restartPolicy": "Never" } },
                },
            })),
        )
        .await;
    assert_422_naming(
        status,
        &answer,
        "spec.template.spec.containers",
        "a Job with an empty template spec",
    );
}

/// `CronJobSpec.jobTemplate` and `.schedule` are both required upstream by
/// validation only (`validateCronJobSpec`,
/// `pkg/apis/batch/validation/validation.go`), so a body with neither answers
/// one 422 naming both — never the decoder's 400 for whichever serde hit first.
#[tokio::test]
async fn a_cronjob_with_no_schedule_or_job_template_is_a_422_naming_both() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/batch/v1/namespaces/default/cronjobs",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": { "name": "bare", "namespace": "default" },
                "spec": {},
            })),
        )
        .await;
    assert_422_naming(status, &answer, "spec.schedule", "a bare CronJob");
    assert_422_naming(
        status,
        &answer,
        "spec.jobTemplate.spec.template.spec.containers",
        "a bare CronJob",
    );
}
