//! #3305: a custom resource a mutating webhook patched is pruned when the
//! admitted object is converted back, not inside the webhook dispatcher.
//!
//! Upstream: the dispatcher only defaults the patched object
//! (webhook/mutating/dispatcher.go:390); the prune is the coercer of
//! `schemaCoercingConverter` (customresource_handler.go:1348-1384) run by
//! `GetObjectConvertor().Convert` (dispatcher.go:236). Mirrors the e2e spec
//! "should mutate custom resource with pruning"
//! (test/e2e/apimachinery/webhook.go:331, testMutatingCustomResourceWebhook
//! with prune=true): two chained webhooks, the second's key is undeclared.

use axum::http::StatusCode;
use base64::Engine;
use rusternetes_common::{
    admission::{AdmissionReview, AdmissionReviewResponse, PatchOp, PatchOperation},
    resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType, Rule,
        RuleWithOperations, SideEffectClass, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use rusternetes_test_support::mock_http::*;
use serde_json::json;
use tokio::sync::oneshot;

const GROUP: &str = "stable.example.com";

/// Adds `/data/<add_key>` once `/data/<require_key>` is present.
async fn start_stage(require_key: &str, add_key: &str) -> (String, oneshot::Sender<()>) {
    let (require_key, add_key) = (require_key.to_string(), add_key.to_string());
    let (tx, rx) = oneshot::channel();
    let route = mock_post(move |r: AdmissionReview| {
        let req = r.request.expect("request");
        let present = req
            .object
            .as_ref()
            .and_then(|o| o.pointer(&format!("/data/{require_key}")))
            .is_some();
        let mut resp = AdmissionReviewResponse::allow(req.uid);
        if present {
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: format!("/data/{add_key}"),
                value: Some(json!("yes")),
                from: None,
            }];
            resp.patch = Some(
                base64::engine::general_purpose::STANDARD
                    .encode(serde_json::to_vec(&patch).unwrap()),
            );
            resp.patch_type = Some("JSONPatch".into());
        }
        reply_json(&AdmissionReview {
            api_version: "admission.k8s.io/v1".into(),
            kind: "AdmissionReview".into(),
            request: None,
            response: Some(resp),
        })
    });
    let addr = serve_graceful(route, async {
        rx.await.ok();
    });
    (format!("http://{addr}"), tx)
}

fn webhook(name: &str, url: String) -> MutatingWebhook {
    MutatingWebhook {
        name: name.into(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![RuleWithOperations {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec![GROUP.into()],
                api_versions: vec!["v1".into()],
                resources: vec!["widgets".into()],
                scope: None,
            },
        }],
        failure_policy: Some(FailurePolicy::Fail),
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".into()],
        match_conditions: None,
        reinvocation_policy: None,
    }
}

#[tokio::test]
async fn a_key_a_webhook_adds_outside_the_schema_is_pruned_before_storage() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": { "name": format!("widgets.{GROUP}") },
                "spec": {
                    "group": GROUP, "scope": "Namespaced",
                    "names": { "plural": "widgets", "kind": "Widget", "listKind": "WidgetList" },
                    "versions": [{
                        "name": "v1", "served": true, "storage": true,
                        "schema": { "openAPIV3Schema": {
                            "type": "object",
                            "properties": { "data": {
                                "type": "object",
                                "properties": {
                                    "mutation-start": { "type": "string" },
                                    "mutation-stage-1": { "type": "string" }
                                }
                            } }
                        } }
                    }]
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("widgets.{GROUP}")).await;

    let (url1, _s1) = start_stage("mutation-start", "mutation-stage-1").await;
    let (url2, _s2) = start_stage("mutation-stage-1", "mutation-stage-2").await;
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "MutatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-widgets"),
        webhooks: Some(vec![
            webhook("stage-1.widgets.io", url1),
            webhook("stage-2.widgets.io", url2),
        ]),
    };
    api.storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "mutate-widgets"),
            &cfg,
        )
        .await
        .unwrap();

    let (status, out) = api
        .post(
            &format!("/apis/{GROUP}/v1/namespaces/default/widgets"),
            &json!({
                "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
                "metadata": { "name": "w1" },
                "data": { "mutation-start": "yes" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    // Stage 2 ran (it needs stage 1's key) but its output is undeclared, so
    // exactly the two declared keys remain (webhook.go:2216-2223).
    assert_eq!(
        out["data"],
        json!({ "mutation-start": "yes", "mutation-stage-1": "yes" }),
        "{out}"
    );
}
