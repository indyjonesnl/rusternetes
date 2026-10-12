//! #3260: a custom resource is defaulted and pruned as it is decoded, so a
//! mutating admission webhook is sent the coerced object.
//!
//! Upstream: `DecoderToVersion` builds `schemaCoercingDecoder` and
//! `unstructuredDefaulter` into the request codec
//! (customresource_handler.go:1189-1200); `schemaCoercingDecoder.Decode`
//! applies `unstructuredSchemaCoercer.apply` to the decoded object
//! (:1321-1346), which prunes (:1432-1441) before admission runs.

use axum::http::StatusCode;
use rusternetes_common::{
    admission::{AdmissionReview, AdmissionReviewResponse},
    resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType, Rule,
        RuleWithOperations, SideEffectClass, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::Filter;

const GROUP: &str = "stable.example.com";

async fn start_webhook(seen: Arc<Mutex<Vec<Value>>>) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let req = r.request.expect("request");
            seen.lock()
                .unwrap()
                .push(serde_json::to_value(&req).unwrap());
            warp::reply::json(&AdmissionReview {
                api_version: "admission.k8s.io/v1".into(),
                kind: "AdmissionReview".into(),
                request: None,
                response: Some(AdmissionReviewResponse::allow(req.uid)),
            })
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{addr}"), tx)
}

#[tokio::test]
async fn a_mutating_webhook_is_sent_the_pruned_and_defaulted_object() {
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
                            "properties": { "spec": {
                                "type": "object",
                                "properties": {
                                    "size": { "type": "integer" },
                                    "color": { "type": "string", "default": "red" }
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

    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _stop) = start_webhook(seen.clone()).await;
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "MutatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-widgets"),
        webhooks: Some(vec![MutatingWebhook {
            name: "mutate.widgets.io".into(),
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
        }]),
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
                "spec": { "size": 1, "unknown": "dropped" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "webhook calls: {seen:?}");
    let object = &seen[0]["object"];
    assert!(
        object["spec"].get("unknown").is_none(),
        "the webhook must not see a field the schema prunes: {object}"
    );
    assert_eq!(
        object["spec"]["color"], "red",
        "the webhook must see the schema default: {object}"
    );
}
