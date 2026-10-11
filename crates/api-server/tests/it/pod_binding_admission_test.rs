//! #2146 item 2: admission webhooks matching `pods/binding` see the `Binding`.
//!
//! Upstream: `BindingREST.Create` (`pkg/registry/core/pod/storage/storage.go:
//! 177-201`) calls `createValidation(ctx, binding.DeepCopyObject())` after
//! `ValidatePodBinding`, and the handler (`CreateNamedResource`,
//! `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go`) runs the
//! mutating chain first with `admission.Create` on the `Binding`.

use axum::http::StatusCode;
use rusternetes_common::{
    admission::{AdmissionReview, AdmissionReviewResponse},
    resources::{
        FailurePolicy, OperationType, Rule, RuleWithOperations, SideEffectClass, ValidatingWebhook,
        ValidatingWebhookConfiguration, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::Filter;

async fn start_recording_denier(seen: Arc<Mutex<Vec<Value>>>) -> (String, oneshot::Sender<()>) {
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
                response: Some(AdmissionReviewResponse::deny(
                    req.uid,
                    "binding denied".into(),
                )),
            })
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{addr}"), tx)
}

async fn install_webhook(api: &TestApiServer, url: String) {
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "ValidatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-binding"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "deny.binding.io".into(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec!["pods/binding".into()],
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
        }]),
    };
    api.storage
        .create(
            &build_key("validatingwebhookconfigurations", None, "deny-binding"),
            &cfg,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn validating_webhook_on_pods_binding_sees_the_binding_and_can_deny() {
    let api = TestApiServer::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _shutdown) = start_recording_denier(seen.clone()).await;
    install_webhook(&api, url).await;

    let (s, out) = api
        .post(
            "/api/v1/namespaces/default/pods",
            &json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": "p1"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    seen.lock().unwrap().clear();

    let (s, out) = api
        .post(
            "/api/v1/namespaces/default/pods/p1/binding",
            &json!({
                "apiVersion": "v1", "kind": "Binding",
                "metadata": {"name": "p1", "namespace": "default"},
                "target": {"kind": "Node", "name": "node-1"}
            }),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{out}");
    assert!(out.to_string().contains("binding denied"), "{out}");

    let seen = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(seen.len(), 1, "webhook must be called exactly once");
    let req = &seen[0];
    assert_eq!(req["operation"], "CREATE");
    assert_eq!(req["kind"]["kind"], "Binding");
    assert_eq!(req["resource"]["resource"], "pods");
    assert_eq!(req["subResource"], "binding");
    assert_eq!(req["object"]["kind"], "Binding");
    assert_eq!(req["object"]["target"]["name"], "node-1");

    let pod: Value = api
        .storage
        .get(&build_key("pods", Some("default"), "p1"))
        .await
        .unwrap();
    assert!(pod["spec"].get("nodeName").is_none(), "{pod}");
}
