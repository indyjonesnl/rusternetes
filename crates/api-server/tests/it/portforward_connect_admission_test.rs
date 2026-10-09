//! #2698: `pods/portforward` must run CONNECT admission webhooks, like exec
//! and attach already do.
//!
//! Upstream: `ConnectResource`
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:177-216`)
//! decodes the request options (`getRequestOptions`, a 400 on a bad query) and
//! then, BEFORE `connecter.Connect`, runs mutating + validating admission with
//! `admission.Connect` on the decoded options object. For port-forward the
//! connecter is `PortForwardREST` (`pkg/registry/core/pod/rest/subresources.go:
//! 256-279`), `NewConnectOptions` returns `&api.PodPortForwardOptions{}`
//! (`:268`), whose v1 form is `{kind: PodPortForwardOptions, ports: [int32]}`
//! (`staging/src/k8s.io/api/core/v1/types.go:7314-7322`).

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
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::Filter;

/// Deny-everything webhook that records the `request` it was sent.
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
                    "port-forward denied".into(),
                )),
            })
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{addr}"), tx)
}

async fn install_connect_webhook(api: &TestApiServer, url: String, resource: &str) {
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "ValidatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-connect"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "deny.connect.io".into(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Connect],
                rule: Rule {
                    api_groups: vec!["".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec![resource.into()],
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
            &build_key("validatingwebhookconfigurations", None, "deny-connect"),
            &cfg,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn portforward_is_denied_by_connect_webhook_with_options_object() {
    let api = TestApiServer::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _shutdown) = start_recording_denier(seen.clone()).await;
    install_connect_webhook(&api, url, "pods/portforward").await;

    // The pod does not exist: admission runs before the pod is fetched, so a
    // denying webhook must win (403), not the 404 from the pod lookup.
    let (status, body) = api
        .get("/api/v1/namespaces/default/pods/target/portforward?ports=80,8080")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert!(
        body.to_string().contains("port-forward denied"),
        "denial reason must reach the client: {body}"
    );

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "webhook must be called exactly once");
    let req = &seen[0];
    assert_eq!(req["operation"], "CONNECT");
    assert_eq!(req["kind"]["kind"], "PodPortForwardOptions");
    assert_eq!(req["resource"]["resource"], "pods");
    assert_eq!(req["subResource"], "portforward");
    assert_eq!(req["name"], "target");
    assert_eq!(req["namespace"], "default");
    assert_eq!(req["object"]["kind"], "PodPortForwardOptions");
    assert_eq!(req["object"]["apiVersion"], "v1");
    assert_eq!(req["object"]["ports"], serde_json::json!([80, 8080]));
}

/// `getRequestOptions` fails before admission (rest.go:194-198): a malformed
/// `ports` query is a 400 and no webhook is consulted.
#[tokio::test]
async fn portforward_bad_ports_is_400_before_admission() {
    let api = TestApiServer::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _shutdown) = start_recording_denier(seen.clone()).await;
    install_connect_webhook(&api, url, "pods/portforward").await;

    let (status, _) = api
        .get("/api/v1/namespaces/default/pods/target/portforward?ports=abc")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(seen.lock().unwrap().is_empty());
}
