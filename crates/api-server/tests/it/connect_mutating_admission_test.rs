//! #2892: CONNECT on exec/attach/portforward runs MUTATING admission as well
//! as validating admission, and rejects dry-run.
//!
//! Upstream: `ConnectResource`
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:177-216`):
//! - `:179-182` `if isDryRun(req.URL)` -> `NewBadRequest("dryRun is not
//!   supported")` before anything else (`isDryRun`, `:399-401`, is true when
//!   the `dryRun` query key is present at all);
//! - `:199-216` the mutating `Admit` runs on the decoded options object, then
//!   the validating `Validate` runs on the SAME (mutated) object, both with
//!   `admission.Connect`.

use axum::http::StatusCode;
use rusternetes_common::{
    admission::{AdmissionReview, AdmissionReviewResponse, PatchOp, PatchOperation},
    resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType, Rule,
        RuleWithOperations, SideEffectClass, ValidatingWebhook, ValidatingWebhookConfiguration,
        WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use warp::Filter;

async fn start_webhook(
    seen: Arc<Mutex<Vec<Value>>>,
    respond: impl Fn(String) -> AdmissionReviewResponse + Send + Sync + 'static,
) -> (String, oneshot::Sender<()>) {
    let respond = Arc::new(respond);
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
                response: Some(respond(req.uid)),
            })
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{addr}"), tx)
}

fn connect_rules(resource: &str) -> Vec<RuleWithOperations> {
    vec![RuleWithOperations {
        operations: vec![OperationType::Connect],
        rule: Rule {
            api_groups: vec!["".into()],
            api_versions: vec!["v1".into()],
            resources: vec![resource.into()],
            scope: None,
        },
    }]
}

async fn install_mutating(api: &TestApiServer, url: String, resource: &str) {
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "MutatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-connect"),
        webhooks: Some(vec![MutatingWebhook {
            name: "mutate.connect.io".into(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: connect_rules(resource),
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
            &build_key("mutatingwebhookconfigurations", None, "mutate-connect"),
            &cfg,
        )
        .await
        .unwrap();
}

async fn install_validating(api: &TestApiServer, url: String, resource: &str) {
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "ValidatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("validate-connect"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "validate.connect.io".into(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: connect_rules(resource),
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
            &build_key("validatingwebhookconfigurations", None, "validate-connect"),
            &cfg,
        )
        .await
        .unwrap();
}

/// rest.go:179-182: `dryRun` present at all -> 400 "dryRun is not supported",
/// before admission runs, for every CONNECT subresource.
#[tokio::test]
async fn connect_rejects_dry_run_before_admission() {
    for (sub, resource, q) in [
        ("exec", "pods/exec", "command=ls&dryRun=All"),
        ("attach", "pods/attach", "stdout=true&dryRun=All"),
        ("portforward", "pods/portforward", "ports=80&dryRun=All"),
        // isDryRun is `len(Query()["dryRun"]) != 0`: an empty value counts.
        ("exec", "pods/exec", "command=ls&dryRun="),
    ] {
        let api = TestApiServer::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (url, _s) = start_webhook(seen.clone(), AdmissionReviewResponse::allow).await;
        install_validating(&api, url, resource).await;
        let (status, body) = api
            .get(&format!("/api/v1/namespaces/default/pods/target/{sub}?{q}"))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sub} {q}: {body}");
        assert!(
            body.to_string().contains("dryRun is not supported"),
            "{sub}: {body}"
        );
        assert!(seen.lock().unwrap().is_empty(), "{sub}: no webhook call");
    }
}

/// rest.go:204-210: a mutating webhook is consulted for CONNECT; a denial is
/// surfaced and wins over the pod lookup.
#[tokio::test]
async fn exec_is_denied_by_mutating_connect_webhook() {
    let api = TestApiServer::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (url, _s) = start_webhook(seen.clone(), |uid| {
        AdmissionReviewResponse::deny(uid, "mutator says no".into())
    })
    .await;
    install_mutating(&api, url, "pods/exec").await;
    let (status, body) = api
        .get("/api/v1/namespaces/default/pods/target/exec?command=ls&stdout=true")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.to_string().contains("mutator says no"), "{body}");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0]["operation"], "CONNECT");
    assert_eq!(seen[0]["object"]["kind"], "PodExecOptions");
    assert_eq!(seen[0]["object"]["command"], serde_json::json!(["ls"]));
}

/// rest.go:204-216: mutating Admit runs first and the validating Validate
/// sees the SAME, mutated options object.
#[tokio::test]
async fn validating_connect_webhook_sees_mutated_options() {
    let api = TestApiServer::new();
    let mseen = Arc::new(Mutex::new(Vec::new()));
    let (murl, _m) = start_webhook(mseen.clone(), |uid| {
        AdmissionReviewResponse::allow_with_patch(
            uid,
            vec![PatchOperation {
                op: PatchOp::Replace,
                path: "/container".into(),
                value: Some(serde_json::json!("sidecar")),
                from: None,
            }],
        )
    })
    .await;
    install_mutating(&api, murl, "pods/exec").await;
    let vseen = Arc::new(Mutex::new(Vec::new()));
    let (vurl, _v) = start_webhook(vseen.clone(), |uid| {
        AdmissionReviewResponse::deny(uid, "stop here".into())
    })
    .await;
    install_validating(&api, vurl, "pods/exec").await;

    let (status, _) = api
        .get("/api/v1/namespaces/default/pods/target/exec?command=ls&stdout=true")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(mseen.lock().unwrap().len(), 1);
    let v = vseen.lock().unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0]["object"]["container"], "sidecar");
}
