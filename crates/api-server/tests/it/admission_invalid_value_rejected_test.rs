//! A mutating admission plugin that writes an INVALID value must be rejected
//! with 422 and nothing stored (#2188, from #1965).
//!
//! Upstream guarantees this by ordering: the handler runs mutating admission
//! first (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go`
//! :183-209, `update.go` likewise), and only then `Store.Create`/`Update`,
//! whose `rest.BeforeCreate`/`BeforeUpdate`
//! (`registry/rest/create.go`, `update.go`) run `PrepareForCreate` +
//! `Validate` on the mutated object, with validating admission as the
//! callback after that. Here: `endpoints/handlers/create.rs` and
//! `registry/generic/store.rs` (`before_create`, then validating admission,
//! then storage).
//!
//! The mutator is a warp mock webhook (the pattern of
//! `conformance_apimachinery_admission_webhooks.rs`) whose JSON patch adds a
//! ConfigMap `data` key that fails ConfigMap validation ("a valid config key
//! must consist of alphanumeric characters, '-', '_' or '.'").

use axum::http::StatusCode;
use rusternetes_common::{
    admission::{AdmissionReview, AdmissionReviewResponse, PatchOp, PatchOperation},
    resources::{
        MutatingWebhook, MutatingWebhookConfiguration, OperationType, Rule, RuleWithOperations,
        SideEffectClass, WebhookClientConfig,
    },
    types::ObjectMeta,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use warp::Filter;

/// Mutating mock: adds `data["bad key"]`, an invalid ConfigMap key.
async fn start_invalid_key_mutator() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(|r: AdmissionReview| {
            let uid = r.request.map(|q| q.uid).unwrap_or_else(|| "u".into());
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: "/data/bad key".to_string(),
                value: Some(json!("v")),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(AdmissionReviewResponse {
                    uid,
                    allowed: true,
                    status: None,
                    patch: Some(b64),
                    patch_type: Some("JSONPatch".to_string()),
                    audit_annotations: None,
                    warnings: None,
                }),
            })
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

async fn register_mutator(mem: &MemoryStorage, url: String, op: OperationType) {
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("invalid-key-mutator"),
        webhooks: Some(vec![MutatingWebhook {
            name: "invalid-key.k8s.io".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![op],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["configmaps".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            match_conditions: None,
            reinvocation_policy: None,
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "invalid-key-mutator"),
        &cfg,
    )
    .await
    .unwrap();
}

fn assert_invalid(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["reason"], "Invalid", "{body}");
    assert!(
        body.to_string().contains("a valid config key must consist"),
        "422 must carry the validation error for the admission-written key: {body}"
    );
}

#[tokio::test]
async fn create_rejects_value_written_by_mutating_admission() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    let (url, _s) = start_invalid_key_mutator().await;
    register_mutator(&mem, url, OperationType::Create).await;

    let cm = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "mutated-invalid"},
        "data": {"ok": "1"}
    });
    let (status, body) = api.post("/api/v1/namespaces/default/configmaps", &cm).await;
    assert_invalid(status, &body);

    // Rejected, not stored.
    assert!(
        mem.get::<Value>(&build_key("configmaps", Some("default"), "mutated-invalid"))
            .await
            .is_err(),
        "an object invalidated by admission must not be persisted"
    );
}

#[tokio::test]
async fn update_rejects_value_written_by_mutating_admission() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();

    // Created before the mutator is registered, so it is valid.
    let cm = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "to-update"},
        "data": {"ok": "1"}
    });
    let (status, body) = api.post("/api/v1/namespaces/default/configmaps", &cm).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (url, _s) = start_invalid_key_mutator().await;
    register_mutator(&mem, url, OperationType::Update).await;

    let mut updated = body.clone();
    updated["data"] = json!({"ok": "2"});
    let (status, body) = api
        .put("/api/v1/namespaces/default/configmaps/to-update", &updated)
        .await;
    assert_invalid(status, &body);

    // The stored object is untouched.
    let stored: Value = mem
        .get(&build_key("configmaps", Some("default"), "to-update"))
        .await
        .unwrap();
    assert_eq!(stored["data"], json!({"ok": "1"}), "{stored}");
}
