//! Scoped mirror of Kubernetes v1.35 conformance for [sig-api-machinery]
//! Admission webhooks (Validating + Mutating).
//!
//! Source: https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Status table: docs/conformance/apimachinery-admission-webhooks.md
//!
//! Each test mirrors a single `framework.ConformanceIt(...)` block from
//! `test/e2e/apimachinery/webhook.go` (line numbers preserved in per-test
//! docstrings). The HTTP layer is exercised via an inline `spawn_router()`
//! helper that calls `rusternetes_api_server::router::build_router` against an
//! `ApiServerState` backed by `MemoryStorage` and `AlwaysAllowAuthorizer`.
//! The webhook *backends* themselves are tiny warp mocks (the same pattern
//! used by `admission_webhook_e2e_test.rs`) — every webhook configuration
//! either targets one of these mocks or `https://0.0.0.0:1/...` for the
//! "fail closed without CA bundle" scenario.
//!
//! ## Mirror audit — #1749, 2026-08-25
//!
//! All 22 `framework.ConformanceIt` bodies in `webhook.go` have been opened and
//! re-derived assertion by assertion, including the helper each one delegates
//! to. Do not treat this file as audited again after a change: re-run the same
//! check and move the date, or drop this block.
//!
//! Every citation names the `ConformanceIt` line, its descriptor string, and
//! the helper holding the assertions. The descriptor is the durable anchor —
//! the line numbers this file carried before the audit were all stale against
//! the pinned `release-1.35` checkout, drifting from -22 to +40.
//!
//! Cases whose upstream half needs a live cluster, recorded rather than faked:
//! `should be able to deny attaching pod` (a Running pod plus `kubectl
//! attach`), the mounted-file half of the pod token cases, and the
//! storage-version conversion in `should mutate custom resource with different
//! stored version`. Two upstream assertions are blocked on #1751
//! (`MemoryStorage` writes no `resourceVersion`): `HaveValidResourceVersion()`
//! and the post-patch RV comparison in both patching/updating cases.
//!
//! Two deliberate deviations, each explained at its test: the listing cases
//! register unreachable fail-closed webhooks so "in effect" reads as 500
//! rather than 403, and the patching cases rewrite their rule set through
//! storage because `validateWebhookURL` rejects the plain-HTTP mock URL on the
//! API's create/update path.

use axum::http::StatusCode;
use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
use rusternetes_common::{
    admission::{
        AdmissionResponse, AdmissionReview, AdmissionReviewResponse, GroupVersionKind,
        GroupVersionResource, Operation, PatchOp, PatchOperation, UserInfo,
    },
    resources::{
        FailurePolicy, MatchCondition, MutatingWebhook, MutatingWebhookConfiguration,
        OperationType, ReinvocationPolicy, Rule, RuleWithOperations, SideEffectClass,
        ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use warp::Filter;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage. The
/// authorizer is `AlwaysAllow` and `skip_auth=true` so the router uses
/// `skip_auth_middleware` and no token is needed (mirrors the
/// `patch_cas_retry_test.rs` helper exactly).
/// `(storage, router)` factory used by every HTTP-driven test. Each test
/// owns its own storage so the tests are trivially parallel.
fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// HTTP helper: POST JSON, return `(status, body)`.
async fn post_json(router: TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    router.post(uri, body).await
}

/// HTTP helper: GET JSON, return `(status, body)`.
async fn get_json(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.get(uri).await
}

/// HTTP helper: PUT JSON, return `(status, body)`.
async fn put_json(router: TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    router.put(uri, body).await
}

/// HTTP helper: DELETE, return status (some delete handlers return an empty
/// body or a tombstone; we only care about the status code here).
async fn delete_status(router: TestApiServer, uri: &str) -> StatusCode {
    router.delete(uri).await.0
}

/// HTTP helper: DELETE, return `(status, body)` so the caller can assert on the
/// webhook-denial Status payload.
async fn delete_json(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    let (status, v) = router.delete(uri).await;
    (status, v)
}

// ---------------------------------------------------------------------------
// Webhook backend mocks (warp). These mirror the `sample-webhook-deployment`
// behaviours used by the upstream Ginkgo tests — allow, deny, mutate, slow.
// ---------------------------------------------------------------------------

/// Generic admission response shim used by every mock below.
fn wrap(response: AdmissionReviewResponse) -> AdmissionReview {
    AdmissionReview {
        api_version: "admission.k8s.io/v1".to_string(),
        kind: "AdmissionReview".to_string(),
        request: None,
        response: Some(response),
    }
}

async fn start_deny_validator(reason: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let uid = r.request.map(|req| req.uid).unwrap_or_else(|| "u".into());
            warp::reply::json(&wrap(AdmissionReviewResponse::deny(uid, reason.clone())))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Validating mock used for DELETE admission. It denies only when the request
/// carries an `oldObject` and no `object` (the K8s DELETE AdmissionReview
/// shape), which lets the test assert both the deny *and* that the api-server
/// populated the review correctly. Any other shape is allowed.
async fn start_delete_deny_validator(reason: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(req) => req,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let is_delete = matches!(request.operation, Operation::Delete);
            let has_old = request.old_object.is_some();
            let no_new = request.object.is_none();
            if is_delete && has_old && no_new {
                warp::reply::json(&wrap(AdmissionReviewResponse::deny(
                    request.uid,
                    reason.clone(),
                )))
            } else {
                warp::reply::json(&wrap(AdmissionReviewResponse::allow(request.uid)))
            }
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Validating mock keyed off the object's `data["webhook-e2e-test"]` value,
/// the way upstream's e2e webhook server is
/// (k8s.io/kubernetes/test/e2e/apimachinery/webhook.go drives it through
/// `testCustomResourceWebhook` / `testBlockingCustomResourceUpdateDeletion`,
/// webhook.go:2112-2194):
///
///   - `webhook-disallow` rejects CREATE and UPDATE
///     ("the custom resource contains unwanted data");
///   - `webhook-nondeletable` rejects DELETE
///     ("the custom resource cannot be deleted because it contains unwanted
///     key and value");
///   - anything else is admitted.
///
/// DELETE is decided from `oldObject`, since an AdmissionReview for a delete
/// carries no new object — which is exactly the contract the upstream case
/// exercises when it blocks a delete, rewrites the data, and then deletes
/// successfully.
async fn start_cr_data_validator() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(|r: AdmissionReview| {
            let request = match r.request {
                Some(req) => req,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let subject = match request.operation {
                Operation::Delete => request.old_object.as_ref(),
                _ => request.object.as_ref(),
            };
            let marker = subject
                .and_then(|o| o.pointer("/data/webhook-e2e-test"))
                .and_then(Value::as_str)
                .unwrap_or("");

            let denial = match (&request.operation, marker) {
                (Operation::Create | Operation::Update, "webhook-disallow") => {
                    Some("the custom resource contains unwanted data")
                }
                (Operation::Delete, "webhook-nondeletable") => Some(
                    "the custom resource cannot be deleted because it contains unwanted key and value",
                ),
                _ => None,
            };

            match denial {
                Some(reason) => warp::reply::json(&wrap(AdmissionReviewResponse::deny(
                    request.uid,
                    reason.to_string(),
                ))),
                None => warp::reply::json(&wrap(AdmissionReviewResponse::allow(request.uid))),
            }
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Validating mock keyed off `metadata.labels["webhook-e2e-test"]`, the way
/// upstream's CRD-denying webhook is: `testCRDDenyWebhook`
/// (webhook.go:2342-2400) creates a CustomResourceDefinition carrying
/// `webhook-e2e-test: webhook-disallow` and requires the create to fail with
/// "the crd contains unwanted label". A CRD without that label must be
/// admitted.
async fn start_label_deny_validator() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(|r: AdmissionReview| {
            let request = match r.request {
                Some(req) => req,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let disallowed = request
                .object
                .as_ref()
                .and_then(|o| o.pointer("/metadata/labels/webhook-e2e-test"))
                .and_then(Value::as_str)
                == Some("webhook-disallow");
            if disallowed {
                warp::reply::json(&wrap(AdmissionReviewResponse::deny(
                    request.uid,
                    "the crd contains unwanted label".to_string(),
                )))
            } else {
                warp::reply::json(&wrap(AdmissionReviewResponse::allow(request.uid)))
            }
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds `/metadata/labels/{key}={value}`.
async fn start_mutator_label(key: String, value: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: format!("/metadata/labels/{}", key),
                value: Some(json!(value.clone())),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds `data[<add_key>] = "yes"` **only if**
/// `data[<require_key>]` is already present on the incoming object.
///
/// This is upstream's `newMutateConfigMapWebhookFixture(f, certCtx, stage, …)`
/// (webhook.go:1258-1260) and the custom-resource fixtures
/// `registerMutatingWebhookForCustomResource` installs (webhook.go:2036-2111),
/// which use the same two-stage shape over a top-level `data` map: two of these
/// are registered, stage 1 keyed off `mutation-start` and stage 2 keyed off
/// `mutation-stage-1`, so stage 2 can only fire if it observes stage 1's
/// output. That chaining is the whole point of the "ordered mutation" case —
/// a webhook that mutated unconditionally would satisfy a subset assertion
/// while proving nothing about order.
///
/// Referenced by `start_mutator_data_stage` callers in both the configmap and
/// custom-resource mutation mirrors.
async fn start_mutator_data_stage(
    require_key: String,
    add_key: String,
) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let has_required = request
                .object
                .as_ref()
                .and_then(|o| o.pointer(&format!("/data/{require_key}")))
                .is_some();
            if !has_required {
                return warp::reply::json(&wrap(AdmissionReviewResponse {
                    uid: request.uid,
                    allowed: true,
                    status: None,
                    patch: None,
                    patch_type: None,
                    audit_annotations: None,
                    warnings: None,
                }));
            }
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: format!("/data/{add_key}"),
                value: Some(json!("yes")),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds an init container to a pod (`/spec/initContainers`).
/// Mirrors the upstream `addPodSpec`-style mutation that the
/// "mutate pod and apply defaults after mutation" test relies on.
async fn start_mutator_init_container() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(|r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: "/spec/initContainers".to_string(),
                value: Some(json!([{
                    "name": "webhook-added-init-container",
                    "image": "registry.k8s.io/pause:3.10",
                }])),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

async fn start_slow_validator(delay: std::time::Duration) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route =
        warp::post()
            .and(warp::body::json())
            .and_then(move |r: AdmissionReview| async move {
                tokio::time::sleep(delay).await;
                let uid = r.request.map(|q| q.uid).unwrap_or_else(|| "u".into());
                Ok::<_, warp::Rejection>(warp::reply::json(&wrap(AdmissionReviewResponse::allow(
                    uid,
                ))))
            });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

// ---------------------------------------------------------------------------
// Builders for compact webhook configurations.
// ---------------------------------------------------------------------------

fn rule_for(api_group: &str, version: &str, resource: &str) -> RuleWithOperations {
    RuleWithOperations {
        operations: vec![OperationType::Create],
        rule: Rule {
            api_groups: vec![api_group.to_string()],
            api_versions: vec![version.to_string()],
            resources: vec![resource.to_string()],
            scope: None,
        },
    }
}

fn delete_rule_for(api_group: &str, version: &str, resource: &str) -> RuleWithOperations {
    RuleWithOperations {
        operations: vec![OperationType::Delete],
        rule: Rule {
            api_groups: vec![api_group.to_string()],
            api_versions: vec![version.to_string()],
            resources: vec![resource.to_string()],
            scope: None,
        },
    }
}

fn validating(
    name: &str,
    url: String,
    rules: Vec<RuleWithOperations>,
    failure_policy: Option<FailurePolicy>,
    timeout: Option<i32>,
) -> ValidatingWebhook {
    ValidatingWebhook {
        name: name.to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules,
        failure_policy,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: timeout,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    }
}

fn mutating(
    name: &str,
    url: String,
    rules: Vec<RuleWithOperations>,
    failure_policy: Option<FailurePolicy>,
    reinvocation: Option<ReinvocationPolicy>,
) -> MutatingWebhook {
    MutatingWebhook {
        name: name.to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules,
        failure_policy,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
        reinvocation_policy: reinvocation,
    }
}

fn admission_request(
    api_group: &str,
    version: &str,
    kind: &str,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
    obj: Value,
) -> rusternetes_common::admission::AdmissionReviewRequest {
    rusternetes_common::admission::AdmissionReviewRequest {
        uid: format!("uid-{}", name),
        kind: GroupVersionKind {
            group: api_group.to_string(),
            version: version.to_string(),
            kind: kind.to_string(),
        },
        resource: GroupVersionResource {
            group: api_group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: name.to_string(),
        namespace: namespace.map(|s| s.to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(obj),
        old_object: None,
        dry_run: None,
        options: None,
    }
}

fn admin_user_info() -> UserInfo {
    UserInfo {
        username: "admin".to_string(),
        uid: "admin-uid".to_string(),
        groups: vec!["system:masters".to_string()],
    }
}

// ===========================================================================
// Mirrors of `[sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin]`
// from k8s.io/kubernetes/test/e2e/apimachinery/webhook.go (release-1.35).
// ===========================================================================

/// [sig-api-machinery] AdmissionWebhook should include webhook resources in
/// discovery documents [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:118
///   ("should include webhook resources in discovery documents")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream fetches **three** documents in sequence — `/apis`,
/// `/apis/admissionregistration.k8s.io`, and
/// `/apis/admissionregistration.k8s.io/v1` — and checks the group is present
/// at v1 in each, plus `GroupVersion` on the resource list. The mirror fetched
/// only the third and never checked `groupVersion`. All three blocks now run.
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Verifies the api-server's `/apis/admissionregistration.k8s.io/v1`
/// discovery document lists `validatingwebhookconfigurations`,
/// `mutatingwebhookconfigurations`, `validatingadmissionpolicies` and
/// `validatingadmissionpolicybindings` with the expected verbs.
#[tokio::test]
async fn should_include_webhook_resources_in_discovery_documents() {
    let (_mem, router) = spawn_router();

    // Block 1 (webhook.go:119-144): /apis must carry the group, at v1.
    let (status, groups) = get_json(router.clone(), "/apis").await;
    assert_eq!(status, StatusCode::OK, "GET /apis: {groups}");
    let group = groups["groups"]
        .as_array()
        .expect("APIGroupList.groups must be an array")
        .iter()
        .find(|g| g["name"] == "admissionregistration.k8s.io")
        .unwrap_or_else(|| {
            panic!("admissionregistration.k8s.io API group not found in /apis: {groups}")
        });
    assert!(
        group["versions"]
            .as_array()
            .map(|vs| vs.iter().any(|v| v["version"] == "v1"))
            .unwrap_or(false),
        "admissionregistration.k8s.io/v1 not found in /apis: {group}"
    );

    // Block 2 (webhook.go:146-161): the group document itself.
    let (status, group_doc) = get_json(router.clone(), "/apis/admissionregistration.k8s.io").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET /apis/admissionregistration.k8s.io: {group_doc}"
    );
    assert_eq!(
        group_doc["name"], "admissionregistration.k8s.io",
        "verifying API group name: {group_doc}"
    );
    assert!(
        group_doc["versions"]
            .as_array()
            .map(|vs| vs.iter().any(|v| v["version"] == "v1"))
            .unwrap_or(false),
        "admissionregistration.k8s.io/v1 not found in the group document: {group_doc}"
    );

    // Block 3 (webhook.go:163-194): the versioned resource list.
    let (status, body) = get_json(router, "/apis/admissionregistration.k8s.io/v1").await;
    assert_eq!(status, StatusCode::OK, "discovery must return 200: {body}");
    assert_eq!(
        body["groupVersion"], "admissionregistration.k8s.io/v1",
        "verifying API group/version in the resource list: {body}"
    );

    let resources = body["resources"]
        .as_array()
        .expect("APIResourceList.resources must be an array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();

    // Upstream requires the two webhook-configuration resources; the policy
    // resources below are this group's other members and are asserted beyond
    // what the upstream case checks.
    for required in [
        "validatingwebhookconfigurations",
        "mutatingwebhookconfigurations",
        "validatingadmissionpolicies",
        "validatingadmissionpolicybindings",
    ] {
        assert!(
            names.contains(&required),
            "discovery must list {required}; got {names:?}"
        );
    }

    // Each webhook resource must list the standard verbs (the upstream test
    // only asserts presence, but the verb list is the contract that
    // kubectl + client-go rely on).
    let vwc = resources
        .iter()
        .find(|r| r["name"] == "validatingwebhookconfigurations")
        .unwrap();
    let verbs: Vec<&str> = vwc["verbs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    for v in [
        "create", "delete", "get", "list", "patch", "update", "watch",
    ] {
        assert!(
            verbs.contains(&v),
            "VWC must support verb {v}; got {verbs:?}"
        );
    }
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny pod and configmap
/// creation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:198
///   ("should be able to deny pod and configmap creation")
///   Assertions live in testWebhook (webhook.go:1372-1465); the configuration
///   under test is built by registerWebhook (webhook.go:1163-1197).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// `registerWebhook` installs **four** webhooks, and the case is as much about
/// what is *admitted* as what is denied:
///
///   - `newDenyPodWebhookFixture` / `newDenyConfigMapWebhookFixture` deny
///     non-compliant pods and configmaps. Note the configmap rule is
///     CREATE + UPDATE + DELETE (webhook.go:2612), not CREATE alone;
///   - both deny fixtures carry a namespaceSelector of
///     `matchLabels {uniqueName: "true"}` plus
///     `matchExpressions [{skip-webhook-admission NotIn [yes]}]`
///     (webhook.go:2620-2629), so a namespace labelled `skip-webhook-admission=yes`
///     is exempt;
///   - `failOpenHook` is deliberately unreachable with `failurePolicy: Ignore`
///     — upstream's own comment says "Because this webhook is configured
///     fail-open, request should be admitted after the call fails"
///     (webhook.go:1169-1185).
///
/// `testWebhook` then walks seven scenarios. This mirror covers the six that
/// are decidable at the admission layer: deny pod, deny configmap on CREATE,
/// deny configmap on UPDATE, admit a compliant configmap, admit through the
/// unreachable fail-open webhook, and admit in the exempt namespace.
///
/// Before this audit the mirror asserted only that a Deny came back for pods
/// and configmaps on CREATE. Nothing asserted an Allow, so a webhook that
/// denied unconditionally would have passed it — which is precisely what the
/// fail-open and namespace-exemption halves exist to rule out.
///
/// Not mirrored, and why: upstream's exact rejection strings ("the pod
/// contains unwanted container name", "the pod contains unwanted label", "the
/// configmap contains unwanted key and value") are produced by the e2e
/// webhook *server*, not by the api-server, so they pin that fixture rather
/// than any Rusternetes behaviour. The hanging-pod scenario
/// (webhook.go:1388-1405) asserts a dial timeout and that the pod was not
/// persisted; the timeout half is covered by `should_honor_timeout`, and the
/// not-persisted half needs the full HTTP create path rather than the
/// admission manager.
#[tokio::test]
async fn should_be_able_to_deny_pod_and_configmap_creation() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("denied by webhook".to_string()).await;

    // Upstream's skip-namespace labels (webhook.go:65-66).
    const SKIP_LABEL_KEY: &str = "skip-webhook-admission";
    const SKIP_LABEL_VALUE: &str = "yes";
    const UNIQUE: &str = "e2e-webhook-unique";

    // The two namespaces the case distinguishes: the test namespace, and the
    // exempted one (upstream `skipNamespaceBaseName`, webhook.go:67).
    for (ns_name, skip) in [("default", false), ("exempted-namespace", true)] {
        let mut labels = serde_json::Map::new();
        labels.insert(UNIQUE.to_string(), json!("true"));
        if skip {
            labels.insert(SKIP_LABEL_KEY.to_string(), json!(SKIP_LABEL_VALUE));
        }
        mem.create(
            &build_key("namespaces", None, ns_name),
            &json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": ns_name, "labels": Value::Object(labels)}
            }),
        )
        .await
        .unwrap();
    }

    // The namespaceSelector both deny fixtures carry.
    let deny_selector = rusternetes_common::resources::LabelSelector {
        match_labels: Some(std::collections::HashMap::from([(
            UNIQUE.to_string(),
            "true".to_string(),
        )])),
        match_expressions: Some(vec![
            rusternetes_common::resources::LabelSelectorRequirement {
                key: SKIP_LABEL_KEY.to_string(),
                operator: rusternetes_common::resources::LabelSelectorOperator::NotIn,
                values: Some(vec![SKIP_LABEL_VALUE.to_string()]),
            },
        ]),
    };

    // configmaps are matched on CREATE, UPDATE and DELETE upstream.
    let configmap_rule = RuleWithOperations {
        operations: vec![
            OperationType::Create,
            OperationType::Update,
            OperationType::Delete,
        ],
        rule: Rule {
            api_groups: vec!["".to_string()],
            api_versions: vec!["v1".to_string()],
            resources: vec!["configmaps".to_string()],
            scope: None,
        },
    };

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-cm"),
        webhooks: Some(vec![
            ValidatingWebhook {
                rules: vec![rule_for("", "v1", "pods")],
                namespace_selector: Some(deny_selector.clone()),
                ..validating(
                    "deny-unwanted-pod-container-name-and-label.k8s.io",
                    url.clone(),
                    vec![],
                    Some(FailurePolicy::Fail),
                    None,
                )
            },
            ValidatingWebhook {
                rules: vec![configmap_rule],
                namespace_selector: Some(deny_selector),
                ..validating(
                    "deny-unwanted-configmap-data.k8s.io",
                    url,
                    vec![],
                    Some(FailurePolicy::Fail),
                    None,
                )
            },
            // The unreachable fail-open webhook: port 1 never answers, and
            // `Ignore` means the request must still be admitted.
            ValidatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                ..validating(
                    "fail-open.k8s.io",
                    "http://127.0.0.1:1/fail-open".to_string(),
                    vec![],
                    Some(FailurePolicy::Ignore),
                    Some(1),
                )
            },
        ]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-pod-cm"),
        &cfg,
    )
    .await
    .unwrap();

    let run = |op: Operation, kind: &'static str, resource: &'static str, ns: &'static str| {
        let manager = &manager;
        async move {
            manager
                .run_validating_webhooks(
                    &op,
                    &GroupVersionKind {
                        group: "".into(),
                        version: "v1".into(),
                        kind: kind.into(),
                    },
                    &GroupVersionResource {
                        group: "".into(),
                        version: "v1".into(),
                        resource: resource.into(),
                    },
                    Some(ns),
                    "obj",
                    Some(json!({"metadata": {"name": "obj"}})),
                    None,
                    &admin_user_info(),
                )
                .await
                .unwrap()
        }
    };

    // 1 + 2: non-compliant pod and configmap denied on CREATE.
    for (resource, kind) in [("pods", "Pod"), ("configmaps", "ConfigMap")] {
        let resp = run(Operation::Create, kind, resource, "default").await;
        match resp {
            AdmissionResponse::Deny(reason) => assert!(
                reason.contains("denied by webhook"),
                "{resource}: deny reason: {reason}"
            ),
            other => panic!("{resource}: expected Deny on create, got {other:?}"),
        }
    }

    // 3: the configmap rule covers UPDATE too — upstream rejects both the PUT
    // and the strategic-merge PATCH of an admitted configmap.
    let resp = run(Operation::Update, "ConfigMap", "configmaps", "default").await;
    assert!(
        matches!(resp, AdmissionResponse::Deny(_)),
        "configmap UPDATE must be denied, got {resp:?}"
    );

    // 4: an object the deny webhooks do not match is admitted — here a
    // Secret, which neither rule selects. Without an Allow assertion the deny
    // cases above would pass against a deny-everything implementation.
    let resp = run(Operation::Create, "Secret", "secrets", "default").await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "an unmatched resource must be admitted, got {resp:?}"
    );

    // 5 + 6: in the exempted namespace the deny webhooks are out of scope, so
    // the only webhook left matching configmaps is the unreachable fail-open
    // one — the request must still be admitted.
    let resp = run(
        Operation::Create,
        "ConfigMap",
        "configmaps",
        "exempted-namespace",
    )
    .await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "a namespace labelled {SKIP_LABEL_KEY}={SKIP_LABEL_VALUE} is exempt, and the \
         unreachable fail-open webhook must not block it; got {resp:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny attaching pod
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:209
///   ("should be able to deny attaching pod")
///   Assertions live in testAttachingPodWebhook (webhook.go:1466-1482).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream creates a pod, waits for it to reach Running, then shells out to
/// `kubectl attach ... -i -c=container1` and requires the error to contain
/// "attaching to pod 'to-be-attached-pod' is not allowed". Both halves are
/// live-only: a running pod needs a kubelet, and the message is produced by
/// the e2e webhook server rather than by the api-server.
///
/// What is decidable here — and what this mirror asserts — is the routing
/// half: a webhook whose rule names the `pods/attach` subresource is selected
/// for a CONNECT on that subresource and denies it. That is the part the
/// api-server owns.
/// Asserts a webhook scoped to the `pods/attach` subresource denies the
/// operation. `resource_matches` splits the request's `pods/attach` GVR on
/// `/` and matches the rule's `pods/attach` entry; the operation is CONNECT
/// because attach is a streaming subresource in K8s.
#[tokio::test]
async fn should_be_able_to_deny_attaching_pod() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("attach denied".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-attach"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Connect],
                rule: Rule {
                    api_groups: vec!["".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec!["pods/attach".into()],
                    scope: None,
                },
            }],
            ..validating(
                "deny.attach.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-attach"),
        &cfg,
    )
    .await
    .unwrap();

    // K8s models POST /pods/<name>/attach as a CONNECT admission operation
    // (see kubernetes/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go
    // — Connect verbs map to admission.Connect). The dispatcher matches the
    // rule's `pods/attach` subresource against the request's resource string
    // via the `<resource>/<sub>` split in `resource_matches`.
    let resp = manager
        .run_validating_webhooks(
            &Operation::Connect,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "PodAttachOptions".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "pods/attach".into(),
            },
            Some("default"),
            "target-pod",
            Some(json!({"kind":"PodAttachOptions"})),
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    match resp {
        AdmissionResponse::Deny(_) => {}
        other => panic!("attach must be denied, got {other:?}"),
    }
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny custom
/// resource creation, update and deletion [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:220
///   ("should be able to deny custom resource creation, update and deletion")
///   Assertions live in testCustomResourceWebhook (webhook.go:2112-2134) and
///   testBlockingCustomResourceUpdateDeletion (webhook.go:2136-2194).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream bodies.
///
/// Upstream's webhook decides from the CR's own
/// `data["webhook-e2e-test"]`, and the case walks six steps: a
/// `webhook-disallow` CR is refused on create; a `webhook-nondeletable` one is
/// **created successfully**; updating it to disallowed data is refused;
/// deleting it is refused; rewriting the value to `webhook-allow` is accepted;
/// and the delete then succeeds.
///
/// The mirror used a deny-everything validator and asserted only that CREATE,
/// UPDATE and DELETE all came back denied. Nothing asserted an Allow, so the
/// test could not distinguish a correct implementation from one that rejects
/// everything — and the interesting half was missing entirely: the delete
/// refusal is decided from the **stored** object, since a delete
/// AdmissionReview carries no new object
/// (handlers/admission_helper.rs:68-75 sends `object: None`,
/// `old_object: Some(stored)`), and the same delete must succeed once that
/// stored content changes.
/// Verifies a webhook bound to a CRD's resource (`example.com/v1/foos`)
/// denies all of CREATE/UPDATE/DELETE — the dispatcher routes by
/// `(apiGroup, version, resource)` exactly like any built-in resource.
#[tokio::test]
async fn should_be_able_to_deny_custom_resource_creation_update_and_deletion() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_cr_data_validator().await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cr"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![
                    OperationType::Create,
                    OperationType::Update,
                    OperationType::Delete,
                ],
                rule: Rule {
                    api_groups: vec!["example.com".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["foos".to_string()],
                    scope: None,
                },
            }],
            ..validating(
                "deny-unwanted-custom-resource-data.k8s.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-cr"),
        &cfg,
    )
    .await
    .unwrap();

    let gvk = GroupVersionKind {
        group: "example.com".into(),
        version: "v1".into(),
        kind: "Foo".into(),
    };
    let gvr = GroupVersionResource {
        group: "example.com".into(),
        version: "v1".into(),
        resource: "foos".into(),
    };
    let cr = |name: &str, marker: &str| {
        json!({
            "apiVersion": "example.com/v1",
            "kind": "Foo",
            "metadata": {"name": name, "namespace": "default"},
            "data": {"webhook-e2e-test": marker}
        })
    };
    let run = |op: Operation, name: &'static str, object: Option<Value>, old: Option<Value>| {
        let (manager, gvk, gvr) = (&manager, &gvk, &gvr);
        async move {
            manager
                .run_validating_webhooks(
                    &op,
                    gvk,
                    gvr,
                    Some("default"),
                    name,
                    object,
                    old,
                    &admin_user_info(),
                )
                .await
                .unwrap()
        }
    };

    // testCustomResourceWebhook (webhook.go:2112-2134): a CR carrying
    // `webhook-disallow` must be rejected on create.
    let resp = run(
        Operation::Create,
        "cr-instance-1",
        Some(cr("cr-instance-1", "webhook-disallow")),
        None,
    )
    .await;
    match resp {
        AdmissionResponse::Deny(reason) => assert!(
            reason.contains("the custom resource contains unwanted data"),
            "unexpected deny reason: {reason}"
        ),
        other => panic!("create of a disallowed CR must be denied, got {other:?}"),
    }

    // testBlockingCustomResourceUpdateDeletion (webhook.go:2136-2194).
    // 1. a CR marked `webhook-nondeletable` is created *successfully*.
    let resp = run(
        Operation::Create,
        "cr-instance-2",
        Some(cr("cr-instance-2", "webhook-nondeletable")),
        None,
    )
    .await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "a nondeletable CR must still be creatable, got {resp:?}"
    );

    // 2. updating it to disallowed data is denied.
    let resp = run(
        Operation::Update,
        "cr-instance-2",
        Some(cr("cr-instance-2", "webhook-disallow")),
        Some(cr("cr-instance-2", "webhook-nondeletable")),
    )
    .await;
    match resp {
        AdmissionResponse::Deny(reason) => assert!(
            reason.contains("the custom resource contains unwanted data"),
            "unexpected deny reason: {reason}"
        ),
        other => panic!("update to disallowed data must be denied, got {other:?}"),
    }

    // 3. deleting it is denied — and the decision can only come from the
    //    *stored* object, because a delete AdmissionReview carries no new
    //    object (handlers/admission_helper.rs passes `object: None`,
    //    `old_object: Some(stored)`).
    let resp = run(
        Operation::Delete,
        "cr-instance-2",
        None,
        Some(cr("cr-instance-2", "webhook-nondeletable")),
    )
    .await;
    match resp {
        AdmissionResponse::Deny(reason) => assert!(
            reason.contains(
                "the custom resource cannot be deleted because it contains unwanted key and value"
            ),
            "unexpected deny reason: {reason}"
        ),
        other => panic!("delete of a nondeletable CR must be denied, got {other:?}"),
    }

    // 4. rewriting the offending value to `webhook-allow` is admitted.
    let resp = run(
        Operation::Update,
        "cr-instance-2",
        Some(cr("cr-instance-2", "webhook-allow")),
        Some(cr("cr-instance-2", "webhook-nondeletable")),
    )
    .await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "update to compliant data must be admitted, got {resp:?}"
    );

    // 5. and the delete now succeeds — the same operation that was blocked in
    //    step 3, unblocked purely by the stored object's content.
    let resp = run(
        Operation::Delete,
        "cr-instance-2",
        None,
        Some(cr("cr-instance-2", "webhook-allow")),
    )
    .await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "delete must succeed once the offending value is gone, got {resp:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should unconditionally reject
/// operations on fail closed webhook [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:237
///   ("should unconditionally reject operations on fail closed webhook")
///   Assertions live in testFailClosedWebhook (webhook.go:1553-1575).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream does not merely require an error — it requires
/// `apierrors.IsInternalError(err)` (webhook.go:1571-1573), a 500 Status with
/// reason "InternalError". The mirror asserted only `is_err()` and its own
/// comment admitted it did not know the outcome ("maps to a 500/Deny depending
/// on call site"), so the contract went untested. The HTTP half now pins it.
/// Sonobuoy (Round 160): PASS
///
/// A webhook with `failurePolicy: Fail` and no reachable backend must
/// cause matching operations to be rejected.
#[tokio::test]
async fn should_unconditionally_reject_operations_on_fail_closed_webhook() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Unreachable URL + FailurePolicy::Fail + short timeout → manager must
    // surface the failure as an error (which the api-server pipeline maps
    // to a 500/Deny depending on call site).
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("fail-closed"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            timeout_seconds: Some(1),
            ..validating(
                "fail.closed.io",
                "http://127.0.0.1:1/unreachable".to_string(),
                vec![],
                Some(FailurePolicy::Fail),
                Some(1),
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "fail-closed"),
        &cfg,
    )
    .await
    .unwrap();

    let result = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-fail-closed",
            Some(json!({"metadata":{"name":"cm-fail-closed"}})),
            None,
            &admin_user_info(),
        )
        .await;

    assert!(
        result.is_err(),
        "fail-closed webhook with unreachable backend must surface an error, got {result:?}"
    );

    // Upstream is specific about *which* error: `apierrors.IsInternalError`
    // (webhook.go:1571-1573), i.e. a 500 Status with reason "InternalError".
    // Asserting that needs the HTTP path, since the manager returns a Rust
    // error rather than a Status. The old comment here hedged — "maps to a
    // 500/Deny depending on call site" — which left the actual contract
    // untested.
    let (router_mem, router) = spawn_router();
    router_mem
        .create(
            &build_key("validatingwebhookconfigurations", None, "fail-closed"),
            &cfg,
        )
        .await
        .unwrap();
    let (status, body) = post_json(
        router,
        "/api/v1/namespaces/default/configmaps",
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "foo"}
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "an unreachable fail-closed webhook must reject with an internal error: {body}"
    );
    assert_eq!(
        body["reason"], "InternalError",
        "upstream requires apierrors.IsInternalError: {body}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate configmap [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:249
///   ("should mutate configmap")
///   Assertions live in testMutatingConfigMapWebhook (webhook.go:1273-1288);
///   the configuration is built by registerMutatingWebhookForConfigMap
///   (webhook.go:1249-1272).
/// Sonobuoy (Round 160): PASS
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream's Testname is "Admission webhook, **ordered** mutation" and its
/// Description spells the contract out: two webhooks admit configmaps, "one
/// that adds a data key if the configmap already has a specific key, and
/// another that adds a key if the key added by the first webhook is present"
/// (webhook.go:243-247). Stage 2 can only fire if it observes stage 1's
/// output, so the case proves ordering. It then compares the resulting map
/// with `reflect.DeepEqual` against exactly
/// `{mutation-start, mutation-stage-1, mutation-stage-2}`.
///
/// The mirror registered **one** unconditional webhook, mutated
/// `metadata.labels` rather than `data`, and asserted that a single key was
/// present. Ordering was untested, and a subset check cannot catch a mutation
/// that drops `mutation-start` or adds something extra. Both stages are now
/// conditional (`start_mutator_configmap_stage`) and the assertion is exact
/// map equality, as upstream's is.
#[tokio::test]
async fn should_mutate_configmap() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Two chained stages, exactly as `registerMutatingWebhookForConfigMap`
    // registers them (webhook.go:1259-1260): stage 1 keys off the configmap's
    // own `mutation-start`, stage 2 keys off stage 1's output.
    let (url1, _s1) =
        start_mutator_data_stage("mutation-start".into(), "mutation-stage-1".into()).await;
    let (url2, _s2) =
        start_mutator_data_stage("mutation-stage-1".into(), "mutation-stage-2".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cm"),
        webhooks: Some(vec![
            MutatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                ..mutating(
                    "adding-configmap-data-stage-1.k8s.io",
                    url1,
                    vec![],
                    None,
                    None,
                )
            },
            MutatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                ..mutating(
                    "adding-configmap-data-stage-2.k8s.io",
                    url2,
                    vec![],
                    None,
                    None,
                )
            },
        ]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cm"),
        &cfg,
    )
    .await
    .unwrap();

    // Upstream's `toBeMutatedConfigMap` carries a single `mutation-start` key.
    let object = Some(json!({
        "metadata": {"name": "to-be-mutated"},
        "data": {"mutation-start": "yes"}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "to-be-mutated",
            object,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated object");

    // Upstream compares the whole map with reflect.DeepEqual
    // (webhook.go:1284-1287), so this is exact equality, not a subset check:
    // `mutation-start` must survive, both stages must have fired, and nothing
    // else may have been added.
    assert_eq!(
        obj["data"],
        json!({
            "mutation-start": "yes",
            "mutation-stage-1": "yes",
            "mutation-stage-2": "yes"
        }),
        "expected the ordered mutation to produce exactly the three keys: {obj}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate pod and apply defaults
/// after mutation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:260
///   ("should mutate pod and apply defaults after mutation")
///   Assertions live in testMutatingPodWebhook (webhook.go:1339-1354).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream asserts three things, and the third is the half the case is named
/// for: exactly one init container, its name, and that its
/// `TerminationMessagePolicy` **defaulted** to `File`
/// (v1.TerminationMessageReadFile) — which only holds if the api-server's
/// defaulting pass runs *after* the webhook mutation. The webhook's JSON patch
/// deliberately omits the field.
///
/// The mirror asserted the first two and dropped the defaulting one entirely,
/// so "apply defaults after mutation" was untested. It also ran against
/// `AdmissionWebhookManager` directly, where no defaulting happens; it now
/// drives the real `POST /api/v1/namespaces/default/pods` path and checks both
/// the response and the stored object.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_pod_and_apply_defaults_after_mutation() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_init_container().await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("adding-init-container"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..mutating("adding-init-container.k8s.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key(
            "mutatingwebhookconfigurations",
            None,
            "adding-init-container",
        ),
        &cfg,
    )
    .await
    .unwrap();

    // Upstream's `toBeMutatedPod` (webhook.go:1356-1370): one container, no
    // initContainers, and crucially no terminationMessagePolicy anywhere.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "webhook-to-be-mutated"},
        "spec": {"containers": [{"name": "example", "image": "registry.k8s.io/pause:3.10"}]}
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed: {body}"
    );

    // 1: exactly one init container — upstream tests the count, not presence.
    let init = body["spec"]["initContainers"]
        .as_array()
        .unwrap_or_else(|| panic!("initContainers must be present after mutation: {body}"));
    assert_eq!(
        init.len(),
        1,
        "expect pod to have 1 init container, got {init:?}"
    );

    // 2: the webhook-added name.
    assert_eq!(
        init[0]["name"], "webhook-added-init-container",
        "unexpected init container name: {body}"
    );

    // 3: the defaulting half the case is named for. The webhook's JSON patch
    // sets no terminationMessagePolicy, so the api-server's defaulting pass
    // must run *after* the mutation and fill in "File"
    // (handlers/defaults.rs:129-131; upstream
    // v1.TerminationMessageReadFile).
    assert_eq!(
        init[0]["terminationMessagePolicy"], "File",
        "expect the init terminationMessagePolicy to default to File — \
         defaulting must run after webhook mutation: {body}"
    );

    // The same must hold for what was persisted, not just the response.
    let stored: Value = mem
        .get(&build_key("pods", Some("default"), "webhook-to-be-mutated"))
        .await
        .expect("pod must be stored");
    assert_eq!(
        stored["spec"]["initContainers"][0]["terminationMessagePolicy"], "File",
        "stored pod must carry the defaulted terminationMessagePolicy: {stored}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should not be able to mutate or
/// prevent deletion of webhook configuration objects [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:272
///   ("should not be able to mutate or prevent deletion of webhook configuration objects")
///   Assertions live in testWebhooksForWebhookConfigurations
///   (webhook.go:1696-1830).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// The case has two halves, and the mirror only had one. Webhook
/// *configuration* objects are exempt from admission webhooks — the lockout
/// protection — which upstream checks by (a) registering a mutating webhook
/// that stamps a label onto everything and requiring the freshly-created
/// configuration NOT to carry it, and (b) deleting the configuration while a
/// deletion-denying webhook is registered. The mirror covered only the
/// deletion half, and only for ValidatingWebhookConfiguration; upstream does
/// both halves for both kinds.
/// Sonobuoy (Round 160): PASS
///
/// A deny-everything webhook that registers itself against admissionregistration
/// resources must NOT be invoked on those resources — that would lock out the
/// cluster. We assert that even with such a webhook configured, the
/// `webhook_matches()` filter excludes its own resource kind. We exercise via
/// HTTP: register the deny webhook, then PUT an update to its own config and
/// expect 200, then DELETE it and expect 200.
#[tokio::test]
async fn should_not_be_able_to_mutate_or_prevent_deletion_of_webhook_configuration_objects() {
    let (mem, router) = spawn_router();
    let (raw_url, _shutdown) = start_deny_validator("would deny everything".into()).await;
    // clientConfig.url must use the https scheme (upstream validateWebhookURL,
    // enforced on the update path here). This test updates the webhook config
    // itself through the router, so the URL is validated. The deny server is
    // only contacted if the self-targeting protection regressed and the webhook
    // actually fired — where the scheme/transport mismatch still yields a
    // non-200, keeping the assertion meaningful.
    let url = raw_url.replacen("http://", "https://", 1);

    // Register a deny-all webhook directly in storage to skip the create
    // round-trip's CEL validation overhead.
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("self-targeting"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for(
                "admissionregistration.k8s.io",
                "v1",
                "validatingwebhookconfigurations",
            )],
            ..validating(
                "self.target.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "self-targeting"),
        &cfg,
    )
    .await
    .unwrap();

    // Update via PUT through the real router. If the webhook were invoked
    // we'd see a 5xx; if the protection works we see 200.
    let mut updated = cfg.clone();
    updated.metadata.labels = Some({
        let mut m = std::collections::HashMap::new();
        m.insert("touched".into(), "true".into());
        m
    });
    let (status, body) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/self-targeting",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update must succeed: {body}");

    // DELETE must also succeed (200/202/204 — any 2xx).
    let status = delete_status(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/self-targeting",
    )
    .await;
    assert!(status.is_success(), "delete must succeed, got {status}");

    // The other half of the upstream case: webhook *configuration* objects
    // must also come back unmutated. Upstream registers a mutating webhook
    // that stamps `addedLabelKey=addedLabelValue` onto everything it sees and
    // then requires that the freshly-created configuration does NOT carry it
    // (webhook.go:1746-1748 for the validating config, and again for the
    // mutating one at webhook.go:1806-1808).
    let (label_url_raw, _label_shutdown) =
        start_mutator_label("webhook-e2e-test".into(), "webhook-added-label".into()).await;
    let label_url = label_url_raw.replacen("http://", "https://", 1);
    let mutate_everything = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("adding-label"),
        webhooks: Some(vec![
            MutatingWebhook {
                rules: vec![rule_for(
                    "admissionregistration.k8s.io",
                    "v1",
                    "validatingwebhookconfigurations",
                )],
                ..mutating("adding-label.k8s.io", label_url.clone(), vec![], None, None)
            },
            MutatingWebhook {
                rules: vec![rule_for(
                    "admissionregistration.k8s.io",
                    "v1",
                    "mutatingwebhookconfigurations",
                )],
                ..mutating("adding-label-2.k8s.io", label_url, vec![], None, None)
            },
        ]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "adding-label"),
        &mutate_everything,
    )
    .await
    .unwrap();

    // A dummy validating configuration, created through the router.
    let dummy_validating = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "dummy-validating"},
        "webhooks": [{
            "name": "dummy-validating-webhook.k8s.io",
            "clientConfig": {"url": "https://127.0.0.1:1/"},
            // Deliberately matches no real resource, as upstream's does.
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["invalid"]
            }],
            "failurePolicy": "Ignore",
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"]
        }]
    });
    let (status, created) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &dummy_validating,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "creating a validating webhook configuration must succeed: {created}"
    );
    assert!(
        created["metadata"]["labels"]["webhook-e2e-test"].is_null(),
        "expected the validating webhook configuration not to be mutated by          mutating webhooks, but it was: {created}"
    );

    // And the same for a mutating configuration.
    let dummy_mutating = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "dummy-mutating"},
        "webhooks": [{
            "name": "dummy-mutating-webhook.k8s.io",
            "clientConfig": {"url": "https://127.0.0.1:1/"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["invalid"]
            }],
            "failurePolicy": "Ignore",
            "sideEffects": "None",
            "admissionReviewVersions": ["v1"]
        }]
    });
    let (status, created) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &dummy_mutating,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "creating a mutating webhook configuration must succeed: {created}"
    );
    assert!(
        created["metadata"]["labels"]["webhook-e2e-test"].is_null(),
        "expected the mutating webhook configuration not to be mutated by          mutating webhooks, but it was: {created}"
    );

    // Both must remain deletable — upstream deletes each one right after
    // creating it (webhook.go:1754-1757 and :1814-1817).
    for (plural, name) in [
        ("validatingwebhookconfigurations", "dummy-validating"),
        ("mutatingwebhookconfigurations", "dummy-mutating"),
    ] {
        let status = delete_status(
            router.clone(),
            &format!("/apis/admissionregistration.k8s.io/v1/{plural}/{name}"),
        )
        .await;
        assert!(
            status.is_success(),
            "deleting {plural}/{name} must succeed, got {status}"
        );
    }
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:284
///   ("should mutate custom resource")
///   Assertions live in testMutatingCustomResourceWebhook with prune=false
///   (webhook.go:2196-2224).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Same two-stage chained shape as the configmap case, over the CR's
/// top-level `data` map, compared with `reflect.DeepEqual`. The mirror ran one
/// unconditional webhook against `metadata.labels` and asserted a single
/// label, so neither the ordering nor the exact-map contract was tested.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_custom_resource() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // The same two chained stages upstream registers for custom resources
    // (registerMutatingWebhookForCustomResource, webhook.go:2036-2111).
    let (url1, _s1) =
        start_mutator_data_stage("mutation-start".into(), "mutation-stage-1".into()).await;
    let (url2, _s2) =
        start_mutator_data_stage("mutation-stage-1".into(), "mutation-stage-2".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cr"),
        webhooks: Some(vec![
            MutatingWebhook {
                rules: vec![rule_for("example.com", "v1", "foos")],
                ..mutating(
                    "adding-custom-resource-data-stage-1.k8s.io",
                    url1,
                    vec![],
                    None,
                    None,
                )
            },
            MutatingWebhook {
                rules: vec![rule_for("example.com", "v1", "foos")],
                ..mutating(
                    "adding-custom-resource-data-stage-2.k8s.io",
                    url2,
                    vec![],
                    None,
                    None,
                )
            },
        ]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cr"),
        &cfg,
    )
    .await
    .unwrap();

    let cr = Some(json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "cr-instance-1", "namespace": "default"},
        "data": {"mutation-start": "yes"}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "example.com".into(),
                version: "v1".into(),
                kind: "Foo".into(),
            },
            &GroupVersionResource {
                group: "example.com".into(),
                version: "v1".into(),
                resource: "foos".into(),
            },
            Some("default"),
            "cr-instance-1",
            cr,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated CR");

    // Upstream compares the whole `data` map with reflect.DeepEqual
    // (webhook.go:2214-2223) — exact equality, and with `prune == false` all
    // three keys must be present.
    assert_eq!(
        obj["data"],
        json!({
            "mutation-start": "yes",
            "mutation-stage-1": "yes",
            "mutation-stage-2": "yes"
        }),
        "expected both stages to have fired: {obj}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should deny crd creation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:300
///   ("should deny crd creation")
///   Assertions live in testCRDDenyWebhook (webhook.go:2342-2400).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream's webhook denies on a *label*: the CRD it submits carries
/// `webhook-e2e-test: webhook-disallow` and the create must fail with "the crd
/// contains unwanted label". The mirror pointed a deny-everything validator at
/// a CRD with no labels at all, so the label condition was never exercised and
/// no Allow was asserted. Both the denied and the admitted case now run.
/// Sonobuoy (Round 160): PASS
///
/// A validating webhook scoped to
/// `apiextensions.k8s.io/v1.customresourcedefinitions` must deny CRD CREATE.
#[tokio::test]
async fn should_deny_crd_creation() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_label_deny_validator().await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-crd"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
            )],
            ..validating(
                "deny-crd.k8s.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-crd"),
        &cfg,
    )
    .await
    .unwrap();

    let crd = |labels: Value| {
        json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foos.example.com", "labels": labels}
        })
    };
    let run = |object: Value| {
        let manager = &manager;
        async move {
            manager
                .run_validating_webhooks(
                    &Operation::Create,
                    &GroupVersionKind {
                        group: "apiextensions.k8s.io".into(),
                        version: "v1".into(),
                        kind: "CustomResourceDefinition".into(),
                    },
                    &GroupVersionResource {
                        group: "apiextensions.k8s.io".into(),
                        version: "v1".into(),
                        resource: "customresourcedefinitions".into(),
                    },
                    None,
                    "foos.example.com",
                    Some(object),
                    None,
                    &admin_user_info(),
                )
                .await
                .unwrap()
        }
    };

    // Upstream's CRD carries the disallowed label and the create must fail
    // with the webhook's message (webhook.go:2396-2400).
    let resp = run(crd(json!({"webhook-e2e-test": "webhook-disallow"}))).await;
    match resp {
        AdmissionResponse::Deny(reason) => assert!(
            reason.contains("the crd contains unwanted label"),
            "unexpected deny reason: {reason}"
        ),
        other => panic!("a CRD with the disallowed label must be denied, got {other:?}"),
    }

    // The denial is label-conditional: without it the same CRD is admitted.
    // Nothing in the mirror asserted an Allow before, so a deny-everything
    // webhook satisfied it.
    let resp = run(crd(json!({"webhook-e2e-test": "webhook-allow"}))).await;
    assert!(
        matches!(resp, AdmissionResponse::Allow),
        "a CRD without the disallowed label must be admitted, got {resp:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource with
/// different stored version [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:314
///   ("should mutate custom resource with different stored version")
///   Assertions live in testMultiVersionCustomResourceWebhook
///   (webhook.go:2226-2287).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream's sequence is: create a CR while **v1** is the storage version;
/// patch the CRD so **v2** becomes storage; then JSON-patch the CR through the
/// **v2** client adding `/dummy`. It then asserts the mutating webhook's three
/// `data` keys are all present *and* `dummy == "test"` — i.e. the webhook
/// still fires, and its mutations survive, when the requested version is not
/// the stored one.
///
/// Not mirrored, and why: the storage-version switch and the read-modify-write
/// through a different served version need the CRD conversion path (a
/// storage-version patch on the CRD, then a versioned client round-trip),
/// which `AdmissionWebhookManager` does not sit on. What is decidable here is
/// the half the manager owns — that the mutating webhook is selected and
/// applies for each served version — and that is what this mirror asserts.
/// Making the conversion half assertable is a bigger piece of work than the
/// mirror audit; it is not silently claimed as covered.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_custom_resource_with_different_stored_version() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_label("crv".into(), "v2".into()).await;

    // Register a webhook matching both v1 and v2 of the CR. The manager must
    // dispatch to it regardless of the stored version (the upstream test
    // creates the CR via v1, then again via v2, and asserts both are mutated).
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cr-vers"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["example.com".into()],
                    api_versions: vec!["v1".into(), "v2".into()],
                    resources: vec!["foos".into()],
                    scope: None,
                },
            }],
            ..mutating("mutate.cr.vers.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cr-vers"),
        &cfg,
    )
    .await
    .unwrap();

    for version in ["v1", "v2"] {
        let cr = Some(json!({
            "apiVersion": format!("example.com/{version}"),
            "kind": "Foo",
            "metadata": {"name": "cv", "labels": {}}
        }));
        let (_resp, mutated) = manager
            .run_mutating_webhooks(
                &Operation::Create,
                &GroupVersionKind {
                    group: "example.com".into(),
                    version: version.into(),
                    kind: "Foo".into(),
                },
                &GroupVersionResource {
                    group: "example.com".into(),
                    version: version.into(),
                    resource: "foos".into(),
                },
                Some("default"),
                "cv",
                cr,
                None,
                &admin_user_info(),
            )
            .await
            .unwrap();
        let obj = mutated.expect("mutated CR");
        assert_eq!(
            obj["metadata"]["labels"]["crv"],
            json!("v2"),
            "version {version}"
        );
    }
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource with
/// pruning [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:331
///   ("should mutate custom resource with pruning")
///   Assertions live in testMutatingCustomResourceWebhook with prune=true
///   (webhook.go:2196-2224).
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// The pruning variant is the same two chained webhooks against a structural
/// schema that declares `mutation-start` and `mutation-stage-1` but not
/// `mutation-stage-2`: stage 2 still runs, and its key must be pruned back off
/// after admission, leaving exactly the two declared keys. The mirror tested
/// the right *concept* with its own `spec.replicas` / `spec.notInSchema`
/// fixture, but not upstream's object shape and not as an exact-map
/// comparison.
///
/// Verifies the K8s contract: after a mutating webhook injects a field into
/// a CR via JSON patch, the api-server runs structural-schema pruning. Any
/// field absent from the CRD's `openAPIV3Schema` must be stripped (unless
/// `x-kubernetes-preserve-unknown-fields` is set).
///
/// Setup: a CRD declares `spec.replicas` only. A mutating webhook adds an
/// extra field `/spec/notInSchema`. After `run_mutating_webhooks`, the
/// returned object must have `spec.replicas` intact and `spec.notInSchema`
/// removed by the pruning pass.
#[tokio::test]
async fn should_mutate_custom_resource_with_pruning() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Upstream's pruning variant declares a structural schema that knows
    // `mutation-start` and `mutation-stage-1` but *not* `mutation-stage-2`,
    // with `x-kubernetes-preserve-unknown-fields` absent — so the key stage 2
    // adds is pruned back off after admission
    // (webhook.go:331-357 sets the CRD up, testMutatingCustomResourceWebhook
    // asserts the result with prune=true).
    let crd_json = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "foos.example.com"},
        "spec": {
            "group": "example.com",
            "names": {"plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object",
                                "properties": {
                                    "mutation-start": {"type": "string"},
                                    "mutation-stage-1": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });
    let crd: rusternetes_common::resources::CustomResourceDefinition =
        serde_json::from_value(crd_json).unwrap();
    mem.create(
        &build_key("customresourcedefinitions", None, "foos.example.com"),
        &crd,
    )
    .await
    .unwrap();

    let (url1, _s1) =
        start_mutator_data_stage("mutation-start".into(), "mutation-stage-1".into()).await;
    let (url2, _s2) =
        start_mutator_data_stage("mutation-stage-1".into(), "mutation-stage-2".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cr-prune"),
        webhooks: Some(vec![
            MutatingWebhook {
                rules: vec![rule_for("example.com", "v1", "foos")],
                ..mutating(
                    "adding-custom-resource-data-stage-1.k8s.io",
                    url1,
                    vec![],
                    None,
                    None,
                )
            },
            MutatingWebhook {
                rules: vec![rule_for("example.com", "v1", "foos")],
                ..mutating(
                    "adding-custom-resource-data-stage-2.k8s.io",
                    url2,
                    vec![],
                    None,
                    None,
                )
            },
        ]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cr-prune"),
        &cfg,
    )
    .await
    .unwrap();

    let cr = Some(json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "cr-instance-1", "namespace": "default"},
        "data": {"mutation-start": "yes"}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "example.com".into(),
                version: "v1".into(),
                kind: "Foo".into(),
            },
            &GroupVersionResource {
                group: "example.com".into(),
                version: "v1".into(),
                resource: "foos".into(),
            },
            Some("default"),
            "cr-instance-1",
            cr,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated CR");

    // With prune=true upstream expects exactly the two declared keys —
    // stage 2 still ran, but its output is not in the schema and must be
    // pruned away (webhook.go:2216-2223).
    assert_eq!(
        obj["data"],
        json!({
            "mutation-start": "yes",
            "mutation-stage-1": "yes"
        }),
        "mutation-stage-2 is undeclared and must be pruned after mutation: {obj}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should honor timeout [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:370
///   ("should honor timeout")
///   Assertions live in testSlowWebhookTimeoutFailEarly (webhook.go:2480-2494)
///   and testSlowWebhookTimeoutNoError (webhook.go:2495-2509).
/// Sonobuoy (Round 160+): PASS — the slow webhook is aborted at the
/// `timeoutSeconds` boundary and the surfaced error reads as a timeout and
/// names the queried endpoint, which is what upstream matches on. The deadline
/// is enforced by [`AdmissionWebhookManager::call_webhook_with_ca`] wrapping
/// the inner reqwest call in `tokio::time::timeout`.
#[tokio::test]
async fn should_honor_timeout() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    // The webhook's own response latency. The 1s `timeoutSeconds` below must
    // abort the call long before this elapses. Keeping a wide gap lets the
    // wall-clock assertion tolerate tokio-timer wakeup jitter under
    // CPU-saturated parallel test load — the old fixed `< 3s` bound flaked at
    // ~3.1s on a loaded runner even though the deadline fired correctly. When
    // the timeout works the test still returns in ~1s; this longer sleep only
    // matters as an upper bound if the timeout regresses.
    let slow_webhook_sleep = std::time::Duration::from_secs(10);
    let (url, _shutdown) = start_slow_validator(slow_webhook_sleep).await;
    let url_for_slow = url.clone();

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("slow-fail"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            timeout_seconds: Some(1),
            ..validating("slow.io", url, vec![], Some(FailurePolicy::Fail), Some(1))
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "slow-fail"),
        &cfg,
    )
    .await
    .unwrap();

    let started = std::time::Instant::now();
    let res = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-slow",
            Some(json!({"metadata":{"name":"cm-slow"}})),
            None,
            &admin_user_info(),
        )
        .await;
    let elapsed = started.elapsed();

    // Primary, load-independent proof the deadline fired: the slow validator
    // only ever returns `allow`, so an Err can only originate from the 1s
    // timeout aborting the call (the message assertion below pins it to the
    // timeout specifically). The wall-clock check is a secondary guard that the
    // call was aborted *early* rather than blocking for the webhook's full
    // response — asserted relative to `slow_webhook_sleep` so it does not flake
    // when the runtime's timers are delayed under parallel-test CPU pressure.
    assert!(res.is_err(), "slow webhook + FailurePolicy=Fail must error");
    assert!(
        elapsed < slow_webhook_sleep,
        "1s deadline must abort before the {slow_webhook_sleep:?} webhook response; took {elapsed:?}"
    );
    // Upstream `testSlowWebhookTimeoutFailEarly`
    // (`test/e2e/apimachinery/webhook.go:2480-2494`) makes exactly two checks
    // on the returned error:
    //
    //     isTimeoutError := strings.Contains(err.Error(), `context deadline exceeded`) ||
    //                       strings.Contains(err.Error(), `timeout`)
    //     isErrorQueryingWebhook := strings.Contains(err.Error(), `/always-allow-delay-5s?timeout=1s`)
    //
    // This used to assert the literal substring "HTTP/dial timeout" instead,
    // and the webhook client spliced that phrase into its error to satisfy it.
    // The phrase occurs in upstream only inside `framework.Failf` — the message
    // printed when the assertion *fails*. It is not something any Kubernetes
    // component emits, and nothing upstream matches on it.
    let msg = format!("{}", res.unwrap_err());
    assert!(
        msg.contains("context deadline exceeded") || msg.contains("timeout"),
        "error must read as a timeout (upstream webhook.go:2488); got {msg:?}"
    );
    assert!(
        msg.contains("?timeout=1s"),
        "error must name the queried endpoint with its timeout query \
         (upstream webhook.go:2489); got {msg:?}"
    );

    // Upstream runs three more scenarios after the fail-early one, all of
    // which must produce **no** error (webhook.go:379-392):
    //
    //   2. timeout shorter than latency, failurePolicy Ignore
    //   3. timeout longer than latency, failurePolicy Fail
    //   4. timeout unset — defaulted to 10s in v1 — failurePolicy Fail
    //
    // The mirror stopped after scenario 1, so neither the fail-open path nor
    // the timeout default was covered. Scenarios 3 and 4 use a short-latency
    // validator: what upstream is pinning is the *relationship* between the
    // timeout and the latency, and reproducing its literal 5s response would
    // add seconds of wall clock for nothing.
    let (fast_url, _fast_shutdown) =
        start_slow_validator(std::time::Duration::from_millis(50)).await;

    let scenarios: [(&str, Option<i32>, FailurePolicy, String); 3] = [
        (
            "slow-ignore",
            Some(1),
            FailurePolicy::Ignore,
            url_for_slow.clone(),
        ),
        (
            "slow-long-timeout",
            Some(10),
            FailurePolicy::Fail,
            fast_url.clone(),
        ),
        ("slow-default-timeout", None, FailurePolicy::Fail, fast_url),
    ];

    for (name, timeout, policy, hook_url) in scenarios {
        let cfg = ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            webhooks: Some(vec![ValidatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                timeout_seconds: timeout,
                ..validating("slow.io", hook_url, vec![], Some(policy), timeout)
            }]),
        };
        let scenario_mem = Arc::new(MemoryStorage::new());
        scenario_mem
            .create(
                &build_key("validatingwebhookconfigurations", None, name),
                &cfg,
            )
            .await
            .unwrap();
        let scenario_manager = AdmissionWebhookManager::new(scenario_mem);

        let res = scenario_manager
            .run_validating_webhooks(
                &Operation::Create,
                &GroupVersionKind {
                    group: "".into(),
                    version: "v1".into(),
                    kind: "ConfigMap".into(),
                },
                &GroupVersionResource {
                    group: "".into(),
                    version: "v1".into(),
                    resource: "configmaps".into(),
                },
                Some("default"),
                "cm-slow",
                Some(json!({"metadata": {"name": "cm-slow"}})),
                None,
                &admin_user_info(),
            )
            .await;

        match res {
            Ok(AdmissionResponse::Allow) => {}
            other => panic!("{name}: expected no error and an Allow, got {other:?}"),
        }
    }
}

/// [sig-api-machinery] AdmissionWebhook patching/updating a validating
/// webhook should work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:402
///   ("patching/updating a validating webhook should work")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// The case is about the reconfiguration *taking effect*, not about the object
/// round-tripping. Upstream: create the config and confirm a non-compliant
/// configmap is denied; Update the rules to drop CREATE and confirm the same
/// configmap is now admitted; Patch CREATE back in and confirm it is denied
/// again (webhook.go:427-478).
///
/// The mirror was a pure CRUD round-trip — create, PUT to add a resource to the
/// rule, GET to confirm the rule changed — and never issued a single admission
/// request, so the behaviour the case exists to pin was untested. All three
/// admission checks now run against the real router, and the CRUD round-trip
/// is kept as the closing section.
///
/// Deviation: the rule set is rewritten through storage rather than through the
/// API. `validateWebhookURL` requires an https `clientConfig.url` on the
/// create/update path and the mock validator serves plain HTTP, so routing the
/// rewrites through the API would leave the webhook unreachable and every
/// attempt would return 500 (fail-closed) regardless of whether the rule
/// matched. Upstream sidesteps this by using a `service` reference rather than
/// a URL.
///
/// Upstream also asserts `HaveValidResourceVersion()` on create and an RV
/// increase after the patch (webhook.go:417, :471); both are unobservable on
/// `MemoryStorage` for the reason recorded in #1751.
/// Sonobuoy (Round 160): PASS
///
/// Verifies that POST → GET → PUT → GET round-trips a ValidatingWebhookConfiguration
/// through the REST API and preserves an update to the `rules` field.
#[tokio::test]
async fn patching_updating_a_validating_webhook_should_work() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_deny_validator("denied by webhook".to_string()).await;

    // The rule set is rewritten straight through storage rather than through
    // the API. `validateWebhookURL` requires an https `clientConfig.url` on the
    // create/update path, and the mock validator serves plain HTTP — routing
    // the rewrites through the API would leave the webhook unreachable, so
    // every attempt below would come back 500 (fail-closed) instead of
    // exercising whether the *rule* matches. The configuration object's own
    // create/update round-trip through the API is asserted at the end.
    let store_rules = |operations: Vec<&'static str>| {
        let url = url.clone();
        let mem = mem.clone();
        async move {
            let cfg = ValidatingWebhookConfiguration {
                api_version: "admissionregistration.k8s.io/v1".to_string(),
                kind: "ValidatingWebhookConfiguration".to_string(),
                metadata: rusternetes_common::types::ObjectMeta::new("vwc-patchable"),
                webhooks: Some(vec![ValidatingWebhook {
                    rules: vec![RuleWithOperations {
                        operations: operations
                            .into_iter()
                            .map(|o| match o {
                                "CREATE" => OperationType::Create,
                                "UPDATE" => OperationType::Update,
                                other => panic!("unhandled operation {other}"),
                            })
                            .collect(),
                        rule: Rule {
                            api_groups: vec!["".to_string()],
                            api_versions: vec!["v1".to_string()],
                            resources: vec!["configmaps".to_string()],
                            scope: None,
                        },
                    }],
                    ..validating(
                        "deny-unwanted-configmap-data.k8s.io",
                        url,
                        vec![],
                        Some(FailurePolicy::Fail),
                        None,
                    )
                }]),
            };
            let key = build_key("validatingwebhookconfigurations", None, "vwc-patchable");
            if mem.get::<Value>(&key).await.is_ok() {
                mem.update(&key, &cfg).await.unwrap();
            } else {
                mem.create(&key, &cfg).await.unwrap();
            }
        }
    };

    let attempt = |router: TestApiServer, name: &'static str| async move {
        let (status, _body) = post_json(
            router,
            "/api/v1/namespaces/default/configmaps",
            &json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name},
                "data": {"webhook-e2e-test": "webhook-disallow"}
            }),
        )
        .await;
        status
    };

    // 1. While the rule covers CREATE, the configmap is rejected.
    store_rules(vec!["CREATE"]).await;
    assert_eq!(
        attempt(router.clone(), "cm-1").await,
        StatusCode::FORBIDDEN,
        "the webhook rule covers CREATE, so the configmap must be denied"
    );

    // 2. Drop CREATE from the rule (upstream swaps to UPDATE only,
    //    webhook.go:442-448) — the same create must now be admitted.
    store_rules(vec!["UPDATE"]).await;
    assert_eq!(
        attempt(router.clone(), "cm-2").await,
        StatusCode::CREATED,
        "with CREATE removed from the rule the configmap must be admitted"
    );

    // 3. Put CREATE back (webhook.go:465-471) — denied again.
    store_rules(vec!["CREATE", "UPDATE"]).await;
    assert_eq!(
        attempt(router.clone(), "cm-3").await,
        StatusCode::FORBIDDEN,
        "restoring CREATE to the rule must make the webhook fire again"
    );

    // The configuration object itself must also create and update through the
    // API, which is the half the old mirror covered.
    let api_cfg = |resources: Value| {
        json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": {"name": "vwc-api-roundtrip"},
            "webhooks": [{
                "name": "vwc.patch.io",
                "clientConfig": {"url": "https://127.0.0.1:1/hook"},
                "rules": [{
                    "operations": ["CREATE"],
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "resources": resources
                }],
                "failurePolicy": "Ignore",
                "sideEffects": "None",
                "admissionReviewVersions": ["v1"]
            }]
        })
    };
    let (status, created) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &api_cfg(json!(["pods"])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create config: {created}");

    let (status, updated) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-api-roundtrip",
        &api_cfg(json!(["pods", "configmaps"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update config: {updated}");

    let (status, fetched) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-api-roundtrip",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get config: {fetched}");
    let resources: Vec<&str> = fetched["webhooks"][0]["rules"][0]["resources"]
        .as_array()
        .expect("resources array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        resources.contains(&"pods") && resources.contains(&"configmaps"),
        "the update must stick, got {resources:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook patching/updating a mutating webhook
/// should work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:497
///   ("patching/updating a mutating webhook should work")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// The mutating counterpart of `patching_updating_a_validating_webhook_should_work`
/// and it had the same gap: upstream creates the configuration and confirms a
/// configmap comes back **mutated**, Updates the rules to drop CREATE and
/// confirms the next configmap is **not** mutated, then Patches CREATE back in
/// and confirms mutation resumes (webhook.go:501-572). The mirror only
/// round-tripped the configuration object and never created a configmap, so
/// none of that was tested. Same storage-rewrite deviation as the validating
/// case, for the same `validateWebhookURL` reason.
///
/// Upstream's `HaveValidResourceVersion()` and post-patch RV comparison
/// (webhook.go:512, :549) are unobservable on `MemoryStorage` — #1751.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn patching_updating_a_mutating_webhook_should_work() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("mutated-by-webhook".into(), "yes".into()).await;

    // Same storage-rewrite deviation as the validating counterpart: the mock
    // serves plain HTTP, which `validateWebhookURL` rejects on the API's
    // create/update path.
    let store_rules = |operations: Vec<&'static str>| {
        let url = url.clone();
        let mem = mem.clone();
        async move {
            let cfg = MutatingWebhookConfiguration {
                api_version: "admissionregistration.k8s.io/v1".to_string(),
                kind: "MutatingWebhookConfiguration".to_string(),
                metadata: rusternetes_common::types::ObjectMeta::new("mwc-patchable"),
                webhooks: Some(vec![MutatingWebhook {
                    rules: vec![RuleWithOperations {
                        operations: operations
                            .into_iter()
                            .map(|o| match o {
                                "CREATE" => OperationType::Create,
                                "UPDATE" => OperationType::Update,
                                other => panic!("unhandled operation {other}"),
                            })
                            .collect(),
                        rule: Rule {
                            api_groups: vec!["".to_string()],
                            api_versions: vec!["v1".to_string()],
                            resources: vec!["configmaps".to_string()],
                            scope: None,
                        },
                    }],
                    ..mutating("adding-configmap-data.k8s.io", url, vec![], None, None)
                }]),
            };
            let key = build_key("mutatingwebhookconfigurations", None, "mwc-patchable");
            if mem.get::<Value>(&key).await.is_ok() {
                mem.update(&key, &cfg).await.unwrap();
            } else {
                mem.create(&key, &cfg).await.unwrap();
            }
        }
    };

    // Returns whether the created configmap came back carrying the webhook's
    // label.
    let was_mutated = |router: TestApiServer, name: &'static str| async move {
        let (status, body) = post_json(
            router,
            "/api/v1/namespaces/default/configmaps",
            &json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name, "labels": {}}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
        body["metadata"]["labels"]["mutated-by-webhook"] == "yes"
    };

    // 1. The rule covers CREATE — the configmap is mutated.
    store_rules(vec!["CREATE"]).await;
    assert!(
        was_mutated(router.clone(), "cm-1").await,
        "the webhook rule covers CREATE, so the configmap must be mutated"
    );

    // 2. Drop CREATE (upstream webhook.go:524-529) — no longer mutated.
    store_rules(vec!["UPDATE"]).await;
    assert!(
        !was_mutated(router.clone(), "cm-2").await,
        "with CREATE removed from the rule the configmap must not be mutated"
    );

    // 3. Put CREATE back (webhook.go:543-551) — mutated again.
    store_rules(vec!["CREATE", "UPDATE"]).await;
    assert!(
        was_mutated(router.clone(), "cm-3").await,
        "restoring CREATE to the rule must make the webhook fire again"
    );

    // The configuration object itself must create and update through the API.
    let api_cfg = |resources: Value| {
        json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "MutatingWebhookConfiguration",
            "metadata": {"name": "mwc-api-roundtrip"},
            "webhooks": [{
                "name": "mwc.patch.io",
                "clientConfig": {"url": "https://127.0.0.1:1/hook"},
                "rules": [{
                    "operations": ["CREATE"],
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "resources": resources
                }],
                "failurePolicy": "Ignore",
                "sideEffects": "None",
                "admissionReviewVersions": ["v1"]
            }]
        })
    };
    let (status, created) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &api_cfg(json!(["pods"])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create config: {created}");
    let (status, updated) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-api-roundtrip",
        &api_cfg(json!(["pods", "configmaps"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update config: {updated}");
    let resources: Vec<&str> = updated["webhooks"][0]["rules"][0]["resources"]
        .as_array()
        .expect("resources array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        resources.contains(&"pods") && resources.contains(&"configmaps"),
        "the update must stick, got {resources:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook listing validating webhooks should
/// work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:574
///   ("listing validating webhooks should work")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream creates **ten** configurations sharing a run-scoped label, lists
/// them *by that label selector* and requires exactly ten, then brackets the
/// DeleteCollection with admission attempts: a non-compliant configmap is
/// refused while the webhooks exist and accepted once the collection is gone
/// (webhook.go:575-646). The mirror created three, listed and deleted without
/// any selector, and never issued an admission request — so neither the
/// selector nor the effect of the collection delete was tested.
///
/// Deviation: the registered webhooks are unreachable with
/// `failurePolicy: Fail`, so "in effect" reads as 500 rather than upstream's
/// 403. The bracket is what the case is about, and this keeps the
/// configurations creatable through the API, which `validateWebhookURL` would
/// otherwise block for a plain-HTTP mock.
/// Sonobuoy (Round 160): PASS
///
/// Creates several VWCs, lists them, then deletes the collection and
/// asserts the list is empty.
#[tokio::test]
async fn listing_validating_webhooks_should_work() {
    let (_mem, router) = spawn_router();
    // Upstream creates ten, all carrying the same run-scoped label, and both
    // the list and the DeleteCollection are label-selected
    // (webhook.go:575-593).
    const TEST_LIST_SIZE: usize = 10;
    const SELECTOR: &str = "e2e-list-test-uuid=listing-validating";

    for i in 0..TEST_LIST_SIZE {
        let (status, body) = post_json(
            router.clone(),
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            &json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingWebhookConfiguration",
                "metadata": {
                    "name": format!("e2e-list-{i}"),
                    "labels": {"e2e-list-test-uuid": "listing-validating"}
                },
                "webhooks": [{
                    "name": "deny-unwanted-configmap-data.k8s.io",
                    // Unreachable on purpose: with failurePolicy Fail, "the
                    // webhook is in effect" is observable as a 500 on any
                    // matching request, and its absence as a successful create.
                    "clientConfig": {"url": "https://127.0.0.1:1/configmaps"},
                    "rules": [{
                        "operations": ["CREATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["configmaps"]
                    }],
                    "failurePolicy": "Fail",
                    "sideEffects": "None",
                    "admissionReviewVersions": ["v1"]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create e2e-list-{i}: {body}");
    }

    // Listing by the selector must return exactly the ten.
    let (status, body) = get_json(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list: {body}");
    assert_eq!(
        body["items"].as_array().map(Vec::len),
        Some(TEST_LIST_SIZE),
        "label-selected list must return exactly {TEST_LIST_SIZE} items: {body}"
    );

    // While they exist, a matching request cannot get through.
    let (status, _) = post_json(
        router.clone(),
        "/api/v1/namespaces/default/configmaps",
        &json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm-before"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "with fail-closed webhooks registered the configmap must not be created"
    );

    // DeleteCollection, by the same selector.
    let status = delete_status(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert!(status.is_success(), "deletecollection: {status}");

    let (_, body) = get_json(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert_eq!(
        body["items"].as_array().map(Vec::len),
        Some(0),
        "list must be empty after deletecollection: {body}"
    );

    // And now the same request succeeds — the collection delete took effect.
    let (status, body) = post_json(
        router,
        "/api/v1/namespaces/default/configmaps",
        &json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm-after"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "once the webhooks are deleted the configmap must be created: {body}"
    );
}

/// [sig-api-machinery] AdmissionWebhook listing mutating webhooks should
/// work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:648
///   ("listing mutating webhooks should work")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// The mutating counterpart of `listing_validating_webhooks_should_work`, and
/// it had the same three gaps: three configurations instead of ten, no label
/// selector on either the list or the DeleteCollection, and no admission
/// request bracketing the delete (webhook.go:649-716). Same fail-closed
/// deviation as the validating case.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn listing_mutating_webhooks_should_work() {
    let (_mem, router) = spawn_router();
    // Upstream creates ten, all carrying the same run-scoped label, and both
    // the list and the DeleteCollection are label-selected
    // (webhook.go:649-667).
    const TEST_LIST_SIZE: usize = 10;
    const SELECTOR: &str = "e2e-list-test-uuid=listing-mutating";

    for i in 0..TEST_LIST_SIZE {
        let (status, body) = post_json(
            router.clone(),
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            &json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "MutatingWebhookConfiguration",
                "metadata": {
                    "name": format!("e2e-list-{i}"),
                    "labels": {"e2e-list-test-uuid": "listing-mutating"}
                },
                "webhooks": [{
                    "name": "adding-configmap-data.k8s.io",
                    // Unreachable on purpose: with failurePolicy Fail, "the
                    // webhook is in effect" is observable as a 500 on any
                    // matching request, and its absence as a successful create.
                    "clientConfig": {"url": "https://127.0.0.1:1/configmaps"},
                    "rules": [{
                        "operations": ["CREATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["configmaps"]
                    }],
                    "failurePolicy": "Fail",
                    "sideEffects": "None",
                    "admissionReviewVersions": ["v1"]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create e2e-list-{i}: {body}");
    }

    // Listing by the selector must return exactly the ten.
    let (status, body) = get_json(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list: {body}");
    assert_eq!(
        body["items"].as_array().map(Vec::len),
        Some(TEST_LIST_SIZE),
        "label-selected list must return exactly {TEST_LIST_SIZE} items: {body}"
    );

    // While they exist, a matching request cannot get through.
    let (status, _) = post_json(
        router.clone(),
        "/api/v1/namespaces/default/configmaps",
        &json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm-before"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "with fail-closed webhooks registered the configmap must not be created"
    );

    // DeleteCollection, by the same selector.
    let status = delete_status(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert!(status.is_success(), "deletecollection: {status}");

    let (_, body) = get_json(
        router.clone(),
        &format!(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations?labelSelector={SELECTOR}"
        ),
    )
    .await;
    assert_eq!(
        body["items"].as_array().map(Vec::len),
        Some(0),
        "list must be empty after deletecollection: {body}"
    );

    // And now the same request succeeds — the collection delete took effect.
    let (status, body) = post_json(
        router,
        "/api/v1/namespaces/default/configmaps",
        &json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm-after"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "once the webhooks are deleted the configmap must be created: {body}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should be able to create and update
/// validating webhook configurations with match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:717
///   ("should be able to create and update validating webhook configurations with match conditions")
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_be_able_to_create_and_update_validating_webhook_configurations_with_match_conditions(
) {
    let (_mem, router) = spawn_router();

    let mc = vec![MatchCondition {
        name: "exclude-leases".into(),
        // Use a CEL expression that the api-server's permissive matcher
        // accepts (references undeclared variables fall through the
        // type-checker per handlers/admission_webhook.rs:75-82).
        expression: "object.metadata.name != 'leases'".into(),
    }];

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("vwc-match-cond"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(mc.clone()),
            ..validating(
                "match.cond.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");
    // Upstream compares the whole slice with gomega.Equal — names and
    // expressions both (webhook.go:734 / :788), not just its length.
    assert_eq!(
        serde_json::to_value(&mc).unwrap(),
        body["webhooks"][0]["matchConditions"],
        "the created object must echo the match conditions exactly: {body}"
    );

    // Update: add a second match condition.
    let mut updated: ValidatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        let conds = hooks[0].match_conditions.get_or_insert_with(Vec::new);
        conds.push(MatchCondition {
            name: "exclude-events".into(),
            expression: "object.kind != 'Event'".into(),
        });
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-match-cond",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-match-cond",
    )
    .await;
    let final_cfg: ValidatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    let conds = final_cfg.webhooks.unwrap()[0]
        .match_conditions
        .clone()
        .unwrap();
    // Upstream re-reads and compares the whole updated slice, again with
    // gomega.Equal (webhook.go:761 / :815).
    assert_eq!(
        conds,
        vec![
            mc[0].clone(),
            MatchCondition {
                name: "exclude-events".into(),
                expression: "object.kind != 'Event'".into(),
            },
        ],
        "the updated match conditions must round-trip exactly"
    );
}

/// [sig-api-machinery] AdmissionWebhook should be able to create and update
/// mutating webhook configurations with match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:771
///   ("should be able to create and update mutating webhook configurations with match conditions")
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_be_able_to_create_and_update_mutating_webhook_configurations_with_match_conditions()
{
    let (_mem, router) = spawn_router();

    let mc = vec![MatchCondition {
        name: "exclude-system".into(),
        expression: "object.metadata.namespace != 'kube-system'".into(),
    }];

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mwc-match-cond"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(mc.clone()),
            ..mutating(
                "mwc.match.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");
    // Upstream compares the whole slice with gomega.Equal — names and
    // expressions both (webhook.go:734 / :788), not just its length.
    assert_eq!(
        serde_json::to_value(&mc).unwrap(),
        body["webhooks"][0]["matchConditions"],
        "the created object must echo the match conditions exactly: {body}"
    );

    let mut updated: MutatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        let conds = hooks[0].match_conditions.get_or_insert_with(Vec::new);
        conds.push(MatchCondition {
            name: "exclude-priv".into(),
            expression: "object.metadata.name != 'priv'".into(),
        });
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-match-cond",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-match-cond",
    )
    .await;
    let final_cfg: MutatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    let conds = final_cfg.webhooks.unwrap()[0]
        .match_conditions
        .clone()
        .unwrap();
    // Upstream re-reads and compares the whole updated slice, again with
    // gomega.Equal (webhook.go:761 / :815).
    assert_eq!(
        conds,
        vec![
            mc[0].clone(),
            MatchCondition {
                name: "exclude-priv".into(),
                expression: "object.metadata.name != 'priv'".into(),
            },
        ],
        "the updated match conditions must round-trip exactly"
    );
}

/// [sig-api-machinery] AdmissionWebhook should reject validating webhook
/// configurations with invalid match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:825
///   ("should reject validating webhook configurations with invalid match conditions")
/// Sonobuoy (Round 160): PASS
///
/// The api-server compiles every CEL `matchConditions[].expression` at
/// admission time; invalid syntax must produce a 4xx (the handler maps it
/// to `InvalidResource` → 422).
#[tokio::test]
async fn should_reject_validating_webhook_configurations_with_invalid_match_conditions() {
    let (_mem, router) = spawn_router();

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("vwc-invalid-mc"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(vec![MatchCondition {
                name: "invalid-expression-1".into(),
                // Upstream's exact malformed CEL (webhook.go:827-830 and
                // :854-857). An *empty* expression — what this fixture used
                // to send — fails the required-field check instead, which is
                // a different code path and never reaches the CEL compiler.
                expression: "... [] bad expression".into(),
            }]),
            ..validating(
                "invalid.mc.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert!(
        status.is_client_error(),
        "create with invalid CEL must be 4xx; got {status} {body}"
    );
    // Upstream requires the message to name the CEL failure, not merely to be
    // a rejection (webhook.go:838-840 / :860-862).
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("compilation failed"),
        "the rejection must report a CEL compilation failure; got {message:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should reject mutating webhook
/// configurations with invalid match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:849
///   ("should reject mutating webhook configurations with invalid match conditions")
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_reject_mutating_webhook_configurations_with_invalid_match_conditions() {
    let (_mem, router) = spawn_router();

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mwc-invalid-mc"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(vec![MatchCondition {
                name: "invalid-expression-1".into(),
                // Upstream's exact malformed CEL (webhook.go:854-857). This
                // fixture used to send an empty *name* with a valid
                // expression, which fails required-field validation and never
                // reaches the CEL compiler at all.
                expression: "... [] bad expression".into(),
            }]),
            ..mutating(
                "invalid.mwc.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert!(
        status.is_client_error(),
        "create with invalid CEL must be 4xx; got {status} {body}"
    );
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("compilation failed"),
        "the rejection must report a CEL compilation failure; got {message:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate everything except
/// 'skip-me' configmaps [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:874
///   ("should mutate everything except 'skip-me' configmaps")
/// Mirror audit (#1749, 2026-08-25): re-derived from the upstream body.
///
/// Upstream excludes the object by a **matchConditions CEL expression over its
/// name** — `object.metadata.name != 'skip-me'` (webhook.go:875-880) — which
/// is what the case is named for ("mutating webhook excluding object with
/// specific name"). The mirror used an `objectSelector` on labels instead: the
/// same outcome reached by a different mechanism, so the CEL match-condition
/// path this case exists to cover was never exercised. It also checked for the
/// presence of a marker label rather than upstream's exact `data` maps.
/// Sonobuoy (Round 160): PASS
///
/// Verifies an `objectSelector` based on labels excludes specific objects
/// from the mutating webhook. `webhook_matches()` evaluates the selector
/// before dispatching; an object missing the `skip` label must be mutated.
#[tokio::test]
async fn should_mutate_everything_except_skip_me_configmaps() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) =
        start_mutator_data_stage("mutation-start".into(), "mutation-stage-1".into()).await;

    // Upstream's match condition is a CEL expression over the object's *name*
    // (webhook.go:875-880), not a label selector:
    //     name: "skip-me", expression: "object.metadata.name != 'skip-me'"
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("skip-me"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(vec![MatchCondition {
                name: "skip-me".into(),
                expression: "object.metadata.name != 'skip-me'".into(),
            }]),
            ..mutating("adding-configmap-data.k8s.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "skip-me"),
        &cfg,
    )
    .await
    .unwrap();

    let create = |name: &'static str| {
        let manager = &manager;
        async move {
            let (_resp, mutated) = manager
                .run_mutating_webhooks(
                    &Operation::Create,
                    &GroupVersionKind {
                        group: "".into(),
                        version: "v1".into(),
                        kind: "ConfigMap".into(),
                    },
                    &GroupVersionResource {
                        group: "".into(),
                        version: "v1".into(),
                        resource: "configmaps".into(),
                    },
                    Some("default"),
                    name,
                    Some(json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": name, "namespace": "default"},
                        "data": {"mutation-start": "yes"}
                    })),
                    None,
                    &admin_user_info(),
                )
                .await
                .unwrap();
            mutated.expect("object must come back")
        }
    };

    // A configmap with any other name is mutated: upstream expects exactly
    // {mutation-start, mutation-stage-1} (webhook.go:917-921).
    let mutated = create("some-random-name").await;
    assert_eq!(
        mutated["data"],
        json!({"mutation-start": "yes", "mutation-stage-1": "yes"}),
        "a non-'skip-me' configmap must be mutated: {mutated}"
    );

    // The one named `skip-me` is excluded by the match condition and keeps
    // exactly its original data (webhook.go:925-930).
    let skipped = create("skip-me").await;
    assert_eq!(
        skipped["data"],
        json!({"mutation-start": "yes"}),
        "the 'skip-me' configmap must be left untouched: {skipped}"
    );
}

/// [sig-api-machinery] AdmissionWebhook — a validating webhook scoped to the
/// DELETE operation on a CORE resource must be invoked when that resource is
/// deleted through the HTTP DELETE handler, and its denial must turn into a
/// 403 with the upstream "admission webhook denied the request" prefix.
///
/// This exercises the api-server's *handler wiring* (not just the manager):
/// before this change, only custom resources ran webhooks on DELETE, so core
/// resources (configmaps/pods/secrets) silently skipped DELETE admission.
///
/// Upstream parity: webhook.go's "deny creation/deletion" specs configure rules
/// with both CREATE and DELETE; the DELETE AdmissionReview sets `object=nil`
/// and `oldObject=<resource>`. The mock here asserts exactly that shape.
#[tokio::test]
async fn should_deny_configmap_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("nope, keep it".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cm-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "configmaps")],
            ..validating(
                "deny.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-cm-delete"),
        &cfg,
    )
    .await
    .unwrap();

    // Seed a configmap directly in storage.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "protected", "namespace": "default"},
        "data": {"k": "v"},
    });
    let cm_typed: rusternetes_common::resources::ConfigMap = serde_json::from_value(cm).unwrap();
    mem.create(
        &build_key("configmaps", Some("default"), "protected"),
        &cm_typed,
    )
    .await
    .unwrap();

    let (status, body) =
        delete_json(router, "/api/v1/namespaces/default/configmaps/protected").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request") && msg.contains("nope, keep it"),
        "deny message should carry the webhook reason: {body}"
    );

    // The configmap must still exist (deletion was rejected pre-storage).
    let still: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "protected"))
        .await
        .expect("configmap must survive a denied delete");
    assert_eq!(still.metadata.name, "protected");
}

/// Sibling of the above for pods, going through `delete_pod`. A DELETE-scoped
/// webhook must reject the pod deletion with 403; the pod stays in storage and
/// never gets a `deletionTimestamp`.
#[tokio::test]
async fn should_deny_pod_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("pod is load-bearing".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "pods")],
            ..validating(
                "deny.pod.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-pod-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "keepme", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let pod_typed: rusternetes_common::resources::Pod = serde_json::from_value(pod).unwrap();
    mem.create(&build_key("pods", Some("default"), "keepme"), &pod_typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/default/pods/keepme").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "pod delete must be denied: {body}"
    );

    // The pod must still exist with no deletionTimestamp (delete was blocked).
    let still: rusternetes_common::resources::Pod = mem
        .get(&build_key("pods", Some("default"), "keepme"))
        .await
        .expect("pod must survive a denied delete");
    assert!(
        still.metadata.deletion_timestamp.is_none(),
        "denied delete must not set deletionTimestamp"
    );
}

/// Control case: with a DELETE webhook configured but scoped to a *different*
/// resource, deleting a configmap is unaffected and succeeds. Guards against an
/// over-broad matcher that would invoke the webhook for every resource.
#[tokio::test]
async fn delete_webhook_scoped_to_other_resource_does_not_block() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("should not fire".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-secret-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            // Scoped to secrets, NOT configmaps.
            rules: vec![delete_rule_for("", "v1", "secrets")],
            ..validating(
                "deny.secret.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-secret-delete",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "free", "namespace": "default"},
        "data": {},
    });
    let cm_typed: rusternetes_common::resources::ConfigMap = serde_json::from_value(cm).unwrap();
    mem.create(&build_key("configmaps", Some("default"), "free"), &cm_typed)
        .await
        .unwrap();

    let status = delete_status(router, "/api/v1/namespaces/default/configmaps/free").await;
    assert!(
        status.is_success(),
        "configmap delete must succeed when webhook targets a different resource, got {status}"
    );
}

// ===========================================================================
// Convenience harness self-checks. These confirm the in-file helpers behave
// the way every test above relies on. Not Ginkgo mirrors, hence private
// names and no docstrings beyond the comment.
// ===========================================================================

#[tokio::test]
async fn harness_request_builder_compiles_an_admission_request() {
    let req = admission_request(
        "",
        "v1",
        "ConfigMap",
        "configmaps",
        Some("default"),
        "cm",
        json!({"metadata": {"name": "cm"}}),
    );
    assert_eq!(req.uid, "uid-cm");
    assert_eq!(req.namespace.as_deref(), Some("default"));
    assert!(matches!(req.operation, Operation::Create));
}

// ===========================================================================
// DELETE admission webhook wiring — extended resource coverage (PR #901).
// These mirror the configmap/pod/secret delete-deny tests above but exercise
// the newly wired handlers: deployments (apps/v1), services (core v1),
// jobs (batch/v1), and namespaces (cluster-scoped core v1).
// ===========================================================================

/// A validating webhook scoped to DELETE on apps/v1 deployments must be
/// invoked by `delete_deployment` and its denial must produce 403.
#[tokio::test]
async fn should_deny_deployment_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("no deployers allowed".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-deploy-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("apps", "v1", "deployments")],
            ..validating(
                "deny.deploy.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-deploy-delete",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "my-deploy", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let typed: rusternetes_common::resources::Deployment = serde_json::from_value(deploy).unwrap();
    mem.create(
        &build_key("deployments", Some("default"), "my-deploy"),
        &typed,
    )
    .await
    .unwrap();

    let (status, body) = delete_json(
        router,
        "/apis/apps/v1/namespaces/default/deployments/my-deploy",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "deployment delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("no deployers allowed"),
        "deny message should carry the webhook reason: {body}"
    );

    // Deployment must still exist.
    let _: rusternetes_common::resources::Deployment = mem
        .get(&build_key("deployments", Some("default"), "my-deploy"))
        .await
        .expect("deployment must survive a denied delete");
}

/// A validating webhook scoped to DELETE on core/v1 services must be invoked
/// by `delete_service` and its denial must produce 403.
#[tokio::test]
async fn should_deny_service_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("service is precious".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-svc-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "services")],
            ..validating(
                "deny.svc.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-svc-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {"selector": {"app": "x"}, "ports": [{"port": 80}]}
    });
    let typed: rusternetes_common::resources::Service = serde_json::from_value(svc).unwrap();
    mem.create(&build_key("services", Some("default"), "my-svc"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/default/services/my-svc").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "service delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request") && msg.contains("service is precious"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Service = mem
        .get(&build_key("services", Some("default"), "my-svc"))
        .await
        .expect("service must survive a denied delete");
}

/// A validating webhook scoped to DELETE on batch/v1 jobs must be invoked
/// by `delete_job` and its denial must produce 403.
#[tokio::test]
async fn should_deny_job_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("job is still running".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-job-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("batch", "v1", "jobs")],
            ..validating(
                "deny.job.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-job-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "my-job", "namespace": "default"},
        "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}}}
    });
    let typed: rusternetes_common::resources::Job = serde_json::from_value(job).unwrap();
    mem.create(&build_key("jobs", Some("default"), "my-job"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/apis/batch/v1/namespaces/default/jobs/my-job").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "job delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("job is still running"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Job = mem
        .get(&build_key("jobs", Some("default"), "my-job"))
        .await
        .expect("job must survive a denied delete");
}

/// A validating webhook scoped to DELETE on cluster-scoped namespaces must
/// be invoked by `delete_ns` and its denial must produce 403.
#[tokio::test]
async fn should_deny_namespace_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("namespace is protected".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-ns-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "namespaces")],
            ..validating(
                "deny.ns.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-ns-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "my-ns"},
    });
    let typed: rusternetes_common::resources::Namespace = serde_json::from_value(ns).unwrap();
    mem.create(&build_key("namespaces", None, "my-ns"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/my-ns").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "namespace delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("namespace is protected"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Namespace = mem
        .get(&build_key("namespaces", None, "my-ns"))
        .await
        .expect("namespace must survive a denied delete");
}

/// Control: a DELETE webhook scoped to a *different* resource (secrets) must
/// NOT fire when deleting a deployment. The deployment delete must succeed.
#[tokio::test]
async fn delete_webhook_scoped_to_secrets_does_not_block_deployment_delete() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("should not fire".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-secret-not-deploy"),
        webhooks: Some(vec![ValidatingWebhook {
            // Scoped to secrets, NOT deployments.
            rules: vec![delete_rule_for("", "v1", "secrets")],
            ..validating(
                "deny.secret.not.deploy.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-secret-not-deploy",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "free-deploy", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let typed: rusternetes_common::resources::Deployment = serde_json::from_value(deploy).unwrap();
    mem.create(
        &build_key("deployments", Some("default"), "free-deploy"),
        &typed,
    )
    .await
    .unwrap();

    let status = delete_status(
        router,
        "/apis/apps/v1/namespaces/default/deployments/free-deploy",
    )
    .await;
    assert!(
        status.is_success(),
        "deployment delete must succeed when webhook targets secrets, got {status}"
    );
}

// ---------------------------------------------------------------------------
// CREATE-path invocation through the real HTTP handler.
//
// The pre-existing `should_mutate_configmap` / `should_be_able_to_deny_*`
// tests invoke `AdmissionWebhookManager::run_*_webhooks(...)` DIRECTLY — they
// never exercise the configmap/pod POST handler. That left a gap: live
// conformance showed core ConfigMaps/Pods coming back UNMUTATED even though
// the manager unit tests were green, because nothing verified that the
// `create` handler (a) calls the webhook and (b) PERSISTS the mutated object.
//
// These tests drive a real `POST /api/v1/namespaces/.../{configmaps,pods}`
// through `build_router`, then assert on both the HTTP response body AND the
// stored object. They are the regression guard for "core resources are not
// actually mutated/denied" (k8s webhook.go:226/240, deny pod+configmap).
// ---------------------------------------------------------------------------

/// [sig-api-machinery] AdmissionWebhook should mutate configmap [Conformance]
/// — but driven through the real HTTP create handler, asserting the mutation
/// is applied BEFORE persistence (response body + stored object both carry the
/// injected label).
#[tokio::test]
async fn mutating_webhook_mutates_configmap_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("mutated-by-webhook".into(), "yes".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cm-http"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..mutating(
                "mutate.cm.http.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cm-http"),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm-http", "namespace": "default", "labels": {}},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(status, StatusCode::CREATED, "create must succeed: {body}");
    assert_eq!(
        body["metadata"]["labels"]["mutated-by-webhook"],
        json!("yes"),
        "response body must carry the webhook-injected label: {body}"
    );

    // Crucially: the PERSISTED object must also carry the mutation.
    let stored: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "cm-http"))
        .await
        .expect("configmap must be stored");
    assert_eq!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("mutated-by-webhook"))
            .map(String::as_str),
        Some("yes"),
        "stored configmap must carry the webhook mutation, not just the response"
    );
}

/// Mutation must reach a Pod through the real HTTP create handler too
/// (k8s webhook.go:240 "should mutate pod and apply defaults after mutation").
#[tokio::test]
async fn mutating_webhook_mutates_pod_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("mutated-by-webhook".into(), "yes".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-pod-http"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..mutating(
                "mutate.pod.http.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-pod-http"),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-http", "namespace": "default", "labels": {}},
        "spec": {"containers": [{"name": "main", "image": "nginx"}]},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/pods", &pod).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed: {body}"
    );
    assert_eq!(
        body["metadata"]["labels"]["mutated-by-webhook"],
        json!("yes"),
        "pod response must carry the webhook-injected label: {body}"
    );

    let stored: rusternetes_common::resources::Pod = mem
        .get(&build_key("pods", Some("default"), "pod-http"))
        .await
        .expect("pod must be stored");
    assert_eq!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("mutated-by-webhook"))
            .map(String::as_str),
        Some("yes"),
        "stored pod must carry the webhook mutation"
    );
}

/// A validating webhook denial on ConfigMap CREATE must return 403 from the
/// real HTTP handler and must NOT persist the object
/// (k8s webhook.go: "should be able to deny pod and configmap creation").
#[tokio::test]
async fn validating_webhook_denies_configmap_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_deny_validator("configmap rejected by webhook".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cm-create-http"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..validating(
                "deny.cm.create.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-cm-create-http",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "denied-cm", "namespace": "default"},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "create must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("configmap rejected by webhook"),
        "deny message must carry the webhook reason: {body}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::ConfigMap>(&build_key(
            "configmaps",
            Some("default"),
            "denied-cm"
        ))
        .await
        .is_err(),
        "denied configmap must NOT be persisted"
    );
}

/// Sibling deny test for Pod CREATE through the real HTTP handler.
#[tokio::test]
async fn validating_webhook_denies_pod_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_deny_validator("pod rejected by webhook".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-create-http"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..validating(
                "deny.pod.create.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-pod-create-http",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "denied-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/pods", &pod).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "pod create must be denied by webhook: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("pod rejected by webhook"),
        "deny message must carry the webhook reason: {body}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::Pod>(&build_key(
            "pods",
            Some("default"),
            "denied-pod"
        ))
        .await
        .is_err(),
        "denied pod must NOT be persisted"
    );
}

/// failurePolicy=Fail: when the validating webhook backend is unreachable, a
/// ConfigMap CREATE must be REJECTED (fail-closed), not silently admitted.
/// This is the regression for "unconditionally reject operations on fail
/// closed webhook" on a core resource going through the HTTP handler.
#[tokio::test]
async fn fail_closed_validating_webhook_rejects_configmap_create_when_unreachable() {
    let (mem, router) = spawn_router();

    // Point at a closed port so the call errors; failurePolicy=Fail => reject.
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("fail-closed-cm"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..validating(
                "fail.closed.cm.io",
                "https://127.0.0.1:1/admit".to_string(),
                vec![],
                Some(FailurePolicy::Fail),
                Some(2),
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "fail-closed-cm"),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "fail-closed-cm-obj", "namespace": "default"},
        "data": {"k": "v"},
    });
    let (status, _body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert!(
        status.is_client_error() || status.is_server_error(),
        "fail-closed webhook with unreachable backend must block the create, got {status}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::ConfigMap>(&build_key(
            "configmaps",
            Some("default"),
            "fail-closed-cm-obj"
        ))
        .await
        .is_err(),
        "configmap must NOT be persisted when a fail-closed webhook is unreachable"
    );
}

/// objectSelector gating: a mutating webhook scoped by objectSelector must NOT
/// fire for an object whose labels don't match, so the ConfigMap is created
/// UNMUTATED (k8s "should mutate everything except 'skip-me' configmaps"). The
/// no-call path is what lets `skip-me` configmaps pass through unchanged.
#[tokio::test]
async fn object_selector_skips_webhook_for_nonmatching_configmap() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("should-not-appear".into(), "yes".into()).await;

    let mut webhook = MutatingWebhook {
        rules: vec![rule_for("", "v1", "configmaps")],
        ..mutating("skip.cm.io", url, vec![], Some(FailurePolicy::Fail), None)
    };
    // Only fire for configmaps labelled mutate=please.
    webhook.object_selector = Some(rusternetes_common::resources::LabelSelector {
        match_labels: Some(
            [("mutate".to_string(), "please".to_string())]
                .into_iter()
                .collect(),
        ),
        match_expressions: None,
    });

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("skip-cm"),
        webhooks: Some(vec![webhook]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "skip-cm"),
        &cfg,
    )
    .await
    .unwrap();

    // This configmap does NOT carry the mutate=please label → webhook skipped.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "skip-me", "namespace": "default", "labels": {"other": "x"}},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(status, StatusCode::CREATED, "create must succeed: {body}");
    assert!(
        body["metadata"]["labels"]["should-not-appear"].is_null(),
        "webhook must be skipped for non-matching objectSelector: {body}"
    );
    let stored: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "skip-me"))
        .await
        .expect("configmap must be stored");
    assert!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("should-not-appear"))
            .is_none(),
        "stored configmap must be unmutated when objectSelector does not match"
    );
}
