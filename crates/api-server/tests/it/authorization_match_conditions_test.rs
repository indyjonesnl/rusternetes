//! Authorization webhook `matchConditions` (#3161).
//!
//! Ported from `webhook_v1_test.go` `TestV1WebhookMatchConditions`
//! (plugin/pkg/authorizer/webhook), `validation.go` `compileMatchConditions`
//! :778-820 and `authorization/cel/matcher.go` `Eval`.

use axum::{extract::State, routing::post, Json, Router};
use rusternetes_api_server::authorization_match_conditions::{CelMatcher, WebhookMatchCondition};
use rusternetes_api_server::authorization_webhook::{
    Backoff, FailurePolicy, WebhookAuthorizer, WebhookClientConfig,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Opinion, RequestAttributes};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn review(State(n): State<Arc<AtomicUsize>>, Json(body): Json<Value>) -> Json<Value> {
    n.fetch_add(1, Ordering::SeqCst);
    Json(json!({
        "apiVersion": body["apiVersion"],
        "kind": "SubjectAccessReview",
        "status": {"allowed": true},
    }))
}

async fn serve() -> (String, Arc<AtomicUsize>) {
    let n = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/testserver", post(review))
        .with_state(n.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/testserver", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (url, n)
}

fn authorizer(url: &str, policy: FailurePolicy, exprs: &[&str]) -> WebhookAuthorizer {
    let path = std::env::temp_dir().join(format!(
        "authz-mc-{}-{}.kubeconfig",
        std::process::id(),
        url.rsplit(':').next().unwrap().replace('/', "_")
    ));
    std::fs::write(
        &path,
        format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: {url}\nusers:\n- name: u\n  user: {{}}\ncontexts:\n- name: x\n  context: {{cluster: c, user: u}}\ncurrent-context: x\n"
        ),
    )
    .unwrap();
    let cfg = WebhookClientConfig::load(&path).unwrap();
    let backoff = Backoff {
        duration: Duration::ZERO,
        factor: 0.0,
        jitter: 0.0,
        steps: 1,
    };
    WebhookAuthorizer::new(cfg, "v1", Duration::ZERO, Duration::ZERO, backoff, policy)
        .unwrap()
        .with_match_conditions(&exprs.iter().map(|e| mc(e)).collect::<Vec<_>>())
        .unwrap()
}

fn alice() -> RequestAttributes {
    let mut extra = std::collections::HashMap::new();
    extra.insert("key1".to_string(), vec!["a".to_string(), "b".to_string()]);
    RequestAttributes {
        namespace: Some("kittensandponies".into()),
        ..RequestAttributes::new(
            UserInfo {
                username: "alice".into(),
                uid: "1".into(),
                groups: vec!["group1".into(), "group2".into()],
                extra,
            },
            "get",
            "pods",
        )
    }
}

fn mc(e: &str) -> WebhookMatchCondition {
    WebhookMatchCondition {
        expression: e.into(),
    }
}

// "match all against all expressions" (TestV1WebhookMatchConditions).
#[tokio::test]
async fn all_true_conditions_call_the_webhook() {
    let (url, n) = serve().await;
    let a = authorizer(
        &url,
        FailurePolicy::NoOpinion,
        &[
            "request.user == 'alice'",
            "request.uid == '1'",
            "('group1' in request.groups)",
            "has(request.resourceAttributes) && request.resourceAttributes.namespace == 'kittensandponies'",
        ],
    );
    let o = a.authorize_opinion(&alice()).await.unwrap();
    assert!(matches!(o, Opinion::Allow), "{o:?}");
    assert_eq!(n.load(Ordering::SeqCst), 1);
}

// webhook.go:213-216: a FALSE condition skips the webhook -> NoOpinion, no error.
#[tokio::test]
async fn false_condition_skips_webhook_with_no_opinion() {
    let (url, n) = serve().await;
    let a = authorizer(
        &url,
        FailurePolicy::Deny,
        &["request.user == 'alice'", "request.user == 'bob'"],
    );
    let o = a.authorize_opinion(&alice()).await.unwrap();
    assert!(matches!(o, Opinion::NoOpinion { error: None, .. }), "{o:?}");
    assert_eq!(n.load(Ordering::SeqCst), 0);
}

// webhook.go:208-212: an eval error with no FALSE -> decisionOnError.
#[tokio::test]
async fn eval_error_follows_failure_policy() {
    let (url, n) = serve().await;
    // Resource request: request.nonResourceAttributes is absent.
    let expr = "request.nonResourceAttributes.path == '/x'";
    let deny = authorizer(&url, FailurePolicy::Deny, &[expr]);
    assert!(matches!(
        deny.authorize_opinion(&alice()).await.unwrap(),
        Opinion::Deny(_)
    ));
    let no = authorizer(&url, FailurePolicy::NoOpinion, &[expr]);
    assert!(matches!(
        no.authorize_opinion(&alice()).await.unwrap(),
        Opinion::NoOpinion { error: Some(_), .. }
    ));
    assert_eq!(n.load(Ordering::SeqCst), 0);
}

// matcher.go: a FALSE after an erroring expression still wins ("return early").
#[test]
fn false_wins_over_earlier_error() {
    let m = CelMatcher::compile(&[
        mc("request.nonResourceAttributes.path == '/x'"),
        mc("request.user == 'bob'"),
    ])
    .unwrap()
    .unwrap();
    let req = json!({"user": "alice", "groups": [], "uid": "", "extra": {}});
    assert_eq!(m.eval(&req), Ok(false));
}

// compileMatchConditions :778-820.
#[test]
fn compile_validation() {
    assert!(CelMatcher::compile(&[]).unwrap().is_none());
    let e = CelMatcher::compile(&[mc("  ")]).unwrap_err();
    assert!(e[0].contains("Required"), "{e:?}");
    let e =
        CelMatcher::compile(&[mc("request.user == 'a'"), mc("request.user == 'a'")]).unwrap_err();
    assert!(e[0].contains("Duplicate"), "{e:?}");
    let many: Vec<_> = (0..65)
        .map(|i| mc(&format!("request.uid == '{i}'")))
        .collect();
    let e = CelMatcher::compile(&many).unwrap_err();
    assert!(e[0].contains("Too many"), "{e:?}");
    let e = CelMatcher::compile(&[mc("request.user ==")]).unwrap_err();
    assert!(e[0].contains("compilation failed"), "{e:?}");
}
