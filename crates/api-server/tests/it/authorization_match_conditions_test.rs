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

// compile.go:189-203 (`MatchCondition.ReturnTypes` is `[bool]`): a non-bool
// expression is rejected at compile time, not at evaluation (#3265).
#[test]
fn non_bool_expression_is_a_compile_error() {
    let e = CelMatcher::compile(&[mc("6")]).unwrap_err();
    assert_eq!(
        e,
        vec![
            r#"matchConditions[0].expression: Invalid value: "6": must evaluate to bool but got int"#
        ],
        "{e:?}"
    );
}

// compile_test.go `TestCompileCELExpression`: the typed environment
// (`mustBuildEnv`, compile.go:173-259) rejects undeclared references and
// undefined fields at compile time.
#[test]
fn compile_against_the_typed_subject_access_review_environment() {
    let cases: &[(&str, &str)] = &[
        ("request.user == 'bob'", ""),
        ("request.time == 'now'", "undefined field 'time'"),
        ("request++'", "Syntax error"),
        ("request.user", "must evaluate to bool"),
        ("x.user", "undeclared reference to 'x'"),
        (
            "request.resourceAttributes.fieldSelector.rawSelector == 'foo'",
            "",
        ),
        (
            "request.resourceAttributes.fieldSelector.requirements.exists(r, r.key == 'foo' && r.operator == 'In' && ('bar' in r.values))",
            "",
        ),
        (
            "request.resourceAttributes.labelSelector.rawSelector == 'foo'",
            "",
        ),
        (
            "request.resourceAttributes.labelSelector.requirements.exists(r, r.key == 'foo' && r.operator == 'In' && ('bar' in r.values))",
            "",
        ),
        (
            "request.resourceAttributes.bogus == 'x'",
            "undefined field 'bogus'",
        ),
        (
            "request.nonResourceAttributes.bogus == 'x'",
            "undefined field 'bogus'",
        ),
        (
            "request.resourceAttributes.fieldSelector.bogus == 'x'",
            "undefined field 'bogus'",
        ),
        ("has(request.bogus)", "undefined field 'bogus'"),
        ("request.?bogus.orValue('') == 'x'", "undefined field 'bogus'"),
    ];
    for (expr, want) in cases {
        let got = CelMatcher::compile(&[mc(expr)]);
        if want.is_empty() {
            assert!(got.is_ok(), "{expr}: {got:?}");
        } else {
            let errs = got.expect_err(expr).join("\n");
            assert!(errs.contains(want), "{expr}: {errs}");
        }
    }
}

// compile.go:128-159: selector usage is detected for `.x` and `.?x`.
#[test]
fn compile_records_selector_usage() {
    let m = |e: &str| CelMatcher::compile(&[mc(e)]).unwrap().unwrap();
    let none = m("request.user == 'a'");
    assert!(!none.uses_field_selector() && !none.uses_label_selector());
    let f = m("request.resourceAttributes.fieldSelector.rawSelector == 'foo'");
    assert!(f.uses_field_selector() && !f.uses_label_selector());
    let l = m("request.resourceAttributes.?labelSelector.hasValue()");
    assert!(!l.uses_field_selector() && l.uses_label_selector());
    // matcher.go:33-36: the matcher records the OR across expressions.
    let both = CelMatcher::compile(&[
        mc("has(request.resourceAttributes.fieldSelector)"),
        mc("has(request.resourceAttributes.labelSelector)"),
    ])
    .unwrap()
    .unwrap();
    assert!(both.uses_field_selector() && both.uses_label_selector());
}

// matcher.go:60-62 + compile.go:293-325: the selectors reach the expression
// only through `convertObjectToUnstructured`, when an expression uses them.
#[tokio::test]
async fn selector_requirements_are_visible_to_match_conditions() {
    let (url, n) = serve().await;
    let a = authorizer(
        &url,
        FailurePolicy::Deny,
        &["request.resourceAttributes.fieldSelector.requirements.exists(r, r.key == 'metadata.name' && r.operator == 'In' && ('x' in r.values))"],
    );
    let mut attrs = alice();
    attrs.verb = "list".into();
    attrs.set_selectors("metadata.name=x", "");
    let o = a.authorize_opinion(&attrs).await.unwrap();
    assert!(matches!(o, Opinion::Allow), "{o:?}");
    assert_eq!(n.load(Ordering::SeqCst), 1);
}
