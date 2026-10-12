//! `--authorization-config` with Webhook authorizers (#3294): the file's
//! `timeout`, `failurePolicy`, `cache*Requests`, `matchConditions` and `name`
//! reach the webhook authorizer (`newForConfig`, reload.go:124-167).

use axum::{extract::State, routing::post, Json, Router};
use rusternetes_api_server::authorizer::build_authorizer_with_config;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_storage::MemoryStorage;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct Mock {
    allow: bool,
    called: Arc<AtomicUsize>,
}

async fn review(State(m): State<Mock>, Json(body): Json<Value>) -> Json<Value> {
    m.called.fetch_add(1, Ordering::SeqCst);
    Json(json!({
        "apiVersion": body["apiVersion"],
        "kind": "SubjectAccessReview",
        "status": {"allowed": m.allow},
    }))
}

async fn serve(allow: bool) -> (String, Arc<AtomicUsize>) {
    let called = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/testserver", post(review))
        .with_state(Mock {
            allow,
            called: called.clone(),
        });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (format!("http://{addr}/testserver"), called)
}

fn tmp(tag: &str, contents: &str) -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = std::env::temp_dir().join(format!(
        "authz-config-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().into_owned()
}

fn kubeconfig(server: &str) -> String {
    tmp(
        "kc",
        &format!(
            "clusters:\n- name: remote\n  cluster:\n    server: {server}\nusers:\n- name: apiserver\n  user:\n    token: sekret\ncurrent-context: c\ncontexts:\n- name: c\n  context:\n    cluster: remote\n    user: apiserver\n"
        ),
    )
}

/// One webhook authorizer; `extra` is more `webhook:` keys, `tail` more
/// authorizers.
fn config_text(kubeconfig: &str, policy: &str, extra: &str, tail: &str) -> String {
    format!(
        "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n  name: remote\n  webhook:\n    timeout: 5s\n    subjectAccessReviewVersion: v1\n    failurePolicy: {policy}\n    connectionInfo:\n      type: KubeConfigFile\n      kubeConfigFile: {kubeconfig}\n{extra}{tail}"
    )
}

fn config(kubeconfig: &str, extra: &str, tail: &str) -> String {
    tmp("cfg", &config_text(kubeconfig, "NoOpinion", extra, tail))
}

fn build(cfg: &str) -> anyhow::Result<Arc<dyn Authorizer>> {
    build_authorizer_with_config(
        Arc::new(MemoryStorage::new()),
        false,
        &[],
        "",
        &Default::default(),
        cfg,
        false,
    )
}

fn attrs(name: &str) -> RequestAttributes {
    RequestAttributes::new(
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        },
        "get",
        "pods",
    )
    .with_namespace("default")
}

async fn allowed(a: &Arc<dyn Authorizer>, name: &str) -> bool {
    matches!(a.authorize(&attrs(name)).await.unwrap(), Decision::Allow)
}

#[tokio::test]
async fn webhook_from_config_authorizes() {
    let (url, called) = serve(true).await;
    let a = build(&config(&kubeconfig(&url), "", "")).unwrap();
    assert!(allowed(&a, "alice").await);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    let (url, _) = serve(false).await;
    let a = build(&config(&kubeconfig(&url), "", "")).unwrap();
    assert!(!allowed(&a, "alice").await);
}

// SetDefaults_WebhookConfiguration: both caches on by default (5m / 30s).
#[tokio::test]
async fn responses_are_cached_unless_the_config_disables_it() {
    let (url, called) = serve(true).await;
    let a = build(&config(&kubeconfig(&url), "", "")).unwrap();
    assert!(allowed(&a, "alice").await);
    assert!(allowed(&a, "alice").await);
    assert_eq!(called.load(Ordering::SeqCst), 1);

    // reload.go:155-160: a disabled cache zeroes that TTL.
    let (url, called) = serve(true).await;
    let a = build(&config(
        &kubeconfig(&url),
        "    cacheAuthorizedRequests: false\n",
        "",
    ))
    .unwrap();
    assert!(allowed(&a, "alice").await);
    assert!(allowed(&a, "alice").await);
    assert_eq!(called.load(Ordering::SeqCst), 2);
}

// webhook.go:207-222: a FALSE matchCondition skips the webhook.
#[tokio::test]
async fn match_conditions_gate_the_webhook() {
    let (url, called) = serve(true).await;
    let a = build(&config(
        &kubeconfig(&url),
        "    matchConditionSubjectAccessReviewVersion: v1\n    matchConditions:\n    - expression: \"request.user == 'alice'\"\n",
        "",
    ))
    .unwrap();
    assert!(!allowed(&a, "bob").await);
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(allowed(&a, "alice").await);
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

// reload.go:136-143: failurePolicy is the decision when the webhook errors.
#[tokio::test]
async fn failure_policy_decides_when_the_webhook_is_unreachable() {
    let kc = kubeconfig("http://127.0.0.1:1/testserver");
    let allow_all = "- type: AlwaysAllow\n  name: alwaysallow\n";
    let a = build(&tmp("np", &config_text(&kc, "NoOpinion", "", allow_all))).unwrap();
    assert!(allowed(&a, "alice").await, "NoOpinion falls through");
    let a = build(&tmp("deny", &config_text(&kc, "Deny", "", allow_all))).unwrap();
    assert!(!allowed(&a, "alice").await, "Deny stops the chain");
}

// reload.go:129 dereferences KubeConfigFile, which InClusterConfig must not set.
#[test]
fn in_cluster_config_hits_upstreams_nil_dereference() {
    let cfg = tmp(
        "incluster",
        "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n  name: remote\n  webhook:\n    timeout: 5s\n    subjectAccessReviewVersion: v1\n    failurePolicy: NoOpinion\n    connectionInfo:\n      type: InClusterConfig\n",
    );
    let e = build(&cfg).err().unwrap().to_string();
    assert!(e.contains("nil pointer dereference"), "{e}");
}

#[test]
fn invalid_webhook_config_is_refused_with_field_paths() {
    let cfg = tmp(
        "bad",
        "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n  name: remote\n  webhook:\n    timeout: 60s\n    subjectAccessReviewVersion: v1\n    failurePolicy: NoOpinion\n    connectionInfo:\n      type: KubeConfigFile\n      kubeConfigFile: relative/kc\n",
    );
    let e = build(&cfg).err().unwrap().to_string();
    assert!(
        e.contains(r#"[authorizers[0].timeout: Invalid value: "1m0s": must be > 0s and <= 30s, authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "relative/kc": must be an absolute path]"#),
        "{e}"
    );
}
