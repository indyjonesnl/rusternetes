//! `--authorization-config` hot reload and authorizer metrics (#3315): the
//! file is watched, a changed file rebuilds the Webhook authorizers only and
//! swaps them in atomically, a bad file keeps the last good configuration
//! (`checkFile`, pkg/kubeapiserver/authorizer/reload.go:226-274).

use axum::{extract::State, routing::post, Json, Router};
use rusternetes_api_server::authorizer::build_authorizer_with_config;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_storage::MemoryStorage;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn review(State(allow): State<bool>, Json(body): Json<Value>) -> Json<Value> {
    Json(json!({
        "apiVersion": body["apiVersion"],
        "kind": "SubjectAccessReview",
        "status": {"allowed": allow},
    }))
}

async fn serve(allow: bool) -> String {
    let app = Router::new()
        .route("/testserver", post(review))
        .with_state(allow);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}/testserver")
}

fn dir() -> std::path::PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = std::env::temp_dir().join(format!(
        "authz-reload-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn kubeconfig(d: &std::path::Path, tag: &str, server: &str) -> String {
    let p = d.join(format!("{tag}.kubeconfig"));
    std::fs::write(
        &p,
        format!(
            "clusters:\n- name: remote\n  cluster:\n    server: {server}\nusers:\n- name: apiserver\n  user:\n    token: sekret\ncurrent-context: c\ncontexts:\n- name: c\n  context:\n    cluster: remote\n    user: apiserver\n"
        ),
    )
    .unwrap();
    p.to_string_lossy().into_owned()
}

fn webhook(kc: &str) -> String {
    format!(
        "- type: Webhook\n  name: remote\n  webhook:\n    timeout: 5s\n    subjectAccessReviewVersion: v1\n    failurePolicy: Deny\n    cacheAuthorizedRequests: false\n    cacheUnauthorizedRequests: false\n    connectionInfo:\n      type: KubeConfigFile\n      kubeConfigFile: {kc}\n"
    )
}

const HEAD: &str =
    "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n";

fn write(path: &std::path::Path, body: &str) {
    // Atomic replace, as a ConfigMap/secret volume update does.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{HEAD}{body}")).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn build(cfg: &std::path::Path) -> Arc<dyn Authorizer> {
    build_authorizer_with_config(
        Arc::new(MemoryStorage::new()),
        false,
        &[],
        "",
        &Default::default(),
        &cfg.to_string_lossy(),
        false,
    )
    .unwrap()
}

async fn allowed(a: &Arc<dyn Authorizer>) -> bool {
    let attrs = RequestAttributes::new(
        UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        },
        "get",
        "pods",
    )
    .with_namespace("default");
    matches!(a.authorize(&attrs).await.unwrap(), Decision::Allow)
}

async fn eventually(a: &Arc<dyn Authorizer>, want: bool) -> bool {
    for _ in 0..100 {
        if allowed(a).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

fn counter(name: &str, label: &str, value: &str) -> f64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == name)
        .flat_map(|f| f.get_metric().iter())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == label && l.value() == value)
        })
        .map(|m| m.get_counter().value())
        .sum()
}

async fn counter_reaches(name: &str, label: &str, value: &str, want: f64) -> bool {
    for _ in 0..100 {
        if counter(name, label, value) >= want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

const RELOADS: &str = "apiserver_authorization_config_controller_automatic_reloads_total";

#[tokio::test]
async fn changed_webhook_is_swapped_in() {
    let d = dir();
    let deny = serve(false).await;
    let allow = serve(true).await;
    let cfg = d.join("authz.yaml");
    write(&cfg, &webhook(&kubeconfig(&d, "a", &deny)));
    let a = build(&cfg);
    assert!(!allowed(&a).await);
    let before = counter(RELOADS, "status", "success");
    write(&cfg, &webhook(&kubeconfig(&d, "b", &allow)));
    assert!(eventually(&a, true).await, "new webhook never took effect");
    assert!(counter(RELOADS, "status", "success") >= before + 1.0);
}

#[tokio::test]
async fn invalid_file_keeps_the_last_good_config() {
    let d = dir();
    let allow = serve(true).await;
    let cfg = d.join("authz.yaml");
    write(&cfg, &webhook(&kubeconfig(&d, "a", &allow)));
    let a = build(&cfg);
    assert!(allowed(&a).await);
    let before = counter(RELOADS, "status", "failure");
    std::fs::write(&cfg, "not: [valid").unwrap();
    assert!(counter_reaches(RELOADS, "status", "failure", before + 1.0).await);
    assert!(allowed(&a).await, "last good config must stay in force");
}

// requireNonWebhookTypes (config.go:142-144): every non-Webhook type given at
// startup must remain, so dropping AlwaysDeny is refused and the old chain
// (which allows via the webhook) stays.
#[tokio::test]
async fn removing_a_non_webhook_type_is_refused() {
    let d = dir();
    let allow = serve(true).await;
    let deny = serve(false).await;
    let cfg = d.join("authz.yaml");
    let deny_all = "- type: AlwaysDeny\n  name: alwaysdeny\n";
    write(
        &cfg,
        &format!("{}{deny_all}", webhook(&kubeconfig(&d, "a", &allow))),
    );
    let a = build(&cfg);
    assert!(allowed(&a).await);
    let before = counter(RELOADS, "status", "failure");
    write(&cfg, &webhook(&kubeconfig(&d, "b", &deny)));
    assert!(counter_reaches(RELOADS, "status", "failure", before + 1.0).await);
    assert!(allowed(&a).await);
}

// newForConfig (reload.go:107-110 and siblings): a type that was not enabled
// at startup cannot be added by a reload.
#[tokio::test]
async fn adding_a_type_not_enabled_at_startup_is_refused() {
    let d = dir();
    let allow = serve(true).await;
    let cfg = d.join("authz.yaml");
    write(&cfg, &webhook(&kubeconfig(&d, "a", &allow)));
    let a = build(&cfg);
    assert!(allowed(&a).await);
    let before = counter(RELOADS, "status", "failure");
    let rbac = "- type: RBAC\n  name: rbac\n";
    write(
        &cfg,
        &format!("{}{rbac}", webhook(&kubeconfig(&d, "b", &allow))),
    );
    assert!(counter_reaches(RELOADS, "status", "failure", before + 1.0).await);
}

// authorizationmetrics.InstrumentedAuthorizer (metrics.go:57-86).
#[tokio::test]
async fn terminal_decisions_are_counted_by_type_name_decision() {
    let d = dir();
    let cfg = d.join("authz.yaml");
    write(&cfg, "- type: AlwaysAllow\n  name: alwaysallow\n");
    let a = build(&cfg);
    let name = "apiserver_authorization_decisions_total";
    let before = counter(name, "name", "alwaysallow");
    assert!(allowed(&a).await);
    assert!(counter(name, "name", "alwaysallow") >= before + 1.0);
}
