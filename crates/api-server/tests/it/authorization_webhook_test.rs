//! `--authorization-mode=Webhook` (#2895).
//!
//! Ported from `staging/src/k8s.io/apiserver/plugin/pkg/authorizer/webhook/`
//! `webhook_v1_test.go` (`TestV1Webhook`, `TestV1WebhookCache`,
//! `TestV1NewFromConfig`), `webhook_v1beta1_test.go`, and from
//! `pkg/kubeapiserver/options/authorization_test.go` (`TestAuthzValidate`,
//! `TestAddFlags`). The upstream mock speaks TLS with client certs; these
//! tests use plain HTTP, the transport is not what they pin.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use rusternetes_api_server::authorization_webhook::{
    parse_go_duration, validate_webhook_args, AuthorizationWebhookArgs, Backoff, FailurePolicy,
    WebhookAuthorizer, WebhookClientConfig,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{
    Authorizer, Decision, Opinion, RequestAttributes, UnionAuthorizer,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Mock {
    allow: bool,
    denied: bool,
    reason: String,
    status_code: u16,
    retry_after: Option<String>,
    called: usize,
    last: Option<Value>,
}

type Shared = Arc<Mutex<Mock>>;

async fn review(State(m): State<Shared>, Json(body): Json<Value>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let mut m = m.lock().unwrap();
    m.called += 1;
    m.last = Some(body.clone());
    if !(200..300).contains(&m.status_code) {
        let mut resp = (StatusCode::from_u16(m.status_code).unwrap(), "HTTP Error").into_response();
        if let Some(ra) = &m.retry_after {
            resp.headers_mut()
                .insert("retry-after", ra.parse().unwrap());
        }
        return resp;
    }
    Json(json!({
        "apiVersion": body["apiVersion"],
        "kind": "SubjectAccessReview",
        "status": {"allowed": m.allow, "denied": m.denied, "reason": m.reason},
    }))
    .into_response()
}

/// Start the mock webhook; returns its URL (path `/testserver`, as upstream).
async fn serve(mock: Shared) -> String {
    let app = Router::new()
        .route("/testserver", post(review))
        .with_state(mock);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}/testserver")
}

fn kubeconfig_file(server: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = std::env::temp_dir().join(format!(
        "authz-webhook-{}-{}.yaml",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(
        &p,
        format!(
            "clusters:\n- name: remote\n  cluster:\n    server: {server}\nusers:\n- name: apiserver\n  user:\n    token: sekret\ncurrent-context: c\ncontexts:\n- name: c\n  context:\n    cluster: remote\n    user: apiserver\n"
        ),
    )
    .unwrap();
    p
}

/// `testRetryBackoff = wait.Backoff{Steps: 5}` (zero sleep).
fn test_backoff() -> Backoff {
    Backoff {
        duration: Duration::ZERO,
        factor: 0.0,
        jitter: 0.0,
        steps: 5,
    }
}

fn authorizer(url: &str, version: &str, ttl: Duration, policy: FailurePolicy) -> WebhookAuthorizer {
    let cfg = WebhookClientConfig::load(&kubeconfig_file(url)).unwrap();
    WebhookAuthorizer::new(cfg, version, ttl, ttl, test_backoff(), policy).unwrap()
}

fn user(name: &str) -> UserInfo {
    UserInfo {
        username: name.to_string(),
        uid: String::new(),
        groups: vec![],
        extra: Default::default(),
    }
}

fn resource(name: &str) -> RequestAttributes {
    RequestAttributes {
        namespace: Some("kittensandponies".into()),
        ..RequestAttributes::new(user(name), "get", "pods")
    }
}

fn is_allow(o: &Opinion) -> bool {
    matches!(o, Opinion::Allow)
}

fn allowing_mock() -> Shared {
    Arc::new(Mutex::new(Mock {
        allow: true,
        status_code: 200,
        ..Default::default()
    }))
}

// TestV1Webhook: the SubjectAccessReview we send.
#[tokio::test]
async fn sends_a_v1_subject_access_review() {
    let mock = allowing_mock();
    let url = serve(mock.clone()).await;
    let wh = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::NoOpinion);

    // empty user, non-resource request
    let a = RequestAttributes {
        is_non_resource_request: true,
        ..RequestAttributes::new(user(""), "", "")
    };
    assert!(is_allow(&wh.authorize_opinion(&a).await.unwrap()));
    let got = mock.lock().unwrap().last.clone().unwrap();
    assert_eq!(got["apiVersion"], "authorization.k8s.io/v1");
    assert_eq!(got["kind"], "SubjectAccessReview");
    assert_eq!(got["spec"], json!({"nonResourceAttributes": {}}));

    // resource request with everything set
    let mut u = user("jane");
    u.uid = "1".into();
    u.groups = vec!["group1".into(), "group2".into()];
    let a = RequestAttributes {
        namespace: Some("kittensandponies".into()),
        api_group: "group3".into(),
        name: Some("my-pod".into()),
        subresource: Some("proxy".into()),
        path: Some("/foo".into()),
        ..RequestAttributes::new(u, "GET", "pods")
    };
    assert!(is_allow(&wh.authorize_opinion(&a).await.unwrap()));
    let got = mock.lock().unwrap().last.clone().unwrap();
    assert_eq!(
        got["spec"],
        json!({
            "resourceAttributes": {
                "namespace": "kittensandponies", "verb": "GET", "group": "group3",
                "resource": "pods", "subresource": "proxy", "name": "my-pod"
            },
            "user": "jane", "groups": ["group1", "group2"], "uid": "1"
        })
    );
}

// webhook_v1beta1_test.go: version v1beta1 sends and expects v1beta1.
#[tokio::test]
async fn v1beta1_version_sends_a_v1beta1_review() {
    let mock = allowing_mock();
    let url = serve(mock.clone()).await;
    let wh = authorizer(&url, "v1beta1", Duration::ZERO, FailurePolicy::NoOpinion);
    assert!(is_allow(
        &wh.authorize_opinion(&resource("jane")).await.unwrap()
    ));
    let got = mock.lock().unwrap().last.clone().unwrap();
    assert_eq!(got["apiVersion"], "authorization.k8s.io/v1beta1");
    assert_eq!(got["spec"]["user"], "jane");
}

// subjectAccessReviewInterfaceFromConfig (webhook.go:470-476).
#[test]
fn unsupported_version_is_rejected() {
    let cfg = WebhookClientConfig::load(&kubeconfig_file("http://127.0.0.1:1/x")).unwrap();
    let err = WebhookAuthorizer::new(
        cfg,
        "v2",
        Duration::ZERO,
        Duration::ZERO,
        test_backoff(),
        FailurePolicy::NoOpinion,
    )
    .err()
    .unwrap()
    .to_string();
    assert_eq!(
        err,
        "unsupported webhook authorizer version \"v2\", supported versions are \"v1\", \"v1beta1\""
    );
}

// TestV1WebhookCache, row for row.
#[tokio::test]
async fn cache_and_retry_table() {
    let mock: Shared = Arc::new(Mutex::new(Mock::default()));
    let url = serve(mock.clone()).await;
    // "forever" (100 days)
    let wh = authorizer(
        &url,
        "v1",
        Duration::from_secs(2400 * 3600),
        FailurePolicy::NoOpinion,
    );
    let alice = resource("alice");
    let bob = resource("bob");
    let big = |n: &str| {
        let mut a = RequestAttributes::new(user(n), "v".repeat(2000), "r".repeat(2000));
        a.api_group = "g".repeat(2000);
        a.name = Some("n".repeat(2000));
        // upstream also has a 2000-char APIVersion; we carry none, so the
        // subresource makes up the 10000 bytes.
        a.subresource = Some("a".repeat(2000));
        a.namespace = Some("kittensandponies".into());
        a
    };
    let (bob_big, alice_big) = (big("bob"), big("alice"));

    // (name, attr, allow, status, expected_err, expected_authorized, expected_calls)
    #[allow(clippy::type_complexity)]
    let rows: Vec<(&str, &RequestAttributes, bool, u16, bool, bool, usize)> = vec![
        ("server errors retry", &alice, false, 500, true, false, 5),
        ("429s retry", &alice, false, 429, true, false, 5),
        ("404 doesnt retry", &alice, false, 404, true, false, 1),
        ("403 doesnt retry", &alice, false, 403, true, false, 1),
        ("401 doesnt retry", &alice, false, 401, true, false, 1),
        (
            "alice successful request",
            &alice,
            true,
            200,
            false,
            true,
            1,
        ),
        ("alice cached request", &alice, false, 500, false, true, 0),
        ("bob failed request", &bob, false, 500, true, false, 5),
        (
            "bob unauthorized request",
            &bob,
            false,
            200,
            false,
            false,
            1,
        ),
        (
            "bob unauthorized cached request",
            &bob,
            false,
            500,
            false,
            false,
            0,
        ),
        (
            "ridiculous unauthorized request",
            &bob_big,
            false,
            200,
            false,
            false,
            1,
        ),
        (
            "ridiculous unauthorized request again",
            &bob_big,
            false,
            200,
            false,
            false,
            1,
        ),
        (
            "ridiculous authorized request",
            &alice_big,
            true,
            200,
            false,
            true,
            1,
        ),
        (
            "ridiculous authorized request again",
            &alice_big,
            true,
            200,
            false,
            true,
            1,
        ),
    ];
    for (name, attr, allow, status, want_err, want_auth, want_calls) in rows {
        {
            let mut m = mock.lock().unwrap();
            m.called = 0;
            m.allow = allow;
            m.status_code = status;
        }
        let got = wh.authorize_opinion(attr).await;
        // Errors ride on a NoOpinion (failurePolicy=NoOpinion), as upstream's
        // (DecisionNoOpinion, "", err).
        let (authorized, errored) = match &got {
            Ok(Opinion::Allow) => (true, false),
            Ok(Opinion::NoOpinion { error, .. }) => (false, error.is_some()),
            Ok(Opinion::Deny(_)) => (false, false),
            Err(_) => (false, true),
        };
        assert_eq!(errored, want_err, "{name}: error");
        assert_eq!(authorized, want_auth, "{name}: authorized");
        assert_eq!(mock.lock().unwrap().called, want_calls, "{name}: calls");
    }
}

// A zero TTL caches nothing (reload.go:145-151).
#[tokio::test]
async fn zero_ttl_does_not_cache() {
    let mock = allowing_mock();
    let url = serve(mock.clone()).await;
    let wh = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::NoOpinion);
    wh.authorize_opinion(&resource("alice")).await.unwrap();
    wh.authorize_opinion(&resource("alice")).await.unwrap();
    assert_eq!(mock.lock().unwrap().called, 2);
}

// An authorized answer is cached for the authorized TTL, an unauthorized one
// for the unauthorized TTL (webhook.go:280-286).
#[tokio::test]
async fn authorized_and_unauthorized_ttls_are_independent() {
    let mock = allowing_mock();
    let url = serve(mock.clone()).await;
    let cfg = WebhookClientConfig::load(&kubeconfig_file(&url)).unwrap();
    let wh = WebhookAuthorizer::new(
        cfg,
        "v1",
        Duration::from_secs(3600), // authorized
        Duration::ZERO,            // unauthorized
        test_backoff(),
        FailurePolicy::NoOpinion,
    )
    .unwrap();
    wh.authorize_opinion(&resource("alice")).await.unwrap();
    wh.authorize_opinion(&resource("alice")).await.unwrap();
    assert_eq!(mock.lock().unwrap().called, 1, "authorized is cached");
    mock.lock().unwrap().allow = false;
    wh.authorize_opinion(&resource("bob")).await.unwrap();
    wh.authorize_opinion(&resource("bob")).await.unwrap();
    assert_eq!(mock.lock().unwrap().called, 3, "unauthorized is not");
}

// Authorize's closing switch (webhook.go:288-297).
#[tokio::test]
async fn status_maps_to_opinion() {
    let mock: Shared = Arc::new(Mutex::new(Mock {
        status_code: 200,
        ..Default::default()
    }));
    let url = serve(mock.clone()).await;
    let wh = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::NoOpinion);

    mock.lock().unwrap().reason = "nope".into();
    assert_eq!(
        wh.authorize_opinion(&resource("a")).await.unwrap(),
        Opinion::NoOpinion {
            reason: "nope".into(),
            error: None
        },
        "neither allowed nor denied is NoOpinion"
    );
    mock.lock().unwrap().denied = true;
    assert_eq!(
        wh.authorize_opinion(&resource("a")).await.unwrap(),
        Opinion::Deny("nope".into())
    );
    // allowed AND denied: Deny, with upstream's error alongside. Our
    // Opinion::Deny carries no error, so the error text is in the reason.
    mock.lock().unwrap().allow = true;
    match wh.authorize_opinion(&resource("a")).await.unwrap() {
        Opinion::Deny(reason) => {
            assert!(reason
                .contains("webhook subject access review returned both allow and deny response"))
        }
        other => panic!("allowed+denied must deny, got {other:?}"),
    }
}

// decisionOnError (reload.go:136-143): failurePolicy.
#[tokio::test]
async fn failure_policy_decides_on_error() {
    let mock: Shared = Arc::new(Mutex::new(Mock {
        status_code: 403,
        ..Default::default()
    }));
    let url = serve(mock.clone()).await;
    let no_opinion = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::NoOpinion);
    assert!(matches!(
        no_opinion.authorize_opinion(&resource("a")).await.unwrap(),
        Opinion::NoOpinion { error: Some(_), .. }
    ));
    let deny = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::Deny);
    assert!(matches!(
        deny.authorize_opinion(&resource("a")).await.unwrap(),
        Opinion::Deny(_)
    ));
}

// Retry-After is "an explicit confirmation we should retry"
// (DefaultShouldRetry, util/webhook/webhook.go:53).
#[tokio::test]
async fn retry_after_header_retries() {
    let mock: Shared = Arc::new(Mutex::new(Mock {
        status_code: 503,
        retry_after: Some("0".into()),
        ..Default::default()
    }));
    let url = serve(mock.clone()).await;
    let wh = authorizer(&url, "v1", Duration::ZERO, FailurePolicy::NoOpinion);
    wh.authorize_opinion(&resource("a")).await.unwrap();
    assert_eq!(mock.lock().unwrap().called, 5);
    mock.lock().unwrap().retry_after = None;
    mock.lock().unwrap().called = 0;
    wh.authorize_opinion(&resource("a")).await.unwrap();
    assert_eq!(mock.lock().unwrap().called, 1, "bare 503 is not retried");
}

// A webhook's explicit deny stops the union; NoOpinion falls through
// (union.go).
#[tokio::test]
async fn explicit_deny_stops_the_union() {
    let mock: Shared = Arc::new(Mutex::new(Mock {
        status_code: 200,
        denied: true,
        reason: "no".into(),
        ..Default::default()
    }));
    let url = serve(mock.clone()).await;
    let wh: Arc<dyn Authorizer> = Arc::new(authorizer(
        &url,
        "v1",
        Duration::ZERO,
        FailurePolicy::NoOpinion,
    ));
    let allow: Arc<dyn Authorizer> = Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer);
    let u = UnionAuthorizer::new(vec![wh.clone(), allow.clone()]);
    assert_eq!(
        u.authorize(&resource("a")).await.unwrap(),
        Decision::Deny("no".into())
    );
    mock.lock().unwrap().denied = false;
    let u = UnionAuthorizer::new(vec![wh, allow]);
    assert_eq!(u.authorize(&resource("a")).await.unwrap(), Decision::Allow);
}

// TestV1NewFromConfig (webhook_v1_test.go:68-): a config that cannot work is
// refused at load time.
#[test]
fn kubeconfig_without_a_server_is_rejected() {
    let p = std::env::temp_dir().join(format!(
        "authz-webhook-noserver-{}.yaml",
        std::process::id()
    ));
    std::fs::write(&p, "clusters:\n- name: a\n  cluster: {}\nusers: []\n").unwrap();
    assert!(WebhookClientConfig::load(&p).is_err());
    assert!(WebhookClientConfig::load(std::path::Path::new("/nonexistent/kubeconfig")).is_err());
}

// TestAuthzValidate (authorization_test.go:91-115).
#[test]
fn validate_webhook_flags() {
    let modes = |m: &[&str]| m.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let args = |file: &str| AuthorizationWebhookArgs {
        config_file: if file.is_empty() {
            None
        } else {
            Some(file.into())
        },
        ..Default::default()
    };
    let join = |e: Vec<String>| e.join("; ");
    assert!(join(validate_webhook_args(&modes(&["Webhook"]), &args("")))
        .contains("authorization-mode Webhook's authorization config file not passed"));
    assert!(
        join(validate_webhook_args(&modes(&["RBAC"]), &args("c.yaml")))
            .contains("cannot specify --authorization-webhook-config-file without mode Webhook")
    );
    assert!(validate_webhook_args(&modes(&["Webhook"]), &args("c.yaml")).is_empty());
    assert!(validate_webhook_args(&modes(&["Node", "RBAC"]), &args("")).is_empty());
}

// TestAddFlags (authorization_test.go:143-160) + the defaults at :79-82.
#[test]
fn flags_parse_with_upstream_defaults() {
    use clap::Parser;
    #[derive(Parser)]
    struct W {
        #[command(flatten)]
        a: AuthorizationWebhookArgs,
    }
    let d = W::try_parse_from(["x"]).unwrap().a;
    assert_eq!(d.config_file, None);
    assert_eq!(d.version, "v1beta1");
    assert_eq!(d.cache_authorized_ttl, Duration::from_secs(300));
    assert_eq!(d.cache_unauthorized_ttl, Duration::from_secs(30));

    let w = W::try_parse_from([
        "x",
        "--authorization-webhook-config-file=webhook_config_file.yaml",
        "--authorization-webhook-version=v1",
        "--authorization-webhook-cache-authorized-ttl=60s",
        "--authorization-webhook-cache-unauthorized-ttl=30s",
    ])
    .unwrap()
    .a;
    assert_eq!(w.config_file.as_deref(), Some("webhook_config_file.yaml"));
    assert_eq!(w.version, "v1");
    assert_eq!(w.cache_authorized_ttl, Duration::from_secs(60));
    assert_eq!(w.cache_unauthorized_ttl, Duration::from_secs(30));
}

#[test]
fn go_durations_parse() {
    assert_eq!(parse_go_duration("5m").unwrap(), Duration::from_secs(300));
    assert_eq!(
        parse_go_duration("1h30m").unwrap(),
        Duration::from_secs(5400)
    );
    assert_eq!(
        parse_go_duration("1.5s").unwrap(),
        Duration::from_millis(1500)
    );
    assert_eq!(
        parse_go_duration("250ms").unwrap(),
        Duration::from_millis(250)
    );
    assert_eq!(parse_go_duration("0").unwrap(), Duration::ZERO);
    assert!(parse_go_duration("5").is_err());
    assert!(parse_go_duration("abc").is_err());
}
