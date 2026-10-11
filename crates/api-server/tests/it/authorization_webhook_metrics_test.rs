//! Authorization webhook + matchCondition metrics (#3165).
//!
//! Ported from `plugin/pkg/authorizer/webhook/webhook.go:253-273`
//! (RecordWebhookEvaluation/Duration/FailOpen), `webhook/metrics/metrics.go`
//! and `authorization/cel/matcher.go:44-82` + `cel/metrics.go`.

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use rusternetes_api_server::authorization_match_conditions::WebhookMatchCondition;
use rusternetes_api_server::authorization_webhook::{
    Backoff, FailurePolicy, WebhookAuthorizer, WebhookClientConfig,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, RequestAttributes};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

async fn review(State(code): State<Arc<u16>>, Json(body): Json<Value>) -> axum::response::Response {
    use axum::response::IntoResponse;
    if *code != 200 {
        return (StatusCode::from_u16(*code).unwrap(), "err").into_response();
    }
    Json(
        json!({"apiVersion": body["apiVersion"], "kind": "SubjectAccessReview",
        "status": {"allowed": true}}),
    )
    .into_response()
}

async fn serve(code: u16) -> String {
    let app = Router::new()
        .route("/t", post(review))
        .with_state(Arc::new(code));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/t", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    url
}

fn authorizer(url: &str, name: &str, policy: FailurePolicy, exprs: &[&str]) -> WebhookAuthorizer {
    let path = std::env::temp_dir().join(format!(
        "authz-metrics-{}-{name}.kubeconfig",
        std::process::id()
    ));
    std::fs::write(
        &path,
        format!("apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: {url}\nusers:\n- name: u\n  user: {{}}\ncontexts:\n- name: x\n  context: {{cluster: c, user: u}}\ncurrent-context: x\n"),
    )
    .unwrap();
    let cfg = WebhookClientConfig::load(&path).unwrap();
    let b = Backoff {
        duration: Duration::ZERO,
        factor: 0.0,
        jitter: 0.0,
        steps: 1,
    };
    let conds: Vec<_> = exprs
        .iter()
        .map(|e| WebhookMatchCondition {
            expression: (*e).into(),
        })
        .collect();
    WebhookAuthorizer::new(cfg, "v1", Duration::ZERO, Duration::ZERO, b, policy)
        .unwrap()
        .with_name(name)
        .with_match_conditions(&conds)
        .unwrap()
}

fn attrs() -> RequestAttributes {
    RequestAttributes::new(
        UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
            extra: Default::default(),
        },
        "get",
        "pods",
    )
}

/// Value of a counter, or a histogram's sample count, for the series whose
/// labels include `labels`.
fn sample(metric: &str, labels: &[(&str, &str)]) -> u64 {
    for f in prometheus::gather() {
        if f.name() != metric {
            continue;
        }
        for m in f.get_metric() {
            let l: Vec<_> = m
                .get_label()
                .iter()
                .map(|p| (p.name(), p.value()))
                .collect();
            if labels.iter().all(|x| l.contains(x)) {
                let h = m.get_histogram().get_sample_count();
                return if h > 0 {
                    h
                } else {
                    m.get_counter().value() as u64
                };
            }
        }
    }
    0
}

#[tokio::test]
async fn success_records_evaluation_and_duration() {
    let a = authorizer(&serve(200).await, "m-ok", FailurePolicy::NoOpinion, &[]);
    a.authorize_opinion(&attrs()).await.unwrap();
    let l = [("name", "m-ok"), ("result", "success")];
    assert_eq!(
        sample("apiserver_authorization_webhook_evaluations_total", &l),
        1
    );
    assert_eq!(
        sample("apiserver_authorization_webhook_duration_seconds", &l),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_webhook_evaluations_fail_open_total",
            &[("name", "m-ok")]
        ),
        0
    );
}

#[tokio::test]
async fn error_with_no_opinion_policy_records_fail_open() {
    let a = authorizer(&serve(403).await, "m-err", FailurePolicy::NoOpinion, &[]);
    a.authorize_opinion(&attrs()).await.unwrap();
    let l = [("name", "m-err"), ("result", "error")];
    assert_eq!(
        sample("apiserver_authorization_webhook_evaluations_total", &l),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_webhook_evaluations_fail_open_total",
            &l
        ),
        1
    );
}

#[tokio::test]
async fn gateway_timeout_is_a_timeout_result_and_deny_policy_does_not_fail_open() {
    let a = authorizer(&serve(504).await, "m-to", FailurePolicy::Deny, &[]);
    a.authorize_opinion(&attrs()).await.unwrap();
    let l = [("name", "m-to"), ("result", "timeout")];
    assert_eq!(
        sample("apiserver_authorization_webhook_evaluations_total", &l),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_webhook_evaluations_fail_open_total",
            &l
        ),
        0
    );
}

#[tokio::test]
async fn match_condition_metrics() {
    let url = serve(200).await;
    // FALSE -> exclusion + evaluation histogram, webhook not called.
    let a = authorizer(
        &url,
        "m-mc-x",
        FailurePolicy::NoOpinion,
        &["request.user == 'bob'"],
    );
    a.authorize_opinion(&attrs()).await.unwrap();
    let l = [("type", "Webhook"), ("name", "m-mc-x")];
    assert_eq!(
        sample(
            "apiserver_authorization_match_condition_exclusions_total",
            &l
        ),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_match_condition_evaluation_seconds",
            &l
        ),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_match_condition_evaluation_errors_total",
            &l
        ),
        0
    );
    // Error -> failure counter.
    let b = authorizer(
        &url,
        "m-mc-e",
        FailurePolicy::NoOpinion,
        &["request.nonResourceAttributes.path == '/x'"],
    );
    b.authorize_opinion(&attrs()).await.unwrap();
    let l = [("type", "Webhook"), ("name", "m-mc-e")];
    assert_eq!(
        sample(
            "apiserver_authorization_match_condition_evaluation_errors_total",
            &l
        ),
        1
    );
    assert_eq!(
        sample(
            "apiserver_authorization_match_condition_exclusions_total",
            &l
        ),
        0
    );
}

/// webhook.go:236-249: `case ctx.Err() != nil: metricsResult = "canceled"`.
/// Rust has no request context; the analogue of a cancelled ctx is the
/// authorize future being dropped (client disconnect / request timeout) while
/// the webhook call is in flight (#3217).
#[tokio::test]
async fn dropped_in_flight_call_records_canceled() {
    async fn slow(Json(_): Json<Value>) -> &'static str {
        tokio::time::sleep(Duration::from_secs(30)).await;
        "{}"
    }
    let app = Router::new().route("/t", post(slow));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/t", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

    let a = authorizer(&url, "m-cancel", FailurePolicy::NoOpinion, &[]);
    let r = tokio::time::timeout(Duration::from_millis(200), a.authorize_opinion(&attrs())).await;
    assert!(r.is_err(), "call should still be in flight");
    let l = [("name", "m-cancel"), ("result", "canceled")];
    assert_eq!(
        sample("apiserver_authorization_webhook_evaluations_total", &l),
        1
    );
    assert_eq!(
        sample("apiserver_authorization_webhook_duration_seconds", &l),
        1
    );
}
