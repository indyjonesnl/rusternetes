//! Ports of `healthz_test.go` (`TestInstallPathHandler` / `TestExcludeChecks` /
//! verbose and failure bodies) against `install_path_handler`.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use rusternetes_kubelet::healthz::{install_path_handler, HealthChecker};
use tower::ServiceExt;

async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn app() -> Router {
    install_path_handler(
        "/healthz",
        vec![
            HealthChecker::ping(),
            HealthChecker::log(),
            HealthChecker::named("syncloop", || {
                Err("sync Loop took longer than expected".into())
            }),
        ],
    )
}

#[tokio::test]
async fn root_fails_verbosely_with_reason_withheld() {
    let (st, body) = get(&app(), "/healthz").await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        "[+]ping ok\n[+]log ok\n[-]syncloop failed: reason withheld\nhealthz check failed\n"
    );
}

#[tokio::test]
async fn exclude_makes_the_root_pass() {
    let a = app();
    let (st, body) = get(&a, "/healthz?exclude=syncloop").await;
    assert_eq!((st, body.as_str()), (StatusCode::OK, "ok"));
    let (_, body) = get(&a, "/healthz?verbose&exclude=syncloop&exclude=nope").await;
    assert_eq!(
        body,
        "[+]ping ok\n[+]log ok\n[+]syncloop excluded: ok\n\
         warn: some health checks cannot be excluded: no matches for \"nope\"\n\
         healthz check passed\n"
    );
}

#[tokio::test]
async fn verbose_lists_every_check() {
    let a = install_path_handler(
        "/healthz",
        vec![HealthChecker::ping(), HealthChecker::log()],
    );
    assert_eq!(
        get(&a, "/healthz?verbose").await,
        (
            StatusCode::OK,
            "[+]ping ok\n[+]log ok\nhealthz check passed\n".to_string()
        )
    );
    assert_eq!(
        get(&a, "/healthz").await,
        (StatusCode::OK, "ok".to_string())
    );
}

#[tokio::test]
async fn sub_paths_serve_each_check() {
    let a = app();
    assert_eq!(
        get(&a, "/healthz/ping").await,
        (StatusCode::OK, "ok".into())
    );
    assert_eq!(get(&a, "/healthz/log").await, (StatusCode::OK, "ok".into()));
    let (st, body) = get(&a, "/healthz/syncloop").await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        "internal server error: sync Loop took longer than expected\n"
    );
    assert_eq!(get(&a, "/healthz/nope").await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn no_checks_installs_ping() {
    let a = install_path_handler("/healthz", vec![]);
    assert_eq!(
        get(&a, "/healthz?verbose").await.1,
        "[+]ping ok\nhealthz check passed\n"
    );
}
