//! `Error::into_response` bodies are negotiated too (#3578).
//!
//! Upstream calls `responsewriters.ErrorNegotiated` at every error site
//! (writers.go:381-397); here a response-layer middleware re-encodes the
//! `Status` an `Error` rendered.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use rusternetes_api_server::negotiated_error::negotiate_error_middleware;
use rusternetes_common::types::{Status, StatusDetails};
use rusternetes_common::Error;
use tower::ServiceExt;

fn app() -> Router {
    Router::new()
        .route(
            "/nf",
            get(|| async { Err::<(), _>(Error::NotFound("pods \"x\" not found".into())) }),
        )
        .route(
            "/busy",
            get(|| async {
                let mut s = Status::failure("slow down", "ServerTimeout", 429);
                s.details = Some(StatusDetails {
                    name: None,
                    group: None,
                    kind: None,
                    uid: None,
                    causes: None,
                    retry_after_seconds: Some(7),
                });
                Err::<(), _>(Error::Status(Box::new(s)))
            }),
        )
        .route("/ok", get(|| async { "fine" }))
        .layer(axum::middleware::from_fn(negotiate_error_middleware))
}

async fn get_with(path: &str, accept: Option<&str>) -> axum::response::Response {
    let mut b = Request::builder().uri(path);
    if let Some(a) = accept {
        b = b.header(header::ACCEPT, a);
    }
    app().oneshot(b.body(Body::empty()).unwrap()).await.unwrap()
}

async fn text(r: axum::response::Response) -> String {
    let b = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&b).into_owned()
}

#[tokio::test]
async fn default_is_json() {
    let r = get_with("/nf", None).await;
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/json");
    let v: serde_json::Value = serde_json::from_str(&text(r).await).unwrap();
    assert_eq!(v["reason"], "NotFound");
}

#[tokio::test]
async fn yaml_accept_gets_yaml() {
    let r = get_with("/nf", Some("application/yaml")).await;
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/yaml");
    let v: serde_json::Value = serde_yaml::from_str(&text(r).await).unwrap();
    assert_eq!(v["reason"], "NotFound");
}

#[tokio::test]
async fn protobuf_accept_gets_protobuf() {
    let r = get_with("/nf", Some("application/vnd.kubernetes.protobuf")).await;
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        r.headers()[header::CONTENT_TYPE],
        "application/vnd.kubernetes.protobuf"
    );
}

#[tokio::test]
async fn unacceptable_accept_falls_back_to_json() {
    let r = get_with("/nf", Some("text/html")).await;
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/json");
}

#[tokio::test]
async fn retry_after_survives() {
    let r = get_with("/busy", Some("application/yaml")).await;
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.headers()[header::RETRY_AFTER], "7");
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/yaml");
}

#[tokio::test]
async fn success_bodies_untouched() {
    let r = get_with("/ok", Some("application/yaml")).await;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(text(r).await, "fine");
}
