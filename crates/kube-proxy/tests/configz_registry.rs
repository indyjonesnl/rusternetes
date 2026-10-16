//! Port of component-base/configz/configz_test.go:38-105.
use axum::{body::Body, http::Request};
use rusternetes_kube_proxy::serve_metrics::{metrics_router_with_config, ConfigRegistry};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn configz_registration_update_delete_and_duplicate() {
    let registry = Arc::new(ConfigRegistry::default());
    registry.register("testing").unwrap();
    let error = registry.register("testing").unwrap_err();
    assert_eq!(error.to_string(), "register config \"testing\" twice");
    for (value, expected) in [
        (Some("blah"), "{\"testing\":\"blah\"}"),
        (Some("bing"), "{\"testing\":\"bing\"}"),
        (None, "{}"),
    ] {
        if let Some(value) = value {
            registry.set("testing", serde_json::json!(value)).unwrap();
        } else {
            registry.delete("testing");
        }
        let response = metrics_router_with_config("iptables", Arc::clone(&registry))
            .oneshot(
                Request::builder()
                    .uri("/configz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, expected);
    }
}
