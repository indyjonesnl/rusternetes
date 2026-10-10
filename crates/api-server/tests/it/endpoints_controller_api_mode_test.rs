//! Regression for #3075: the EndpointsController, running over the API-backed
//! storage (the static kube-controller-manager pod's mode), must publish an
//! Endpoints object for a selector Service that matches no pods
//! (test/e2e/network/endpoints.go:254 "should create and delete Endpoints for a
//! Service with a selector that matches no pods").

use std::sync::Arc;
use std::time::Duration;

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

#[tokio::test]
async fn empty_selector_service_gets_endpoints_over_api_storage() {
    let api = TestApiServer::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api-server");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });

    let (s, b) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"endpoints-403"}}),
        )
        .await;
    assert_eq!(s.as_u16(), 201, "{b}");

    // Same shape as the e2e client's Service: no labels, named port, no targetPort.
    let (s, b) = api
        .post(
            "/api/v1/namespaces/endpoints-403/services",
            &json!({
                "apiVersion":"v1","kind":"Service",
                "metadata":{"name":"example-empty-selector"},
                "spec":{
                    "selector":{"does-not-match-anything":"endpoints-and-endpoint-slices-should-still-be-created"},
                    "ports":[{"name":"example","port":80,"protocol":"TCP"}]
                }
            }),
        )
        .await;
    assert_eq!(s.as_u16(), 201, "{b}");

    let client =
        Arc::new(ApiClient::new(&format!("http://{address}"), true, None).expect("build client"));
    let controller = Arc::new(EndpointsController::new(Arc::new(ApiStorage::new(client))));
    let run = tokio::spawn(async move {
        let _ = controller.run().await;
    });

    let mut found = false;
    for _ in 0..30 {
        let (s, _) = api
            .get("/api/v1/namespaces/endpoints-403/endpoints/example-empty-selector")
            .await;
        if s.as_u16() == 200 {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    run.abort();
    server.abort();
    assert!(found, "no Endpoints created for the empty-selector Service");
}
