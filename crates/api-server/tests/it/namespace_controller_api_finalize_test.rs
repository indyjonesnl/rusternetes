//! Regression test for namespace cleanup through the controller's API-backed
//! storage path. This is the path Hydrophone exercises when it waits for its
//! `conformance` namespace to disappear.

use std::sync::Arc;

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controllers::namespace::NamespaceController;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

#[tokio::test]
async fn controller_finalizes_namespace_through_api_subresource() {
    let api = TestApiServer::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api-server");
    let address = listener.local_addr().expect("test api-server address");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });

    let namespace = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "conformance"}
    });
    let (create_status, created) = api.post("/api/v1/namespaces", &namespace).await;
    assert_eq!(create_status.as_u16(), 201, "create failed: {created}");

    let (delete_status, deleted) = api.delete("/api/v1/namespaces/conformance").await;
    assert!(
        delete_status.is_success(),
        "namespace delete failed: {deleted}"
    );

    let client = Arc::new(
        ApiClient::new(&format!("http://{address}"), true, None).expect("build api-server client"),
    );
    let storage = Arc::new(ApiStorage::new(client));
    let controller = NamespaceController::new(storage);

    // Namespace finalization is intentionally two-phase: one cycle writes
    // deletion conditions, and the next drains spec.finalizers via /finalize.
    controller
        .reconcile_all()
        .await
        .expect("first namespace reconciliation");
    controller
        .reconcile_all()
        .await
        .expect("second namespace reconciliation");

    let (get_status, _) = api.get("/api/v1/namespaces/conformance").await;
    assert_eq!(
        get_status.as_u16(),
        404,
        "namespace must disappear after controller finalization"
    );

    server.abort();
}
