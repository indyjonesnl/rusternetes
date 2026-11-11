//! `ApiClient::get` on a 404 returns the typed `StatusError` client-go hands
//! back (rest/request.go:1489-1511 `Result.Error()`), so
//! `apierrors.IsNotFound` (apimachinery errors.go:529-538) and the Status
//! message survive instead of being dropped.

use k3rs_client::api_status::is_not_found;
use k3rs_client::http::{ApiClient, GetError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn server(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    url
}

#[tokio::test]
async fn get_404_carries_the_status_body() {
    let url = server(
        r#"{"kind":"Status","status":"Failure","reason":"NotFound","message":"daemonsets.apps \"x\" not found","code":404}"#,
    )
    .await;
    let client = ApiClient::new(&url, false, None).unwrap();
    match client.get::<serde_json::Value>("/x").await {
        Err(GetError::NotFound(status)) => {
            assert_eq!(status.code, 404);
            assert_eq!(status.reason, "NotFound");
            assert_eq!(
                status.to_string(),
                "Error from server (NotFound): daemonsets.apps \"x\" not found"
            );
            assert!(is_not_found(&anyhow::Error::new(status)));
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn get_404_without_status_body_derives_not_found_from_the_code() {
    let url = server("nope").await;
    let client = ApiClient::new(&url, false, None).unwrap();
    match client.get::<serde_json::Value>("/x").await {
        Err(GetError::NotFound(status)) => {
            assert_eq!(status.reason, "NotFound");
            assert!(is_not_found(&anyhow::Error::new(status)));
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
}
