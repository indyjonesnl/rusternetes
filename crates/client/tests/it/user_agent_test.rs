//! Per-controller `User-Agent`: `restclient.AddUserAgent(&clientConfig, name)`
//! (controller-manager `clientbuilder/client_builder.go:46`) appends the
//! controller name to the default agent (`rest/config.go:626-630`).

use rusternetes_client::http::ApiClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A one-shot HTTP server: answers every request with `{}` and reports each
/// request's `User-Agent` header (None when absent).
async fn server() -> (String, tokio::sync::mpsc::UnboundedReceiver<Option<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                let ua = head.lines().find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("user-agent")
                        .then(|| v.trim().to_string())
                });
                let _ = tx.send(ua);
                let body = "{}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (url, rx)
}

const DEFAULT_UA: &str = "controller-manager/v1.35.0 (linux/amd64) kubernetes/abcdef0";

#[tokio::test]
async fn controller_client_appends_its_name_to_the_default_agent() {
    let (url, mut seen) = server().await;
    let skeleton = ApiClient::new(&url, false, None)
        .unwrap()
        .with_default_user_agent(DEFAULT_UA);

    skeleton
        .for_controller("deployment-controller")
        .get::<serde_json::Value>("/x")
        .await
        .unwrap();
    assert_eq!(
        seen.recv().await.unwrap().as_deref(),
        Some("controller-manager/v1.35.0 (linux/amd64) kubernetes/abcdef0/deployment-controller")
    );

    // Each clone carries its own name; the skeleton keeps the bare default.
    skeleton
        .for_controller("job-controller")
        .get::<serde_json::Value>("/x")
        .await
        .unwrap();
    assert!(seen
        .recv()
        .await
        .unwrap()
        .unwrap()
        .ends_with("/job-controller"));
    skeleton.get::<serde_json::Value>("/x").await.unwrap();
    assert_eq!(seen.recv().await.unwrap().as_deref(), Some(DEFAULT_UA));
}

/// A clone of a clone keeps naming from the DEFAULT, not from the previous
/// controller's agent (`AddUserAgent` always starts from
/// `DefaultKubernetesUserAgent()`).
#[tokio::test]
async fn for_controller_always_starts_from_the_default_agent() {
    let (url, mut seen) = server().await;
    let a = ApiClient::new(&url, false, None)
        .unwrap()
        .with_default_user_agent(DEFAULT_UA)
        .for_controller("a");
    a.for_controller("b")
        .get::<serde_json::Value>("/x")
        .await
        .unwrap();
    assert_eq!(
        seen.recv().await.unwrap().as_deref(),
        Some(format!("{DEFAULT_UA}/b").as_str())
    );
}

/// Every verb carries the agent, watches included.
#[tokio::test]
async fn every_request_kind_sends_the_agent() {
    let (url, mut seen) = server().await;
    let c = ApiClient::new(&url, false, None)
        .unwrap()
        .with_default_user_agent(DEFAULT_UA)
        .for_controller("c");
    let want = format!("{DEFAULT_UA}/c");
    let body = serde_json::json!({});

    let _ = c.get::<serde_json::Value>("/x").await;
    let _ = c.post::<_, serde_json::Value>("/x", &body).await;
    let _ = c.put::<_, serde_json::Value>("/x", &body).await;
    let _ = c
        .patch::<_, serde_json::Value>("/x", &body, "application/merge-patch+json")
        .await;
    let _ = c.delete_with_options("/x", &[], None).await;
    let _ = c.get_stream("/x").await;
    let _ = c.get_text("/x").await;
    let _ = c.resource_exists("/x").await;
    for _ in 0..8 {
        assert_eq!(seen.recv().await.unwrap().as_deref(), Some(want.as_str()));
    }
}

/// A client without a default agent sends none, as before this change.
#[tokio::test]
async fn no_default_agent_sends_no_header() {
    let (url, mut seen) = server().await;
    let c = ApiClient::new(&url, false, None).unwrap();
    c.for_controller("x")
        .get::<serde_json::Value>("/x")
        .await
        .unwrap();
    assert_eq!(seen.recv().await.unwrap(), None);
}
