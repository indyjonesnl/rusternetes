//! The request half of the APIService availability controller's discovery
//! check (`remote_available_controller.go:277-352`).

use k3rs_api_server::handlers::aggregator::{availability_discovery_request, AggregatorTarget};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn backend(reply: &'static str) -> (std::net::SocketAddr, Arc<Mutex<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(String::new()));
    let seen2 = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let seen = seen2.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                *seen.lock().unwrap() = String::from_utf8_lossy(&buf).to_string();
                let _ = sock.write_all(reply.as_bytes()).await;
            });
        }
    });
    (addr, seen)
}

fn target(addr: std::net::SocketAddr) -> AggregatorTarget {
    AggregatorTarget {
        host: addr.ip().to_string(),
        port: addr.port(),
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "http",
        server_name: None,
    }
}

#[tokio::test]
async fn a_2xx_discovery_response_passes_with_the_aggregator_identity() {
    let (addr, seen) = backend("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
    availability_discovery_request(
        &target(addr),
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
    )
    .await
    .unwrap();
    let req = seen.lock().unwrap().to_lowercase();
    assert!(
        req.starts_with("get /apis/wardle.example.com/v1alpha1 http/1.1"),
        "{req}"
    );
    assert!(
        req.contains("x-remote-user: system:kube-aggregator"),
        "{req}"
    );
    assert!(req.contains("x-remote-group: system:masters"), "{req}");
}

#[tokio::test]
async fn the_legacy_v1_apiservice_checks_the_api_path() {
    let (addr, seen) = backend("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
    availability_discovery_request(&target(addr), "v1.", "", "v1")
        .await
        .unwrap();
    assert!(seen.lock().unwrap().starts_with("GET /api/v1 HTTP/1.1"));
}

#[tokio::test]
async fn a_non_2xx_response_is_a_bad_status() {
    let (addr, _) = backend("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n").await;
    let err = availability_discovery_request(&target(addr), "v1alpha1.g", "g", "v1alpha1")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        format!("bad status from http://{addr}/apis/g/v1alpha1: 503")
    );
}

#[tokio::test]
async fn a_redirect_is_not_followed_and_is_a_bad_status() {
    let (addr, _) =
        backend("HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n").await;
    let err = availability_discovery_request(&target(addr), "v1alpha1.g", "g", "v1alpha1")
        .await
        .unwrap_err();
    assert!(err.starts_with("bad status from"), "{err}");
    assert!(err.ends_with(": 302"), "{err}");
}

#[tokio::test]
async fn an_unreachable_backend_is_a_failing_or_missing_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let err = availability_discovery_request(&target(addr), "v1alpha1.g", "g", "v1alpha1")
        .await
        .unwrap_err();
    assert!(
        err.starts_with(&format!(
            "failing or missing response from http://{addr}/apis/g/v1alpha1: "
        )),
        "{err}"
    );
}
