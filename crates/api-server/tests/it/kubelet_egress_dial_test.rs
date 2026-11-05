//! The kubelet client dials through the egress selector's `Cluster` dialer
//! (#3466): `kubelet_client.go:96-104` `makeTransport` sets
//! `DialHolder = Lookup(Cluster)` for every connection to the kubelet (logs,
//! exec, attach, port-forward).

use axum::body::Body;
use rusternetes_api_server::egress_selector::{
    Conn, Dialer, DialerCreator, MetricsOptions, Proxier, ProxyServerConnector,
};
use rusternetes_api_server::handlers::pod_subresources::{
    fetch_kubelet_log_stream, proxy_kubelet_stream, proxy_kubelet_upgrade,
};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A konnectivity-like proxier: records the address asked for and tunnels to
/// the real local backend.
struct Tunnel {
    target: String,
    seen: Arc<Mutex<Vec<String>>>,
}
struct TunnelProxier {
    target: String,
    seen: Arc<Mutex<Vec<String>>>,
}
#[async_trait::async_trait]
impl ProxyServerConnector for Tunnel {
    async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
        Ok(Box::new(TunnelProxier {
            target: self.target.clone(),
            seen: self.seen.clone(),
        }))
    }
}
#[async_trait::async_trait]
impl Proxier for TunnelProxier {
    async fn proxy(&self, addr: &str) -> Result<Conn, String> {
        self.seen.lock().unwrap().push(addr.to_string());
        let c = tokio::net::TcpStream::connect(&self.target)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Box::new(c))
    }
}

fn tunnel_to(target: std::net::SocketAddr) -> (Dialer, Arc<Mutex<Vec<String>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let dialer = DialerCreator {
        connector: Some(Arc::new(Tunnel {
            target: target.to_string(),
            seen: seen.clone(),
        })),
        direct: false,
        options: MetricsOptions {
            transport: "tcp".into(),
            protocol: "HTTPConnect".into(),
        },
    }
    .create_dialer();
    (dialer, seen)
}

/// A one-connection backend answering with `reply` after reading the request.
async fn backend(reply: &'static [u8]) -> std::net::SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = s.read(&mut buf).await.unwrap();
        s.write_all(reply).await.unwrap();
        let _ = s.shutdown().await;
    });
    addr
}

/// An address nothing listens on: reachable only through the tunnel.
const KUBELET: &str = "10.254.254.1:10250";

#[tokio::test]
async fn kubelet_stream_proxy_dials_through_cluster_dialer() {
    let be =
        backend(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 4\r\n\r\nlogs")
            .await;
    let (dialer, seen) = tunnel_to(be);
    let target: axum::http::Uri = format!("http://{KUBELET}/containerLogs/ns/p/c")
        .parse()
        .unwrap();
    let req = axum::http::Request::builder()
        .uri("/x")
        .body(Body::empty())
        .unwrap();
    let resp = proxy_kubelet_stream(target, req, Some(dialer)).await;
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"logs");
    assert_eq!(*seen.lock().unwrap(), vec![KUBELET.to_string()]);
}

#[tokio::test]
async fn kubelet_upgrade_proxy_splices_through_cluster_dialer() {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be = l.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = s.read(&mut buf).await.unwrap();
        s.write_all(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n",
        )
        .await
        .unwrap();
        let mut line = [0u8; 5];
        s.read_exact(&mut line).await.unwrap();
        s.write_all(&line.to_ascii_uppercase()).await.unwrap();
    });
    let (dialer, seen) = tunnel_to(be);

    let front = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front_addr = front.local_addr().unwrap();
    let target: axum::http::Uri = format!("http://{KUBELET}/exec/ns/p/c").parse().unwrap();
    tokio::spawn(async move {
        let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
            let (t, d) = (target.clone(), dialer.clone());
            async move { proxy_kubelet_upgrade(t, req, Some(d)).await }
        });
        axum::serve(front, app).await.unwrap();
    });

    let mut c = tokio::net::TcpStream::connect(front_addr).await.unwrap();
    c.write_all(b"GET /x HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n")
        .await
        .unwrap();
    let mut hdr = [0u8; 1024];
    let n = c.read(&mut hdr).await.unwrap();
    assert!(std::str::from_utf8(&hdr[..n]).unwrap().contains("101"));
    c.write_all(b"hello").await.unwrap();
    let mut out = [0u8; 5];
    c.read_exact(&mut out).await.unwrap();
    assert_eq!(&out, b"HELLO");
    assert_eq!(*seen.lock().unwrap(), vec![KUBELET.to_string()]);
}

#[tokio::test]
async fn kubelet_log_fetch_dials_through_cluster_dialer() {
    use futures::StreamExt;
    let be =
        backend(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 6\r\n\r\nline1\n")
            .await;
    let (dialer, seen) = tunnel_to(be);
    let target: axum::http::Uri = format!("http://{KUBELET}/containerLogs/ns/p/c")
        .parse()
        .unwrap();
    let Ok(mut stream) = fetch_kubelet_log_stream(&target, "p", Some(dialer))
        .await
        .unwrap()
    else {
        panic!("a 200 yields the log stream");
    };
    let mut got = Vec::new();
    while let Some(chunk) = stream.next().await {
        got.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(got, b"line1\n");
    assert_eq!(*seen.lock().unwrap(), vec![KUBELET.to_string()]);

    // A kubelet refusal is converted to a Status, not streamed as log text.
    let be =
        backend(b"HTTP/1.1 404 Not Found\r\nContent-Length: 18\r\n\r\ncontainer not here").await;
    let (dialer, _) = tunnel_to(be);
    match fetch_kubelet_log_stream(&target, "p", Some(dialer))
        .await
        .unwrap()
    {
        Err(resp) => assert_eq!(resp.status(), 404),
        Ok(_) => panic!("a 404 is a Status response"),
    }
}
