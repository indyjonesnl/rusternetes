//! Every api-server listener serves the router through `serve_service`, which
//! makes the peer address available to handlers as `ConnectInfo<SocketAddr>`
//! (Go's `http.Request.RemoteAddr`). The aggregator proxy reads it to append
//! `X-Forwarded-For` (`upgradeaware.go`), and before this nothing in the
//! serving stack supplied it, so the header was never sent.

use axum::extract::ConnectInfo;
use axum::routing::get;
use axum::Router;
use k3rs_api_server::serve_service;
use std::net::SocketAddr;

async fn peer(ConnectInfo(addr): ConnectInfo<SocketAddr>) -> String {
    addr.to_string()
}

#[tokio::test]
async fn handlers_see_the_peer_address() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let app = Router::new().route("/peer", get(peer));
    tokio::spawn(async move {
        axum::serve(listener, serve_service(app)).await.unwrap();
    });

    // Pin the client's local port so the assertion is exact.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let client_addr = socket.local_addr().unwrap();
    let stream = socket.connect(server_addr).await.unwrap();

    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(conn);
    let req = hyper::Request::builder()
        .uri("/peer")
        .header("host", "x")
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(std::str::from_utf8(&body).unwrap(), client_addr.to_string());
}
