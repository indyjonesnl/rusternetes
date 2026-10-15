//! #3761: a watch answered with the in-stream ERROR 410 Expired envelope must
//! be a well-framed HTTP response a real client can read.
//!
//! Upstream: `cacher.Watch` returns `newErrWatcher(err)`
//! (`storage/cacher/cacher.go:580,1335-1358`), a watcher holding one ERROR
//! event; `serveWatch` then streams it as an ordinary 200 watch response
//! (`endpoints/handlers/watch.go`), where Go's net/http chooses the framing.
//! Here the framing is read off raw bytes over a loopback socket.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Decode a chunked body strictly; `None` if the framing is malformed.
fn dechunk(mut raw: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let nl = raw.windows(2).position(|w| w == b"\r\n")?;
        let size = usize::from_str_radix(std::str::from_utf8(&raw[..nl]).ok()?.trim(), 16).ok()?;
        raw = &raw[nl + 2..];
        if size == 0 {
            return Some(out);
        }
        if raw.len() < size + 2 || &raw[size..size + 2] != b"\r\n" {
            return None;
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
}

#[tokio::test]
async fn expired_watch_error_is_well_framed_over_http() {
    let api = TestApiServer::new();
    api.storage.compact_to(999);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = api.router.clone();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(
        b"GET /api/v1/namespaces/default/configmaps?watch=true&resourceVersion=100 HTTP/1.1\r\n\
          Host: localhost\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sock.read_to_end(&mut raw),
    )
    .await
    .expect("response must end")
    .unwrap();
    server.abort();

    assert!(
        !raw.is_empty(),
        "server closed the connection without a response (#3761)"
    );
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("headers");
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let body_raw = &raw[split + 4..];
    eprintln!("RAW: {}", String::from_utf8_lossy(&raw));
    assert!(head.starts_with("http/1.1 200"), "{head}");
    let body = if head.contains("transfer-encoding: chunked") {
        dechunk(body_raw).unwrap_or_else(|| panic!("malformed chunked framing: {body_raw:?}"))
    } else {
        body_raw.to_vec()
    };
    let line = String::from_utf8(body).unwrap();
    let ev: Value = serde_json::from_str(line.trim()).expect("one JSON ERROR line");
    assert_eq!(ev["type"], "ERROR");
    assert_eq!(ev["object"]["code"], 410);
    assert_eq!(ev["object"]["reason"], "Expired");
}
