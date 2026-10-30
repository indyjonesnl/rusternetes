//! Unit tests moved from `src/streaming_server.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::Path,
        http::StatusCode,
        response::{IntoResponse, Response},
    };
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_common::types::ObjectMeta;
    pub use tracing::{info, warn};
}

mod run_checkpoint_tests {
    use super::__parent::*;
    use axum::http::{Method, Request};
    use rusternetes_kubelet::server::ServerState;
    use rusternetes_kubelet::streaming_server::*;
    use rusternetes_storage::{Storage, StorageBackend};
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn app(checkpoint: bool) -> axum::Router {
        let storage = Arc::new(StorageBackend::new_memory());
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".into(),
                kind: "Pod".into(),
            },
            metadata: ObjectMeta {
                name: "p".into(),
                namespace: Some("ns".into()),
                uid: "u1".into(),
                ..Default::default()
            },
            spec: Some(
                serde_json::from_value(serde_json::json!({
                    "nodeName": "n",
                    "containers": [{"name": "main", "image": "x"}]
                }))
                .unwrap(),
            ),
            status: None,
        };
        storage
            .create(
                &rusternetes_storage::build_key("pods", Some("ns"), "p"),
                &pod,
            )
            .await
            .unwrap();
        run_router(
            RunState {
                server: ServerState {
                    node_name: "n".into(),
                    storage,
                    kubelet: None,
                },
                checkpoints_dir: "/tmp/checkpoints".into(),
            },
            checkpoint,
        )
    }

    async fn call(app: &axum::Router, m: Method, path: &str) -> (u16, String) {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(m)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&b).into_owned())
    }

    // server.go:994-998: unknown pod => 404 "pod does not exist".
    #[tokio::test]
    async fn run_unknown_pod_is_404() {
        let app = app(true).await;
        let (s, b) = call(&app, Method::POST, "/run/ns/nope/main?cmd=ls").await;
        assert_eq!((s, b.as_str()), (404, "pod does not exist"));
        let (s, _) = call(&app, Method::POST, "/run/ns/nope/u1/main?cmd=ls").await;
        assert_eq!(s, 404);
    }

    // The /run web service only registers POST routes (server.go:526-529).
    #[tokio::test]
    async fn run_is_post_only() {
        let app = app(true).await;
        assert_eq!(
            call(&app, Method::GET, "/run/ns/p/main?cmd=ls").await.0,
            405
        );
    }

    // server.go:1056-1086, 1100-1115.
    #[tokio::test]
    async fn checkpoint_validates_pod_container_and_timeout() {
        let app = app(true).await;
        let (s, b) = call(&app, Method::POST, "/checkpoint/ns/nope/main").await;
        assert_eq!((s, b.as_str()), (404, "pod does not exist"));
        let (s, b) = call(&app, Method::POST, "/checkpoint/ns/p/other").await;
        assert_eq!((s, b.as_str()), (404, "container other does not exist"));
        let (s, b) = call(&app, Method::POST, "/checkpoint/ns/p/main?timeout=abc").await;
        assert_eq!(
            (s, b.as_str()),
            (404, "cannot parse value of timeout parameter")
        );
    }

    // server.go:620: /checkpoint exists only with the ContainerCheckpoint gate.
    #[tokio::test]
    async fn checkpoint_route_absent_without_gate() {
        let app = app(false).await;
        assert_eq!(
            call(&app, Method::POST, "/checkpoint/ns/p/main").await.0,
            404
        );
    }
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::streaming_server::*;

    #[test]
    fn websocket_request_detection_matches_wsstream() {
        let mk = |pairs: &[(&str, &str)]| {
            let mut h = axum::http::HeaderMap::new();
            for (k, v) in pairs {
                h.append(
                    axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    v.parse().unwrap(),
                );
            }
            h
        };
        assert!(is_websocket_request(&mk(&[
            ("connection", "Upgrade"),
            ("upgrade", "websocket")
        ])));
        assert!(is_websocket_request(&mk(&[
            ("connection", "keep-alive, Upgrade"),
            ("upgrade", "WebSocket")
        ])));
        // SPDY upgrade is not a WebSocket request.
        assert!(!is_websocket_request(&mk(&[
            ("connection", "Upgrade"),
            ("upgrade", "SPDY/3.1")
        ])));
        assert!(!is_websocket_request(&mk(&[])));
    }

    // Upstream `getPortForward` rejects bad options with 400 BEFORE touching the
    // runtime (server.go:1029-1034), so this needs no CRI endpoint.
    #[tokio::test]
    async fn portforward_without_port_over_websocket_is_a_400() {
        use tower::ServiceExt;
        let app = axum::Router::new().route(
            "/portForward/:namespace/:pod",
            axum::routing::get(handle_portforward),
        );
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/portForward/ns/pod")
                    .header("connection", "Upgrade")
                    .header("upgrade", "websocket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn log_params_parse_tail_and_follow() {
        let p = LogParams::from_query("tailLines=10&follow=true&timestamps=false&limitBytes=2048");
        assert_eq!(p.tail_lines, Some(10));
        assert!(p.follow);
        assert_eq!(p.limit_bytes, Some(2048));
    }

    #[test]
    fn log_params_empty_query() {
        let p = LogParams::from_query("");
        assert_eq!(p.tail_lines, None);
        assert!(!p.follow);
        assert!(!p.timestamps);
        assert_eq!(p.limit_bytes, None);
        assert!(p.since_time.is_none());
    }

    #[test]
    fn log_params_timestamps_and_since_time() {
        let p = LogParams::from_query(
            "timestamps=true&sinceTime=2024-01-01T00%3A00%3A00Z&follow=false",
        );
        assert!(p.timestamps);
        assert!(!p.follow);
        assert!(p.since_time.is_some());
    }

    #[test]
    fn exec_params_parse_command_and_tty() {
        let q = "command=ls&command=-la&tty=true&stdin=false&stdout=true&stderr=true";
        let p = ExecParams::from_query(q);
        assert_eq!(p.command, vec!["ls", "-la"]);
        assert!(p.tty);
        assert!(!p.stdin && p.stdout && p.stderr);
    }

    #[test]
    fn exec_params_empty_query() {
        let p = ExecParams::from_query("");
        assert!(p.command.is_empty());
        assert!(!p.stdin && !p.stdout && !p.stderr && !p.tty);
    }

    #[test]
    fn exec_params_single_command() {
        let p = ExecParams::from_query("command=echo&stdin=1&stdout=1");
        assert_eq!(p.command, vec!["echo"]);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }

    // Regression: a Kubernetes api-server proxies exec/attach to the kubelet
    // with the upstream param names input/output/error/tty (not
    // stdin/stdout/stderr). Before the fix these parsed to all-false streams
    // and the CRI rejected the exec with "one of stdin, stdout, or stderr must
    // be set" — every `kubectl exec` through a vanilla api-server failed.
    #[test]
    fn exec_params_parse_upstream_apiserver_names() {
        // What a vanilla api-server sends to the kubelet (values are "1").
        let q = "command=echo&command=hi&output=1&error=1&tty=1";
        let p = ExecParams::from_query(q);
        assert_eq!(p.command, vec!["echo", "hi"]);
        assert!(p.stdout, "output=1 must map to stdout");
        assert!(p.stderr, "error=1 must map to stderr");
        assert!(p.tty, "tty=1 must map to tty");
        assert!(!p.stdin, "no input param => stdin false");
    }

    #[test]
    fn exec_params_input_maps_to_stdin() {
        let p = ExecParams::from_query("command=sh&input=1&output=1");
        assert!(p.stdin, "input=1 must map to stdin");
        assert!(p.stdout);
    }

    #[test]
    fn exec_params_tty_suppresses_stderr() {
        // When tty=true, the caller sets stdin+stdout; stderr is irrelevant
        // (CRI forbids tty && stderr) but we parse what we get.
        let p = ExecParams::from_query("command=bash&tty=true&stdin=true&stdout=true&stderr=false");
        assert!(p.tty);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }

    // Regression: a `follow=true` log stream MUST end (send EOF) once the
    // container has exited — even though its CRI log file persists on disk.
    // Before the fix the tail loop only stopped when the file *vanished*, so
    // `kubectl logs -f` / hydrophone's completion detector blocked forever on a
    // terminated container, wedging every conformance run.
    #[tokio::test]
    async fn follow_stream_ends_when_container_exits() {
        use futures::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        std::fs::write(
            &path,
            b"2024-01-01T00:00:00.000000000Z stdout F hello\n\
              2024-01-01T00:00:00.000000000Z stdout F world\n",
        )
        .unwrap();

        // Report "running" on the first gone-check, "exited" thereafter.
        let calls = Arc::new(AtomicUsize::new(0));
        let gone = {
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                Box::pin(async move { calls.fetch_add(1, Ordering::SeqCst) >= 1 })
                    as futures::future::BoxFuture<'static, bool>
            }
        };

        let params = LogParams::from_query("follow=true");
        let stream = follow_log_stream(path, params, std::time::Duration::from_millis(10), gone);
        futures::pin_mut!(stream);

        // The fix guarantees termination; without it this times out.
        let collected = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut buf = Vec::new();
            while let Some(item) = stream.next().await {
                buf.extend_from_slice(&item.unwrap());
            }
            buf
        })
        .await
        .expect("follow stream must EOF after the container exits (log-follow-no-eof regression)");

        let text = String::from_utf8_lossy(&collected);
        assert!(
            text.contains("hello") && text.contains("world"),
            "got: {text:?}"
        );
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "exit must be polled while idle"
        );
    }

    // Regression (#1713): a completed (`follow=false`) container-log response
    // must be framed exactly like a vanilla kubelet's — chunked, with the log
    // bytes flushed ahead of the terminating `0\r\n\r\n` — because the
    // api-server's websocket log relay drops any read that returns data and
    // EOF together:
    //
    //   n, err := r.Read(buf)
    //   if err != nil { if err == io.EOF { return nil } ... }  // n bytes lost
    //   if n > 0 { websocket.Message.Send(ws, buf[:n]) }
    //
    // (`apimachinery/pkg/util/httpstream/wsstream.messageCopy`). Go's net/http
    // hands back `(n, io.EOF)` in one call both for a `Content-Length` body
    // (`net/http/transfer.go` `body.readLocked` — "If we can return an EOF here
    // along with the read data, do so") and for a chunked body whose terminator
    // is already buffered. A vanilla kubelet avoids both by writing through
    // `flushwriter.Wrap(response.ResponseWriter)`
    // (`pkg/kubelet/server/server.go` `getContainerLogs`): measured against the
    // vanilla kubelet in the swap harness, `cl=-1`, `read n=117 err=<nil>` then
    // `read n=0 err=EOF`. Before the fix this endpoint sent
    // `content-length: 19`, so the conformance spec "should support retrieving
    // logs from the container over websockets" read an empty log through a
    // vanilla api-server (`kubectl logs` uses `io.Copy` and was unaffected).
    #[tokio::test]
    async fn container_logs_response_is_chunked_and_flushes_before_terminator() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let app = axum::Router::new().route(
            "/containerLogs/ns/pod/main",
            axum::routing::get(|| async {
                container_logs_response(b"container is alive\n".to_vec())
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // The data-before-terminator split is what the client observes. A
        // single-write body makes it unobservable on every attempt, so a
        // handful of attempts separates the two framings without flaking.
        let mut splits = 0;
        for _ in 0..10 {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            sock.write_all(b"GET /containerLogs/ns/pod/main HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            let first = String::from_utf8_lossy(&buf[..n]).to_lowercase();

            assert!(
                first.contains("transfer-encoding: chunked"),
                "log response must be chunked, got:\n{first}"
            );
            assert!(
                !first.contains("content-length:"),
                "log response must not set Content-Length, got:\n{first}"
            );
            assert!(
                first.contains("container is alive"),
                "log body must survive the framing, got:\n{first}"
            );
            if !first.contains("0\r\n\r\n") {
                splits += 1;
            }
        }
        assert!(
            splits > 0,
            "log bytes must be flushed ahead of the chunked terminator; every \
             attempt delivered both in one read"
        );
    }
    /// `kubectl logs --since=1s` sends `sinceSeconds=1`, not `sinceTime`.
    ///
    /// Dropping it silently returned the whole log, which made
    /// `[sig-cli] Kubectl logs logs should be able to retrieve and filter logs`
    /// flaky: it asserts recent(--since=1s) < older(--since=24h) and got
    /// `expected recent(26) to be less than older(26)`. Measured directly
    /// before the fix: 20 lines for --since=1s, --since=24h and unfiltered
    /// alike.
    #[test]
    fn log_params_parse_since_seconds() {
        let p = LogParams::from_query("sinceSeconds=1&timestamps=true");
        assert_eq!(p.since_seconds, Some(1));
        assert!(p.timestamps);
        assert!(p.since_time.is_none());
    }

    #[test]
    fn log_params_ignore_a_nonpositive_or_unparsable_since_seconds() {
        assert_eq!(LogParams::from_query("sinceSeconds=0").since_seconds, None);
        assert_eq!(LogParams::from_query("sinceSeconds=-5").since_seconds, None);
        assert_eq!(
            LogParams::from_query("sinceSeconds=abc").since_seconds,
            None
        );
        assert_eq!(LogParams::from_query("sinceSeconds=").since_seconds, None);
    }

    /// sinceSeconds becomes an absolute cutoff near "now minus N".
    #[test]
    fn since_seconds_resolves_to_a_recent_cutoff() {
        let before = chrono::Utc::now().timestamp();
        let opts = log_read_options(&LogParams::from_query("sinceSeconds=60"));
        let after = chrono::Utc::now().timestamp();

        let cutoff = opts.since_unix.expect("sinceSeconds must produce a cutoff");
        assert!(
            cutoff >= before - 61 && cutoff <= after - 59,
            "cutoff {cutoff} should be ~60s before now ({before}..{after})"
        );
    }

    /// Upstream forbids sending both; if both arrive anyway, the explicit
    /// timestamp wins over the relative one.
    #[test]
    fn since_time_takes_precedence_over_since_seconds() {
        let opts = log_read_options(&LogParams::from_query(
            "sinceTime=2020-01-01T00:00:00Z&sinceSeconds=60",
        ));
        assert_eq!(
            opts.since_unix,
            Some(1_577_836_800),
            "sinceTime (2020-01-01T00:00:00Z) must win"
        );
    }

    /// No since* params means no cutoff — the whole log, as before.
    #[test]
    fn no_since_params_means_no_cutoff() {
        let opts = log_read_options(&LogParams::from_query("tailLines=5"));
        assert!(opts.since_unix.is_none());
        assert_eq!(opts.tail_lines, Some(5));
    }
}

mod pod_log_query_bool_tests {
    use rusternetes_kubelet::streaming_server::is_true;

    /// PodLogOptions follow/previous/timestamps/insecureSkipTLSVerifyBackend
    /// are decoded upstream with `runtime.Convert_Slice_string_To_bool`, so
    /// only absence, "0" and a case-insensitive "false" are false. The old
    /// helper compared against "true"/"1" only, so `?follow=t` silently did
    /// not follow.
    #[test]
    fn log_flags_follow_upstream_query_conversion() {
        for v in ["true", "1", "t", "T", "TRUE", "yes", ""] {
            assert!(is_true(v), "{v:?} must be true");
        }
        for v in ["0", "false", "False", "FALSE"] {
            assert!(!is_true(v), "{v:?} must be false");
        }
    }
}
