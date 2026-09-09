//! A kubelet error on `pods/log` must reach the client as a `Status`, not as
//! the kubelet's raw text.
//!
//! Upstream never streams a kubelet error response through. `LocationStreamer`
//! runs a `ResponseChecker` before it hands the body to the client
//! (`staging/src/k8s.io/apiserver/pkg/registry/generic/rest/streamer.go:100-105`),
//! and pods/log installs `NewGenericHttpResponseChecker(api.Resource("pods/log"), name)`
//! (`pkg/registry/core/pod/rest/log.go:111`). That checker converts anything
//! outside 200..=206 into a typed API error
//! (`generic/rest/response_checker.go:46-67`):
//!
//! ```go
//! bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxReadLength))  // 50000
//! switch {
//! case resp.StatusCode == http.StatusInternalServerError:
//!     return errors.NewInternalError(fmt.Errorf("%s", bodyText))
//! case resp.StatusCode == http.StatusBadRequest:
//!     return errors.NewBadRequest(bodyText)
//! case resp.StatusCode == http.StatusNotFound:
//!     return errors.NewGenericServerResponse(resp.StatusCode, "", ..., bodyText, 0, false)
//! }
//! return errors.NewGenericServerResponse(resp.StatusCode, "", ..., bodyText, 0, false)
//! ```
//!
//! Rusternetes proxied the kubelet response verbatim, so a client got a bare
//! 400 with `text/plain`. client-go cannot parse that as a `Status`, falls back
//! to `NewGenericServerResponse`, and prints its generic 400 wording — losing
//! the kubelet's actual explanation:
//!
//! ```text
//! Unable to fetch gc-1009/pod1/nginx logs: the server rejected our request
//! for an unknown reason (get pods pod1)
//! ```
//!
//! ("for an unknown reason" is `NewGenericServerResponse`'s
//! `http.StatusBadRequest` arm, `apimachinery/pkg/api/errors/errors.go:451-453`
//! — reached only when the body did not decode as a `Status`.)
//!
//! That is #1920, seen in the api-server vanilla-swap run 34361805618 where the
//! e2e framework's post-failure log collection came back empty for exactly the
//! two pods whose containers had never started.

use rusternetes_common::resources::{
    DaemonEndpoint, Node, NodeAddress, NodeDaemonEndpoints, NodeStatus, Pod, PodSpec,
};
use rusternetes_common::tls::TlsConfig;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Spawn an HTTPS/1.1 stand-in for the kubelet that answers every request with
/// `status` and `body`. The kubelet serves `:10250` over TLS, so this must
/// present a serving cert for the proxy's handshake to complete.
async fn spawn_kubelet(
    status_line: &'static str,
    body: String,
    max_requests: usize,
) -> (u16, tokio::task::JoinHandle<()>) {
    let tlsc = TlsConfig::generate_self_signed("127.0.0.1", vec!["127.0.0.1".to_string()])
        .expect("generate self-signed serving cert");
    let mut server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(tlsc.cert, tlsc.key)
        .expect("build mock kubelet server config");
    server_cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        for _ in 0..max_requests {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(sock).await else {
                    return;
                };
                let mut buf = vec![0u8; 4096];
                let _ = tls.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status_line,
                    body.len(),
                    body
                );
                let _ = tls.write_all(response.as_bytes()).await;
                let _ = tls.flush().await;
                let _ = tls.shutdown().await;
            });
        }
    });
    (port, handle)
}

fn node_on_loopback(name: &str, port: i32) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name),
        spec: None,
        status: Some(NodeStatus {
            addresses: Some(vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "127.0.0.1".to_string(),
            }]),
            daemon_endpoints: Some(NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint { port }),
            }),
            ..NodeStatus::default()
        }),
    }
}

/// Seed a node whose kubelet is the stub, plus a pod scheduled onto it, then
/// `GET .../pods/{name}/log` and return `(status, body)`.
async fn fetch_logs(status_line: &'static str, kubelet_body: &str) -> (u16, String) {
    let (port, _handle) = spawn_kubelet(status_line, kubelet_body.to_string(), 4).await;

    let api = TestApiServer::new();
    api.storage
        .create(
            &build_key("nodes", None, "log-node"),
            &node_on_loopback("log-node", port as i32),
        )
        .await
        .expect("create node");

    let mut pod = Pod::new(
        "log-pod",
        PodSpec {
            containers: vec![rusternetes_common::resources::Container {
                name: "nginx".to_string(),
                image: "nginx".to_string(),
                ..Default::default()
            }],
            node_name: Some("log-node".to_string()),
            ..Default::default()
        },
    );
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    api.storage
        .create(&build_key("pods", Some("default"), "log-pod"), &pod)
        .await
        .expect("create pod");

    let (status, _headers, body, _) = api
        .send_full(
            "GET",
            "/api/v1/namespaces/default/pods/log-pod/log?container=nginx",
            None,
            None,
            None,
        )
        .await;
    (status.as_u16(), String::from_utf8_lossy(&body).to_string())
}

/// The exact shape from #1920: the kubelet refuses because the container never
/// started. The client must get a decodable `Status` carrying that sentence.
#[tokio::test]
async fn a_kubelet_400_on_pod_logs_becomes_a_status_with_the_kubelet_message() {
    let kubelet_text =
        "container \"nginx\" in pod \"log-pod\" is waiting to start: ContainerCreating";
    let (status, body) = fetch_logs("400 Bad Request", kubelet_text).await;

    assert_eq!(status, 400, "status code must be preserved: {body}");

    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_else(|e| {
        panic!(
            "pods/log error body is not JSON ({e}), so every client reports \
             \"the server rejected our request for an unknown reason\" and the \
             kubelet's explanation is lost. Upstream converts it with \
             NewBadRequest (generic/rest/response_checker.go:57). Body: {body}"
        )
    });
    assert_eq!(parsed["kind"], "Status", "not a Status: {body}");
    assert_eq!(parsed["status"], "Failure", "{body}");
    assert_eq!(parsed["reason"], "BadRequest", "{body}");
    assert_eq!(
        parsed["code"], 400,
        "Status.code must match the HTTP status: {body}"
    );
    assert!(
        parsed["message"]
            .as_str()
            .is_some_and(|m| m.contains("is waiting to start: ContainerCreating")),
        "the kubelet's explanation must survive into Status.message: {body}"
    );
}

/// 500 takes upstream's `NewInternalError` arm, which is a different reason.
#[tokio::test]
async fn a_kubelet_500_on_pod_logs_becomes_an_internal_error_status() {
    let (status, body) = fetch_logs("500 Internal Server Error", "kubelet exploded").await;

    assert_eq!(status, 500, "{body}");
    let parsed: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"));
    assert_eq!(parsed["kind"], "Status", "{body}");
    assert_eq!(
        parsed["reason"], "InternalError",
        "upstream uses NewInternalError for 500 \
         (generic/rest/response_checker.go:55): {body}"
    );
    assert!(
        parsed["message"]
            .as_str()
            .is_some_and(|m| m.contains("kubelet exploded")),
        "{body}"
    );
}

/// Every other code goes through `NewGenericServerResponse`, which maps the
/// code to a reason (`apimachinery/pkg/api/errors/errors.go:437-524`).
///
/// Two things upstream does here look wrong and are not:
///
/// 1. The kubelet's own text is **dropped**. `NewGenericServerResponse` only
///    keeps `serverMessage` for 403/406/415 and 5xx, and the checker passes
///    `isUnexpectedResponse: false`, so no `causes` entry carries it either
///    (`errors.go:500-509`). Only the 400 and 500 arms of the checker preserve
///    the backend's explanation.
/// 2. The message has a **double space** after the `(`. The checker passes an
///    empty verb, and the suffix is
///    `fmt.Sprintf("%s (%s %s %s)", message, strings.ToLower(verb), qualifiedResource, name)`
///    (`errors.go:496`). Pinned verbatim — error wording is contract, and a
///    tidied-up version would be a divergence.
#[tokio::test]
async fn a_kubelet_404_on_pod_logs_becomes_a_notfound_status() {
    let (status, body) = fetch_logs("404 Not Found", "pod not found on this node").await;

    assert_eq!(status, 404, "{body}");
    let parsed: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"));
    assert_eq!(parsed["kind"], "Status", "{body}");
    assert_eq!(parsed["reason"], "NotFound", "{body}");
    assert_eq!(parsed["code"], 404, "{body}");
    assert_eq!(
        parsed["message"], "the server could not find the requested resource ( pods/log log-pod)",
        "{body}"
    );
    assert_eq!(parsed["details"]["kind"], "pods/log", "{body}");
    assert_eq!(parsed["details"]["name"], "log-pod", "{body}");
}

/// The success path must stay a raw byte stream: the checker only fires
/// outside 200..=206, and log output is not JSON.
#[tokio::test]
async fn a_successful_log_fetch_is_still_streamed_verbatim() {
    let (status, body) = fetch_logs("200 OK", "hello from the container\nsecond line\n").await;

    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body, "hello from the container\nsecond line\n",
        "log bytes must pass through untouched -- wrapping them in a Status \
         would break every `kubectl logs`"
    );
}
