//! Pod subresource handlers
//!
//! Implements pod subresources required for Kubernetes conformance:
//! - /logs - Get container logs
//! - /exec - Execute commands in containers (proxied to kubelet)
//! - /attach - Attach to running containers (proxied to kubelet)
//! - /portforward - Forward ports to pods (SPDY and WebSocket)

use crate::{
    handlers::node_conn::{node_conn, NodeConn},
    middleware::AuthContext,
    spdy, spdy_handlers,
    state::ApiServerState,
    streaming,
};
use axum::{
    body::Body,
    extract::{ws::WebSocketUpgrade, Path, Query, Request, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Error, Result,
};
use rusternetes_storage::Storage;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info};

/// Simple percent-decoding for URL query parameters
fn percent_decode_str(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(s) = std::str::from_utf8(&hex) {
                if let Ok(val) = u8::from_str_radix(s, 16) {
                    result.push(val as char);
                    continue;
                }
            }
            result.push('%');
            result.push(hi as char);
            result.push(lo as char);
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

#[derive(Debug, Deserialize)]
pub struct ExecQuery {
    /// Container in which to execute the command
    pub container: Option<String>,
    /// Command to execute
    pub command: Vec<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

/// Decode the exec/attach query string into [`ExecQuery`].
///
/// Extracted from the handler so the boolean decoding is unit-testable. The
/// stream flags go through `k8s_query_bool` because upstream decodes them with
/// `runtime.Convert_Slice_string_To_bool`
/// (`apimachinery/pkg/runtime/conversion.go:79-95`): only absence, `"0"` and a
/// case-insensitive `"false"` are false, so `?stdin=t`, `?stdin=yes` and even
/// `?stdin=` all mean true. The previous `value == "true" || value == "1"`
/// comparison silently dropped every other spelling, which for `tty` means a
/// client asking for a TTY quietly not getting one.
pub(crate) fn parse_exec_query(raw_query: &str) -> ExecQuery {
    let mut command = Vec::new();
    let mut container = None;
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    let mut tty = false;
    for pair in raw_query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let value = percent_decode_str(value);
            match key {
                "command" => command.push(value),
                "container" => container = Some(value),
                "stdin" => stdin = rusternetes_common::query::k8s_query_bool(&value),
                "stdout" => stdout = rusternetes_common::query::k8s_query_bool(&value),
                "stderr" => stderr = rusternetes_common::query::k8s_query_bool(&value),
                "tty" => tty = rusternetes_common::query::k8s_query_bool(&value),
                _ => {}
            }
        }
    }
    ExecQuery {
        container,
        command,
        stdin,
        stdout,
        stderr,
        tty,
    }
}

#[derive(Debug, Deserialize)]
pub struct AttachQuery {
    /// Container to attach to
    pub container: Option<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

#[derive(Debug, Deserialize)]
pub struct PortForwardQuery {
    /// Ports to forward
    pub ports: Option<String>,
}

/// GET /api/v1/namespaces/{namespace}/pods/{name}/log
///
/// Proxies log retrieval to the pod's kubelet using a plain HTTP reverse proxy.
/// Mirrors `pkg/registry/core/pod/rest/log.go` (LogREST) + upstream's
/// `streamLocation` — the kubelet exposes `/containerLogs/{ns}/{pod}/{ctr}?<params>`
/// and handles all follow/tailLines/limitBytes logic server-side.
/// Maximum bytes read from a kubelet error response body — upstream
/// `maxReadLength` (staging/src/k8s.io/apiserver/pkg/registry/generic/rest/response_checker.go:37).
const KUBELET_ERROR_MAX_READ: usize = 50_000;

/// Upstream `GenericHttpResponseChecker.Check`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/rest/response_checker.go:46-67),
/// installed on `pods/log` by `pkg/registry/core/pod/rest/log.go:111` as
/// `NewGenericHttpResponseChecker(api.Resource("pods/log"), name)`.
///
/// The api-server never streams a kubelet error response through to the
/// client: `LocationStreamer` runs this check before it hands over the body
/// (`generic/rest/streamer.go:100-105`) and converts anything outside
/// `200..=206` into a typed API error, so the client always gets a `Status`.
///
/// We used to proxy the kubelet's response verbatim, which meant a plain-text
/// `400` body. client-go cannot decode that as a `Status`, falls back to
/// `NewGenericServerResponse`, and prints its canned 400 wording — so the
/// kubelet's actual explanation was lost:
///
/// ```text
/// Unable to fetch gc-1009/pod1/nginx logs: the server rejected our request
/// for an unknown reason (get pods pod1)
/// ```
///
/// That cost the e2e framework its container logs on exactly the runs where a
/// spec had failed (#1920).
async fn check_kubelet_log_response(resp: Response, pod_name: &str) -> Response {
    let code = resp.status();
    // Upstream: `resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent`.
    if (StatusCode::OK..=StatusCode::PARTIAL_CONTENT).contains(&code) {
        return resp;
    }

    let (_parts, body) = resp.into_parts();
    let text = read_limited(body, KUBELET_ERROR_MAX_READ).await;
    kubelet_error_response(code, text, pod_name)
}

/// The checker's three arms, then `NewGenericServerResponse` for everything
/// else. Shared by both log paths: the plain proxy and the websocket fetch
/// below, which upstream also covers because its websocket reader consumes the
/// very stream `InputStream` returns — after the checker has run.
fn kubelet_error_response(code: StatusCode, text: String, pod_name: &str) -> Response {
    match code {
        StatusCode::BAD_REQUEST => Error::BadRequest(text).into_response(),
        // `errors.NewInternalError(fmt.Errorf("%s", bodyText))`, whose message
        // is `fmt.Sprintf("Internal error occurred: %v", err)`
        // (apimachinery/pkg/api/errors/errors.go:387-397).
        StatusCode::INTERNAL_SERVER_ERROR => {
            Error::Internal(format!("Internal error occurred: {text}")).into_response()
        }
        _ => generic_server_response(code, pod_name),
    }
}

/// `errors.NewGenericServerResponse(code, "", api.Resource("pods/log"), name, bodyText, 0, false)`
/// (apimachinery/pkg/api/errors/errors.go:437-524).
///
/// Note what upstream does **not** do here: with `isUnexpectedResponse: false`
/// the kubelet's `bodyText` is dropped entirely for these codes, and the
/// message is the canned one for the status code with the qualified resource
/// appended. The empty verb is what leaves a double space after the `(` —
/// `fmt.Sprintf("%s (%s %s %s)", message, strings.ToLower(verb), ...)` with
/// `verb == ""`. Reproduced exactly rather than tidied: the wording is what
/// clients match on.
fn generic_server_response(code: StatusCode, pod_name: &str) -> Response {
    let (reason, message) = match code {
        StatusCode::CONFLICT => ("Conflict", "the server reported a conflict".to_string()),
        StatusCode::NOT_FOUND => (
            "NotFound",
            "the server could not find the requested resource".to_string(),
        ),
        StatusCode::UNAUTHORIZED => (
            "Unauthorized",
            "the server has asked for the client to provide credentials".to_string(),
        ),
        StatusCode::FORBIDDEN => ("Forbidden", String::new()),
        StatusCode::METHOD_NOT_ALLOWED => (
            "MethodNotAllowed",
            "the server does not allow this method on the requested resource".to_string(),
        ),
        StatusCode::UNPROCESSABLE_ENTITY => (
            "Invalid",
            "the server rejected our request due to an error in our request".to_string(),
        ),
        StatusCode::SERVICE_UNAVAILABLE => (
            "ServiceUnavailable",
            "the server is currently unable to handle the request".to_string(),
        ),
        StatusCode::GATEWAY_TIMEOUT => (
            "Timeout",
            "the server was unable to return a response in the time allotted, but may still be processing the request".to_string(),
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            "TooManyRequests",
            "the server has received too many requests and has asked us to try again later"
                .to_string(),
        ),
        _ => (
            "Unknown",
            format!(
                "the server responded with the status code {} but did not return more information",
                code.as_u16()
            ),
        ),
    };

    let message = format!("{message} ( pods/log {pod_name})");
    let details = rusternetes_common::types::StatusDetails {
        name: Some(pod_name.to_string()),
        group: None,
        kind: Some("pods/log".to_string()),
        uid: None,
        causes: None,
        retry_after_seconds: None,
    };
    (
        code,
        axum::Json(rusternetes_common::types::Status::failure_with_details(
            message,
            reason,
            code.as_u16(),
            details,
        )),
    )
        .into_response()
}

/// `io.ReadAll(io.LimitReader(resp.Body, maxReadLength))` — truncate rather
/// than fail, which is what `LimitReader` does. `axum::body::to_bytes` errors
/// out on an oversized body instead, and an error here would replace the
/// kubelet's explanation with a message about the explanation.
async fn read_limited(body: Body, limit: usize) -> String {
    use http_body_util::BodyExt;
    let mut body = body;
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    let take = (limit - buf.len()).min(data.len());
                    buf.extend_from_slice(&data[..take]);
                }
            }
            Some(Err(_)) | None => break,
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

pub async fn get_logs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    debug!("Getting logs for pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("log");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Determine the container from the query string before consuming `req`.
    let raw_query = req.uri().query().unwrap_or("").to_string();
    let container_name: Option<String> = raw_query.split('&').find_map(|pair| {
        pair.split_once('=')
            .filter(|(k, _)| *k == "container")
            .map(|(_, v)| v.to_string())
    });

    // Load the pod to verify it exists and resolve container + node.
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Resolve container name (default to first container).
    let container = if let Some(c) = container_name {
        c
    } else {
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Require spec.nodeName — upstream returns 400 when the pod is unscheduled.
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::InvalidResource(format!(
                "pod {}/{} has not been assigned to a node",
                namespace, name
            ))
        })?
        .to_string();

    // Resolve kubelet connection parameters from the node.
    let node_key = rusternetes_storage::build_key("nodes", None::<&str>, &node_name);
    let node: rusternetes_common::resources::Node = state.storage.get(&node_key).await?;
    let conn = node_conn(&node, None)?;

    // Build the upstream-faithful kubelet containerLogs URL.
    let target_url = build_kubelet_stream_url(
        &conn,
        "containerLogs",
        &namespace,
        &name,
        &container,
        &raw_query,
    );
    info!(
        "Proxying logs {}/{} to kubelet: {}",
        namespace, name, target_url
    );

    // WebSocket path: the e2e conformance client opens a websocket to the /log
    // subresource (subprotocol "binary.k8s.io") and reads the log bytes back as
    // websocket messages. Upstream serves this via
    // `responsewriters.StreamObject` -> `wsstream.NewReader(out, true, ...)`
    // (staging/.../endpoints/handlers/responsewriters/writers.go:65-66): the
    // api-server terminates the websocket and reuses the SAME plain-HTTP kubelet
    // log fetch, framing each chunk per the single-stream wsstream protocol
    // (no channel byte).
    if let Some(ws) = ws {
        // Decide the framing from the negotiated subprotocol BEFORE consuming
        // the request. Mirrors upstream `handshake` (wsstream/conn.go:142):
        // "base64.binary.k8s.io" base64-encodes; everything else is raw binary.
        let requested_protocol = req
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let base64 = streaming::log_protocol_is_base64(requested_protocol.as_deref());

        // Fetch the kubelet log stream over plain HTTP (same target URL the
        // non-upgrade path proxies). We build a fresh GET so the websocket
        // upgrade headers are not forwarded to the kubelet.
        let stream = match fetch_kubelet_log_stream(&target_url, &name).await? {
            Ok(stream) => stream,
            // The kubelet refused: answer the HTTP request with a Status
            // instead of upgrading and framing the error text as log output.
            Err(resp) => return Ok(resp),
        };

        return Ok(ws
            .protocols(streaming::LOG_WS_PROTOCOLS)
            .on_upgrade(move |socket| streaming::handle_logs_websocket(socket, stream, base64))
            .into_response());
    }

    // Plain HTTP proxy (non-upgrade) — handles both follow and non-follow since
    // the kubelet log endpoint is plain HTTP (no WebSocket / SPDY upgrade).
    // A kubelet error is converted to a Status before anything is streamed,
    // exactly as upstream's ResponseChecker does (#1920).
    let proxied = rusternetes_streamproxy::proxy_stream(target_url, req).await;
    Ok(check_kubelet_log_response(proxied, &name).await)
}

/// Fetch the kubelet `containerLogs` stream over plain HTTP and expose it as a
/// byte stream for pumping over a websocket. Used only by the websocket log
/// path; the non-upgrade path uses `rusternetes_streamproxy::proxy_stream`.
///
/// The kubelet handles all `follow`/`tailLines`/`limitBytes`/`sinceSeconds`
/// query parameters server-side — they are already baked into `target_url`.
type LogByteStream = std::pin::Pin<
    Box<
        dyn futures::Stream<Item = std::result::Result<axum::body::Bytes, reqwest::Error>>
            + Send
            + 'static,
    >,
>;

/// Load the api-server's kubelet client identity (kubeadm
/// `apiserver-kubelet-client` cert+key) as a reqwest [`Identity`], if present.
/// Mirrors [`rusternetes_streamproxy::tls`]'s connector loading — same paths,
/// same trust model — but for the reqwest client used by the websocket log
/// fetch. Returns `None` when the files are absent (rusternetes' own cluster).
fn load_kubelet_client_identity() -> Option<reqwest::Identity> {
    const CERT: &str = "/etc/kubernetes/pki/apiserver-kubelet-client.crt";
    const KEY: &str = "/etc/kubernetes/pki/apiserver-kubelet-client.key";
    let mut pem = std::fs::read(CERT).ok()?;
    pem.push(b'\n');
    pem.extend_from_slice(&std::fs::read(KEY).ok()?);
    match reqwest::Identity::from_pem(&pem) {
        Ok(id) => Some(id),
        Err(e) => {
            tracing::warn!("failed to build kubelet client identity: {e}");
            None
        }
    }
}

/// Fetch the kubelet log stream, or the `Response` that must be returned
/// instead when the kubelet refused.
///
/// The websocket path needs the same check as the plain proxy: without it the
/// kubelet's error text is framed into the websocket as if it were log output,
/// which is worse than the plain path's bad content type — the client cannot
/// tell an error from a log line at all.
async fn fetch_kubelet_log_stream(
    target_url: &Uri,
    pod_name: &str,
) -> Result<std::result::Result<LogByteStream, Response>> {
    // `danger_accept_invalid_certs` mirrors the api-server->kubelet trust model
    // used by the streaming proxy (the kubelet serves a self-signed cert).
    let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(true);
    // Present the kubeadm `apiserver-kubelet-client` cert when it exists, so a
    // vanilla kubelet (`--anonymous-auth=false`) authenticates the api-server's
    // websocket log fetch instead of returning 401 (#1670). Absent on
    // rusternetes' own cluster, whose kubelet needs no client cert.
    if let Some(identity) = load_kubelet_client_identity() {
        builder = builder.identity(identity);
    }
    let client = builder
        .build()
        .map_err(|e| Error::Internal(format!("failed to build kubelet log client: {e}")))?;

    let mut resp = client
        .get(target_url.to_string())
        .send()
        .await
        .map_err(|e| Error::Internal(format!("kubelet log request failed: {e}")))?;

    let code = resp.status();
    if !(StatusCode::OK..=StatusCode::PARTIAL_CONTENT).contains(&code) {
        // `io.ReadAll(io.LimitReader(resp.Body, maxReadLength))`, chunk by
        // chunk so an oversized body is truncated rather than rejected.
        let mut buf: Vec<u8> = Vec::new();
        while buf.len() < KUBELET_ERROR_MAX_READ {
            match resp.chunk().await {
                Ok(Some(data)) => {
                    let take = (KUBELET_ERROR_MAX_READ - buf.len()).min(data.len());
                    buf.extend_from_slice(&data[..take]);
                }
                Ok(None) | Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&buf).into_owned();
        return Ok(Err(kubelet_error_response(code, text, pod_name)));
    }

    Ok(Ok(Box::pin(resp.bytes_stream())))
}

/// Build the kubelet stream URL for an exec or attach subresource.
///
/// Upstream: `pkg/registry/core/pod/strategy.go::streamLocation` builds
/// `/{kind}/{ns}/{pod}/{container}?<raw_query>` on the pod's node and
/// upgrade-proxies.  We replicate that exact shape here.
pub fn build_kubelet_stream_url(
    conn: &NodeConn,
    kind: &str,
    ns: &str,
    pod: &str,
    container: &str,
    raw_query: &str,
) -> Uri {
    let path = format!("/{kind}/{ns}/{pod}/{container}");
    let uri_str = if raw_query.is_empty() {
        format!("{}://{}:{}{}", conn.scheme, conn.host, conn.port, path)
    } else {
        format!(
            "{}://{}:{}{}?{}",
            conn.scheme, conn.host, conn.port, path, raw_query
        )
    };
    uri_str.parse().expect("kubelet stream URL is always valid")
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/exec
///
/// Proxies exec to the pod's kubelet using an upgrade-aware reverse proxy.
/// Mirrors `pkg/registry/core/pod/strategy.go::streamLocation`.
pub async fn exec(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    req: Request,
) -> Result<Response> {
    let raw_query = req.uri().query().unwrap_or("").to_string();

    // Parse query params to build webhook admission object (command/container).
    let query = parse_exec_query(&raw_query);

    info!("Exec {}/{}: cmd={:?}", namespace, name, query.command);

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("exec");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Run admission webhooks for Connect operation (exec)
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodExecOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/exec".to_string(),
        };
        let exec_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodExecOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or(""),
            "command": query.command
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(exec_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // Fetch the pod and require spec.nodeName (upstream: 400 if unset).
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::InvalidResource(format!(
                "pod {}/{} has not been assigned to a node",
                namespace, name
            ))
        })?
        .to_string();

    // Resolve the container name (default to first container).
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Fetch the node and resolve kubelet connection parameters.
    let node_key = rusternetes_storage::build_key("nodes", None::<&str>, &node_name);
    let node: rusternetes_common::resources::Node = state.storage.get(&node_key).await?;
    let conn = node_conn(&node, None)?;

    // Build the upstream-faithful kubelet exec URL.
    let target_url = build_kubelet_stream_url(
        &conn,
        "exec",
        &namespace,
        &name,
        &container_name,
        &raw_query,
    );
    info!(
        "Proxying exec {}/{} to kubelet: {}",
        namespace, name, target_url
    );

    // Upgrade-proxy the request to the kubelet (handles SPDY, WebSocket, and plain HTTP).
    // The WebSocket upgrade (if any) is handled transparently by proxy_upgrade via the
    // raw hyper OnUpgrade future; we don't need to handle it separately.
    Ok(rusternetes_streamproxy::proxy_upgrade(target_url, req).await)
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/attach
///
/// Proxies attach to the pod's kubelet using an upgrade-aware reverse proxy.
/// Mirrors `pkg/registry/core/pod/strategy.go::streamLocation`.
pub async fn attach(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<AttachQuery>,
    req: Request,
) -> Result<Response> {
    info!("Attaching to pod {}/{}", namespace, name);

    let raw_query = req.uri().query().unwrap_or("").to_string();

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("attach");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Run admission webhooks for Connect operation (attach)
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodAttachOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/attach".to_string(),
        };
        let attach_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodAttachOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or("")
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(attach_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // Fetch the pod and require spec.nodeName (upstream: 400 if unset).
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::InvalidResource(format!(
                "pod {}/{} has not been assigned to a node",
                namespace, name
            ))
        })?
        .to_string();

    // Resolve the container name (default to first container).
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Fetch the node and resolve kubelet connection parameters.
    let node_key = rusternetes_storage::build_key("nodes", None::<&str>, &node_name);
    let node: rusternetes_common::resources::Node = state.storage.get(&node_key).await?;
    let conn = node_conn(&node, None)?;

    // Build the upstream-faithful kubelet attach URL.
    let target_url = build_kubelet_stream_url(
        &conn,
        "attach",
        &namespace,
        &name,
        &container_name,
        &raw_query,
    );
    info!(
        "Proxying attach {}/{} to kubelet: {}",
        namespace, name, target_url
    );

    // Upgrade-proxy the request to the kubelet (handles SPDY, WebSocket, and plain HTTP).
    // Suppress the unused `ws` warning — the upgrade is handled transparently by
    // proxy_upgrade via the raw hyper OnUpgrade future.
    Ok(rusternetes_streamproxy::proxy_upgrade(target_url, req).await)
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/portforward
/// Forward ports to a pod (supports both SPDY and WebSocket)
pub async fn portforward(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<PortForwardQuery>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    info!("Port forwarding to pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("portforward");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Parse ports from query parameter
    let ports: Vec<u16> = if let Some(ref ports_str) = query.ports {
        ports_str
            .split(',')
            .filter_map(|p| p.trim().parse().ok())
            .collect()
    } else {
        vec![]
    };

    if ports.is_empty() {
        return Err(Error::InvalidResource(
            "No ports specified for port forwarding".to_string(),
        ));
    }

    // Check if this is a SPDY upgrade request (kubectl uses SPDY)
    if spdy::is_spdy_request(&req) {
        info!(
            "Upgrading port-forward to SPDY for pod {}/{}, ports: {:?} (kubectl compatibility)",
            namespace, name, ports
        );

        // Create SPDY upgrade response
        let response = spdy::create_spdy_upgrade_response().map_err(|e| {
            Error::Internal(format!("Failed to create SPDY upgrade response: {}", e))
        })?;

        // Spawn task to handle SPDY connection after upgrade
        tokio::spawn(async move {
            match spdy::upgrade_to_spdy(req).await {
                Ok(spdy_conn) => {
                    spdy_handlers::handle_spdy_portforward(spdy_conn, pod, ports).await;
                }
                Err(e) => {
                    tracing::error!("Failed to upgrade to SPDY: {}", e);
                }
            }
        });

        return Ok(response.into_response());
    }

    // Handle WebSocket upgrade if requested
    if let Some(ws) = ws {
        info!(
            "Upgrading port-forward to WebSocket for pod {}/{}, ports: {:?}",
            namespace, name, ports
        );
        Ok(ws
            .on_upgrade(move |socket| streaming::handle_portforward_websocket(socket, pod, ports))
            .into_response())
    } else {
        // No upgrade requested - return error
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "text/plain")
            .body(Body::from(
                "Port forward requires protocol upgrade (SPDY or WebSocket). Use:\n\
                - kubectl (uses SPDY automatically)\n\
                - WebSocket protocol for custom clients\n",
            ))
            .unwrap())
    }
}

/// Feature name kubelets advertise in `node.status.declaredFeatures` once they
/// support the CPU-resize path for Guaranteed-QoS pods (KEP-5328 — KEP-1287
/// integration). The api-server uses this constant when deciding whether to
/// admit a `/pods/{name}/resize` request that mutates a Guaranteed pod's CPU.
pub(crate) const GUARANTEED_QOS_POD_CPU_RESIZE: &str = "GuaranteedQoSPodCPUResize";

/// Returns `true` if `pod` is Guaranteed-QoS.
///
/// Delegates to [`rusternetes_common::qos::get_pod_qos`], the port of
/// `pkg/apis/core/v1/helper/qos/qos.go::GetPodQOS`. Upstream's gate reads the
/// **published** class off the pod being resized
/// (`guaranteed_cpu_resize.go:64`, `oldPodInfo.Status.QOSClass !=
/// v1.PodQOSGuaranteed`), which is what `get_pod_qos` prefers before falling
/// back to computing.
///
/// The partial Guaranteed-only check this used to carry — `spec.containers`
/// only, quantities compared as strings — is gone: a pod whose init container
/// declares no limits is `Burstable`, so the CPU-resize gate must not fire for
/// it.
fn is_guaranteed_qos(pod: &rusternetes_common::resources::Pod) -> bool {
    rusternetes_common::qos::get_pod_qos(pod) == rusternetes_common::qos::QoSClass::Guaranteed
}

/// Returns `true` if the CPU resource request OR limit differs between any
/// matching container in `desired` vs `current`. The match is by container
/// name; missing-on-one-side counts as a change.
fn resize_changes_cpu(
    current: &rusternetes_common::resources::Pod,
    desired: &rusternetes_common::resources::Pod,
) -> bool {
    let (Some(cur_spec), Some(des_spec)) = (current.spec.as_ref(), desired.spec.as_ref()) else {
        return false;
    };
    for desired_c in &des_spec.containers {
        let Some(current_c) = cur_spec
            .containers
            .iter()
            .find(|c| c.name == desired_c.name)
        else {
            continue;
        };
        let cur_cpu_req = current_c
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("cpu"));
        let des_cpu_req = desired_c
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("cpu"));
        let cur_cpu_lim = current_c
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .and_then(|m| m.get("cpu"));
        let des_cpu_lim = desired_c
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .and_then(|m| m.get("cpu"));
        if cur_cpu_req != des_cpu_req || cur_cpu_lim != des_cpu_lim {
            return true;
        }
    }
    false
}

/// Node-declared-feature admission for `/pods/{name}/resize` (KEP-5328 +
/// KEP-1287). When both `NodeDeclaredFeatures` and `InPlacePodVerticalScaling`
/// are enabled, a CPU resize against a Guaranteed-QoS pod is admitted only if
/// the target node has advertised `GuaranteedQoSPodCPUResize` in
/// `status.declaredFeatures`.
///
/// Returns:
///   - `Ok(())` when the gate combination does not apply (either gate off, or
///     the change does not touch CPU, or the pod is not Guaranteed), when the
///     pod is not yet bound to a node, when the node does not exist (the
///     resize is allowed in that case — upstream defers to scheduler), or
///     when the node *has* declared the feature.
///   - `Err(Error::Forbidden(_))` when the target node exists, both gates are
///     on, the pod is Guaranteed, the resize touches CPU, and the node has
///     not declared `GuaranteedQoSPodCPUResize`.
///
/// Upstream: `plugin/pkg/admission/noderestriction/admission.go` consults
/// `nodeFeatures` when admitting `/pods/{name}/resize`; missing
/// `GuaranteedQoSPodCPUResize` produces:
///   "spec.containers[*].resources: Forbidden: node \"<n>\" has not declared
///    feature \"GuaranteedQoSPodCPUResize\""
pub(crate) async fn check_node_declared_features_for_resize<S: Storage>(
    storage: &S,
    current: &rusternetes_common::resources::Pod,
    desired: &rusternetes_common::resources::Pod,
) -> Result<()> {
    use rusternetes_common::feature_gates::{enabled, Feature};

    // Both gates must be on. Mirrors upstream's
    // `if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures)
    //   && utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling)`
    // in the resize strategy.
    if !enabled(Feature::NodeDeclaredFeatures) || !enabled(Feature::InPlacePodVerticalScaling) {
        return Ok(());
    }

    // The check only fires when the change actually touches CPU on a
    // Guaranteed pod — label-only resizes (or memory-only) sail through.
    if !is_guaranteed_qos(current) || !resize_changes_cpu(current, desired) {
        return Ok(());
    }

    // Pod must already be bound to a node — there is nothing to consult
    // otherwise. Upstream returns nil in that case.
    let Some(node_name) = current
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_ref())
        .filter(|n| !n.is_empty())
    else {
        return Ok(());
    };

    let node_key = rusternetes_storage::build_key("nodes", None, node_name);
    let node: rusternetes_common::resources::Node = match storage.get(&node_key).await {
        Ok(n) => n,
        // Missing node: upstream's NodeRestriction admission allows the
        // request through and lets the scheduler / node controller catch up.
        Err(Error::NotFound(_)) => return Ok(()),
        Err(e) => return Err(e),
    };

    let declares_resize = node
        .status
        .as_ref()
        .and_then(|s| s.declared_features.as_ref())
        .map(|features| features.iter().any(|f| f == GUARANTEED_QOS_POD_CPU_RESIZE))
        .unwrap_or(false);

    if declares_resize {
        Ok(())
    } else {
        Err(Error::Forbidden(format!(
            "spec.containers[*].resources: Forbidden: node \"{}\" has not declared feature \"{}\"",
            node_name, GUARANTEED_QOS_POD_CPU_RESIZE
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exec_kubelet_url_matches_upstream_shape() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.5".into(),
                port: 10250,
                scheme: "http",
            },
            "exec",
            "ns1",
            "pod1",
            "c1",
            "command=id&tty=false&stdin=false&stdout=true&stderr=true",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.5:10250/exec/ns1/pod1/c1?command=id&tty=false&stdin=false&stdout=true&stderr=true"
        );
    }

    #[test]
    fn attach_kubelet_url_matches_upstream_shape() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "192.168.1.10".into(),
                port: 10250,
                scheme: "http",
            },
            "attach",
            "default",
            "my-pod",
            "main",
            "stdin=true&stdout=true",
        );
        assert_eq!(
            u.to_string(),
            "http://192.168.1.10:10250/attach/default/my-pod/main?stdin=true&stdout=true"
        );
    }

    #[test]
    fn kubelet_url_no_query() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.1".into(),
                port: 10250,
                scheme: "http",
            },
            "exec",
            "kube-system",
            "coredns",
            "coredns",
            "",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.1:10250/exec/kube-system/coredns/coredns"
        );
    }

    #[test]
    fn logs_kubelet_url_uses_containerlogs_path() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.6".into(),
                port: 10250,
                scheme: "http",
            },
            "containerLogs",
            "ns1",
            "pod1",
            "c1",
            "tailLines=5&follow=false",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.6:10250/containerLogs/ns1/pod1/c1?tailLines=5&follow=false"
        );
    }
}

#[cfg(test)]
mod exec_query_bool_tests {
    use super::parse_exec_query;

    /// Upstream decodes stdin/stdout/stderr/tty with
    /// `runtime.Convert_Slice_string_To_bool`
    /// (apimachinery/pkg/runtime/conversion.go:79-95): only absence, "0" and a
    /// case-insensitive "false" are false; every other value is true. The old
    /// `value == "true" || value == "1"` comparison dropped "t", "T", "yes"
    /// and "", so a client asking for a TTY quietly did not get one.
    #[test]
    fn stream_flags_follow_upstream_query_conversion() {
        for v in ["true", "1", "t", "T", "TRUE", "yes", ""] {
            let q = parse_exec_query(&format!("stdin={v}&stdout={v}&stderr={v}&tty={v}"));
            assert!(q.stdin, "stdin={v:?} must be true");
            assert!(q.stdout, "stdout={v:?} must be true");
            assert!(q.stderr, "stderr={v:?} must be true");
            assert!(q.tty, "tty={v:?} must be true");
        }
        for v in ["0", "false", "False", "FALSE"] {
            let q = parse_exec_query(&format!("stdin={v}&stdout={v}&stderr={v}&tty={v}"));
            assert!(!q.stdin, "stdin={v:?} must be false");
            assert!(!q.tty, "tty={v:?} must be false");
        }
    }

    #[test]
    fn absent_stream_flags_default_to_false() {
        let q = parse_exec_query("command=sh&container=c");
        assert!(!q.stdin && !q.stdout && !q.stderr && !q.tty);
        assert_eq!(q.container.as_deref(), Some("c"));
        assert_eq!(q.command, vec!["sh".to_string()]);
    }
}
