//! HTTP handlers for `/exec` and `/attach` — upgrade-proxy to the containerd-rs
//! CRI streaming server (upstream-faithful kubelet streaming).
//!
//! Upstream reference: `pkg/kubelet/server/server.go` routes
//! `/exec/{podNamespace}/{podID}/{containerName}` (+ `/{uid}/` variant).
//! `getExec` calls CRI `Exec` (returns a URL on the runtime streaming server)
//! then `proxyStream` = upgrade-aware proxy to that URL.
//!
//! This module replicates the same flow: resolve the container id from the CRI
//! `ListContainers` labels, call CRI `Exec`/`Attach` to get the runtime stream
//! URL, `rewrite_stream_url` it to the node-local streaming server, then
//! `proxy_upgrade(url, req)`.

use axum::{
    body::Body,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::ObjectMeta;
use tracing::{info, warn};

/// Query parameters for exec/attach requests, mirroring upstream
/// `remotecommandserver.Options`.
#[derive(Debug, Clone, Default)]
pub struct ExecParams {
    /// Command to execute (repeatable: `command=ls&command=-la`).
    pub command: Vec<String>,
    /// Connect stdin.
    pub stdin: bool,
    /// Stream stdout.
    pub stdout: bool,
    /// Stream stderr.
    pub stderr: bool,
    /// Allocate a pseudo-TTY.
    pub tty: bool,
}

impl ExecParams {
    /// Parse exec/attach query params from a raw query string.
    ///
    /// Supports repeated `command=` keys (upstream passes each argv element as a
    /// separate `command=` value). Boolean params (`stdin`, `stdout`, `stderr`,
    /// `tty`) accept `"true"` / `"1"` (case-insensitive); anything else is false.
    pub fn from_query(query: &str) -> Self {
        let mut command = Vec::new();
        let mut stdin = false;
        let mut stdout = false;
        let mut stderr = false;
        let mut tty = false;

        for pair in query.split('&') {
            let (key, value) = if let Some(pos) = pair.find('=') {
                (&pair[..pos], &pair[pos + 1..])
            } else {
                (pair, "")
            };
            // Percent-decode simple `+` and `%XX` sequences for the value.
            let value = decode_simple(value);
            match key {
                "command" if !value.is_empty() => {
                    command.push(value);
                }
                // A Kubernetes api-server proxies exec/attach to the kubelet
                // using the upstream param names — ExecStdinParam="input",
                // ExecStdoutParam="output", ExecStderrParam="error",
                // ExecTTYParam="tty" (k8s.io/api/core/v1/types.go). Also accept
                // the raw stream names (stdin/stdout/stderr) that the
                // rusternetes api-server forwards verbatim, so the kubelet works
                // behind BOTH a vanilla control plane and the native stack.
                // Without "input"/"output"/"error", a vanilla api-server's exec
                // request parses to all-false streams and the CRI rejects it
                // with "one of stdin, stdout, or stderr must be set".
                "input" | "stdin" => stdin = is_true(&value),
                "output" | "stdout" => stdout = is_true(&value),
                "error" | "stderr" => stderr = is_true(&value),
                "tty" => tty = is_true(&value),
                _ => {}
            }
        }
        ExecParams {
            command,
            stdin,
            stdout,
            stderr,
            tty,
        }
    }
}

/// Decode a boolean query parameter the way Kubernetes does.
///
/// Delegates to the shared implementation so the pod-log parameters
/// (`follow`, `previous`, `timestamps`, `insecureSkipTLSVerifyBackend`) follow
/// the same rule as everything else: only `"0"` and a case-insensitive
/// `"false"` are false. The previous `"true"`/`"1"`-only comparison dropped
/// `?follow=t` and `?follow=yes`, which upstream treats as true.
pub fn is_true(s: &str) -> bool {
    rusternetes_common::query::k8s_query_bool(s)
}

/// Minimal percent-decode: replace `+` with space and `%XX` hex sequences.
fn decode_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
            out.push('%');
            i += 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Query parameters for `/containerLogs` requests, mirroring upstream
/// `v1.PodLogOptions` fields forwarded by the api-server.
#[derive(Debug, Clone, Default)]
pub struct LogParams {
    /// Only return the last N lines of container output.
    pub tail_lines: Option<i32>,
    /// Stop after this many bytes.
    pub limit_bytes: Option<i64>,
    /// Stay attached and stream new output as it is produced.
    pub follow: bool,
    /// Preserve the RFC3339Nano timestamp prefix on each emitted line.
    pub timestamps: bool,
    /// Only lines at or after this RFC3339 timestamp (resolved to Unix epoch).
    pub since_time: Option<String>,
    /// Only lines emitted within the last N seconds.
    ///
    /// `kubectl logs --since=1s` sends `sinceSeconds=1`, NOT `sinceTime`, so a
    /// kubelet that only understands `sinceTime` silently returns the whole log.
    /// That is what made
    /// `[sig-cli] Kubectl logs logs should be able to retrieve and filter logs`
    /// flaky: it asserts `recent(--since=1s) < older(--since=24h)` and both
    /// queries returned every line, so it only passed when the pod happened to
    /// emit more output between the two calls
    /// (`expected recent(26) to be less than older(26)`).
    ///
    /// Upstream carries both fields in `v1.PodLogOptions` and rejects setting
    /// both at once ("at most one of `sinceTime` or `sinceSeconds` may be
    /// specified"), so `since_time` wins here when both arrive.
    pub since_seconds: Option<i64>,
}

impl LogParams {
    /// Parse `/containerLogs` query params from a raw query string.
    ///
    /// Integer fields (`tailLines`, `limitBytes`) are parsed with
    /// `str::parse`; silently ignored on parse failure. Booleans accept
    /// `"true"` / `"1"` (case-insensitive).
    pub fn from_query(query: &str) -> Self {
        let mut tail_lines: Option<i32> = None;
        let mut limit_bytes: Option<i64> = None;
        let mut follow = false;
        let mut timestamps = false;
        let mut since_time: Option<String> = None;
        let mut since_seconds: Option<i64> = None;

        for pair in query.split('&') {
            let (key, value) = if let Some(pos) = pair.find('=') {
                (&pair[..pos], &pair[pos + 1..])
            } else {
                (pair, "")
            };
            let value = decode_simple(value);
            match key {
                "tailLines" => tail_lines = value.parse().ok(),
                "limitBytes" => limit_bytes = value.parse().ok(),
                "follow" => follow = is_true(&value),
                "timestamps" => timestamps = is_true(&value),
                "sinceTime" if !value.is_empty() => {
                    since_time = Some(value);
                }
                "sinceSeconds" => since_seconds = value.parse().ok().filter(|s: &i64| *s > 0),
                _ => {}
            }
        }
        LogParams {
            tail_lines,
            limit_bytes,
            follow,
            timestamps,
            since_time,
            since_seconds,
        }
    }
}

/// Map [`LogParams`] to [`rusternetes_cri::stream::LogReadOptions`].
pub fn log_read_options(params: &LogParams) -> rusternetes_cri::stream::LogReadOptions {
    // sinceTime wins when both are present (upstream validation forbids that
    // combination outright); otherwise sinceSeconds becomes an absolute cutoff
    // relative to now, which is what the CRI log reader filters on.
    let since_unix = params
        .since_time
        .as_deref()
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|t| t.timestamp())
        })
        .or_else(|| {
            params
                .since_seconds
                .map(|secs| chrono::Utc::now().timestamp() - secs)
        });
    rusternetes_cri::stream::LogReadOptions {
        timestamps: params.timestamps,
        tail_lines: params.tail_lines,
        limit_bytes: params.limit_bytes,
        since_unix,
    }
}

/// Path parameters for the exec/attach routes (3-segment variant: no uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath3 {
    pub namespace: String,
    pub pod: String,
    pub container: String,
}

/// Path parameters for the exec/attach routes (4-segment variant: with uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath4 {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
}

/// Build a minimal [`Pod`] value sufficient for [`resolve_container_id`].
fn pod_for_resolve(namespace: &str, pod_name: &str, uid: &str) -> Pod {
    Pod {
        type_meta: rusternetes_common::types::TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        metadata: ObjectMeta {
            name: pod_name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uid.to_string(),
            ..Default::default()
        },
        spec: None,
        status: None,
    }
}

/// Resolve the container id and build the upgrade target URI, then proxy.
async fn exec_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("exec_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "exec_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("exec_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Exec → get the runtime stream URL.
    let stream_url = match cri
        .exec(rusternetes_cri::v1::ExecRequest {
            container_id: container_id.clone(),
            cmd: params.command.clone(),
            tty: params.tty,
            stdin: params.stdin,
            stdout: params.stdout,
            // stderr is not multiplexed under a tty (the pty merges it into
            // stdout), matching the kubelet/remotecommand contract.
            stderr: params.stderr && !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("exec_proxy: CRI Exec failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Rewrite the stream URL to the node-local streaming server port.
    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "exec_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

/// Resolve the container id and proxy an attach request to the CRI stream.
async fn attach_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("attach_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "attach_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("attach_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Attach → get the runtime stream URL.
    let stream_url = match cri
        .attach(rusternetes_cri::v1::AttachRequest {
            container_id: container_id.clone(),
            stdin: params.stdin,
            tty: params.tty,
            stdout: params.stdout,
            stderr: params.stderr && !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("attach_proxy: CRI Attach failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "attach_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

/// Path parameters for the portForward routes (2-segment variant: no uid).
#[derive(serde::Deserialize, Debug)]
pub struct PortForwardPath2 {
    pub namespace: String,
    pub pod: String,
}

/// Path parameters for the portForward routes (3-segment variant: with uid).
#[derive(serde::Deserialize, Debug)]
pub struct PortForwardPath3 {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
}

/// `wsstream.IsWebSocketRequest`
/// (`staging/src/k8s.io/apimachinery/pkg/util/httpstream/wsstream/conn.go`):
/// `Upgrade: websocket` plus a `Connection` header containing `upgrade`.
pub fn is_websocket_request(headers: &axum::http::HeaderMap) -> bool {
    let has_token = |name: &str, token: &str| {
        headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case(token))
    };
    has_token("upgrade", "websocket") && has_token("connection", "upgrade")
}

/// Parse options, resolve the pod sandbox, call CRI `PortForward`, then
/// upgrade-proxy to the runtime stream URL.
///
/// Port of `Server.getPortForward` (`pkg/kubelet/server/server.go:1026-1049`):
/// `NewV4Options` -> `GetPortForward` (`kubelet_pods.go:2746`, which calls
/// `kuberuntime_sandbox.go:370-387` = CRI `PortForward` on the newest sandbox of
/// the pod) -> `proxyStream`. Non-WebSocket (SPDY) requests carry their ports in
/// the stream headers, so the CRI request gets no ports for them.
async fn portforward_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let ports = match rusternetes_cri::stream::parse_port_forward_ports(
        &query,
        is_websocket_request(req.headers()),
    ) {
        Ok(p) => p,
        Err(e) => {
            warn!("portforward_proxy: bad options: {e}");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("portforward_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let sandbox_id = match rusternetes_cri::stream::resolve_sandbox_id(&mut cri, &pod).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            warn!("portforward_proxy: no sandbox for ns={namespace} pod={pod_name}");
            return (
                StatusCode::NOT_FOUND,
                format!("pod not found ({pod_name:?}_{namespace:?})"),
            )
                .into_response();
        }
        Err(e) => {
            warn!("portforward_proxy: resolve_sandbox_id failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let stream_url = match cri.port_forward(&sandbox_id, &ports).await {
        Ok(url) => url,
        Err(e) => {
            warn!("portforward_proxy: CRI PortForward failed for {sandbox_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("portforward_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };
    info!(
        "portforward_proxy: ns={namespace} pod={pod_name} sandbox={sandbox_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("portforward_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

// ---------------------------------------------------------------------------
// Axum route handlers
// ---------------------------------------------------------------------------

/// `POST /exec/:namespace/:pod/:container`
pub async fn handle_exec(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /exec/:namespace/:pod/:uid/:container`
pub async fn handle_exec_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:container`
pub async fn handle_attach(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:uid/:container`
pub async fn handle_attach_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

/// `GET|POST /portForward/:namespace/:pod`
pub async fn handle_portforward(
    Path(p): Path<PortForwardPath2>,
    req: axum::extract::Request,
) -> Response {
    portforward_proxy(&p.namespace, &p.pod, "", req).await
}

/// `GET|POST /portForward/:namespace/:pod/:uid`
pub async fn handle_portforward_uid(
    Path(p): Path<PortForwardPath3>,
    req: axum::extract::Request,
) -> Response {
    portforward_proxy(&p.namespace, &p.pod, &p.uid, req).await
}

/// `GET /containerLogs/:namespace/:pod/:container`
///
/// Reads the container's CRI logfile directly from the local node filesystem.
/// When `follow=true` the response body is a tailing stream that stays open
/// and pushes new lines as they are written; otherwise the current log
/// contents are returned in one shot.
pub async fn handle_container_logs(
    Path(p): Path<ExecPath3>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = LogParams::from_query(&query);

    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("handle_container_logs: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(&p.namespace, &p.pod, "");
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, &p.container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "handle_container_logs: container not found: ns={} pod={} container={}",
                    p.namespace, p.pod, p.container
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {:?} not found in pod {:?} (namespace {:?})",
                        p.container, p.pod, p.namespace
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("handle_container_logs: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    let log_path =
        match rusternetes_cri::stream::resolve_log_path(&mut cri, &pod, &container_id).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                warn!("handle_container_logs: no log_path for container_id={container_id}");
                return (
                    StatusCode::NOT_FOUND,
                    "container log file not available".to_string(),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("handle_container_logs: resolve_log_path failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    info!(
        "handle_container_logs: ns={} pod={} container={} container_id={} log_path={} follow={}",
        p.namespace,
        p.pod,
        p.container,
        container_id,
        log_path.display(),
        params.follow
    );

    if params.follow {
        // Tailing stream: emit existing backlog then poll for new bytes, closing
        // (EOF) once the container has exited — see follow_log_stream.
        let stream = follow_log_stream(
            log_path,
            params,
            std::time::Duration::from_millis(500),
            cri_container_gone(container_id.clone()),
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Body::from_stream(stream))
            .unwrap();
    }

    // Non-follow: read current log contents and return in one shot.
    let opts = log_read_options(&params);
    match rusternetes_cri::stream::read_log_file(&log_path, &opts) {
        Ok(bytes) => container_logs_response(bytes),
        Err(e) => {
            warn!("handle_container_logs: read_log_file failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// `/run` and `/checkpoint` (InstallAuthRequiredHandlers, server.go:519-629)
// ---------------------------------------------------------------------------

/// Route template of the `/checkpoint` web service (server.go:624-626).
pub const CHECKPOINT_PATH: &str = "/checkpoint/:namespace/:pod/:container";

/// State of the `/run` and `/checkpoint` handlers: `s.host.GetPodByName` is a
/// lookup of the pods bound to this node and `kl.getCheckpointsDir()` is
/// `<rootDir>/checkpoints` (kubelet_getters.go:89, `DefaultKubeletCheckpointsDirName`).
#[derive(Clone)]
pub struct RunState {
    pub server: crate::server::ServerState,
    pub checkpoints_dir: std::path::PathBuf,
}

/// `kubecontainer.GetPodFullName`: `name_namespace`.
fn pod_full_name(pod: &Pod) -> String {
    format!(
        "{}_{}",
        pod.metadata.name,
        pod.metadata.namespace.as_deref().unwrap_or("")
    )
}

/// `s.host.GetPodByName(namespace, name)`.
async fn get_pod_by_name(state: &RunState, namespace: &str, name: &str) -> Option<Pod> {
    let list = crate::server::pods_for_node(&state.server.storage, &state.server.node_name)
        .await
        .ok()?;
    list.items.into_iter().find(|p| {
        p.metadata.name == name && p.metadata.namespace.as_deref().unwrap_or("") == namespace
    })
}

/// `response.WriteError(status, err)`: the error text as a plain body.
fn write_error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, msg.into()).into_response()
}

/// `writeJSONResponse` (server.go:1011-1022): a nil body is a bare 200.
fn write_json_response(data: Vec<u8>) -> Response {
    if data.is_empty() {
        return StatusCode::OK.into_response();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(data))
        .unwrap()
}

/// All values of query key `key`, percent-decoded (`req.Request.URL.Query()[key]`).
fn query_values(query: &str, key: &str) -> Vec<String> {
    query
        .split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_simple(k) == key).then(|| decode_simple(v))
        })
        .collect()
}

/// `(*Kubelet).findContainer` (kubelet_pods.go): the container of the pod
/// matching `podFullName` (and `podUID` when given), as a CRI container id.
async fn find_container(
    pod: &Pod,
    pod_uid: &str,
    container: &str,
) -> anyhow::Result<Option<String>> {
    if !pod_uid.is_empty() && pod.metadata.uid != pod_uid {
        return Ok(None);
    }
    let mut cri = rusternetes_cri::stream::connect().await?;
    rusternetes_cri::stream::resolve_container_id(&mut cri, pod, container).await
}

/// `(*Server).getRun` (server.go:992-1008): run a command synchronously and
/// return the combined output. `cmd` is split on spaces (the legacy `cmd`
/// query parameter, not exec's `command`).
async fn run_in_container(
    state: RunState,
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    query: &str,
) -> Response {
    let Some(pod) = get_pod_by_name(&state, namespace, pod_name).await else {
        return write_error(StatusCode::NOT_FOUND, "pod does not exist");
    };
    // `strings.Split(request.QueryParameter("cmd"), " ")`
    let cmd: Vec<String> = query_values(query, "cmd")
        .into_iter()
        .next()
        .unwrap_or_default()
        .split(' ')
        .map(String::from)
        .collect();
    // `kl.RunInContainer` (kubelet_pods.go:2698-2708): timeout 0.
    let container_id = match find_container(&pod, uid, container).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return write_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("container not found ({container:?})"),
            )
        }
        Err(e) => return write_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => return write_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match rusternetes_cri::stream::exec_sync(&mut cri, &container_id, &cmd, 0).await {
        Ok((mut stdout, stderr, exit_code)) => {
            if exit_code != 0 {
                // remote_runtime.go ExecSync: `exec.CodeExitError`.
                return write_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "command '{}' exited with {exit_code}: {}",
                        cmd.join(" "),
                        String::from_utf8_lossy(&stderr)
                    ),
                );
            }
            // kuberuntime RunInContainer: `append(stdout, stderr...)`.
            stdout.extend_from_slice(&stderr);
            write_json_response(stdout)
        }
        Err(e) => write_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// `POST /run/:namespace/:pod/:container`
pub async fn handle_run(
    axum::extract::State(state): axum::extract::State<RunState>,
    Path(p): Path<ExecPath3>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    run_in_container(state, &p.namespace, &p.pod, "", &p.container, &query).await
}

/// `POST /run/:namespace/:pod/:uid/:container`
pub async fn handle_run_uid(
    axum::extract::State(state): axum::extract::State<RunState>,
    Path(p): Path<ExecPath4>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    run_in_container(state, &p.namespace, &p.pod, &p.uid, &p.container, &query).await
}

/// `(*Server).checkpoint` (server.go:1053-1136) with
/// `(*Kubelet).CheckpointContainer` (kubelet.go:3259-3291).
pub async fn handle_checkpoint(
    axum::extract::State(state): axum::extract::State<RunState>,
    Path(p): Path<ExecPath3>,
    req: axum::extract::Request,
) -> Response {
    let Some(pod) = get_pod_by_name(&state, &p.namespace, &p.pod).await else {
        return write_error(StatusCode::NOT_FOUND, "pod does not exist");
    };
    let container = p.container;
    let found = pod.spec.as_ref().is_some_and(|s| {
        s.containers.iter().any(|c| c.name == container)
            || s.init_containers
                .iter()
                .flatten()
                .any(|c| c.name == container)
            || s.ephemeral_containers
                .iter()
                .flatten()
                .any(|c| c.name == container)
    });
    if !found {
        return write_error(
            StatusCode::NOT_FOUND,
            format!("container {container} does not exist"),
        );
    }

    // "If the user specified one or multiple values for timeouts we are using
    // the last available value."
    let mut timeout: i64 = 0;
    if let Some(last) = query_values(req.uri().query().unwrap_or(""), "timeout").pop() {
        match last.parse::<i64>() {
            Ok(t) => timeout = t,
            Err(_) => {
                return write_error(
                    StatusCode::NOT_FOUND,
                    "cannot parse value of timeout parameter",
                )
            }
        }
    }

    let failed = |e: String| {
        write_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "checkpointing of {}/{}/{} failed ({e})",
                p.namespace, p.pod, container
            ),
        )
    };
    let container_id = match find_container(&pod, "", &container).await {
        Ok(Some(id)) => id,
        Ok(None) => return failed(format!("container {container} not found")),
        Err(e) => return failed(e.to_string()),
    };
    let location = state.checkpoints_dir.join(format!(
        "checkpoint-{}-{}-{}.tar",
        pod_full_name(&pod),
        container,
        chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
    ));
    let location = location.to_string_lossy().into_owned();
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => return failed(e.to_string()),
    };
    if let Err(e) = cri
        .checkpoint_container(&container_id, &location, timeout)
        .await
    {
        return failed(e.to_string());
    }
    write_json_response(format!("{{\"items\":[\"{location}\"]}}").into_bytes())
}

/// The `/run` and `/checkpoint` routes of `InstallAuthRequiredHandlers`;
/// `/checkpoint` only when `checkpoint_enabled` (the `ContainerCheckpoint`
/// gate, server.go:620-628).
pub fn run_router(state: RunState, checkpoint_enabled: bool) -> axum::Router {
    use axum::routing::post;
    let mut router = axum::Router::new()
        .route("/run/:namespace/:pod/:container", post(handle_run))
        .route("/run/:namespace/:pod/:uid/:container", post(handle_run_uid));
    if checkpoint_enabled {
        router = router.route(CHECKPOINT_PATH, post(handle_checkpoint));
    }
    router.with_state(state)
}

/// Build the `/containerLogs` response for a completed (`follow=false`) read.
///
/// The body is deliberately a one-item *stream*, not `Body::from(bytes)`, so
/// hyper frames it chunked with no `Content-Length` — the framing upstream
/// produces by writing logs through `flushwriter.Wrap(response.ResponseWriter)`
/// (`pkg/kubelet/server/server.go` `getContainerLogs`).
///
/// The framing is load-bearing, not cosmetic. For a `Content-Length` body Go's
/// net/http hands the last bytes to the caller *together with* `io.EOF`
/// (`net/http/transfer.go` `body.readLocked`: "If we can return an EOF here
/// along with the read data, do so"), and the api-server's websocket log relay
/// inspects the error before the byte count:
///
/// ```text
/// n, err := r.Read(buf)
/// if err != nil { if err == io.EOF { return nil } ... }  // n bytes dropped
/// if n > 0 { websocket.Message.Send(ws, buf[:n]) }
/// ```
///
/// (`apimachinery/pkg/util/httpstream/wsstream.messageCopy`). So a
/// fixed-length body is discarded whole and `GET pods/<p>/log` over a websocket
/// returns nothing, failing the conformance spec "should support retrieving
/// logs from the container over websockets" (#1713). The plain-HTTP path uses
/// `io.Copy`, which honours `(n > 0, io.EOF)`, which is why `kubectl logs` kept
/// working. The `follow=true` branch above was already streamed and unaffected.
pub fn container_logs_response(bytes: Vec<u8>) -> Response {
    let stream = async_stream::stream! {
        yield Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(bytes));
        // Yield once more so hyper sees a pending body and flushes the data
        // chunk on its own, ahead of the terminating `0\r\n\r\n`. Upstream gets
        // this for free from `flushwriter.Wrap` — each log write is flushed and
        // the terminator only follows when the handler returns. Without the
        // gap both land in one TCP segment, Go's chunked reader consumes the
        // terminator in the same `Read` as the data and hands back
        // `(n, io.EOF)` — which `wsstream.messageCopy` throws away.
        tokio::task::yield_now().await;
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Render a run of raw CRI log bytes (one or more `\n`-delimited lines) into the
/// client-facing output, honoring `timestamps` and the `since_unix` filter.
fn render_cri_lines(complete: &[u8], timestamps: bool, since_unix: Option<i64>) -> Vec<u8> {
    let mut out = Vec::new();
    for line in complete.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let parsed = rusternetes_cri::stream::parse_cri_log_line(line);
        if let Some(since) = since_unix {
            if let Some(ts) = parsed.timestamp_unix {
                if ts < since {
                    continue;
                }
            }
        }
        if timestamps && !parsed.timestamp_prefix.is_empty() {
            out.extend_from_slice(parsed.timestamp_prefix.as_bytes());
        }
        out.extend_from_slice(&parsed.message);
    }
    out
}

/// Build a tailing `Stream` over the container log file.
///
/// Emits the existing backlog first (honoring `tail_lines`/`since_time`), then
/// polls for new bytes every `poll_interval`. The stream ends when:
///   * the log file disappears (container removed), OR
///   * the container is no longer running (`container_gone()` returns true) and
///     all remaining bytes have been drained — this is the EOF that a
///     `follow=true` reader (e.g. `kubectl logs -f`, hydrophone's completion
///     detector) blocks on. Upstream kubelet's `ReadLogs` stops following once
///     the container exits; without this a terminated container's persisted log
///     file keeps the stream open forever (#log-follow-no-eof).
///   * a bounded pre-file wait elapses if the file never appears.
///
/// `container_gone` is injected so the tail loop is unit-testable without a live
/// CRI runtime; production wires it to a CRI `ContainerStatus` poll.
pub fn follow_log_stream<F>(
    log_path: std::path::PathBuf,
    params: LogParams,
    poll_interval: std::time::Duration,
    container_gone: F,
) -> impl futures::Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
where
    F: Fn() -> futures::future::BoxFuture<'static, bool> + Send + 'static,
{
    use std::io::{Read, Seek, SeekFrom};

    let opts = log_read_options(&params);
    let timestamps = opts.timestamps;
    let since_unix = opts.since_unix;

    async_stream::stream! {
        // Emit existing backlog; a missing file is non-fatal (container may
        // not have written yet — fall through to the tail loop).
        match rusternetes_cri::stream::read_log_file(&log_path, &opts) {
            Ok(bytes) if !bytes.is_empty() => {
                yield Ok(bytes::Bytes::from(bytes));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(
                    "follow_log_stream: backlog read failed (may not exist yet): {e}"
                );
            }
        }

        let mut offset: u64 = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
        let mut carry: Vec<u8> = Vec::new();
        let mut seen_file = std::fs::metadata(&log_path).is_ok();
        let mut waited_ticks: u32 = 0;
        // Pre-file wait scaled to the poll interval (~120s at the 500ms default).
        let max_pre_file_ticks: u32 =
            (std::time::Duration::from_secs(120).as_millis()
                / poll_interval.as_millis().max(1)) as u32;

        loop {
            tokio::time::sleep(poll_interval).await;
            let len = match std::fs::metadata(&log_path) {
                Ok(m) => {
                    seen_file = true;
                    m.len()
                }
                Err(_) if !seen_file => {
                    // Container may have exited without ever writing a log file;
                    // don't wait the full pre-file budget in that case.
                    if container_gone().await {
                        break;
                    }
                    waited_ticks += 1;
                    if waited_ticks >= max_pre_file_ticks {
                        break;
                    }
                    continue;
                }
                Err(_) => break, // file vanished — container removed
            };
            if len < offset {
                // Truncated / rotated — restart from the beginning.
                offset = 0;
                carry.clear();
            }
            if len == offset {
                // Caught up. If the container has exited there will be no more
                // output — close the stream so `follow` readers get EOF.
                if container_gone().await {
                    // Flush any trailing partial line (no terminating newline).
                    if !carry.is_empty() {
                        let out = render_cri_lines(&carry, timestamps, since_unix);
                        if !out.is_empty() {
                            yield Ok(bytes::Bytes::from(out));
                        }
                    }
                    break;
                }
                continue;
            }
            let mut f = match std::fs::File::open(&log_path) {
                Ok(f) => f,
                Err(_) => break,
            };
            if f.seek(SeekFrom::Start(offset)).is_err() {
                break;
            }
            let mut buf = Vec::new();
            if f.read_to_end(&mut buf).is_err() {
                break;
            }
            offset = len;
            carry.extend_from_slice(&buf);

            // Only process complete lines; keep any partial tail.
            let last_nl = carry.iter().rposition(|&b| b == b'\n');
            let Some(idx) = last_nl else { continue };
            let complete: Vec<u8> = carry.drain(..=idx).collect();

            let out = render_cri_lines(&complete, timestamps, since_unix);
            if !out.is_empty() {
                yield Ok(bytes::Bytes::from(out));
            }
        }
    }
}

/// Production `container_gone` predicate: polls CRI `ContainerStatus` and reports
/// true once the container is no longer in the RUNNING state. A CRI/RPC error is
/// treated as "not gone" so a transient blip doesn't prematurely close a live
/// stream (the file-vanished path still terminates it on removal).
fn cri_container_gone(
    container_id: String,
) -> impl Fn() -> futures::future::BoxFuture<'static, bool> + Send + 'static {
    move || {
        let container_id = container_id.clone();
        Box::pin(async move {
            let mut cri = match rusternetes_cri::stream::connect().await {
                Ok(c) => c,
                Err(_) => return false,
            };
            match cri.container_status(&container_id, false).await {
                Ok(resp) => resp
                    .status
                    .map(|s| {
                        s.state != rusternetes_cri::v1::ContainerState::ContainerRunning as i32
                    })
                    .unwrap_or(false),
                Err(_) => false,
            }
        })
    }
}
