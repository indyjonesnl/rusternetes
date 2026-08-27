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

fn is_true(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    l == "true" || l == "1"
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
fn log_read_options(params: &LogParams) -> rusternetes_cri::stream::LogReadOptions {
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
fn container_logs_response(bytes: Vec<u8>) -> Response {
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
fn follow_log_stream<F>(
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

#[cfg(test)]
mod tests {
    use super::*;

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
