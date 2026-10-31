//! `/logs/` system-log handler (#3478).
//!
//! Ports `(*Server).InstallSystemLogHandler`, the `/logs/` arm of
//! `(*Server).InstallDebuggingDisabledHandlers`
//! (pkg/kubelet/server/server.go) and `(*Kubelet).ServeLogs` /
//! the `kl.logServer` built in `(*Kubelet).Run` (pkg/kubelet/kubelet.go):
//! `http.StripPrefix("/logs/", http.FileServer(http.Dir("/var/log/")))`, and,
//! when the `NodeLogQuery` gate and `enableSystemLogQuery` are on, the
//! query-aware variant of that handler plus `journalServer.ServeHTTP`
//! (pkg/kubelet/kubelet_server_journal.go:62-89), whose engine lives in
//! [`crate::node_log_query`].

use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get, MethodRouter},
    Router,
};
use futures::StreamExt;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

use crate::node_log_query::{aggregate_message, new_node_log_query};

/// `logsPath` (server.go).
pub const LOGS_PATH: &str = "/logs/";
/// `nodeLogDir` (pkg/kubelet/kubelet_server_journal.go).
pub const NODE_LOG_DIR: &str = "/var/log/";

/// `http.Error(w, msg, http.StatusMethodNotAllowed)` as built by
/// `getHandlerForDisabledEndpoint` / `InstallDebuggingDisabledHandlers`.
fn disabled(msg: &'static str) -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!("{msg}\n"),
    )
        .into_response()
}

/// Install the `/logs/` routes per `NewServer`'s gating (server.go:315-325):
/// `EnableDebuggingHandlers` off => `InstallDebuggingDisabledHandlers` answers
/// every method on `/logs/` with 405 "Debug endpoints are disabled."; on =>
/// `InstallSystemLogHandler`, which serves `GET` only when
/// `EnableSystemLogHandler` is on and otherwise answers every method with 405
/// "logs endpoint is disabled.". `enable_system_log_query` is
/// `NodeLogQuery && EnableSystemLogQuery` (kubelet.go:1783): it swaps the
/// plain file server for the query-aware handler built in `Kubelet.Run`
/// (kubelet.go:1784-1812).
pub fn install_logs_handlers(
    router: Router,
    enable_debugging_handlers: bool,
    enable_system_log_handler: bool,
    enable_system_log_query: bool,
    root: PathBuf,
) -> Router {
    let routes =
        |r: Router, h: MethodRouter| r.route("/logs/", h.clone()).route("/logs/*logpath", h);
    if !enable_debugging_handlers {
        return routes(
            router,
            any(|| async { disabled("Debug endpoints are disabled.") }),
        );
    }
    if !enable_system_log_handler {
        return routes(
            router,
            any(|| async { disabled("logs endpoint is disabled.") }),
        );
    }
    routes(
        router,
        get(move |uri: Uri, headers: HeaderMap| {
            let root = root.clone();
            async move {
                if enable_system_log_query {
                    serve_logs_query(&root, &uri, &headers).await
                } else {
                    serve_logs(&root, &uri).await
                }
            }
        }),
    )
}

/// `http.Error(w, msg, code)`.
fn http_error(code: StatusCode, msg: &str) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!("{msg}\n"),
    )
        .into_response()
}

/// `req.URL.Query()` as `(key, value)` pairs in request order.
fn query_pairs(uri: &Uri) -> Vec<(String, String)> {
    url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}

/// The `kl.logServer` handler of `Kubelet.Run` (kubelet.go:1784-1812) when
/// `NodeLogQuery` and `enableSystemLogQuery` are on.
async fn serve_logs_query(root: &Path, uri: &Uri, headers: &HeaderMap) -> Response {
    let path = unescape(uri.path());
    let upath = path.strip_prefix(LOGS_PATH).unwrap_or("");
    match new_node_log_query(&query_pairs(uri)) {
        Err(errs) => http_error(StatusCode::BAD_REQUEST, &aggregate_message(&errs)),
        Ok(Some(nlq)) => {
            if !upath.is_empty() && upath != "/" {
                return http_error(StatusCode::NOT_ACCEPTABLE, "path not allowed in query mode");
            }
            let errs = nlq.validate_in(&root.to_string_lossy());
            if !errs.is_empty() {
                return http_error(StatusCode::NOT_ACCEPTABLE, &aggregate_message(&errs));
            }
            // Validation ensures that the request does not query services and
            // files at the same time.
            if !nlq.services.is_empty() {
                return serve_journal(nlq, headers);
            }
            // Validation ensures that the request does not explicitly query
            // multiple files at the same time.
            if nlq.files.len() == 1 {
                return serve_logs_path(root, &nlq.files[0].replace('\\', "/")).await;
            }
            serve_logs_path(root, upath).await
        }
        // Fall back in case the caller is directly trying to query a file.
        Ok(None) => serve_logs_path(root, upath).await,
    }
}

/// `io.Writer` over a response body channel; `Err` once the client is gone.
struct ChannelWriter(tokio::sync::mpsc::Sender<Result<axum::body::Bytes, std::io::Error>>);

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .blocking_send(Ok(axum::body::Bytes::copy_from_slice(buf)))
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// `journalServer.ServeHTTP` (kubelet_server_journal.go:65-89): stream the
/// logging command's output, gzip-encoded when the request's
/// `Accept-Encoding` is exactly `gzip`.
fn serve_journal(nlq: crate::node_log_query::NodeLogQuery, headers: &HeaderMap) -> Response {
    let gzip = headers
        .get(header::ACCEPT_ENCODING)
        .is_some_and(|v| v.as_bytes() == b"gzip");
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    tokio::task::spawn_blocking(move || {
        let w = ChannelWriter(tx);
        if gzip {
            let mut gz = flate2::write::GzEncoder::new(w, flate2::Compression::fast());
            nlq.copy(&mut gz);
            let _ = gz.finish();
        } else {
            let mut w = w;
            nlq.copy(&mut w);
        }
    });
    let mut b = Response::builder().header(header::CONTENT_TYPE, "text/plain;charset=UTF-8");
    if gzip {
        b = b.header(header::CONTENT_ENCODING, "gzip");
    }
    b.body(Body::from_stream(
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ))
    .unwrap()
}

/// Minimal `url.PathUnescape` for the request path.
fn unescape(p: &str) -> String {
    let b = p.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&p[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `path.Clean("/" + upath)` as `http.FileServer` applies it: `..` can never
/// climb above the root.
fn clean(upath: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    for seg in upath.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s.to_string()),
        }
    }
    parts
}

fn redirect(to: &str) -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(header::LOCATION, to.to_string())],
    )
        .into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "404 page not found\n").into_response()
}

fn forbidden() -> Response {
    (StatusCode::FORBIDDEN, "403 Forbidden\n").into_response()
}

/// `http.StripPrefix("/logs/", http.FileServer(http.Dir(root)))`.
async fn serve_logs(root: &Path, uri: &Uri) -> Response {
    let path = unescape(uri.path());
    let upath = path.strip_prefix(LOGS_PATH).unwrap_or("");
    serve_logs_path(root, upath).await
}

/// The `http.FileServer` half of [`serve_logs`] for an already-stripped path.
async fn serve_logs_path(root: &Path, upath: &str) -> Response {
    let parts = clean(upath);
    let mut fs_path = root.to_path_buf();
    fs_path.extend(&parts);
    let base = parts.last().cloned().unwrap_or_default();

    // serveFile: a request for "/index.html" redirects to "./".
    if upath == "index.html" || upath.ends_with("/index.html") {
        return redirect("./");
    }
    let meta = match tokio::fs::metadata(&fs_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return forbidden(),
        Err(_) => return not_found(),
    };
    let has_slash = upath.ends_with('/') || upath.is_empty();
    if meta.is_dir() {
        if !has_slash {
            return redirect(&format!("{base}/"));
        }
        let index = fs_path.join("index.html");
        if tokio::fs::metadata(&index)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            return serve_file(&index).await;
        }
        return dir_list(&fs_path).await;
    }
    if has_slash {
        return redirect(&format!("../{base}"));
    }
    serve_file(&fs_path).await
}

fn content_type_for(path: &Path, head: &[u8]) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("json") => "application/json",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("gz") => "application/gzip",
        Some("txt") | Some("log") => "text/plain; charset=utf-8",
        _ => {
            // net/http.DetectContentType, reduced to the text/binary split.
            if std::str::from_utf8(head).is_ok() && !head.contains(&0) {
                "text/plain; charset=utf-8"
            } else {
                "application/octet-stream"
            }
        }
    }
}

async fn serve_file(path: &Path) -> Response {
    let mut f = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return forbidden(),
        Err(_) => return not_found(),
    };
    let len = f.metadata().await.map(|m| m.len()).ok();
    let mut head = vec![0u8; 512];
    let n = f.read(&mut head).await.unwrap_or(0);
    head.truncate(n);
    let ct = content_type_for(path, &head);
    let first = axum::body::Bytes::from(head);
    let rest = futures::stream::unfold(f, |mut f| async move {
        let mut buf = vec![0u8; 64 * 1024];
        match f.read(&mut buf).await {
            Ok(0) | Err(_) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, std::io::Error>(axum::body::Bytes::from(buf)), f))
            }
        }
    });
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(first) }).chain(rest);
    let mut b = Response::builder().header(header::CONTENT_TYPE, ct);
    if let Some(len) = len {
        b = b.header(header::CONTENT_LENGTH, len);
    }
    b.body(Body::from_stream(stream)).unwrap()
}

/// `net/http.dirList`.
async fn dir_list(dir: &Path) -> Response {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error reading directory\n",
            )
                .into_response()
        }
    };
    let mut entries: Vec<(String, bool)> = Vec::new();
    while let Ok(Some(e)) = rd.next_entry().await {
        let name = e.file_name().to_string_lossy().into_owned();
        // Dir.Open follows symlinks, so a link to a directory lists as one.
        let is_dir = tokio::fs::metadata(e.path())
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false);
        entries.push((name, is_dir));
    }
    entries.sort();
    let mut out = String::from(
        "<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n",
    );
    for (mut name, is_dir) in entries {
        if is_dir {
            name.push('/');
        }
        let href: String = name
            .bytes()
            .map(|c| match c {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    (c as char).to_string()
                }
                _ => format!("%{c:02X}"),
            })
            .collect();
        let text = name
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&#34;")
            .replace('\'', "&#39;");
        out.push_str(&format!("<a href=\"{href}\">{text}</a>\n"));
    }
    out.push_str("</pre>\n");
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], out).into_response()
}
