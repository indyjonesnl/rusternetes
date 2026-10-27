//! `/logs/` system-log handler (#3478).
//!
//! Ports `(*Server).InstallSystemLogHandler`, the `/logs/` arm of
//! `(*Server).InstallDebuggingDisabledHandlers`
//! (pkg/kubelet/server/server.go) and `(*Kubelet).ServeLogs` /
//! the `kl.logServer` built in `(*Kubelet).Run` (pkg/kubelet/kubelet.go):
//! `http.StripPrefix("/logs/", http.FileServer(http.Dir("/var/log/")))`.

use axum::{
    body::Body,
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get, MethodRouter},
    Router,
};
use futures::StreamExt;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

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
/// "logs endpoint is disabled.". (`EnableSystemLogQuery` / the journal
/// handler are not ported.)
pub fn install_logs_handlers(
    router: Router,
    enable_debugging_handlers: bool,
    enable_system_log_handler: bool,
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
        get(move |uri: Uri| {
            let root = root.clone();
            async move { serve_logs(&root, &uri).await }
        }),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    async fn call(app: &Router, method: Method, path: &str) -> (u16, String, Option<String>) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let loc = resp
            .headers()
            .get("location")
            .map(|v| v.to_str().unwrap().to_string());
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).to_string(), loc)
    }

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let outer = tempfile::tempdir().unwrap();
        std::fs::write(outer.path().join("secret"), "TOP-SECRET").unwrap();
        let root = outer.path().join("log");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("kubelet.log"), "line1\n").unwrap();
        std::fs::write(root.join("google.log"), "g\n").unwrap();
        std::fs::create_dir(root.join("pods")).unwrap();
        (outer, root)
    }

    // server_test.go:TestServeLogs -- directory index lists the files.
    #[tokio::test]
    async fn serves_directory_index_and_files() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/").await;
        assert_eq!(st, 200, "{body}");
        assert!(
            body.contains("<a href=\"kubelet.log\">kubelet.log</a>"),
            "{body}"
        );
        assert!(
            body.contains("<a href=\"google.log\">google.log</a>"),
            "{body}"
        );
        assert!(body.contains("<a href=\"pods/\">pods/</a>"), "{body}");
        let (st, body, _) = call(&app, Method::GET, "/logs/kubelet.log").await;
        assert_eq!((st, body.as_str()), (200, "line1\n"));
        let (st, _, _) = call(&app, Method::GET, "/logs/missing.log").await;
        assert_eq!(st, 404);
    }

    // http.FileServer redirects a directory without trailing slash.
    #[tokio::test]
    async fn directory_without_slash_redirects() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, root);
        let (st, _, loc) = call(&app, Method::GET, "/logs/pods").await;
        assert_eq!((st, loc.as_deref()), (301, Some("pods/")));
    }

    // http.FileServer cleans the path: it never escapes the root.
    #[tokio::test]
    async fn path_traversal_stays_inside_root() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, root);
        for p in [
            "/logs/../secret",
            "/logs/pods/../../secret",
            "/logs/%2e%2e/secret",
        ] {
            let (st, body, _) = call(&app, Method::GET, p).await;
            assert!(!body.contains("TOP-SECRET"), "{p} leaked: {st} {body}");
        }
    }

    // server_test.go:TestDebuggingDisabledHandlers (/logs/kubelet.log arm).
    #[tokio::test]
    async fn debugging_disabled_overrides_system_log_handler() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), false, true, root);
        for m in [Method::GET, Method::POST] {
            let (st, body, _) = call(&app, m, "/logs/kubelet.log").await;
            assert_eq!(
                (st, body.as_str()),
                (405, "Debug endpoints are disabled.\n")
            );
        }
        let (st, body, _) = call(&app, Method::GET, "/logs/").await;
        assert_eq!(
            (st, body.as_str()),
            (405, "Debug endpoints are disabled.\n")
        );
    }

    // server_test.go:TestDisablingLogAndProfilingHandler (/logs arm).
    #[tokio::test]
    async fn system_log_handler_disabled() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, false, root);
        for m in [Method::GET, Method::POST] {
            let (st, body, _) = call(&app, m, "/logs/kubelet.log").await;
            assert_eq!((st, body.as_str()), (405, "logs endpoint is disabled.\n"));
        }
    }

    // v1beta1/defaults.go:122-124,280-282: nil => true.
    #[test]
    fn config_defaults_enable_handlers() {
        let c = crate::config::KubeletConfiguration::default();
        assert!(c.debugging_handlers_enabled() && c.system_log_handler_enabled());
        let c: crate::config::KubeletConfiguration =
            serde_yaml::from_str("enableDebuggingHandlers: false\nenableSystemLogHandler: false\n")
                .unwrap();
        assert!(!c.debugging_handlers_enabled() && !c.system_log_handler_enabled());
    }
}
