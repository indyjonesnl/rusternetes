//! The endpoint-level deprecation `Warning:` header: port of
//! `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:781-797,814`
//! (`deprecation.WarningMessage` per route, `utilwarning.AddWarningsHandler`)
//! over the deduplicating, truncating recorder of
//! `staging/src/k8s.io/apiserver/pkg/endpoints/filters/warning.go`.
//!
//! Deviation (structural): upstream installs the warning on each route and the
//! per-request recorder is shared with every later `warning.AddWarning`. Here
//! handlers write their own `Warning` headers on the response
//! ([`crate::endpoints::handlers::rest::respond`]), so the route-level
//! middleware runs the same recorder over `[endpoint warnings, then the
//! headers the handler wrote]`, which yields the order and dedup upstream's
//! recorder produces (the route's warnings are added before the handler runs).
//!
//! Only routes upstream installs get a warning: `removeDeletedKinds`
//! (`server/deleted_kinds.go:184`) drops a kind once `ShouldServeForVersion`
//! is false, which for a GA build is `removed <= current`.

use axum::extract::Request;
use axum::http::{header, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

use super::{
    is_deprecated, lookup, should_serve_for_version, warning_message, CURRENT_MAJOR_MINOR,
};

/// `truncateAtTotalRunes` / `truncateItemRunes` (filters/warning.go:38-41).
const TRUNCATE_AT_TOTAL_RUNES: usize = 4 * 1024;
const TRUNCATE_ITEM_RUNES: usize = 256;

/// The route's accumulated warnings (installer.go:781-797): the deprecation
/// message when the kind is deprecated and still served.
pub fn endpoint_warnings(
    group: &str,
    version: &str,
    resource: &str,
    subresource: &str,
) -> Vec<String> {
    let obj = lookup(group, version, resource, subresource);
    if let Some(removed) = obj.and_then(|l| l.removed) {
        if !should_serve_for_version(removed) {
            return Vec::new();
        }
    }
    let (major, minor) = CURRENT_MAJOR_MINOR;
    if is_deprecated(obj, major as i64, minor as i64) {
        vec![warning_message(obj)]
    } else {
        Vec::new()
    }
}

/// The request's deduplicating `warning.Recorder` (filters/warning.go:46-117),
/// with the response writer's `Warning` header values collected in `headers`.
#[derive(Default)]
pub struct Recorder {
    recorded: std::collections::HashSet<String>,
    ordered: Vec<(String, String)>,
    written: usize,
    truncating: bool,
    headers: Vec<String>,
    total_runes: Option<usize>,
    item_runes: Option<usize>,
}

/// `net.NewWarningHeader(299, agent, text)` (apimachinery util/net/http.go:655):
/// `None` for an invalid agent or text.
fn new_warning_header(agent: &str, text: &str) -> Option<String> {
    let agent = if agent.is_empty() {
        "-"
    } else if agent
        .chars()
        .any(|c| c == '\\' || c == '"' || c.is_whitespace() || c.is_control())
    {
        return None;
    } else {
        agent
    };
    if text.chars().any(char::is_control) {
        return None;
    }
    let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
    Some(format!("299 {agent} \"{escaped}\""))
}

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn with_limits(total: usize, item: usize) -> Self {
        Self {
            total_runes: Some(total),
            item_runes: Some(item),
            ..Self::default()
        }
    }

    fn total(&self) -> usize {
        self.total_runes.unwrap_or(TRUNCATE_AT_TOTAL_RUNES)
    }

    fn item(&self) -> usize {
        self.item_runes.unwrap_or(TRUNCATE_ITEM_RUNES)
    }

    fn truncate(text: &str, runes: usize) -> String {
        text.chars().take(runes).collect()
    }

    /// `AddWarning(agent, text)`.
    pub fn add_warning(&mut self, agent: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        // already exceeded the limit and truncating
        if self.written >= self.total() && self.truncating {
            return;
        }
        // dedupe if already warned
        if !self.recorded.insert(text.to_string()) {
            return;
        }
        self.ordered.push((agent.to_string(), text.to_string()));

        let mut text = text.to_string();
        let mut len = text.chars().count();
        if self.truncating && len > self.item() {
            text = Self::truncate(&text, self.item());
            len = self.item();
        }
        let Some(header) = new_warning_header(agent, &text) else {
            return;
        };
        if self.written + len <= self.total() || self.truncating {
            self.written += len;
            self.headers.push(header);
            return;
        }
        // enable truncation, reset, and replay the existing items truncated
        self.truncating = true;
        self.written = 0;
        self.headers.clear();
        tracing::error!("exceeded max warning header size, truncating");
        let item = self.item();
        for (agent, text) in &self.ordered {
            let mut text = text.clone();
            let mut len = text.chars().count();
            if len > item {
                text = Self::truncate(&text, item);
                len = item;
            }
            if let Some(h) = new_warning_header(agent, &text) {
                self.written += len;
                self.headers.push(h);
            }
        }
    }

    /// The `Warning` header values written so far.
    pub fn headers(&self) -> &[String] {
        &self.headers
    }
}

/// `(agent, text)` of a `299 <agent> "<quoted>"` header value.
fn parse_warning_header(value: &str) -> Option<(String, String)> {
    let rest = value.strip_prefix("299 ")?;
    let (agent, quoted) = rest.split_once(' ')?;
    let inner = quoted.strip_prefix('"')?.strip_suffix('"')?;
    let mut text = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            text.push(chars.next()?);
        } else {
            text.push(c);
        }
    }
    Some((agent.to_string(), text))
}

/// `AddWarningsHandler`: add the route's warnings before the handler runs
/// and merge them with the handler's own through the recorder.
pub async fn add_endpoint_warnings(req: Request, next: Next) -> Response {
    let meta = crate::apiserver_request_metrics::request_meta(&req);
    let warnings = if meta.is_resource_request {
        endpoint_warnings(
            &meta.api_group,
            &meta.api_version,
            &meta.resource,
            &meta.subresource,
        )
    } else {
        Vec::new()
    };
    let mut resp = next.run(req).await;
    if warnings.is_empty() {
        return resp;
    }
    let mut recorder = Recorder::new();
    for w in &warnings {
        recorder.add_warning("", w);
    }
    for v in resp.headers().get_all(header::WARNING) {
        if let Some((agent, text)) = v.to_str().ok().and_then(parse_warning_header) {
            recorder.add_warning(&agent, &text);
        }
    }
    resp.headers_mut().remove(header::WARNING);
    for h in recorder.headers() {
        if let Ok(v) = HeaderValue::from_str(h) {
            resp.headers_mut().append(header::WARNING, v);
        }
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(args: &[(&str, &str)]) -> Vec<String> {
        let mut r = Recorder::new();
        for (a, t) in args {
            r.add_warning(a, t);
        }
        r.headers().to_vec()
    }

    /// `Test_recorder_AddWarning` (filters/warning_test.go:25).
    #[test]
    fn recorder_add_warning() {
        assert!(run(&[]).is_empty());
        assert!(run(&[("", "")]).is_empty());
        assert_eq!(run(&[("", "mytext")]), [r#"299 - "mytext""#]);
        assert_eq!(run(&[("myagent", "mytext")]), [r#"299 myagent "mytext""#]);
        assert_eq!(
            run(&[("myagent", "mytext"), ("myagent2", "mytext")]),
            [r#"299 myagent "mytext""#]
        );
        assert_eq!(
            run(&[("", "mytext1"), ("", "mytext2"), ("", "mytext3")]),
            [
                r#"299 - "mytext1""#,
                r#"299 - "mytext2""#,
                r#"299 - "mytext3""#
            ]
        );
        assert!(run(&[("my agent", "mytext")]).is_empty());
        assert!(run(&[("myagent", "my\ntext")]).is_empty());
    }

    /// `TestTruncation` (filters/warning_test.go:102).
    #[test]
    fn recorder_truncation() {
        let mut r = Recorder::with_limits(25, 5);
        r.add_warning("", "aaaaaaaaaa");
        r.add_warning("-", "aaaaaaaaaa");
        r.add_warning("", "bb");
        r.add_warning("", "ccc");
        r.add_warning("", "Iñtërnâtiô");
        assert_eq!(
            r.headers(),
            [
                r#"299 - "aaaaaaaaaa""#,
                r#"299 - "bb""#,
                r#"299 - "ccc""#,
                r#"299 - "Iñtërnâtiô""#
            ]
        );
        r.add_warning("", "e");
        r.add_warning("", "ffffffffff");
        r.add_warning("", "ffffffffff");
        r.add_warning("", "gggggggggg");
        r.add_warning("", "h");
        assert_eq!(
            r.headers(),
            [
                r#"299 - "aaaaa""#,
                r#"299 - "bb""#,
                r#"299 - "ccc""#,
                r#"299 - "Iñtër""#,
                r#"299 - "e""#,
                r#"299 - "fffff""#,
                r#"299 - "ggggg""#
            ]
        );
    }

    #[test]
    fn served_deprecated_kind_warns() {
        // resource.k8s.io/v1beta1 DeviceClass: deprecated 1.35, removed 1.38.
        let w = endpoint_warnings("resource.k8s.io", "v1beta1", "deviceclasses", "");
        assert_eq!(
            w,
            ["resource.k8s.io/v1beta1 DeviceClass is deprecated in v1.35+, unavailable in v1.38+"]
        );
    }

    /// `removeDeletedKinds`: a removed version has no route, so no warning.
    #[test]
    fn removed_kind_has_no_route_so_no_warning() {
        assert!(endpoint_warnings("apps", "v1beta1", "deployments", "").is_empty());
    }

    #[test]
    fn stable_kind_does_not_warn() {
        assert!(endpoint_warnings("apps", "v1", "deployments", "").is_empty());
    }

    #[test]
    fn parses_what_the_handlers_write() {
        let h = crate::handlers::validation::format_warning_header(r#"a "b" \c"#);
        assert_eq!(
            parse_warning_header(&h),
            Some(("-".to_string(), r#"a "b" \c"#.to_string()))
        );
    }

    async fn serve(uri: &str, handler_warnings: &'static [&'static str]) -> Vec<String> {
        use axum::{body::Body, http::Request, middleware, routing::get, Router};
        use tower::ServiceExt;
        let app = Router::new()
            .route(
                "/apis/resource.k8s.io/v1beta1/deviceclasses",
                get(move || async move {
                    let mut r = axum::response::Response::new(Body::empty());
                    for w in handler_warnings {
                        r.headers_mut().append(
                            header::WARNING,
                            HeaderValue::from_str(
                                &crate::handlers::validation::format_warning_header(w),
                            )
                            .unwrap(),
                        );
                    }
                    r
                }),
            )
            .route("/apis/apps/v1/deployments", get(|| async { "ok" }))
            .layer(middleware::from_fn(add_endpoint_warnings));
        let resp = app
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        resp.headers()
            .get_all(header::WARNING)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect()
    }

    /// AddWarningsHandler: the route's warning is added before the handler's
    /// own, and a repeated text is recorded once.
    #[tokio::test]
    async fn middleware_puts_route_warning_first_and_dedups() {
        const ROUTE: &str =
            "resource.k8s.io/v1beta1 DeviceClass is deprecated in v1.35+, unavailable in v1.38+";
        let got = serve(
            "/apis/resource.k8s.io/v1beta1/deviceclasses",
            &["handler says hi", ROUTE],
        )
        .await;
        assert_eq!(
            got,
            [
                format!("299 - \"{ROUTE}\""),
                "299 - \"handler says hi\"".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn middleware_leaves_stable_routes_alone() {
        assert!(serve("/apis/apps/v1/deployments", &[]).await.is_empty());
    }
}
