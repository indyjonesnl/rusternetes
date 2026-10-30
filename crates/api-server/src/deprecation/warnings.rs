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

    pub fn with_limits(total: usize, item: usize) -> Self {
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
pub fn parse_warning_header(value: &str) -> Option<(String, String)> {
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
