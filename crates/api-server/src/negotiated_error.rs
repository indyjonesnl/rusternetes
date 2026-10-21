//! `ErrorNegotiated` (#3517): serialize an error `Status` per the request's
//! `Accept` header.
//!
//! Ported from
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/responsewriters/writers.go`
//!   (`ErrorNegotiated` :381-397, `WriteObjectNegotiated` :323-361,
//!   `WriteRawJSON` :400-409),
//! - `.../handlers/negotiation/negotiate.go` (`NegotiateOutputMediaType`
//!   :51-63, `isPrettyPrint` :102-118, `DefaultEndpointRestrictions` :137-147,
//!   `acceptMediaTypeOptions`, `NegotiateMediaTypeOptions`),
//! - `vendor/github.com/munnerz/goautoneg/autoneg.go` (`ParseAccept`),
//! - `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/codec_factory.go`
//!   (`newSerializersForScheme`: json, yaml, protobuf, in that order; the
//!   JSON serializer has a pretty variant, `NewCodecFactory` sets
//!   `Pretty: true`) and `.../serializer/json/json.go` (`doEncode`).
//!
//! The negotiated serializer is `legacyscheme.Codecs`; the supported media
//! types are therefore `application/json`, `application/yaml` and
//! `application/vnd.kubernetes.protobuf`. (The CBOR serializer is only added
//! with the alpha `CBORServingAndStorage` gate, which is off.)
//!
//! Not ported: response compression (`negotiateContentEncoding`) only kicks in
//! above a 128KiB body, which a `Status` never reaches.

use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use rusternetes_common::types::Status;
use std::collections::HashMap;

const JSON: &str = "application/json";
const YAML: &str = "application/yaml";
const PROTOBUF: &str = "application/vnd.kubernetes.protobuf";

/// A `runtime.SerializerInfo` of the negotiated serializer, in
/// `SupportedMediaTypes()` order.
struct Serializer {
    media_type: &'static str,
    ty: &'static str,
    sub_type: &'static str,
    /// `StreamSerializer != nil`: json and protobuf.
    streams: bool,
    /// `PrettySerializer != nil`: json only.
    pretty: bool,
}

const SERIALIZERS: [Serializer; 3] = [
    Serializer {
        media_type: JSON,
        ty: "application",
        sub_type: "json",
        streams: true,
        pretty: true,
    },
    Serializer {
        media_type: YAML,
        ty: "application",
        sub_type: "yaml",
        streams: false,
        pretty: false,
    },
    Serializer {
        media_type: PROTOBUF,
        ty: "application",
        sub_type: "vnd.kubernetes.protobuf",
        streams: true,
        pretty: false,
    },
];

/// `goautoneg.Accept`.
struct Accept {
    ty: String,
    sub_type: String,
    q: f64,
    params: HashMap<String, String>,
}

/// `strings.TrimFunc(s, r == ' ')`.
fn trim(s: &str) -> &str {
    s.trim_matches(' ')
}

/// `nextSplitElement`.
fn next_split(s: &str, sep: char) -> (&str, &str) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    }
}

/// `goautoneg.ParseAccept`: the clauses of an Accept header, sorted.
fn parse_accept(header: &str) -> Vec<Accept> {
    let mut accept = Vec::new();
    let mut remaining = header;
    while !remaining.is_empty() {
        let part;
        (part, remaining) = next_split(remaining, ',');
        let part = trim(part);
        let mut a = Accept {
            ty: String::new(),
            sub_type: String::new(),
            q: 1.0,
            params: HashMap::new(),
        };
        let (sp, mut remaining_part) = next_split(part, ';');
        let (sp0, sp_remaining) = next_split(sp, '/');
        a.ty = trim(sp0).to_string();
        if sp_remaining.is_empty() {
            if a.ty == "*" {
                a.sub_type = "*".to_string();
            } else {
                continue;
            }
        } else {
            let (sp1, sp_remaining) = next_split(sp_remaining, '/');
            if !sp_remaining.is_empty() {
                continue;
            }
            a.sub_type = trim(sp1).to_string();
        }
        if remaining_part.is_empty() {
            accept.push(a);
            continue;
        }
        while !remaining_part.is_empty() {
            let sp;
            (sp, remaining_part) = next_split(remaining_part, ';');
            let (sp0, sp_remaining) = next_split(sp, '=');
            if sp_remaining.is_empty() {
                continue;
            }
            let (sp1, sp_remaining) = next_split(sp_remaining, '=');
            if !sp_remaining.is_empty() {
                continue;
            }
            let token = trim(sp0);
            if token == "q" {
                // `strconv.ParseFloat(sp1, 32)`: 0 on error.
                a.q = sp1.parse::<f32>().map(f64::from).unwrap_or(0.0);
            } else {
                a.params.insert(token.to_string(), trim(sp1).to_string());
            }
        }
        accept.push(a);
    }
    // `sort.Sort(accept)` with goautoneg's (non-total) `Less`. Go sorts slices
    // of fewer than 13 elements by insertion sort, which is what is done here
    // so the order matches for any realistic header.
    for i in 1..accept.len() {
        let mut j = i;
        while j > 0 && less(&accept[j], &accept[j - 1]) {
            accept.swap(j, j - 1);
            j -= 1;
        }
    }
    accept
}

/// `acceptSlice.Less`.
fn less(ai: &Accept, aj: &Accept) -> bool {
    ai.q > aj.q || (ai.ty != "*" && aj.ty == "*") || (ai.sub_type != "*" && aj.sub_type == "*")
}

/// `acceptMediaTypeOptions` under `DefaultEndpointRestrictions`
/// (negotiate.go:137-147): `Some(pretty)` when the clause is allowed.
fn accept_media_type_options(
    params: &HashMap<String, String>,
    accepts: &Serializer,
) -> Option<bool> {
    let mut pretty = false;
    let mut convert = false;
    for (k, v) in params {
        match k.as_str() {
            // A media-type parameter, not a query parameter: upstream decodes
            // it as `v == "1"` (negotiate.go:230-231), not with ParseBool.
            "pretty" => pretty = v == "1",
            "as" | "g" | "v" => convert = true,
            // AllowsStreamSchema(s) == (s == "watch").
            "stream" if !v.is_empty() && (!accepts.streams || v != "watch") => return None,
            // AllowsServerVersion is always false.
            "sv" if !v.is_empty() => return None,
            _ => {}
        }
    }
    // AllowsMediaTypeTransform(.., gvk) == (gvk == nil).
    if convert {
        return None;
    }
    Some(pretty)
}

/// `NegotiateMediaTypeOptions`: the serializer and its `pretty` option.
fn negotiate_media_type_options(header: &str) -> Option<(&'static Serializer, bool)> {
    if header.is_empty() {
        return Some((&SERIALIZERS[0], false));
    }
    for clause in parse_accept(header) {
        for accepts in &SERIALIZERS {
            let matches = (clause.ty == accepts.ty && clause.sub_type == accepts.sub_type)
                || (clause.ty == accepts.ty && clause.sub_type == "*")
                || (clause.ty == "*" && clause.sub_type == "*");
            if matches {
                if let Some(pretty) = accept_media_type_options(&clause.params, accepts) {
                    return Some((accepts, pretty));
                }
            }
        }
    }
    None
}

/// `isPrettyPrint` (negotiate.go:102-118).
fn is_pretty_print(headers: &HeaderMap, raw_query: Option<&str>) -> bool {
    if let Some(q) = raw_query.filter(|q| q.contains("pretty")) {
        let pp = url::form_urlencoded::parse(q.as_bytes())
            .find(|(k, _)| k == "pretty")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        if !pp.is_empty() {
            // `strconv.ParseBool`.
            return matches!(pp.as_str(), "1" | "t" | "T" | "TRUE" | "true" | "True");
        }
    }
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    ua.starts_with("curl") || ua.starts_with("Wget") || ua.starts_with("Mozilla/5.0")
}

/// `encoding/json` escapes `<`, `>`, `&`, U+2028 and U+2029 in strings.
fn go_html_escape(json: String) -> String {
    json.replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn response(status: StatusCode, media_type: &'static str, body: Vec<u8>) -> Response {
    let mut r = (status, body).into_response();
    r.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(media_type));
    r
}

/// `WriteRawJSON` (writers.go:400-409).
fn write_raw_json(status: StatusCode, object: &Status) -> Response {
    match serde_json::to_string_pretty(object) {
        Ok(s) => response(status, JSON, go_html_escape(s).into_bytes()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `ErrorNegotiated` (writers.go:381-397) with
/// `metav1.Unversioned` as the group version and
/// `negotiation.DefaultEndpointRestrictions`.
pub fn error_negotiated(status: &Status, headers: &HeaderMap, raw_query: Option<&str>) -> Response {
    let code = status.code.unwrap_or(0);
    let status_code = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let retry_after = status
        .details
        .as_ref()
        .and_then(|d| d.retry_after_seconds)
        .filter(|s| *s > 0);

    let mut response = if status_code == StatusCode::NO_CONTENT {
        status_code.into_response()
    } else {
        write_object_negotiated(status, status_code, headers, raw_query)
    };
    if let Some(delay) = retry_after {
        if let Ok(v) = HeaderValue::from_str(&delay.to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, v);
        }
    }
    response
}

/// `WriteObjectNegotiated` (writers.go:323-361) for an error `Status`.
fn write_object_negotiated(
    status: &Status,
    status_code: StatusCode,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let Ok(object) = serde_json::to_value(status) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some((serializer, pretty_option)) = negotiate_media_type_options(accept) else {
        // The original (non-successful) error must not be hidden behind a
        // negotiation problem (writers.go:335-340).
        return write_raw_json(status_code, status);
    };
    let mut audit_response = None;
    if let Ok(v) = serde_json::to_value(status) {
        audit_response = Some(rusternetes_common::audit::AuditResponseObject(v));
    }
    let mut resp = match serializer.media_type {
        JSON => {
            let pretty =
                (pretty_option || is_pretty_print(headers, raw_query)) && serializer.pretty;
            if pretty {
                // `json.MarshalIndent(obj, "", "  ")`: no trailing newline.
                write_raw_json(status_code, status)
            } else {
                // `json.NewEncoder(w).Encode(obj)`: trailing newline.
                let mut s = go_html_escape(serde_json::to_string(status).unwrap_or_default());
                s.push('\n');
                response(status_code, JSON, s.into_bytes())
            }
        }
        YAML => match serde_yaml::to_string(&object) {
            Ok(s) => response(status_code, YAML, s.into_bytes()),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        _ => {
            let json = serde_json::to_string(status).unwrap_or_default();
            let bytes = rusternetes_middleware::response::encode_native_or_wrapped(
                json.as_bytes(),
                &status.api_version,
                &status.kind,
            );
            response(status_code, PROTOBUF, bytes)
        }
    };
    if let Some(a) = audit_response {
        resp.extensions_mut().insert(a);
    }
    resp
}

/// `RespondWithError` (errors.go:40-43): `X-Content-Type-Options: nosniff`,
/// then `ErrorNegotiated`.
pub fn respond_with_error(
    status: &Status,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let mut r = error_negotiated(status, headers, raw_query);
    r.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    r
}

/// The response-layer counterpart of upstream calling `ErrorNegotiated` at
/// every error site (`scope.err` -> `responsewriters.ErrorNegotiated`,
/// `endpoints/handlers/rest.go`; `responsewriters.ErrorNegotiated` itself
/// :381-397). `Error::into_response` has no request, so it tags its JSON body
/// with [`ErrorStatus`]; this middleware re-encodes such a body per the
/// request's `Accept` (json / pretty / yaml / protobuf). Other headers and
/// extensions (audit object, `Allow`, ...) are kept; `Retry-After` is
/// re-derived from `details.retryAfterSeconds` as upstream does.
pub async fn negotiate_error_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let headers = req.headers().clone();
    let query = req.uri().query().map(str::to_owned);
    let mut resp = next.run(req).await;
    let Some(rusternetes_common::error::ErrorStatus(status)) =
        resp.extensions_mut()
            .remove::<rusternetes_common::error::ErrorStatus>()
    else {
        return resp;
    };
    let (old, _) = resp.into_parts();
    let mut new = error_negotiated(&status, &headers, query.as_deref());
    for (name, value) in old.headers.iter() {
        if name == header::CONTENT_TYPE
            || name == header::CONTENT_LENGTH
            || name == header::RETRY_AFTER
        {
            continue;
        }
        new.headers_mut().insert(name.clone(), value.clone());
    }
    new.extensions_mut().extend(old.extensions);
    new
}
