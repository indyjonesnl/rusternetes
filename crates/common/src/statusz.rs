//! Port of `k8s.io/apiserver/pkg/server/statusz` (the `/statusz` z-page).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/server/statusz/statusz.go`
//! * `staging/src/k8s.io/apiserver/pkg/server/statusz/registry.go`
//! * `staging/src/k8s.io/apiserver/pkg/server/statusz/textserializer.go`
//! * `staging/src/k8s.io/apiserver/pkg/server/statusz/negotiate/negotiation.go`
//! * `staging/src/k8s.io/apiserver/pkg/server/statusz/api/v1alpha1/types.go`
//! * content negotiation: `apiserver/pkg/endpoints/handlers/negotiation/negotiate.go`
//!   and `vendor/github.com/munnerz/goautoneg/autoneg.go`
//! * error rendering: `apiserver/pkg/endpoints/handlers/responsewriters/{writers,status}.go`
//! * gate-gated install: `cmd/kube-controller-manager/app/controllermanager.go:225-234`,
//!   `cmd/kube-scheduler/app/server.go:394-396` (`ComponentStatusz`,
//!   `staging/src/k8s.io/component-base/zpages/features/kube_features.go:43-45`).
//!
//! Deliberate reductions (no upstream equivalent in this tree): the serializer
//! set is JSON + text/plain only; `application/yaml` and
//! `application/vnd.kubernetes.protobuf` negotiate (they are in the codec
//! factory's list) and then hit `handleStatusz`'s `default:` branch, exactly as
//! upstream. gzip response compression (`APIResponseCompression`) is not
//! applied; the body is far below its 128 KiB threshold.

use crate::compatibility::{EffectiveVersion, DEFAULT_KUBE_BINARY_VERSION};
use crate::feature_gates::Version;
use chrono::{DateTime, TimeZone, Utc};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::collections::HashMap;

/// `DefaultStatuszPath` (statusz.go).
pub const DEFAULT_STATUSZ_PATH: &str = "/statusz";
/// `Kind` (statusz.go).
pub const KIND: &str = "Statusz";
/// `GroupName` (statusz.go).
pub const GROUP_NAME: &str = "config.k8s.io";
/// `Version` (statusz.go).
pub const VERSION: &str = "v1alpha1";

/// `delimiters` (statusz.go): one is picked at random per text response.
pub const DELIMITERS: [&str; 4] = [":", ": ", "=", " "];

/// `nonDebuggingEndpoints` (statusz.go).
const NON_DEBUGGING_ENDPOINTS: [&str; 5] = ["/apis", "/api", "/openid", "/openapi", "/.well-known"];

/// `headerFmt` (statusz.go) minus the component name.
const HEADER_WARNING: &str = "Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.";

/// `statuszRegistry` (registry.go).
pub trait StatuszRegistry: Send + Sync {
    fn process_start_time(&self) -> DateTime<Utc>;
    fn go_version(&self) -> String;
    fn binary_version(&self) -> Version;
    fn emulation_version(&self) -> Option<Version>;
    fn paths(&self) -> Vec<String>;
    fn deprecated_versions(&self) -> HashMap<String, bool>;
}

/// `registry` (registry.go).
#[derive(Debug, Clone, Default)]
pub struct Registry {
    /// `effectiveVersion` (nil upstream when none was supplied).
    pub effective_version: Option<EffectiveVersion>,
    /// `listedPaths`: "an alphabetically sorted list of paths to be reported at /".
    pub listed_paths: Option<Vec<String>>,
    /// `deprecatedVersionsMap`.
    pub deprecated_versions_map: HashMap<String, bool>,
}

impl Registry {
    /// `NewRegistry` (statusz.go).
    pub fn new(effective_version: Option<EffectiveVersion>) -> Self {
        Self {
            effective_version,
            listed_paths: None,
            deprecated_versions_map: HashMap::new(),
        }
    }

    /// `WithListedPaths` (registry.go).
    pub fn with_listed_paths(mut self, listed_paths: Vec<String>) -> Self {
        self.listed_paths = Some(listed_paths);
        self
    }
}

/// `compbasemetrics.GetProcessStart` (component-base/metrics/processstarttime.go):
/// the `process_start_time_seconds` of client_golang's process collector, i.e.
/// `/proc/self/stat` starttime (clock ticks, `USER_HZ` = 100) plus the boot
/// time from `/proc/stat`.
fn get_process_start() -> Result<f64, String> {
    const USER_HZ: f64 = 100.0;
    let stat = std::fs::read_to_string("/proc/self/stat").map_err(|e| e.to_string())?;
    // comm may contain spaces/parens: fields resume after the last ')'.
    let rest = stat.rsplit_once(')').ok_or("malformed /proc/self/stat")?.1;
    // `rest` starts at field 3 (state); starttime is field 22.
    let starttime: f64 = rest
        .split_whitespace()
        .nth(19)
        .ok_or("short /proc/self/stat")?
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    let btime: f64 = std::fs::read_to_string("/proc/stat")
        .map_err(|e| e.to_string())?
        .lines()
        .find_map(|l| l.strip_prefix("btime "))
        .ok_or("no btime in /proc/stat")?
        .trim()
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    Ok(btime + starttime / USER_HZ)
}

impl StatuszRegistry for Registry {
    /// `processStartTime` (registry.go): an error is logged and the zero
    /// start (`time.Unix(0, 0)`) used.
    fn process_start_time(&self) -> DateTime<Utc> {
        let start = match get_process_start() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Could not get process start time, {e}");
                0.0
            }
        };
        Utc.timestamp_opt(start as i64, 0)
            .single()
            .unwrap_or_default()
    }

    /// `utilversion.Get().GoVersion`: the toolchain-agnostic marker the
    /// rest of the tree reports for this field (`kubectl version`).
    fn go_version(&self) -> String {
        "rust".to_string()
    }

    /// `binaryVersion` (registry.go:67-72). Without an effective version
    /// upstream parses the build's `utilversion.Get()`; the build's binary
    /// version here is [`DEFAULT_KUBE_BINARY_VERSION`] (apiserver/pkg/util/
    /// compatibility/version.go:34-39 fallback).
    fn binary_version(&self) -> Version {
        match &self.effective_version {
            Some(ev) => ev.binary_version(),
            None => DEFAULT_KUBE_BINARY_VERSION,
        }
    }

    /// `emulationVersion` (registry.go:74-79).
    fn emulation_version(&self) -> Option<Version> {
        self.effective_version
            .as_ref()
            .map(|ev| ev.emulation_version())
    }

    /// `paths` (registry.go:81-86).
    fn paths(&self) -> Vec<String> {
        self.listed_paths.clone().unwrap_or_default()
    }

    /// `deprecatedVersions` (registry.go:88-90).
    fn deprecated_versions(&self) -> HashMap<String, bool> {
        self.deprecated_versions_map.clone()
    }
}

/// `v1alpha1.Statusz` (api/v1alpha1/types.go).
#[derive(Debug, Clone, PartialEq)]
pub struct Statusz {
    /// `ObjectMeta.Name`.
    pub name: String,
    pub start_time: DateTime<Utc>,
    pub uptime_seconds: i64,
    pub go_version: String,
    pub binary_version: String,
    pub emulation_version: String,
    pub paths: Vec<String>,
}

impl Serialize for Statusz {
    /// JSON tags of types.go: `emulationVersion,omitempty`; `paths` has no
    /// omitempty so a nil slice encodes as `null`; `metadata` carries the
    /// zero `creationTimestamp` as `null`; `metav1.Time` is RFC 3339 UTC to the
    /// second.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Meta<'a> {
            name: &'a str,
            creation_timestamp: Option<()>,
        }
        let n = if self.emulation_version.is_empty() {
            8
        } else {
            9
        };
        let mut st = s.serialize_struct("Statusz", n)?;
        st.serialize_field("kind", KIND)?;
        st.serialize_field("apiVersion", &format!("{GROUP_NAME}/{VERSION}"))?;
        st.serialize_field(
            "metadata",
            &Meta {
                name: &self.name,
                creation_timestamp: None,
            },
        )?;
        st.serialize_field(
            "startTime",
            &self.start_time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        )?;
        st.serialize_field("uptimeSeconds", &self.uptime_seconds)?;
        st.serialize_field("goVersion", &self.go_version)?;
        st.serialize_field("binaryVersion", &self.binary_version)?;
        if !self.emulation_version.is_empty() {
            st.serialize_field("emulationVersion", &self.emulation_version)?;
        }
        if self.paths.is_empty() {
            st.serialize_field("paths", &None::<Vec<String>>)?;
        } else {
            st.serialize_field("paths", &self.paths)?;
        }
        st.end()
    }
}

/// `statusz(componentName, reg)` (statusz.go).
pub fn statusz(component_name: &str, reg: &dyn StatuszRegistry) -> Statusz {
    let start_time = reg.process_start_time();
    let uptime_seconds = (Utc::now() - start_time).num_seconds().max(0);
    Statusz {
        name: component_name.to_string(),
        start_time,
        uptime_seconds,
        go_version: reg.go_version(),
        binary_version: reg.binary_version().to_string(),
        emulation_version: reg
            .emulation_version()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        paths: aggregate_paths(&reg.paths()),
    }
}

/// `uptime` (statusz.go).
pub fn uptime(t: DateTime<Utc>) -> String {
    let up_since = (Utc::now() - t).num_seconds();
    format!(
        "{} hr {:02} min {:02} sec",
        up_since / 3600,
        (up_since / 60) % 60,
        up_since % 60
    )
}

/// `aggregatePaths` (statusz.go): the first path segment of each listed path,
/// de-duplicated and sorted, minus [`NON_DEBUGGING_ENDPOINTS`].
pub fn aggregate_paths(listed_paths: &[String]) -> Vec<String> {
    let mut paths = std::collections::BTreeSet::new();
    for listed in listed_paths {
        let parts: Vec<&str> = listed.split('/').collect();
        if parts.len() < 2 || parts[1].is_empty() {
            continue;
        }
        let folder = format!("/{}", parts[1]);
        if !NON_DEBUGGING_ENDPOINTS.contains(&folder.as_str()) {
            paths.insert(folder);
        }
    }
    paths.into_iter().collect()
}

/// A rendered HTTP response.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

// ---------------------------------------------------------------------------
// Content negotiation (negotiate.go + goautoneg + statusz/negotiate).
// ---------------------------------------------------------------------------

/// The codec factory's `SupportedMediaTypes()` (codec_factory.go:60-95, plus
/// the `text/plain` `WithSerializer` entry of statusz.go `Install`): json,
/// yaml, protobuf, text/plain.
const SUPPORTED: [(&str, &str, &str); 4] = [
    ("application/json", "application", "json"),
    ("application/yaml", "application", "yaml"),
    (
        "application/vnd.kubernetes.protobuf",
        "application",
        "vnd.kubernetes.protobuf",
    ),
    ("text/plain", "text", "plain"),
];

/// `schema.GroupVersionKind` pieces collected from the `g`/`v`/`as` params.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Gvk {
    pub(crate) group: String,
    pub(crate) version: String,
    pub(crate) kind: String,
}

/// `goautoneg.Accept`.
#[derive(Debug)]
struct Accept {
    typ: String,
    sub_type: String,
    q: f64,
    params: Vec<(String, String)>,
}

/// `goautoneg.ParseAccept` (autoneg.go). The sort is Go's `sort.Sort` with
/// `acceptSlice.Less`; for the <= 12 clauses `insertionSort` handles in
/// `pdqsort` this is an insertion sort, which is what is used here.
fn parse_accept(header: &str) -> Vec<Accept> {
    fn split_once_sep(s: &str, sep: char) -> (&str, &str) {
        match s.find(sep) {
            Some(i) => (&s[..i], &s[i + 1..]),
            None => (s, ""),
        }
    }
    let trim = |s: &str| s.trim_matches(' ').to_string();
    let mut accept: Vec<Accept> = Vec::new();
    let mut remaining = header;
    while !remaining.is_empty() {
        let (part, rest) = split_once_sep(remaining, ',');
        remaining = rest;
        let part = part.trim_matches(' ');
        let (sp, mut remaining_part) = split_once_sep(part, ';');
        let (sp0, sp_remaining) = split_once_sep(sp, '/');
        let typ = trim(sp0);
        let sub_type;
        if sp_remaining.is_empty() {
            if typ == "*" {
                sub_type = "*".to_string();
            } else {
                continue;
            }
        } else {
            let (sp1, sp_rem2) = split_once_sep(sp_remaining, '/');
            if !sp_rem2.is_empty() {
                continue;
            }
            sub_type = trim(sp1);
        }
        let mut a = Accept {
            typ,
            sub_type,
            q: 1.0,
            params: Vec::new(),
        };
        if remaining_part.is_empty() {
            accept.push(a);
            continue;
        }
        while !remaining_part.is_empty() {
            let (sp, rp) = split_once_sep(remaining_part, ';');
            remaining_part = rp;
            let (k, v_rem) = split_once_sep(sp, '=');
            if v_rem.is_empty() {
                continue;
            }
            let (v, extra) = split_once_sep(v_rem, '=');
            if !extra.is_empty() {
                continue;
            }
            let token = trim(k);
            if token == "q" {
                a.q = v.parse::<f32>().map(f64::from).unwrap_or(0.0);
            } else {
                // Go map semantics: a repeated key overwrites.
                let val = trim(v);
                match a.params.iter_mut().find(|(pk, _)| *pk == token) {
                    Some(slot) => slot.1 = val,
                    None => a.params.push((token, val)),
                }
            }
        }
        accept.push(a);
    }
    // acceptSlice.Less
    let less = |ai: &Accept, aj: &Accept| {
        ai.q > aj.q
            || (ai.typ != "*" && aj.typ == "*")
            || (ai.sub_type != "*" && aj.sub_type == "*")
    };
    for i in 1..accept.len() {
        let mut j = i;
        while j > 0 && less(&accept[j], &accept[j - 1]) {
            accept.swap(j, j - 1);
            j -= 1;
        }
    }
    accept
}

/// `StatuszEndpointRestrictions.AllowsMediaTypeTransform` (negotiate/negotiation.go).
pub(crate) fn allows_media_type_transform(
    mime_type: &str,
    mime_sub_type: &str,
    gvk: &Option<Gvk>,
    kind: &str,
) -> bool {
    if mime_type == "text" && mime_sub_type == "plain" {
        return gvk.is_none();
    }
    matches!(gvk, Some(g) if g.group == GROUP_NAME && g.version == VERSION && g.kind == kind)
}

/// The result of `MediaTypeOptions` for one accepted serializer.
pub struct Negotiated {
    pub(crate) media_type: &'static str,
    pub(crate) pretty: bool,
    pub(crate) convert: Option<Gvk>,
}

/// `NegotiateMediaTypeOptions` + `acceptMediaTypeOptions` (negotiate.go) with
/// `StatuszEndpointRestrictions` (`AllowsServerVersion`/`AllowsStreamSchema`
/// always false).
pub fn negotiate_media_type_options(header: &str, kind: &str) -> Option<Negotiated> {
    if header.is_empty() {
        let (mt, _, _) = SUPPORTED[0];
        return Some(Negotiated {
            media_type: mt,
            pretty: false,
            convert: None,
        });
    }
    for clause in parse_accept(header) {
        for (media_type, t, st) in SUPPORTED {
            let matches = (clause.typ == t && clause.sub_type == st)
                || (clause.typ == t && clause.sub_type == "*")
                || (clause.typ == "*" && clause.sub_type == "*");
            if !matches {
                continue;
            }
            if let Some(n) = accept_media_type_options(&clause.params, media_type, t, st, kind) {
                return Some(n);
            }
        }
    }
    None
}

fn accept_media_type_options(
    params: &[(String, String)],
    media_type: &'static str,
    t: &str,
    st: &str,
    kind: &str,
) -> Option<Negotiated> {
    let mut convert: Option<Gvk> = None;
    let mut pretty = false;
    for (k, v) in params {
        match k.as_str() {
            "as" => convert.get_or_insert_with(Gvk::default).kind = v.clone(),
            "g" => convert.get_or_insert_with(Gvk::default).group = v.clone(),
            "v" => convert.get_or_insert_with(Gvk::default).version = v.clone(),
            // AllowsStreamSchema is always false (and no statusz serializer
            // has a stream serializer).
            "stream" if !v.is_empty() => return None,
            // AllowsServerVersion is always false.
            "sv" if !v.is_empty() => return None,
            "pretty" => pretty = v == "1",
            _ => {}
        }
    }
    if !allows_media_type_transform(t, st, &convert, kind) {
        return None;
    }
    Some(Negotiated {
        media_type,
        pretty,
        convert,
    })
}

/// `isPrettyPrint` (negotiate.go).
pub fn is_pretty_print(raw_query: &str, user_agent: &str) -> bool {
    if raw_query.contains("pretty") {
        let pp = raw_query
            .split('&')
            .filter_map(|kv| kv.split_once('=').or(Some((kv, ""))))
            .find(|(k, _)| *k == "pretty")
            .map(|(_, v)| v)
            .unwrap_or("");
        if !pp.is_empty() {
            // strconv.ParseBool
            return matches!(pp, "1" | "t" | "T" | "TRUE" | "true" | "True");
        }
    }
    user_agent.starts_with("curl")
        || user_agent.starts_with("Wget")
        || user_agent.starts_with("Mozilla/5.0")
}

/// `WriteRawJSON` of an `ErrorToAPIStatus` status (writers.go:399-409,
/// status.go:34-83). Every error the statusz handler emits is rendered this
/// way: `ErrorNegotiated` re-negotiates the original `Accept` under
/// `DefaultEndpointRestrictions`, which rejects the `g/v/as` conversion, so
/// `WriteObjectNegotiated` falls back to raw JSON for the failure status.
pub(crate) fn error_response(code: u16, reason: &str, message: &str) -> Response {
    let status = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": code,
    });
    Response {
        status: code,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: serde_json::to_vec_pretty(&status).unwrap_or_default(),
    }
}

/// `errNotAcceptable` (negotiation/errors.go): reason `NotAcceptable`.
pub fn not_acceptable() -> Response {
    let supported: Vec<&str> = SUPPORTED.iter().map(|(m, _, _)| *m).collect();
    error_response(
        406,
        "NotAcceptable",
        &format!(
            "only the following media types are accepted: {}",
            supported.join(", ")
        ),
    )
}

/// A plain (non-`metav1.Status`) error: `ErrorToAPIStatus` default branch.
pub fn plain_error(message: &str) -> Response {
    error_response(500, "Unknown", message)
}

/// `html.EscapeString`.
pub(crate) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('\'', "&#39;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&#34;")
}

/// Go `time.UnixDate` of a `time.Time` in the local zone
/// (`"Mon Jan _2 15:04:05 MST 2006"`); zones without an abbreviation print
/// as a numeric offset like Go does.
fn unix_date(t: DateTime<Utc>) -> String {
    let l = t.with_timezone(&chrono::Local);
    let secs = chrono::Offset::fix(l.offset()).local_minus_utc();
    let tz = if secs == 0 {
        "UTC".to_string()
    } else {
        let (sign, s) = if secs < 0 { ('-', -secs) } else { ('+', secs) };
        if (s / 60) % 60 == 0 {
            format!("{sign}{:02}", s / 3600)
        } else {
            format!("{sign}{:02}{:02}", s / 3600, (s / 60) % 60)
        }
    };
    format!("{} {tz} {}", l.format("%a %b %e %H:%M:%S"), l.format("%Y"))
}

/// `statuszTextSerializer.Encode` (textserializer.go).
fn encode_text(component_name: &str, obj: &Statusz, delim: &str) -> Vec<u8> {
    let delim = html_escape(delim);
    let start_time = html_escape(&unix_date(obj.start_time));
    let uptime_str = html_escape(&uptime(obj.start_time));
    let go_version = html_escape(&obj.go_version);
    let binary_version = html_escape(&obj.binary_version);
    let emulation = if obj.emulation_version.is_empty() {
        String::new()
    } else {
        format!(
            "Emulation version{delim} {}",
            html_escape(&obj.emulation_version)
        )
    };
    let mut paths = obj.paths.join(" ");
    if !paths.is_empty() {
        paths = format!("Paths{delim} {}", html_escape(&paths));
    }
    format!(
        "\n{component_name} statusz\n{HEADER_WARNING}\n\nStarted{delim} {start_time}\nUp{delim} {uptime_str}\nGo version{delim} {go_version}\nBinary version{delim} {binary_version}\n{emulation}\n{paths}\n"
    )
    .into_bytes()
}

/// `handleStatusz` (statusz.go) with an injectable delimiter set (the Go test
/// overrides the package variable). `raw_query` and `user_agent` feed
/// `isPrettyPrint`.
pub fn handle_statusz_with_delimiters(
    component_name: &str,
    reg: &dyn StatuszRegistry,
    accept: &str,
    user_agent: &str,
    raw_query: &str,
    delimiters: &[&str],
) -> Response {
    let obj = statusz(component_name, reg);
    let text = |delimiters: &[&str]| {
        let delim = if delimiters.is_empty() {
            ":"
        } else {
            let i = uuid::Uuid::new_v4().as_bytes()[0] as usize % delimiters.len();
            delimiters[i]
        };
        encode_text(component_name, &obj, delim)
    };

    if accept.trim().is_empty() {
        // writePlainTextResponse
        return Response {
            status: 200,
            headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
            body: text(delimiters),
        };
    }

    let Some(n) = negotiate_media_type_options(accept, KIND) else {
        return not_acceptable();
    };
    let pretty = n.pretty || is_pretty_print(raw_query, user_agent);

    match n.media_type {
        "application/json" => {
            let Some(convert) = n.convert.as_ref() else {
                return plain_error(
                    "content negotiation failed: mediaType.Convert is nil for application/json",
                );
            };
            let mut headers = Vec::new();
            if reg
                .deprecated_versions()
                .get(&convert.version)
                .copied()
                .unwrap_or(false)
            {
                headers.push((
                    "Warning".into(),
                    "299 - \"This version of the statusz endpoint is deprecated. Please use a newer version.\"".into(),
                ));
            }
            // SerializeObject's media type: generateMediaTypeWithGVK.
            let mut content_type = n.media_type.to_string();
            if !convert.group.is_empty() {
                content_type += &format!(";g={}", convert.group);
            }
            if !convert.version.is_empty() {
                content_type += &format!(";v={}", convert.version);
            }
            if !convert.kind.is_empty() {
                content_type += &format!(";as={}", convert.kind);
            }
            headers.push(("Content-Type".into(), content_type));
            // json.Serializer: Pretty = MarshalIndent("", "  ") (no newline);
            // otherwise json.Encoder.Encode (compact + "\n").
            let body = if pretty {
                serde_json::to_vec_pretty(&obj)
            } else {
                serde_json::to_vec(&obj).map(|mut b| {
                    b.push(b'\n');
                    b
                })
            };
            match body {
                Ok(body) => Response {
                    status: 200,
                    headers,
                    body,
                },
                Err(e) => plain_error(&e.to_string()),
            }
        }
        "text/plain" => Response {
            status: 200,
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: text(delimiters),
        },
        other => plain_error(&format!(
            "content negotiation failed: unsupported media type '{other}'"
        )),
    }
}

/// `handleStatusz` (statusz.go) with the production [`DELIMITERS`].
pub fn handle_statusz(
    component_name: &str,
    reg: &dyn StatuszRegistry,
    accept: &str,
    user_agent: &str,
    raw_query: &str,
) -> Response {
    handle_statusz_with_delimiters(
        component_name,
        reg,
        accept,
        user_agent,
        raw_query,
        &DELIMITERS,
    )
}

/// `statusz.Install` (statusz.go) onto an axum router, replacing upstream's
/// `mux.Handle(DefaultStatuszPath, ...)`.
#[cfg(feature = "axum-support")]
pub fn install<S>(
    router: axum::Router<S>,
    component_name: &str,
    reg: std::sync::Arc<dyn StatuszRegistry>,
) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    use axum::http::{header, HeaderName, HeaderValue, StatusCode};
    use axum::response::IntoResponse;

    let component_name = component_name.to_string();
    router.route(
        DEFAULT_STATUSZ_PATH,
        axum::routing::any(move |req: axum::extract::Request| {
            let reg = reg.clone();
            let component_name = component_name.clone();
            async move {
                let header_str = |name| {
                    req.headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                };
                let resp = handle_statusz(
                    &component_name,
                    reg.as_ref(),
                    header_str(header::ACCEPT),
                    header_str(header::USER_AGENT),
                    req.uri().query().unwrap_or(""),
                );
                let mut out = (
                    StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    resp.body,
                )
                    .into_response();
                for (k, v) in resp.headers {
                    if let (Ok(k), Ok(v)) =
                        (HeaderName::try_from(k.as_str()), HeaderValue::from_str(&v))
                    {
                        out.headers_mut().insert(k, v);
                    }
                }
                out
            }
        }),
    )
}

/// The `if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz)`
/// guard around `statusz.Install` in `controllermanager.go:225` and
/// `kube-scheduler/app/server.go:394`. The registry is only built when the gate
/// is on, as upstream builds it inside the `if`.
#[cfg(feature = "axum-support")]
pub fn install_if_enabled<S>(
    router: axum::Router<S>,
    component_name: &str,
    registry: impl FnOnce() -> Registry,
) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if crate::feature_gates::enabled(crate::feature_gates::Feature::ComponentStatusz) {
        install(router, component_name, std::sync::Arc::new(registry()))
    } else {
        router
    }
}
