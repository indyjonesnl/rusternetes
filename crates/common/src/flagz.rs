//! Port of `k8s.io/apiserver/pkg/server/flagz` (the `/flagz` z-page).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/server/flagz/flagz.go` (`Install`, `handleFlagz`,
//!   `writePlainTextResponse`, `flagz`)
//! * `.../flagz/flagreader.go` (`Reader`, `NamedFlagSetsReader`, `"classified"` redaction)
//! * `.../flagz/registry.go` (`registry`, cached plain-text response)
//! * `.../flagz/textserializer.go` (`flagzTextSerializer.Encode`)
//! * `.../flagz/negotiate/negotiation.go` (`FlagzEndpointRestrictions`)
//! * `.../flagz/api/v1alpha1/types.go` (`Flagz`)
//! * gate-gated install: `cmd/kube-controller-manager/app/controllermanager.go:219-223`,
//!   `cmd/kube-scheduler/app/server.go:388-392` (`ComponentFlagz`), reader construction at
//!   `cmd/kube-controller-manager/app/options/options.go:535-539` and
//!   `cmd/kube-scheduler/app/options/options.go:270-274`.
//!
//! Content negotiation is shared with [`crate::statusz`] (same upstream
//! `negotiation.NegotiateOutputMediaType`); only the endpoint restrictions
//! (`kind == "Flagz"`) and the writers differ.

use crate::statusz::{
    is_pretty_print, negotiate_media_type_options, not_acceptable, plain_error, Response,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// `flagz.DefaultFlagzPath`.
pub const DEFAULT_FLAGZ_PATH: &str = "/flagz";
/// `flagz.Kind`.
pub const KIND: &str = "Flagz";
/// `flagz.GroupName`.
pub const GROUP_NAME: &str = "config.k8s.io";
/// `flagz.Version`.
pub const VERSION: &str = "v1alpha1";

/// `flagz.Reader`.
pub trait FlagReader: Send + Sync {
    /// `GetFlagz() map[string]string`.
    fn get_flagz(&self) -> BTreeMap<String, String>;
}

/// One `pflag.Flag` as seen by `convertNamedFlagSetToFlags`.
#[derive(Debug, Clone, PartialEq)]
pub struct Flag {
    pub name: String,
    /// `flag.Value.String()`.
    pub value: String,
    /// `flag.Annotations["classified"]` is set and non-empty.
    pub classified: bool,
}

/// `NamedFlagSetsReader`.
#[derive(Debug, Clone, Default)]
pub struct NamedFlagSetsReader {
    pub flags: Vec<Flag>,
}

impl FlagReader for NamedFlagSetsReader {
    /// `convertNamedFlagSetToFlags` (flagreader.go:35-50).
    fn get_flagz(&self) -> BTreeMap<String, String> {
        let mut flags = BTreeMap::new();
        for flag in &self.flags {
            let value = if flag.classified {
                "CLASSIFIED".to_string()
            } else {
                flag.value.clone()
            };
            flags.insert(flag.name.clone(), value);
        }
        flags
    }
}

/// `registry` (registry.go).
pub struct Registry {
    reader: Box<dyn FlagReader>,
    deprecated_versions_map: BTreeMap<String, bool>,
    cached_plain_text_response: Mutex<Option<Vec<u8>>>,
}

impl Registry {
    pub fn new(reader: Box<dyn FlagReader>) -> Self {
        Self {
            reader,
            deprecated_versions_map: BTreeMap::new(),
            cached_plain_text_response: Mutex::new(None),
        }
    }

    pub fn with_deprecated_version(mut self, version: &str) -> Self {
        self.deprecated_versions_map.insert(version.into(), true);
        self
    }

    pub fn cached_plain_text_response(&self) -> Option<Vec<u8>> {
        self.cached_plain_text_response.lock().unwrap().clone()
    }
}

/// `headerFmt` (textserializer.go).
const HEADER_WARNING: &str = "Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.";

/// `v1alpha1.Flagz` rendered per its JSON tags (types.go): `flags,omitempty`,
/// `metadata` carrying the zero `creationTimestamp` as `null`.
fn flagz_json(component_name: &str, flags: &BTreeMap<String, String>) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "kind": KIND,
        "apiVersion": format!("{GROUP_NAME}/{VERSION}"),
        "metadata": {"name": component_name, "creationTimestamp": null},
    });
    if !flags.is_empty() {
        obj["flags"] = serde_json::to_value(flags).unwrap_or_default();
    }
    obj
}

/// `flagzTextSerializer.Encode` (textserializer.go:33-56).
fn encode_text(component_name: &str, reader: &dyn FlagReader, delimiters: &[&str]) -> Vec<u8> {
    let mut out = format!("\n{component_name} flagz\n{HEADER_WARNING}\n");
    let separator = if delimiters.is_empty() {
        ":"
    } else {
        delimiters[uuid::Uuid::new_v4().as_bytes()[0] as usize % delimiters.len()]
    };
    // BTreeMap iterates in `sort.Strings` order.
    for (key, value) in reader.get_flagz() {
        out.push_str(&format!("{key}{separator}{value}\n"));
    }
    out.into_bytes()
}

/// `writePlainTextResponse` (flagz.go:136-171): the first encoding is cached
/// for the life of the registry.
fn write_plain_text_response(
    component_name: &str,
    reg: &Registry,
    delimiters: &[&str],
) -> Response {
    let mut cached = reg.cached_plain_text_response.lock().unwrap();
    let body = cached
        .get_or_insert_with(|| encode_text(component_name, reg.reader.as_ref(), delimiters))
        .clone();
    Response {
        status: 200,
        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
        body,
    }
}

/// `handleFlagz` (flagz.go:75-134) with an injectable delimiter set.
pub fn handle_flagz_with_delimiters(
    component_name: &str,
    reg: &Registry,
    accept: &str,
    user_agent: &str,
    raw_query: &str,
    delimiters: &[&str],
) -> Response {
    if accept.trim().is_empty() {
        return write_plain_text_response(component_name, reg, delimiters);
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
                .deprecated_versions_map
                .get(&convert.version)
                .copied()
                .unwrap_or(false)
            {
                headers.push((
                    "Warning".into(),
                    "299 - \"This version of the flagz endpoint is deprecated. Please use a newer version.\"".into(),
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
            let obj = flagz_json(component_name, &reg.reader.get_flagz());
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
        "text/plain" => write_plain_text_response(component_name, reg, delimiters),
        other => plain_error(&format!(
            "content negotiation failed: unsupported media type '{other}'"
        )),
    }
}

/// `handleFlagz` with the production delimiters.
pub fn handle_flagz(
    component_name: &str,
    reg: &Registry,
    accept: &str,
    user_agent: &str,
    raw_query: &str,
) -> Response {
    handle_flagz_with_delimiters(
        component_name,
        reg,
        accept,
        user_agent,
        raw_query,
        &crate::statusz::DELIMITERS,
    )
}

/// `flagz.Install` (flagz.go:47-73) onto an axum router, replacing upstream's
/// `mux.Handle(DefaultFlagzPath, ...)`.
#[cfg(feature = "axum-support")]
pub fn install<S>(
    router: axum::Router<S>,
    component_name: &str,
    reader: Box<dyn FlagReader>,
) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    use axum::http::{header, HeaderName, HeaderValue, StatusCode};
    use axum::response::IntoResponse;

    let component_name = component_name.to_string();
    let reg = std::sync::Arc::new(Registry::new(reader));
    router.route(
        DEFAULT_FLAGZ_PATH,
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
                let resp = handle_flagz(
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

/// The `if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz)`
/// plus `if flagReader != nil` guards around `flagz.Install`
/// (controllermanager.go:219-223, kube-scheduler/app/server.go:388-392).
#[cfg(feature = "axum-support")]
pub fn install_if_enabled<S>(
    router: axum::Router<S>,
    component_name: &str,
    reader: Option<Box<dyn FlagReader>>,
) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    match reader {
        Some(reader)
            if crate::feature_gates::enabled(crate::feature_gates::Feature::ComponentFlagz) =>
        {
            install(router, component_name, reader)
        }
        _ => router,
    }
}

/// Builds the flag list a `cliflag.NamedFlagSets` would hold for a clap
/// command: every `--long` option with its current value
/// (`pflag.Flag.Value.String()`, defaults included; an unset option without a
/// default is the empty string as for a pflag string). Multi-valued options
/// render as pflag's slice form `[a,b]`. Clap's own `help`/`version` options
/// are not in upstream's flag sets (cobra adds `help` outside them).
#[cfg(feature = "clap-support")]
pub fn flags_from_clap(cmd: &clap::Command, matches: &clap::ArgMatches) -> NamedFlagSetsReader {
    let mut flags = Vec::new();
    for arg in cmd.get_arguments() {
        let Some(name) = arg.get_long() else { continue };
        if name == "help" || name == "version" {
            continue;
        }
        let raw: Vec<String> = matches
            .try_get_raw(arg.get_id().as_str())
            .ok()
            .flatten()
            .map(|v| v.map(|s| s.to_string_lossy().into_owned()).collect())
            .unwrap_or_default();
        let multi = matches!(arg.get_action(), clap::ArgAction::Append)
            || arg.get_num_args().is_some_and(|r| r.max_values() > 1);
        let value = if multi {
            format!("[{}]", raw.join(","))
        } else {
            raw.join(",")
        };
        flags.push(Flag {
            name: name.to_string(),
            value,
            classified: false,
        });
    }
    NamedFlagSetsReader { flags }
}
