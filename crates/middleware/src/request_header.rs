//! Front-proxy (request-header) authentication.
//!
//! Ported from upstream
//! `staging/src/k8s.io/apiserver/pkg/authentication/request/headerrequest/requestheader.go`
//! (`New`, `AuthenticateRequest`, `ClearAuthenticationHeaders`, `headerValue`,
//! `allHeaderValues`, `newExtra`, `unescapeExtraKey`) and the subject check of
//! `x509.Verifier.verifySubject`
//! (`staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go:257-269`).
//!
//! The certificate chain itself is verified against `--requestheader-client-ca-file`
//! by the api-server's TLS acceptor (`Verifier.AuthenticateRequest`, x509.go:230-255);
//! this module is the part that runs after that verification.

use axum::http::{HeaderMap, HeaderName};
use rusternetes_common::auth::UserInfo;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-connection front-proxy state, injected by the api-server's acceptor as
/// `Option<FrontProxyConnection>` (`Some` only when `--requestheader-client-ca-file`
/// is set).
///
/// The TLS handshake trusts the union of `--client-ca-file` and
/// `--requestheader-client-ca-file` (`ApplyClientCert`, server/config.go:535-548),
/// so which authenticator a presented cert is valid for is decided afterwards,
/// per authenticator, against that authenticator's own CA bundle
/// (`x509.Verifier`, x509.go:229-255). The acceptor records both verdicts here.
#[derive(Clone, Debug)]
pub struct FrontProxyConnection {
    pub config: Arc<RequestHeaderConfig>,
    /// The chain the client presented, leaf first (empty: no client cert).
    pub presented: Arc<Vec<Vec<u8>>>,
    /// The chain verifies against `--requestheader-client-ca-file`.
    pub verified_by_request_header_ca: bool,
    /// `Some(verdict)` when `--client-ca-file` is set (x509 authentication
    /// enabled, config.go:128); `None` when it is not.
    pub verified_by_client_ca: Option<bool>,
}

impl FrontProxyConnection {
    /// The chain `PeerCertificates` should carry: only what the x509
    /// authenticator would accept.
    pub fn x509_chain(&self) -> Vec<Vec<u8>> {
        if self.verified_by_client_ca == Some(true) {
            self.presented.as_ref().clone()
        } else {
            Vec::new()
        }
    }

    /// `Verifier.AuthenticateRequest` around the header authenticator
    /// (x509.go:230-255 over requestheader.go:131-150).
    ///
    /// `Ok(None)` is "not ok, no error" (union moves on); `Err` is an
    /// authenticator error (the union collects it, and fails the request
    /// if no later authenticator succeeds).
    pub fn authenticate(&self, headers: &mut HeaderMap) -> Result<Option<UserInfo>, String> {
        let Some(leaf) = self.presented.first() else {
            return Ok(None);
        };
        if !self.verified_by_request_header_ca {
            return Err("x509: certificate signed by unknown authority".to_string());
        }
        // verifySubject reads pkix.Name.CommonName (the last CN attribute).
        let cn = x509_parser::parse_x509_certificate(leaf)
            .ok()
            .and_then(|(_, c)| {
                c.subject()
                    .iter_common_name()
                    .last()
                    .and_then(|a| a.as_str().ok().map(str::to_string))
            })
            .unwrap_or_default();
        self.config.verify_subject(&cn)?;
        Ok(self.config.authenticate(headers))
    }

    /// The x509 authenticator's verdict on this connection's chain
    /// (`x509.Verifier` with `CommonNameUserConversion`): `Err` when x509 is
    /// enabled and the presented chain does not verify against
    /// `--client-ca-file`.
    pub fn x509_error(&self) -> Option<String> {
        (self.verified_by_client_ca == Some(false) && !self.presented.is_empty())
            .then(|| "x509: certificate signed by unknown authority".to_string())
    }
}

/// `headerrequest` configuration: header names are matched case-insensitively.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestHeaderConfig {
    pub username_headers: Vec<String>,
    pub uid_headers: Vec<String>,
    pub group_headers: Vec<String>,
    pub extra_header_prefixes: Vec<String>,
    /// `--requestheader-allowed-names`; empty allows any verified subject.
    pub allowed_names: Vec<String>,
}

impl RequestHeaderConfig {
    /// `New` (requestheader.go:67-92): every header is trimmed and an empty
    /// one is an error (`trimHeaders`, :102-112).
    pub fn new(
        name_headers: &[String],
        uid_headers: &[String],
        group_headers: &[String],
        extra_header_prefixes: &[String],
        allowed_names: &[String],
    ) -> Result<Self, String> {
        Ok(Self {
            username_headers: trim_headers(name_headers)?,
            uid_headers: trim_headers(uid_headers)?,
            group_headers: trim_headers(group_headers)?,
            extra_header_prefixes: trim_headers(extra_header_prefixes)?,
            allowed_names: allowed_names.to_vec(),
        })
    }

    /// `AuthenticateRequest` (requestheader.go:131-150). Clears the headers it
    /// consumed on success. Returns `None` ("not ok") when no username header
    /// carries a value.
    pub fn authenticate(&self, headers: &mut HeaderMap) -> Option<UserInfo> {
        let name = header_value(headers, &self.username_headers);
        if name.is_empty() {
            return None;
        }
        let uid = header_value(headers, &self.uid_headers);
        let groups = all_header_values(headers, &self.group_headers);
        let extra = new_extra(headers, &self.extra_header_prefixes);

        // clear headers used for authentication
        self.clear_headers(headers);

        Some(UserInfo {
            username: name,
            uid,
            groups,
            extra,
        })
    }

    /// `Verifier.verifySubject` (x509.go:257-269).
    pub fn verify_subject(&self, common_name: &str) -> Result<(), String> {
        // No CN restrictions
        if self.allowed_names.is_empty() {
            return Ok(());
        }
        if self.allowed_names.iter().any(|n| n == common_name) {
            return Ok(());
        }
        Err(format!(
            "x509: subject with cn={common_name} is not in the allowed list"
        ))
    }

    /// `ClearAuthenticationHeaders` over this config's headers (:152-168).
    pub fn clear_headers(&self, headers: &mut HeaderMap) {
        for name in self
            .username_headers
            .iter()
            .chain(&self.uid_headers)
            .chain(&self.group_headers)
        {
            if let Ok(n) = HeaderName::from_bytes(name.as_bytes()) {
                headers.remove(n);
            }
        }
        for prefix in &self.extra_header_prefixes {
            // we have the raw key so avoid relying on canonicalization
            let doomed: Vec<HeaderName> = headers
                .keys()
                .filter(|k| has_prefix_ignore_case(k.as_str(), prefix))
                .cloned()
                .collect();
            for k in doomed {
                headers.remove(k);
            }
        }
    }
}

/// `trimHeaders` (requestheader.go:102-112).
fn trim_headers(names: &[String]) -> Result<Vec<String>, String> {
    names
        .iter()
        .map(|n| {
            let t = n.trim();
            if t.is_empty() {
                Err(format!("empty header {n:?}"))
            } else {
                Ok(t.to_string())
            }
        })
        .collect()
}

/// The standard front-proxy headers `WithAuthentication` always strips
/// (`standardRequestHeaderConfig`, filters/authentication.go:55-60).
pub fn standard_request_header_config() -> RequestHeaderConfig {
    RequestHeaderConfig {
        username_headers: vec!["X-Remote-User".to_string()],
        uid_headers: vec!["X-Remote-Uid".to_string()],
        group_headers: vec!["X-Remote-Group".to_string()],
        extra_header_prefixes: vec!["X-Remote-Extra-".to_string()],
        allowed_names: Vec::new(),
    }
}

/// `hasPrefixIgnoreCase` (requestheader.go:170-172).
fn has_prefix_ignore_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn value_string(v: &axum::http::HeaderValue) -> String {
    String::from_utf8_lossy(v.as_bytes()).into_owned()
}

/// `headerValue` (:174-182): the first configured header whose first value is
/// non-empty (`http.Header.Get` reads only the first value).
fn header_value(h: &HeaderMap, names: &[String]) -> String {
    for name in names {
        let Ok(n) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        if let Some(v) = h.get(n) {
            if !v.is_empty() {
                return value_string(v);
            }
        }
    }
    String::new()
}

/// `allHeaderValues` (:184-200): every non-empty value of every header, in
/// configured-header order.
fn all_header_values(h: &HeaderMap, names: &[String]) -> Vec<String> {
    let mut ret = Vec::new();
    for name in names {
        let Ok(n) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        for v in h.get_all(n) {
            if !v.is_empty() {
                ret.push(value_string(v));
            }
        }
    }
    ret
}

/// `unescapeExtraKey` (:202-208): `url.PathUnescape`, keeping the raw key
/// when it is malformed (any `%` not followed by two hex digits).
fn unescape_extra_key(encoded: &str) -> String {
    let b = encoded.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = |c: Option<&u8>| c.and_then(|c| (*c as char).to_digit(16));
            match (hex(b.get(i + 1)), hex(b.get(i + 2))) {
                (Some(hi), Some(lo)) => {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                }
                _ => return encoded.to_string(),
            }
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `newExtra` (:210-228): prefixes outermost so values keep prefix order.
fn new_extra(h: &HeaderMap, prefixes: &[String]) -> HashMap<String, Vec<String>> {
    let mut ret: HashMap<String, Vec<String>> = HashMap::new();
    for prefix in prefixes {
        for (name, value) in h.iter() {
            if !has_prefix_ignore_case(name.as_str(), prefix) {
                continue;
            }
            // `strings.ToLower(headerName[len(prefix):])`; names are already lowercase.
            let key = unescape_extra_key(&name.as_str()[prefix.len()..].to_ascii_lowercase());
            ret.entry(key).or_default().push(value_string(value));
        }
    }
    ret
}
