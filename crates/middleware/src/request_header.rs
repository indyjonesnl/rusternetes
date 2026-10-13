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

/// The DER chain the TLS layer verified against `--requestheader-client-ca-file`
/// (leaf first). Injected per connection by the api-server's acceptor, separate
/// from `PeerCertificates` (which is the `--client-ca-file` verdict) because
/// upstream verifies the two authenticators against their own CA bundles.
#[derive(Clone, Debug)]
pub struct FrontProxyPeerCertificates(pub Arc<Vec<Vec<u8>>>);

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn headers(pairs: &[(&str, &[&str])]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, vs) in pairs {
            for v in *vs {
                h.append(
                    HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    HeaderValue::from_str(v).unwrap(),
                );
            }
        }
        h
    }

    type Extra = Vec<(&'static str, Vec<&'static str>)>;
    type Expected = (&'static str, &'static str, Vec<&'static str>, Extra);

    struct Case {
        name: &'static str,
        name_headers: Vec<String>,
        uid_headers: Vec<String>,
        group_headers: Vec<String>,
        extra_prefixes: Vec<String>,
        request: HeaderMap,
        final_headers: Option<HeaderMap>,
        expected: Option<Expected>,
    }

    fn base(name: &'static str) -> Case {
        Case {
            name,
            name_headers: vec![],
            uid_headers: vec![],
            group_headers: vec![],
            extra_prefixes: vec![],
            request: HeaderMap::new(),
            final_headers: None,
            expected: None,
        }
    }

    /// Table ported from `TestRequestHeader` (requestheader_test.go:30-293).
    #[test]
    fn request_header_table() {
        let mut cases: Vec<Case> = vec![base("empty")];
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            ..base("user no match")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            expected: Some(("Bob", "", vec![], vec![])),
            request: headers(&[("x-remote-user", &["Bob"])]),
            ..base("user match")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            request: headers(&[
                ("prefixed-x-remote-user-with-suffix", &["Bob"]),
                ("x-remote-user-with-suffix", &["Bob"]),
            ]),
            ..base("user exact match")
        });
        cases.push(Case {
            name_headers: s(&[
                "X-Remote-User",
                "A-Second-X-Remote-User",
                "Another-X-Remote-User",
            ]),
            request: headers(&[
                ("x-remote-user", &["", "First header, second value"]),
                (
                    "a-second-x-remote-user",
                    &["Second header, first value", "Second header, second value"],
                ),
                ("another-x-remote-user", &["Third header, first value"]),
            ]),
            expected: Some(("Second header, first value", "", vec![], vec![])),
            ..base("user first match")
        });
        cases.push(Case {
            name_headers: s(&["x-REMOTE-user"]),
            request: headers(&[("x-remote-user", &["Bob"])]),
            expected: Some(("Bob", "", vec![], vec![])),
            ..base("user case-insensitive")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            group_headers: s(&["X-Remote-Group"]),
            request: headers(&[("x-remote-user", &["Bob"])]),
            expected: Some(("Bob", "", vec![], vec![])),
            ..base("groups none")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            group_headers: s(&["X-Remote-Group-1", "X-Remote-Group-2"]),
            request: headers(&[
                ("x-remote-user", &["Bob"]),
                ("x-remote-group-1", &["one-a", "one-b"]),
                ("x-remote-group-2", &["two-a", "two-b"]),
            ]),
            expected: Some(("Bob", "", vec!["one-a", "one-b", "two-a", "two-b"], vec![])),
            ..base("groups all matches")
        });
        cases.push(Case {
            name_headers: s(&["X-REMOTE-User"]),
            group_headers: s(&["X-REMOTE-Group"]),
            request: headers(&[("x-remote-user", &["Bob"]), ("x-remote-group", &["Users"])]),
            expected: Some(("Bob", "", vec!["Users"], vec![])),
            ..base("groups case-insensitive")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            uid_headers: s(&["X-Remote-Uid"]),
            request: headers(&[("x-remote-user", &["Bob"])]),
            expected: Some(("Bob", "", vec![], vec![])),
            ..base("uid none")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            uid_headers: s(&["X-Remote-Uid"]),
            request: headers(&[
                ("x-remote-user", &["Bob"]),
                ("x-remote-uid-1", &["8f5ea9d1-a5ed-4d02-80a2-26709216350b"]),
                ("x-remote-uid-2", &["c7644180-c774-4a9b-81e5-3eef76f087ab"]),
            ]),
            final_headers: Some(headers(&[
                ("x-remote-uid-1", &["8f5ea9d1-a5ed-4d02-80a2-26709216350b"]),
                ("x-remote-uid-2", &["c7644180-c774-4a9b-81e5-3eef76f087ab"]),
            ])),
            expected: Some(("Bob", "", vec![], vec![])),
            ..base("uid exact match")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            uid_headers: s(&["X-Remote-Uid-1", "X-Remote-Uid-2"]),
            request: headers(&[
                ("x-remote-user", &["Bob"]),
                ("x-remote-uid-1", &["8f5ea9d1-a5ed-4d02-80a2-26709216350b"]),
                ("x-remote-uid-2", &["c7644180-c774-4a9b-81e5-3eef76f087ab"]),
            ]),
            expected: Some((
                "Bob",
                "8f5ea9d1-a5ed-4d02-80a2-26709216350b",
                vec![],
                vec![],
            )),
            ..base("uid first match")
        });
        let extra_request = |with_unrelated: bool, with_uid: bool| {
            let mut v: Vec<(&str, &[&str])> = vec![];
            if with_unrelated {
                v.push(("x-group-remote", &["snorlax"]));
                v.push(("x-group-bear", &["panda"]));
            }
            v.push(("x-remote-user", &["Bob"]));
            if with_uid {
                v.push(("x-remote-uid", &["2ca80fb0-60ea-4ecf-951c-89af843b0402"]));
            }
            v.extend([
                ("x-remote-group-1", &["one-a", "one-b"][..]),
                ("x-remote-group-2", &["two-a", "two-b"][..]),
                ("x-remote-extra-1-key1", &["alfa", "bravo"][..]),
                ("x-remote-extra-1-key2", &["charlie", "delta"][..]),
                ("x-remote-extra-1-", &["india", "juliet"][..]),
                ("x-remote-extra-2-", &["kilo", "lima"][..]),
                ("x-remote-extra-2-key1", &["echo", "foxtrot"][..]),
                ("x-remote-extra-2-key2", &["golf", "hotel"][..]),
            ]);
            headers(&v)
        };
        let extra_expected: Extra = vec![
            ("key1", vec!["alfa", "bravo", "echo", "foxtrot"]),
            ("key2", vec!["charlie", "delta", "golf", "hotel"]),
            ("", vec!["india", "juliet", "kilo", "lima"]),
        ];
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            uid_headers: s(&["X-Remote-UID"]),
            group_headers: s(&["X-Remote-Group-1", "X-Remote-Group-2"]),
            extra_prefixes: s(&["X-Remote-Extra-1-", "X-Remote-Extra-2-"]),
            request: extra_request(false, true),
            expected: Some((
                "Bob",
                "2ca80fb0-60ea-4ecf-951c-89af843b0402",
                vec!["one-a", "one-b", "two-a", "two-b"],
                extra_expected.clone(),
            )),
            ..base("extra prefix matches case-insensitive")
        });
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            group_headers: s(&["X-Remote-Group-1", "X-Remote-Group-2"]),
            extra_prefixes: s(&["X-Remote-Extra-1-", "X-Remote-Extra-2-"]),
            request: extra_request(true, false),
            final_headers: Some(headers(&[
                ("x-group-remote", &["snorlax"]),
                ("x-group-bear", &["panda"]),
            ])),
            expected: Some((
                "Bob",
                "",
                vec!["one-a", "one-b", "two-a", "two-b"],
                extra_expected,
            )),
            ..base("extra prefix matches case-insensitive with unrelated headers")
        });
        // The Go case also has a key with a literal backslash, which is not a
        // legal header-name character for the `http` crate, so it is omitted.
        cases.push(Case {
            name_headers: s(&["X-Remote-User"]),
            uid_headers: s(&["X-Remote-Uid"]),
            group_headers: s(&["X-Remote-Group"]),
            extra_prefixes: s(&["X-Remote-Extra-"]),
            request: headers(&[
                ("x-remote-user", &["Bob"]),
                ("x-remote-uid", &["2ca80fb0-60ea-4ecf-951c-89af843b0402"]),
                ("x-remote-group", &["one-a", "one-b"]),
                ("x-remote-extra-alpha", &["alphabetical"]),
                ("x-remote-extra-alph4num3r1c", &["alphanumeric"]),
                ("x-remote-extra-percent%20encoded", &["percent encoded"]),
                (
                    "x-remote-extra-almost%zzpercent%xxencoded",
                    &["not quite percent encoded"],
                ),
                (
                    "x-remote-extra-example.com%2fpercent%2520encoded",
                    &["url with double percent encoding"],
                ),
                (
                    "x-remote-extra-example.com%2f%e4%bb%8a%e6%97%a5%e3%81%af",
                    &["url with unicode"],
                ),
                (
                    "x-remote-extra-abc123!#$+.-_*^`~|'",
                    &["header key legal characters"],
                ),
            ]),
            expected: Some((
                "Bob",
                "2ca80fb0-60ea-4ecf-951c-89af843b0402",
                vec!["one-a", "one-b"],
                vec![
                    ("alpha", vec!["alphabetical"]),
                    ("alph4num3r1c", vec!["alphanumeric"]),
                    ("percent encoded", vec!["percent encoded"]),
                    (
                        "almost%zzpercent%xxencoded",
                        vec!["not quite percent encoded"],
                    ),
                    (
                        "example.com/percent%20encoded",
                        vec!["url with double percent encoding"],
                    ),
                    ("example.com/今日は", vec!["url with unicode"]),
                    ("abc123!#$+.-_*^`~|'", vec!["header key legal characters"]),
                ],
            )),
            ..base("escaped extra keys")
        });

        for case in cases {
            let cfg = RequestHeaderConfig::new(
                &case.name_headers,
                &case.uid_headers,
                &case.group_headers,
                &case.extra_prefixes,
                &[],
            )
            .unwrap();
            let mut req = case.request.clone();
            let got = cfg.authenticate(&mut req);
            match (&case.expected, got) {
                (None, None) => continue,
                (Some(_), None) => panic!("{}: expected ok, got none", case.name),
                (None, Some(u)) => panic!("{}: expected not ok, got {u:?}", case.name),
                (Some((name, uid, groups, extra)), Some(u)) => {
                    assert_eq!(u.username, *name, "{}", case.name);
                    assert_eq!(u.uid, *uid, "{}", case.name);
                    assert_eq!(
                        u.groups,
                        groups.iter().map(|g| g.to_string()).collect::<Vec<_>>(),
                        "{}",
                        case.name
                    );
                    assert_eq!(u.extra.len(), extra.len(), "{}: {:?}", case.name, u.extra);
                    for (k, v) in extra {
                        assert_eq!(
                            u.extra.get(*k).cloned().unwrap_or_default(),
                            v.iter().map(|x| x.to_string()).collect::<Vec<_>>(),
                            "{}: extra {k:?}",
                            case.name
                        );
                    }
                }
            }
            let want = case.final_headers.clone().unwrap_or_default();
            assert_eq!(req, want, "{}: unexpected final headers", case.name);
        }
    }

    #[test]
    fn new_rejects_empty_headers() {
        // trimHeaders (requestheader.go:102-112)
        let err =
            RequestHeaderConfig::new(&s(&["X-Remote-User", "  "]), &[], &[], &[], &[]).unwrap_err();
        assert_eq!(err, "empty header \"  \"");
        assert!(RequestHeaderConfig::new(&[], &s(&[""]), &[], &[], &[]).is_err());
        assert!(RequestHeaderConfig::new(&[], &[], &s(&[" "]), &[], &[]).is_err());
        assert!(RequestHeaderConfig::new(&[], &[], &[], &s(&[""]), &[]).is_err());
        let ok = RequestHeaderConfig::new(&s(&[" X-Remote-User "]), &[], &[], &[], &[]).unwrap();
        assert_eq!(ok.username_headers, s(&["X-Remote-User"]));
    }

    #[test]
    fn verify_subject_enforces_allowed_names() {
        // verifySubject (x509.go:257-269)
        let open = RequestHeaderConfig::default();
        assert!(open.verify_subject("anything").is_ok());
        let cfg =
            RequestHeaderConfig::new(&[], &[], &[], &[], &s(&["front-proxy-client"])).unwrap();
        assert!(cfg.verify_subject("front-proxy-client").is_ok());
        assert_eq!(
            cfg.verify_subject("someone-else").unwrap_err(),
            "x509: subject with cn=someone-else is not in the allowed list"
        );
    }

    #[test]
    fn standard_headers_are_cleared_even_when_unconfigured() {
        // standardRequestHeaderConfig (filters/authentication.go:55-60)
        let mut h = headers(&[
            ("x-remote-user", &["root"]),
            ("x-remote-uid", &["1"]),
            ("x-remote-group", &["system:masters"]),
            ("x-remote-extra-scopes", &["a"]),
            ("x-keep", &["me"]),
        ]);
        standard_request_header_config().clear_headers(&mut h);
        assert_eq!(h, headers(&[("x-keep", &["me"])]));
    }
}
