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

use crate::statusz::Response;
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
    fn get_flagz(&self) -> BTreeMap<String, String> {
        todo!()
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

/// `handleFlagz` (flagz.go) with an injectable delimiter set.
pub fn handle_flagz_with_delimiters(
    _component_name: &str,
    _reg: &Registry,
    _accept: &str,
    _user_agent: &str,
    _raw_query: &str,
    _delimiters: &[&str],
) -> Response {
    todo!()
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

#[cfg(test)]
mod tests {
    use super::*;

    // flagz_test.go:wantTmpl
    fn want_tmpl(component: &str) -> String {
        format!("\n{component} flagz\nWarning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.\n")
    }

    fn reader() -> NamedFlagSetsReader {
        NamedFlagSetsReader {
            flags: vec![Flag {
                name: "test-flag".into(),
                value: "test-value".into(),
                classified: false,
            }],
        }
    }

    fn reg() -> Registry {
        Registry::new(Box::new(reader()))
    }

    fn run(reg: &Registry, accept: &str) -> Response {
        handle_flagz("test-server", reg, accept, "", "")
    }

    // flagz_test.go TestHandleFlagz: text/plain, empty Accept, wildcard, bad json + wildcard.
    #[test]
    fn text_plain_fallbacks() {
        for accept in [
            "text/plain",
            "",
            "*/*",
            "application/json;v=foo;g=config.k8s.io;as=Flagz,*/*",
        ] {
            let r = run(&reg(), accept);
            assert_eq!(r.status, 200, "{accept}");
            let body = String::from_utf8(r.body).unwrap();
            assert!(body.contains(&want_tmpl("test-server")), "{accept}: {body}");
            assert!(body.contains("test-flag"), "{body}");
            assert_eq!(
                r.headers
                    .iter()
                    .find(|(k, _)| k == "Content-Type")
                    .unwrap()
                    .1,
                "text/plain; charset=utf-8"
            );
        }
    }

    // flagz_test.go TestHandleFlagz: "valid request for v1alpha1".
    #[test]
    fn json_v1alpha1() {
        let r = run(
            &reg(),
            "application/json;v=v1alpha1;g=config.k8s.io;as=Flagz",
        );
        assert_eq!(r.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["kind"], "Flagz");
        assert_eq!(v["apiVersion"], "config.k8s.io/v1alpha1");
        assert_eq!(v["metadata"]["name"], "test-server");
        assert_eq!(v["flags"]["test-flag"], "test-value");
        assert!(r.header("Warning").is_none());
    }

    // "deprecated version request"
    #[test]
    fn deprecated_version_warns() {
        let reg = reg().with_deprecated_version("v1alpha1");
        let r = run(&reg, "application/json;v=v1alpha1;g=config.k8s.io;as=Flagz");
        assert_eq!(r.status, 200);
        assert!(r.header("Warning").unwrap().contains("deprecated"));
    }

    // "unsupported accept header" / "application/json without params" / "missing params"
    // plus Statusz kind is not a valid Flagz conversion.
    #[test]
    fn not_acceptable_cases() {
        for accept in [
            "application/xml",
            "application/json",
            "application/json;v=v1alpha1;g=config.k8s.io",
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
        ] {
            assert_eq!(run(&reg(), accept).status, 406, "{accept}");
        }
    }

    // flagz_test.go TestCache: the first text/plain response is cached; later
    // flag changes are not reflected.
    #[test]
    fn plain_text_response_is_cached() {
        use std::sync::{Arc, Mutex as M};
        struct Live(Arc<M<BTreeMap<String, String>>>);
        impl FlagReader for Live {
            fn get_flagz(&self) -> BTreeMap<String, String> {
                self.0.lock().unwrap().clone()
            }
        }
        let flags = Arc::new(M::new(BTreeMap::from([(
            "test-flag".to_string(),
            "test-value".to_string(),
        )])));
        let reg = Registry::new(Box::new(Live(flags.clone())));
        assert!(reg.cached_plain_text_response().is_none());
        let first = run(&reg, "text/plain");
        assert_eq!(first.status, 200);
        let cached = reg.cached_plain_text_response().expect("cached");
        flags
            .lock()
            .unwrap()
            .insert("new-flag".into(), "new-value".into());
        let second = run(&reg, "text/plain");
        assert_eq!(second.status, 200);
        assert_eq!(reg.cached_plain_text_response().unwrap(), cached);
        assert_eq!(second.body, cached);
        assert!(!String::from_utf8(second.body).unwrap().contains("new-flag"));
    }

    // textserializer.go: keys sorted, one separator drawn from the delimiter set.
    #[test]
    fn text_flags_sorted_with_delimiter() {
        let r = NamedFlagSetsReader {
            flags: vec![
                Flag {
                    name: "b".into(),
                    value: "2".into(),
                    classified: false,
                },
                Flag {
                    name: "a".into(),
                    value: "1".into(),
                    classified: false,
                },
            ],
        };
        let reg = Registry::new(Box::new(r));
        let resp = handle_flagz_with_delimiters("c", &reg, "text/plain", "", "", &["="]);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.ends_with("\na=1\nb=2\n"), "{body}");
    }

    // flagreader.go: classified flags are redacted.
    #[test]
    fn classified_flags_redacted() {
        let r = NamedFlagSetsReader {
            flags: vec![
                Flag {
                    name: "token".into(),
                    value: "s3cret".into(),
                    classified: true,
                },
                Flag {
                    name: "v".into(),
                    value: "2".into(),
                    classified: false,
                },
            ],
        };
        let m = r.get_flagz();
        assert_eq!(m["token"], "CLASSIFIED");
        assert_eq!(m["v"], "2");
    }
}
