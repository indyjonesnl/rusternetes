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

use crate::compatibility::EffectiveVersion;
use crate::feature_gates::Version;
use chrono::{DateTime, Utc};
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
    pub effective_version: Option<EffectiveVersion>,
    pub listed_paths: Option<Vec<String>>,
    pub deprecated_versions_map: HashMap<String, bool>,
}

impl Registry {
    /// `NewRegistry` (statusz.go).
    pub fn new(effective_version: Option<EffectiveVersion>) -> Self {
        let _ = effective_version;
        todo!()
    }

    /// `WithListedPaths` (registry.go).
    pub fn with_listed_paths(mut self, listed_paths: Vec<String>) -> Self {
        let _ = &mut self;
        let _ = listed_paths;
        todo!()
    }
}

impl StatuszRegistry for Registry {
    fn process_start_time(&self) -> DateTime<Utc> {
        todo!()
    }
    fn go_version(&self) -> String {
        todo!()
    }
    fn binary_version(&self) -> Version {
        todo!()
    }
    fn emulation_version(&self) -> Option<Version> {
        todo!()
    }
    fn paths(&self) -> Vec<String> {
        todo!()
    }
    fn deprecated_versions(&self) -> HashMap<String, bool> {
        todo!()
    }
}

/// `v1alpha1.Statusz` (api/v1alpha1/types.go).
#[derive(Debug, Clone, PartialEq)]
pub struct Statusz {
    pub name: String,
    pub start_time: DateTime<Utc>,
    pub uptime_seconds: i64,
    pub go_version: String,
    pub binary_version: String,
    pub emulation_version: String,
    pub paths: Vec<String>,
}

/// `statusz(componentName, reg)` (statusz.go).
pub fn statusz(component_name: &str, reg: &dyn StatuszRegistry) -> Statusz {
    let _ = (component_name, reg);
    todo!()
}

/// `uptime` (statusz.go).
pub fn uptime(t: DateTime<Utc>) -> String {
    let _ = t;
    todo!()
}

/// `aggregatePaths` (statusz.go).
pub fn aggregate_paths(listed_paths: &[String]) -> Vec<String> {
    let _ = listed_paths;
    todo!()
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

/// `handleStatusz` (statusz.go) with an injectable delimiter set (the Go test
/// overrides the package variable).
pub fn handle_statusz_with_delimiters(
    component_name: &str,
    reg: &dyn StatuszRegistry,
    accept: &str,
    user_agent: &str,
    raw_query: &str,
    delimiters: &[&str],
) -> Response {
    let _ = (
        component_name,
        reg,
        accept,
        user_agent,
        raw_query,
        delimiters,
    );
    todo!()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compatibility::EffectiveVersion;
    use chrono::{Local, SubsecRound};

    // statusz_test.go:wantTmpl / wantTmplWithoutEmulation.
    const HEADER: &str = "\n{c} statusz\nWarning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.\n";

    fn want_tmpl(
        component: &str,
        start: &str,
        up: &str,
        go: &str,
        bin: &str,
        emu: Option<&str>,
    ) -> String {
        let emu = match emu {
            Some(e) => format!("Emulation version: {e}"),
            None => String::new(),
        };
        format!(
            "{}\nStarted: {start}\nUp: {up}\nGo version: {go}\nBinary version: {bin}\n{emu}\nPaths: /livez /readyz\n",
            HEADER.replace("{c}", component)
        )
    }

    struct FakeRegistry {
        start_time: DateTime<Utc>,
        go_ver: String,
        binary_ver: Version,
        emulation_ver: Option<Version>,
        listed_paths: Vec<String>,
        deprecated: HashMap<String, bool>,
    }

    impl StatuszRegistry for FakeRegistry {
        fn process_start_time(&self) -> DateTime<Utc> {
            self.start_time
        }
        fn go_version(&self) -> String {
            self.go_ver.clone()
        }
        fn binary_version(&self) -> Version {
            self.binary_ver
        }
        fn emulation_version(&self) -> Option<Version> {
            self.emulation_ver
        }
        fn paths(&self) -> Vec<String> {
            self.listed_paths.clone()
        }
        fn deprecated_versions(&self) -> HashMap<String, bool> {
            self.deprecated.clone()
        }
    }

    fn unix_date(t: DateTime<Utc>) -> String {
        // Go `time.UnixDate` of a `time.Time` in the local zone.
        let l = t.with_timezone(&Local);
        let tz = if l.offset().local_minus_utc() == 0 {
            "UTC".to_string()
        } else {
            let s = l.offset().local_minus_utc();
            let (sign, s) = if s < 0 { ('-', -s) } else { ('+', s) };
            if (s / 60) % 60 == 0 {
                format!("{sign}{:02}", s / 3600)
            } else {
                format!("{sign}{:02}{:02}", s / 3600, (s / 60) % 60)
            }
        };
        format!("{} {tz} {}", l.format("%a %b %e %H:%M:%S"), l.format("%Y"))
    }

    fn fake(emulation: Option<Version>, deprecated: &[&str]) -> FakeRegistry {
        FakeRegistry {
            start_time: Utc::now(),
            go_ver: "1.21".into(),
            binary_ver: Version::major_minor(1, 31),
            emulation_ver: emulation,
            listed_paths: [
                "/livez/poststarthook/peer-discovery-cache-sync",
                "/livez/post",
                "/readyz/informer-sync",
                "/readyz/log",
                "/readyz/ping",
            ]
            .map(String::from)
            .to_vec(),
            deprecated: deprecated.iter().map(|v| (v.to_string(), true)).collect(),
        }
    }

    fn call(reg: &FakeRegistry, accept: &str) -> Response {
        handle_statusz_with_delimiters("test-server", reg, accept, "", "", &[":"])
    }

    fn assert_text(reg: &FakeRegistry, accept: &str, with_emulation: bool) {
        let resp = call(reg, accept);
        assert_eq!(resp.status, 200, "accept {accept:?}");
        let want = want_tmpl(
            "test-server",
            &unix_date(reg.start_time),
            &uptime(reg.start_time),
            "1.21",
            "1.31",
            with_emulation.then_some("1.30"),
        );
        assert_eq!(String::from_utf8(resp.body).unwrap(), want);
    }

    // TestHandleStatusz: "valid request for text/plain"
    #[test]
    fn text_plain() {
        assert_text(
            &fake(Some(Version::major_minor(1, 30)), &[]),
            "text/plain",
            true,
        );
    }

    // "no accept header"
    #[test]
    fn no_accept_header() {
        assert_text(&fake(Some(Version::major_minor(1, 30)), &[]), "", true);
    }

    // "missing emulation version"
    #[test]
    fn missing_emulation_version() {
        assert_text(&fake(None, &[]), "text/plain", false);
    }

    // "wildcard accept header"
    #[test]
    fn wildcard_accept_header() {
        assert_text(&fake(Some(Version::major_minor(1, 30)), &[]), "*/*", true);
    }

    // "bad json header fall back wildcard"
    #[test]
    fn bad_json_header_falls_back_to_wildcard() {
        assert_text(
            &fake(Some(Version::major_minor(1, 30)), &[]),
            "application/json;v=foo;g=config.k8s.io;as=Statusz,*/*",
            true,
        );
    }

    fn assert_json(resp: &Response, reg: &FakeRegistry) {
        assert_eq!(resp.status, 200);
        let got: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(got["kind"], "Statusz");
        assert_eq!(got["apiVersion"], "config.k8s.io/v1alpha1");
        assert_eq!(got["metadata"]["name"], "test-server");
        assert!(got["metadata"]["creationTimestamp"].is_null());
        assert_eq!(
            got["startTime"],
            reg.start_time
                .trunc_subsecs(0)
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        );
        assert_eq!(got["uptimeSeconds"], 0);
        assert_eq!(got["goVersion"], "1.21");
        assert_eq!(got["binaryVersion"], "1.31");
        assert_eq!(got["emulationVersion"], "1.30");
        assert_eq!(got["paths"], serde_json::json!(["/livez", "/readyz"]));
    }

    // "valid request for v1alpha1"
    #[test]
    fn json_v1alpha1() {
        let reg = fake(Some(Version::major_minor(1, 30)), &[]);
        let resp = call(
            &reg,
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
        );
        assert_json(&resp, &reg);
        assert_eq!(
            resp.header("Content-Type"),
            Some("application/json;g=config.k8s.io;v=v1alpha1;as=Statusz")
        );
        assert!(resp.header("Warning").is_none());
    }

    // "deprecated version request"
    #[test]
    fn json_deprecated_version_warns() {
        let reg = fake(Some(Version::major_minor(1, 30)), &["v1alpha1"]);
        let resp = call(
            &reg,
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
        );
        assert_json(&resp, &reg);
        assert!(resp.header("Warning").unwrap().contains("deprecated"));
    }

    fn assert_status(resp: &Response, code: u16) -> serde_json::Value {
        assert_eq!(resp.status, code);
        assert_eq!(resp.header("Content-Type"), Some("application/json"));
        let s: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(s["kind"], "Status");
        assert_eq!(s["apiVersion"], "v1");
        assert_eq!(s["status"], "Failure");
        assert_eq!(s["code"], code);
        s
    }

    // "invalid accept header"
    #[test]
    fn invalid_accept_header_is_406() {
        let s = assert_status(&call(&fake(None, &[]), "application/xml"), 406);
        assert_eq!(s["reason"], "NotAcceptable");
        assert_eq!(
            s["message"],
            "only the following media types are accepted: application/json, application/yaml, application/vnd.kubernetes.protobuf, text/plain"
        );
    }

    // "application/json without params"
    #[test]
    fn json_without_params_is_406() {
        assert_status(&call(&fake(None, &[]), "application/json"), 406);
    }

    // "application/json with missing as"
    #[test]
    fn json_with_missing_as_is_406() {
        assert_status(
            &call(
                &fake(None, &[]),
                "application/json;v=v1alpha1;g=config.k8s.io",
            ),
            406,
        );
    }

    // statusz.go:handleStatusz `default:` branch: yaml passes negotiation
    // (AllowsMediaTypeTransform) but only json and text/plain are handled.
    #[test]
    fn yaml_negotiates_then_fails_500() {
        let s = assert_status(
            &call(
                &fake(None, &[]),
                "application/yaml;v=v1alpha1;g=config.k8s.io;as=Statusz",
            ),
            500,
        );
        assert_eq!(s["reason"], "Unknown");
        assert_eq!(
            s["message"],
            "content negotiation failed: unsupported media type 'application/yaml'"
        );
    }

    // textserializer.go: html.EscapeString of the delimiter and randomised choice.
    #[test]
    fn production_delimiters_are_one_of_the_four() {
        let reg = fake(None, &[]);
        let resp = handle_statusz("test-server", &reg, "text/plain", "", "");
        let body = String::from_utf8(resp.body).unwrap();
        assert!(
            DELIMITERS
                .iter()
                .any(|d| body.contains(&format!("\nGo version{d} 1.21\n"))),
            "{body}"
        );
    }

    // goautoneg.ParseAccept: q-ordering; wildcard sorts after concrete types.
    #[test]
    fn q_ordering_prefers_text_over_wildcard_json() {
        assert_text(&fake(None, &[]), "*/*;q=0.1, text/plain", false);
    }

    // pretty printing: `isPrettyPrint` (negotiate.go) for curl user agents.
    #[test]
    fn curl_user_agent_gets_indented_json() {
        let reg = fake(None, &[]);
        let resp = handle_statusz_with_delimiters(
            "test-server",
            &reg,
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
            "curl/8.0",
            "",
            &[":"],
        );
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.starts_with("{\n  \"kind\": \"Statusz\""), "{body}");
        assert!(!body.ends_with('\n'));
        // Non-pretty json.Encoder output is compact with a trailing newline.
        let resp = call(
            &reg,
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
        );
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.starts_with("{\"kind\":\"Statusz\"") && body.ends_with("}\n"));
    }

    // `Paths` has no omitempty: an empty list encodes as null.
    #[test]
    fn empty_paths_encode_as_null() {
        let mut reg = fake(None, &[]);
        reg.listed_paths.clear();
        let resp = call(
            &reg,
            "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
        );
        let got: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(got["paths"].is_null());
        assert!(got.get("emulationVersion").is_none());
    }

    // aggregatePaths: first segment only, sorted, deduped, non-debugging dropped.
    #[test]
    fn aggregate_paths_folds_and_filters() {
        let paths: Vec<String> = [
            "/readyz/ping",
            "/livez/a",
            "/livez",
            "/apis/x",
            "/api",
            "/openapi/v2",
            "/openid/x",
            "/.well-known/x",
            "/",
            "",
            "noslash",
            "/metrics",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(
            aggregate_paths(&paths),
            vec!["/livez", "/metrics", "/readyz"]
        );
    }

    #[test]
    fn uptime_format() {
        let t = Utc::now() - chrono::Duration::seconds(3600 + 2 * 60 + 3);
        assert_eq!(uptime(t), "1 hr 02 min 03 sec");
    }

    // registry_test.go:TestBinaryVersion
    #[test]
    fn registry_binary_version() {
        let ev = EffectiveVersion::from_string("1.2.3", "", "").unwrap();
        let reg = Registry::new(Some(ev));
        assert_eq!(reg.binary_version(), Version::major_minor(1, 2));
        // Without an effective version: the build's binary version.
        let reg = Registry::new(None);
        assert_eq!(
            reg.binary_version(),
            crate::compatibility::DEFAULT_KUBE_BINARY_VERSION
        );
    }

    // registry_test.go:TestEmulationVersion
    #[test]
    fn registry_emulation_version() {
        let mut ev = EffectiveVersion::from_string("1.35", "", "").unwrap();
        ev.set_emulation_version(Version::major_minor(1, 34));
        let reg = Registry::new(Some(ev));
        assert_eq!(reg.emulation_version(), Some(Version::major_minor(1, 34)));
        assert_eq!(Registry::new(None).emulation_version(), None);
    }

    #[test]
    fn registry_paths_and_deprecations() {
        let reg = Registry::new(None);
        assert!(reg.paths().is_empty());
        assert!(reg.deprecated_versions().is_empty());
        let reg = reg.with_listed_paths(vec!["/a/b".into()]);
        assert_eq!(reg.paths(), vec!["/a/b".to_string()]);
    }

    #[test]
    fn process_start_time_is_in_the_past_and_recent_enough() {
        let start = Registry::new(None).process_start_time();
        assert!(start <= Utc::now());
        assert!(start.timestamp() > 1_000_000_000, "{start}");
    }
}
