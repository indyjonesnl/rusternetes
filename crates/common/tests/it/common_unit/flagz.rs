//! Unit tests moved from `src/flagz.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::statusz::{
        is_pretty_print, negotiate_media_type_options, not_acceptable, plain_error, Response,
    };
    pub use std::collections::BTreeMap;
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::flagz::*;

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

#[cfg(feature = "axum-support")]
mod axum_tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use rusternetes_common::flagz::*;
    use serial_test::serial;
    use tower::ServiceExt;

    async fn get(app: axum::Router) -> u16 {
        let req = Request::builder()
            .uri("/flagz")
            .body(Body::empty())
            .unwrap();
        app.oneshot(req).await.unwrap().status().as_u16()
    }

    fn reader() -> Option<Box<dyn FlagReader>> {
        Some(Box::new(NamedFlagSetsReader::default()))
    }

    // controllermanager.go:219: gate off => /flagz is not registered.
    #[tokio::test]
    #[serial]
    async fn not_installed_when_gate_off() {
        let _g = with_feature(Feature::ComponentFlagz, false);
        let app = install_if_enabled(axum::Router::new(), "kube-scheduler", reader());
        assert_eq!(get(app).await, 404);
    }

    // `if flagReader != nil` (server.go:389).
    #[tokio::test]
    #[serial]
    async fn not_installed_without_reader() {
        let _g = with_feature(Feature::ComponentFlagz, true);
        let app = install_if_enabled(axum::Router::new(), "kube-scheduler", None);
        assert_eq!(get(app).await, 404);
    }

    #[tokio::test]
    #[serial]
    async fn installed_when_gate_on() {
        let _g = with_feature(Feature::ComponentFlagz, true);
        let app = install_if_enabled(axum::Router::new(), "kube-scheduler", reader());
        assert_eq!(get(app).await, 200);
    }
}

#[cfg(feature = "clap-support")]
mod clap_tests {
    use super::__parent::*;
    use clap::{Arg, ArgAction, Command};
    use rusternetes_common::flagz::*;

    #[test]
    fn reads_defaults_and_parsed_values() {
        let cmd = Command::new("x")
            .arg(Arg::new("v").long("v").default_value("2"))
            .arg(Arg::new("name").long("name"))
            .arg(Arg::new("on").long("on").action(ArgAction::SetTrue))
            .arg(Arg::new("gates").long("gates").action(ArgAction::Append))
            .arg(Arg::new("pos"));
        let m = cmd
            .clone()
            .get_matches_from(["x", "--name", "n", "--gates", "a", "--gates", "b"]);
        let got = flags_from_clap(&cmd, &m).get_flagz();
        assert_eq!(got["v"], "2");
        assert_eq!(got["name"], "n");
        assert_eq!(got["on"], "false");
        assert_eq!(got["gates"], "[a,b]");
        assert!(!got.contains_key("pos") && !got.contains_key("help"));
    }
}
