//! Unit tests moved from `src/dump.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::borrow::Cow;
    pub use std::cell::RefCell;
    pub use std::sync::Once;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::dump::*;

    #[test]
    fn dumps_enabled_reads_env_once() {
        // Cannot reliably mutate process env across tests, so just assert
        // the function does not panic and returns a stable bool.
        let a = dumps_enabled();
        let b = dumps_enabled();
        assert_eq!(a, b);
    }

    #[test]
    fn redact_passthrough_for_plain_pod() {
        let input = br#"{"kind":"Pod","metadata":{"name":"p"}}"#;
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_replaces_secret_data_values() {
        let input = br#"{"kind":"Secret","data":{"token":"YWJjZA==","empty":""}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["token"], "<redacted len=4>");
        assert_eq!(v["data"]["empty"], "<redacted len=0>");
    }

    #[test]
    fn redact_replaces_secret_string_data_values() {
        let input = br#"{"kind":"Secret","stringData":{"pw":"hunter2"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stringData"]["pw"], "<redacted len=7>");
    }

    #[test]
    fn redact_walks_secret_list_items() {
        let input = br#"{"kind":"SecretList","items":[
        {"kind":"Secret","data":{"k":"YWI="}},
        {"kind":"Secret","stringData":{"k":"v"}}
    ]}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["items"][0]["data"]["k"], "<redacted len=2>");
        assert_eq!(v["items"][1]["stringData"]["k"], "<redacted len=1>");
    }

    #[test]
    fn redact_walks_admission_review_object() {
        let input = br#"{"kind":"AdmissionReview","request":{
        "object":{"kind":"Secret","data":{"k":"YWI="}},
        "oldObject":{"kind":"Secret","stringData":{"k":"x"}}
    }}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["request"]["object"]["data"]["k"], "<redacted len=2>");
        assert_eq!(
            v["request"]["oldObject"]["stringData"]["k"],
            "<redacted len=1>"
        );
    }

    #[test]
    fn redact_passthrough_for_malformed_json() {
        let input = b"not json at all";
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_leaves_configmap_alone() {
        let input = br#"{"kind":"ConfigMap","data":{"k":"v"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["k"], "v");
    }

    #[tokio::test]
    async fn with_payload_makes_bytes_visible_inside_scope() {
        let body = bytes::Bytes::from_static(b"hello");
        let seen = with_payload(body.clone(), async {
            CURRENT_PAYLOAD.with(|cell| cell.borrow().clone())
        })
        .await;
        assert_eq!(seen.as_deref(), Some(b"hello".as_ref()));
    }

    #[tokio::test]
    async fn current_payload_outside_scope_returns_err() {
        let res = CURRENT_PAYLOAD.try_with(|cell| cell.borrow().clone());
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn panic_hook_logs_payload_under_scope() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default, Clone)]
        struct Capture(Arc<Mutex<Vec<String>>>);

        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut visitor = StrVisitor::default();
                event.record(&mut visitor);
                self.0.lock().unwrap().push(visitor.0);
            }
        }

        #[derive(Default)]
        struct StrVisitor(String);
        impl tracing::field::Visit for StrVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, " {}={:?}", field.name(), value);
            }
        }

        let capture = Capture::default();
        let sub = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(sub);

        install_panic_hook("test-component");

        let body = bytes::Bytes::from_static(br#"{"kind":"Pod"}"#);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(with_payload(body, async {
                panic!("boom");
            }))
        }));

        let logs = capture.0.lock().unwrap().clone();
        assert!(
            logs.iter()
                .any(|l| l.contains("test-component") && l.contains("Pod")),
            "no dump log captured; got: {logs:?}"
        );
    }

    #[cfg(feature = "axum-support")]
    #[tokio::test]
    async fn dumping_json_decodes_valid_body() {
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::{header, Request};

        #[derive(serde::Deserialize)]
        struct Foo {
            x: i32,
        }

        let req = Request::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"x":7}"#))
            .unwrap();
        let DumpingJson(foo) = DumpingJson::<Foo>::from_request(req, &()).await.unwrap();
        assert_eq!(foo.x, 7);
    }

    #[cfg(feature = "axum-support")]
    #[tokio::test]
    async fn dumping_json_rejects_invalid_body_with_same_status_as_json() {
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::{header, Request, StatusCode};
        use axum::response::IntoResponse;

        #[derive(Debug, serde::Deserialize)]
        struct Foo {
            _x: i32,
        }

        let req = Request::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let err = DumpingJson::<Foo>::from_request(req, &())
            .await
            .unwrap_err();
        let resp = err.into_response();
        assert!(
            resp.status() == StatusCode::BAD_REQUEST
                || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
        );
    }
}
