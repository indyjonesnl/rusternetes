//! Unit tests moved from `src/zpages.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::flagz::{self, NamedFlagSetsReader};
    pub use rusternetes_common::statusz;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_api_server::zpages::*;
    use rusternetes_common::feature_gates::with_feature;
    use serial_test::serial;
    use tower::ServiceExt;

    fn reader() -> NamedFlagSetsReader {
        NamedFlagSetsReader {
            flags: vec![flagz::Flag {
                name: "v".into(),
                value: "2".into(),
                classified: false,
            }],
        }
    }

    async fn status(app: axum::Router, path: &str) -> u16 {
        app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    // genericapiserver.go:466 / server.go:157: gates off => not registered.
    #[tokio::test]
    #[serial]
    async fn nothing_installed_when_gates_off() {
        let _a = with_feature(Feature::ComponentStatusz, false);
        let _b = with_feature(Feature::ComponentFlagz, false);
        let app = install_with(axum::Router::new(), Some(reader()));
        assert_eq!(status(app.clone(), "/statusz").await, 404);
        assert_eq!(status(app, "/flagz").await, 404);
    }

    #[tokio::test]
    #[serial]
    async fn both_installed_when_gates_on() {
        let _a = with_feature(Feature::ComponentStatusz, true);
        let _b = with_feature(Feature::ComponentFlagz, true);
        let app = install_with(axum::Router::new(), Some(reader()));
        assert_eq!(status(app.clone(), "/statusz").await, 200);
        assert_eq!(status(app, "/flagz").await, 200);
    }

    // server.go:158: `c.Generic.Flagz != nil` guards /flagz only.
    #[tokio::test]
    #[serial]
    async fn flagz_needs_a_reader() {
        let _a = with_feature(Feature::ComponentStatusz, true);
        let _b = with_feature(Feature::ComponentFlagz, true);
        let app = install_with(axum::Router::new(), None);
        assert_eq!(status(app.clone(), "/statusz").await, 200);
        assert_eq!(status(app, "/flagz").await, 404);
    }

    #[test]
    fn listed_paths_are_sorted() {
        let p = listed_paths(true);
        let mut s = p.clone();
        s.sort();
        assert_eq!(p, s);
        assert!(p.contains(&"/flagz".to_string()));
        assert!(!listed_paths(false).contains(&"/flagz".to_string()));
    }
}
