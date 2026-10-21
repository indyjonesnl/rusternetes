//! `/statusz` and `/flagz` for the kube-apiserver.
//!
//! Ported from:
//! * `staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go:466-468`
//!   (`PrepareRun`): `statusz.Install(NonGoRestfulMux, "apiserver",
//!   statusz.NewRegistry(s.EffectiveVersion, WithListedPaths(s.ListedPaths())))`
//!   behind `zpagesfeatures.ComponentStatusz`.
//! * `pkg/controlplane/apiserver/server.go:157-161` (`completedConfig.New`):
//!   `flagz.Install(NonGoRestfulMux, name, c.Generic.Flagz)` behind
//!   `ComponentFlagz` and `c.Generic.Flagz != nil`.
//! * `cmd/kube-apiserver/app/server.go:131-134`: `s.Flagz =
//!   flagz.NamedFlagSetsReader{FlagSets: namedFlagSets}`.
//!
//! The handlers themselves are `rusternetes_common::{statusz,flagz}`
//! (`component-base/zpages/{statusz,flagz}`), shared with the other
//! components. The routes are registered on the router that sits behind the
//! handler chain's authorization filter: upstream `/statusz` and `/flagz` are
//! non-resource URLs authorized against `system:monitoring`
//! (`bootstrappolicy/policy.go:301-307`), not exempt from authorization.

use rusternetes_common::feature_gates::{enabled, Feature};
use rusternetes_common::flagz::{self, NamedFlagSetsReader};
use rusternetes_common::statusz;
use std::sync::OnceLock;

/// `Config.Flagz`: set once at startup from the parsed command line; unset
/// leaves `/flagz` uninstalled (`c.Generic.Flagz != nil`).
static FLAGZ: OnceLock<NamedFlagSetsReader> = OnceLock::new();

/// `s.Flagz = flagz.NamedFlagSetsReader{...}` (cmd/kube-apiserver/app/server.go:132).
pub fn set_flagz_reader(reader: NamedFlagSetsReader) {
    let _ = FLAGZ.set(reader);
}

/// `s.ListedPaths()` at `PrepareRun`: the non-resource paths this server
/// serves, sorted (`APIServerHandler.ListedPaths`, handler.go:103-113).
/// `statusz` aggregates `/api*`, `/apis*` etc. itself. `/flagz` is installed
/// earlier (in `New`) than `/statusz` (in `PrepareRun`), so it is listed.
fn listed_paths(flagz_installed: bool) -> Vec<String> {
    let mut paths: Vec<String> = [
        "/.well-known/openid-configuration",
        "/api",
        "/apis",
        "/healthz",
        "/livez",
        "/metrics",
        "/openapi/v2",
        "/openapi/v3",
        "/openid/v1/jwks",
        "/readyz",
        "/swagger.json",
        "/version",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if flagz_installed {
        paths.push(flagz::DEFAULT_FLAGZ_PATH.to_string());
    }
    paths.sort();
    paths
}

/// Register `/flagz` and `/statusz` on `router` when their gates are on.
pub fn install<S>(router: axum::Router<S>) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    install_with(router, FLAGZ.get().cloned())
}

fn install_with<S>(router: axum::Router<S>, reader: Option<NamedFlagSetsReader>) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    // `name` is the controlplane server name, `kube-apiserver`
    // (cmd/kube-apiserver/app/server.go `Use: "kube-apiserver"`).
    let flagz_installed = reader.is_some() && enabled(Feature::ComponentFlagz);
    let router = flagz::install_if_enabled(
        router,
        "kube-apiserver",
        reader.map(|r| Box::new(r) as Box<dyn flagz::FlagReader>),
    );
    statusz::install_if_enabled(router, "apiserver", || {
        statusz::Registry::new(Some(
            rusternetes_common::compatibility::current_effective_version(),
        ))
        .with_listed_paths(listed_paths(flagz_installed))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
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
