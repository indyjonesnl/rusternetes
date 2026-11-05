//! Port of `healthz.InstallPathHandler`
//! (`staging/src/k8s.io/apiserver/pkg/server/healthz/healthz.go:195-224`) and
//! `handleRootHealth` (`:272-345`), as the kubelet's server wires it
//! (`pkg/kubelet/server/server.go:429-436`: `ping`, `log`, `syncloop`).
//!
//! For `path` it serves the aggregate handler (`?verbose`, repeated
//! `?exclude=<check>`; failures are always verbose with the reason withheld and
//! status 500) and one `path/<check>` sub-path per check (`adaptCheckToHandler`:
//! `ok`, or 500 `internal server error: <err>`).
//!
//! Not ported: `GroupedHealthChecker` (no kubelet check is grouped) and the
//! `firstTimeHealthy` callback (the kubelet passes none). The apiserver-side
//! `/healthz` in this repo is a bare `ok` handler with no checker machinery, so
//! there was nothing shared to reuse.

use crate::server_metrics_endpoints::{observe_healthcheck, HealthcheckStatus};
use axum::extract::{RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

type CheckFn = dyn Fn() -> Result<(), String> + Send + Sync;

/// `healthz.HealthChecker`: a name and a check.
#[derive(Clone)]
pub struct HealthChecker {
    name: String,
    check: Arc<CheckFn>,
}

impl HealthChecker {
    /// `healthz.NamedCheck`.
    pub fn named(
        name: impl Into<String>,
        check: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            check: Arc::new(check),
        }
    }

    /// `healthz.PingHealthz`.
    pub fn ping() -> Self {
        Self::named("ping", || Ok(()))
    }

    /// `healthz.LogHealthz` (`healthz.go:80-93`): the check always passes; it
    /// only logs that it ran.
    pub fn log() -> Self {
        Self::named("log", || {
            tracing::info!("Healthz check");
            Ok(())
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// `getExcludedChecks`: every `exclude` query value.
fn excluded_checks(query: Option<&str>) -> BTreeSet<String> {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .filter(|(k, _)| k == "exclude")
        .map(|(_, v)| v.into_owned())
        .collect()
}

fn has_verbose(query: Option<&str>) -> bool {
    url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()).any(|(k, _)| k == "verbose")
}

/// `handleRootHealth`.
fn handle_root_health(name: &str, checks: &[HealthChecker], query: Option<&str>) -> Response {
    let excluded = excluded_checks(query);
    let mut unknown_excluded = excluded.clone();
    let mut out = String::new();
    let mut failed: Vec<&str> = Vec::new();
    for check in checks {
        if excluded.contains(check.name()) {
            unknown_excluded.remove(check.name());
            let _ = writeln!(out, "[+]{} excluded: ok", check.name());
            continue;
        }
        match (check.check)() {
            Err(err) => {
                observe_healthcheck(check.name(), name, HealthcheckStatus::Error);
                // The endpoint is public: the reason is logged, not returned.
                let _ = writeln!(out, "[-]{} failed: reason withheld", check.name());
                tracing::info!("{}: [-]{} failed: {err}", name, check.name());
                failed.push(check.name());
            }
            Ok(()) => {
                observe_healthcheck(check.name(), name, HealthcheckStatus::Success);
                let _ = writeln!(out, "[+]{} ok", check.name());
            }
        }
    }
    if !unknown_excluded.is_empty() {
        let quoted: Vec<String> = unknown_excluded.iter().map(|n| format!("{n:?}")).collect();
        let _ = writeln!(
            out,
            "warn: some health checks cannot be excluded: no matches for {}",
            quoted.join(",")
        );
    }
    // Always verbose on failure.
    if !failed.is_empty() {
        tracing::info!("{} check failed: {}", failed.join(","), name);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{out}{name} check failed\n"),
        )
            .into_response();
    }
    let headers = [
        (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
    ];
    if !has_verbose(query) {
        return (headers, "ok").into_response();
    }
    (headers, format!("{out}{name} check passed\n")).into_response()
}

/// `adaptCheckToHandler`.
fn handle_single_check(check: &HealthChecker) -> Response {
    match (check.check)() {
        Ok(()) => "ok".into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}\n"),
        )
            .into_response(),
    }
}

/// `healthz.InstallPathHandler(mux, path, checks...)`. With no checks the ping
/// handler is installed.
pub fn install_path_handler(path: &str, checks: Vec<HealthChecker>) -> Router {
    let checks = if checks.is_empty() {
        vec![HealthChecker::ping()]
    } else {
        checks
    };
    let name = path
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let all = Arc::new(checks);
    let root_checks = all.clone();
    let mut router = Router::new().route(
        path,
        get(move |RawQuery(q): RawQuery| {
            let (name, checks) = (name.clone(), root_checks.clone());
            async move { handle_root_health(&name, &checks, q.as_deref()) }
        }),
    );
    for check in all.iter() {
        let c = check.clone();
        router = router.route(
            &format!("{path}/{}", check.name()),
            get(move |State(()): State<()>| {
                let c = c.clone();
                async move { handle_single_check(&c) }
            }),
        );
    }
    router
}
