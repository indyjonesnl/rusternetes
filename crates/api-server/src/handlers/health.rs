//! `/healthz`, `/livez`, `/readyz` and their per-check subpaths.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/healthz/healthz.go`
//! (`InstallPathHandlerWithHealthyFunc`, `handleRootHealth`,
//! `adaptCheckToHandler`, `getExcludedChecks`) and
//! `staging/src/k8s.io/apiserver/pkg/server/healthz.go`
//! (`AddHealthChecks` / `AddBootSequenceHealthChecks`: boot-sequence checks and
//! post-start-hook checks are on all three endpoints).

use crate::state::ApiServerState;
use axum::extract::State;
use axum::extract::{Path, RawQuery};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type CheckFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// `healthz.HealthChecker`: a named check.
pub struct HealthChecker {
    pub name: String,
    check: Box<dyn Fn() -> CheckFuture + Send + Sync>,
}

impl HealthChecker {
    pub fn new(
        name: impl Into<String>,
        check: impl Fn() -> CheckFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            check: Box::new(check),
        }
    }

    /// A check that is already evaluated (`NamedCheck` over a ready value).
    pub fn ready(name: impl Into<String>, result: Result<(), String>) -> Self {
        Self::new(name, move || {
            let result = result.clone();
            Box::pin(async move { result })
        })
    }

    async fn run(&self) -> Result<(), String> {
        (self.check)().await
    }
}

/// Which endpoint a check list is for (`healthzRegistry` / `livezRegistry` /
/// `readyzRegistry`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HealthEndpoint {
    Healthz,
    Livez,
    Readyz,
}

impl HealthEndpoint {
    /// `name` in `InstallPathHandlerWithHealthyFunc`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Healthz => "healthz",
            Self::Livez => "livez",
            Self::Readyz => "readyz",
        }
    }
}

/// The checks installed on `endpoint`: `PingHealthz`, `LogHealthz`, the storage
/// check (`etcd` on healthz/livez, `etcd-readiness` on readyz), then the
/// boot-sequence checks and every `poststarthook/<name>` check.
pub fn health_checks(state: &Arc<ApiServerState>, endpoint: HealthEndpoint) -> Vec<HealthChecker> {
    let mut checks = vec![
        // `PingHealthz`.
        HealthChecker::ready("ping", Ok(())),
        // `LogHealthz` fails when klog's flush is blocked; this server logs
        // through `tracing`, which has no such flush to block on.
        HealthChecker::ready("log", Ok(())),
    ];
    let storage_state = state.clone();
    checks.push(HealthChecker::new(
        match endpoint {
            HealthEndpoint::Readyz => "etcd-readiness",
            _ => "etcd",
        },
        move || {
            let state = storage_state.clone();
            Box::pin(async move { check_storage(&state).await })
        },
    ));
    let hooks = crate::post_start_hooks::global();
    for (name, result) in hooks.boot_checks() {
        checks.push(HealthChecker::ready(name, result));
    }
    for (name, result) in hooks.checks() {
        checks.push(HealthChecker::ready(name, result.map_err(str::to_string)));
    }
    checks
}

fn text_response(status: StatusCode, body: String) -> Response {
    let mut resp = (status, body).into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

/// `formatQuoted`: `"a","b"`.
fn format_quoted(names: &BTreeSet<String>) -> String {
    names
        .iter()
        .map(|n| format!("{n:?}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// `handleRootHealth`: serves `checks` for the endpoint called `name`.
/// `query` is the raw query string (`exclude=<check>` repeated, `verbose`).
pub async fn handle_root_health(
    name: &str,
    checks: &[HealthChecker],
    query: Option<&str>,
) -> Response {
    let params: Vec<(String, String)> = url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
        .into_owned()
        .collect();
    let excluded: BTreeSet<String> = params
        .iter()
        .filter(|(k, _)| k == "exclude")
        .map(|(_, v)| v.clone())
        .collect();
    let mut unknown_excluded = excluded.clone();
    let mut failed_verbose_log = String::new();
    let mut failed_checks = Vec::new();
    let mut individual = String::new();
    for check in checks {
        if excluded.contains(&check.name) {
            unknown_excluded.remove(&check.name);
            individual.push_str(&format!("[+]{} excluded: ok\n", check.name));
            continue;
        }
        match check.run().await {
            Err(err) => {
                // "don't include the error since this endpoint is public."
                individual.push_str(&format!("[-]{} failed: reason withheld\n", check.name));
                failed_verbose_log.push_str(&format!("[-]{} failed: {err}\n", check.name));
                failed_checks.push(check.name.clone());
            }
            Ok(()) => individual.push_str(&format!("[+]{} ok\n", check.name)),
        }
    }
    if !unknown_excluded.is_empty() {
        individual.push_str(&format!(
            "warn: some health checks cannot be excluded: no matches for {}\n",
            format_quoted(&unknown_excluded)
        ));
    }
    // "always be verbose on failure"
    if !failed_checks.is_empty() {
        tracing::info!(
            "{} check failed: {name}\n{failed_verbose_log}",
            failed_checks.join(",")
        );
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{individual}{name} check failed\n"),
        );
    }
    if !params.iter().any(|(k, _)| k == "verbose") {
        return text_response(StatusCode::OK, "ok".to_string());
    }
    text_response(StatusCode::OK, format!("{individual}{name} check passed\n"))
}

/// `adaptCheckToHandler`: `<endpoint>/<check name>`; `None` is a 404 (no such
/// check on the endpoint's mux).
pub async fn handle_single_check(checks: &[HealthChecker], name: &str) -> Response {
    let Some(check) = checks.iter().find(|c| c.name == name) else {
        return text_response(StatusCode::NOT_FOUND, "404 page not found\n".to_string());
    };
    match check.run().await {
        Err(e) => text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}\n"),
        ),
        Ok(()) => text_response(StatusCode::OK, "ok".to_string()),
    }
}

pub async fn healthz(
    State(state): State<Arc<ApiServerState>>,
    RawQuery(query): RawQuery,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Healthz);
    handle_root_health("healthz", &checks, query.as_deref()).await
}

pub async fn livez(
    State(state): State<Arc<ApiServerState>>,
    RawQuery(query): RawQuery,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Livez);
    handle_root_health("livez", &checks, query.as_deref()).await
}

pub async fn readyz(
    State(state): State<Arc<ApiServerState>>,
    RawQuery(query): RawQuery,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Readyz);
    handle_root_health("readyz", &checks, query.as_deref()).await
}

pub async fn healthz_check(
    State(state): State<Arc<ApiServerState>>,
    Path(name): Path<String>,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Healthz);
    handle_single_check(&checks, &name).await
}

pub async fn livez_check(
    State(state): State<Arc<ApiServerState>>,
    Path(name): Path<String>,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Livez);
    handle_single_check(&checks, &name).await
}

pub async fn readyz_check(
    State(state): State<Arc<ApiServerState>>,
    Path(name): Path<String>,
) -> Response {
    let checks = health_checks(&state, HealthEndpoint::Readyz);
    handle_single_check(&checks, &name).await
}

/// Storage connectivity check (the `etcd` / `etcd-readiness` checks).
async fn check_storage(state: &Arc<ApiServerState>) -> Result<(), String> {
    use k3rs_common::resources::namespace::Namespace;
    use k3rs_storage::Storage;

    // Try to list namespaces as a health check
    state
        .storage
        .list::<Namespace>("/registry/namespaces/")
        .await
        .map_err(|e| format!("Storage check failed: {}", e))?;

    Ok(())
}

/// Metrics endpoint - returns Prometheus metrics
pub async fn metrics(State(state): State<Arc<ApiServerState>>) -> String {
    state.metrics.gather()
}

/// CPU profiling endpoint - returns pprof CPU profile
/// This endpoint captures CPU profiling data for performance analysis
#[cfg(feature = "pprof")]
pub async fn pprof_profile() -> Result<Vec<u8>, (StatusCode, String)> {
    use pprof::ProfilerGuard;
    use std::time::Duration;

    // Create a profiler guard
    let guard = ProfilerGuard::new(100).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to start profiler: {}", e),
        )
    })?;

    // Profile for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Build the report
    match guard.report().build() {
        Ok(report) => {
            let mut buffer = Vec::new();
            report.flamegraph(&mut buffer).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to generate flamegraph: {}", e),
                )
            })?;
            Ok(buffer)
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to build profile report: {}", e),
        )),
    }
}

/// Heap profiling endpoint - returns heap allocation profile
/// This endpoint provides information about memory allocations
#[cfg(feature = "pprof")]
pub async fn pprof_heap() -> Result<String, (StatusCode, String)> {
    // In Rust, we don't have runtime heap profiling like Go
    // This would require integration with jemalloc or another allocator
    // For now, return a stub response
    Ok("Heap profiling requires jemalloc or similar allocator integration".to_string())
}

/// Goroutine profiling endpoint (Rust equivalent: thread/task info)
/// This endpoint provides information about active threads and async tasks
pub async fn pprof_goroutine() -> Result<String, (StatusCode, String)> {
    // Collect thread information
    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Get Tokio runtime stats if available
    let runtime_info = format!(
        "Active threads: {}\nNote: Detailed async task profiling requires additional runtime instrumentation\n",
        thread_count
    );

    Ok(runtime_info)
}

/// Block profiling endpoint stub
/// Returns information about blocking operations
pub async fn pprof_block() -> Result<String, (StatusCode, String)> {
    Ok("Block profiling in Rust requires custom instrumentation\n\
        Consider using tokio-console for async task blocking analysis"
        .to_string())
}

/// Mutex profiling endpoint stub
/// Returns information about mutex contention
pub async fn pprof_mutex() -> Result<String, (StatusCode, String)> {
    Ok("Mutex profiling in Rust requires custom instrumentation\n\
        Consider using parking_lot with contention metrics"
        .to_string())
}

/// Symbol lookup endpoint
/// Returns symbol information for profiling
pub async fn pprof_symbol() -> String {
    "Symbol lookup not implemented\n\
     Rust binaries contain symbol information in debug builds"
        .to_string()
}

/// Trace endpoint - returns execution trace
/// This endpoint captures execution traces for analysis
pub async fn pprof_trace() -> Result<String, (StatusCode, String)> {
    Ok(
        "Execution tracing in Rust requires tokio-console or tracing subscriber integration"
            .to_string(),
    )
}

/// OpenID Connect discovery endpoint
/// Returns the OIDC provider metadata for service account issuer discovery
pub async fn openid_configuration(State(state): State<Arc<ApiServerState>>) -> Response {
    // Configured keys: `openIDConfigJSON` (pkg/serviceaccount/openidmetadata.go),
    // honouring --service-account-issuer and --service-account-jwks-uri.
    match state.token_manager.openid_config_json() {
        Ok(Some(cfg)) => return Json(cfg).into_response(),
        // `NewOpenIDMetadataProvider` refused (controlplane/apiserver/
        // server.go:104-125): the endpoints are not installed.
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
        Ok(None) => {}
    }
    Json(serde_json::json!({
        "issuer": "https://kubernetes.default.svc.cluster.local",
        "jwks_uri": "https://kubernetes.default.svc.cluster.local/openid/v1/jwks",
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"]
    }))
    .into_response()
}

/// JSON Web Key Set endpoint
/// Returns the public keys used to verify service account tokens.
/// K8s ref: pkg/serviceaccount/openidmetadata.go — publicJWKSFromKeys
pub async fn openid_jwks(State(state): State<Arc<ApiServerState>>) -> Response {
    // Both endpoints are installed or skipped together
    // (controlplane/apiserver/server.go:104-125).
    if state.token_manager.openid_config_json().is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    // Configured keys: `openIDKeysetJSON` (--service-account-key-file).
    if let Some(set) = state.token_manager.openid_keyset_json() {
        return Json(set).into_response();
    }
    // Try to load the RSA public key from standard paths
    let key_paths = [
        "/etc/kubernetes/pki/sa.pub",
        "/root/.k3rs/keys/sa-signing-key.pub",
    ];

    for path in &key_paths {
        if let Ok(pem_data) = std::fs::read(path) {
            if let Some(jwk) = rsa_pem_to_jwk(&pem_data) {
                return Json(serde_json::json!({ "keys": [jwk] })).into_response();
            }
        }
    }

    // No RSA key available — return empty JWKS
    Json(serde_json::json!({ "keys": [] })).into_response()
}

/// Convert an RSA public key PEM to JWK format for OIDC discovery.
/// K8s uses the jose library; we extract n and e from the DER directly.
fn rsa_pem_to_jwk(pem_data: &[u8]) -> Option<serde_json::Value> {
    use base64::Engine;

    // Parse PEM to get DER bytes
    let pem_str = std::str::from_utf8(pem_data).ok()?;
    let der_b64: String = pem_str
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(&der_b64)
        .ok()?;

    // Parse SubjectPublicKeyInfo DER to extract RSA n and e
    // ASN.1 structure: SEQUENCE { SEQUENCE { OID, NULL }, BIT STRING { SEQUENCE { INTEGER n, INTEGER e } } }
    let (n, e) = extract_rsa_params_from_spki(&der)?;

    // Compute key ID as SHA-256 thumbprint of JWK canonical form
    use sha2::{Digest, Sha256};
    let n_b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&n);
    let e_b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&e);

    let canonical = format!(r#"{{"e":"{}","kty":"RSA","n":"{}"}}"#, e_b64url, n_b64url);
    let kid = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(canonical.as_bytes()));

    Some(serde_json::json!({
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": n_b64url,
        "e": e_b64url,
    }))
}

/// Extract RSA modulus (n) and exponent (e) from a SubjectPublicKeyInfo DER.
fn extract_rsa_params_from_spki(der: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    // Quick ASN.1 DER parser for RSA SPKI
    // Skip outer SEQUENCE, inner SEQUENCE (OID+NULL), BIT STRING header
    let mut pos = 0;

    // Outer SEQUENCE
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;

    // Inner SEQUENCE (algorithm identifier)
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    let inner_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    pos += inner_len; // Skip algorithm identifier

    // BIT STRING
    if pos >= der.len() || der[pos] != 0x03 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;
    if pos >= der.len() {
        return None;
    }
    pos += 1; // Skip unused bits byte (should be 0)

    // Inner SEQUENCE containing n and e
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;

    // INTEGER n
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let n_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    let mut n = der[pos..pos + n_len].to_vec();
    // Strip leading zero (ASN.1 uses it for positive numbers)
    if !n.is_empty() && n[0] == 0 {
        n.remove(0);
    }
    pos += n_len;

    // INTEGER e
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let e_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    let e = der[pos..pos + e_len].to_vec();

    Some((n, e))
}

fn read_length(der: &[u8], pos: usize) -> Option<usize> {
    if pos >= der.len() {
        return None;
    }
    if der[pos] & 0x80 == 0 {
        Some(der[pos] as usize)
    } else {
        let num_bytes = (der[pos] & 0x7f) as usize;
        let mut len = 0usize;
        for i in 0..num_bytes {
            if pos + 1 + i >= der.len() {
                return None;
            }
            len = (len << 8) | der[pos + 1 + i] as usize;
        }
        Some(len)
    }
}

fn skip_length(der: &[u8], pos: usize) -> Option<usize> {
    if pos >= der.len() {
        return None;
    }
    if der[pos] & 0x80 == 0 {
        Some(pos + 1)
    } else {
        let num_bytes = (der[pos] & 0x7f) as usize;
        Some(pos + 1 + num_bytes)
    }
}
