//! `WithMaxInFlightLimit`, the in-flight request limiter used when API
//! Priority and Fairness is off.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/filters/maxinflight.go`
//! (`WithMaxInFlightLimit`, `retryAfter`, `nonMutatingRequestVerbs`), chosen in
//! `pkg/server/config.go:1022-1033` (`c.FlowControl == nil`), with the options
//! of `pkg/server/options/server_run_options.go` (`--max-requests-inflight`,
//! `--max-mutating-requests-inflight`, `Validate` :197-203).
//!
//! Requests the long-running check matches (`BasicLongRunningRequestCheck`
//! with kube-apiserver's verbs and subresources: watch, proxy, and
//! attach/exec/proxy/log/portforward) are not counted, as upstream.
//!
//! Deviations: axum's `next.run` returns once the response head is ready, so
//! a slot is released when the handler returns, not when a streamed body has
//! finished (the same as the APF filter here), which is more permissive than
//! upstream, never stricter. The `executing_*_concurrency` ratio gauges are
//! not ported; the executing watermark feeds the same inflight-request gauges
//! as the APF filter (`startWatermarkMaintenance`).

use std::sync::{Arc, OnceLock};

use axum::extract::{Request, State};
use axum::http::{header::RETRY_AFTER, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::Semaphore;

use crate::apiserver_request_metrics::{self, RequestMeta};
use crate::audit::request_info;
use crate::flow_control_filter::is_long_running;

/// `retryAfter`.
const RETRY_AFTER_SECONDS: &str = "1";
/// `user.SystemPrivilegedGroup`.
const SYSTEM_PRIVILEGED_GROUP: &str = "system:masters";
const COMPONENT: &str = "apiserver";
/// `nonMutatingRequestVerbs`.
const NON_MUTATING_REQUEST_VERBS: &[&str] = &["get", "list", "watch"];

/// The channels of `WithMaxInFlightLimit`: a limit of 0 leaves that kind of
/// request unlimited (`nil` channel).
pub struct MaxInFlight {
    non_mutating: Option<Arc<Semaphore>>,
    mutating: Option<Arc<Semaphore>>,
}

impl MaxInFlight {
    /// `nil` when both limits are 0, where upstream returns the handler as is.
    pub fn new(non_mutating_limit: usize, mutating_limit: usize) -> Option<Self> {
        if non_mutating_limit == 0 && mutating_limit == 0 {
            return None;
        }
        let sem = |n: usize| (n != 0).then(|| Arc::new(Semaphore::new(n)));
        Some(Self {
            non_mutating: sem(non_mutating_limit),
            mutating: sem(mutating_limit),
        })
    }
}

/// `ServerRunOptions.Validate` for the two limits (server_run_options.go:197-203).
pub fn validate_limits(max_requests: i64, max_mutating: i64) -> Vec<String> {
    let mut errs = Vec::new();
    if max_requests < 0 {
        errs.push("--max-requests-inflight can not be negative value".to_string());
    }
    if max_mutating < 0 {
        errs.push("--max-mutating-requests-inflight can not be negative value".to_string());
    }
    errs
}

/// `tooManyRequests` (priority-and-fairness.go:377-381) with `retryAfter`.
fn too_many_requests() -> Response {
    let mut r = (
        StatusCode::TOO_MANY_REQUESTS,
        "Too many requests, please try again later.\n",
    )
        .into_response();
    r.headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER_SECONDS));
    r
}

/// The handler `WithMaxInFlightLimit` returns.
pub async fn with_max_in_flight_limit(
    State(limits): State<Arc<MaxInFlight>>,
    req: Request,
    next: Next,
) -> Response {
    let attrs = request_info(req.method(), req.uri().path(), req.uri().query());

    // "Skip tracking long running events."
    if is_long_running(
        &attrs.verb,
        attrs.is_resource_request,
        &attrs.subresource,
        &attrs.path,
    ) {
        return next.run(req).await;
    }

    let is_mutating = !NON_MUTATING_REQUEST_VERBS.contains(&attrs.verb.as_str());
    let Some(sem) = (if is_mutating {
        &limits.mutating
    } else {
        &limits.non_mutating
    }) else {
        return next.run(req).await;
    };

    match sem.clone().try_acquire_owned() {
        Ok(permit) => {
            // `watermark.recordMutating/recordReadOnly` around the request.
            apiserver_request_metrics::note_executing_delta(is_mutating, 1);
            let resp = next.run(req).await;
            drop(permit);
            apiserver_request_metrics::note_executing_delta(is_mutating, -1);
            resp
        }
        Err(_) => {
            // "A system:master is so powerful that they should always get an
            // answer. It's a super-admin or a loopback connection."
            let privileged = req
                .extensions()
                .get::<rusternetes_middleware::AuthContext>()
                .is_some_and(|c| c.user.groups.iter().any(|g| g == SYSTEM_PRIVILEGED_GROUP));
            if privileged {
                return next.run(req).await;
            }
            let meta = RequestMeta {
                method: req.method().as_str().to_string(),
                query: req.uri().query().unwrap_or("").to_string(),
                path: attrs.path.clone(),
                apply_patch: false,
                is_resource_request: attrs.is_resource_request,
                verb: attrs.verb.clone(),
                namespace: attrs.namespace.clone(),
                name: attrs.name.clone(),
                api_group: attrs.api_group.clone(),
                api_version: attrs.api_version.clone(),
                resource: attrs.resource.clone(),
                subresource: attrs.subresource.clone(),
                ..Default::default()
            };
            apiserver_request_metrics::record_dropped_request(&meta, COMPONENT, is_mutating);
            apiserver_request_metrics::record_request_termination(
                &meta,
                COMPONENT,
                StatusCode::TOO_MANY_REQUESTS.as_u16(),
            );
            too_many_requests()
        }
    }
}

static MAX_IN_FLIGHT: OnceLock<Arc<MaxInFlight>> = OnceLock::new();

/// Install the process-wide limiter built at startup; the router layers it
/// where `WithPriorityAndFairness` would be when APF is off.
pub fn install_max_in_flight(limits: Arc<MaxInFlight>) {
    let _ = MAX_IN_FLIGHT.set(limits);
}

pub fn installed() -> Option<Arc<MaxInFlight>> {
    MAX_IN_FLIGHT.get().cloned()
}
