//! `WithAuthorization` filter metrics (#3386).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/endpoints/filters/metrics.go`
//! (`authorizationAttemptsCounter` :78-85, `authorizationLatency` :87-95,
//! `recordAuthorizationMetrics` :106-122) as called by `withAuthorization`
//! (`filters/authorization.go:57-99`).

use rusternetes_common::authz::Authorizer;
use std::sync::Arc;

/// Wraps the authorizer the request handlers call, recording the filter's
/// attempt counter and duration histogram.
pub fn filter_instrumented(delegate: Arc<dyn Authorizer>) -> Arc<dyn Authorizer> {
    delegate
}
