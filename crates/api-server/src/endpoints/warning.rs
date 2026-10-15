//! Port of `staging/src/k8s.io/apiserver/pkg/endpoints/warning/warning.go`
//! (`AddWarningsHandler`) together with the endpoint-level warnings
//! `endpoints/installer.go:781-797` computes for it.
//!
//! Upstream wraps every installed route:
//!
//! ```go
//! handler = metrics.InstrumentRouteFunc(action.Verb, ..., deprecated, removedRelease, handler)
//! handler = utilwarning.AddWarningsHandler(handler, warnings)
//! ```
//!
//! (`installer.go:810-814`), and `AddWarningsHandler` calls
//! `warning.AddWarning(ctx, "", msg)` for each before the handler runs, so the
//! route's warnings are the first the request's recorder
//! (`endpoints/filters/warning.go`) sees, and a handler warning with the same
//! text is dropped as a duplicate (`recorder.AddWarning`, `r.recorded[text]`).
//!
//! DEVIATION: there is no request-scoped recorder here; handlers write their
//! `Warning:` headers onto the response themselves. The route's warnings are
//! therefore merged into the response headers after the handler returns, in
//! the order and with the de-duplication the recorder would have produced.
//! The recorder's total-size truncation (`truncateAtTotalRunes`) is not
//! ported, as it is not for any other warning source.

use axum::http::{header, HeaderMap, HeaderValue};

/// `AddWarningsHandler` (warning.go:27-39) over the route warnings of the
/// request's resource (`installer.go:781-797`).
pub async fn add_warnings_handler(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Routes are installed for resources only (`registerResourceHandlers`), so
    // a non-resource request has no endpoint-level warnings.
    let attrs = crate::audit::request_info(req.method(), req.uri().path(), req.uri().query());
    let warnings = if attrs.is_resource_request {
        crate::deprecation::endpoint_warnings(
            &attrs.api_group,
            &attrs.api_version,
            &attrs.resource,
            &attrs.subresource,
        )
    } else {
        Vec::new()
    };
    let mut resp = next.run(req).await;
    // warning.go:28-30: `if len(warnings) == 0 { return handler }`.
    if !warnings.is_empty() {
        add_route_warnings(resp.headers_mut(), &warnings);
    }
    resp
}

/// Record `route_warnings` ahead of the warnings the handler already wrote,
/// as `recorder.AddWarning` (filters/warning.go:69-92) would have had they
/// been added first: an empty text is dropped, a repeated text is recorded
/// once, and a text `net.NewWarningHeader` rejects (control characters,
/// apimachinery util/net/http.go:664-666) is not written.
pub fn add_route_warnings(headers: &mut HeaderMap, route_warnings: &[String]) {
    let written: Vec<HeaderValue> = headers.get_all(header::WARNING).iter().cloned().collect();
    headers.remove(header::WARNING);
    let mut recorded: Vec<HeaderValue> = Vec::new();
    for text in route_warnings {
        if text.is_empty() || text.chars().any(char::is_control) {
            continue;
        }
        let value = crate::handlers::validation::format_warning_header(text);
        let Ok(value) = HeaderValue::from_str(&value) else {
            continue;
        };
        if !recorded.contains(&value) {
            recorded.push(value);
        }
    }
    for value in written {
        if !recorded.contains(&value) {
            recorded.push(value);
        }
    }
    for value in recorded {
        headers.append(header::WARNING, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, middleware, routing::get, Router};
    use tower::ServiceExt;

    fn warnings(h: &HeaderMap) -> Vec<String> {
        h.get_all(header::WARNING)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect()
    }

    const MAP_WARNING: &str = "299 - \"admissionregistration.k8s.io/v1alpha1 MutatingAdmissionPolicy is deprecated in v1.35+, unavailable in v1.38+\"";

    fn app() -> Router {
        Router::new()
            .route(
                "/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies",
                get(|| async { "ok" }),
            )
            .route(
                "/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies/:name",
                get(|| async {
                    let mut h = HeaderMap::new();
                    h.append(
                        header::WARNING,
                        HeaderValue::from_static("299 - \"from the handler\""),
                    );
                    h.append(header::WARNING, HeaderValue::from_static(MAP_WARNING));
                    (axum::http::StatusCode::NOT_FOUND, h, "missing")
                }),
            )
            .route(
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies",
                get(|| async { "ok" }),
            )
            .layer(middleware::from_fn(add_warnings_handler))
    }

    async fn get_warnings(uri: &str) -> Vec<String> {
        let resp = app()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        warnings(resp.headers())
    }

    // installer.go:781-797 + warning.go:27-39: a request to a deprecated
    // route carries the route's WarningMessage as `Warning: 299 - "..."`.
    #[tokio::test]
    async fn deprecated_route_warns() {
        assert_eq!(
            get_warnings("/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies")
                .await,
            vec![MAP_WARNING.to_string()]
        );
    }

    // warning.go:28-30: no warnings, the handler is returned unwrapped.
    #[tokio::test]
    async fn stable_route_does_not_warn() {
        assert!(
            get_warnings("/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies")
                .await
                .is_empty()
        );
    }

    // The route's warning is added before the handler runs, so it comes first,
    // and the handler's identical text is a duplicate (filters/warning.go
    // `recorder.AddWarning`). Error responses carry it too: the warning is
    // recorded before the handler writes anything.
    #[tokio::test]
    async fn route_warning_precedes_and_dedups_handler_warnings() {
        assert_eq!(
            get_warnings("/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies/x")
                .await,
            vec![
                MAP_WARNING.to_string(),
                "299 - \"from the handler\"".to_string()
            ]
        );
    }

    // Test_recorder_AddWarning (filters/warning_test.go:25-100): empty text is
    // dropped and duplicate text recorded once.
    #[test]
    fn add_route_warnings_follows_the_recorder() {
        let mut h = HeaderMap::new();
        add_route_warnings(&mut h, &[]);
        assert!(warnings(&h).is_empty());
        add_route_warnings(
            &mut h,
            &[
                String::new(),
                "mytext1".into(),
                "mytext2".into(),
                "mytext1".into(),
            ],
        );
        assert_eq!(
            warnings(&h),
            vec![
                "299 - \"mytext1\"".to_string(),
                "299 - \"mytext2\"".to_string()
            ]
        );
    }
}
