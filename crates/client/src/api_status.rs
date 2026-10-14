//! Typed API error carried by [`crate::http::ApiClient`] failures.
//!
//! Port of the `StatusError` surface client-go hands back from a non-2xx
//! response, plus the code-fallback predicates in
//! `staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go`
//! (`knownReasons` :47-68, `NewGenericServerResponse` :437-525,
//! `reasonAndCodeForError` :817-822, `Is*` :529-684, `IsUnsupportedMediaType`
//! :607-615). client-go's `Result.Error()` (rest/request.go:1489-1511)
//! replaces the code-derived generic error with the body's `Status` when the
//! body decodes as a `Status` with `status == Failure`.
//!
//! The `Display` text is unchanged from the pre-typed implementation
//! (`Error from server (<reason>): <message>` / `request failed with status
//! ...`) so string-matching callers keep working; the code and reason are now
//! available structurally via [`reason_and_code_for_error`].

use std::fmt;

/// Reasons in `knownReasons` (errors.go:47-68). A reason outside this set
/// (including "" / `Unknown`) lets the HTTP code decide.
pub const KNOWN_REASONS: &[&str] = &[
    "Unauthorized",
    "Forbidden",
    "NotFound",
    "AlreadyExists",
    "Conflict",
    "Gone",
    "Invalid",
    "ServerTimeout",
    "StoreReadError",
    "Timeout",
    "TooManyRequests",
    "BadRequest",
    "MethodNotAllowed",
    "NotAcceptable",
    "RequestEntityTooLarge",
    "UnsupportedMediaType",
    "InternalError",
    "Expired",
    "ServiceUnavailable",
];

/// The `metav1.Status` fields of a failed API call that error predicates read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiStatusError {
    /// `Status.Code` (0 when the body's Status carried none).
    pub code: u16,
    /// `Status.Reason`; empty is `StatusReasonUnknown`.
    pub reason: String,
    /// Text rendered by `Display` (legacy format, see module docs).
    pub message: String,
    /// `Status.Details.RetryAfterSeconds`; `None` is `Details == nil`
    /// (no details at all), `Some(0)` is details without a delay.
    pub retry_after_seconds: Option<i32>,
}

impl fmt::Display for ApiStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ApiStatusError {}

/// Reason `NewGenericServerResponse` (errors.go:437-493) derives from the code
/// when the body carried no usable `Status`. `verb` is the HTTP method.
pub fn generic_reason_for_code(code: u16, verb: &str) -> &'static str {
    match code {
        409 if verb.eq_ignore_ascii_case("POST") => "AlreadyExists",
        409 => "Conflict",
        404 => "NotFound",
        400 => "BadRequest",
        401 => "Unauthorized",
        403 => "Forbidden",
        406 => "NotAcceptable",
        415 => "UnsupportedMediaType",
        405 => "MethodNotAllowed",
        422 => "Invalid",
        503 => "ServiceUnavailable",
        504 => "Timeout",
        429 => "TooManyRequests",
        c if c >= 500 => "InternalError",
        _ => "",
    }
}

/// `reasonAndCodeForError` (errors.go:817-822): finds an [`ApiStatusError`]
/// anywhere in the chain (`errors.As`), else `("", 0)`.
pub fn reason_and_code_for_error(err: &anyhow::Error) -> (String, u16) {
    err.chain()
        .find_map(|c| c.downcast_ref::<ApiStatusError>())
        .map(|s| (s.reason.clone(), s.code))
        .unwrap_or_default()
}

/// `apierrors.SuggestsClientDelay` (errors.go:791-805): the suggested seconds
/// to wait and true when the error carries `Status.Details` and either its
/// reason is `ServerTimeout` or `RetryAfterSeconds > 0`. Finds the status
/// anywhere in the chain (`errors.As`).
pub fn suggests_client_delay(err: &anyhow::Error) -> Option<i32> {
    let s = err
        .chain()
        .find_map(|c| c.downcast_ref::<ApiStatusError>())?;
    let secs = s.retry_after_seconds?;
    if s.reason == "ServerTimeout" || secs > 0 {
        Some(secs)
    } else {
        None
    }
}

/// `utilnet.IsHTTP2ConnectionLost` (apimachinery/pkg/util/net/util.go:51-54):
/// the rendered error contains "http2: client connection lost". Go's
/// `err.Error()` includes every wrapped cause, so each link of the chain is
/// searched.
pub fn is_http2_connection_lost(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|c| c.to_string().contains("http2: client connection lost"))
}

/// The shared `reason == X || (!knownReasons[reason] && code == C)` shape of
/// `IsNotFound`/`IsConflict`/`IsInvalid`/`IsBadRequest`/`IsForbidden`/
/// `IsNotAcceptable`/`IsUnsupportedMediaType`.
fn reason_or_code(err: &anyhow::Error, reason: &str, code: u16) -> bool {
    let (r, c) = reason_and_code_for_error(err);
    r == reason || (!KNOWN_REASONS.contains(&r.as_str()) && c == code)
}

/// `IsNotFound` (errors.go:529-538).
pub fn is_not_found(err: &anyhow::Error) -> bool {
    reason_or_code(err, "NotFound", 404)
}

/// `IsAlreadyExists` (errors.go:542-544): reason only, no code fallback.
pub fn is_already_exists(err: &anyhow::Error) -> bool {
    reason_and_code_for_error(err).0 == "AlreadyExists"
}

/// `IsConflict` (errors.go:548-557).
pub fn is_conflict(err: &anyhow::Error) -> bool {
    reason_or_code(err, "Conflict", 409)
}

/// `IsGone` (errors.go:574-583).
pub fn is_gone(err: &anyhow::Error) -> bool {
    reason_or_code(err, "Gone", 410)
}

/// `IsResourceExpired` (errors.go:588-590): `ReasonForError(err) == Expired`,
/// reason only, no code fallback.
pub fn is_resource_expired(err: &anyhow::Error) -> bool {
    reason_and_code_for_error(err).0 == "Expired"
}

/// `IsInvalid` (errors.go:561-570).
pub fn is_invalid(err: &anyhow::Error) -> bool {
    reason_or_code(err, "Invalid", 422)
}

/// `IsBadRequest` (errors.go:647-656).
pub fn is_bad_request(err: &anyhow::Error) -> bool {
    reason_or_code(err, "BadRequest", 400)
}

/// `IsForbidden` (errors.go:675-684).
pub fn is_forbidden(err: &anyhow::Error) -> bool {
    reason_or_code(err, "Forbidden", 403)
}

/// `IsTimeout` (errors.go:689-698).
pub fn is_timeout(err: &anyhow::Error) -> bool {
    reason_or_code(err, "Timeout", 504)
}

/// `IsInternalError` (errors.go:711-720).
pub fn is_internal_error(err: &anyhow::Error) -> bool {
    reason_or_code(err, "InternalError", 500)
}

/// `IsTooManyRequests` (errors.go:725-738): the code check does not require an
/// unknown reason.
pub fn is_too_many_requests(err: &anyhow::Error) -> bool {
    let (reason, code) = reason_and_code_for_error(err);
    reason == "TooManyRequests" || code == 429
}

/// `IsUnsupportedMediaType` (errors.go:607-615).
pub fn is_unsupported_media_type(err: &anyhow::Error) -> bool {
    reason_or_code(err, "UnsupportedMediaType", 415)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(code: u16, reason: &str) -> anyhow::Error {
        anyhow::Error::new(ApiStatusError {
            code,
            reason: reason.into(),
            message: "m".into(),
            retry_after_seconds: None,
        })
        .context("outer")
    }

    #[test]
    fn code_fallback_only_for_unknown_reason() {
        assert!(is_unsupported_media_type(&err(415, "")));
        assert!(is_unsupported_media_type(&err(415, "SomethingNew")));
        assert!(!is_unsupported_media_type(&err(415, "Forbidden")));
        assert!(is_unsupported_media_type(&err(0, "UnsupportedMediaType")));
        assert!(!is_unsupported_media_type(&err(200, "")));
    }

    // errors.go IsTimeout / IsInternalError / IsTooManyRequests
    #[test]
    fn retry_predicates() {
        assert!(is_timeout(&err(504, "")));
        assert!(is_timeout(&err(0, "Timeout")));
        assert!(!is_timeout(&err(504, "Forbidden")));
        assert!(is_internal_error(&err(500, "")));
        assert!(is_internal_error(&err(0, "InternalError")));
        assert!(!is_internal_error(&err(500, "Forbidden")));
        assert!(is_too_many_requests(&err(429, "")));
        // IsTooManyRequests' code check does not require an unknown reason
        assert!(is_too_many_requests(&err(429, "Forbidden")));
        assert!(is_too_many_requests(&err(0, "TooManyRequests")));
        assert!(!is_too_many_requests(&err(500, "")));
    }

    #[test]
    fn already_exists_has_no_code_fallback() {
        assert!(!is_already_exists(&err(409, "")));
        assert!(is_already_exists(&err(409, "AlreadyExists")));
        assert!(is_conflict(&err(409, "")));
        assert!(!is_conflict(&err(409, "AlreadyExists")));
    }

    #[test]
    fn gone_has_code_fallback_expired_does_not() {
        assert!(is_gone(&err(410, "")));
        assert!(is_gone(&err(0, "Gone")));
        assert!(!is_gone(&err(410, "Expired")));
        assert!(is_resource_expired(&err(410, "Expired")));
        assert!(!is_resource_expired(&err(410, "")));
    }

    #[test]
    fn untyped_error_is_unknown_zero() {
        let e = anyhow::anyhow!("boom");
        assert_eq!(reason_and_code_for_error(&e), (String::new(), 0));
        assert!(!is_not_found(&e));
    }

    #[test]
    fn generic_reason_matches_upstream_switch() {
        assert_eq!(generic_reason_for_code(409, "POST"), "AlreadyExists");
        assert_eq!(generic_reason_for_code(409, "PUT"), "Conflict");
        assert_eq!(generic_reason_for_code(422, "PUT"), "Invalid");
        assert_eq!(generic_reason_for_code(500, "GET"), "InternalError");
        assert_eq!(generic_reason_for_code(418, "GET"), "");
    }

    fn with_delay(reason: &str, secs: Option<i32>) -> anyhow::Error {
        anyhow::Error::new(ApiStatusError {
            code: 500,
            reason: reason.into(),
            message: "m".into(),
            retry_after_seconds: secs,
        })
        .context("outer")
    }

    #[test]
    fn suggests_client_delay_matches_upstream() {
        let d = |r: &str, s: Option<i32>| suggests_client_delay(&with_delay(r, s));
        // ServerTimeout with details: suggested even at 0 seconds.
        assert_eq!(d("ServerTimeout", Some(0)), Some(0));
        assert_eq!(d("ServerTimeout", Some(3)), Some(3));
        // Any reason with RetryAfterSeconds > 0.
        assert_eq!(d("", Some(5)), Some(5));
        // Details without a delay, or no details at all: no suggestion.
        assert_eq!(d("", Some(0)), None);
        assert_eq!(d("", None), None);
        // ServerTimeout without Details (Details == nil) is not suggested.
        assert_eq!(d("ServerTimeout", None), None);
        assert_eq!(suggests_client_delay(&anyhow::anyhow!("boom")), None);
    }

    #[test]
    fn http2_connection_lost_searches_the_chain() {
        let e = anyhow::anyhow!("http2: client connection lost").context("Post failed");
        assert!(is_http2_connection_lost(&e));
        let wrapped = anyhow::anyhow!("send: http2: client connection lost");
        assert!(is_http2_connection_lost(&wrapped));
        assert!(!is_http2_connection_lost(&anyhow::anyhow!(
            "connection reset"
        )));
    }
}
