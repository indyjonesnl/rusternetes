use thiserror::Error;

use crate::validation::field::ErrorList;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    /// Legacy 422 Invalid error carrying only a flat string. Used by handler
    /// paths that have not yet been refactored to thread a structured
    /// `field::ErrorList`. The IntoResponse impl parses the upstream-style
    /// `"<field>: <ErrorType>: <detail>"` wording back into a single
    /// `StatusCause` so the response body still matches upstream shape.
    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    /// Structured 422 Invalid error carrying a `field::ErrorList`. Each entry
    /// maps to one `Status.details.causes[]` entry with the upstream
    /// `FieldValueXxx` reason taxonomy — mirrors `NewInvalid` upstream.
    ///
    /// Validators should accumulate ALL violations into the list rather than
    /// short-circuiting on the first failure; the IntoResponse impl emits
    /// one cause per error in field-path order.
    #[error("Invalid resource: {causes}", causes = format_error_list(.0))]
    Invalid(ErrorList),

    /// Generic bad-request error mapped to HTTP 400 / reason=BadRequest.
    ///
    /// Upstream Kubernetes uses 400/BadRequest for strict-decode errors and
    /// other "client supplied a syntactically malformed request" cases. Use
    /// `InvalidResource` instead when the request parses cleanly but fails a
    /// field-level validator (that maps to 422/Invalid).
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    /// A write the storage backend refused because the serialized object
    /// exceeded its request-size limit (etcd `request is too large`, or a gRPC
    /// `ResourceExhausted` "trying to send message larger than max").
    /// Upstream passes the raw backend error through the store untouched and
    /// the handlers recognise it with `isTooLargeError`
    /// (endpoints/handlers/rest.go:457-471); here the backend tags it so
    /// [`Error::is_too_large_error`] can. It surfaces as a 500 InternalError,
    /// exactly as the raw etcd error does upstream.
    #[error("Storage error: {0}")]
    StorageTooLarge(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Authorization error: {0}")]
    Authorization(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Too many requests: {0}")]
    TooManyRequests(String),

    #[error("Gone: {0}")]
    Gone(String),

    /// Like [`Error::Gone`] but for a compacted continue token during a
    /// *paginated* list: carries a fresh "inconsistent" continue token so the
    /// client can resume from the same key at the current resource version.
    /// Maps to `410 Gone` (reason `Gone`) with the token attached to the
    /// response `metadata.continue`, mirroring upstream
    /// `handleCompactedErrorForPaging` (etcd3/errors.go), which returns a
    /// ResourceExpired 410 whose `ListMeta.Continue` is a fresh (rv=-1) token
    /// (`test/e2e/apimachinery/chunking.go` "continue listing from the last
    /// key ... though the list is inconsistent").
    #[error("{message}")]
    GoneWithContinue {
        message: String,
        continue_token: String,
    },

    #[error("Unsupported media type: {0}")]
    UnsupportedMediaType(String),

    #[error("Not acceptable: {0}")]
    NotAcceptable(String),

    #[error("Internal error: {0}")]
    Internal(String),

    /// An error that is an API `Status` of its own, upstream's
    /// `*errors.StatusError`: the reason, code and `details` (causes,
    /// `retryAfterSeconds`) are the constructor's, not derived from a variant.
    /// Used where upstream builds a Status by hand, e.g. the eviction
    /// subresource's `createTooManyRequestsError`
    /// (`pkg/registry/core/pod/storage/eviction.go:413-421`).
    #[error("{}", .0.message.as_deref().unwrap_or(""))]
    Status(Box<crate::types::Status>),
}

/// Render an `ErrorList` upstream-style: one error per line joined by `; `.
fn format_error_list(errs: &ErrorList) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

impl Error {
    /// `isTooLargeError` (endpoints/handlers/rest.go:457-471): the write was
    /// refused for its serialized size, so the handlers may retry it without
    /// managedFields.
    pub fn is_too_large_error(&self) -> bool {
        matches!(self, Error::StorageTooLarge(_))
    }

    /// `errors.NewInvalid(qualifiedKind, name, errs)`
    /// (`apimachinery/pkg/api/errors/errors.go:284-312`, release-1.35): a 422
    /// Status whose message is `<Kind> "<name>" is invalid: <aggregate>` (the
    /// qualified kind renders as `Kind.group` for a non-core group), whose
    /// `details` carry group/kind/name, and whose causes carry the error
    /// type, `ErrorBody()` as the message and the field path.
    pub fn new_invalid(group: &str, kind: &str, name: &str, errs: ErrorList) -> Error {
        let qualified = if group.is_empty() {
            kind.to_string()
        } else {
            format!("{kind}.{group}")
        };
        let message = if errs.is_empty() {
            format!("{qualified} \"{name}\" is invalid")
        } else {
            // `ErrorList.ToAggregate()` dedups identical messages and renders
            // one error as itself, several as `[a, b]`.
            let mut msgs: Vec<String> = Vec::new();
            for e in &errs {
                let m = e.to_string();
                if !msgs.contains(&m) {
                    msgs.push(m);
                }
            }
            if msgs.len() == 1 {
                format!("{qualified} \"{name}\" is invalid: {}", msgs[0])
            } else {
                format!("{qualified} \"{name}\" is invalid: [{}]", msgs.join(", "))
            }
        };
        let causes: Vec<crate::types::StatusCause> = errs
            .iter()
            .map(|e| crate::types::StatusCause {
                reason: Some(e.error_type.cause_reason().to_string()),
                message: Some(e.error_body()),
                field: Some(e.field.clone()),
            })
            .collect();
        let details = crate::types::StatusDetails {
            // `Name`/`Group`/`Kind` are `omitempty` in metav1.StatusDetails.
            name: Some(name.to_string()).filter(|n| !n.is_empty()),
            group: Some(group.to_string()).filter(|g| !g.is_empty()),
            kind: Some(kind.to_string()).filter(|k| !k.is_empty()),
            uid: None,
            causes: Some(causes).filter(|c| !c.is_empty()),
            retry_after_seconds: None,
        };
        Error::Status(Box::new(crate::types::Status::failure_with_details(
            message, "Invalid", 422, details,
        )))
    }

    /// `errs.ToAggregate()` returned from a REST storage method as a plain
    /// `error` (e.g. `BindingREST.Create`,
    /// `pkg/registry/core/pod/storage/storage.go:189`). It is not an
    /// `APIStatus`, so `ErrorToAPIStatus`'s default arm
    /// (`apiserver/pkg/endpoints/handlers/responsewriters/status.go:60-82`)
    /// renders HTTP 500, reason `StatusReasonUnknown` (`""`, omitted on the
    /// wire), `Message: err.Error()` and no details. The message is
    /// `aggregate.Error()` (`apimachinery/pkg/util/errors/errors.go:70-96`):
    /// one error as itself, several deduplicated as `[a, b]`.
    pub fn aggregate(errs: &ErrorList) -> Error {
        Error::unknown_error(Error::aggregate_message(errs))
    }

    /// `errs.ToAggregate().Error()` — the text [`Error::aggregate`] carries.
    pub fn aggregate_message(errs: &ErrorList) -> String {
        let mut msgs: Vec<String> = Vec::new();
        for e in errs {
            let m = e.to_string();
            if !msgs.contains(&m) {
                msgs.push(m);
            }
        }
        if msgs.len() == 1 {
            msgs.remove(0)
        } else {
            format!("[{}]", msgs.join(", "))
        }
    }

    /// Any plain Go `error` that is not an `APIStatus` reaching the endpoint:
    /// `ErrorToAPIStatus`'s default arm
    /// (`apiserver/pkg/endpoints/handlers/responsewriters/status.go:60-81`)
    /// renders HTTP 500, reason `StatusReasonUnknown` (`""`, omitted on the
    /// wire), `Message: err.Error()` and no details.
    pub fn unknown_error(message: impl Into<String>) -> Error {
        let mut status = crate::types::Status::failure(message, "", 500);
        status.reason = None;
        Error::Status(Box::new(status))
    }

    /// `errors.NewGenericServerResponse(code, verb, qualifiedResource, name,
    /// serverMessage, retryAfterSeconds, isUnexpectedResponse)`
    /// (`apimachinery/pkg/api/errors/errors.go:437-524`, release-1.35).
    ///
    /// `group`/`resource` are the `schema.GroupResource`; an empty `resource`
    /// is `qualifiedResource.Empty()` (the qualified resource's `String()` is
    /// `resource.group`, or just `resource` for the core group). `details` is
    /// always present, with `causes` only for `is_unexpected_response`.
    #[allow(clippy::too_many_arguments)]
    pub fn generic_server_response(
        code: u16,
        verb: &str,
        group: &str,
        resource: &str,
        name: &str,
        server_message: &str,
        retry_after_seconds: i32,
        is_unexpected_response: bool,
    ) -> Error {
        let mut reason = "Unknown";
        let mut message = format!(
            "the server responded with the status code {code} but did not return more information"
        );
        match code {
            409 => {
                reason = if verb == "POST" {
                    "AlreadyExists"
                } else {
                    "Conflict"
                };
                message = "the server reported a conflict".to_string();
            }
            404 => {
                reason = "NotFound";
                message = "the server could not find the requested resource".to_string();
            }
            400 => {
                reason = "BadRequest";
                message = "the server rejected our request for an unknown reason".to_string();
            }
            401 => {
                reason = "Unauthorized";
                message = "the server has asked for the client to provide credentials".to_string();
            }
            403 => {
                reason = "Forbidden";
                // The server message has details about who is trying to
                // perform what action. Keep its message.
                message = server_message.to_string();
            }
            406 => {
                reason = "NotAcceptable";
                message = if server_message.is_empty() || server_message == "unknown" {
                    "the server was unable to respond with a content type that the client supports"
                        .to_string()
                } else {
                    server_message.to_string()
                };
            }
            415 => {
                reason = "UnsupportedMediaType";
                message = server_message.to_string();
            }
            405 => {
                reason = "MethodNotAllowed";
                message =
                    "the server does not allow this method on the requested resource".to_string();
            }
            422 => {
                reason = "Invalid";
                message =
                    "the server rejected our request due to an error in our request".to_string();
            }
            503 => {
                reason = "ServiceUnavailable";
                message = "the server is currently unable to handle the request".to_string();
            }
            504 => {
                reason = "Timeout";
                message = "the server was unable to return a response in the time allotted, but may still be processing the request".to_string();
            }
            429 => {
                reason = "TooManyRequests";
                message =
                    "the server has received too many requests and has asked us to try again later"
                        .to_string();
            }
            c if c >= 500 => {
                reason = "InternalError";
                message = format!(
                    "an error on the server ({server_message:?}) has prevented the request from succeeding"
                );
            }
            _ => {}
        }
        if !resource.is_empty() {
            let qualified = if group.is_empty() {
                resource.to_string()
            } else {
                format!("{resource}.{group}")
            };
            message = if name.is_empty() {
                format!("{message} ({} {qualified})", verb.to_lowercase())
            } else {
                format!("{message} ({} {qualified} {name})", verb.to_lowercase())
            };
        }
        let causes = is_unexpected_response.then(|| {
            vec![crate::types::StatusCause {
                reason: Some("UnexpectedServerResponse".to_string()),
                message: Some(server_message.to_string()),
                field: None,
            }]
        });
        let details = crate::types::StatusDetails {
            name: Some(name.to_string()).filter(|n| !n.is_empty()),
            group: Some(group.to_string()).filter(|g| !g.is_empty()),
            kind: Some(resource.to_string()).filter(|r| !r.is_empty()),
            uid: None,
            causes,
            retry_after_seconds: Some(retry_after_seconds).filter(|s| *s != 0),
        };
        Error::Status(Box::new(crate::types::Status::failure_with_details(
            message, reason, code, details,
        )))
    }

    /// Returns the machine-readable reason string matching Kubernetes StatusReason values
    pub fn reason(&self) -> &str {
        match self {
            Error::NotFound(_) => "NotFound",
            Error::AlreadyExists(_) => "AlreadyExists",
            Error::InvalidResource(_) => "Invalid",
            Error::Invalid(_) => "Invalid",
            Error::BadRequest(_) => "BadRequest",
            Error::Serialization(_) => "BadRequest",
            Error::Storage(_) | Error::StorageTooLarge(_) => "InternalError",
            Error::Network(_) => "ServiceUnavailable",
            Error::Authentication(_) => "Unauthorized",
            // endpoints/filters/authorization.go:87-91 (InternalError).
            Error::Authorization(_) => "InternalError",
            Error::Forbidden(_) => "Forbidden",
            Error::Conflict(_) => "Conflict",
            Error::TooManyRequests(_) => "TooManyRequests",
            Error::Gone(_) => "Gone",
            Error::GoneWithContinue { .. } => "Gone",
            Error::UnsupportedMediaType(_) => "UnsupportedMediaType",
            Error::NotAcceptable(_) => "NotAcceptable",
            Error::Internal(_) => "InternalError",
            Error::Status(status) => status.reason.as_deref().unwrap_or(""),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "axum-support")]
/// Response extension carrying the `Status` an [`Error`] was rendered from.
///
/// Upstream writes every error through `responsewriters.ErrorNegotiated`
/// (`apiserver/pkg/endpoints/handlers/responsewriters/writers.go:381-397`),
/// which has the request in hand. `IntoResponse` has no request, so it renders
/// JSON and leaves this marker for the api-server's
/// `negotiated_error::negotiate_error_middleware` to re-encode per `Accept`.
#[derive(Clone, Debug)]
pub struct ErrorStatus(pub crate::types::Status);

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::Json;

        // A compacted continue token during pagination carries a fresh
        // "inconsistent" continue token that must surface in the 410 response's
        // `metadata.continue` (upstream `handleCompactedErrorForPaging`).
        let mut continue_meta: Option<String> = None;

        // Extract resource name from error message for StatusDetails
        // A hand-built Status is written as it is, with `Retry-After` taken
        // from `details.retryAfterSeconds` (`responsewriters/writers.go:385`).
        if let Error::Status(status_obj) = self {
            let code = StatusCode::from_u16(status_obj.code.unwrap_or(500))
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let retry_after = status_obj
                .details
                .as_ref()
                .and_then(|d| d.retry_after_seconds)
                .filter(|s| *s > 0);
            let audit_obj = serde_json::to_value(&*status_obj).ok();
            let negotiable = ErrorStatus((*status_obj).clone());
            let mut response = (code, Json(*status_obj)).into_response();
            response.extensions_mut().insert(negotiable);
            if let Some(v) = audit_obj {
                response
                    .extensions_mut()
                    .insert(crate::audit::AuditResponseObject(v));
            }
            if let Some(seconds) = retry_after {
                if let Ok(value) = axum::http::HeaderValue::from_str(&seconds.to_string()) {
                    response
                        .headers_mut()
                        .insert(axum::http::header::RETRY_AFTER, value);
                }
            }
            return response;
        }

        let (status, message, reason, details) = match self {
            Error::Status(_) => unreachable!("handled above"),
            Error::NotFound(msg) => {
                let (message, details) = resource_error_status(&msg, "not found");
                (StatusCode::NOT_FOUND, message, "NotFound", details)
            }
            Error::AlreadyExists(msg) => {
                let (message, details) = resource_error_status(&msg, "already exists");
                (StatusCode::CONFLICT, message, "AlreadyExists", details)
            }
            Error::InvalidResource(msg) => {
                let details = extract_resource_details_for_invalid(&msg);
                (StatusCode::UNPROCESSABLE_ENTITY, msg, "Invalid", details)
            }
            Error::Invalid(errs) => {
                let msg = format_error_list(&errs);
                let details = build_invalid_details_from_errors(&errs);
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    msg,
                    "Invalid",
                    Some(details),
                )
            }
            // Mirrors upstream k8s apimachinery/pkg/api/errors/errors.go:
            // strict decoding errors and other syntactic / client-malformed
            // requests return HTTP 400 with reason=BadRequest (NOT 422/Invalid,
            // which is reserved for semantic field-validation errors).
            Error::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg, "BadRequest", None),
            Error::Authentication(msg) => (StatusCode::UNAUTHORIZED, msg, "Unauthorized", None),
            // An authorizer evaluation error (no Allow, no explicit Deny) is a
            // 500, endpoints/filters/authorization.go:87-91; denials are
            // `Error::Forbidden`.
            Error::Authorization(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "InternalError",
                None,
            ),
            Error::Forbidden(msg) => (StatusCode::FORBIDDEN, msg, "Forbidden", None),
            Error::Conflict(msg) => {
                let details = extract_resource_details(&msg);
                (StatusCode::CONFLICT, msg, "Conflict", details)
            }
            Error::TooManyRequests(msg) => {
                (StatusCode::TOO_MANY_REQUESTS, msg, "TooManyRequests", None)
            }
            Error::Gone(msg) => (StatusCode::GONE, msg, "Gone", None),
            Error::GoneWithContinue {
                message,
                continue_token,
            } => {
                continue_meta = Some(continue_token);
                (StatusCode::GONE, message, "Gone", None)
            }
            Error::Storage(msg) | Error::StorageTooLarge(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "InternalError",
                None,
            ),
            Error::Network(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                msg,
                "ServiceUnavailable",
                None,
            ),
            Error::Serialization(e) => (StatusCode::BAD_REQUEST, e.to_string(), "BadRequest", None),
            Error::UnsupportedMediaType(msg) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                msg,
                "UnsupportedMediaType",
                None,
            ),
            Error::NotAcceptable(msg) => (StatusCode::NOT_ACCEPTABLE, msg, "NotAcceptable", None),
            Error::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "InternalError",
                None,
            ),
        };

        let mut status_obj = if let Some(details) = details {
            crate::types::Status::failure_with_details(&message, reason, status.as_u16(), details)
        } else {
            crate::types::Status::failure(&message, reason, status.as_u16())
        };

        // Attach the fresh inconsistent continue token to `metadata.continue`
        // so a client can resume the list across the compaction boundary.
        if let Some(token) = continue_meta {
            status_obj.metadata = Some(crate::types::ListMeta {
                resource_version: None,
                continue_token: Some(token),
                remaining_item_count: None,
            });
        }

        // A 5xx is the server's own fault and never carries its cause anywhere
        // else: upstream logs it (`apiserver/pkg/endpoints/handlers/responsewriters`
        // routes 5xx through `runtime.HandleError`), we did not, so the only
        // record of e.g. a storage-layer LIST failure was the client's copy of
        // the message. `[sig-api-machinery] Garbage collector should not be
        // blocked by dependency circle` aborts its poll on the first LIST error
        // and then discards the message through an upstream `%w`-of-nil bug
        // (test/e2e/apimachinery/garbage_collector.go:868), leaving
        // `failed to list pods: %!w(<nil>)` and nothing to debug from (#1804).
        if status.is_server_error() {
            tracing::error!(
                status = status.as_u16(),
                reason = reason,
                "api-server returning a server error: {}",
                message
            );
        }

        let audit_obj = serde_json::to_value(&status_obj).ok();
        let negotiable = ErrorStatus(status_obj.clone());
        let mut response = (status, Json(status_obj)).into_response();
        response.extensions_mut().insert(negotiable);
        if let Some(v) = audit_obj {
            response
                .extensions_mut()
                .insert(crate::audit::AuditResponseObject(v));
        }
        response
    }
}

/// Recover the `(resource, name)` identity that upstream's error constructors
/// take as arguments, from whatever a caller happened to put in the error.
///
/// Upstream never builds these messages from a storage path — it is handed a
/// `schema.GroupResource` and a name (`NewNotFound(qualifiedResource, name)`),
/// so both the message and `Status.details` are derived from the same
/// structured identity. Rusternetes' storage layer raises `NotFound(key)` /
/// `AlreadyExists(key)` with the raw `/registry/...` key instead, so the
/// boundary has to parse it back.
///
/// Two accepted inputs:
/// - a storage key, `/registry/{resource}[/{namespace}]/{name}`;
/// - a message a handler already rendered, `{resource} "{name}" <suffix>`,
///   which is re-parsed rather than scraped so `details` stays consistent with
///   whichever path produced the error.
#[cfg(feature = "axum-support")]
fn resource_identity(msg: &str) -> Option<(String, String)> {
    if let Some(rest) = msg.strip_prefix("/registry/") {
        let parts: Vec<&str> = rest.split('/').filter(|p| !p.is_empty()).collect();
        // The first segment is the resource, the last is the object name; a
        // namespace may sit between them. Names never contain a slash, so the
        // last segment is the whole name — including dots, as in
        // `kube-root-ca.crt`.
        if parts.len() >= 2 {
            return Some((parts[0].to_string(), parts[parts.len() - 1].to_string()));
        }
        return None;
    }

    // `configmaps "kube-root-ca.crt" not found`
    let (resource, rest) = msg.split_once(" \"")?;
    let name = rest.split('"').next()?;
    if resource.is_empty() || name.is_empty() || resource.contains(' ') {
        return None;
    }
    Some((resource.to_string(), name.to_string()))
}

/// Build the `(message, details)` pair for a resource-scoped error, mirroring
/// `NewNotFound` / `NewAlreadyExists`: the message is
/// `{resource} "{name}" {suffix}` and `details` carries the resource as `kind`
/// and the bare object name as `name`.
///
/// Falls back to the caller's text when the identity cannot be recovered, so a
/// free-form message still produces a valid Status rather than a 500.
#[cfg(feature = "axum-support")]
fn resource_error_status(msg: &str, suffix: &str) -> (String, Option<crate::types::StatusDetails>) {
    match resource_identity(msg) {
        Some((resource, name)) => {
            let message = format!("{resource} \"{name}\" {suffix}");
            let details = crate::types::StatusDetails {
                name: Some(name),
                group: None,
                kind: Some(resource),
                uid: None,
                causes: None,
                retry_after_seconds: None,
            };
            (message, Some(details))
        }
        None => (msg.to_string(), extract_resource_details(msg)),
    }
}

/// Extract resource name from error messages and return StatusDetails.
///
/// Fallback for messages `resource_identity` cannot parse.
#[cfg(feature = "axum-support")]
fn extract_resource_details(msg: &str) -> Option<crate::types::StatusDetails> {
    let path = msg.split(": ").last()?;
    let name = if path.starts_with("/registry/") {
        path.rsplit('/').next().unwrap_or(path).to_string()
    } else {
        path.to_string()
    };

    if name.is_empty() {
        return None;
    }

    Some(crate::types::StatusDetails {
        name: Some(name),
        group: None,
        kind: None,
        uid: None,
        causes: None,
        retry_after_seconds: None,
    })
}

/// Build `Status.details` from a structured `field::ErrorList`, mirroring
/// upstream `apimachinery/pkg/api/errors/errors.go::NewInvalid`. Each
/// `field::Error` becomes one `StatusCause` whose `reason` is the upstream
/// `FieldValueXxx` mapping of the underlying `ErrorType`, `field` is the
/// real field path, and `message` is the full upstream-style error line
/// (`<field>: <ErrorType>: <detail>`).
#[cfg(feature = "axum-support")]
fn build_invalid_details_from_errors(errs: &ErrorList) -> crate::types::StatusDetails {
    let causes: Vec<crate::types::StatusCause> = errs
        .iter()
        .map(|e| crate::types::StatusCause {
            reason: Some(e.error_type.cause_reason().to_string()),
            message: Some(e.to_string()),
            field: Some(e.field.clone()),
        })
        .collect();
    crate::types::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: if causes.is_empty() {
            None
        } else {
            Some(causes)
        },
        retry_after_seconds: None,
    }
}

/// Best-effort parser for legacy `Error::InvalidResource(String)` messages
/// that follow upstream's `"<field>: <ErrorType>: <detail>"` shape (e.g.
/// `"spec.containers: Required value"`,
/// `"spec.containers[1].name: Duplicate value: \"ctr-a\""`,
/// `"spec.nodeName: Invalid value: \"node-2\": field is immutable"`).
///
/// Returns `None` if the message does not look like a field error — callers
/// fall back to a minimal hardcoded cause in that case.
#[cfg(feature = "axum-support")]
fn parse_legacy_field_error(msg: &str) -> Option<(crate::validation::field::ErrorType, String)> {
    use crate::validation::field::ErrorType;

    // Split into `<field>: <rest>` — `rest` carries the ErrorType label and
    // (optionally) the bad value and detail. We only need the type label here.
    let (field, rest) = msg.split_once(": ")?;
    if field.is_empty() || field.contains(' ') {
        return None;
    }
    // Each ErrorType has a canonical leading label (see `ErrorType::as_str`).
    // Match longest-prefix first so `"Invalid value"` is not matched as
    // `"Invalid"` (which is not actually one of upstream's labels).
    let labels: &[(&str, ErrorType)] = &[
        ("Required value", ErrorType::Required),
        ("Duplicate value", ErrorType::Duplicate),
        ("Unsupported value", ErrorType::NotSupported),
        ("Forbidden", ErrorType::Forbidden),
        ("Invalid value", ErrorType::Invalid),
        ("Not found", ErrorType::NotFound),
        ("Too long", ErrorType::TooLong),
        ("Too many", ErrorType::TooMany),
        ("Internal error", ErrorType::Internal),
    ];
    for (label, ty) in labels {
        if rest == *label || rest.starts_with(&format!("{label}:")) {
            return Some((*ty, field.to_string()));
        }
    }
    None
}

/// Extract resource details for Invalid errors, including causes.
#[cfg(feature = "axum-support")]
fn extract_resource_details_for_invalid(msg: &str) -> Option<crate::types::StatusDetails> {
    // Try to parse upstream-shaped legacy messages into a structured cause.
    // Falls back to the previous hardcoded shape only when parsing fails so
    // that older callers that emit free-form text don't suddenly 500.
    let (reason, field) = match parse_legacy_field_error(msg) {
        Some((ty, field)) => (ty.cause_reason().to_string(), field),
        None => ("FieldValueInvalid".to_string(), "metadata.name".to_string()),
    };
    Some(crate::types::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: Some(vec![crate::types::StatusCause {
            reason: Some(reason),
            message: Some(msg.to_string()),
            field: Some(field),
        }]),
        retry_after_seconds: None,
    })
}

#[cfg(all(test, feature = "axum-support"))]
mod tests {
    /// `NewGenericServerResponse(422, "", GroupResource{}, "", msg, 0, false)`
    /// (errors.go:476-478, :511-523): reason Invalid, canned message (the
    /// server message is dropped), empty details.
    #[test]
    fn generic_server_response_422_is_canned_with_empty_details() {
        let Error::Status(s) =
            Error::generic_server_response(422, "", "", "", "", "boom: not found", 0, false)
        else {
            panic!("expected Status");
        };
        assert_eq!(s.code, Some(422));
        assert_eq!(s.reason.as_deref(), Some("Invalid"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server rejected our request due to an error in our request")
        );
        assert_eq!(
            serde_json::to_value(s.details.as_ref().unwrap()).unwrap(),
            serde_json::json!({})
        );
    }

    /// errors.go:437-524: verb-dependent 409, the qualified-resource suffix
    /// (with and without a name), the >=500 wording, unexpected-response
    /// causes and retry-after.
    #[test]
    fn generic_server_response_follows_the_upstream_switch() {
        let st = |e: Error| match e {
            Error::Status(s) => *s,
            other => panic!("expected Status, got {other:?}"),
        };
        let s = st(Error::generic_server_response(
            409,
            "POST",
            "apps",
            "deployments",
            "d",
            "x",
            0,
            false,
        ));
        assert_eq!(s.reason.as_deref(), Some("AlreadyExists"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server reported a conflict (post deployments.apps d)")
        );
        let s = st(Error::generic_server_response(
            409, "GET", "", "pods", "", "x", 0, false,
        ));
        assert_eq!(s.reason.as_deref(), Some("Conflict"));
        assert_eq!(
            s.message.as_deref(),
            Some("the server reported a conflict (get pods)")
        );
        let s = st(Error::generic_server_response(
            500, "", "", "", "", "oops", 10, true,
        ));
        assert_eq!(s.reason.as_deref(), Some("InternalError"));
        assert_eq!(
            s.message.as_deref(),
            Some("an error on the server (\"oops\") has prevented the request from succeeding")
        );
        let d = s.details.unwrap();
        assert_eq!(d.retry_after_seconds, Some(10));
        let c = &d.causes.unwrap()[0];
        assert_eq!(c.reason.as_deref(), Some("UnexpectedServerResponse"));
        assert_eq!(c.message.as_deref(), Some("oops"));
        let s = st(Error::generic_server_response(
            418, "", "", "", "", "", 0, false,
        ));
        assert_eq!(s.reason.as_deref(), Some("Unknown"));
        assert_eq!(
            s.message.as_deref(),
            Some(
                "the server responded with the status code 418 but did not return more information"
            )
        );
        let s = st(Error::generic_server_response(
            406, "", "", "", "", "unknown", 0, false,
        ));
        assert_eq!(
            s.message.as_deref(),
            Some("the server was unable to respond with a content type that the client supports")
        );
    }

    // endpoints/filters/authorization.go:87-91: an authorizer error that did not
    // come with an Allow is `responsewriters.InternalError` (500), not Forbidden.
    #[test]
    fn authorizer_error_is_a_500_internal_error() {
        use axum::response::IntoResponse;
        let e = Error::Authorization("webhook failed".into());
        assert_eq!(e.reason(), "InternalError");
        assert_eq!(
            e.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn too_large_storage_error_is_recognised_and_is_a_500() {
        use axum::response::IntoResponse;
        let e = Error::StorageTooLarge("etcdserver: request is too large".into());
        assert!(e.is_too_large_error());
        assert!(!Error::Storage("etcdserver: request is too large".into()).is_too_large_error());
        assert_eq!(e.reason(), "InternalError");
        assert_eq!(
            e.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    use super::*;
    use crate::validation::field::{ErrorType, Path};

    /// `TestNewInvalid` (apimachinery pkg/api/errors/errors_test.go:124-220).
    #[test]
    fn new_invalid_matches_upstream_test_new_invalid() {
        let path = Path::new("field[0].name");
        let cases: Vec<(crate::validation::field::Error, &str, &str)> = vec![
            (
                crate::validation::field::Error::duplicate(&path, "bar"),
                "FieldValueDuplicate",
                r#"Kind "name" is invalid: field[0].name: Duplicate value: "bar""#,
            ),
            (
                crate::validation::field::Error::invalid(&path, "bar", "detail"),
                "FieldValueInvalid",
                r#"Kind "name" is invalid: field[0].name: Invalid value: "bar": detail"#,
            ),
            (
                crate::validation::field::Error::required(&path, ""),
                "FieldValueRequired",
                r#"Kind "name" is invalid: field[0].name: Required value"#,
            ),
        ];
        for (err, reason, msg) in cases {
            let body = err.error_body();
            let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![err]) else {
                panic!("not a Status")
            };
            assert_eq!(st.code, Some(422));
            assert_eq!(st.reason.as_deref(), Some("Invalid"));
            assert_eq!(st.message.as_deref(), Some(msg));
            let d = st.details.unwrap();
            assert_eq!(d.kind.as_deref(), Some("Kind"));
            assert_eq!(d.name.as_deref(), Some("name"));
            let c = &d.causes.unwrap()[0];
            assert_eq!(c.reason.as_deref(), Some(reason));
            assert_eq!(c.field.as_deref(), Some("field[0].name"));
            assert_eq!(c.message.as_deref(), Some(body.as_str()));
        }
    }

    /// `TestNewInvalid` cases beyond the three above (errors_test.go:124-220):
    /// NotFound, NotSupported, Required and the nil error list, plus
    /// `Cause.Type = CauseType(err.Type)` (errors.go:289), which makes an
    /// `Internal` error `InternalError` and a `TypeInvalid` one
    /// `FieldValueTypeInvalid` rather than collapsing them to
    /// `FieldValueInvalid`.
    #[test]
    fn new_invalid_matches_upstream_remaining_cases() {
        use crate::validation::field::{Error as FE, ErrorType};
        let p = Path::new("field[0].name");
        let cases: Vec<(FE, &str, &str)> = vec![
            (
                FE::not_found(&p, "bar"),
                "FieldValueNotFound",
                r#"Kind "name" is invalid: field[0].name: Not found: "bar""#,
            ),
            (
                FE::not_supported(&p, "bar", &[] as &[&str]),
                "FieldValueNotSupported",
                r#"Kind "name" is invalid: field[0].name: Unsupported value: "bar""#,
            ),
            (
                FE::internal(&p, "boom"),
                "InternalError",
                r#"Kind "name" is invalid: field[0].name: Internal error: boom"#,
            ),
        ];
        for (err, reason, msg) in cases {
            let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![err]) else {
                panic!("not a Status")
            };
            assert_eq!(st.message.as_deref(), Some(msg));
            let c = &st.details.unwrap().causes.unwrap()[0];
            assert_eq!(c.reason.as_deref(), Some(reason), "{msg}");
        }
        assert_eq!(
            ErrorType::TypeInvalid.cause_reason(),
            "FieldValueTypeInvalid"
        );
        assert_eq!(ErrorType::Internal.cause_reason(), "InternalError");

        // `IsInvalid(NewInvalid(kind("Test"), "2", nil))` (errors_test.go:72).
        let Error::Status(st) = Error::new_invalid("", "Test", "2", vec![]) else {
            panic!()
        };
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
        assert!(st.details.unwrap().causes.is_none());
    }

    /// `StatusDetails.Name`/`Kind` are `omitempty` (meta/v1 types.go), so an
    /// empty name or kind must not serialize (delete.go:81 passes name "").
    #[test]
    fn new_invalid_omits_empty_name_and_kind() {
        let e = crate::validation::field::Error::invalid(&Path::new("f"), "x", "d");
        let Error::Status(st) = Error::new_invalid("", "", "", vec![e]) else {
            panic!("expected Status");
        };
        let v = serde_json::to_value(st.details.unwrap()).unwrap();
        assert!(v.get("name").is_none(), "{v}");
        assert!(v.get("kind").is_none(), "{v}");
    }

    #[test]
    fn new_invalid_with_no_errors_and_with_several() {
        let Error::Status(st) = Error::new_invalid("", "Kind", "name", vec![]) else {
            panic!()
        };
        assert_eq!(st.message.as_deref(), Some(r#"Kind "name" is invalid"#));
        let p = Path::new("a");
        let e1 = crate::validation::field::Error::required(&p, "");
        let e2 = crate::validation::field::Error::required(&Path::new("b"), "");
        // Duplicate messages are dropped by `ToAggregate`; several render in
        // brackets, `, `-joined (util/errors/errors.go:70-94).
        let Error::Status(st) =
            Error::new_invalid("apps", "Deployment", "d", vec![e1.clone(), e1, e2])
        else {
            panic!()
        };
        assert_eq!(
            st.message.as_deref(),
            Some(r#"Deployment.apps "d" is invalid: [a: Required value, b: Required value]"#)
        );
        assert_eq!(st.details.unwrap().group.as_deref(), Some("apps"));
    }

    #[test]
    fn parse_legacy_required() {
        let (ty, field) = parse_legacy_field_error("spec.containers: Required value").unwrap();
        assert_eq!(ty, ErrorType::Required);
        assert_eq!(field, "spec.containers");
    }

    #[test]
    fn parse_legacy_duplicate_with_value() {
        let (ty, field) =
            parse_legacy_field_error("spec.containers[1].name: Duplicate value: \"ctr-a\"")
                .unwrap();
        assert_eq!(ty, ErrorType::Duplicate);
        assert_eq!(field, "spec.containers[1].name");
    }

    #[test]
    fn parse_legacy_forbidden() {
        let (ty, field) = parse_legacy_field_error(
            "spec.ephemeralContainers: Forbidden: cannot be set on create",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Forbidden);
        assert_eq!(field, "spec.ephemeralContainers");
    }

    #[test]
    fn parse_legacy_invalid_with_detail() {
        let (ty, field) = parse_legacy_field_error(
            "spec.nodeName: Invalid value: \"node-2\": field is immutable",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Invalid);
        assert_eq!(field, "spec.nodeName");
    }

    #[test]
    fn parse_legacy_unsupported() {
        let (ty, field) = parse_legacy_field_error(
            "spec.restartPolicy: Unsupported value: \"InvalidPolicy\": supported values: \"Always\", \"OnFailure\", \"Never\"",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::NotSupported);
        assert_eq!(field, "spec.restartPolicy");
    }

    #[test]
    fn build_details_from_error_list_one_cause_per_error() {
        let p = Path::new("spec");
        let errs = vec![
            crate::validation::field::Error::required(&p.child("containers"), ""),
            crate::validation::field::Error::not_supported(
                &p.child("restartPolicy"),
                "InvalidPolicy",
                &["Always", "OnFailure", "Never"],
            ),
        ];
        let details = build_invalid_details_from_errors(&errs);
        let causes = details.causes.unwrap();
        assert_eq!(causes.len(), 2);
        assert_eq!(causes[0].reason.as_deref(), Some("FieldValueRequired"));
        assert_eq!(causes[0].field.as_deref(), Some("spec.containers"));
        assert_eq!(causes[1].reason.as_deref(), Some("FieldValueNotSupported"));
        assert_eq!(causes[1].field.as_deref(), Some("spec.restartPolicy"));
    }
}
