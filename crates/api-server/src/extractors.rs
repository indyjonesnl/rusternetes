//! Request extractors that produce Kubernetes-shaped error responses.
//!
//! Axum's stock [`axum::Json`] extractor, when it fails to deserialize a
//! request body, returns a plain-text `application/json`-ish rejection with no
//! `metav1.Status` envelope. client-go cannot parse that and surfaces the
//! generic "the server rejected our request due to an error in our request"
//! message (see `k8s.io/apimachinery/pkg/api/errors`). Wrapping the rejection
//! in our [`rusternetes_common::Error`] makes every body-decode failure come
//! back as a proper `Status{ kind: "Status", status: "Failure", ... }`.

use axum::{
    extract::{rejection::JsonRejection, FromRequest, Request},
    Json as AxumJson,
};
use rusternetes_common::Error;
use serde::de::DeserializeOwned;

/// Drop-in replacement for [`axum::Json`] whose rejection is a
/// `metav1.Status` body instead of axum's default plain-text response.
///
/// Mirrors upstream apiserver behaviour: a body that fails to decode is a
/// client error (HTTP 400, reason `BadRequest`), not a 422/Invalid.
pub struct Json<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for Json<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Error;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match AxumJson::<T>::from_request(req, state).await {
            Ok(AxumJson(value)) => Ok(Json(value)),
            Err(rejection) => Err(json_rejection_to_error(rejection)),
        }
    }
}

/// Map an axum [`JsonRejection`] to the appropriate Kubernetes error.
fn json_rejection_to_error(rejection: JsonRejection) -> Error {
    match rejection {
        // Wrong / missing Content-Type → 415, matching the stock extractor's
        // status code but with a Status body.
        JsonRejection::MissingJsonContentType(e) => Error::UnsupportedMediaType(e.body_text()),
        // Syntax / type / data errors are all client-malformed bodies → 400.
        other => Error::BadRequest(other.body_text()),
    }
}
