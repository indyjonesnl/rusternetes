//! Port of `endpoints/handlers/get.go` (`GetResource`, :86-113).
//!
//! `GetOptions.resourceVersion` is not honoured: the Store reads the latest
//! object, as every rusternetes GET does today.

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::Result;

use super::rest::{authorize, respond, RequestScope};
use crate::registry::rest::{Object, RequestContext};
use crate::state::ApiServerState;

/// GET of a named object.
pub async fn get_resource<T: Object>(
    state: &ApiServerState,
    scope: &RequestScope<T>,
    user: &UserInfo,
    namespace: Option<&str>,
    name: &str,
) -> Result<Response> {
    authorize(
        state,
        user,
        "get",
        &scope.resource,
        scope.subresource,
        namespace,
        Some(name),
    )
    .await?;
    let ctx = RequestContext::new(namespace);
    let obj = scope.store.get(&ctx, name).await?;
    Ok(respond(StatusCode::OK, &obj, &ctx))
}
