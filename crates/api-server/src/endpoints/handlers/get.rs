//! Port of `endpoints/handlers/get.go` (`GetResource`, :86-113).

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::{Error, Result};

use super::rest::{authorize, respond_object, RequestScope};
use crate::registry::generic::GetOptions;
use crate::registry::rest::{Object, RequestContext};
use crate::state::ApiServerState;

/// The `metav1.GetOptions` a GET's query carries, as `GetResource` decodes
/// them (get.go:90-110): `export` is refused (:93-104), and
/// `resourceVersion` reaches `Store.Get` as `GetOptions.ResourceVersion`.
pub fn decode_get_options(params: &HashMap<String, String>) -> Result<GetOptions> {
    if let Some(export) = params.get("export") {
        // `runtime.Convert_Slice_string_To_bool`
        // (apimachinery/pkg/runtime/conversion.go:79-95): only "0" and
        // "false" (any case) are false; any other value, "" included, is true.
        let value = !(export == "0" || export.eq_ignore_ascii_case("false"));
        if value {
            return Err(Error::BadRequest(
                "the export parameter, deprecated since v1.14, is no longer supported".to_string(),
            ));
        }
    }
    Ok(GetOptions {
        resource_version: params.get("resourceVersion").cloned().unwrap_or_default(),
    })
}

/// GET of a named object.
pub async fn get_resource<T: Object>(
    state: &ApiServerState,
    scope: &RequestScope<T>,
    user: &UserInfo,
    namespace: Option<&str>,
    name: &str,
    params: &HashMap<String, String>,
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
    let options = decode_get_options(params)?;
    let ctx =
        RequestContext::new(namespace).with_group_version(&scope.kind.group, &scope.kind.version);
    let obj = scope.store.get(&ctx, name, &options).await?;
    Ok(respond_object(scope, StatusCode::OK, &obj, &ctx))
}
