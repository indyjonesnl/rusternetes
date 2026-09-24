//! Port of `endpoints/handlers/rest.go`: the per-resource `RequestScope` and
//! the helpers every verb handler shares.

use std::collections::HashMap;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Decision, RequestAttributes};
use rusternetes_common::dump::decode_request_body;
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;
use serde::Serialize;

use crate::registry::generic::Store;
use crate::registry::rest::{Object, RequestContext};
use crate::ssa::{ApplyError, ApplyOptions, ApplyOutcome};
use crate::state::ApiServerState;

/// A resource's server-side apply: merge an apply configuration into the
/// live object (or into nothing, on create). Upstream's is the one
/// schema-driven `FieldManager.Apply`; ours is per resource until the
/// structured-merge port covers every type.
pub type ApplyFn<T> = fn(
    Option<&T>,
    &serde_json::Value,
    &ApplyOptions,
) -> std::result::Result<ApplyOutcome<T>, ApplyError>;

/// `RequestScope` (rest.go:63-101), reduced to what the handlers consult.
pub struct RequestScope<T: Object> {
    /// `Kind`: the GroupVersionKind served, and handed to admission.
    pub kind: GroupVersionKind,
    /// `Resource`: the GroupVersionResource served.
    pub resource: GroupVersionResource,
    /// The `rest.Storage` behind the endpoints.
    pub store: Store<T, StorageBackend>,
    /// Server-side apply, when the resource supports it.
    pub apply: Option<ApplyFn<T>>,
}

impl<T: Object> RequestScope<T> {
    /// The `apiVersion` a request body must carry, if it carries one.
    pub fn api_version(&self) -> String {
        if self.kind.group.is_empty() {
            self.kind.version.clone()
        } else {
            format!("{}/{}", self.kind.group, self.kind.version)
        }
    }

    pub(super) fn namespace_scoped(&self) -> bool {
        self.store.create_strategy.namespace_scoped()
    }
}

/// The `WithAuthorization` filter's check for a resource request
/// (endpoints/filters/authorization.go).
pub(super) async fn authorize(
    state: &ApiServerState,
    user: &UserInfo,
    verb: &str,
    resource: &GroupVersionResource,
    namespace: Option<&str>,
    name: Option<&str>,
) -> Result<()> {
    let mut attrs = RequestAttributes::new(user.clone(), verb, resource.resource.clone())
        .with_api_group(resource.group.clone());
    if let Some(ns) = namespace {
        attrs = attrs.with_namespace(ns);
    }
    if let Some(name) = name {
        attrs = attrs.with_name(name);
    }
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => Ok(()),
        Decision::Deny(reason) => Err(Error::Forbidden(reason)),
    }
}

/// The `dryRun` query parameter as `metav1.*Options.DryRun` decodes it.
pub(super) fn dry_run_param(params: &HashMap<String, String>) -> Option<Vec<String>> {
    params.get("dryRun").map(|v| vec![v.clone()])
}

/// `dryrun.IsDryRun` (apiserver/pkg/util/dryrun/dryrun.go): any value is a
/// dry run. The options validation that runs first admits only `All`.
pub(super) fn is_dry_run(dry_run: Option<&[String]>) -> bool {
    dry_run.is_some_and(|d| !d.is_empty())
}

/// Decode a create/update body the way `createHandler` and `UpdateResource`
/// do (create.go:116-148, update.go:104-136): into the typed object, with
/// unknown and duplicate fields rejected, warned about or ignored per
/// `fieldValidation` (`validate_strict_fields` implements that directive,
/// defaulting to Warn as `fieldValidation("")` does, rest.go:409-413), and
/// with a body naming another API version rejected.
///
/// Returns the object and the strict-decoding warnings.
pub(super) fn decode<T: Object>(
    scope: &RequestScope<T>,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<(T, Vec<String>)> {
    // A generic JSON parse keeps the last of a duplicated key, where a typed
    // parse refuses the document. Upstream's strict decoder reports the
    // duplicate as a strict error, which Warn only warns about, so parse
    // generically first and let `validate_strict_fields` judge duplicates
    // from the raw bytes.
    let value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return Err(decode_request_body::<T>(body)
                .err()
                .unwrap_or_else(|| Error::BadRequest(e.to_string())))
        }
    };

    if let Some(api_version) = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
    {
        let expected = scope.api_version();
        if api_version != expected {
            return Err(Error::BadRequest(format!(
                "the API version in the data ({api_version}) does not match the expected API version ({expected})"
            )));
        }
    }

    let obj: T = match serde_json::from_value(value.clone()) {
        Ok(obj) => obj,
        // The typed decode of the original bytes fails the same way, and its
        // error quotes the body as the client sent it.
        Err(e) => {
            return Err(decode_request_body::<T>(body)
                .err()
                .unwrap_or_else(|| Error::BadRequest(e.to_string())));
        }
    };

    let warnings = crate::handlers::validation::validate_strict_fields(params, body, &obj)?;
    Ok((obj, warnings))
}

/// `checkName` (rest.go:272-290).
pub(super) fn check_name<T: Object>(obj: &T, name: &str, namespace: Option<&str>) -> Result<()> {
    let meta = obj.metadata();
    if meta.name.is_empty() {
        // `ObjectName` refuses an empty name with `errEmptyName`
        // (namer.go:74-85).
        return Err(Error::BadRequest(format!(
            "the name of the object ({name} based on URL) was undeterminable: name must be provided"
        )));
    }
    if meta.name != name {
        return Err(Error::BadRequest(format!(
            "the name of the object ({}) does not match the name on the URL ({name})",
            meta.name
        )));
    }
    if let Some(namespace) = namespace.filter(|ns| !ns.is_empty()) {
        let obj_namespace = meta.namespace.as_deref().unwrap_or("");
        if !obj_namespace.is_empty() && obj_namespace != namespace {
            return Err(Error::BadRequest(format!(
                "the namespace of the object ({obj_namespace}) does not match the namespace on the request ({namespace})"
            )));
        }
    }
    Ok(())
}

/// `transformResponseObject` for a JSON client, with the request's warnings
/// as `Warning: 299` headers (endpoints/filters/warning.go).
pub(super) fn respond<B: Serialize>(
    status: StatusCode,
    body: &B,
    ctx: &RequestContext,
) -> Response {
    let mut headers = HeaderMap::new();
    for warning in ctx.warnings() {
        let value = crate::handlers::validation::format_warning_header(&warning);
        if let Ok(v) = HeaderValue::from_str(&value) {
            headers.append(header::WARNING, v);
        }
    }
    (status, headers, Json(body)).into_response()
}
