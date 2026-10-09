//! ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding endpoints at
//! `admissionregistration.k8s.io/v1beta1` (#2847).
//!
//! Upstream's `v1beta1Storage`
//! (`pkg/registry/admissionregistration/rest/storage_apiserver.go:155-185`)
//! installs the same `NewREST` stores of `validatingadmissionpolicies`
//! (+`/status`) and `validatingadmissionpolicybindings` that v1 uses, so both
//! versions read and write one storage whose version is v1. The v1beta1 and v1
//! types are field-for-field identical
//! (`staging/src/k8s.io/api/admissionregistration/v1beta1/types.go`), so the
//! conversion (`pkg/apis/admissionregistration/v1beta1/zz_generated.conversion.go`)
//! is the `apiVersion` alone: a request body is stored as v1
//! ([`convert_policy_to_internal`]) and every response, list item and watch
//! event is encoded back as v1beta1 ([`to_v1beta1`]), as `EncoderForVersion`
//! does. Defaulting is shared too
//! (`pkg/apis/admissionregistration/v1beta1/defaults.go` equals the v1 one).
//!
//! Served only while `admissionregistration.k8s.io/v1beta1` is enabled; see
//! [`crate::handlers::mutating_admission_policy`] for the gate that stands in
//! for `--runtime-config`.

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::handlers::mutating_admission_policy::gate;
use crate::handlers::validating_admission_policy::{patch_content_type, store_crud_handlers};
use crate::registry::admissionregistration::{
    validatingadmissionpolicy, validatingadmissionpolicybinding,
};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    resources::{ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding},
    Error, List, Result,
};
use rusternetes_storage::{build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;

const GROUP: &str = "admissionregistration.k8s.io";
const VERSION: &str = "v1beta1";
const STORED_API_VERSION: &str = "admissionregistration.k8s.io/v1";
const SERVED_API_VERSION: &str = "admissionregistration.k8s.io/v1beta1";

/// A body decoded at v1beta1 is stored as v1 (the storage version), then
/// defaulted as v1 is.
fn convert_policy_to_internal(policy: &mut ValidatingAdmissionPolicy) {
    policy.api_version = STORED_API_VERSION.to_string();
    validatingadmissionpolicy::convert_to_internal(policy);
}

fn convert_binding_to_internal(binding: &mut ValidatingAdmissionPolicyBinding) {
    binding.api_version = STORED_API_VERSION.to_string();
    validatingadmissionpolicybinding::convert_to_internal(binding);
}

/// A stored object, list, or watch event in the v1beta1 encoding.
pub(crate) fn to_v1beta1(value: &mut serde_json::Value) {
    let restamp = |v: &mut serde_json::Value| {
        if v.get("apiVersion").and_then(|a| a.as_str()) == Some(STORED_API_VERSION) {
            v["apiVersion"] = serde_json::json!(SERVED_API_VERSION);
        }
    };
    restamp(value);
    if let Some(items) = value.get_mut("items").and_then(|i| i.as_array_mut()) {
        items.iter_mut().for_each(restamp);
    }
}

async fn convert_response(response: Response) -> Result<Response> {
    if !response.status().is_success() {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let out = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut value) => {
            to_v1beta1(&mut value);
            serde_json::to_vec(&value).unwrap_or_else(|_| bytes.to_vec())
        }
        Err(_) => bytes.to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    Ok(Response::from_parts(parts, Body::from(out)))
}

/// Every streamed object (initial `ADDED`s, `DELETED`, ...) as v1beta1.
fn watch_converter() -> crate::handlers::watch::WatchObjectConverter {
    Arc::new(|mut val: serde_json::Value| {
        Box::pin(async move {
            to_v1beta1(&mut val);
            val
        })
    })
}

// ===== ValidatingAdmissionPolicy =====

fn policy_scope(
    state: &ApiServerState,
    subresource: Option<&'static str>,
) -> RequestScope<ValidatingAdmissionPolicy> {
    let (store, status_store) =
        validatingadmissionpolicy::new_stores(state.storage.clone(), state.authorizer.clone());
    RequestScope {
        kind: GroupVersionKind {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            kind: "ValidatingAdmissionPolicy".to_string(),
        },
        resource: GroupVersionResource {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            resource: "validatingadmissionpolicies".to_string(),
        },
        subresource,
        store: Box::new(if subresource.is_some() {
            status_store
        } else {
            store
        }),
        apply: Some(crate::ssa::apply_legacy::<ValidatingAdmissionPolicy>),
        convert_to_internal: Some(convert_policy_to_internal),
        patch_conversion: None,
    }
}

fn policy_main_scope(state: &ApiServerState) -> RequestScope<ValidatingAdmissionPolicy> {
    policy_scope(state, None)
}

store_crud_handlers!(
    scope: policy_main_scope,
    create: create_validating_admission_policy,
    get: get_validating_admission_policy,
    update: update_validating_admission_policy,
    patch: patch_validating_admission_policy,
    delete: delete_validating_admission_policy,
    deletecollection: deletecollection_validatingadmissionpolicies,
    gate: gate,
    convert: convert_response,
);

pub async fn list_validating_admission_policies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped_converted::<ValidatingAdmissionPolicy>(
            state,
            auth_ctx,
            "validatingadmissionpolicies",
            GROUP,
            crate::handlers::watch::watch_params_from_query(&params),
            watch_converter(),
            Some((
                "ValidatingAdmissionPolicy".to_string(),
                SERVED_API_VERSION.to_string(),
            )),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicies")
        .with_api_group(GROUP);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("validatingadmissionpolicies", None);
    let mut items = state
        .storage
        .list::<ValidatingAdmissionPolicy>(&prefix)
        .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new("ValidatingAdmissionPolicyList", SERVED_API_VERSION, items);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    let mut value = serde_json::to_value(&list).map_err(|e| Error::Internal(e.to_string()))?;
    to_v1beta1(&mut value);
    Ok(Json(value).into_response())
}

/// GET `/status`.
pub async fn get_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    let out = endpoints::get_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
    )
    .await?;
    convert_response(out).await
}

/// PUT `/status`.
pub async fn update_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out = endpoints::update_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        &body,
    )
    .await?;
    convert_response(out).await
}

/// PATCH `/status`.
pub async fn patch_validating_admission_policy_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    gate()?;
    let out = endpoints::patch_resource(
        &state,
        &policy_scope(&state, Some("status")),
        &auth_ctx.user,
        None,
        &name,
        &params,
        patch_content_type(&headers),
        &body,
    )
    .await?;
    convert_response(out).await
}

// ===== ValidatingAdmissionPolicyBinding =====

fn binding_scope(state: &ApiServerState) -> RequestScope<ValidatingAdmissionPolicyBinding> {
    RequestScope {
        kind: GroupVersionKind {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            kind: "ValidatingAdmissionPolicyBinding".to_string(),
        },
        resource: GroupVersionResource {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            resource: "validatingadmissionpolicybindings".to_string(),
        },
        subresource: None,
        store: Box::new(validatingadmissionpolicybinding::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<ValidatingAdmissionPolicyBinding>),
        convert_to_internal: Some(convert_binding_to_internal),
        patch_conversion: None,
    }
}

store_crud_handlers!(
    scope: binding_scope,
    create: create_validating_admission_policy_binding,
    get: get_validating_admission_policy_binding,
    update: update_validating_admission_policy_binding,
    patch: patch_validating_admission_policy_binding,
    delete: delete_validating_admission_policy_binding,
    deletecollection: deletecollection_validatingadmissionpolicybindings,
    gate: gate,
    convert: convert_response,
);

pub async fn list_validating_admission_policy_bindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped_converted::<
            ValidatingAdmissionPolicyBinding,
        >(
            state,
            auth_ctx,
            "validatingadmissionpolicybindings",
            GROUP,
            crate::handlers::watch::watch_params_from_query(&params),
            watch_converter(),
            Some((
                "ValidatingAdmissionPolicyBinding".to_string(),
                SERVED_API_VERSION.to_string(),
            )),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicybindings")
        .with_api_group(GROUP);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("validatingadmissionpolicybindings", None);
    let mut items = state
        .storage
        .list::<ValidatingAdmissionPolicyBinding>(&prefix)
        .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "ValidatingAdmissionPolicyBindingList",
        SERVED_API_VERSION,
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    let mut value = serde_json::to_value(&list).map_err(|e| Error::Internal(e.to_string()))?;
    to_v1beta1(&mut value);
    Ok(Json(value).into_response())
}

/// Watch `validatingadmissionpolicies` at v1beta1 (404 unless the gate is on).
pub async fn watch_validatingadmissionpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<crate::handlers::watch::WatchParams>,
) -> Result<Response> {
    gate()?;
    crate::handlers::watch::watch_cluster_scoped_converted::<ValidatingAdmissionPolicy>(
        state,
        auth_ctx,
        "validatingadmissionpolicies",
        GROUP,
        params,
        watch_converter(),
        Some((
            "ValidatingAdmissionPolicy".to_string(),
            SERVED_API_VERSION.to_string(),
        )),
    )
    .await
}

/// Watch `validatingadmissionpolicybindings` at v1beta1.
pub async fn watch_validatingadmissionpolicybindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<crate::handlers::watch::WatchParams>,
) -> Result<Response> {
    gate()?;
    crate::handlers::watch::watch_cluster_scoped_converted::<ValidatingAdmissionPolicyBinding>(
        state,
        auth_ctx,
        "validatingadmissionpolicybindings",
        GROUP,
        params,
        watch_converter(),
        Some((
            "ValidatingAdmissionPolicyBinding".to_string(),
            SERVED_API_VERSION.to_string(),
        )),
    )
    .await
}
