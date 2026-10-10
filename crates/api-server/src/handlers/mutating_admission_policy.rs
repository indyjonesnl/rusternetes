//! MutatingAdmissionPolicy and MutatingAdmissionPolicyBinding endpoints
//! (`admissionregistration.k8s.io/v1beta1`).
//!
//! Writes go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the strategies of
//! [`crate::registry::admissionregistration`], as the validating siblings do.
//!
//! Served only while the `MutatingAdmissionPolicy` gate is on: upstream
//! registers the storage only when `admissionregistration.k8s.io/v1beta1` is
//! enabled (`pkg/registry/admissionregistration/rest/storage_apiserver.go:187-
//! 205`), a group-version that is in `betaAPIGroupVersionsDisabledByDefault`
//! (`pkg/controlplane/instance.go`). Rusternetes has no `--runtime-config`, so
//! the gate stands in for it, as `ClusterTrustBundle` does for
//! `certificates.k8s.io/v1beta1`; with it off every verb is a 404.

use crate::endpoints::handlers::RequestScope;
use crate::handlers::validating_admission_policy::store_crud_handlers;
use crate::registry::admissionregistration::{
    mutatingadmissionpolicy, mutatingadmissionpolicybinding,
};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::{
    admission::{GroupVersionKind, GroupVersionResource},
    authz::{Decision, RequestAttributes},
    feature_gates::{enabled, Feature},
    resources::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding},
    Error, List, Result,
};
use rusternetes_storage::build_prefix;
use std::sync::Arc;

/// The 404 of a resource whose storage was not installed because the
/// `MutatingAdmissionPolicy` gate is off.
pub fn gate() -> Result<()> {
    if enabled(Feature::MutatingAdmissionPolicy) {
        Ok(())
    } else {
        Err(Error::NotFound(
            "the server could not find the requested resource".to_string(),
        ))
    }
}

const GROUP: &str = "admissionregistration.k8s.io";
const VERSION: &str = "v1beta1";

// ===== MutatingAdmissionPolicy =====

fn policy_scope(state: &ApiServerState) -> RequestScope<MutatingAdmissionPolicy> {
    RequestScope {
        kind: GroupVersionKind {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            kind: "MutatingAdmissionPolicy".to_string(),
        },
        resource: GroupVersionResource {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            resource: "mutatingadmissionpolicies".to_string(),
        },
        subresource: None,
        store: Box::new(mutatingadmissionpolicy::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<MutatingAdmissionPolicy>),
        convert_to_internal: Some(mutatingadmissionpolicy::convert_to_internal),
        patch_conversion: None,
    }
}

store_crud_handlers!(
    scope: policy_scope,
    create: create_mutating_admission_policy,
    get: get_mutating_admission_policy,
    update: update_mutating_admission_policy,
    patch: patch_mutating_admission_policy,
    delete: delete_mutating_admission_policy,
    deletecollection: deletecollection_mutatingadmissionpolicies,
    gate: gate,
);

pub async fn list_mutating_admission_policies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<MutatingAdmissionPolicy>(
            state,
            auth_ctx,
            "mutatingadmissionpolicies",
            GROUP,
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "mutatingadmissionpolicies")
        .with_api_group(GROUP);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("mutatingadmissionpolicies", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;
    let mut items = crate::handlers::list_options::list_items::<MutatingAdmissionPolicy, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "MutatingAdmissionPolicyList",
        "admissionregistration.k8s.io/v1beta1",
        items,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(Json(list).into_response())
}

// ===== MutatingAdmissionPolicyBinding =====

fn binding_scope(state: &ApiServerState) -> RequestScope<MutatingAdmissionPolicyBinding> {
    RequestScope {
        kind: GroupVersionKind {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            kind: "MutatingAdmissionPolicyBinding".to_string(),
        },
        resource: GroupVersionResource {
            group: GROUP.to_string(),
            version: VERSION.to_string(),
            resource: "mutatingadmissionpolicybindings".to_string(),
        },
        subresource: None,
        store: Box::new(mutatingadmissionpolicybinding::new_store(
            state.storage.clone(),
            state.authorizer.clone(),
        )),
        apply: Some(crate::ssa::apply_legacy::<MutatingAdmissionPolicyBinding>),
        convert_to_internal: Some(mutatingadmissionpolicybinding::convert_to_internal),
        patch_conversion: None,
    }
}

store_crud_handlers!(
    scope: binding_scope,
    create: create_mutating_admission_policy_binding,
    get: get_mutating_admission_policy_binding,
    update: update_mutating_admission_policy_binding,
    patch: patch_mutating_admission_policy_binding,
    delete: delete_mutating_admission_policy_binding,
    deletecollection: deletecollection_mutatingadmissionpolicybindings,
    gate: gate,
);

pub async fn list_mutating_admission_policy_bindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    gate()?;
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<MutatingAdmissionPolicyBinding>(
            state,
            auth_ctx,
            "mutatingadmissionpolicybindings",
            GROUP,
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "mutatingadmissionpolicybindings")
        .with_api_group(GROUP);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let prefix = build_prefix("mutatingadmissionpolicybindings", None);
    // ValidateListOptions + the resourceVersion floor (#2683).
    crate::handlers::list_options::prepare_list(&*state.storage, &params).await?;
    let mut items = crate::handlers::list_options::list_items::<MutatingAdmissionPolicyBinding, _>(
        &*state.storage,
        &prefix,
        &params,
    )
    .await?;
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "MutatingAdmissionPolicyBindingList",
        "admissionregistration.k8s.io/v1beta1",
        items,
    );
    list.metadata.resource_version = Some(
        crate::handlers::list_options::list_resource_version(&state.storage, &params, &list.items)
            .await,
    );
    Ok(Json(list).into_response())
}
