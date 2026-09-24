//! The admission chain as the handlers and the Store see it.
//!
//! Upstream's chain is ordered by `pkg/kubeapiserver/options/plugins.go:106-110`:
//! MutatingAdmissionPolicy, MutatingAdmissionWebhook, ValidatingAdmissionPolicy,
//! ValidatingAdmissionWebhook, ResourceQuota. The mutating half runs in the
//! handler (create.go:202-206) or as a `TransformFunc` inside `Store.Update`
//! (update.go:170-189, patch.go:631-651); the validating half is handed to the
//! Store as `rest.AdmissionToValidateObjectFunc` /
//! `AdmissionToValidateObjectUpdateFunc` / `AdmissionToValidateObjectDeleteFunc`
//! (registry/rest/rest.go) and runs on the fully formed object.
//!
//! Of those plugins Rusternetes implements the two webhook plugins and
//! ValidatingAdmissionPolicy. MutatingAdmissionPolicy is not implemented, and
//! ResourceQuota admission is not wired to the resources on this path.

use async_trait::async_trait;
use rusternetes_common::admission::{
    self, AdmissionResponse, GroupVersionKind, GroupVersionResource, Operation,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::{Error, Result};

use super::rest::authorize;
use crate::registry::rest::{
    Object, RequestContext, TransformFunc, ValidateObject, ValidateObjectUpdate,
};
use crate::state::ApiServerState;

/// One request's view of the admission chain: upstream's `admission.Interface`
/// plus the static parts of `admission.NewAttributesRecord`.
pub struct Admission<'a> {
    pub state: &'a ApiServerState,
    pub kind: &'a GroupVersionKind,
    pub resource: &'a GroupVersionResource,
    pub namespace: Option<&'a str>,
    pub user: &'a UserInfo,
    pub dry_run: bool,
}

fn denied(reason: &str) -> Error {
    Error::Forbidden(format!("admission webhook denied the request: {reason}"))
}

fn to_value<T: Object>(obj: &T) -> Result<serde_json::Value> {
    serde_json::to_value(obj).map_err(|e| Error::Internal(e.to_string()))
}

impl Admission<'_> {
    fn user_info(&self) -> admission::UserInfo {
        admission::UserInfo {
            username: self.user.username.clone(),
            uid: self.user.uid.clone(),
            groups: self.user.groups.clone(),
        }
    }

    /// The mutating plugins: `MutationInterface.Admit`.
    pub async fn admit<T: Object>(&self, op: Operation, obj: T, old: Option<&T>) -> Result<T> {
        let name = obj.metadata().name.clone();
        let (response, mutated) = self
            .state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &op,
                self.kind,
                self.resource,
                self.namespace,
                &name,
                Some(to_value(&obj)?),
                old.map(to_value).transpose()?,
                &self.user_info(),
                self.dry_run,
            )
            .await?;
        if let AdmissionResponse::Deny(reason) = &response {
            return Err(denied(reason));
        }
        match mutated {
            Some(value) => serde_json::from_value(value).map_err(|e| {
                Error::Internal(format!(
                    "failed to decode the object mutated by admission: {e}"
                ))
            }),
            None => Ok(obj),
        }
    }

    /// The validating plugins: `ValidationInterface.Validate`. `obj` is `None`
    /// on DELETE, `old` is `None` on CREATE.
    pub async fn validate<T: Object>(
        &self,
        op: Operation,
        obj: Option<&T>,
        old: Option<&T>,
    ) -> Result<()> {
        let name = obj
            .or(old)
            .map(|o| o.metadata().name.clone())
            .unwrap_or_default();
        let obj = obj.map(to_value).transpose()?;
        let old = old.map(to_value).transpose()?;

        self.state
            .webhook_manager
            .run_validating_admission_policies_ext(
                &op,
                self.kind,
                obj.as_ref(),
                old.as_ref(),
                Some(&self.resource.resource),
                self.namespace,
            )
            .await?;

        if let AdmissionResponse::Deny(reason) = self
            .state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &op,
                self.kind,
                self.resource,
                self.namespace,
                &name,
                obj,
                old,
                &self.user_info(),
                self.dry_run,
            )
            .await?
        {
            return Err(denied(&reason));
        }
        Ok(())
    }
}

/// `rest.AdmissionToValidateObjectFunc` for CREATE. With `authorize_create`
/// it is also `withAuthorization` (update.go:258-283): an update or patch
/// that turns into a create must be allowed to `create`.
pub struct CreateValidation<'a> {
    pub admission: &'a Admission<'a>,
    pub authorize_create: bool,
}

#[async_trait]
impl<T: Object> ValidateObject<T> for CreateValidation<'_> {
    async fn validate(&self, _ctx: &RequestContext, obj: &T) -> Result<()> {
        if self.authorize_create {
            let a = self.admission;
            authorize(
                a.state,
                a.user,
                "create",
                a.resource,
                a.namespace,
                Some(&obj.metadata().name),
            )
            .await?;
        }
        self.admission
            .validate(Operation::Create, Some(obj), None)
            .await
    }
}

/// `rest.AdmissionToValidateObjectUpdateFunc`.
pub struct UpdateValidation<'a> {
    pub admission: &'a Admission<'a>,
}

#[async_trait]
impl<T: Object> ValidateObjectUpdate<T> for UpdateValidation<'_> {
    async fn validate(&self, _ctx: &RequestContext, obj: &T, old: &T) -> Result<()> {
        self.admission
            .validate(Operation::Update, Some(obj), Some(old))
            .await
    }
}

/// `rest.AdmissionToValidateObjectDeleteFunc`: the object is nil and the
/// stored object is the old one.
pub struct DeleteValidation<'a> {
    pub admission: &'a Admission<'a>,
}

#[async_trait]
impl<T: Object> ValidateObject<T> for DeleteValidation<'_> {
    async fn validate(&self, _ctx: &RequestContext, obj: &T) -> Result<()> {
        self.admission
            .validate::<T>(Operation::Delete, None, Some(obj))
            .await
    }
}

/// The mutating-admission `TransformFunc` of `UpdateResource`
/// (update.go:170-189) and the patcher's `applyAdmission` (patch.go:631-651):
/// CREATE when there is no live object (`hasUID(old)` is false), UPDATE
/// otherwise.
pub struct MutatingAdmission<'a> {
    pub admission: &'a Admission<'a>,
}

#[async_trait]
impl<T: Object> TransformFunc<T> for MutatingAdmission<'_> {
    async fn transform(&self, _ctx: &RequestContext, new: Option<T>, old: Option<&T>) -> Result<T> {
        let new = new.ok_or_else(|| {
            Error::Internal("mutating admission ran before an object was built".to_string())
        })?;
        match old.filter(|o| !o.metadata().uid.is_empty()) {
            None => self.admission.admit(Operation::Create, new, None).await,
            Some(old) => {
                self.admission
                    .admit(Operation::Update, new, Some(old))
                    .await
            }
        }
    }
}
