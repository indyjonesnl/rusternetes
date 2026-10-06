//! CertificateSigningRequest strategy and storage — port of
//! `pkg/registry/certificates/certificates/strategy.go` and
//! `pkg/registry/certificates/certificates/storage/storage.go`.
//!
//! Not modelled: declarative validation
//! (`ValidateDeclarativelyWithMigrationChecks`). The `approve`/`sign` checks
//! on the signer are admission plugins, in
//! [`crate::admission::certificates`].

use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use rusternetes_common::resources::certificates::CertificateSigningRequestCondition;
use rusternetes_common::resources::{CertificateSigningRequest, CertificateSigningRequestStatus};
use rusternetes_common::validation::certificatesigningrequest::{
    validate_certificate_signing_request_approval_update,
    validate_certificate_signing_request_create,
    validate_certificate_signing_request_status_update,
    validate_certificate_signing_request_update_main,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_unconstrained, validate_object_meta, validate_object_meta_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

const APPROVED: &str = "Approved";
const DENIED: &str = "Denied";

/// `csrStrategy` (strategy.go:40-135).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<CertificateSigningRequest> for Strategy {
    /// Clears the user info the client set and injects the requester's from
    /// the request context; a CSR cannot be created pre-approved
    /// (strategy.go:75-100).
    fn prepare_for_create(&self, ctx: &RequestContext, obj: &mut CertificateSigningRequest) {
        let spec = &mut obj.spec;
        spec.username = None;
        spec.uid = None;
        spec.groups = None;
        spec.extra = None;
        if let Some(user) = &ctx.user {
            spec.username = Some(user.username.clone());
            spec.uid = Some(user.uid.clone());
            spec.groups = Some(user.groups.clone());
            if !user.extra.is_empty() {
                spec.extra = Some(user.extra.clone());
            }
        }
        obj.status = Some(CertificateSigningRequestStatus {
            conditions: Some(Vec::new()),
            certificate: None,
        });
    }

    /// `ValidateCertificateSigningRequestCreate`, preceded by its
    /// `ValidateObjectMeta`.
    fn validate(&self, _ctx: &RequestContext, obj: &CertificateSigningRequest) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_unconstrained,
            &Path::new("metadata"),
        );
        errs.extend(validate_certificate_signing_request_create(obj));
        errs
    }
}

impl RestUpdateStrategy<CertificateSigningRequest> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Certificate requests are immutable after creation except via
    /// subresources (strategy.go:102-110).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) {
        obj.spec = old.spec.clone();
        obj.status = old.status.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) -> ErrorList {
        validate_with_meta(obj, old, validate_certificate_signing_request_update_main)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<CertificateSigningRequest> for Strategy {}

/// `validateCertificateSigningRequestUpdate` runs `ValidateObjectMetaUpdate`
/// next to the object checks (validation.go:297-299).
fn validate_with_meta(
    obj: &CertificateSigningRequest,
    old: &CertificateSigningRequest,
    validate: fn(&CertificateSigningRequest, &CertificateSigningRequest) -> ErrorList,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate(obj, old));
    errs
}

fn condition_indices(csr: &CertificateSigningRequest, type_: &str) -> Vec<usize> {
    csr.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|c| {
            c.iter()
                .enumerate()
                .filter(|(_, c)| c.type_ == type_)
                .map(|(i, _)| i)
                .collect()
        })
        .unwrap_or_default()
}

/// `preserveConditionInstances` (strategy.go:163-177): copy the instances of
/// `type_` from the old CSR, unless the update added or removed some.
fn preserve_condition_instances(
    new: &mut CertificateSigningRequest,
    old: &CertificateSigningRequest,
    type_: &str,
) {
    let old_indices = condition_indices(old, type_);
    let new_indices = condition_indices(new, type_);
    if old_indices.len() != new_indices.len() {
        return;
    }
    let (Some(old_conditions), Some(new_conditions)) = (
        old.status.as_ref().and_then(|s| s.conditions.as_ref()),
        new.status.as_mut().and_then(|s| s.conditions.as_mut()),
    ) else {
        return;
    };
    for (o, n) in old_indices.into_iter().zip(new_indices) {
        new_conditions[n] = old_conditions[o].clone();
    }
}

fn is_blank(time: &Option<String>) -> bool {
    time.as_deref().is_none_or(str::is_empty)
}

/// `populateConditionTimestamps` (strategy.go:195-217): fill a missing
/// `lastUpdateTime`, and a missing `lastTransitionTime` from the old
/// condition with the same type and status, else now.
fn populate_condition_timestamps(
    new: &mut CertificateSigningRequest,
    old: &CertificateSigningRequest,
) {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let old_conditions: &[CertificateSigningRequestCondition] = old
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or(&[]);
    let Some(conditions) = new.status.as_mut().and_then(|s| s.conditions.as_mut()) else {
        return;
    };
    for condition in conditions {
        if is_blank(&condition.last_update_time) {
            condition.last_update_time = Some(now.clone());
        }
        if is_blank(&condition.last_transition_time) {
            let previous = old_conditions
                .iter()
                .find(|o| {
                    o.type_ == condition.type_
                        && o.status == condition.status
                        && !is_blank(&o.last_transition_time)
                })
                .and_then(|o| o.last_transition_time.clone());
            condition.last_transition_time = Some(previous.unwrap_or_else(|| now.clone()));
        }
    }
}

/// `csrStatusStrategy` (strategy.go:137-235).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<CertificateSigningRequest> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `/status` must not modify the spec, and keeps the existing
    /// Approved/Denied conditions (adding or removing one is a validation
    /// error instead).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) {
        obj.spec = old.spec.clone();
        preserve_condition_instances(obj, old, APPROVED);
        preserve_condition_instances(obj, old, DENIED);
        populate_condition_timestamps(obj, old);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) -> ErrorList {
        validate_with_meta(obj, old, validate_certificate_signing_request_status_update)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `csrApprovalStrategy` (strategy.go:276-310).
pub struct ApprovalStrategy;

impl NamespaceScopedStrategy for ApprovalStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<CertificateSigningRequest> for ApprovalStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Only the conditions may change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) {
        populate_condition_timestamps(obj, old);
        let new_conditions = obj.status.as_ref().and_then(|s| s.conditions.clone());
        obj.spec = old.spec.clone();
        let mut status = old
            .status
            .clone()
            .unwrap_or(CertificateSigningRequestStatus {
                conditions: None,
                certificate: None,
            });
        status.conditions = new_conditions;
        obj.status = Some(status);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &CertificateSigningRequest,
        old: &CertificateSigningRequest,
    ) -> ErrorList {
        validate_with_meta(
            obj,
            old,
            validate_certificate_signing_request_approval_update,
        )
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:41-72): the main, status and approval stores.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<CertificateSigningRequest, StorageBackend>,
    Store<CertificateSigningRequest, StorageBackend>,
    Store<CertificateSigningRequest, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("certificates.k8s.io", "certificatesigningrequests"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    let approval = store.with_update_strategy(Arc::new(ApprovalStrategy));
    (store, status, approval)
}
