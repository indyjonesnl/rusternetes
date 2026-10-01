//! ServiceAccount strategy and storage — port of
//! `pkg/registry/core/serviceaccount/strategy.go` and
//! `pkg/registry/core/serviceaccount/storage/storage.go`.
//!
//! The `/token` subresource (`TokenREST`) is served by its own handler.

use std::sync::Arc;

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::ServiceAccount;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `sa.EnforceMountableSecretsAnnotation`
/// (staging/src/k8s.io/api/core/v1/types.go:6292).
const ENFORCE_MOUNTABLE_SECRETS_ANNOTATION: &str = "kubernetes.io/enforce-mountable-secrets";

/// `cleanSecretReferences` (strategy.go:67-71): a reference keeps only its name.
fn clean_secret_references(sa: &mut ServiceAccount) {
    for secret in sa.secrets.iter_mut().flatten() {
        *secret = ObjectReference {
            name: secret.name.take(),
            ..Default::default()
        };
    }
}

/// `ValidateServiceAccount` (validation.go:7642-7645): ObjectMeta with
/// `ValidateServiceAccountName` (`NameIsDNSSubdomain`).
fn validate(sa: &ServiceAccount) -> ErrorList {
    validate_object_meta(
        &sa.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    )
}

fn has_enforce_annotation(sa: &ServiceAccount) -> bool {
    sa.metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(ENFORCE_MOUNTABLE_SECRETS_ANNOTATION))
}

/// `warnIfHasEnforceMountableSecretsAnnotation` (strategy.go:84-99): warn
/// only when the request newly sets the annotation.
fn warn_if_has_enforce_annotation(
    sa: &ServiceAccount,
    old: Option<&ServiceAccount>,
) -> Vec<String> {
    if old.is_some_and(has_enforce_annotation) || !has_enforce_annotation(sa) {
        return Vec::new();
    }
    vec![format!(
        "metadata.annotations[{ENFORCE_MOUNTABLE_SECRETS_ANNOTATION}]: deprecated in v1.32+; prefer separate namespaces to isolate access to mounted secrets"
    )]
}

/// `strategy` (strategy.go:36-46).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ServiceAccount> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ServiceAccount) {
        clean_secret_references(obj);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ServiceAccount) -> ErrorList {
        validate(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &ServiceAccount) -> Vec<String> {
        warn_if_has_enforce_annotation(obj, None)
    }
}

impl RestUpdateStrategy<ServiceAccount> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ServiceAccount,
        _old: &ServiceAccount,
    ) {
        clean_secret_references(obj);
    }

    /// `ValidateServiceAccountUpdate` (validation.go:7648-7652).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ServiceAccount,
        old: &ServiceAccount,
    ) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        errs.extend(validate(obj));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &ServiceAccount,
        old: &ServiceAccount,
    ) -> Vec<String> {
        warn_if_has_enforce_annotation(obj, Some(old))
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ServiceAccount> for Strategy {}

/// `NewREST` (storage/storage.go:45-60): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ServiceAccount, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "serviceaccounts"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}
