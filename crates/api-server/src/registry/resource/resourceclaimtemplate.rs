//! ResourceClaimTemplate strategy and storage — port of
//! `pkg/registry/resource/resourceclaimtemplate/strategy.go` and
//! `pkg/registry/resource/resourceclaimtemplate/storage/storage.go`.
//!
//! The admin-access namespace check is described in [`super::admin`].

use std::sync::Arc;

use rusternetes_common::resources::ResourceClaimTemplate;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::resourceclaim::{
    validate_resource_claim_template, validate_resource_claim_template_update,
};
use rusternetes_storage::StorageBackend;

use super::admin::authorized_for_admin;
use super::spec::drop_disabled_fields;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy, ValidateFuture,
};

/// `resourceClaimTemplateStrategy` (strategy.go:35-52).
pub struct Strategy {
    storage: Arc<StorageBackend>,
}

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ResourceClaimTemplate> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ResourceClaimTemplate) {
        drop_disabled_fields(&mut obj.spec.spec, None);
    }

    /// `AuthorizedForAdmin` (strategy.go:64), ahead of the validation errors.
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ResourceClaimTemplate,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let ns = ctx
                .namespace
                .clone()
                .or_else(|| obj.metadata.namespace.clone())
                .unwrap_or_default();
            authorized_for_admin(&self.storage, &obj.spec.spec.devices.requests, &ns).await
        })
    }

    /// `ValidateResourceClaimTemplate` (validation.go:605-609): ObjectMeta with
    /// `ValidateResourceClaimTemplateName` (`NameIsDNSSubdomain`), then the spec.
    fn validate(&self, _ctx: &RequestContext, obj: &ResourceClaimTemplate) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            true,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_resource_claim_template(obj));
        errs
    }
}

impl RestUpdateStrategy<ResourceClaimTemplate> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceClaimTemplate,
        old: &ResourceClaimTemplate,
    ) {
        drop_disabled_fields(&mut obj.spec.spec, Some(&old.spec.spec));
    }

    /// `ValidateResourceClaimTemplate` then `ValidateResourceClaimTemplateUpdate`
    /// (strategy.go:92-96).
    fn validate_update(
        &self,
        ctx: &RequestContext,
        obj: &ResourceClaimTemplate,
        old: &ResourceClaimTemplate,
    ) -> ErrorList {
        let mut errs = RestCreateStrategy::validate(self, ctx, obj);
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &Path::new("metadata"),
        ));
        errs.extend(validate_resource_claim_template_update(obj, old));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ResourceClaimTemplate> for Strategy {}

/// `NewREST` (storage/storage.go): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ResourceClaimTemplate, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("resource.k8s.io", "resourceclaimtemplates"),
        Arc::new(Strategy { storage }),
    );
    store.return_deleted_object = true;
    store
}
