//! ReferenceGrant strategy and storage — the custom-resource strategy
//! (`customresource/strategy.go`) for the gateway-api CRD
//! `gateway.networking.k8s.io_referencegrants.yaml` (namespaced, version
//! `v1beta1`, no status subresource).
//!
//! The store key is `/registry/referencegrants/<ns>/<name>`, which is what the
//! CSI provisioner lists (`csi_provisioner.rs`, the `IsGranted` input).

use std::sync::Arc;

use rusternetes_common::resources::ReferenceGrant;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::referencegrant::validate_reference_grant;
use rusternetes_storage::StorageBackend;

use super::GROUP;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use crate::registry::snapshot::same_json;

/// `customResourceStrategy` for ReferenceGrant.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ReferenceGrant> for Strategy {
    /// `PrepareForCreate` (strategy.go:117-129): no status subresource, so
    /// nothing to clear; the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ReferenceGrant) {
        obj.metadata.generation = Some(1);
    }

    /// `customResourceValidator.Validate` (validator.go:53).
    fn validate(&self, _ctx: &RequestContext, obj: &ReferenceGrant) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            true,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_reference_grant(obj));
        errs
    }
}

impl RestUpdateStrategy<ReferenceGrant> for Strategy {
    /// `AllowCreateOnUpdate` is false for custom resources (strategy.go:262).
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:132-165): "except for the changes to
    /// `metadata`, any other changes cause the generation to increment" — the
    /// grant has only a spec.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ReferenceGrant,
        old: &ReferenceGrant,
    ) {
        if !same_json(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ReferenceGrant,
        _old: &ReferenceGrant,
    ) -> ErrorList {
        validate_reference_grant(obj)
    }

    /// `AllowUnconditionalUpdate` is false for custom resources
    /// (strategy.go:267-269).
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ReferenceGrant> for Strategy {}

/// The store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ReferenceGrant, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new(GROUP, "referencegrants"),
        Arc::new(Strategy),
    )
}
