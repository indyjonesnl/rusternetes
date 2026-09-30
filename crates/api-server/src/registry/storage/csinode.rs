//! CSINode strategy and storage — port of
//! `pkg/registry/storage/csinode/strategy.go` and
//! `pkg/registry/storage/csinode/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::CSINode;
use rusternetes_common::validation::csinode::{
    validate_csi_node, validate_csi_node_update, CsiNodeValidationOptions,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// Both `Validate` and `ValidateUpdate` set `AllowLongNodeID: true`
/// (strategy.go:57-59, :88-90).
const OPTS: CsiNodeValidationOptions = CsiNodeValidationOptions {
    allow_long_node_id: true,
};

/// `ValidateCSINode` (validation.go:306-310): ObjectMeta with
/// `ValidateNodeName` (`NameIsDNSSubdomain`), then the spec.
fn validate(obj: &CSINode) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_csi_node(obj, OPTS));
    errs
}

/// `csiNodeStrategy` (strategy.go:30-46).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<CSINode> for Strategy {
    /// `PrepareForCreate` does nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut CSINode) {}

    fn validate(&self, _ctx: &RequestContext, obj: &CSINode) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<CSINode> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` does nothing.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut CSINode, _old: &CSINode) {}

    /// `ValidateUpdate`: `ValidateCSINode` then `ValidateCSINodeUpdate`
    /// (which runs `ValidateCSINode` again, as upstream does).
    fn validate_update(&self, _ctx: &RequestContext, obj: &CSINode, old: &CSINode) -> ErrorList {
        let mut errs = validate(obj);
        errs.extend(validate_csi_node_update(obj, old, OPTS));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `csiNodeStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<CSINode> for Strategy {}

/// `NewREST` (storage/storage.go:45-52): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<CSINode, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("storage.k8s.io", "csinodes"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}
