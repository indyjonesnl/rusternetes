//! ControllerRevision strategy and storage — port of
//! `pkg/registry/apps/controllerrevision/strategy.go` and
//! `pkg/registry/apps/controllerrevision/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::ControllerRevision;
use rusternetes_common::validation::controllerrevision::{
    validate_controller_revision, validate_controller_revision_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `controllerrevision.Strategy` (strategy.go:32-39).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ControllerRevision> for Strategy {
    /// `PrepareForCreate` (strategy.go:58-60) changes nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ControllerRevision) {}

    /// `ValidateControllerRevisionCreate` (strategy.go:62-66).
    fn validate(&self, _ctx: &RequestContext, obj: &ControllerRevision) -> ErrorList {
        validate_controller_revision(obj)
    }
}

impl RestUpdateStrategy<ControllerRevision> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:71-74) changes nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut ControllerRevision,
        _old: &ControllerRevision,
    ) {
    }

    /// `ValidateControllerRevisionUpdate` (strategy.go:80-84).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ControllerRevision,
        old: &ControllerRevision,
    ) -> ErrorList {
        validate_controller_revision_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// The strategy implements neither `RESTGracefulDeleteStrategy` nor
/// `GarbageCollectionDeleteStrategy`, so deletion takes the defaults.
impl RestDeleteStrategy<ControllerRevision> for Strategy {}

/// `NewREST` (storage/storage.go:36-54): a plain `genericregistry.Store`
/// driven by [`Strategy`].
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ControllerRevision, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "controllerrevisions"),
        Arc::new(Strategy),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        let ctx = RequestContext::new(Some("default"));
        assert!(Strategy.default_garbage_collection_policy(&ctx).is_none());
        assert!(Strategy.graceful().is_none());
    }
}
