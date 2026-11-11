//! Unit tests moved from `src/registry/apps/controllerrevision.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use k3rs_api_server::registry::generic::Store;
    pub use k3rs_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use k3rs_common::resources::ControllerRevision;
    pub use k3rs_common::validation::controllerrevision::{
        validate_controller_revision, validate_controller_revision_update,
    };
    pub use k3rs_common::validation::field::ErrorList;
    pub use k3rs_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use k3rs_api_server::registry::apps::controllerrevision::*;

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
