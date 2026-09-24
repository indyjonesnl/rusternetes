//! ConfigMap strategy and storage — port of
//! `pkg/registry/core/configmap/strategy.go` and
//! `pkg/registry/core/configmap/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::ConfigMap;
use rusternetes_common::validation::configmap::{validate_config_map, validate_config_map_update};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `configmap.Strategy` (strategy.go:37-43).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ConfigMap> for Strategy {
    /// `dropDisabledFields(configMap, nil)` (strategy.go:55-58), which drops
    /// nothing: ConfigMap has no feature-gated fields.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ConfigMap) {}

    fn validate(&self, _ctx: &RequestContext, obj: &ConfigMap) -> ErrorList {
        validate_config_map(obj)
    }
}

impl RestUpdateStrategy<ConfigMap> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `dropDisabledFields(newConfigMap, oldConfigMap)` — nothing to drop.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut ConfigMap, _old: &ConfigMap) {}

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ConfigMap,
        old: &ConfigMap,
    ) -> ErrorList {
        validate_config_map_update(old, obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// ConfigMap uses the default delete strategy: no graceful deletion and no
/// default garbage-collection policy.
impl RestDeleteStrategy<ConfigMap> for Strategy {}

/// `NewREST` (storage/storage.go:36-58): a plain `genericregistry.Store`
/// driven by [`Strategy`].
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ConfigMap, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "configmaps"),
        Arc::new(Strategy),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The strategy flags `strategy_test.go` and the store configuration pin.
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
