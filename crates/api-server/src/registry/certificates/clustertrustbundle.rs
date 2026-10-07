//! ClusterTrustBundle strategy and storage — port of
//! `pkg/registry/certificates/clustertrustbundle/strategy.go` and
//! `storage/storage.go`.
//!
//! `getAttrs` (storage.go:68-79) makes `spec.signerName` selectable next to
//! the `metadata` fields; the Store's list/watch filter on the serialized
//! object (`handlers::filtering`), where `spec.signerName` already resolves.
//! The `ClusterTrustBundleAttest` admission plugin is in
//! [`crate::admission::certificates`].

use std::sync::Arc;

use rusternetes_common::resources::ClusterTrustBundle;
use rusternetes_common::validation::clustertrustbundle::{
    validate_cluster_trust_bundle, validate_cluster_trust_bundle_update,
    ValidateClusterTrustBundleOptions,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `strategy` (strategy.go:36-39), with `names.SimpleNameGenerator`.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:45-47.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ClusterTrustBundle> for Strategy {
    /// PrepareForCreate (strategy.go:49) changes nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ClusterTrustBundle) {}

    /// Validate (strategy.go:51-54).
    fn validate(&self, _ctx: &RequestContext, obj: &ClusterTrustBundle) -> ErrorList {
        validate_cluster_trust_bundle(obj, ValidateClusterTrustBundleOptions::default())
    }
}

impl RestUpdateStrategy<ClusterTrustBundle> for Strategy {
    /// strategy.go:64-66.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// PrepareForUpdate (strategy.go:68) changes nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut ClusterTrustBundle,
        _old: &ClusterTrustBundle,
    ) {
    }

    /// ValidateUpdate (strategy.go:70-74).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ClusterTrustBundle,
        old: &ClusterTrustBundle,
    ) -> ErrorList {
        validate_cluster_trust_bundle_update(obj, old)
    }

    /// strategy.go:80-82.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ClusterTrustBundle> for Strategy {}

/// `NewREST` (storage/storage.go:45-66).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ClusterTrustBundle, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("certificates.k8s.io", "clustertrustbundles"),
        Arc::new(Strategy),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// strategy_test.go:26-48.
    #[test]
    fn strategy_flags_and_warnings() {
        let s = Strategy;
        let obj = ClusterTrustBundle::default();
        let ctx = RequestContext::new(None);
        assert!(RestCreateStrategy::warnings_on_create(&s, &ctx, &obj).is_empty());
        assert!(!RestUpdateStrategy::<ClusterTrustBundle>::allow_create_on_update(&s));
        assert!(RestUpdateStrategy::warnings_on_update(&s, &ctx, &obj, &obj).is_empty());
        assert!(!RestUpdateStrategy::<ClusterTrustBundle>::allow_unconditional_update(&s));
        assert!(!NamespaceScopedStrategy::namespace_scoped(&s));
    }
}
