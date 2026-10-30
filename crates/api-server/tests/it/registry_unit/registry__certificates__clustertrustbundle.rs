//! Unit tests moved from `src/registry/certificates/clustertrustbundle.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::ClusterTrustBundle;
    pub use rusternetes_common::validation::clustertrustbundle::{
        validate_cluster_trust_bundle, validate_cluster_trust_bundle_update,
        ValidateClusterTrustBundleOptions,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::certificates::clustertrustbundle::*;

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
