//! Unit tests moved from `src/registry/core/resourcequota.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::{ResourceQuota, ResourceQuotaStatus};
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::resourcequota::{
        validate_resource_quota, validate_resource_quota_status_update,
        validate_resource_quota_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::resourcequota::*;

    fn quota(hard: serde_json::Value) -> ResourceQuota {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "q", "namespace": "default", "resourceVersion": "1"},
            "spec": {"hard": hard},
            "status": {"hard": {"pods": "1"}, "used": {"pods": "0"}}
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `TestResourceQuotaStrategy` (strategy_test.go): create clears status.
    #[test]
    fn prepare_for_create_clears_status() {
        let mut q = quota(serde_json::json!({"pods": "1"}));
        Strategy.prepare_for_create(&ctx(), &mut q);
        assert_eq!(q.status, Some(ResourceQuotaStatus::default()));
        let errs = Strategy.validate(&ctx(), &q);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn update_keeps_status_and_status_update_keeps_spec() {
        let old = quota(serde_json::json!({"pods": "1"}));
        let mut new = quota(serde_json::json!({"pods": "2"}));
        new.status = None;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status, old.status);
        assert_eq!(new.spec.hard, Some([("pods".into(), "2".into())].into()));

        let mut status = quota(serde_json::json!({"pods": "5"}));
        status.status = Some(ResourceQuotaStatus::default());
        StatusStrategy.prepare_for_update(&ctx(), &mut status, &old);
        assert_eq!(status.spec.hard, old.spec.hard);
        assert_eq!(status.status, Some(ResourceQuotaStatus::default()));
    }

    /// `Test_WarningsOnCreate` (strategy_test.go:64-140).
    #[test]
    fn a_request_above_its_limit_warns() {
        let cases = [
            (serde_json::json!({}), vec![]),
            (
                serde_json::json!({
                    "requests.cpu": "500m", "limits.cpu": "1",
                    "requests.memory": "1Gi", "limits.memory": "2Gi",
                    "requests.storage": "1Gi", "limits.storage": "2Gi",
                    "requests.ephemeral-storage": "1Gi", "limits.ephemeral-storage": "2Gi"
                }),
                vec![],
            ),
            (
                serde_json::json!({
                    "requests.cpu": "2", "limits.cpu": "1",
                    "requests.memory": "3Gi", "limits.memory": "2Gi",
                    "requests.storage": "3Gi", "limits.storage": "2Gi",
                    "requests.ephemeral-storage": "3Gi", "limits.ephemeral-storage": "2Gi"
                }),
                vec![
                    "ResourceQuota requests.cpu (2) should be less than limits.cpu (1)",
                    "ResourceQuota requests.memory (3Gi) should be less than limits.memory (2Gi)",
                    "ResourceQuota requests.storage (3Gi) should be less than limits.storage (2Gi)",
                    "ResourceQuota requests.ephemeral-storage (3Gi) should be less than limits.ephemeral-storage (2Gi)",
                ],
            ),
            (
                serde_json::json!({
                    "cpu": "2", "limits.cpu": "1",
                    "memory": "3Gi", "limits.memory": "2Gi"
                }),
                vec![
                    "ResourceQuota cpu (2) should be less than limits.cpu (1)",
                    "ResourceQuota memory (3Gi) should be less than limits.memory (2Gi)",
                ],
            ),
        ];
        for (hard, want) in cases {
            let warnings = Strategy.warnings_on_create(&ctx(), &quota(hard.clone()));
            assert_eq!(warnings, want, "{hard}");
        }
    }
}
