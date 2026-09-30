//! ResourceQuota strategies and storage — port of
//! `pkg/registry/core/resourcequota/strategy.go` and
//! `pkg/registry/core/resourcequota/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{ResourceQuota, ResourceQuotaStatus};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::resourcequota::{
    validate_resource_quota, validate_resource_quota_status_update, validate_resource_quota_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `knownResourceNames` (strategy.go:79-85): the resources whose request is
/// checked against their limit.
const KNOWN_RESOURCE_NAMES: &[&str] = &["cpu", "memory", "storage", "ephemeral-storage"];

/// `resourcequotaStrategy` (strategy.go:33-40).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ResourceQuota> for Strategy {
    /// `PrepareForCreate` (strategy.go:60-63): status is cleared; the quota
    /// controller computes it.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ResourceQuota) {
        obj.status = Some(ResourceQuotaStatus::default());
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ResourceQuota) -> ErrorList {
        validate_resource_quota(obj)
    }

    /// `WarningsOnCreate` (strategy.go:87-108): a request above its limit
    /// warns. For cpu and memory the bare name stands in for the request.
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &ResourceQuota) -> Vec<String> {
        let Some(hard) = obj.spec.hard.as_ref() else {
            return Vec::new();
        };
        let quantity = |name: &str| hard.get(name).and_then(|v| Quantity::parse(v).ok());
        let mut warnings = Vec::new();
        for &resource_name in KNOWN_RESOURCE_NAMES {
            let mut request_name = format!("requests.{resource_name}");
            let mut request = quantity(&request_name);
            if request.is_none() && (resource_name == "cpu" || resource_name == "memory") {
                request = quantity(resource_name);
                if request.is_some() {
                    request_name = resource_name.to_string();
                }
            }
            let limit_name = format!("limits.{resource_name}");
            let limit = quantity(&limit_name);
            if let (Some(request), Some(limit)) = (request, limit) {
                if request.cmp_value(&limit) == std::cmp::Ordering::Greater {
                    warnings.push(format!(
                        "ResourceQuota {request_name} ({request}) should be less than {limit_name} ({limit})"
                    ));
                }
            }
        }
        warnings
    }
}

impl RestUpdateStrategy<ResourceQuota> for Strategy {
    /// strategy.go:115-117.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:66-70): status only changes through
    /// `/status`.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceQuota,
        old: &ResourceQuota,
    ) {
        obj.status = old.status.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceQuota,
        old: &ResourceQuota,
    ) -> ErrorList {
        validate_resource_quota_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// ResourceQuotas use the default delete strategy.
impl RestDeleteStrategy<ResourceQuota> for Strategy {}

/// `resourcequotaStatusStrategy` (strategy.go:134-167): the update strategy
/// of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<ResourceQuota> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:153-157: only status may change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceQuota,
        old: &ResourceQuota,
    ) {
        obj.spec = old.spec.clone();
    }

    /// strategy.go:159-161.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceQuota,
        old: &ResourceQuota,
    ) -> ErrorList {
        validate_resource_quota_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:41-66): the ResourceQuota store, which
/// returns the deleted object.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ResourceQuota, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "resourcequotas"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}

/// The `/status` store: the ResourceQuota store updating with
/// [`StatusStrategy`] (storage.go:61-63).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<ResourceQuota, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;

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
