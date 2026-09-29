//! LimitRange strategy and storage — port of
//! `pkg/registry/core/limitrange/strategy.go` and
//! `pkg/registry/core/limitrange/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::LimitRange;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::limitrange::validate_limit_range;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// The v1 defaulting a decoded LimitRange goes through:
/// `SetDefaults_LimitRangeItem` (pkg/apis/core/v1/defaults.go:360-390).
pub fn convert_to_internal(lr: &mut LimitRange) {
    crate::handlers::defaults::apply_limit_range_defaults(lr);
}

/// `limitrangeStrategy` (strategy.go:31-38).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<LimitRange> for Strategy {
    /// `PrepareForCreate` (strategy.go:44-49): a LimitRange created without a
    /// name is named with a fresh UUID.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut LimitRange) {
        if obj.metadata.name.is_empty() {
            obj.metadata.name = uuid::Uuid::new_v4().to_string();
        }
    }

    fn validate(&self, _ctx: &RequestContext, obj: &LimitRange) -> ErrorList {
        validate_limit_range(obj)
    }
}

impl RestUpdateStrategy<LimitRange> for Strategy {
    /// strategy.go:68-70: a PUT may create a LimitRange.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// strategy.go:51-52: nothing to prepare.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut LimitRange, _old: &LimitRange) {}

    /// strategy.go:72-76: an update is validated as a create.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &LimitRange,
        _old: &LimitRange,
    ) -> ErrorList {
        validate_limit_range(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// LimitRange uses the default delete strategy.
impl RestDeleteStrategy<LimitRange> for Strategy {}

/// `NewREST` (storage/storage.go:34-53).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<LimitRange, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "limitranges"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit_range() -> LimitRange {
        let mut lr: LimitRange = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "LimitRange",
            "metadata": {"name": "l", "namespace": "default", "resourceVersion": "1"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "min": {"memory": "64Mi"}},
                {"type": "Pod", "max": {"cpu": "4"}}
            ]}
        }))
        .unwrap();
        convert_to_internal(&mut lr);
        lr
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
    }

    /// `SetDefaults_LimitRangeItem`: a Container item's default limit comes
    /// from `max`, its default request from the default limit and then `min`.
    /// A Pod item is left alone.
    #[test]
    fn container_items_are_defaulted() {
        let lr = limit_range();
        let container = &lr.spec.limits[0];
        assert_eq!(container.default.as_ref().unwrap()["cpu"], "2");
        let request = container.default_request.as_ref().unwrap();
        assert_eq!(request["cpu"], "2");
        assert_eq!(request["memory"], "64Mi");
        let pod = &lr.spec.limits[1];
        assert!(pod.default.is_none() && pod.default_request.is_none());
    }

    /// An explicit default is not overwritten by `max`, and the default
    /// request follows it. Empty maps stay unset (`omitempty`).
    #[test]
    fn an_explicit_default_is_kept() {
        let mut lr: LimitRange = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "l"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "default": {"cpu": "1"}},
                {"type": "Container"}
            ]}
        }))
        .unwrap();
        convert_to_internal(&mut lr);
        let item = &lr.spec.limits[0];
        assert_eq!(item.default.as_ref().unwrap()["cpu"], "1");
        assert_eq!(item.default_request.as_ref().unwrap()["cpu"], "1");
        let bare = &lr.spec.limits[1];
        assert!(bare.default.is_none() && bare.default_request.is_none());
    }

    #[test]
    fn prepare_for_create_names_an_unnamed_limit_range() {
        let mut lr = limit_range();
        lr.metadata.name.clear();
        Strategy.prepare_for_create(&ctx(), &mut lr);
        assert!(uuid::Uuid::parse_str(&lr.metadata.name).is_ok());
        let errs = Strategy.validate(&ctx(), &lr);
        assert!(errs.is_empty(), "{errs:?}");
    }
}
