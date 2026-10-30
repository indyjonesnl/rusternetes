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
