//! DaemonSet strategies and storage — port of
//! `pkg/registry/apps/daemonset/strategy.go` and
//! `pkg/registry/apps/daemonset/storage/storage.go`.
//!
//! The internal `spec.templateGeneration` is the
//! `deprecated.daemonset.template.generation` annotation here, which is how
//! the v1 conversion carries it (see [`DaemonSet::template_generation`]).

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{DaemonSet, DaemonSetStatus};
use rusternetes_common::validation::apps::{
    validate_daemonset, validate_daemonset_status_update, validate_daemonset_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::ssa::ResetFields;

/// The v1 defaulting a decoded DaemonSet goes through:
/// `SetDefaults_DaemonSet` (pkg/apis/apps/v1/defaults.go) and the pod
/// template's defaults.
pub fn convert_to_internal(ds: &mut DaemonSet) {
    crate::handlers::defaults::apply_daemonset_defaults(ds);
}

/// `daemonSetStrategy` (strategy.go:36-42).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<DaemonSet> for Strategy {
    /// `PrepareForCreate` (strategy.go:70-80): status is cleared, the
    /// generation starts at 1 and the template generation at least at 1.
    /// `DropDisabledTemplateFields` is strategy.go:79.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut DaemonSet) {
        obj.status = Some(DaemonSetStatus::default());
        obj.metadata.generation = Some(1);
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            None,
        );
        let template_generation = obj.template_generation();
        obj.set_template_generation(template_generation.max(1));
    }

    fn validate(&self, _ctx: &RequestContext, obj: &DaemonSet) -> ErrorList {
        validate_daemonset(obj)
    }

    // `WarningsOnCreate` (strategy.go:124-127) is only
    // `GetWarningsForPodTemplate`, which is not ported (#1996).
}

impl RestUpdateStrategy<DaemonSet> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:83-114): status and the template
    /// generation are kept. A template change bumps both the template
    /// generation and the generation. Any other spec change bumps only the
    /// generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut DaemonSet, old: &DaemonSet) {
        obj.status = old.status.clone();
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            Some(&old.spec.template),
        );
        let old_template_generation = old.template_generation();
        obj.set_template_generation(old_template_generation);

        let old_generation = old.metadata.generation.unwrap_or(0);
        if !semantic_equal(&old.spec.template, &obj.spec.template) {
            obj.set_template_generation(old_template_generation + 1);
            obj.metadata.generation = Some(old_generation + 1);
            return;
        }
        if !semantic_equal(&old.spec, &obj.spec) {
            obj.metadata.generation = Some(old_generation + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &DaemonSet,
        old: &DaemonSet,
    ) -> ErrorList {
        validate_daemonset_update(obj, old)
    }

    // `WarningsOnUpdate` (strategy.go:151-159) is only
    // `GetWarningsForPodTemplate`, which is not ported (#1996).

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:57-67): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["status"]])
    }
}

/// `DefaultGarbageCollectionPolicy` (strategy.go:48-50): `DeleteDependents`.
impl RestDeleteStrategy<DaemonSet> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::DeleteDependents)
    }
}

/// `daemonSetStatusStrategy` (strategy.go:166-197): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<DaemonSet> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:183-187: only status may change.
    ///
    /// The template generation lives in an annotation here, not in the spec,
    /// so it is copied too. Upstream restores it along with the spec.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut DaemonSet, old: &DaemonSet) {
        obj.spec = old.spec.clone();
        obj.set_template_generation(old.template_generation());
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &DaemonSet,
        old: &DaemonSet,
    ) -> ErrorList {
        validate_daemonset_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:173-181): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["spec"]])
    }
}

/// `NewREST` (storage/storage.go:41-65): the DaemonSet store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<DaemonSet, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "daemonsets"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the DaemonSet store updating with
/// [`StatusStrategy`] (storage.go:60-62).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<DaemonSet, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
