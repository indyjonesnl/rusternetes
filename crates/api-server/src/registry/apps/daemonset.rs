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
    /// `DropDisabledTemplateFields` drops nothing we model (see the Deployment
    /// strategy).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut DaemonSet) {
        obj.status = Some(DaemonSetStatus::default());
        obj.metadata.generation = Some(1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn daemon_set() -> DaemonSet {
        let mut ds: DaemonSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "DaemonSet",
            "metadata": {"name": "d", "namespace": "default", "generation": 4, "resourceVersion": "1"},
            "spec": {
                "selector": {"matchLabels": {"app": "d"}},
                "template": {
                    "metadata": {"labels": {"app": "d"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"desiredNumberScheduled": 2, "currentNumberScheduled": 2,
                       "numberReady": 1, "numberMisscheduled": 0}
        }))
        .unwrap();
        convert_to_internal(&mut ds);
        ds.set_template_generation(3);
        ds
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert_eq!(
            Strategy.default_garbage_collection_policy(&ctx()),
            Some(GarbageCollectionPolicy::DeleteDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    #[test]
    fn prepare_for_create_resets_status_and_seeds_generations() {
        let mut ds = daemon_set();
        ds.metadata.annotations = None;
        Strategy.prepare_for_create(&ctx(), &mut ds);
        assert_eq!(ds.status, Some(DaemonSetStatus::default()));
        assert_eq!(ds.metadata.generation, Some(1));
        assert_eq!(ds.template_generation(), 1);

        // A template generation the client supplied above 1 is kept.
        let mut ds = daemon_set();
        Strategy.prepare_for_create(&ctx(), &mut ds);
        assert_eq!(ds.template_generation(), 3);
    }

    #[test]
    fn prepare_for_update_bumps_generations_by_what_changed() {
        let old = daemon_set();

        // Metadata only: nothing moves, and status and the template generation
        // the client dropped are restored.
        let mut annotated = old.clone();
        annotated.status = None;
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.status, old.status);
        assert_eq!(annotated.metadata.generation, Some(4));
        assert_eq!(annotated.template_generation(), 3);

        // A client cannot set the template generation itself.
        let mut forged = old.clone();
        forged.set_template_generation(9);
        Strategy.prepare_for_update(&ctx(), &mut forged, &old);
        assert_eq!(forged.template_generation(), 3);
        assert_eq!(forged.metadata.generation, Some(4));

        // A non-template spec change: the generation only.
        let mut slower = old.clone();
        slower.spec.min_ready_seconds = Some(5);
        Strategy.prepare_for_update(&ctx(), &mut slower, &old);
        assert_eq!(slower.metadata.generation, Some(5));
        assert_eq!(slower.template_generation(), 3);

        // A template change: both.
        let mut reimaged = old.clone();
        reimaged.spec.template.spec.containers[0].image = "j".into();
        Strategy.prepare_for_update(&ctx(), &mut reimaged, &old);
        assert_eq!(reimaged.metadata.generation, Some(5));
        assert_eq!(reimaged.template_generation(), 4);
        let errs = Strategy.validate_update(&ctx(), &reimaged, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn status_prepare_for_update_keeps_spec_and_template_generation() {
        let old = daemon_set();
        let mut new = old.clone();
        new.spec.min_ready_seconds = Some(9);
        new.metadata.annotations = None;
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.status = Some(DaemonSetStatus {
            number_ready: 2,
            ..DaemonSetStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.min_ready_seconds, old.spec.min_ready_seconds);
        assert_eq!(new.template_generation(), 3);
        assert_eq!(new.metadata.labels.unwrap()["l"], "new");
        assert_eq!(new.status.unwrap().number_ready, 2);
    }
}
