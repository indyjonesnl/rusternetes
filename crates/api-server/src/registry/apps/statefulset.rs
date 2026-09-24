//! StatefulSet strategies and storage — port of
//! `pkg/registry/apps/statefulset/strategy.go` and
//! `pkg/registry/apps/statefulset/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::{
    Scale, ScaleSpec, ScaleStatus, StatefulSet, StatefulSetStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_statefulset, validate_statefulset_status_update, validate_statefulset_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};

/// The v1 defaulting a decoded StatefulSet goes through:
/// `SetDefaults_StatefulSet` (pkg/apis/apps/v1/defaults.go) and the pod
/// template's defaults.
pub fn convert_to_internal(ss: &mut StatefulSet) {
    crate::handlers::defaults::apply_statefulset_defaults(ss);
}

/// `maxUnavailableInUse` (strategy.go:84-93).
fn max_unavailable_in_use(ss: Option<&StatefulSet>) -> bool {
    ss.and_then(|ss| ss.spec.update_strategy.as_ref())
        .and_then(|us| us.rolling_update.as_ref())
        .is_some_and(|ru| ru.max_unavailable.is_some())
}

/// `dropStatefulSetDisabledFields` (strategy.go:120-126): with
/// `MaxUnavailableStatefulSet` off (the 1.35 default) a new
/// `rollingUpdate.maxUnavailable` is dropped unless the stored object already
/// uses it.
fn drop_disabled_fields(new: &mut StatefulSet, old: Option<&StatefulSet>) {
    if !feature_gates::enabled(Feature::MaxUnavailableStatefulSet) && !max_unavailable_in_use(old) {
        if let Some(ru) = new
            .spec
            .update_strategy
            .as_mut()
            .and_then(|us| us.rolling_update.as_mut())
        {
            ru.max_unavailable = None;
        }
    }
}

/// The `revisionHistoryLimit` warning of `WarningsOnCreate` /
/// `WarningsOnUpdate` (strategy.go:136-147, 168-183). `GetWarningsForPodTemplate` and the
/// per-template `GetWarningsForPersistentVolumeClaimSpec` are not ported
/// (#1996, #2000).
fn revision_history_limit_warning(ss: &StatefulSet) -> Vec<String> {
    match ss.spec.revision_history_limit {
        Some(limit) if limit < 0 => vec![
            "spec.revisionHistoryLimit: a negative value retains all historical revisions; a value >= 0 is recommended".to_string(),
        ],
        _ => Vec::new(),
    }
}

/// `statefulSetStrategy` (strategy.go:38-44).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<StatefulSet> for Strategy {
    /// `PrepareForCreate` (strategy.go:72-81): status is cleared, the generation starts at 1 and
    /// gated fields are dropped.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut StatefulSet) {
        obj.status = Some(StatefulSetStatus::default());
        obj.metadata.generation = Some(1);
        drop_disabled_fields(obj, None);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &StatefulSet) -> ErrorList {
        validate_statefulset(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &StatefulSet) -> Vec<String> {
        revision_history_limit_warning(obj)
    }
}

impl RestUpdateStrategy<StatefulSet> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:96-110): status is kept, gated fields are dropped, and a
    /// spec change bumps the generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut StatefulSet, old: &StatefulSet) {
        obj.status = old.status.clone();
        drop_disabled_fields(obj, Some(old));
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StatefulSet,
        old: &StatefulSet,
    ) -> ErrorList {
        validate_statefulset_update(obj, old)
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &StatefulSet,
        _old: &StatefulSet,
    ) -> Vec<String> {
        revision_history_limit_warning(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `DefaultGarbageCollectionPolicy` (strategy.go:50-52): `DeleteDependents`.
impl RestDeleteStrategy<StatefulSet> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::DeleteDependents)
    }
}

/// `statefulSetStatusStrategy` (strategy.go:190-225): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<StatefulSet> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:208-213: only status may change; labels are not reset.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut StatefulSet, old: &StatefulSet) {
        obj.spec = old.spec.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &StatefulSet,
        old: &StatefulSet,
    ) -> ErrorList {
        validate_statefulset_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:85-112): the StatefulSet store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<StatefulSet, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "statefulsets"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the StatefulSet store updating with
/// [`StatusStrategy`].
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<StatefulSet, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// `scaleFromStatefulSet` (storage/storage.go:260-282).
pub fn scale_from_stateful_set(ss: &StatefulSet) -> Result<Scale, String> {
    let selector = ss.spec.selector.as_selector_string()?;
    let meta = &ss.metadata;
    Ok(Scale {
        type_meta: TypeMeta {
            kind: "Scale".to_string(),
            api_version: "autoscaling/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: meta.name.clone(),
            namespace: meta.namespace.clone(),
            uid: meta.uid.clone(),
            resource_version: meta.resource_version.clone(),
            creation_timestamp: meta.creation_timestamp,
            ..ObjectMeta::default()
        },
        spec: ScaleSpec {
            replicas: ss.spec.replicas.unwrap_or(0),
        },
        status: ScaleStatus {
            replicas: ss.status.as_ref().map_or(0, |s| s.replicas),
            selector,
        },
    })
}

/// `ScaleREST{store: statefulSetRest.Store}` (storage/storage.go:75).
pub fn new_scale_rest(storage: Arc<StorageBackend>) -> ScaleRest<StatefulSet> {
    ScaleRest::new(
        new_store(storage),
        Scalable {
            to_scale: scale_from_stateful_set,
            set_replicas: |ss, replicas| ss.spec.replicas = Some(replicas),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::policy::IntOrString;
    use std::collections::HashMap;

    fn stateful_set() -> StatefulSet {
        let mut ss: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "StatefulSet",
            "metadata": {"name": "s", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "serviceName": "svc",
                "selector": {"matchLabels": {"app": "s"}},
                "template": {
                    "metadata": {"labels": {"app": "s"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut ss);
        ss
    }

    fn with_max_unavailable(mut ss: StatefulSet) -> StatefulSet {
        ss.spec
            .update_strategy
            .as_mut()
            .unwrap()
            .rolling_update
            .as_mut()
            .unwrap()
            .max_unavailable = Some(IntOrString::Int(2));
        ss
    }

    fn max_unavailable(ss: &StatefulSet) -> Option<IntOrString> {
        ss.spec
            .update_strategy
            .as_ref()?
            .rolling_update
            .as_ref()?
            .max_unavailable
            .clone()
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
    fn prepare_for_create_resets_status_and_generation() {
        let mut ss = stateful_set();
        Strategy.prepare_for_create(&ctx(), &mut ss);
        assert_eq!(ss.status, Some(StatefulSetStatus::default()));
        assert_eq!(ss.metadata.generation, Some(1));
    }

    /// `dropStatefulSetDisabledFields` with the gate at its 1.35 default
    /// (off): dropped on create and on an update that introduces it, kept
    /// when the stored object already used it.
    #[test]
    #[serial_test::serial]
    fn max_unavailable_is_dropped_while_the_gate_is_off() {
        let _gate = feature_gates::with_feature(Feature::MaxUnavailableStatefulSet, false);

        let mut created = with_max_unavailable(stateful_set());
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(max_unavailable(&created), None);

        let old = stateful_set();
        let mut introduced = with_max_unavailable(old.clone());
        Strategy.prepare_for_update(&ctx(), &mut introduced, &old);
        assert_eq!(max_unavailable(&introduced), None);

        let old = with_max_unavailable(stateful_set());
        let mut kept = old.clone();
        Strategy.prepare_for_update(&ctx(), &mut kept, &old);
        assert_eq!(max_unavailable(&kept), Some(IntOrString::Int(2)));
    }

    #[test]
    #[serial_test::serial]
    fn max_unavailable_is_kept_with_the_gate_on() {
        let _gate = feature_gates::with_feature(Feature::MaxUnavailableStatefulSet, true);
        let mut created = with_max_unavailable(stateful_set());
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(max_unavailable(&created), Some(IntOrString::Int(2)));
    }

    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = stateful_set();

        let mut annotated = old.clone();
        annotated.status = None;
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.status, old.status);
        assert_eq!(annotated.metadata.generation, Some(4));

        let mut scaled = old.clone();
        scaled.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));
    }

    #[test]
    fn status_prepare_for_update_keeps_spec_only() {
        let old = stateful_set();
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.status = Some(StatefulSetStatus {
            replicas: 2,
            ..StatefulSetStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, Some(1));
        assert_eq!(new.metadata.labels.unwrap()["l"], "new");
        assert_eq!(new.status.unwrap().replicas, 2);
    }

    #[test]
    fn a_negative_revision_history_limit_warns() {
        let mut ss = stateful_set();
        ss.spec.revision_history_limit = Some(10);
        assert!(Strategy.warnings_on_create(&ctx(), &ss).is_empty());
        ss.spec.revision_history_limit = Some(-1);
        let warnings = Strategy.warnings_on_create(&ctx(), &ss);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].starts_with("spec.revisionHistoryLimit:"));
        assert_eq!(Strategy.warnings_on_update(&ctx(), &ss, &ss).len(), 1);
    }

    #[test]
    fn scale_from_stateful_set_maps_replicas_and_selector() {
        let ss = stateful_set();
        let scale = scale_from_stateful_set(&ss).unwrap();
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
        assert_eq!(scale.status.selector, "app=s");
    }
}
