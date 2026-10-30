//! StatefulSet strategies and storage — port of
//! `pkg/registry/apps/statefulset/strategy.go` and
//! `pkg/registry/apps/statefulset/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::pod_warnings::get_warnings_for_pod_template;
use rusternetes_common::resources::{
    Scale, ScaleSpec, ScaleStatus, StatefulSet, StatefulSetStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_statefulset, validate_statefulset_status_update, validate_statefulset_update,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_storage::StorageBackend;

use crate::registry::core::persistentvolumeclaim::get_warnings_for_persistent_volume_claim_spec;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};
use crate::ssa::ResetFields;

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
    rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
        &mut new.spec.template,
        old.map(|o| &o.spec.template),
    );
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

/// The `revisionHistoryLimit` warning shared by `WarningsOnCreate` /
/// `WarningsOnUpdate` (strategy.go:143-145, 178-180).
fn revision_history_limit_warning(ss: &StatefulSet) -> Vec<String> {
    match ss.spec.revision_history_limit {
        Some(limit) if limit < 0 => vec![
            "spec.revisionHistoryLimit: a negative value retains all historical revisions; a value >= 0 is recommended".to_string(),
        ],
        _ => Vec::new(),
    }
}

/// `pvcutil.GetWarningsForPersistentVolumeClaimSpec` over every
/// `volumeClaimTemplates[i]` (strategy.go:139-141, 175-177). `child` is the
/// path segment appended after the index: upstream's create path uses none, its
/// update path uses the (miscapitalised) `Spec`, ported as written.
fn volume_claim_template_warnings(ss: &StatefulSet, child: Option<&str>) -> Vec<String> {
    let base = Path::new("spec").child("volumeClaimTemplates");
    ss.spec
        .volume_claim_templates
        .iter()
        .flatten()
        .enumerate()
        .flat_map(|(i, pvc)| {
            let at = match child {
                Some(c) => base.index(i).child(c),
                None => base.index(i),
            };
            get_warnings_for_persistent_volume_claim_spec(&at, &pvc.spec)
        })
        .collect()
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
        let mut warnings = get_warnings_for_pod_template(
            &Path::new("spec").child("template"),
            &obj.spec.template,
            None,
        );
        warnings.extend(volume_claim_template_warnings(obj, None));
        warnings.extend(revision_history_limit_warning(obj));
        warnings
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
        old: &StatefulSet,
    ) -> Vec<String> {
        // strategy.go:172-174: only a generation change re-evaluates the template.
        let mut warnings = if obj.metadata.generation != old.metadata.generation {
            get_warnings_for_pod_template(
                &Path::new("spec").child("template"),
                &obj.spec.template,
                Some(&old.spec.template),
            )
        } else {
            Vec::new()
        };
        warnings.extend(volume_claim_template_warnings(obj, Some("Spec")));
        warnings.extend(revision_history_limit_warning(obj));
        warnings
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:59-69): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["status"]])
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

    /// `GetResetFields` (strategy.go:197-205): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["spec"]])
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
