//! ReplicaSet strategies and storage — port of
//! `pkg/registry/apps/replicaset/strategy.go` and
//! `pkg/registry/apps/replicaset/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{ReplicaSet, ReplicaSetStatus, Scale, ScaleSpec, ScaleStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_replicaset, validate_replicaset_status_update, validate_replicaset_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};
use crate::ssa::ResetFields;

/// The v1 defaulting a decoded ReplicaSet goes through:
/// `SetDefaults_ReplicaSet` (pkg/apis/apps/v1/defaults.go) and the pod
/// template's defaults.
pub fn convert_to_internal(rs: &mut ReplicaSet) {
    crate::handlers::defaults::apply_replicaset_defaults(rs);
}

/// `rsStrategy` (strategy.go:46-52).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ReplicaSet> for Strategy {
    /// strategy.go:80-86, including `DropDisabledTemplateFields` (:86).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ReplicaSet) {
        obj.status = Some(ReplicaSetStatus::default());
        obj.metadata.generation = Some(1);
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            None,
        );
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ReplicaSet) -> ErrorList {
        validate_replicaset(obj)
    }

    /// strategy.go:119-127. `GetWarningsForPodTemplate` is not ported
    /// (#1996); the name warning is.
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &ReplicaSet) -> Vec<String> {
        let msgs = is_dns1123_label(&obj.metadata.name);
        if msgs.is_empty() {
            return Vec::new();
        }
        vec![format!(
            "metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: [{}]",
            msgs.join(" ")
        )]
    }
}

impl RestUpdateStrategy<ReplicaSet> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:90-109: status is kept and a spec change bumps the
    /// generation. Unlike a Deployment, annotations do not.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut ReplicaSet, old: &ReplicaSet) {
        obj.status = old.status.clone();
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            Some(&old.spec.template),
        );
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ReplicaSet,
        old: &ReplicaSet,
    ) -> ErrorList {
        validate_replicaset_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:69-77): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["status"]])
    }
}

/// strategy.go:58-60.
impl RestDeleteStrategy<ReplicaSet> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::DeleteDependents)
    }
}

/// `rsStatusStrategy` (strategy.go:192-232): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<ReplicaSet> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:209-215: only status may change. Unlike the Deployment
    /// status strategy, labels are not reset. `dropDisabledStatusFields` drops
    /// `terminatingReplicas` when its gate is off.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut ReplicaSet, old: &ReplicaSet) {
        obj.spec = old.spec.clone();
        drop_disabled_status_fields(&mut obj.status, &old.status);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ReplicaSet,
        old: &ReplicaSet,
    ) -> ErrorList {
        validate_replicaset_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:199-207): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["spec"]])
    }
}

/// `NewREST` (storage/storage.go:87-116): the ReplicaSet store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ReplicaSet, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "replicasets"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the ReplicaSet store updating with
/// [`StatusStrategy`].
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<ReplicaSet, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// `scaleFromReplicaSet` (storage/storage.go:266-288).
pub fn scale_from_replica_set(rs: &ReplicaSet) -> Result<Scale, String> {
    let selector = rs.spec.selector.as_selector_string()?;
    let meta = &rs.metadata;
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
            replicas: rs.spec.replicas,
        },
        status: ScaleStatus {
            replicas: rs.status.as_ref().map_or(0, |s| s.replicas),
            selector,
        },
    })
}

/// `ScaleREST{store: replicaSetRest.Store}` (storage/storage.go:77).
pub fn new_scale_rest(storage: Arc<StorageBackend>) -> ScaleRest<ReplicaSet> {
    ScaleRest::new(
        new_store(storage),
        Scalable {
            to_scale: scale_from_replica_set,
            set_replicas: |rs, replicas| rs.spec.replicas = replicas,
        },
    )
}

/// `dropDisabledStatusFields` (pkg/registry/apps/replicaset/strategy.go): with
/// `DeploymentReplicaSetTerminatingReplicas` off, `status.terminatingReplicas`
/// is dropped unless the old status already carries it.
fn drop_disabled_status_fields(
    status: &mut Option<ReplicaSetStatus>,
    old: &Option<ReplicaSetStatus>,
) {
    if !rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::DeploymentReplicaSetTerminatingReplicas,
    ) && old
        .as_ref()
        .is_none_or(|o| o.terminating_replicas.is_none())
    {
        if let Some(s) = status.as_mut() {
            s.terminating_replicas = None;
        }
    }
}
