//! ReplicaSet strategies and storage — port of
//! `pkg/registry/apps/replicaset/strategy.go` and
//! `pkg/registry/apps/replicaset/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::{ReplicaSet, ReplicaSetStatus, Scale, ScaleSpec, ScaleStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_replicaset, validate_replicaset_status_update, validate_replicaset_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_storage::StorageBackend;

use crate::registry::equality::semantic_equal;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};

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
    /// strategy.go:80-86. `DropDisabledTemplateFields` drops nothing we
    /// model (see the Deployment strategy).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ReplicaSet) {
        obj.status = Some(ReplicaSetStatus::default());
        obj.metadata.generation = Some(1);
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
    /// status strategy, labels are not reset. `dropDisabledStatusFields` is a
    /// no-op: its `DeploymentReplicaSetTerminatingReplicas` gate is on by
    /// default in 1.35.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut ReplicaSet, old: &ReplicaSet) {
        obj.spec = old.spec.clone();
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
}

/// `NewREST` (storage/storage.go:87-116): the ReplicaSet store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ReplicaSet, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "replicasets"),
        Arc::new(Strategy),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn replica_set() -> ReplicaSet {
        let mut rs: ReplicaSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "ReplicaSet",
            "metadata": {"name": "r", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "r"}},
                "template": {
                    "metadata": {"labels": {"app": "r"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut rs);
        rs
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

    /// strategy.go:80-86.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut rs = replica_set();
        Strategy.prepare_for_create(&ctx(), &mut rs);
        assert_eq!(rs.status, Some(ReplicaSetStatus::default()));
        assert_eq!(rs.metadata.generation, Some(1));
    }

    /// strategy.go:90-109: only a spec change bumps the generation.
    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = replica_set();

        let mut annotated = old.clone();
        annotated.status = None;
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.status, old.status);
        assert_eq!(
            annotated.metadata.generation,
            Some(4),
            "annotations do not bump"
        );

        let mut scaled = old.clone();
        scaled.spec.replicas = 3;
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));
    }

    /// strategy.go:209-215: spec is kept, labels are not reset.
    #[test]
    fn status_prepare_for_update_keeps_spec_only() {
        let old = replica_set();
        let mut new = old.clone();
        new.spec.replicas = 9;
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.status = Some(ReplicaSetStatus {
            replicas: 2,
            ..ReplicaSetStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, 1);
        assert_eq!(new.metadata.labels.unwrap()["l"], "new");
        assert_eq!(new.status.unwrap().replicas, 2);
    }

    /// storage.go:266-288.
    #[test]
    fn scale_from_replica_set_maps_replicas_and_selector() {
        let rs = replica_set();
        let scale = scale_from_replica_set(&rs).unwrap();
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
        assert_eq!(scale.status.selector, "app=r");
        assert_eq!(scale.metadata.name, "r");
    }
}
