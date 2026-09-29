//! Deployment strategies and storage — port of
//! `pkg/registry/apps/deployment/strategy.go` and
//! `pkg/registry/apps/deployment/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::{Deployment, DeploymentStatus, Scale, ScaleSpec, ScaleStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_deployment, validate_deployment_status_update, validate_deployment_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_storage::StorageBackend;
use serde::Serialize;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};

/// `apiequality.Semantic.DeepEqual` over serialized values: unlike
/// `reflect.DeepEqual` it treats nil and empty slices and maps as equal
/// (third_party/forked/golang/reflect/deep_equal.go). Serialized, "nil" is an
/// absent key or `null`, so both sides drop those and empty containers first.
fn semantic_equal<A: Serialize>(a: &A, b: &A) -> bool {
    fn normalize(v: &mut serde_json::Value) -> bool {
        use serde_json::Value;
        match v {
            Value::Null => true,
            Value::Object(map) => {
                map.retain(|_, child| !normalize(child));
                map.is_empty()
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    normalize(item);
                }
                items.is_empty()
            }
            _ => false,
        }
    }
    let normalized = |v: &A| {
        let mut v = serde_json::to_value(v).unwrap_or_default();
        if normalize(&mut v) {
            v = serde_json::Value::Null;
        }
        v
    };
    normalized(a) == normalized(b)
}

/// The v1 defaulting a decoded Deployment goes through: `SetDefaults_Deployment`
/// (pkg/apis/apps/v1/defaults.go:38-73) and the pod template's defaults.
pub fn convert_to_internal(deployment: &mut Deployment) {
    crate::handlers::defaults::apply_deployment_defaults(deployment);
}

/// `deploymentStrategy` (strategy.go:38-46).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Deployment> for Strategy {
    /// strategy.go:72-79. `DropDisabledTemplateFields` drops nothing we
    /// model: every gated pod field it covers is either on by default in 1.35
    /// or absent from our types.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Deployment) {
        obj.status = Some(DeploymentStatus::default());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &Deployment) -> ErrorList {
        validate_deployment(obj)
    }

    /// strategy.go:88-96. `GetWarningsForPodTemplate` is not ported (#1990
    /// follow-up); the name warning is.
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Deployment) -> Vec<String> {
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

impl RestUpdateStrategy<Deployment> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:108-123: status is kept, and a change to the spec or the
    /// annotations — which are copied onto the ReplicaSets — bumps the
    /// generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Deployment, old: &Deployment) {
        obj.status = old.status.clone();
        if !semantic_equal(&obj.spec, &old.spec)
            || !semantic_equal(&obj.metadata.annotations, &old.metadata.annotations)
        {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Deployment,
        old: &Deployment,
    ) -> ErrorList {
        validate_deployment_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// strategy.go:51-54.
impl RestDeleteStrategy<Deployment> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::DeleteDependents)
    }
}

/// `deploymentStatusStrategy` (strategy.go:142-185): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<Deployment> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:162-169: only status may change. `dropDisabledStatusFields`
    /// is a no-op, as its `DeploymentReplicaSetTerminatingReplicas` gate is on
    /// by default in 1.35 (pkg/features/kube_features.go:1270-1273).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Deployment, old: &Deployment) {
        obj.spec = old.spec.clone();
        obj.metadata.labels = old.metadata.labels.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Deployment,
        old: &Deployment,
    ) -> ErrorList {
        validate_deployment_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:91-112): the Deployment store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Deployment, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "deployments"),
        Arc::new(Strategy),
    )
}

/// The `/status` store: the Deployment store updating with
/// [`StatusStrategy`] (storage/storage.go:109-111).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Deployment, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// `scaleFromDeployment` (storage/storage.go:381-406).
pub fn scale_from_deployment(deployment: &Deployment) -> Result<Scale, String> {
    let selector = deployment.spec.selector.as_selector_string()?;
    let meta = &deployment.metadata;
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
            replicas: deployment.spec.replicas.unwrap_or(0),
        },
        status: ScaleStatus {
            replicas: deployment
                .status
                .as_ref()
                .and_then(|s| s.replicas)
                .unwrap_or(0),
            selector,
        },
    })
}

/// `ScaleREST{store: deploymentRest.Store}` (storage/storage.go:80-83).
pub fn new_scale_rest(storage: Arc<StorageBackend>) -> ScaleRest<Deployment> {
    ScaleRest::new(
        new_store(storage),
        Scalable {
            to_scale: scale_from_deployment,
            set_replicas: |deployment, replicas| deployment.spec.replicas = Some(replicas),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn deployment() -> Deployment {
        let mut d: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "d", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "d"}},
                "template": {
                    "metadata": {"labels": {"app": "d"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut d);
        d
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

    /// strategy.go:72-79.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut d = deployment();
        Strategy.prepare_for_create(&ctx(), &mut d);
        assert_eq!(d.status, Some(DeploymentStatus::default()));
        assert_eq!(d.metadata.generation, Some(1));
    }

    /// strategy.go:108-123, the cases of upstream `TestStatusUpdates` and
    /// the generation rules.
    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec_or_annotations() {
        let old = deployment();

        let mut same = old.clone();
        same.status = None;
        same.metadata.labels = Some(HashMap::from([("l".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut same, &old);
        assert_eq!(same.status, old.status);
        assert_eq!(same.metadata.generation, Some(4), "labels do not bump");

        let mut scaled = old.clone();
        scaled.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));

        let mut annotated = old.clone();
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.metadata.generation, Some(5));

        // Semantic equality: an empty map is no change.
        let mut empty = old.clone();
        empty.metadata.annotations = Some(HashMap::new());
        Strategy.prepare_for_update(&ctx(), &mut empty, &old);
        assert_eq!(empty.metadata.generation, Some(4));
    }

    /// strategy.go:162-169.
    #[test]
    fn status_prepare_for_update_keeps_spec_and_labels() {
        let mut old = deployment();
        old.metadata.labels = Some(HashMap::from([("l".into(), "old".into())]));
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.metadata.annotations = Some(HashMap::from([("a".into(), "kept".into())]));
        new.status = Some(DeploymentStatus {
            replicas: Some(2),
            ..DeploymentStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, Some(1));
        assert_eq!(new.metadata.labels, old.metadata.labels);
        assert_eq!(new.metadata.annotations.unwrap()["a"], "kept");
        assert_eq!(new.status.unwrap().replicas, Some(2));
    }

    #[test]
    fn a_name_that_is_not_a_dns_label_warns() {
        let mut d = deployment();
        assert!(Strategy.warnings_on_create(&ctx(), &d).is_empty());
        d.metadata.name = "a.b".to_string();
        let warnings = Strategy.warnings_on_create(&ctx(), &d);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].starts_with("metadata.name: this is used in Pod names and hostnames"),
            "{warnings:?}"
        );
    }
}
