//! Deployment strategies and storage — port of
//! `pkg/registry/apps/deployment/strategy.go` and
//! `pkg/registry/apps/deployment/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::pod_warnings::get_warnings_for_pod_template;
use rusternetes_common::resources::{Deployment, DeploymentStatus, Scale, ScaleSpec, ScaleStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{
    validate_deployment, validate_deployment_status_update, validate_deployment_update,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use crate::registry::scale::{Scalable, ScaleRest};
use crate::ssa::ResetFields;

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
    /// strategy.go:72-79, including `DropDisabledTemplateFields` (:79).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Deployment) {
        obj.status = Some(DeploymentStatus::default());
        obj.metadata.generation = Some(1);
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            None,
        );
    }

    fn validate(&self, _ctx: &RequestContext, obj: &Deployment) -> ErrorList {
        validate_deployment(obj)
    }

    /// strategy.go:88-96: the name warning, then
    /// `GetWarningsForPodTemplate(spec.template)`.
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Deployment) -> Vec<String> {
        let msgs = is_dns1123_label(&obj.metadata.name);
        let mut warnings = Vec::new();
        if !msgs.is_empty() {
            warnings.push(format!(
                "metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: [{}]",
                msgs.join(" ")
            ));
        }
        warnings.extend(get_warnings_for_pod_template(
            &Path::new("spec").child("template"),
            &obj.spec.template,
            None,
        ));
        warnings
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
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.template,
            Some(&old.spec.template),
        );
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

    /// strategy.go:133-140: only a generation change (a spec or annotation
    /// change) re-evaluates the pod template.
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Deployment,
        old: &Deployment,
    ) -> Vec<String> {
        if obj.metadata.generation == old.metadata.generation {
            return Vec::new();
        }
        get_warnings_for_pod_template(
            &Path::new("spec").child("template"),
            &obj.spec.template,
            Some(&old.spec.template),
        )
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (strategy.go:63-71): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["status"]])
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
    /// drops `terminatingReplicas` when its gate is off.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Deployment, old: &Deployment) {
        obj.spec = old.spec.clone();
        obj.metadata.labels = old.metadata.labels.clone();
        drop_disabled_status_fields(&mut obj.status, &old.status);
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

    /// `GetResetFields` (strategy.go:161-168). `metadata.labels` is a deeper
    /// path than the apply engine's per-top-level-key ownership can exclude,
    /// so only `spec` takes effect there.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("apps/v1", &[&["spec"], &["metadata", "labels"]])
    }
}

/// `NewREST` (storage/storage.go:91-112): the Deployment store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Deployment, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apps", "deployments"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
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

/// `dropDisabledStatusFields` (pkg/registry/apps/deployment/strategy.go): with
/// `DeploymentReplicaSetTerminatingReplicas` off, `status.terminatingReplicas`
/// is dropped unless the old status already carries it.
fn drop_disabled_status_fields(
    status: &mut Option<DeploymentStatus>,
    old: &Option<DeploymentStatus>,
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
