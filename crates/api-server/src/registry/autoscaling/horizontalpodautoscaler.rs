//! HorizontalPodAutoscaler strategy and storage — port of
//! `pkg/registry/autoscaling/horizontalpodautoscaler/strategy.go` and
//! `pkg/registry/autoscaling/horizontalpodautoscaler/storage/storage.go`.
//!
//! The feature gates the strategy consults are `HPAConfigurableTolerance`
//! (beta, on in 1.35) and `HPAScaleToZero` (alpha, off, no later entry in
//! `kube_features.go`). With the first on, `dropDisabledFields` returns before
//! it clears anything and is not modelled; with the second off,
//! `validationOptionsForHorizontalPodAutoscaler` only lowers the `minReplicas`
//! bound for an old object that already had 0, which
//! `rusternetes_common::validation::hpa` implements.

use std::sync::Arc;

use rusternetes_common::resources::{
    HPAScalingPolicy, HPAScalingRules, HorizontalPodAutoscaler, HorizontalPodAutoscalerStatus,
    MetricSpec, MetricTarget, ResourceMetricSource,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::hpa::{
    validate_horizontal_pod_autoscaler, validate_horizontal_pod_autoscaler_status_update,
    validate_horizontal_pod_autoscaler_update,
};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// `autoscaling.DefaultCPUUtilization` (pkg/apis/autoscaling/annotations.go:34).
const DEFAULT_CPU_UTILIZATION: i32 = 80;

fn default_scale_up_rules() -> HPAScalingRules {
    // defaultHPAScaleUpRules (pkg/apis/autoscaling/v2/defaults.go:30-47).
    HPAScalingRules {
        stabilization_window_seconds: Some(0),
        select_policy: Some("Max".to_string()),
        policies: Some(vec![
            HPAScalingPolicy {
                policy_type: "Pods".to_string(),
                value: 4,
                period_seconds: 15,
            },
            HPAScalingPolicy {
                policy_type: "Percent".to_string(),
                value: 100,
                period_seconds: 15,
            },
        ]),
        tolerance: None,
    }
}

fn default_scale_down_rules() -> HPAScalingRules {
    // defaultHPAScaleDownRules (defaults.go:48-62): the stabilization window
    // stays unset, the controller's flag supplies it.
    HPAScalingRules {
        stabilization_window_seconds: None,
        select_policy: Some("Max".to_string()),
        policies: Some(vec![HPAScalingPolicy {
            policy_type: "Percent".to_string(),
            value: 100,
            period_seconds: 15,
        }]),
        tolerance: None,
    }
}

/// `copyHPAScalingRules` (defaults.go:121-137): every non-nil field of `from`
/// overrides the default in `to`.
fn copy_scaling_rules(from: Option<&HPAScalingRules>, mut to: HPAScalingRules) -> HPAScalingRules {
    let Some(from) = from else { return to };
    if from.select_policy.is_some() {
        to.select_policy = from.select_policy.clone();
    }
    if from.stabilization_window_seconds.is_some() {
        to.stabilization_window_seconds = from.stabilization_window_seconds;
    }
    if from.policies.is_some() {
        to.policies = from.policies.clone();
    }
    if from.tolerance.is_some() {
        to.tolerance = from.tolerance.clone();
    }
    to
}

/// The autoscaling/v2 defaulting a decoded HorizontalPodAutoscaler goes
/// through: `SetDefaults_HorizontalPodAutoscaler`
/// (pkg/apis/autoscaling/v2/defaults.go:64-100). Runs on create and update.
pub fn convert_to_internal(hpa: &mut HorizontalPodAutoscaler) {
    let spec = &mut hpa.spec;
    spec.min_replicas.get_or_insert(1);

    if spec.metrics.as_ref().is_none_or(|m| m.is_empty()) {
        spec.metrics = Some(vec![MetricSpec {
            metric_type: "Resource".to_string(),
            resource: Some(ResourceMetricSource {
                name: "cpu".to_string(),
                target: MetricTarget {
                    target_type: "Utilization".to_string(),
                    value: None,
                    average_value: None,
                    average_utilization: Some(DEFAULT_CPU_UTILIZATION),
                },
            }),
            pods: None,
            object: None,
            external: None,
            container_resource: None,
        }]);
    }

    // SetDefaults_HorizontalPodAutoscalerBehavior: a behavior with any rule
    // gets both directions fully populated.
    if let Some(behavior) = spec.behavior.as_mut() {
        behavior.scale_up = Some(copy_scaling_rules(
            behavior.scale_up.as_ref(),
            default_scale_up_rules(),
        ));
        behavior.scale_down = Some(copy_scaling_rules(
            behavior.scale_down.as_ref(),
            default_scale_down_rules(),
        ));
    }
}

/// `ValidateHorizontalPodAutoscaler` (validation.go:119-123): ObjectMeta with
/// `NameIsDNSSubdomain`, then the spec.
fn validate(obj: &HorizontalPodAutoscaler) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_horizontal_pod_autoscaler(obj));
    errs
}

/// `ValidateHorizontalPodAutoscalerUpdate` (validation.go:126-131).
fn validate_update(obj: &HorizontalPodAutoscaler, old: &HorizontalPodAutoscaler) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_horizontal_pod_autoscaler_update(obj, old));
    errs
}

/// A Go `autoscaling.HorizontalPodAutoscalerStatus{}`: the status is not a
/// pointer, so an empty one still serializes `currentReplicas` and
/// `desiredReplicas`.
fn empty_status() -> HorizontalPodAutoscalerStatus {
    HorizontalPodAutoscalerStatus {
        observed_generation: None,
        last_scale_time: None,
        current_replicas: 0,
        desired_replicas: 0,
        current_metrics: None,
        conditions: None,
    }
}

/// `autoscalerStrategy` (strategy.go:37-125).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<HorizontalPodAutoscaler> for Strategy {
    /// Create cannot set status (strategy.go:70-76).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut HorizontalPodAutoscaler) {
        obj.status = Some(empty_status());
    }

    fn validate(&self, _ctx: &RequestContext, obj: &HorizontalPodAutoscaler) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<HorizontalPodAutoscaler> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Update is not allowed to set status (strategy.go:101-108).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut HorizontalPodAutoscaler,
        old: &HorizontalPodAutoscaler,
    ) {
        obj.status = old.status.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &HorizontalPodAutoscaler,
        old: &HorizontalPodAutoscaler,
    ) -> ErrorList {
        validate_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<HorizontalPodAutoscaler> for Strategy {}

/// `autoscalerStatusStrategy` (strategy.go:127-158).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<HorizontalPodAutoscaler> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Status changes are not allowed to update spec (strategy.go:150-155);
    /// the metadata a status write cannot touch is kept as well.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut HorizontalPodAutoscaler,
        old: &HorizontalPodAutoscaler,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `ValidateHorizontalPodAutoscalerStatusUpdate` (validation.go:136-142).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &HorizontalPodAutoscaler,
        old: &HorizontalPodAutoscaler,
    ) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        errs.extend(validate_horizontal_pod_autoscaler_status_update(obj));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go): the main store and the status store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<HorizontalPodAutoscaler, StorageBackend>,
    Store<HorizontalPodAutoscaler, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("autoscaling", "horizontalpodautoscalers"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
