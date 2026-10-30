//! `VolumeBindingArgs` scheduler plugin configuration: defaulting, validation
//! and the shape -> scorer-shape conversion.
//!
//! Ported from:
//! - `pkg/scheduler/apis/config/v1/defaults.go:192-208` (`SetDefaults_VolumeBindingArgs`)
//! - `pkg/scheduler/apis/config/validation/validation_pluginargs.go:176-211`
//!   (`validateFunctionShape`) and `:266-290` (`ValidateVolumeBindingArgs*`)
//! - `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go:642-653`
//!   (shape scaled by `MaxNodeScore / MaxCustomPriorityScore`).

use crate::framework_runtime::{default_plugins, merge_plugins, PluginSet, Plugins};
use crate::volume_binding::{FunctionShapePoint, MAX_CUSTOM_PRIORITY_SCORE, MAX_NODE_SCORE};
use rusternetes_common::go_duration::format_go_duration;
use rusternetes_common::label_selector::{LabelOperator, LabelRequirement};
use rusternetes_common::quota::is_extended_resource_name;
use rusternetes_common::resources::pod::{
    NodeAffinity, NodeSelectorRequirement, NodeSelectorTerm, TopologySpreadConstraint,
};
use rusternetes_common::validation::field::{self, BadValue, ErrorList, Path};
use rusternetes_common::validation::metav1::{is_qualified_name, validate_label_name};
use serde::{Deserialize, Serialize};

/// `config.UtilizationShapePoint` (pkg/scheduler/apis/config/types.go).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UtilizationShapePoint {
    pub utilization: i32,
    pub score: i32,
}

/// `config.VolumeBindingArgs` / `configv1.VolumeBindingArgs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeBindingArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_timeout_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<UtilizationShapePoint>>,
}

/// `DefaultBindTimeoutSeconds` (v1/defaults.go:193-194).
const DEFAULT_BIND_TIMEOUT_SECONDS: i64 = 600;

impl VolumeBindingArgs {
    /// `SetDefaults_VolumeBindingArgs` (v1/defaults.go:192-208).
    pub fn set_defaults(&mut self, storage_capacity_scoring: bool) {
        if self.bind_timeout_seconds.is_none() {
            self.bind_timeout_seconds = Some(DEFAULT_BIND_TIMEOUT_SECONDS);
        }
        if self.shape.as_ref().is_none_or(|s| s.is_empty()) && storage_capacity_scoring {
            self.shape = Some(vec![
                UtilizationShapePoint {
                    utilization: 0,
                    score: MAX_CUSTOM_PRIORITY_SCORE as i32,
                },
                UtilizationShapePoint {
                    utilization: 100,
                    score: 0,
                },
            ]);
        }
    }

    /// `ValidateVolumeBindingArgsWithOptions` (validation_pluginargs.go:274-290).
    pub fn validate(&self, path: &Path, allow_storage_capacity_scoring: bool) -> ErrorList {
        let mut errs = ErrorList::new();
        let timeout = self.bind_timeout_seconds.unwrap_or(0);
        if timeout < 0 {
            errs.push(field::Error::invalid(
                &path.child("bindTimeoutSeconds"),
                timeout,
                "invalid BindTimeoutSeconds, should not be a negative value",
            ));
        }
        if allow_storage_capacity_scoring {
            errs.extend(validate_function_shape(
                self.shape.as_deref().unwrap_or(&[]),
                &path.child("shape"),
            ));
        } else if let Some(shape) = &self.shape {
            // When the feature is off, return an error if the config is not nil.
            // This prevents unexpected configuration from taking effect when the
            // feature turns on in the future.
            errs.push(field::Error::invalid(
                &path.child("shape"),
                field::BadValue::marshal(shape),
                "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring",
            ));
        }
        errs
    }

    /// volume_binding.go:642-653: the scorer shape, each score scaled by
    /// `MaxNodeScore / MaxCustomPriorityScore`.
    pub fn scorer_shape(&self) -> Vec<FunctionShapePoint> {
        self.shape
            .iter()
            .flatten()
            .map(|p| FunctionShapePoint {
                utilization: p.utilization as i64,
                score: p.score as i64 * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
            })
            .collect()
    }
}

/// `config.DefaultPreemptionArgs` / `configv1.DefaultPreemptionArgs`
/// (pkg/scheduler/apis/config/types_pluginargs.go).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DefaultPreemptionArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_candidate_nodes_percentage: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_candidate_nodes_absolute: Option<i32>,
}

impl DefaultPreemptionArgs {
    /// `SetDefaults_DefaultPreemptionArgs` (v1/defaults.go:177-184).
    pub fn set_defaults(&mut self) {
        self.min_candidate_nodes_percentage.get_or_insert(10);
        self.min_candidate_nodes_absolute.get_or_insert(100);
    }

    /// `ValidateDefaultPreemptionArgs` (validation_pluginargs.go:41-57), with
    /// `validateMinCandidateNodesPercentage` (:61-66) and
    /// `validateMinCandidateNodesAbsolute` (:70-75). Run after `set_defaults`.
    pub fn validate(&self, path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();
        let pct = self.min_candidate_nodes_percentage.unwrap_or(0);
        let abs = self.min_candidate_nodes_absolute.unwrap_or(0);
        let pct_path = path.child("minCandidateNodesPercentage");
        let abs_path = path.child("minCandidateNodesAbsolute");
        if !(0..=100).contains(&pct) {
            errs.push(field::Error::invalid(
                &pct_path,
                pct,
                "not in valid range [0, 100]",
            ));
        }
        if abs < 0 {
            errs.push(field::Error::invalid(
                &abs_path,
                abs,
                "not in valid range [0, inf)",
            ));
        }
        if pct == 0 && abs == 0 {
            errs.push(field::Error::invalid(
                &pct_path,
                pct,
                "cannot be zero at the same time as minCandidateNodesAbsolute",
            ));
            errs.push(field::Error::invalid(
                &abs_path,
                abs,
                "cannot be zero at the same time as minCandidateNodesPercentage",
            ));
        }
        errs
    }
}

/// `config.InterPodAffinityArgs` / `configv1.InterPodAffinityArgs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InterPodAffinityArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_pod_affinity_weight: Option<i32>,
    /// `IgnorePreferredTermsOfExistingPods` (types_pluginargs.go:56-58):
    /// ignore existing pods' preferred affinity rules when the incoming pod
    /// has no inter-pod affinity terms of its own.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_preferred_terms_of_existing_pods: bool,
}

impl InterPodAffinityArgs {
    /// `SetDefaults_InterPodAffinityArgs` (v1/defaults.go:186-190).
    pub fn set_defaults(&mut self) {
        self.hard_pod_affinity_weight.get_or_insert(1);
    }

    /// `ValidateInterPodAffinityArgs` / `validateHardPodAffinityWeight`
    /// (validation_pluginargs.go:88-104). Run after `set_defaults`.
    pub fn validate(&self, path: &Path) -> ErrorList {
        let w = self.hard_pod_affinity_weight.unwrap_or(0);
        if (0..=100).contains(&w) {
            ErrorList::new()
        } else {
            vec![field::Error::invalid(
                &path.child("hardPodAffinityWeight"),
                w,
                "not in valid range [0, 100]",
            )]
        }
    }
}

/// `configv1.ResourceSpec` / `config.ResourceSpec`
/// (staging/src/k8s.io/kube-scheduler/config/v1/types_pluginargs.go).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceSpec {
    pub name: String,
    #[serde(default)]
    pub weight: i64,
}

/// `defaultResourceSpec` (v1/defaults.go:33-36).
pub fn default_resource_spec() -> Vec<ResourceSpec> {
    vec![
        ResourceSpec {
            name: "cpu".to_string(),
            weight: 1,
        },
        ResourceSpec {
            name: "memory".to_string(),
            weight: 1,
        },
    ]
}

/// `config.NodeResourcesBalancedAllocationArgs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeResourcesBalancedAllocationArgs {
    #[serde(default)]
    pub resources: Vec<ResourceSpec>,
}

impl NodeResourcesBalancedAllocationArgs {
    /// `SetDefaults_NodeResourcesBalancedAllocationArgs` (v1/defaults.go:210-221).
    pub fn set_defaults(&mut self) {
        if self.resources.is_empty() {
            self.resources = default_resource_spec();
            return;
        }
        // If the weight is not set or it is explicitly set to 0, then apply the
        // default weight(1) instead.
        for r in &mut self.resources {
            if r.weight == 0 {
                r.weight = 1;
            }
        }
    }

    /// `ValidateNodeResourcesBalancedAllocationArgs`
    /// (validation_pluginargs.go:224-238). Run after `set_defaults`.
    pub fn validate(&self, path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();
        let mut seen: Vec<&str> = Vec::new();
        for (i, r) in self.resources.iter().enumerate() {
            let rp = path.child("resources").index(i);
            if seen.contains(&r.name.as_str()) {
                errs.push(field::Error::duplicate(&rp.child("name"), r.name.as_str()));
            } else {
                seen.push(&r.name);
            }
            if r.weight != 1 {
                errs.push(field::Error::invalid(
                    &rp.child("weight"),
                    r.weight,
                    "must be 1",
                ));
            }
        }
        errs
    }
}

/// `config.PodTopologySpreadArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodTopologySpreadArgs {
    #[serde(default)]
    pub default_constraints: Vec<TopologySpreadConstraint>,
    #[serde(default)]
    pub defaulting_type: String,
}

impl PodTopologySpreadArgs {
    /// `SetDefaults_PodTopologySpreadArgs` (v1/defaults.go:223-227).
    pub fn set_defaults(&mut self) {
        if self.defaulting_type.is_empty() {
            self.defaulting_type = "System".to_string();
        }
    }

    /// `ValidatePodTopologySpreadArgs` (validation_pluginargs.go:99-129) with
    /// `validateDefaultingType` (:132-140), `validateTopologyKey` (:142-149),
    /// `validateWhenUnsatisfiable` (:151-161) and `validateConstraintNotRepeat`
    /// (:163-171). Run after `set_defaults`.
    pub fn validate(&self, path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();
        let dt_path = path.child("defaultingType");
        let dt = self.defaulting_type.as_str();
        if dt != "System" && dt != "List" {
            errs.push(field::Error::not_supported(
                &dt_path,
                dt,
                &["System", "List"],
            ));
        } else if dt == "System" && !self.default_constraints.is_empty() {
            errs.push(field::Error::invalid(
                &dt_path,
                dt,
                "when .defaultConstraints are not empty",
            ));
        }
        let constraints_path = path.child("defaultConstraints");
        for (i, c) in self.default_constraints.iter().enumerate() {
            let p = constraints_path.index(i);
            if c.max_skew <= 0 {
                errs.push(field::Error::invalid(
                    &p.child("maxSkew"),
                    c.max_skew,
                    "not in valid range (0, inf)",
                ));
            }
            let tk = p.child("topologyKey");
            if c.topology_key.is_empty() {
                errs.push(field::Error::required(&tk, "can not be empty"));
            } else {
                errs.extend(validate_label_name(&c.topology_key, &tk));
            }
            let wu = p.child("whenUnsatisfiable");
            if c.when_unsatisfiable.is_empty() {
                errs.push(field::Error::required(&wu, "can not be empty"));
            } else if c.when_unsatisfiable != "DoNotSchedule"
                && c.when_unsatisfiable != "ScheduleAnyway"
            {
                errs.push(field::Error::not_supported(
                    &wu,
                    c.when_unsatisfiable.as_str(),
                    &["DoNotSchedule", "ScheduleAnyway"],
                ));
            }
            if c.label_selector.is_some() {
                errs.push(field::Error::forbidden(
                    &p.child("labelSelector"),
                    "constraint must not define a selector, as they deduced for each pod",
                ));
            }
            if self.default_constraints[..i].iter().any(|o| {
                o.topology_key == c.topology_key && o.when_unsatisfiable == c.when_unsatisfiable
            }) {
                errs.push(field::Error::duplicate(
                    &constraints_path.index(i),
                    format!("{{{}, {}}}", c.topology_key, c.when_unsatisfiable),
                ));
            }
        }
        errs
    }
}

/// `config.NodeAffinityArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAffinityArgs {
    #[serde(default)]
    pub added_affinity: Option<NodeAffinity>,
}

impl NodeAffinityArgs {
    /// `ValidateNodeAffinityArgs` (validation_pluginargs.go:241-259), which
    /// runs `nodeaffinity.NewNodeSelector` / `NewPreferredSchedulingTerms`
    /// (component-helpers/scheduling/corev1/nodeaffinity/nodeaffinity.go:40-55,
    /// :112-131) over the added affinity and reports their parse errors. There
    /// is no `SetDefaults_NodeAffinityArgs` upstream.
    pub fn validate(&self, path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();
        let Some(affinity) = &self.added_affinity else {
            return errs;
        };
        if let Some(ns) = &affinity.required_during_scheduling_ignored_during_execution {
            let p = path
                .child("addedAffinity")
                .child("requiredDuringSchedulingIgnoredDuringExecution")
                .child("nodeSelectorTerms");
            for (i, term) in ns.node_selector_terms.iter().enumerate() {
                // nil or empty term selects no objects (nodeaffinity.go:60-63).
                if is_empty_node_selector_term(term) {
                    continue;
                }
                errs.extend(node_selector_term_errors(term, &p.index(i)));
            }
        }
        if let Some(terms) = &affinity.preferred_during_scheduling_ignored_during_execution {
            let p = path
                .child("addedAffinity")
                .child("preferredDuringSchedulingIgnoredDuringExecution");
            for (i, term) in terms.iter().enumerate() {
                // nodeaffinity.go:119: a 0 weight or empty term skips parsing.
                if term.weight == 0 || is_empty_node_selector_term(&term.preference) {
                    continue;
                }
                errs.extend(node_selector_term_errors(&term.preference, &p.index(i)));
            }
        }
        errs
    }
}

/// `isEmptyNodeSelectorTerm` (nodeaffinity.go:135-137).
fn is_empty_node_selector_term(term: &NodeSelectorTerm) -> bool {
    term.match_expressions.as_ref().is_none_or(|v| v.is_empty())
        && term.match_fields.as_ref().is_none_or(|v| v.is_empty())
}

/// `newNodeSelectorTerm`'s `parseErrs` (nodeaffinity.go:170-188).
fn node_selector_term_errors(term: &NodeSelectorTerm, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if let Some(exprs) = term.match_expressions.as_ref().filter(|v| !v.is_empty()) {
        errs.extend(requirements_as_selector_errors(
            exprs,
            &path.child("matchExpressions"),
        ));
    }
    if let Some(fields) = term.match_fields.as_ref().filter(|v| !v.is_empty()) {
        errs.extend(requirements_as_field_selector_errors(
            fields,
            &path.child("matchFields"),
        ));
    }
    errs
}

/// `nodeSelectorRequirementsAsSelector` errors (nodeaffinity.go:214-257).
fn requirements_as_selector_errors(reqs: &[NodeSelectorRequirement], path: &Path) -> ErrorList {
    const VALID: [&str; 6] = ["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"];
    let mut errs = ErrorList::new();
    for (i, expr) in reqs.iter().enumerate() {
        let p = path.index(i);
        let op = match expr.operator.as_str() {
            "In" => LabelOperator::In,
            "NotIn" => LabelOperator::NotIn,
            "Exists" => LabelOperator::Exists,
            "DoesNotExist" => LabelOperator::DoesNotExist,
            "Gt" => LabelOperator::GreaterThan,
            "Lt" => LabelOperator::LessThan,
            other => {
                errs.push(field::Error::not_supported(
                    &p.child("operator"),
                    other,
                    &VALID,
                ));
                continue;
            }
        };
        errs.extend(LabelRequirement::validate_at(
            &p,
            &expr.key,
            op,
            expr.values.as_deref().unwrap_or(&[]),
        ));
    }
    errs
}

/// `nodeSelectorRequirementsAsFieldSelector` errors (nodeaffinity.go:260-297).
fn requirements_as_field_selector_errors(
    reqs: &[NodeSelectorRequirement],
    path: &Path,
) -> ErrorList {
    const VALID: [&str; 2] = ["In", "NotIn"];
    let mut errs = ErrorList::new();
    for (i, expr) in reqs.iter().enumerate() {
        let p = path.index(i);
        match expr.operator.as_str() {
            "In" | "NotIn" => {
                if expr.values.as_ref().is_none_or(|v| v.len() != 1) {
                    // A nil Go slice marshals to `null`, an empty one to `[]`.
                    let bad = match &expr.values {
                        Some(v) => BadValue::from(v.clone()),
                        None => BadValue::Json(serde_json::Value::Null),
                    };
                    errs.push(field::Error::invalid(
                        &p.child("values"),
                        bad,
                        "must have one element",
                    ));
                }
            }
            other => errs.push(field::Error::not_supported(
                &p.child("operator"),
                other,
                &VALID,
            )),
        }
    }
    errs
}

/// `config.ScoringStrategy`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScoringStrategy {
    #[serde(default, rename = "type")]
    pub strategy_type: String,
    #[serde(default)]
    pub resources: Vec<ResourceSpec>,
    #[serde(default)]
    pub requested_to_capacity_ratio: Option<RequestedToCapacityRatioParam>,
}

/// `config.RequestedToCapacityRatioParam`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestedToCapacityRatioParam {
    #[serde(default)]
    pub shape: Vec<UtilizationShapePoint>,
}

/// `config.NodeResourcesFitArgs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeResourcesFitArgs {
    #[serde(default)]
    pub ignored_resources: Vec<String>,
    #[serde(default)]
    pub ignored_resource_groups: Vec<String>,
    #[serde(default)]
    pub scoring_strategy: Option<ScoringStrategy>,
}

impl NodeResourcesFitArgs {
    /// `SetDefaults_NodeResourcesFitArgs` (v1/defaults.go:229-244). Note the
    /// strategy `type` is only defaulted together with a nil `scoringStrategy`.
    pub fn set_defaults(&mut self) {
        let strategy = self
            .scoring_strategy
            .get_or_insert_with(|| ScoringStrategy {
                strategy_type: "LeastAllocated".to_string(),
                resources: default_resource_spec(),
                requested_to_capacity_ratio: None,
            });
        if strategy.resources.is_empty() {
            // If no resources specified, use the default set.
            strategy.resources.extend(default_resource_spec());
        }
        for r in &mut strategy.resources {
            if r.weight == 0 {
                r.weight = 1;
            }
        }
    }

    /// `ValidateNodeResourcesFitArgs` (validation_pluginargs.go:295-336) with
    /// `validateResources` (:213-222).
    pub fn validate(&self, path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();
        let res_path = path.child("ignoredResources");
        for (i, res) in self.ignored_resources.iter().enumerate() {
            errs.extend(validate_label_name(res, &res_path.index(i)));
        }
        let group_path = path.child("ignoredResourceGroups");
        for (i, group) in self.ignored_resource_groups.iter().enumerate() {
            let p = group_path.index(i);
            if group.contains('/') {
                errs.push(field::Error::invalid(
                    &p,
                    group.as_str(),
                    "resource group name can't contain '/'",
                ));
            }
            errs.extend(validate_label_name(group, &p));
        }
        let strategy_path = path.child("scoringStrategy");
        if let Some(s) = &self.scoring_strategy {
            // supportedScoringStrategyTypes, as `sets.List` (sorted).
            const SUPPORTED: [&str; 3] = [
                "LeastAllocated",
                "MostAllocated",
                "RequestedToCapacityRatio",
            ];
            if !SUPPORTED.contains(&s.strategy_type.as_str()) {
                errs.push(field::Error::not_supported(
                    &strategy_path.child("type"),
                    s.strategy_type.as_str(),
                    &SUPPORTED,
                ));
            }
            let resources_path = strategy_path.child("resources");
            for (i, r) in s.resources.iter().enumerate() {
                if r.weight <= 0 || r.weight > 100 {
                    errs.push(field::Error::invalid(
                        &resources_path.index(i).child("weight"),
                        r.weight,
                        format!("resource weight of {} not in valid range (0, 100]", r.name),
                    ));
                }
            }
            if let Some(rtcr) = &s.requested_to_capacity_ratio {
                errs.extend(validate_function_shape(
                    &rtcr.shape,
                    &strategy_path.child("shape"),
                ));
            }
        }
        errs
    }
}

/// `metav1.Duration` as signed Go nanoseconds: JSON string parsed by
/// `time.ParseDuration` (apimachinery meta/v1/duration.go `UnmarshalJSON`).
/// Signed because validation must see negative values.
mod metav1_duration_nanos {
    use rusternetes_common::go_duration::{format_go_duration, parse_go_duration};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(n) => s.serialize_str(&format_go_duration(*n)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        match Option::<String>::deserialize(d)? {
            Some(s) => parse_go_duration(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// `configv1.DynamicResourcesFilterTimeoutDefault` (kube-scheduler/config/v1/types.go:463).
const DYNAMIC_RESOURCES_FILTER_TIMEOUT_DEFAULT: i64 = 10 * NANOS_PER_SECOND;
/// `configv1.DynamicResourcesBindingTimeoutDefault` (kube-scheduler/config/v1/types.go:464).
const DYNAMIC_RESOURCES_BINDING_TIMEOUT_DEFAULT: i64 = 600 * NANOS_PER_SECOND;

/// The scheduler features `ValidateDynamicResourcesArgs` and
/// `SetDefaults_DynamicResourcesArgs` consult (`feature.Features` /
/// `feature.DefaultFeatureGate`), as nanosecond-free plain flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DynamicResourcesFeatures {
    pub dra_scheduler_filter_timeout: bool,
    pub dra_device_binding_conditions: bool,
    pub dra_resource_claim_device_status: bool,
}

impl DynamicResourcesFeatures {
    /// `NewSchedulerFeaturesFromGates(feature.DefaultFeatureGate)` (validation.go:162)
    /// for the three gates used here.
    pub fn from_gates() -> Self {
        use rusternetes_common::feature_gates::{enabled, Feature};
        Self {
            dra_scheduler_filter_timeout: enabled(Feature::DRASchedulerFilterTimeout),
            dra_device_binding_conditions: enabled(Feature::DRADeviceBindingConditions),
            dra_resource_claim_device_status: enabled(Feature::DRAResourceClaimDeviceStatus),
        }
    }
}

/// `config.DynamicResourcesArgs` (types_pluginargs.go); timeouts are Go
/// nanoseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DynamicResourcesArgs {
    #[serde(default, with = "metav1_duration_nanos")]
    pub filter_timeout: Option<i64>,
    #[serde(default, with = "metav1_duration_nanos")]
    pub binding_timeout: Option<i64>,
}

impl DynamicResourcesArgs {
    /// `SetDefaults_DynamicResourcesArgs` (v1/defaults.go:247-257): the filter
    /// timeout defaults only with `DRASchedulerFilterTimeout`, the binding
    /// timeout only with both `DRADeviceBindingConditions` and
    /// `DRAResourceClaimDeviceStatus`.
    pub fn set_defaults(&mut self, fts: DynamicResourcesFeatures) {
        if self.filter_timeout.is_none() && fts.dra_scheduler_filter_timeout {
            self.filter_timeout = Some(DYNAMIC_RESOURCES_FILTER_TIMEOUT_DEFAULT);
        }
        if self.binding_timeout.is_none()
            && fts.dra_device_binding_conditions
            && fts.dra_resource_claim_device_status
        {
            self.binding_timeout = Some(DYNAMIC_RESOURCES_BINDING_TIMEOUT_DEFAULT);
        }
    }

    /// `ValidateDynamicResourcesArgs` (validation_pluginargs.go:336-363).
    /// Unlike the REST API, setting a field whose feature is disabled is an
    /// error. The bad value is a `*metav1.Duration`, marshalled as its
    /// `Duration.String()` JSON string.
    pub fn validate(&self, path: &Path, fts: DynamicResourcesFeatures) -> ErrorList {
        let mut errs = ErrorList::new();
        let bad = |n: i64| BadValue::marshal(&format_go_duration(n));
        if fts.dra_scheduler_filter_timeout {
            if let Some(n) = self.filter_timeout.filter(|n| *n < 0) {
                errs.push(field::Error::invalid(
                    &path.child("filterTimeout"),
                    bad(n),
                    "must be zero or positive",
                ));
            }
        } else if self.filter_timeout.is_some() {
            errs.push(field::Error::forbidden(
                &path.child("filterTimeout"),
                "DRASchedulingFilterTimeout feature gate is disabled",
            ));
        }
        if fts.dra_device_binding_conditions && fts.dra_resource_claim_device_status {
            if let Some(n) = self.binding_timeout.filter(|n| *n < NANOS_PER_SECOND) {
                errs.push(field::Error::invalid(
                    &path.child("bindingTimeout"),
                    bad(n),
                    "must be at least 1 second",
                ));
            }
        } else if self.binding_timeout.is_some() {
            errs.push(field::Error::forbidden(
                &path.child("bindingTimeout"),
                "DRADeviceBindingConditions or DRAResourceClaimDeviceStatus feature gate is disabled",
            ));
        }
        errs
    }
}

/// `v1.SchemeGroupVersion.String()` (pkg/scheduler/apis/config/v1/register.go);
/// the only config version served, so it is both the removed-plugin version
/// and the configuration's version in `isPluginInvalid` messages.
const CONFIG_API_VERSION: &str = "kubescheduler.config.k8s.io/v1";

/// `invalidPluginsByVersion` for v1 (validation.go:104-114).
const INVALID_PLUGINS_V1: [&str; 4] = [
    "AzureDiskLimits",
    "CinderLimits",
    "EBSLimits",
    "GCEPDLimits",
];

/// The slice of `KubeSchedulerConfiguration` the scheduler reads
/// (`profiles[].pluginConfig[]`, pkg/scheduler/apis/config/types.go).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KubeSchedulerConfiguration {
    #[serde(default)]
    profiles: Vec<Profile>,
    #[serde(default)]
    percentage_of_nodes_to_score: Option<i32>,
    /// v1/defaults.go:108-110 defaults a nil value to 16.
    #[serde(default)]
    parallelism: Option<i32>,
    /// v1/defaults.go:158-160 defaults a nil value to 1.
    #[serde(default)]
    pod_initial_backoff_seconds: Option<i64>,
    /// v1/defaults.go:162-164 defaults a nil value to 10.
    #[serde(default)]
    pod_max_backoff_seconds: Option<i64>,
    /// `extenders` (kube-scheduler/config/v1/types.go:317 `Extender`). v1
    /// defaults.go sets nothing for extenders.
    #[serde(default)]
    extenders: Vec<Extender>,
}

/// The validated slice of `config.Extender` (kube-scheduler/config/v1/types.go:317).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Extender {
    #[serde(default)]
    prioritize_verb: String,
    #[serde(default)]
    weight: i64,
    #[serde(default)]
    bind_verb: String,
    #[serde(default)]
    managed_resources: Vec<ExtenderManagedResource>,
}

/// `config.ExtenderManagedResource` (kube-scheduler/config/v1/types.go:362).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtenderManagedResource {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    #[serde(default)]
    plugins: Option<Plugins>,
    #[serde(default)]
    scheduler_name: Option<String>,
    #[serde(default)]
    percentage_of_nodes_to_score: Option<i32>,
    #[serde(default)]
    plugin_config: Vec<PluginConfig>,
}

#[derive(Debug, Deserialize)]
struct PluginConfig {
    name: String,
    #[serde(default)]
    args: Option<serde_yaml::Value>,
}

/// Extract, default and validate the `VolumeBinding` plugin args from a
/// `KubeSchedulerConfiguration` document (YAML or JSON). `Ok(defaults)` when the
/// document has no `VolumeBinding` pluginConfig. `Err` carries the aggregated
/// validation errors, mirroring `ValidateVolumeBindingArgs` run per profile
/// (validation.go ValidateKubeSchedulerConfiguration; only the first profile is
/// consulted because Rusternetes runs a single `default-scheduler` profile).
pub fn volume_binding_args_from_config(
    doc: &str,
    storage_capacity_scoring: bool,
) -> Result<VolumeBindingArgs, String> {
    let (resolved, _) = resolve_profiles(doc, storage_capacity_scoring)?;
    let (_, args) = resolved
        .into_iter()
        .next()
        .expect("resolve_profiles always yields at least one profile");
    Ok(args)
}

/// Build the scheduling profiles of a `KubeSchedulerConfiguration` document
/// (`profile.NewMap` over `cfg.Profiles`, scheduler.go `New`): every profile is
/// defaulted and validated, then indexed by scheduler name.
pub fn profiles_from_config(
    doc: &str,
    storage_capacity_scoring: bool,
) -> Result<crate::profile::Map, String> {
    let (resolved, global_percentage) = resolve_profiles(doc, storage_capacity_scoring)?;
    let mut m = crate::profile::Map::new_map(resolved.into_iter().map(|(p, _)| p).collect())?;
    m.percentage_of_nodes_to_score = global_percentage;
    Ok(m)
}

/// Default + validate every profile (an empty list yields the one default
/// profile, v1/defaults.go:111-113), returning each with its resolved
/// `VolumeBindingArgs`, plus the global `percentageOfNodesToScore`
/// (v1/defaults.go:127-129 defaults a nil value to
/// `DefaultPercentageOfNodesToScore` = 0).
fn resolve_profiles(
    doc: &str,
    storage_capacity_scoring: bool,
) -> Result<(Vec<(crate::profile::Profile, VolumeBindingArgs)>, i32), String> {
    let mut cfg: KubeSchedulerConfiguration =
        serde_yaml::from_str(doc).map_err(|e| format!("invalid scheduler config: {e}"))?;
    let (mut errs, init_errs) = validate_profiles(&cfg)?;
    if cfg.profiles.is_empty() {
        cfg.profiles.push(Profile::default());
    }
    let single = cfg.profiles.len() == 1;
    let mut resolved = Vec::new();
    for (idx, profile) in cfg.profiles.iter().enumerate() {
        let mut args = VolumeBindingArgs::default();
        let mut path = Path::new("profiles").index(idx);
        for (i, pc) in profile.plugin_config.iter().enumerate() {
            if pc.name == "VolumeBinding" {
                path = path.child("pluginConfig").index(i).child("args");
                if let Some(v) = &pc.args {
                    args = serde_yaml::from_value(v.clone())
                        .map_err(|e| format!("{path}: invalid VolumeBindingArgs: {e}"))?;
                }
                break;
            }
        }
        args.set_defaults(storage_capacity_scoring);
        errs.extend(args.validate(&path, storage_capacity_scoring));
        // SetDefaults_KubeSchedulerConfiguration names only a lone profile.
        let name = match (&profile.scheduler_name, single) {
            (Some(n), _) => n.clone(),
            (None, true) => crate::profile::DEFAULT_SCHEDULER_NAME.to_string(),
            (None, false) => String::new(),
        };
        // v1 SetDefaults_KubeSchedulerProfile: merge the profile's plugins
        // over the default plugin set (defaults.go:72).
        let plugins = merge_plugins(default_plugins(), profile.plugins.as_ref());
        let plugin_config_names: Vec<String> = profile
            .plugin_config
            .iter()
            .map(|pc| pc.name.clone())
            .collect();
        let mut p = crate::profile::Profile::named(&name).with_volume_binding_args(&args);
        p.plugins = Some(plugins);
        p.plugin_config_names = plugin_config_names;
        p.percentage_of_nodes_to_score = profile.percentage_of_nodes_to_score;
        // Plugin args consumed at runtime (`New*` constructors): the
        // validated, defaulted args of the profile's pluginConfig entries.
        for (i, pc) in profile.plugin_config.iter().enumerate() {
            let args_path = Path::new("profiles")
                .index(idx)
                .child("pluginConfig")
                .index(i)
                .child("args");
            match pc.name.as_str() {
                "NodeAffinity" => {
                    let a: NodeAffinityArgs = decode_args(pc, &args_path)?;
                    p.added_affinity = a.added_affinity;
                }
                "NodeResourcesFit" => {
                    let mut a: NodeResourcesFitArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    if let Some(s) = crate::score::ResourceAllocationScorer::from_fit_args(&a) {
                        p.fit_scorer = s;
                    }
                }
                "NodeResourcesBalancedAllocation" => {
                    let mut a: NodeResourcesBalancedAllocationArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    p.balanced_scorer =
                        crate::score::ResourceAllocationScorer::balanced(a.resources);
                }
                "InterPodAffinity" => {
                    let mut a: InterPodAffinityArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    p.inter_pod_affinity = crate::inter_pod_affinity_score::Args {
                        hard_pod_affinity_weight: a.hard_pod_affinity_weight.unwrap_or(1),
                        ignore_preferred_terms_of_existing_pods: a
                            .ignore_preferred_terms_of_existing_pods,
                    };
                }
                "PodTopologySpread" => {
                    let mut a: PodTopologySpreadArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    p.pod_topology_spread = crate::pod_topology_spread_score::Args::from_config(&a);
                }
                "DefaultPreemption" => {
                    let mut a: DefaultPreemptionArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    p.min_candidate_nodes_percentage =
                        a.min_candidate_nodes_percentage.unwrap_or(10);
                    p.min_candidate_nodes_absolute = a.min_candidate_nodes_absolute.unwrap_or(100);
                }
                _ => {}
            }
        }
        // The queue sort plugin of the resolved framework; `Map::new_map`
        // reports a framework that fails to build.
        let queue_sort = crate::framework_runtime::new_framework(
            &crate::framework_runtime::Registry::in_tree(),
            &crate::framework_runtime::ProfileSpec {
                scheduler_name: &name,
                plugins: p.plugins.as_ref(),
                plugin_config_names: &p.plugin_config_names,
                percentage_of_nodes_to_score: p.percentage_of_nodes_to_score,
            },
        )
        .ok()
        .and_then(|f| {
            f.plugins_at(crate::framework_runtime::ExtensionPoint::QueueSort)
                .first()
                .cloned()
        })
        .unwrap_or_else(|| crate::profile::DEFAULT_QUEUE_SORT.to_string());
        // The last matching pluginConfig wins, as validateCommonQueueSort.
        p.queue_sort_args = profile
            .plugin_config
            .iter()
            .rfind(|pc| pc.name == queue_sort)
            .and_then(|pc| pc.args.as_ref())
            .and_then(|v| serde_json::to_value(v).ok());
        resolved.push((p, args));
    }
    if errs.is_empty() {
        errs = init_errs;
    }
    if errs.is_empty() {
        Ok((resolved, cfg.percentage_of_nodes_to_score.unwrap_or(0)))
    } else {
        Err(errs
            .iter()
            .map(|e| format!("{}: {}", e.field, e.error_body()))
            .collect::<Vec<_>>()
            .join(", "))
    }
}

/// Profile checks of `ValidateKubeSchedulerConfiguration` (validation.go:58-67)
/// and `validateKubeSchedulerProfile` (validation.go:141-149): required
/// `schedulerName`, `percentageOfNodesToScore` range, duplicate
/// `schedulerName`, and `validatePluginConfig` (validation.go:189-197,
/// `seenPluginConfig`). The default name is applied only to a single profile
/// (v1/defaults.go:116-119 SetDefaults_KubeSchedulerConfiguration), and an
/// empty profile list gets one default profile (:111-113).
fn validate_profiles(cfg: &KubeSchedulerConfiguration) -> Result<(ErrorList, ErrorList), String> {
    let profiles_path = Path::new("profiles");
    let mut errs = ErrorList::new();
    // Errors from plugin construction (`New*`), which only runs once the
    // configuration validated.
    let mut init_errs = ErrorList::new();
    // validation.go:52-54; nil defaults to 16 (v1/defaults.go:108-110).
    let parallelism = cfg.parallelism.unwrap_or(16);
    if parallelism <= 0 {
        errs.push(field::Error::invalid(
            &Path::new("parallelism"),
            parallelism,
            "should be an integer value greater than zero",
        ));
    }
    let single = cfg.profiles.len() <= 1;
    let mut existing: Vec<&str> = Vec::new();
    for (i, profile) in cfg.profiles.iter().enumerate() {
        let path = profiles_path.index(i);
        let name = match (&profile.scheduler_name, single) {
            (Some(n), _) => n.as_str(),
            (None, true) => "default-scheduler",
            (None, false) => "",
        };
        if name.is_empty() {
            errs.push(field::Error::required(&path.child("schedulerName"), ""));
        }
        errs.extend(validate_percentage_of_nodes_to_score(
            &path.child("percentageOfNodesToScore"),
            profile.percentage_of_nodes_to_score,
        ));
        // validatePluginConfig's stagesToPluginSet loop (validation.go:166-182).
        if let Some(pl) = &profile.plugins {
            let plugins_path = path.child("plugins");
            for (stage, set) in [
                ("preEnqueue", &pl.pre_enqueue),
                ("queueSort", &pl.queue_sort),
                ("preFilter", &pl.pre_filter),
                ("filter", &pl.filter),
                ("postFilter", &pl.post_filter),
                ("preScore", &pl.pre_score),
                ("score", &pl.score),
                ("reserve", &pl.reserve),
                ("permit", &pl.permit),
                ("preBind", &pl.pre_bind),
                ("bind", &pl.bind),
                ("postBind", &pl.post_bind),
            ] {
                // validatePluginSetForInvalidPlugins (validation.go:129-137).
                for (k, plugin) in set.enabled.iter().enumerate() {
                    if INVALID_PLUGINS_V1.contains(&plugin.name.as_str()) {
                        errs.push(field::Error::invalid(
                            &plugins_path.child(stage).child("enabled").index(k),
                            plugin.name.as_str(),
                            format!(
                                "was invalid in version \"{CONFIG_API_VERSION}\" (KubeSchedulerConfiguration is version \"{CONFIG_API_VERSION}\")"
                            ),
                        ));
                    }
                }
            }
        }
        let mut seen: Vec<&str> = Vec::new();
        for (j, pc) in profile.plugin_config.iter().enumerate() {
            let pc_path = path.child("pluginConfig").index(j);
            if seen.contains(&pc.name.as_str()) {
                errs.push(field::Error::duplicate(&pc_path, pc.name.as_str()));
            } else {
                seen.push(&pc.name);
            }
            if INVALID_PLUGINS_V1.contains(&pc.name.as_str()) {
                errs.push(field::Error::invalid(
                    &pc_path,
                    pc.name.as_str(),
                    format!(
                        "was invalid in version \"{CONFIG_API_VERSION}\" (KubeSchedulerConfiguration is version \"{CONFIG_API_VERSION}\")"
                    ),
                ));
                continue;
            }
            let args_path = pc_path.child("args");
            // The args are decoded by plugin name (scheme/scheme.go), so the
            // "has to match plugin args" type-mismatch branch
            // (validation.go:199-203) is unreachable here.
            match pc.name.as_str() {
                "DefaultPreemption" => {
                    let mut a: DefaultPreemptionArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    errs.extend(a.validate(&args_path));
                }
                "InterPodAffinity" => {
                    let mut a: InterPodAffinityArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    errs.extend(a.validate(&args_path));
                }
                "NodeAffinity" => {
                    let a: NodeAffinityArgs = decode_args(pc, &args_path)?;
                    errs.extend(a.validate(&args_path));
                }
                "NodeResourcesBalancedAllocation" => {
                    let mut a: NodeResourcesBalancedAllocationArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    errs.extend(a.validate(&args_path));
                }
                "PodTopologySpread" => {
                    let mut a: PodTopologySpreadArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    errs.extend(a.validate(&args_path));
                }
                // validation.go:158 keys this validator "NodeResourcesFitArgs"
                // (sic), never the plugin's name "NodeResourcesFit", and its
                // args type can never match (args decode by `name + "Args"`,
                // kube-scheduler/config/v1/types.go:272), so upstream reports
                // a type mismatch for that name (validation.go:199-203) and
                // does NOT validate NodeResourcesFit's args in this pass. They
                // are validated when the plugin is constructed instead (NewFit,
                // fit.go:193), collected in `init_errs`.
                // validation.go:161-163 validates DynamicResourcesArgs with
                // the default feature gates; defaults (v1/defaults.go:247)
                // are applied first by the scheme.
                "DynamicResources" => {
                    let mut a: DynamicResourcesArgs = decode_args(pc, &args_path)?;
                    let fts = DynamicResourcesFeatures::from_gates();
                    a.set_defaults(fts);
                    errs.extend(a.validate(&args_path, fts));
                }
                "NodeResourcesFitArgs" => {
                    errs.push(field::Error::invalid(
                        &args_path,
                        BadValue::marshal(&pc.args),
                        "has to match plugin args",
                    ));
                }
                "NodeResourcesFit" => {
                    let mut a: NodeResourcesFitArgs = decode_args(pc, &args_path)?;
                    a.set_defaults();
                    // `ValidateNodeResourcesFitArgs(nil, args)`: nil path.
                    init_errs.extend(a.validate(&Path::default()));
                }
                _ => {}
            }
        }
        if existing.contains(&name) {
            // The bad value is a *field.Path, which json.Marshal renders as
            // `{}` (apimachinery field/errors.go:92-97).
            errs.push(field::Error::duplicate(
                &path.child("schedulerName"),
                BadValue::Marshaled("{}".to_string()),
            ));
        }
        existing.push(name);
    }
    if !cfg.profiles.is_empty() {
        errs.extend(validate_common_queue_sort(&profiles_path, &cfg.profiles));
    }
    errs.extend(validate_percentage_of_nodes_to_score(
        &Path::new("percentageOfNodesToScore"),
        cfg.percentage_of_nodes_to_score,
    ));
    // validation.go:70-77.
    let initial = cfg.pod_initial_backoff_seconds.unwrap_or(1);
    let max = cfg.pod_max_backoff_seconds.unwrap_or(10);
    if initial <= 0 {
        errs.push(field::Error::invalid(
            &Path::new("podInitialBackoffSeconds"),
            initial,
            "must be greater than 0",
        ));
    }
    if max < initial {
        errs.push(field::Error::invalid(
            &Path::new("podMaxBackoffSeconds"),
            max,
            "must be greater than or equal to PodInitialBackoffSeconds",
        ));
    }
    // validation.go:82.
    errs.extend(validate_extenders(&Path::new("extenders"), &cfg.extenders));
    Ok((errs, init_errs))
}

/// `validateExtenders` (validation.go:256-286).
fn validate_extenders(fld_path: &Path, extenders: &[Extender]) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut binders = 0;
    let mut managed: Vec<&str> = Vec::new();
    for (i, extender) in extenders.iter().enumerate() {
        let path = fld_path.index(i);
        if !extender.prioritize_verb.is_empty() && extender.weight <= 0 {
            errs.push(field::Error::invalid(
                &path.child("weight"),
                extender.weight,
                "must have a positive weight applied to it",
            ));
        }
        if !extender.bind_verb.is_empty() {
            binders += 1;
        }
        for (j, resource) in extender.managed_resources.iter().enumerate() {
            let mr_path = path.child("managedResources").index(j);
            errs.extend(validate_extended_resource_name(
                &mr_path.child("name"),
                &resource.name,
            ));
            if managed.contains(&resource.name.as_str()) {
                errs.push(field::Error::invalid(
                    &mr_path.child("name"),
                    resource.name.as_str(),
                    "duplicate extender managed resource name",
                ));
            }
            managed.push(&resource.name);
        }
    }
    if binders > 1 {
        errs.push(field::Error::invalid(
            fld_path,
            format!("found {binders} extenders implementing bind"),
            "only one extender can implement bind",
        ));
    }
    errs
}

/// `validateExtendedResourceName` (validation.go:290-301).
fn validate_extended_resource_name(path: &Path, name: &str) -> ErrorList {
    let mut errs: ErrorList = is_qualified_name(name)
        .into_iter()
        .map(|msg| field::Error::invalid(path, name, msg))
        .collect();
    if !errs.is_empty() {
        return errs;
    }
    if !is_extended_resource_name(name) {
        errs.push(field::Error::invalid(
            path,
            name,
            "is an invalid extended resource name",
        ));
    }
    errs
}

/// `validateCommonQueueSort` (validation.go:219-254).
fn validate_common_queue_sort(path: &Path, profiles: &[Profile]) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut canon = PluginSet::default();
    let mut queue_sort_name = "";
    let mut queue_sort_args: Option<&serde_yaml::Value> = None;
    if let Some(pl) = &profiles[0].plugins {
        canon = pl.queue_sort.clone();
        if let Some(first) = pl.queue_sort.enabled.first() {
            queue_sort_name = first.name.as_str();
        }
        let length = pl.queue_sort.enabled.len();
        if length > 1 {
            errs.push(field::Error::invalid(
                &path
                    .index(0)
                    .child("plugins")
                    .child("queueSort")
                    .child("Enabled"),
                length as i32,
                "only one queue sort plugin can be enabled",
            ));
        }
    }
    // No `break`: the last matching config wins, as upstream.
    for cfg in &profiles[0].plugin_config {
        if !queue_sort_name.is_empty() && cfg.name == queue_sort_name {
            queue_sort_args = cfg.args.as_ref();
        }
    }
    for (i, profile) in profiles.iter().enumerate().skip(1) {
        let curr = profile
            .plugins
            .as_ref()
            .map(|p| p.queue_sort.clone())
            .unwrap_or_default();
        if canon != curr {
            errs.push(field::Error::invalid(
                &path.index(i).child("plugins").child("queueSort"),
                BadValue::Marshaled(curr.go_json()),
                "must be the same for all profiles",
            ));
        }
        for cfg in &profile.plugin_config {
            if cfg.name == queue_sort_name && queue_sort_args != cfg.args.as_ref() {
                errs.push(field::Error::invalid(
                    &path.index(i).child("pluginConfig").child("args"),
                    BadValue::marshal(&cfg.args),
                    "queueSort must be the same for all profiles",
                ));
            }
        }
    }
    errs
}

fn decode_args<T: serde::de::DeserializeOwned + Default>(
    pc: &PluginConfig,
    path: &Path,
) -> Result<T, String> {
    match &pc.args {
        Some(v) => serde_yaml::from_value(v.clone())
            .map_err(|e| format!("{path}: invalid {} args: {e}", pc.name)),
        None => Ok(T::default()),
    }
}

/// `validatePercentageOfNodesToScore` (validation.go:86-92).
fn validate_percentage_of_nodes_to_score(path: &Path, pct: Option<i32>) -> ErrorList {
    match pct {
        Some(v) if !(0..=100).contains(&v) => {
            vec![field::Error::invalid(path, v, "not in valid range [0-100]")]
        }
        _ => ErrorList::new(),
    }
}

/// Read `--config` / `--scheduler-config` and resolve the `VolumeBinding` args
/// (upstream `Options.ConfigFile`, cmd/kube-scheduler/app/options/options.go:203,
/// then `LoadConfigFromFile`). Shared by the standalone and all-in-one
/// binaries. The `StorageCapacityScoring` gate is read from the process-wide
/// feature gates, so apply `--feature-gates` first.
pub fn load_volume_binding_args(path: &str) -> Result<VolumeBindingArgs, String> {
    let doc = std::fs::read_to_string(path).map_err(|e| format!("reading config {path}: {e}"))?;
    let scoring = rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
    );
    volume_binding_args_from_config(&doc, scoring)
        .map_err(|e| format!("invalid config {path}: {e}"))
}

/// [`load_volume_binding_args`] for every profile: read the file and build the
/// scheduler-name-indexed [`crate::profile::Map`].
pub fn load_profiles(path: &str) -> Result<crate::profile::Map, String> {
    let doc = std::fs::read_to_string(path).map_err(|e| format!("reading config {path}: {e}"))?;
    let scoring = rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
    );
    profiles_from_config(&doc, scoring).map_err(|e| format!("invalid config {path}: {e}"))
}

/// `validateFunctionShape` (validation_pluginargs.go:176-211).
fn validate_function_shape(shape: &[UtilizationShapePoint], path: &Path) -> ErrorList {
    const MIN_UTILIZATION: i32 = 0;
    const MAX_UTILIZATION: i32 = 100;
    const MIN_SCORE: i32 = 0;
    const MAX_SCORE: i32 = MAX_CUSTOM_PRIORITY_SCORE as i32;

    let mut errs = ErrorList::new();
    if shape.is_empty() {
        errs.push(field::Error::required(
            path,
            "at least one point must be specified",
        ));
        return errs;
    }
    for i in 1..shape.len() {
        if shape[i - 1].utilization >= shape[i].utilization {
            errs.push(field::Error::invalid(
                &path.index(i).child("utilization"),
                shape[i].utilization,
                "values must be sorted in increasing order",
            ));
            break;
        }
    }
    for (i, point) in shape.iter().enumerate() {
        if point.utilization < MIN_UTILIZATION || point.utilization > MAX_UTILIZATION {
            errs.push(field::Error::invalid(
                &path.index(i).child("utilization"),
                point.utilization,
                format!("not in valid range [{MIN_UTILIZATION}, {MAX_UTILIZATION}]"),
            ));
        }
        if point.score < MIN_SCORE || point.score > MAX_SCORE {
            errs.push(field::Error::invalid(
                &path.index(i).child("score"),
                point.score,
                format!("not in valid range [{MIN_SCORE}, {MAX_SCORE}]"),
            ));
        }
    }
    errs
}
