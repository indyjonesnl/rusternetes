//! `VolumeBindingArgs` scheduler plugin configuration: defaulting, validation
//! and the shape -> scorer-shape conversion.
//!
//! Ported from:
//! - `pkg/scheduler/apis/config/v1/defaults.go:192-208` (`SetDefaults_VolumeBindingArgs`)
//! - `pkg/scheduler/apis/config/validation/validation_pluginargs.go:176-211`
//!   (`validateFunctionShape`) and `:266-290` (`ValidateVolumeBindingArgs*`)
//! - `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go:642-653`
//!   (shape scaled by `MaxNodeScore / MaxCustomPriorityScore`).

use crate::volume_binding::{FunctionShapePoint, MAX_CUSTOM_PRIORITY_SCORE, MAX_NODE_SCORE};
use rusternetes_common::go_duration::format_go_duration;
use rusternetes_common::label_selector::{LabelOperator, LabelRequirement};
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
fn default_resource_spec() -> Vec<ResourceSpec> {
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

/// `config.Plugin` (pkg/scheduler/apis/config/types.go). The internal type has
/// no json tags, so a marshalled bad value uses the Go field names.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
struct Plugin {
    name: String,
    #[serde(default)]
    weight: Option<i32>,
}

/// `config.PluginSet`. `apiequality.Semantic.DeepEqual` treats nil and empty
/// slices as equal, which `Vec` equality already does.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
struct PluginSet {
    #[serde(default)]
    enabled: Vec<Plugin>,
    #[serde(default)]
    disabled: Vec<Plugin>,
}

impl PluginSet {
    /// `json.Marshal` of the internal `config.PluginSet` (no json tags; a nil
    /// slice marshals as `null`).
    fn go_json(&self) -> String {
        fn list(v: &[Plugin]) -> String {
            if v.is_empty() {
                return "null".to_string();
            }
            let items: Vec<String> = v
                .iter()
                .map(|p| {
                    let name = serde_json::Value::String(p.name.clone());
                    format!("{{\"Name\":{name},\"Weight\":{}}}", p.weight.unwrap_or(0))
                })
                .collect();
            format!("[{}]", items.join(","))
        }
        // Struct field order, which serde_json's map would sort away.
        format!(
            "{{\"Enabled\":{},\"Disabled\":{}}}",
            list(&self.enabled),
            list(&self.disabled)
        )
    }
}

/// The extension points `validatePluginConfig` iterates
/// (validation.go:166-182). `multiPoint` is deliberately absent, as upstream.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Plugins {
    #[serde(default)]
    pre_enqueue: PluginSet,
    #[serde(default)]
    queue_sort: PluginSet,
    #[serde(default)]
    pre_filter: PluginSet,
    #[serde(default)]
    filter: PluginSet,
    #[serde(default)]
    post_filter: PluginSet,
    #[serde(default)]
    pre_score: PluginSet,
    #[serde(default)]
    score: PluginSet,
    #[serde(default)]
    reserve: PluginSet,
    #[serde(default)]
    permit: PluginSet,
    #[serde(default)]
    pre_bind: PluginSet,
    #[serde(default)]
    bind: PluginSet,
    #[serde(default)]
    post_bind: PluginSet,
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
    let cfg: KubeSchedulerConfiguration =
        serde_yaml::from_str(doc).map_err(|e| format!("invalid scheduler config: {e}"))?;
    let (mut errs, init_errs) = validate_profiles(&cfg)?;
    let mut args = VolumeBindingArgs::default();
    let mut path = Path::new("profiles").index(0);
    if let Some(profile) = cfg.profiles.first() {
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
    }
    args.set_defaults(storage_capacity_scoring);
    errs.extend(args.validate(&path, storage_capacity_scoring));
    if errs.is_empty() {
        errs = init_errs;
    }
    if errs.is_empty() {
        Ok(args)
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

/// `v1helper.IsNativeResource` (pkg/apis/core/v1/helper/helpers.go:60-63):
/// `IsPrefixedNativeResource` is a `Contains` check on `kubernetes.io/`
/// (helpers.go:53-55), not a prefix check.
fn is_native_resource(name: &str) -> bool {
    !name.contains('/') || name.contains("kubernetes.io/")
}

/// `v1helper.IsExtendedResourceName` (pkg/apis/core/v1/helper/helpers.go:39-49).
fn is_extended_resource_name(name: &str) -> bool {
    if is_native_resource(name) || name.starts_with("requests.") {
        return false;
    }
    is_qualified_name(&format!("requests.{name}")).is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::validation::field::ErrorType;

    fn pt(utilization: i32, score: i32) -> UtilizationShapePoint {
        UtilizationShapePoint { utilization, score }
    }

    fn args(timeout: i64, shape: Option<Vec<UtilizationShapePoint>>) -> VolumeBindingArgs {
        VolumeBindingArgs {
            bind_timeout_seconds: Some(timeout),
            shape,
        }
    }

    fn summary(errs: &ErrorList) -> Vec<(ErrorType, String)> {
        errs.iter()
            .map(|e| (e.error_type, e.field.clone()))
            .collect()
    }

    // validation_pluginargs_test.go TestValidateVolumeBindingArgs
    #[test]
    fn zero_and_positive_timeout_valid() {
        for t in [0, 10] {
            assert!(args(t, None).validate(&Path::default(), false).is_empty());
        }
    }

    #[test]
    fn negative_timeout_invalid() {
        let errs = args(-10, None).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "bindTimeoutSeconds".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "invalid BindTimeoutSeconds, should not be a negative value"
        );
    }

    #[test]
    fn feature_off_shape_nil_ok() {
        assert!(args(10, None).validate(&Path::default(), false).is_empty());
    }

    #[test]
    fn feature_off_shape_set_is_invalid() {
        let errs = args(10, Some(vec![pt(1, 1), pt(3, 3)])).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring"
        );
    }

    #[test]
    fn feature_on_empty_shape_required() {
        let errs = args(10, Some(vec![])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
        // A nil shape with the gate on is also "at least one point".
        let errs = args(10, None).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
    }

    #[test]
    fn feature_on_unsorted_points() {
        let errs = args(10, Some(vec![pt(3, 3), pt(1, 1)])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape[1].utilization".to_string())]
        );
        assert_eq!(errs[0].detail, "values must be sorted in increasing order");
    }

    #[test]
    fn feature_on_out_of_range_points() {
        let errs = args(
            10,
            Some(vec![pt(-1, 1), pt(10, -1), pt(20, 11), pt(101, 1)]),
        )
        .validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![
                (ErrorType::Invalid, "shape[0].utilization".to_string()),
                (ErrorType::Invalid, "shape[1].score".to_string()),
                (ErrorType::Invalid, "shape[2].score".to_string()),
                (ErrorType::Invalid, "shape[3].utilization".to_string()),
            ]
        );
        assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        assert_eq!(errs[1].detail, "not in valid range [0, 10]");
    }

    #[test]
    fn nested_path_prefix() {
        let p = Path::new("profiles").index(0).child("args");
        let errs = args(-1, None).validate(&p, false);
        assert_eq!(errs[0].field, "profiles[0].args.bindTimeoutSeconds");
    }

    #[test]
    fn config_doc_custom_shape_gate_on() {
        let doc = r#"
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: VolumeBinding
    args:
      bindTimeoutSeconds: 30
      shape:
      - utilization: 0
        score: 0
      - utilization: 100
        score: 10
"#;
        let a = volume_binding_args_from_config(doc, true).unwrap();
        assert_eq!(a, args(30, Some(vec![pt(0, 0), pt(100, 10)])));
    }

    #[test]
    fn config_doc_shape_with_gate_off_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      shape:\n      - {utilization: 0, score: 1}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[0].pluginConfig[0].args.shape"),
            "{err}"
        );
        assert!(err.contains("StorageCapacityScoring"), "{err}");
    }

    #[test]
    fn config_doc_without_volume_binding_gets_defaults() {
        let a = volume_binding_args_from_config("profiles: []\n", true).unwrap();
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
    }

    // v1/defaults_test.go TestPluginArgsDefaults, VolumeBindingArgs cases.
    #[test]
    fn defaults_gate_off() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(false);
        assert_eq!(a, args(600, None));
    }

    #[test]
    fn defaults_gate_on() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
    }

    #[test]
    fn defaults_keep_explicit_values() {
        let mut a = args(0, Some(vec![pt(50, 5)]));
        a.set_defaults(true);
        assert_eq!(a, args(0, Some(vec![pt(50, 5)])));
    }

    #[test]
    fn scorer_shape_scales_to_max_node_score() {
        let a = args(10, Some(vec![pt(0, 1), pt(100, 10)]));
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, vec![(0, 10), (100, 100)]);
    }

    #[test]
    fn default_args_scorer_shape_equals_default_shape() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        let want: Vec<_> = crate::volume_binding::default_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, want);
    }

    // #3202: file loader shared by the scheduler binary and the all-in-one
    // binary (--scheduler-config).
    #[test]
    fn load_from_file_reads_defaults_and_validates() {
        let dir = std::env::temp_dir().join(format!("sched-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("ok.yaml");
        std::fs::write(
            &ok,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: 42\n",
        )
        .unwrap();
        let a = load_volume_binding_args(ok.to_str().unwrap()).unwrap();
        assert_eq!(a.bind_timeout_seconds, Some(42));
        let bad = dir.join("bad.yaml");
        std::fs::write(
            &bad,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: -1\n",
        )
        .unwrap();
        let err = load_volume_binding_args(bad.to_str().unwrap()).unwrap_err();
        assert!(err.contains("bindTimeoutSeconds"), "{err}");
        let err = load_volume_binding_args(dir.join("missing.yaml").to_str().unwrap()).unwrap_err();
        assert!(err.contains("missing.yaml"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // #3207: validatePluginConfig seenPluginConfig (validation.go:189-197);
    // validation_test.go "duplicated-plugin-config" -> profiles[0].pluginConfig[1].
    #[test]
    fn duplicate_plugin_config_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n  - name: VolumeBinding\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[0].pluginConfig[1]: Duplicate value"),
            "{err}"
        );
        // Same name in different profiles is fine.
        let ok = "profiles:\n- schedulerName: a\n  pluginConfig:\n  - name: X\n- schedulerName: b\n  pluginConfig:\n  - name: X\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // #3345: validateExtenders (validation.go:256-286). validation_test.go
    // "extender-negative-weight" -> extenders[0].weight.
    #[test]
    fn extender_negative_weight_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  prioritizeVerb: prioritize\n  weight: -1\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders[0].weight: Invalid value: -1: must have a positive weight applied to it"
        );
        // Weight is only checked when a prioritizeVerb is set (validation.go:263).
        let ok = "extenders:\n- urlPrefix: http://x\n  weight: -1\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
        let ok = "extenders:\n- urlPrefix: http://x\n  prioritizeVerb: p\n  weight: 1\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // "extender-duplicate-managed-resources" -> extenders[0].managedResources[1].name.
    #[test]
    fn extender_duplicate_managed_resource_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  managedResources:\n  - name: example.com/foo\n  - name: example.com/foo\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders[0].managedResources[1].name: Invalid value: \"example.com/foo\": duplicate extender managed resource name"
        );
    }

    // "extender-duplicate-bind" -> extenders (validation.go:281-283).
    #[test]
    fn extender_duplicate_bind_rejected() {
        let doc = "extenders:\n- urlPrefix: http://x\n  bindVerb: foo\n- urlPrefix: http://y\n  bindVerb: bar\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "extenders: Invalid value: \"found 2 extenders implementing bind\": only one extender can implement bind"
        );
    }

    // validateExtendedResourceName (validation.go:290-301): qualified-name
    // errors short-circuit; native names are not extended resources.
    #[test]
    fn extender_managed_resource_name_must_be_extended() {
        let doc = "extenders:\n- urlPrefix: http://x\n  managedResources:\n  - name: cpu\n  - name: kubernetes.io/foo\n  - name: requests.example.com/foo\n  - name: \"bad name/x\"\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains(
                "extenders[0].managedResources[0].name: Invalid value: \"cpu\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[1].name: Invalid value: \"kubernetes.io/foo\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[2].name: Invalid value: \"requests.example.com/foo\": is an invalid extended resource name"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "extenders[0].managedResources[3].name: Invalid value: \"bad name/x\": prefix part"
            ),
            "{err}"
        );
        assert!(
            !err.contains("managedResources[3].name: Invalid value: \"bad name/x\": is an invalid"),
            "{err}"
        );
    }

    // validation.go:58-67: duplicate schedulerName across profiles.
    #[test]
    fn duplicate_scheduler_name_rejected() {
        let doc = "profiles:\n- schedulerName: a\n- schedulerName: a\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[1].schedulerName: Duplicate value"),
            "{err}"
        );
    }

    // validation.go:58-67 -> field.Duplicate(path.Child("schedulerName"),
    // profilesPath.Index(idx).Child("schedulerName")): the bad value is a
    // *field.Path, which json.Marshal renders as `{}` (errors.go:92-97).
    #[test]
    fn duplicate_scheduler_name_value_renders_like_go() {
        let doc = "profiles:\n- schedulerName: a\n- schedulerName: a\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(err, "profiles[1].schedulerName: Duplicate value: {}");
    }

    // v1/defaults.go:116-119: the default name is only applied to a single
    // profile; validation.go:141-144 then requires a name on the rest
    // (validation_test.go "scheduler-name-not-set" -> profiles[1].schedulerName).
    #[test]
    fn unnamed_profile_required_when_multiple() {
        let doc = "profiles:\n- schedulerName: a\n- pluginConfig: []\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(err, "profiles[1].schedulerName: Required value");
        let one = "profiles:\n- pluginConfig: []\n";
        assert!(volume_binding_args_from_config(one, false).is_ok());
    }

    // validation.go:86-92 validatePercentageOfNodesToScore, top level and
    // per profile (validation_test.go "bad-percentage-of-nodes-to-score",
    // "greater-than-100-profile-percentage-of-nodes-to-score").
    #[test]
    fn percentage_of_nodes_to_score_range() {
        let err =
            volume_binding_args_from_config("percentageOfNodesToScore: 101\n", false).unwrap_err();
        assert_eq!(
            err,
            "percentageOfNodesToScore: Invalid value: 101: not in valid range [0-100]"
        );
        let err = volume_binding_args_from_config(
            "profiles:\n- schedulerName: a\n- schedulerName: b\n  percentageOfNodesToScore: -1\n",
            false,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "profiles[1].percentageOfNodesToScore: Invalid value: -1: not in valid range [0-100]"
        );
        assert!(volume_binding_args_from_config("percentageOfNodesToScore: 100\n", false).is_ok());
    }

    fn dp(pct: i32, abs: i32) -> DefaultPreemptionArgs {
        DefaultPreemptionArgs {
            min_candidate_nodes_percentage: Some(pct),
            min_candidate_nodes_absolute: Some(abs),
        }
    }

    // validation_pluginargs_test.go TestValidateDefaultPreemptionArgs
    #[test]
    fn default_preemption_valid_default() {
        assert!(dp(10, 100).validate(&Path::default()).is_empty());
    }

    #[test]
    fn default_preemption_percentage_out_of_range() {
        for pct in [-1, 900] {
            let errs = dp(pct, 100).validate(&Path::default());
            assert_eq!(
                summary(&errs),
                vec![(
                    ErrorType::Invalid,
                    "minCandidateNodesPercentage".to_string()
                )]
            );
            assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        }
    }

    #[test]
    fn default_preemption_negative_absolute() {
        let errs = dp(20, -1).validate(&Path::default());
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "minCandidateNodesAbsolute".to_string())]
        );
        assert_eq!(errs[0].detail, "not in valid range [0, inf)");
    }

    #[test]
    fn default_preemption_all_zero() {
        let errs = dp(0, 0).validate(&Path::default());
        assert_eq!(
            summary(&errs),
            vec![
                (
                    ErrorType::Invalid,
                    "minCandidateNodesPercentage".to_string()
                ),
                (ErrorType::Invalid, "minCandidateNodesAbsolute".to_string()),
            ]
        );
        assert_eq!(
            errs[0].detail,
            "cannot be zero at the same time as minCandidateNodesAbsolute"
        );
        assert_eq!(
            errs[1].detail,
            "cannot be zero at the same time as minCandidateNodesPercentage"
        );
    }

    #[test]
    fn default_preemption_one_zero_ok() {
        assert!(dp(0, 100).validate(&Path::default()).is_empty());
        assert!(dp(10, 0).validate(&Path::default()).is_empty());
    }

    // v1/defaults.go SetDefaults_DefaultPreemptionArgs
    #[test]
    fn default_preemption_defaults() {
        let mut a = DefaultPreemptionArgs::default();
        a.set_defaults();
        assert_eq!(a, dp(10, 100));
        let mut b = dp(0, 5);
        b.set_defaults();
        assert_eq!(b, dp(0, 5));
    }

    // validation_pluginargs_test.go TestValidateInterPodAffinityArgs
    #[test]
    fn inter_pod_affinity_weight_range() {
        let ok = |w| {
            InterPodAffinityArgs {
                hard_pod_affinity_weight: Some(w),
            }
            .validate(&Path::default())
        };
        assert!(ok(10).is_empty());
        assert!(ok(0).is_empty());
        assert!(ok(100).is_empty());
        for w in [-1, 101] {
            let errs = ok(w);
            assert_eq!(
                summary(&errs),
                vec![(ErrorType::Invalid, "hardPodAffinityWeight".to_string())]
            );
            assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        }
    }

    // v1/defaults.go SetDefaults_InterPodAffinityArgs
    #[test]
    fn inter_pod_affinity_defaults() {
        let mut a = InterPodAffinityArgs::default();
        a.set_defaults();
        assert_eq!(a.hard_pod_affinity_weight, Some(1));
        let mut b = InterPodAffinityArgs {
            hard_pod_affinity_weight: Some(0),
        };
        b.set_defaults();
        assert_eq!(b.hard_pod_affinity_weight, Some(0));
    }

    // validation.go:107-135 isPluginInvalid + validatePluginConfig
    #[test]
    fn removed_plugin_config_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: AzureDiskLimits\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].pluginConfig[0]: Invalid value: \"AzureDiskLimits\": was invalid in version \"kubescheduler.config.k8s.io/v1\" (KubeSchedulerConfiguration is version \"kubescheduler.config.k8s.io/v1\")"
        );
    }

    #[test]
    fn config_doc_default_preemption_and_inter_pod_affinity_validated() {
        let doc = "profiles:\n- pluginConfig:\n  - name: DefaultPreemption\n    args:\n      minCandidateNodesPercentage: 0\n      minCandidateNodesAbsolute: 0\n  - name: InterPodAffinity\n    args:\n      hardPodAffinityWeight: 101\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.contains(
            "profiles[0].pluginConfig[0].args.minCandidateNodesPercentage: Invalid value: 0: cannot be zero at the same time as minCandidateNodesAbsolute"
        ), "{err}");
        assert!(err.contains(
            "profiles[0].pluginConfig[1].args.hardPodAffinityWeight: Invalid value: 101: not in valid range [0, 100]"
        ), "{err}");
        let ok = "profiles:\n- pluginConfig:\n  - name: DefaultPreemption\n    args:\n      minCandidateNodesAbsolute: 0\n  - name: InterPodAffinity\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // validation.go:155-160 validatePluginConfig -> ValidateNodeAffinityArgs,
    // ValidateNodeResourcesBalancedAllocationArgs, ValidatePodTopologySpreadArgs.
    #[test]
    fn config_doc_node_affinity_balanced_allocation_pts_validated() {
        let doc = "profiles:
- pluginConfig:
  - name: NodeAffinity
    args:
      addedAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - {key: invalid/label/key, operator: In, values: [v]}
  - name: NodeResourcesBalancedAllocation
    args:
      resources:
      - {name: cpu, weight: 2}
  - name: PodTopologySpread
    args:
      defaultingType: System
      defaultConstraints:
      - {maxSkew: 1, topologyKey: key, whenUnsatisfiable: DoNotSchedule}
";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.contains("profiles[0].pluginConfig[0].args.addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key: Invalid value: \"invalid/label/key\""), "{err}");
        assert!(
            err.contains(
                "profiles[0].pluginConfig[1].args.resources[0].weight: Invalid value: 2: must be 1"
            ),
            "{err}"
        );
        assert!(
            err.contains("profiles[0].pluginConfig[2].args.defaultingType: Invalid value: \"System\": when .defaultConstraints are not empty"),
            "{err}"
        );
    }

    // NewFit (noderesources/fit.go:193) validates with a nil path; the
    // validation.go:158 map key is "NodeResourcesFitArgs", so the profile
    // pass never reaches it.
    #[test]
    fn config_doc_node_resources_fit_validated_at_plugin_init() {
        let doc = "profiles:\n- pluginConfig:\n  - name: NodeResourcesFit\n    args:\n      scoringStrategy:\n        type: Invalid\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("scoringStrategy.type: Unsupported value: \"Invalid\": supported values: \"LeastAllocated\", \"MostAllocated\", \"RequestedToCapacityRatio\""),
            "{err}"
        );
    }

    fn yaml<T: serde::de::DeserializeOwned>(s: &str) -> T {
        serde_yaml::from_str(s).unwrap()
    }

    fn fs(errs: &ErrorList) -> Vec<(ErrorType, String)> {
        summary(errs)
    }

    fn et(t: ErrorType, f: &str) -> (ErrorType, String) {
        (t, f.to_string())
    }

    // validation_pluginargs_test.go TestValidatePodTopologySpreadArgs
    #[test]
    fn pts_args_cases() {
        let c = |extra: &str| {
            format!("defaultingType: List\ndefaultConstraints:\n- {{maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule{extra}}}\n")
        };
        let run = |doc: &str| yaml::<PodTopologySpreadArgs>(doc).validate(&Path::default());
        assert!(run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n- {maxSkew: 2, topologyKey: zone, whenUnsatisfiable: ScheduleAnyway}\n").is_empty());
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: -1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "defaultConstraints[0].maxSkew")]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: '', whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Required, "defaultConstraints[0].topologyKey")]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: ''}\n");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Required,
                "defaultConstraints[0].whenUnsatisfiable"
            )]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: unknown action}\n");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::NotSupported,
                "defaultConstraints[0].whenUnsatisfiable"
            )]
        );
        let e = run("defaultingType: List\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n- {maxSkew: 2, topologyKey: node, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Duplicate, "defaultConstraints[1]")]
        );
        assert_eq!(
            e[0].error_body(),
            "Duplicate value: \"{node, DoNotSchedule}\""
        );
        let e = run(&c(", labelSelector: {matchLabels: {a: b}}"));
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Forbidden,
                "defaultConstraints[0].labelSelector"
            )]
        );
        assert!(run("defaultingType: List\n").is_empty());
        assert!(run("defaultingType: System\n").is_empty());
        let e = run("defaultingType: unknown\n");
        assert_eq!(fs(&e), vec![et(ErrorType::NotSupported, "defaultingType")]);
        let e = run("defaultingType: System\ndefaultConstraints:\n- {maxSkew: 1, topologyKey: key, whenUnsatisfiable: DoNotSchedule}\n");
        assert_eq!(fs(&e), vec![et(ErrorType::Invalid, "defaultingType")]);
    }

    // defaults_test.go TestPluginArgsDefaults "PodTopologySpreadArgs ..."
    #[test]
    fn pts_args_defaults() {
        let mut a: PodTopologySpreadArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(a.defaulting_type, "System");
        assert!(a.default_constraints.is_empty());
        let mut a: PodTopologySpreadArgs = yaml(
            "defaultConstraints:\n- {topologyKey: planet, whenUnsatisfiable: DoNotSchedule, maxSkew: 2}\n",
        );
        a.set_defaults();
        assert_eq!(a.defaulting_type, "System");
        assert_eq!(a.default_constraints.len(), 1);
        assert_eq!(a.default_constraints[0].max_skew, 2);
    }

    // validation_pluginargs_test.go TestValidateNodeResourcesBalancedAllocationArgs
    #[test]
    fn balanced_allocation_args_cases() {
        let run =
            |doc: &str| yaml::<NodeResourcesBalancedAllocationArgs>(doc).validate(&Path::default());
        assert!(run("resources: [{name: cpu, weight: 1}, {name: memory, weight: 1}]").is_empty());
        let e = run("resources: [{name: cpu, weight: 2}, {name: memory, weight: 1}]");
        assert_eq!(fs(&e), vec![et(ErrorType::Invalid, "resources[0].weight")]);
        let e = run("resources: [{name: cpu, weight: 1}, {name: cpu, weight: 1}]");
        assert_eq!(fs(&e), vec![et(ErrorType::Duplicate, "resources[1].name")]);
    }

    // defaults_test.go TestPluginArgsDefaults "NodeResourcesBalancedAllocationArgs ..."
    #[test]
    fn balanced_allocation_args_defaults() {
        let mut a: NodeResourcesBalancedAllocationArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(a.resources, default_resource_spec());
        let mut a: NodeResourcesBalancedAllocationArgs = yaml(
            "resources: [{name: cpu}, {name: scalar.io/scalar0}, {name: scalar.io/scalar1, weight: 1}]",
        );
        a.set_defaults();
        assert!(a.resources.iter().all(|r| r.weight == 1));
        assert_eq!(a.resources.len(), 3);
    }

    // validation_pluginargs_test.go TestValidateNodeAffinityArgs
    #[test]
    fn node_affinity_args_cases() {
        let run = |doc: &str| yaml::<NodeAffinityArgs>(doc).validate(&Path::default());
        assert!(run("{}").is_empty());
        let valid = "addedAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
    - matchExpressions: [{key: label-1, operator: In, values: [label-1-val]}]
  preferredDuringSchedulingIgnoredDuringExecution:
  - weight: 1
    preference:
      matchFields: [{key: metadata.name, operator: In, values: [node-1]}]
";
        assert!(run(valid).is_empty());
        let invalid = "addedAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
    - matchExpressions: [{key: invalid/label/key, operator: In, values: [label-1-val]}]
  preferredDuringSchedulingIgnoredDuringExecution:
  - weight: 1
    preference:
      matchFields: [{key: metadata.name, operator: In, values: [node-1, node-2]}]
";
        let e = run(invalid);
        assert_eq!(
            fs(&e),
            vec![
                et(ErrorType::Invalid, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key"),
                et(ErrorType::Invalid, "addedAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].matchFields[0].values"),
            ]
        );
        assert_eq!(
            e[1].error_body(),
            "Invalid value: [\"node-1\",\"node-2\"]: must have one element"
        );
        // nodeSelectorRequirementsAsSelector: unknown operator, Gt value, fields operator.
        let e = run("addedAffinity:\n  requiredDuringSchedulingIgnoredDuringExecution:\n    nodeSelectorTerms:\n    - matchExpressions: [{key: a, operator: Bogus}, {key: b, operator: Gt, values: [x]}]\n      matchFields: [{key: metadata.name, operator: Exists}]\n");
        assert_eq!(
            fs(&e),
            vec![
                et(ErrorType::NotSupported, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator"),
                et(ErrorType::Invalid, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[1].values[0]"),
                et(ErrorType::NotSupported, "addedAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].operator"),
            ]
        );
        // nodeaffinity.go:119: zero weight / empty terms are not parsed.
        assert!(run("addedAffinity:\n  preferredDuringSchedulingIgnoredDuringExecution:\n  - weight: 0\n    preference:\n      matchExpressions: [{key: bad/key/x, operator: In, values: [v]}]\n  - weight: 1\n    preference: {}\n").is_empty());
    }

    // validation_pluginargs_test.go TestValidateFitArgs
    #[test]
    fn fit_args_cases() {
        let default = "scoringStrategy: {type: LeastAllocated, resources: [{name: cpu, weight: 1}, {name: memory, weight: 1}]}\n";
        let run = |extra: &str| {
            yaml::<NodeResourcesFitArgs>(&format!("{extra}{default}")).validate(&Path::default())
        };
        let detail = |e: &ErrorList| {
            e.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join("|")
        };
        let long = "a".repeat(64);
        assert!(
            detail(&run(&format!("ignoredResources: [longvalue{long}]\n")))
                .contains("name part must be no more than 63 bytes")
        );
        assert!(detail(&run("ignoredResources: ['example.com/']\n"))
            .contains("name part must be non-empty"));
        assert!(detail(&run("ignoredResources: [example.com/aaa/bbb]\n"))
            .contains("a valid label key must consist of"));
        assert!(run("ignoredResources: [example.com]\n").is_empty());
        assert!(run("ignoredResourceGroups: [example.com]\n").is_empty());
        assert!(detail(&run("ignoredResourceGroups: ['example.com/']\n"))
            .contains("name part must be non-empty"));
        assert!(detail(&run(&format!("ignoredResourceGroups: [{long}]\n")))
            .contains("name part must be no more than 63 bytes"));
        assert!(detail(&run("ignoredResourceGroups: [example.com/aa]\n"))
            .contains("resource group name can't contain '/'"));
        let e = yaml::<NodeResourcesFitArgs>("scoringStrategy: {type: Invalid}\n")
            .validate(&Path::default());
        assert!(
            detail(&e).contains("Unsupported value: \"Invalid\""),
            "{}",
            detail(&e)
        );
    }

    // validation_pluginargs_test.go TestValidate{Least,Most}AllocatedScoringStrategy
    // and TestValidateRequestedToCapacityRatioScoringStrategy
    #[test]
    fn fit_scoring_strategy_cases() {
        let run = |doc: &str| yaml::<NodeResourcesFitArgs>(doc).validate(&Path::default());
        for ty in ["LeastAllocated", "MostAllocated"] {
            assert!(run(&format!("scoringStrategy: {{type: {ty}}}")).is_empty());
            assert!(run(&format!("scoringStrategy: {{type: {ty}, resources: [{{name: cpu, weight: 1}}, {{name: memory, weight: 10}}]}}")).is_empty());
            for w in [0, 101, -1] {
                let e = run(&format!(
                    "scoringStrategy: {{type: {ty}, resources: [{{name: cpu, weight: {w}}}]}}"
                ));
                assert_eq!(
                    fs(&e),
                    vec![et(
                        ErrorType::Invalid,
                        "scoringStrategy.resources[0].weight"
                    )]
                );
                assert_eq!(
                    e[0].detail,
                    "resource weight of cpu not in valid range (0, 100]"
                );
            }
        }
        let rt = "RequestedToCapacityRatio";
        let e = run(&format!("scoringStrategy: {{type: {rt}}}\n"));
        // validateFunctionShape is only run when requestedToCapacityRatio is set.
        assert!(e.is_empty());
        let e = run(&format!(
            "scoringStrategy: {{type: {rt}, requestedToCapacityRatio: {{}}}}\n"
        ));
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Required, "scoringStrategy.shape")]
        );
        let shaped = |shape: &str, res: &str| {
            run(&format!("scoringStrategy: {{type: {rt}, resources: [{res}], requestedToCapacityRatio: {{shape: [{shape}]}}}}"))
        };
        let ok = "{utilization: 30, score: 3}";
        let e = shaped(ok, "{name: cpu, weight: 101}");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.resources[0].weight"
            )]
        );
        let e = shaped("{utilization: -1, score: 3}", "");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[0].utilization"
            )]
        );
        let e = shaped("{utilization: 101, score: 3}", "");
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[0].utilization"
            )]
        );
        let e = shaped(
            "{utilization: 10, score: 1}, {utilization: 10, score: 2}",
            "",
        );
        assert_eq!(
            fs(&e),
            vec![et(
                ErrorType::Invalid,
                "scoringStrategy.shape[1].utilization"
            )]
        );
        let e = shaped("{utilization: 30, score: -1}", "");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "scoringStrategy.shape[0].score")]
        );
        let e = shaped("{utilization: 30, score: 11}", "");
        assert_eq!(
            fs(&e),
            vec![et(ErrorType::Invalid, "scoringStrategy.shape[0].score")]
        );
    }

    // defaults_test.go TestPluginArgsDefaults "NodeResourcesFitArgs ..."
    #[test]
    fn fit_args_defaults() {
        let mut a: NodeResourcesFitArgs = yaml("{}");
        a.set_defaults();
        assert_eq!(
            a.scoring_strategy,
            Some(ScoringStrategy {
                strategy_type: "LeastAllocated".into(),
                resources: default_resource_spec(),
                requested_to_capacity_ratio: None,
            })
        );
        let mut a: NodeResourcesFitArgs = yaml("scoringStrategy: {type: MostAllocated}");
        a.set_defaults();
        let s = a.scoring_strategy.unwrap();
        assert_eq!(s.strategy_type, "MostAllocated");
        assert_eq!(s.resources, default_resource_spec());
        // The type is NOT defaulted when a strategy is given without one.
        let mut a: NodeResourcesFitArgs = yaml("scoringStrategy: {resources: [{name: cpu}]}");
        a.set_defaults();
        let s = a.scoring_strategy.unwrap();
        assert_eq!(s.strategy_type, "");
        assert_eq!(s.resources[0].weight, 1);
    }

    // validation.go:158 keys the Fit validator "NodeResourcesFitArgs": a
    // pluginConfig of that name always fails the type check (validation.go:199-203).
    #[test]
    fn plugin_config_named_node_resources_fit_args_has_to_match() {
        let doc = "profiles:\n- pluginConfig:\n  - name: NodeResourcesFitArgs\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].pluginConfig[0].args: Invalid value: null: has to match plugin args"
        );
    }

    // validation_test.go TestValidateKubeSchedulerConfiguration
    // "bad-parallelism-invalid-value": parallelism 0 -> Invalid.
    #[test]
    fn parallelism_must_be_positive() {
        let err = volume_binding_args_from_config("parallelism: 0\n", false).unwrap_err();
        assert_eq!(
            err,
            "parallelism: Invalid value: 0: should be an integer value greater than zero"
        );
        assert!(volume_binding_args_from_config("parallelism: 8\n", false).is_ok());
        // Unset defaults to 16 (v1/defaults.go:108-110).
        assert!(volume_binding_args_from_config("{}\n", false).is_ok());
    }

    // validation.go:70-77
    #[test]
    fn backoff_seconds_validated() {
        let err =
            volume_binding_args_from_config("podInitialBackoffSeconds: 0\n", false).unwrap_err();
        assert!(
            err.starts_with("podInitialBackoffSeconds: Invalid value: 0: must be greater than 0")
        );
        let err = volume_binding_args_from_config(
            "podInitialBackoffSeconds: 5\npodMaxBackoffSeconds: 2\n",
            false,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "podMaxBackoffSeconds: Invalid value: 2: must be greater than or equal to PodInitialBackoffSeconds"
        );
        // Defaults 1 / 10; a lone max below the default initial is rejected.
        let err = volume_binding_args_from_config("podMaxBackoffSeconds: 0\n", false).unwrap_err();
        assert!(err.starts_with("podMaxBackoffSeconds: Invalid value: 0:"));
    }

    // "invalid-plugins": removed plugins in plugins.<stage>.enabled.
    #[test]
    fn removed_plugins_in_plugin_sets_rejected() {
        let doc = "profiles:\n- plugins:\n    score:\n      enabled:\n      - name: AzureDiskLimits\n      - name: PodTopologySpread\n      - name: GCEPDLimits\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        let v = "was invalid in version \"kubescheduler.config.k8s.io/v1\" (KubeSchedulerConfiguration is version \"kubescheduler.config.k8s.io/v1\")";
        assert_eq!(
            err,
            format!(
                "profiles[0].plugins.score.enabled[0]: Invalid value: \"AzureDiskLimits\": {v}, profiles[0].plugins.score.enabled[2]: Invalid value: \"GCEPDLimits\": {v}"
            )
        );
        // "valid-plugins"
        let ok = "profiles:\n- plugins:\n    score:\n      enabled:\n      - {name: PodTopologySpread, weight: 2}\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
        // Disabled entries and multiPoint are not checked (validation.go:166-182).
        let ok = "profiles:\n- plugins:\n    score:\n      disabled:\n      - name: AzureDiskLimits\n    multiPoint:\n      enabled:\n      - name: EBSLimits\n";
        assert!(volume_binding_args_from_config(ok, false).is_ok());
    }

    // "different-queue-sort"
    #[test]
    fn queue_sort_must_match_across_profiles() {
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: AnotherSort}]}}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[1].plugins.queueSort: Invalid value: {\"Enabled\":[{\"Name\":\"AnotherSort\",\"Weight\":0}],\"Disabled\":null}: must be the same for all profiles"
        );
        // "one-empty-queue-sort": second profile has no plugins at all.
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(err.starts_with("profiles[1].plugins.queueSort: Invalid value:"));
        // Same queue sort everywhere is fine.
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: PrioritySort}]}}\n";
        assert!(volume_binding_args_from_config(doc, false).is_ok());
    }

    // validation.go:221-228: only one queue sort plugin may be enabled.
    #[test]
    fn only_one_queue_sort_enabled() {
        let doc = "profiles:\n- plugins: {queueSort: {enabled: [{name: A}, {name: B}]}}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[0].plugins.queueSort.Enabled: Invalid value: 2: only one queue sort plugin can be enabled"
        );
    }

    // "mismatch-queue-sort": queueSort pluginConfig args must match too
    // (validation.go:243-247).
    #[test]
    fn queue_sort_plugin_config_must_match() {
        let doc = "profiles:\n- schedulerName: a\n  plugins: {queueSort: {enabled: [{name: S}]}}\n  pluginConfig: [{name: S, args: {x: 1}}]\n- schedulerName: b\n  plugins: {queueSort: {enabled: [{name: S}]}}\n  pluginConfig: [{name: S, args: {x: 2}}]\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert_eq!(
            err,
            "profiles[1].pluginConfig.args: Invalid value: {\"x\":2}: queueSort must be the same for all profiles"
        );
    }

    // validation_pluginargs_test.go TestValidateDynamicResourcesArgs
    // (:1159-1275). Detail is ignored upstream (ignoreBadValueDetail), so
    // type and field are pinned.
    mod dynamic_resources {
        use super::*;

        const SEC: i64 = 1_000_000_000;

        fn fts(filter: bool, binding_cond: bool, device_status: bool) -> DynamicResourcesFeatures {
            DynamicResourcesFeatures {
                dra_scheduler_filter_timeout: filter,
                dra_device_binding_conditions: binding_cond,
                dra_resource_claim_device_status: device_status,
            }
        }
        const ALL: fn() -> DynamicResourcesFeatures = || fts(true, true, true);

        fn a(filter: Option<i64>, binding: Option<i64>) -> DynamicResourcesArgs {
            DynamicResourcesArgs {
                filter_timeout: filter,
                binding_timeout: binding,
            }
        }

        fn check(
            args: DynamicResourcesArgs,
            f: DynamicResourcesFeatures,
            want: &[(ErrorType, &str)],
        ) {
            let errs = args.validate(&Path::default(), f);
            let got: Vec<(ErrorType, String)> = summary(&errs);
            let want: Vec<(ErrorType, String)> =
                want.iter().map(|(t, p)| (*t, p.to_string())).collect();
            assert_eq!(got, want, "{errs:?}");
        }

        #[test]
        fn valid_default_and_disabled() {
            check(a(Some(10 * SEC), Some(600 * SEC)), ALL(), &[]);
            check(a(None, None), ALL(), &[]);
        }

        #[test]
        fn negative_filter_timeout() {
            let errs = a(Some(-SEC), None).validate(&Path::default(), ALL());
            assert_eq!(errs[0].detail, "must be zero or positive");
            check(
                a(Some(-SEC), None),
                ALL(),
                &[(ErrorType::Invalid, "filterTimeout")],
            );
        }

        #[test]
        fn negative_filter_timeout_gate_disabled() {
            let errs = a(Some(-SEC), None).validate(&Path::default(), fts(false, true, true));
            assert_eq!(
                errs[0].detail,
                "DRASchedulingFilterTimeout feature gate is disabled"
            );
            check(
                a(Some(-SEC), None),
                fts(false, true, true),
                &[(ErrorType::Forbidden, "filterTimeout")],
            );
        }

        #[test]
        fn binding_timeout_cases() {
            check(a(None, Some(30 * SEC)), ALL(), &[]);
            check(
                a(None, Some(0)),
                ALL(),
                &[(ErrorType::Invalid, "bindingTimeout")],
            );
            check(
                a(None, Some(-SEC)),
                ALL(),
                &[(ErrorType::Invalid, "bindingTimeout")],
            );
            let errs = a(None, Some(0)).validate(&Path::default(), ALL());
            assert_eq!(errs[0].detail, "must be at least 1 second");
            for f in [fts(true, false, true), fts(true, true, false)] {
                check(
                    a(None, Some(SEC)),
                    f,
                    &[(ErrorType::Forbidden, "bindingTimeout")],
                );
            }
        }

        // v1/defaults.go:247-257
        #[test]
        fn defaults_follow_gates() {
            let mut x = DynamicResourcesArgs::default();
            x.set_defaults(ALL());
            assert_eq!(x, a(Some(10 * SEC), Some(600 * SEC)));
            let mut x = DynamicResourcesArgs::default();
            x.set_defaults(fts(false, true, true));
            assert_eq!(x, a(None, Some(600 * SEC)));
            for f in [fts(true, false, true), fts(true, true, false)] {
                let mut x = DynamicResourcesArgs::default();
                x.set_defaults(f);
                assert_eq!(x, a(Some(10 * SEC), None));
            }
            // Explicit values are kept.
            let mut x = a(Some(0), Some(SEC));
            x.set_defaults(ALL());
            assert_eq!(x, a(Some(0), Some(SEC)));
        }

        // validation.go:161-163 wiring: a negative filterTimeout in
        // pluginConfig is rejected end to end; absent args validate clean
        // with the default gates (defaults then validate).
        #[test]
        #[serial_test::serial]
        fn wired_into_validate_profiles() {
            rusternetes_common::feature_gates::reset_to_defaults();
            let bad = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n    args:\n      filterTimeout: -1s\n";
            let err = volume_binding_args_from_config(bad, false).unwrap_err();
            assert!(
                err.contains("profiles[0].pluginConfig[0].args.filterTimeout: Invalid value"),
                "{err}"
            );
            assert!(err.contains("must be zero or positive"), "{err}");
            let bad = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n    args:\n      bindingTimeout: 1s\n";
            let err = volume_binding_args_from_config(bad, false).unwrap_err();
            assert!(
                err.contains("profiles[0].pluginConfig[0].args.bindingTimeout: Forbidden"),
                "{err}"
            );
            let ok = "profiles:\n- pluginConfig:\n  - name: DynamicResources\n";
            assert!(volume_binding_args_from_config(ok, false).is_ok());
        }
    }
}
