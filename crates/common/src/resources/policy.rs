use crate::types::{LabelSelector, ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ResourceQuota sets aggregate quota restrictions enforced per namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuota {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    #[serde(default)]
    pub spec: ResourceQuotaSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ResourceQuotaStatus>,
}

impl ResourceQuota {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: ResourceQuotaSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// ResourceQuotaSpec defines the desired hard limits to enforce for Quota
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuotaSpec {
    /// Hard is the set of desired hard limits for each named resource
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub hard: Option<HashMap<String, String>>,

    /// A collection of filters that must match each object tracked by a quota
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,

    /// ScopeSelector is also a collection of filters like scopes that must match each object
    #[serde(skip_serializing_if = "Option::is_none", rename = "scopeSelector")]
    pub scope_selector: Option<ScopeSelector>,
}

/// ScopeSelector represents the AND of the selectors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSelector {
    /// A list of scope selector requirements by scope
    #[serde(default)]
    pub match_expressions: Vec<ScopedResourceSelectorRequirement>,
}

/// ScopedResourceSelectorRequirement is a selector that contains values, a scope name, and an operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopedResourceSelectorRequirement {
    /// The name of the scope that the selector applies to
    #[serde(default)]
    pub scope_name: String,

    /// Operator: In, NotIn, Exists, DoesNotExist
    #[serde(default)]
    pub operator: String,

    /// An array of string values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// ResourceQuotaStatus defines the enforced hard limits and observed use
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuotaStatus {
    /// Hard is the set of enforced hard limits for each named resource
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub hard: Option<HashMap<String, String>>,

    /// Used is the current observed total usage of the resource in the namespace
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub used: Option<HashMap<String, String>>,
}

/// LimitRange sets resource usage limits for each kind of resource in a Namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRange {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    #[serde(default)]
    pub spec: LimitRangeSpec,
}

impl LimitRange {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: LimitRangeSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "LimitRange".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
        }
    }
}

/// LimitRangeSpec defines a min/max usage limit for resources that match on kind
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeSpec {
    /// Limits is a list of LimitRangeItem objects
    #[serde(default)]
    pub limits: Vec<LimitRangeItem>,
}

/// LimitRangeItem defines a min/max usage limit for any resource that matches on kind
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeItem {
    /// Type of resource that this limit applies to
    /// Valid values: Pod, Container, PersistentVolumeClaim
    #[serde(rename = "type", default)]
    pub item_type: String,

    /// Max usage constraints on this kind by resource name
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub max: Option<HashMap<String, String>>,

    /// Min usage constraints on this kind by resource name
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub min: Option<HashMap<String, String>>,

    /// Default resource requirement limit value by resource name if not specified
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub default: Option<HashMap<String, String>>,

    /// DefaultRequest is the default resource requirement request value by resource name if not specified
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub default_request: Option<HashMap<String, String>>,

    /// MaxLimitRequestRatio represents the max burst value for the named resource
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub max_limit_request_ratio: Option<HashMap<String, String>>,
}

/// PriorityClass defines mapping from a priority class name to the priority integer value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    /// The value of this priority class
    /// Higher values indicate higher priority
    /// Typical range: -2147483648 to 1000000000
    /// Values above 1000000000 are reserved for system use
    #[serde(default)]
    pub value: i32,

    /// GlobalDefault specifies whether this PriorityClass should be considered as the default
    #[serde(skip_serializing_if = "Option::is_none", rename = "globalDefault")]
    pub global_default: Option<bool>,

    /// Description is an arbitrary string that usually provides guidelines on when this priority class should be used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// PreemptionPolicy is the Policy for preempting pods with lower priority
    /// Valid values: PreemptLowerPriority, Never
    #[serde(skip_serializing_if = "Option::is_none", rename = "preemptionPolicy")]
    pub preemption_policy: Option<String>,
}

impl PriorityClass {
    pub fn new(name: impl Into<String>, value: i32) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "PriorityClass".to_string(),
                api_version: "scheduling.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            value,
            global_default: None,
            description: None,
            preemption_policy: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_global_default(mut self, global_default: bool) -> Self {
        self.global_default = Some(global_default);
        self
    }

    pub fn with_preemption_policy(mut self, policy: impl Into<String>) -> Self {
        self.preemption_policy = Some(policy.into());
        self
    }
}

/// `policy/v1.Eviction` (`staging/src/k8s.io/api/policy/v1/types.go`): the
/// body of a POST to `pods/{name}/eviction`. `metadata` names the pod and
/// `deleteOptions` are handed to the pod's delete.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Eviction {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_options: Option<crate::deletion::DeleteOptions>,
}

/// PodDisruptionBudget limits the number of pods that can be disrupted during voluntary disruptions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudget {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    #[serde(default)]
    pub spec: PodDisruptionBudgetSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PodDisruptionBudgetStatus>,
}

impl PodDisruptionBudget {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: PodDisruptionBudgetSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "PodDisruptionBudget".to_string(),
                api_version: "policy/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// PodDisruptionBudgetSpec describes what disruptions are allowed
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetSpec {
    /// Minimum number of pods that must be available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_available: Option<IntOrString>,

    /// Maximum number of pods that can be unavailable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,

    /// Label selector to identify the set of pods.
    ///
    /// A **pointer** upstream (`staging/src/k8s.io/api/policy/v1/types.go:36-42`,
    /// `Selector *metav1.LabelSelector`), and its doc comment spells out why
    /// that matters: "A null selector will match no pods, while an empty ({})
    /// selector will select all pods within the namespace." Both readers — the
    /// eviction endpoint (`pkg/registry/core/pod/storage/eviction.go:498`) and
    /// the disruption controller
    /// (`pkg/controller/disruption/disruption.go:630`) — go through
    /// `LabelSelectorAsSelector`, which maps `nil` to `labels.Nothing()` and
    /// `&LabelSelector{}` to `labels.Everything()`
    /// (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`). So this is an
    /// `Option`, never a defaulted value: defaulting it would turn a PDB that
    /// guards nothing into one that guards every pod in its namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,

    /// UnhealthyPodEvictionPolicy controls when unhealthy pods should be considered for eviction
    /// Valid values: IfHealthyBudget, AlwaysAllow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unhealthy_pod_eviction_policy: Option<String>,
}

/// IntOrString can be an integer or a string percentage (e.g., "20%")
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum IntOrString {
    /// Upstream's zero value is `{Type: Int, IntVal: 0}` — `Type` is an
    /// `iota`-based enum whose first constant is `Int`
    /// (`apimachinery/pkg/util/intstr/intstr.go:50-54`), so a field of this
    /// type decodes to the integer `0` when the body omits it, and validation
    /// (not the decoder) answers for the missing port.
    Int(i32),
    String(String),
}

impl Default for IntOrString {
    fn default() -> Self {
        IntOrString::Int(0)
    }
}

impl IntOrString {
    /// Encode the way upstream `intstr.MarshalJSON`
    /// (`apimachinery/pkg/util/intstr/intstr.go`) does: an `Int` becomes a JSON
    /// **number**, a `String` a JSON **string**. Callers that need to inspect
    /// the value (validation, defaulting) must go through this rather than
    /// stringifying, so the int-vs-string discriminator Go clients depend on is
    /// never flattened away.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            IntOrString::Int(i) => serde_json::Value::Number((*i).into()),
            IntOrString::String(s) => serde_json::Value::String(s.clone()),
        }
    }
}

impl std::fmt::Display for IntOrString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntOrString::Int(i) => write!(f, "{}", i),
            IntOrString::String(s) => write!(f, "{}", s),
        }
    }
}

/// PodDisruptionBudgetStatus represents the current status of a PDB
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetStatus {
    /// Current number of healthy pods
    #[serde(default)]
    pub current_healthy: i32,

    /// Minimum desired healthy pods
    #[serde(default)]
    pub desired_healthy: i32,

    /// Number of pod disruptions that are currently allowed
    #[serde(default)]
    pub disruptions_allowed: i32,

    /// Total number of pods counted by this disruption budget
    #[serde(default)]
    pub expected_pods: i32,

    /// Most recent generation observed when updating this PDB status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions contain the latest available observations of the PDB's state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PodDisruptionBudgetCondition>>,

    /// DisruptedPods contains information about pods whose eviction was
    /// processed by the API server eviction subresource handler but has not
    /// yet been observed by the PodDisruptionBudget controller.
    /// A pod will be in this map from the time when the API server processed the
    /// eviction request to the time when the pod is seen by PDB controller
    /// as having been marked for deletion (or after a timeout). The key in the map is the name of the pod
    /// and the value is the time when the API server processed the eviction request.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time_map::serialize",
        deserialize_with = "crate::types::k8s_time_map::deserialize"
    )]
    pub disrupted_pods: Option<HashMap<String, DateTime<Utc>>>,
}

/// PodDisruptionBudgetCondition contains details for the current condition of this PDB
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetCondition {
    /// Type of PDB condition
    #[serde(rename = "type", default)]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    #[serde(default)]
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// observedGeneration represents the .metadata.generation the condition was based upon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}
