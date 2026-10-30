// Admission Webhook Configuration resources
//
// This module defines ValidatingWebhookConfiguration and MutatingWebhookConfiguration
// resources that configure external admission webhooks.

use crate::resources::WebhookClientConfig;
use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// ValidatingWebhookConfiguration describes admission webhooks that validate resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingWebhookConfiguration {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<Vec<ValidatingWebhook>>,
}

impl ValidatingWebhookConfiguration {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: ObjectMeta::new(name),
            webhooks: None,
        }
    }
}

/// ValidatingWebhook describes a single validating webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingWebhook {
    /// Name is the full-qualified name of the webhook
    #[serde(default)]
    pub name: String,

    /// ClientConfig defines how to communicate with the webhook
    #[serde(default)]
    pub client_config: WebhookClientConfig,

    /// Rules describes what operations on what resources the webhook cares about
    #[serde(default)]
    pub rules: Vec<RuleWithOperations>,

    /// FailurePolicy defines how unrecognized errors are handled
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,

    /// MatchPolicy defines how the rules are applied
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_policy: Option<MatchPolicy>,

    /// NamespaceSelector decides whether to run the webhook on an object based on namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// ObjectSelector decides whether to run the webhook on an object based on labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_selector: Option<LabelSelector>,

    /// SideEffects states whether this webhook has side effects
    #[serde(default)]
    pub side_effects: SideEffectClass,

    /// TimeoutSeconds specifies the timeout for this webhook (1-30 seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// AdmissionReviewVersions is an ordered list of AdmissionReview versions the webhook accepts
    #[serde(default)]
    pub admission_review_versions: Vec<String>,

    /// MatchConditions are CEL expressions that must be true for the webhook to be called
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,
}

/// MutatingWebhookConfiguration describes admission webhooks that mutate resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingWebhookConfiguration {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<Vec<MutatingWebhook>>,
}

impl MutatingWebhookConfiguration {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "MutatingWebhookConfiguration".to_string(),
            metadata: ObjectMeta::new(name),
            webhooks: None,
        }
    }
}

/// MutatingWebhook describes a single mutating webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingWebhook {
    /// Name is the full-qualified name of the webhook
    #[serde(default)]
    pub name: String,

    /// ClientConfig defines how to communicate with the webhook
    #[serde(default)]
    pub client_config: WebhookClientConfig,

    /// Rules describes what operations on what resources the webhook cares about
    #[serde(default)]
    pub rules: Vec<RuleWithOperations>,

    /// FailurePolicy defines how unrecognized errors are handled
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,

    /// MatchPolicy defines how the rules are applied
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_policy: Option<MatchPolicy>,

    /// NamespaceSelector decides whether to run the webhook on an object based on namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// ObjectSelector decides whether to run the webhook on an object based on labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_selector: Option<LabelSelector>,

    /// SideEffects states whether this webhook has side effects
    #[serde(default)]
    pub side_effects: SideEffectClass,

    /// TimeoutSeconds specifies the timeout for this webhook (1-30 seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// AdmissionReviewVersions is an ordered list of AdmissionReview versions the webhook accepts
    #[serde(default)]
    pub admission_review_versions: Vec<String>,

    /// MatchConditions are CEL expressions that must be true for the webhook to be called
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,

    /// ReinvocationPolicy indicates whether this webhook should be called multiple times
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reinvocation_policy: Option<ReinvocationPolicy>,
}

/// RuleWithOperations describes what operations on what resources the webhook cares about
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuleWithOperations {
    /// Operations is the list of operations the webhook cares about.
    /// `#[serde(default)]` for Go-parity: upstream `operations` is `omitempty`.
    #[serde(default)]
    pub operations: Vec<OperationType>,

    /// Rule is embedded, it describes other criteria
    #[serde(flatten)]
    pub rule: Rule,
}

/// Rule describes what resources and scopes to match.
///
/// The list fields are `#[serde(default)]` to match Go's `json.Unmarshal`:
/// upstream `admissionregistration.Rule` marks `apiGroups`/`apiVersions`/
/// `resources` as `omitempty`, so a client (e.g. the sig-api-machinery webhook
/// e2e) may omit any of them. Decode must admit the object and let validation
/// enforce requirements, rather than erroring with "missing field `apiGroups`".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    /// APIGroups is the API groups the resources belong to ('*' means all)
    #[serde(rename = "apiGroups", default)]
    pub api_groups: Vec<String>,

    /// APIVersions is the API versions the resources belong to ('*' means all)
    #[serde(rename = "apiVersions", default)]
    pub api_versions: Vec<String>,

    /// Resources is a list of resources this rule applies to ('*' means all)
    #[serde(default)]
    pub resources: Vec<String>,

    /// Scope specifies the scope of this rule (Cluster, Namespaced, or *)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// OperationType specifies an operation for a request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationType {
    Create,
    Update,
    Delete,
    Connect,
    #[serde(rename = "*")]
    All,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2475).
    #[serde(untagged)]
    Unknown(String),
}

/// FailurePolicy defines how unrecognized errors from the webhook are handled
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FailurePolicy {
    /// Ignore means the error is ignored and the API request is allowed to continue
    Ignore,
    /// Fail means the API request is rejected
    Fail,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2475).
    #[serde(untagged)]
    Unknown(String),
}

/// MatchPolicy defines how the rules are applied when the request matches multiple rules
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MatchPolicy {
    /// Exact means the request matches only exact rules
    Exact,
    /// Equivalent means the request matches equivalent rules
    Equivalent,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2475).
    #[serde(untagged)]
    Unknown(String),
}

/// SideEffectClass denotes the level of side effects a webhook may have
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum SideEffectClass {
    /// Go's zero value: the field was absent from the body. Upstream's
    /// `SideEffectClass` is a bare string, so an omitted `sideEffects` decodes
    /// to `""` and `validateValidatingWebhook`
    /// (`pkg/apis/admissionregistration/validation/validation.go`) answers
    /// `field.NotSupported` — v1 accepts only `None` and `NoneOnDryRun`. There
    /// is no safe value to invent here: every named variant claims something
    /// about the webhook's behaviour that the client never said.
    #[default]
    #[serde(rename = "")]
    Unspecified,
    /// Unknown means the webhook may have unknown side effects
    Unknown,
    /// None means the webhook has no side effects on dryRun
    None,
    /// Some means the webhook may have side effects on dryRun
    Some,
    /// NoneOnDryRun means the webhook has no side effects when run in dry-run mode
    NoneOnDryRun,
    /// A value outside (named `Unrecognized` because `Unknown` is itself a
    /// valid upstream value) the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2475).
    #[serde(untagged)]
    Unrecognized(String),
}

/// ReinvocationPolicy indicates whether a webhook should be called multiple times
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReinvocationPolicy {
    /// Never means the webhook will not be called more than once in a single admission evaluation
    Never,
    /// IfNeeded means the webhook may be called again as part of the admission evaluation
    IfNeeded,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string`, so any string decodes and validation answers `Unsupported
    /// value` (422), not a 400 decode failure (#2475).
    #[serde(untagged)]
    Unknown(String),
}

/// LabelSelector is used to select resources by labels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    /// MatchLabels is a map of {key,value} pairs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<std::collections::HashMap<String, String>>,

    /// MatchExpressions is a list of label selector requirements
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
}

/// LabelSelectorRequirement is a selector that contains values, a key, and an operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    /// Key is the label key that the selector applies to
    #[serde(default)]
    pub key: String,

    /// Operator represents a key's relationship to a set of values
    #[serde(default)]
    pub operator: LabelSelectorOperator,

    /// Values is an array of string values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// LabelSelectorOperator is the set of operators for label selector requirements
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum LabelSelectorOperator {
    /// Go's zero value: `operator` was absent. Upstream's is a bare string, so
    /// the requirement reaches `ValidateLabelSelectorRequirement`
    /// (`apimachinery/pkg/apis/meta/v1/validation/validation.go`), which answers
    /// `Invalid: not a valid selector operator`. Defaulting to any real operator
    /// would instead select pods the client never asked for.
    #[default]
    #[serde(rename = "")]
    Unspecified,
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

/// MatchCondition represents a condition that must be fulfilled for a request to be sent to a webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MatchCondition {
    /// Name is an identifier for this match condition
    #[serde(default)]
    pub name: String,

    /// Expression is a CEL expression that must evaluate to true
    #[serde(default)]
    pub expression: String,
}
