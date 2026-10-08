// MutatingAdmissionPolicy resources
//
// Port of `staging/src/k8s.io/api/admissionregistration/v1beta1/types.go`
// (release-1.35): `MutatingAdmissionPolicy` (`:1202`), its spec (`:1233`),
// `Mutation` (`:1321`), `ApplyConfiguration` (`:1352`), `JSONPatch` (`:1397`)
// and `MutatingAdmissionPolicyBinding{,Spec}` (`:1506`). The shared pieces
// (`ParamKind`, `MatchResources`, `Variable`, `ParamRef`, `MatchCondition`,
// `FailurePolicy`) are the ones `ValidatingAdmissionPolicy` uses, as upstream
// shares them.

use crate::resources::admission_webhook::MatchCondition;
use crate::resources::serde_helpers::empty_string_as_none;
use crate::resources::validating_admission_policy::{
    FailurePolicy, MatchResources, ParamKind, ParamRef, Variable,
};
use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// MutatingAdmissionPolicy describes the definition of an admission mutation policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingAdmissionPolicy {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<MutatingAdmissionPolicySpec>,
}

impl MutatingAdmissionPolicy {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1beta1".to_string(),
            kind: "MutatingAdmissionPolicy".to_string(),
            metadata: ObjectMeta::new(name),
            spec: None,
        }
    }
}

/// MutatingAdmissionPolicySpec is the specification of the desired behavior of the policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MutatingAdmissionPolicySpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param_kind: Option<ParamKind>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_constraints: Option<MatchResources>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<Vec<Variable>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutations: Option<Vec<Mutation>>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub failure_policy: Option<FailurePolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,

    /// Upstream's field is a plain string with `omitempty`: absent decodes to
    /// the empty string, which validation answers with `Required`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub reinvocation_policy: Option<ReinvocationPolicyType>,
}

/// Mutation specifies the CEL expression which is used to apply the Mutation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Mutation {
    /// An empty string (or absent) is answered with `Required` by `validateMutation`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub patch_type: Option<PatchType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_configuration: Option<ApplyConfiguration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_patch: Option<JsonPatch>,
}

/// PatchType specifies the type of patch operation for a mutation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PatchType {
    ApplyConfiguration,
    #[serde(rename = "JSONPatch")]
    JsonPatch,
    /// A value outside the supported set; upstream's field is a plain string,
    /// so validation answers `Unsupported value` (422), not a 400 (#2475).
    #[serde(untagged)]
    Unknown(String),
}

/// ApplyConfiguration defines the desired configuration values of an object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ApplyConfiguration {
    #[serde(default)]
    pub expression: String,
}

/// JSONPatch defines a JSON Patch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct JsonPatch {
    #[serde(default)]
    pub expression: String,
}

/// ReinvocationPolicyType (`v1.ReinvocationPolicyType`)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReinvocationPolicyType {
    Never,
    IfNeeded,
    /// A value outside the supported set; see [`PatchType::Unknown`].
    #[serde(untagged)]
    Unknown(String),
}

/// MutatingAdmissionPolicyBinding binds a MutatingAdmissionPolicy with parametrized resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingAdmissionPolicyBinding {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<MutatingAdmissionPolicyBindingSpec>,
}

impl MutatingAdmissionPolicyBinding {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1beta1".to_string(),
            kind: "MutatingAdmissionPolicyBinding".to_string(),
            metadata: ObjectMeta::new(name),
            spec: None,
        }
    }
}

/// MutatingAdmissionPolicyBindingSpec is the specification of the MutatingAdmissionPolicyBinding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MutatingAdmissionPolicyBindingSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub param_ref: Option<ParamRef>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_resources: Option<MatchResources>,
}
