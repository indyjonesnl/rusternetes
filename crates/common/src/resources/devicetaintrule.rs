//! `DeviceTaintRule` (`resource.k8s.io/v1alpha3`).
//!
//! Port of `staging/src/k8s.io/api/resource/v1alpha3/types.go`
//! (`DeviceTaintRule` :195-210, `DeviceTaintRuleSpec` :213-226,
//! `DeviceTaintSelector` :231-283, `DeviceTaintRuleStatus` :286-314,
//! `DeviceTaintRuleStatusMaxConditions` :317, list :326-334). Served only
//! while the `DRADeviceTaintRules` feature gate is on (alpha, off in v1.35,
//! `pkg/features/kube_features.go:1240`).

use crate::resources::dra::DeviceTaint;
use crate::types::{Condition, ListMeta, ObjectMeta};
use serde::{Deserialize, Serialize};

/// `DeviceTaintRuleStatusMaxConditions` (types.go:317).
pub const DEVICE_TAINT_RULE_STATUS_MAX_CONDITIONS: usize = 8;

fn default_api_version() -> String {
    "resource.k8s.io/v1alpha3".to_string()
}

fn default_kind() -> String {
    "DeviceTaintRule".to_string()
}

fn default_list_kind() -> String {
    "DeviceTaintRuleList".to_string()
}

/// Adds one taint to all devices which match the selector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaintRule {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Go's `Spec` is a non-pointer struct, so it always serializes.
    #[serde(default)]
    pub spec: DeviceTaintRuleSpec,
    /// Go's `Status` is a non-pointer struct, so it always serializes (`{}`).
    #[serde(default)]
    pub status: DeviceTaintRuleStatus,
}

impl Default for DeviceTaintRule {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            metadata: ObjectMeta::default(),
            spec: DeviceTaintRuleSpec::default(),
            status: DeviceTaintRuleStatus::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaintRuleSpec {
    /// Which device(s) the taint is applied to. The empty selector matches
    /// all devices; without a selector, no devices are matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_selector: Option<DeviceTaintSelector>,
    /// The taint that gets applied to matching devices (non-pointer in Go).
    #[serde(default)]
    pub taint: DeviceTaint,
}

/// `DeviceTaintSelector`. `deviceClassName` and `selectors` are tombstoned
/// since 1.35 (types.go:238-242, :278-282), so they are not decoded.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaintSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaintRuleStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaintRuleList {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_list_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ListMeta,
    #[serde(default)]
    pub items: Vec<DeviceTaintRule>,
}
