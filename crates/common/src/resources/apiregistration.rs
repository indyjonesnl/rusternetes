//! `apiregistration.k8s.io/v1` APIService — the types of
//! `staging/src/k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/types.go`
//! and the helpers of `.../v1/helper/helpers.go`.

use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// `metav1.Time`'s wire form: RFC3339, second precision, `Z` suffix.
fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// `APIService` (types.go:104-117): a server for a particular GroupVersion.
/// Name must be `version.group`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct APIService {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// `Spec APIServiceSpec json:"spec,omitempty"` -- a struct, so always
    /// serialized.
    #[serde(default)]
    pub spec: APIServiceSpec,
    /// `Status APIServiceStatus json:"status,omitempty"` -- likewise.
    #[serde(default)]
    pub status: APIServiceStatus,
}

/// `ServiceReference` (types.go:33-42).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct APIServiceReference {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Defaults to 443 (`SetDefaults_ServiceReference`, v1/defaults.go).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
}

/// `APIServiceSpec` (types.go:46-82).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct APIServiceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<APIServiceReference>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(
        default,
        rename = "insecureSkipTLSVerify",
        skip_serializing_if = "is_false"
    )]
    pub insecure_skip_tls_verify: bool,
    /// `CABundle []byte` -- base64 on the wire, kept as the wire string.
    #[serde(default, rename = "caBundle", skip_serializing_if = "Option::is_none")]
    pub ca_bundle: Option<String>,
    #[serde(default)]
    pub group_priority_minimum: i32,
    #[serde(default)]
    pub version_priority: i32,
}

/// `APIServiceCondition` (types.go:142-155).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct APIServiceCondition {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// `APIServiceStatus` (types.go:158-166).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct APIServiceStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<APIServiceCondition>,
}

/// `Available` (types.go:130).
pub const AVAILABLE: &str = "Available";

/// Message upstream attaches to the `Available` condition of a local
/// APIService (`NewLocalAvailableAPIServiceCondition`, helpers.go:96-104).
pub const LOCAL_AVAILABLE_MESSAGE: &str = "Local APIServices are always available";

/// `NewLocalAvailableAPIServiceCondition` (helpers.go:96-104).
pub fn new_local_available_condition() -> APIServiceCondition {
    APIServiceCondition {
        type_: AVAILABLE.to_string(),
        status: "True".to_string(),
        last_transition_time: Some(now()),
        reason: "Local".to_string(),
        message: LOCAL_AVAILABLE_MESSAGE.to_string(),
    }
}

/// `SetAPIServiceCondition` (helpers.go:108-122).
pub fn set_api_service_condition(api_service: &mut APIService, new: APIServiceCondition) {
    match api_service
        .status
        .conditions
        .iter_mut()
        .find(|c| c.type_ == new.type_)
    {
        None => api_service.status.conditions.push(new),
        Some(existing) => {
            if existing.status != new.status {
                existing.status = new.status;
                existing.last_transition_time = new.last_transition_time;
            }
            existing.reason = new.reason;
            existing.message = new.message;
        }
    }
}

/// `SetDefaults_ServiceReference` (v1/defaults.go): the port defaults to 443.
pub fn set_defaults_api_service(api_service: &mut APIService) {
    if let Some(service) = api_service.spec.service.as_mut() {
        if service.port.is_none() {
            service.port = Some(443);
        }
    }
}
