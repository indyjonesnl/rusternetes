//! Conversion webhook support for Custom Resource Definitions
//!
//! This module implements Kubernetes-compatible conversion webhooks that allow
//! automatic conversion between different versions of custom resources.

use k3rs_common::resources::{CustomResource, CustomResourceDefinition, WebhookClientConfig};
use k3rs_common::Result;
use k3rs_storage::Storage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

/// ConversionReview is the request/response object for conversion webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionReview {
    pub api_version: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<ConversionRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<ConversionResponse>,
}

/// ConversionRequest describes the conversion request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionRequest {
    /// UID is an identifier for the conversion request
    pub uid: String,
    /// DesiredAPIVersion is the version to convert to.
    ///
    /// Wire field is `desiredAPIVersion` (uppercase `API`), matching the
    /// upstream apiextensions.k8s.io/v1 `ConversionRequest` schema —
    /// `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types.go`.
    #[serde(rename = "desiredAPIVersion")]
    pub desired_api_version: String,
    /// Objects is the list of custom resources to convert.
    ///
    /// Tolerate an explicit `null` on deserialize: webhook responses echo the
    /// request with `objects: null`, and we only read the `response` field, so a
    /// null here must not fail the whole parse.
    #[serde(default, deserialize_with = "k3rs_common::serde_helpers::null_default")]
    pub objects: Vec<serde_json::Value>,
}

/// ConversionResponse describes the conversion response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionResponse {
    /// UID echoes the request UID
    pub uid: String,
    /// ConvertedObjects is the list of converted custom resources
    pub converted_objects: Vec<serde_json::Value>,
    /// Result indicates whether the conversion succeeded
    pub result: ConversionResult,
}

/// ConversionResult indicates the success or failure of a conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
}

impl ConversionResult {
    pub fn success() -> Self {
        Self {
            status: "Success".to_string(),
            message: None,
            reason: None,
            code: None,
        }
    }

    pub fn failure(message: String) -> Self {
        Self {
            status: "Failure".to_string(),
            message: Some(message),
            reason: Some("ConversionError".to_string()),
            code: Some(500),
        }
    }
}

/// Conversion webhook client
pub struct ConversionWebhookClient {}

impl ConversionWebhookClient {
    /// Create a new conversion webhook client
    pub fn new() -> Self {
        Self {}
    }

    /// Convert custom resources using a webhook
    pub async fn convert<S: Storage>(
        &self,
        crd: &CustomResourceDefinition,
        objects: Vec<CustomResource>,
        desired_version: &str,
        storage: &Arc<S>,
    ) -> Result<Vec<CustomResource>> {
        // Check if conversion is enabled
        let conversion = crd.spec.conversion.as_ref().ok_or_else(|| {
            k3rs_common::Error::InvalidResource("Conversion not configured for CRD".to_string())
        })?;

        // Get webhook configuration
        let webhook = conversion.webhook.as_ref().ok_or_else(|| {
            k3rs_common::Error::InvalidResource(
                "Webhook conversion strategy requires webhook configuration".to_string(),
            )
        })?;

        // `webhookConverter`'s client (apiextensions-apiserver
        // pkg/apiserver/conversion/webhook_converter.go) is
        // `ClientManager.HookClient`: a service is dialed at the address
        // `ServiceResolver.ResolveEndpoint` returns, through the egress
        // selector's `Cluster` lookup; a URL through `ControlPlane`.
        let target = self.build_webhook_target(&webhook.client_config)?;
        let target =
            k3rs_admission_webhook::AdmissionWebhookClient::resolve_webhook_target(target, storage)
                .await;
        let url = target.url.clone();

        info!(
            "Calling conversion webhook at {} for CRD {} to version {}",
            url, crd.metadata.name, desired_version
        );

        // Prepare conversion request
        let request = ConversionRequest {
            uid: uuid::Uuid::new_v4().to_string(),
            desired_api_version: format!("{}/{}", crd.spec.group, desired_version),
            objects: objects
                .iter()
                .map(|obj| serde_json::to_value(obj).unwrap())
                .collect(),
        };

        let review = ConversionReview {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "ConversionReview".to_string(),
            request: Some(request.clone()),
            response: None,
        };

        // `caBundle` is `[]byte`, base64 on the wire.
        let ca_data = webhook.client_config.ca_bundle.as_ref().map(|ca_b64| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(ca_b64)
                .unwrap_or_else(|_| ca_b64.as_bytes().to_vec())
        });

        // Call webhook
        debug!("Sending conversion request: {:?}", review);

        // A failed conversion webhook is a hard error (surfaced as 500) — it must
        // NOT silently pass through the stored objects unconverted, which would
        // serve the wrong apiVersion than the client requested.
        let body = serde_json::to_vec(&review).map_err(|e| {
            k3rs_common::Error::Internal(format!("failed to encode conversion request: {e}"))
        })?;
        let response = k3rs_admission_webhook::transport::transport()
            .post_json(k3rs_admission_webhook::transport::WebhookHttpRequest {
                target: &target,
                body,
                timeout: Duration::from_secs(30),
                ca_bundle: ca_data.as_deref(),
            })
            .await
            .map_err(|e| {
                k3rs_common::Error::Network(format!(
                    "Conversion webhook {url} unreachable: {}",
                    e.message
                ))
            })?;

        if !(200..300).contains(&response.status) {
            return Err(k3rs_common::Error::Network(format!(
                "Conversion webhook {url} returned status {}",
                response.status
            )));
        }

        // Parse with a universal deserializer: K8s webhooks (notably the e2e
        // CRD conversion webhook) may reply with YAML rather than JSON, and the
        // upstream apiserver tolerates both. serde_yaml accepts JSON too (JSON
        // is a YAML subset), so it covers both content types.
        let body = String::from_utf8_lossy(&response.body).into_owned();
        let review_response: ConversionReview = serde_yaml::from_str(&body).map_err(|e| {
            k3rs_common::Error::Network(format!("Failed to parse webhook response: {e}"))
        })?;

        debug!("Received conversion response: {:?}", review_response);

        // Extract response
        let conv_response = review_response.response.ok_or_else(|| {
            k3rs_common::Error::Network("Webhook response missing response field".to_string())
        })?;

        // Check result
        if conv_response.result.status != "Success" {
            return Err(k3rs_common::Error::Network(format!(
                "Conversion failed: {}",
                conv_response
                    .result
                    .message
                    .unwrap_or_else(|| "Unknown error".to_string())
            )));
        }

        // Deserialize converted objects
        let converted_objects: Vec<CustomResource> = conv_response
            .converted_objects
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                k3rs_common::Error::Network(format!(
                    "Failed to deserialize converted objects: {}",
                    e
                ))
            })?;

        info!(
            "Successfully converted {} objects to version {}",
            converted_objects.len(),
            desired_version
        );

        Ok(converted_objects)
    }

    /// `hookClientConfig` (util/webhook/client.go): see
    /// [`k3rs_admission_webhook::AdmissionWebhookClient::build_webhook_target`].
    pub fn build_webhook_target(
        &self,
        config: &WebhookClientConfig,
    ) -> Result<k3rs_admission_webhook::transport::WebhookTarget> {
        k3rs_admission_webhook::AdmissionWebhookClient::new().build_webhook_target(config)
    }
}

impl Default for ConversionWebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a custom resource to a different version.
///
/// Mirrors the upstream apiextensions-apiserver behavior:
/// - If the stored object is already at `target_version`, return it as-is.
/// - If `spec.conversion` is unset, default to `None` strategy (no schema change,
///   just rewrite `apiVersion`).
/// - For `None` strategy, rewrite the `apiVersion` field on the wire object.
/// - For `Webhook` strategy, POST a `ConversionReview` to the configured
///   webhook URL and return the converted object from the response.
pub async fn convert_custom_resource<S: Storage>(
    crd: &CustomResourceDefinition,
    resource: CustomResource,
    target_version: &str,
    storage: &Arc<S>,
) -> Result<CustomResource> {
    convert_custom_resources(crd, vec![resource], target_version, storage)
        .await
        .map(|mut v| v.remove(0))
}

/// Batch version of [`convert_custom_resource`] — converts a list of CRs in a
/// single ConversionReview round-trip when the strategy is Webhook.
pub async fn convert_custom_resources<S: Storage>(
    crd: &CustomResourceDefinition,
    resources: Vec<CustomResource>,
    target_version: &str,
    storage: &Arc<S>,
) -> Result<Vec<CustomResource>> {
    if resources.is_empty() {
        return Ok(resources);
    }

    // Default strategy is None when spec.conversion — or its strategy — is
    // unset, matching `SetDefaults_CustomResourceDefinitionSpec`
    // (`apiextensions/v1/defaults.go:48-52`).
    let strategy = crd
        .spec
        .conversion
        .as_ref()
        .and_then(|c| c.strategy.clone())
        .unwrap_or(k3rs_common::resources::ConversionStrategyType::None);

    match strategy {
        k3rs_common::resources::ConversionStrategyType::None
        | k3rs_common::resources::ConversionStrategyType::Unknown(_) => {
            // An unknown strategy is refused at admission (validateEnumStrings,
            // validation.go:617); treat any stored one like None.
            // No conversion - just update the API version on objects that need it.
            let target_api_version = format!("{}/{}", crd.spec.group, target_version);
            Ok(resources
                .into_iter()
                .map(|mut r| {
                    if extract_version(&r.api_version) != target_version {
                        r.api_version = target_api_version.clone();
                    }
                    r
                })
                .collect())
        }
        k3rs_common::resources::ConversionStrategyType::Webhook => {
            // Webhook strategy: only objects whose stored version differs from
            // the target need to round-trip through the webhook. Preserve
            // input order when stitching converted objects back into the result.
            let mut needs_idx: Vec<usize> = Vec::new();
            let mut needs_obj: Vec<CustomResource> = Vec::new();
            let mut passthrough: Vec<(usize, CustomResource)> = Vec::new();
            for (i, r) in resources.into_iter().enumerate() {
                if extract_version(&r.api_version) == target_version {
                    passthrough.push((i, r));
                } else {
                    needs_idx.push(i);
                    needs_obj.push(r);
                }
            }

            let converted = if needs_obj.is_empty() {
                Vec::new()
            } else {
                ConversionWebhookClient::new()
                    .convert(crd, needs_obj, target_version, storage)
                    .await?
            };

            let mut indexed: Vec<(usize, CustomResource)> = passthrough;
            indexed.extend(needs_idx.into_iter().zip(converted));
            indexed.sort_by_key(|(i, _)| *i);
            Ok(indexed.into_iter().map(|(_, r)| r).collect())
        }
    }
}

/// Extract version from API version string (e.g., "stable.example.com/v1" -> "v1")
pub fn extract_version(api_version: &str) -> &str {
    api_version.split('/').next_back().unwrap_or(api_version)
}
