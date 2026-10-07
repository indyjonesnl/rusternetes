//! `PodCertificateRequest` (`certificates.k8s.io/v1beta1`).
//!
//! Port of `staging/src/k8s.io/api/certificates/v1beta1/types.go`
//! (`PodCertificateRequestSpec` :384-491, `PodCertificateRequestStatus`
//! :496-565, condition constants :568-586). Served only when the
//! `PodCertificateRequest` feature gate is on (upstream
//! `pkg/features/kube_features.go::PodCertificateRequest`).
//!
//! The `[]byte` spec fields (`pkixPublicKey`, `proofOfPossession`) are base64
//! on the wire, exactly as Go's `encoding/json` encodes `[]byte`.

use crate::types::{k8s_time, Condition, ObjectMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

/// `certificates.MaxPKIXPublicKeySize` (`pkg/apis/certificates/types.go:484`).
pub const MAX_PKIX_PUBLIC_KEY_SIZE: usize = 10 * 1024;
/// `certificates.MaxProofOfPossessionSize` (`types.go:487`).
pub const MAX_PROOF_OF_POSSESSION_SIZE: usize = 10 * 1024;
/// `certificates.MaxCertificateChainSize` (`types.go:493`).
pub const MAX_CERTIFICATE_CHAIN_SIZE: usize = 100 * 1024;
/// `certificates.MinMaxExpirationSeconds` (`types.go:495`).
pub const MIN_MAX_EXPIRATION_SECONDS: i32 = 60 * 60;
/// `certificates.MaxMaxExpirationSeconds` (`types.go:498`).
pub const MAX_MAX_EXPIRATION_SECONDS: i32 = 91 * 24 * 60 * 60;
/// `certificates.KubernetesMaxMaxExpirationSeconds` (`types.go:501`).
pub const KUBERNETES_MAX_MAX_EXPIRATION_SECONDS: i32 = 24 * 60 * 60;

/// `PodCertificateRequestConditionTypeDenied` (v1beta1 `types.go:570`).
pub const CONDITION_TYPE_DENIED: &str = "Denied";
/// `PodCertificateRequestConditionTypeFailed` (`:572`).
pub const CONDITION_TYPE_FAILED: &str = "Failed";
/// `PodCertificateRequestConditionTypeIssued` (`:574`).
pub const CONDITION_TYPE_ISSUED: &str = "Issued";

fn default_api_version() -> String {
    "certificates.k8s.io/v1beta1".to_string()
}

fn default_kind() -> String {
    "PodCertificateRequest".to_string()
}

/// A request for a certificate for one pod, made on its behalf by the kubelet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCertificateRequest {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: PodCertificateRequestSpec,
    /// Go's `Status` is a non-pointer struct, so it always serializes (`{}`).
    #[serde(default)]
    pub status: PodCertificateRequestStatus,
}

impl Default for PodCertificateRequest {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            metadata: ObjectMeta::default(),
            spec: PodCertificateRequestSpec::default(),
            status: PodCertificateRequestStatus::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PodCertificateRequestSpec {
    #[serde(rename = "signerName", default)]
    pub signer_name: String,
    #[serde(rename = "podName", default)]
    pub pod_name: String,
    #[serde(rename = "podUID", default)]
    pub pod_uid: String,
    #[serde(rename = "serviceAccountName", default)]
    pub service_account_name: String,
    #[serde(rename = "serviceAccountUID", default)]
    pub service_account_uid: String,
    #[serde(rename = "nodeName", default)]
    pub node_name: String,
    #[serde(rename = "nodeUID", default)]
    pub node_uid: String,
    #[serde(
        rename = "maxExpirationSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_expiration_seconds: Option<i32>,
    /// PKIX-serialized public key (base64 on the wire).
    #[serde(
        rename = "pkixPublicKey",
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub pkix_public_key: Vec<u8>,
    /// Signature over `podUID` made with the private key (base64 on the wire).
    #[serde(
        rename = "proofOfPossession",
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub proof_of_possession: Vec<u8>,
    #[serde(
        rename = "unverifiedUserAnnotations",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub unverified_user_annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCertificateRequestStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// PEM-wrapped certificate chain (leaf first). Plain string on the wire.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub certificate_chain: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize"
    )]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize"
    )]
    pub begin_refresh_at: Option<DateTime<Utc>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize"
    )]
    pub not_after: Option<DateTime<Utc>>,
}

fn serialize_bytes<S: Serializer>(data: &[u8], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        data,
    ))
}

fn deserialize_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    // Go marshals a nil `[]byte` as `null`.
    let Some(encoded) = Option::<String>::deserialize(d)? else {
        return Ok(Vec::new());
    };
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(serde::de::Error::custom)
}
